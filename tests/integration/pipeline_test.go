//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/57Ajay/relay/internal/api"
	"github.com/57Ajay/relay/internal/event"
	"github.com/57Ajay/relay/internal/kafka"
	"github.com/57Ajay/relay/internal/processor"
	"github.com/57Ajay/relay/internal/storage"
	"github.com/57Ajay/relay/internal/telemetry"
)

type env struct {
	brokers   []string
	topic     string
	dlq       string
	eventType string
	store     *storage.Store
	producer  *kafka.Producer
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// setup creates isolated topics and a unique event type so tests can share
// one Kafka and PostgreSQL instance.
func setup(t *testing.T) *env {
	t.Helper()
	ctx := context.Background()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	e := &env{
		brokers:   []string{getenv("RELAY_TEST_KAFKA_BROKERS", "localhost:9094")},
		topic:     "it-events-" + suffix,
		dlq:       "it-events-dlq-" + suffix,
		eventType: "it.t" + suffix,
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	if err != nil {
		t.Fatal(err)
	}
	adm := kadm.NewClient(cl)
	if _, err := adm.CreateTopics(ctx, 3, 1, nil, e.topic, e.dlq); err != nil {
		t.Fatalf("create topics: %v", err)
	}
	t.Cleanup(func() {
		_, _ = adm.DeleteTopics(context.Background(), e.topic, e.dlq)
		cl.Close()
	})

	e.store, err = storage.Open(ctx, getenv("RELAY_TEST_DATABASE_URL", "postgres://relay:relay@localhost:5432/relay?sslmode=disable"), 10, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.store.Close)
	if err := e.store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	e.producer, err = kafka.NewProducer(e.brokers, e.topic, e.dlq)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.producer.Close(context.Background()) })
	return e
}

type consumer struct {
	metrics *telemetry.ConsumerMetrics
	stop    func()
}

func (e *env) startConsumer(t *testing.T, store processor.Store, cfg processor.Config) *consumer {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := telemetry.NewConsumerMetrics(prometheus.NewRegistry())
	c, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: e.brokers, Group: e.topic + "-group", Topic: e.topic, DLQTopic: e.dlq, Workers: 3, MaxPollRecords: 50, SessionTimeout: 10 * time.Second,
	}, log, m)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MaxAttempts == 0 {
		cfg = processor.Config{MaxAttempts: 3, Backoff: processor.Backoff{Base: 5 * time.Millisecond, Max: 20 * time.Millisecond}, OpTimeout: 5 * time.Second}
	}
	proc := processor.New(store, c.Producer(), cfg, log, m)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := c.Run(ctx, proc); err != nil {
			t.Errorf("consumer run: %v", err)
		}
		c.Close()
	}()
	var once sync.Once
	stop := func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return &consumer{metrics: m, stop: stop}
}

func (e *env) newEvent(id string, amount int) event.Event {
	return event.Event{
		ID:         id,
		Type:       e.eventType,
		OccurredAt: time.Now().UTC(),
		Payload:    json.RawMessage(fmt.Sprintf(`{"amount":%d}`, amount)),
	}
}

func (e *env) stats(t *testing.T) storage.TypeStats {
	t.Helper()
	all, err := e.store.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range all {
		if s.EventType == e.eventType {
			return s
		}
	}
	return storage.TypeStats{}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestHTTPToPostgres(t *testing.T) {
	e := setup(t)
	e.startConsumer(t, e.store, processor.Config{})
	srv := httptest.NewServer(api.New(e.producer, e.store, prometheus.NewRegistry(),
		api.Options{MaxBodyBytes: 1 << 16, MaxInFlight: 16, PublishTimeout: 5 * time.Second},
		slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer srv.Close()

	body, _ := json.Marshal(e.newEvent("", 1299))
	resp, err := http.Post(srv.URL+"/v1/events", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	var accepted struct {
		EventID string `json:"event_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&accepted)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || accepted.EventID == "" {
		t.Fatalf("POST returned %d", resp.StatusCode)
	}

	eventually(t, "processed status", func() bool {
		rec, err := e.store.Get(context.Background(), accepted.EventID)
		return err == nil && rec.Status == event.StatusProcessed
	})
	if s := e.stats(t); s.Count != 1 || s.AmountTotal != 1299 {
		t.Fatalf("stats = %+v, want one event worth 1299", s)
	}
}

// TestRedeliveryIsIdempotent covers both duplicate publishes and the crash
// window between the database commit and the offset commit: the latter is
// simulated by completing an event directly and then delivering it.
func TestRedeliveryIsIdempotent(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	dup, crashed := e.newEvent("dup-"+e.topic, 10), e.newEvent("crash-"+e.topic, 5)
	if applied, err := e.store.Complete(ctx, crashed, 5); err != nil || !applied {
		t.Fatalf("pre-complete: %v %v", applied, err)
	}
	for _, ev := range []event.Event{dup, dup, dup, crashed} {
		if err := e.producer.Publish(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	c := e.startConsumer(t, e.store, processor.Config{})

	eventually(t, "all deliveries handled", func() bool {
		return testutil.ToFloat64(c.metrics.Processed.WithLabelValues("processed"))+
			testutil.ToFloat64(c.metrics.Processed.WithLabelValues("duplicate")) == 4
	})
	if s := e.stats(t); s.Count != 2 || s.AmountTotal != 15 {
		t.Fatalf("stats = %+v, want each event applied exactly once", s)
	}
}

func TestFailuresAreDeadLettered(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	poisoned := e.newEvent("retry-"+e.topic, 1)
	if err := e.producer.Publish(ctx, poisoned); err != nil {
		t.Fatal(err)
	}
	raw, _ := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	defer raw.Close()
	if err := raw.ProduceSync(ctx, &kgo.Record{Topic: e.topic, Value: []byte("{not json")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	e.startConsumer(t, e.store, processor.Config{
		MaxAttempts: 3, Backoff: processor.Backoff{Base: time.Millisecond, Max: 5 * time.Millisecond},
		OpTimeout: 5 * time.Second, FaultRate: 1,
	})

	dlq, _ := kgo.NewClient(kgo.SeedBrokers(e.brokers...), kgo.ConsumeTopics(e.dlq))
	defer dlq.Close()
	reasons := map[string]string{}
	pollCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for len(reasons) < 2 && pollCtx.Err() == nil {
		dlq.PollFetches(pollCtx).EachRecord(func(r *kgo.Record) {
			h := kafka.HeaderCarrier{Record: r}
			reasons[h.Get(kafka.HeaderDLQAttempts)] = h.Get(kafka.HeaderDLQReason)
		})
	}
	if reasons["3"] != "injected fault" || reasons["1"] == "" {
		t.Fatalf("dead letters by attempts = %v", reasons)
	}
	eventually(t, "failed status after 3 attempts", func() bool {
		rec, err := e.store.Get(ctx, poisoned.ID)
		return err == nil && rec.Status == event.StatusFailed && rec.Attempts == 3
	})
}

// slowStore makes processing slow enough that a rebalance and a shutdown
// happen while records are in flight.
type slowStore struct{ *storage.Store }

func (s slowStore) Complete(ctx context.Context, ev event.Event, amount int64) (bool, error) {
	time.Sleep(25 * time.Millisecond)
	return s.Store.Complete(ctx, ev, amount)
}

func TestRebalanceAndShutdownLoseNothing(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	const n = 600
	for i := range n {
		if err := e.producer.Publish(ctx, e.newEvent(fmt.Sprintf("rb-%s-%d", e.topic, i), 1)); err != nil {
			t.Fatal(err)
		}
	}
	first := e.startConsumer(t, slowStore{e.store}, processor.Config{})
	eventually(t, "first consumer to make progress", func() bool { return e.stats(t).Count >= 50 })

	second := e.startConsumer(t, slowStore{e.store}, processor.Config{})
	eventually(t, "rebalance onto the second consumer", func() bool {
		return testutil.ToFloat64(second.metrics.Processed.WithLabelValues("processed")) > 0
	})
	if e.stats(t).Count >= n {
		t.Fatal("first consumer finished before the rebalance; increase n")
	}
	first.stop()

	eventually(t, "all events processed", func() bool { return e.stats(t).Count >= n })
	time.Sleep(time.Second)
	if s := e.stats(t); s.Count != n || s.AmountTotal != n {
		t.Fatalf("stats = %+v, want exactly %d", s, n)
	}
}
