package processor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/57Ajay/relay/internal/event"
)

type fakeStore struct {
	mu            sync.Mutex
	processed     map[string]int
	failed        map[string]string
	claimErr      error
	claimFailures int
	completeErr   func(call int) error
	calls         int
}

func (s *fakeStore) Claim(_ context.Context, ev event.Event) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimFailures > 0 {
		s.claimFailures--
		return false, s.claimErr
	}
	return s.processed[ev.ID] == 0, nil
}

func (s *fakeStore) Complete(_ context.Context, ev event.Event, _ int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.completeErr != nil {
		if err := s.completeErr(s.calls); err != nil {
			return false, err
		}
	}
	s.processed[ev.ID]++
	return true, nil
}

func (s *fakeStore) MarkFailed(_ context.Context, ev event.Event, _ int, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed[ev.ID] = reason
	return nil
}

type fakeDLQ struct {
	mu       sync.Mutex
	attempts []int
}

func (d *fakeDLQ) DeadLetter(_ context.Context, _ *kgo.Record, _ string, attempts int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.attempts = append(d.attempts, attempts)
	return nil
}

func setup() (*Processor, *fakeStore, *fakeDLQ) {
	store := &fakeStore{processed: map[string]int{}, failed: map[string]string{}}
	dlq := &fakeDLQ{}
	cfg := Config{MaxAttempts: 3, Backoff: Backoff{Base: time.Millisecond, Max: 4 * time.Millisecond}, OpTimeout: time.Second}
	return New(store, dlq, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))), store, dlq
}

func record(t *testing.T, id string, payload string) *kgo.Record {
	t.Helper()
	value, err := json.Marshal(event.Event{ID: id, Type: "order.created", OccurredAt: time.Now(), Payload: json.RawMessage(payload)})
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "events", Value: value, Timestamp: time.Now()}
}

func TestDuplicateDeliveryAppliesOnce(t *testing.T) {
	p, store, dlq := setup()
	rec := record(t, "evt-1", `{"amount":10}`)
	for range 3 {
		if err := p.Handle(context.Background(), rec); err != nil {
			t.Fatal(err)
		}
	}
	if store.processed["evt-1"] != 1 || len(dlq.attempts) != 0 {
		t.Fatalf("processed=%d dlq=%v", store.processed["evt-1"], dlq.attempts)
	}
}

func TestTransientFailureIsRetried(t *testing.T) {
	p, store, dlq := setup()
	store.completeErr = func(call int) error {
		if call < 3 {
			return &pgconn.PgError{Code: "40001"}
		}
		return nil
	}
	if err := p.Handle(context.Background(), record(t, "evt-2", `{}`)); err != nil {
		t.Fatal(err)
	}
	if store.processed["evt-2"] != 1 || len(dlq.attempts) != 0 {
		t.Fatalf("processed=%d dlq=%v", store.processed["evt-2"], dlq.attempts)
	}
	if store.calls != 3 {
		t.Fatalf("complete calls = %d, want 3 (two retries)", store.calls)
	}
}

func TestRetryExhaustionDeadLetters(t *testing.T) {
	p, store, dlq := setup()
	store.completeErr = func(int) error { return errors.New("downstream timeout") }
	if err := p.Handle(context.Background(), record(t, "evt-3", `{}`)); err != nil {
		t.Fatal(err)
	}
	if len(dlq.attempts) != 1 || dlq.attempts[0] != 3 || store.failed["evt-3"] == "" {
		t.Fatalf("dlq=%v failed=%v", dlq.attempts, store.failed)
	}
}

func TestPermanentFailuresSkipRetries(t *testing.T) {
	p, store, dlq := setup()
	poison := &kgo.Record{Topic: "events", Value: []byte(`not json`)}
	for _, rec := range []*kgo.Record{poison, record(t, "evt-4", `{"amount":-1}`)} {
		if err := p.Handle(context.Background(), rec); err != nil {
			t.Fatal(err)
		}
	}
	if len(dlq.attempts) != 2 || store.calls != 0 || store.failed["evt-4"] == "" {
		t.Fatalf("dlq=%v completeCalls=%d failed=%v", dlq.attempts, store.calls, store.failed)
	}
	for i, a := range dlq.attempts {
		if a != 1 {
			t.Fatalf("dead letter %d attempts = %d, want 1 (no retries)", i, a)
		}
	}
}

func TestUnavailableDatabaseNeverDeadLetters(t *testing.T) {
	p, store, dlq := setup()
	store.claimErr, store.claimFailures = &net.OpError{Op: "dial", Err: errors.New("connection refused")}, math.MaxInt
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := p.Handle(ctx, record(t, "evt-5", `{}`)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the record to stay unhandled until shutdown", err)
	}
	if len(dlq.attempts) != 0 {
		t.Fatalf("event dead-lettered during an outage: %v", dlq.attempts)
	}
}

func TestOutageDoesNotConsumeRetryBudget(t *testing.T) {
	p, store, dlq := setup()
	store.claimErr, store.claimFailures = &pgconn.PgError{Code: "57P03"}, 10
	store.completeErr = func(call int) error {
		if call == 1 {
			return &pgconn.PgError{Code: "40P01"}
		}
		return nil
	}
	if err := p.Handle(context.Background(), record(t, "evt-6", `{}`)); err != nil {
		t.Fatal(err)
	}
	if store.processed["evt-6"] != 1 || len(dlq.attempts) != 0 {
		t.Fatalf("processed=%d dlq=%v", store.processed["evt-6"], dlq.attempts)
	}
}

func TestClassify(t *testing.T) {
	tests := map[string]struct {
		err  error
		want Class
	}{
		"invalid event":      {&event.ValidationError{Problems: []string{"x"}}, Permanent},
		"check violation":    {&pgconn.PgError{Code: "23514"}, Permanent},
		"serialization":      {&pgconn.PgError{Code: "40001"}, Transient},
		"admin shutdown":     {&pgconn.PgError{Code: "57P01"}, Unavailable},
		"read-only replica":  {&pgconn.PgError{Code: "25006"}, Unavailable},
		"undefined table":    {&pgconn.PgError{Code: "42P01"}, Unavailable},
		"connection refused": {&net.OpError{Op: "dial", Err: errors.New("refused")}, Unavailable},
		"deadline":           {context.DeadlineExceeded, Unavailable},
		"unknown":            {errInjected, Transient},
	}
	for name, tt := range tests {
		if got := Classify(tt.err); got != tt.want {
			t.Errorf("%s: got %s, want %s", name, got, tt.want)
		}
	}
}

func TestBackoffDelayIsBoundedAndJittered(t *testing.T) {
	b := Backoff{Base: 100 * time.Millisecond, Max: 2 * time.Second}
	for attempt, ceiling := range map[int]time.Duration{1: 100 * time.Millisecond, 3: 400 * time.Millisecond, 10: 2 * time.Second, 500: 2 * time.Second} {
		seen := map[time.Duration]bool{}
		for range 100 {
			d := b.Delay(attempt)
			if d < ceiling/2 || d > ceiling {
				t.Fatalf("attempt %d: delay %s outside [%s, %s]", attempt, d, ceiling/2, ceiling)
			}
			seen[d] = true
		}
		if len(seen) < 2 {
			t.Fatalf("attempt %d: delays are not jittered", attempt)
		}
	}
}

func TestPanicIsContainedAndDeadLettered(t *testing.T) {
	p, store, dlq := setup()
	store.completeErr = func(int) error { panic("boom") }
	if err := p.Handle(context.Background(), record(t, "evt-7", `{}`)); err != nil {
		t.Fatal(err)
	}
	if len(dlq.attempts) != 1 || store.failed["evt-7"] == "" {
		t.Fatalf("dlq=%v failed=%v", dlq.attempts, store.failed)
	}
}
