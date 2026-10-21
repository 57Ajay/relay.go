package kafka

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/57Ajay/relay/internal/telemetry"
)

// Handler returns nil once a record is fully handled; an error leaves it uncommitted for redelivery.
type Handler interface {
	Handle(ctx context.Context, rec *kgo.Record) error
}

type ConsumerConfig struct {
	Brokers        []string
	Group          string
	Topic          string
	DLQTopic       string
	Workers        int
	MaxPollRecords int
	SessionTimeout time.Duration
}

// Consumer processes the partitions of a batch concurrently, records within a partition in order.
// Rebalances are blocked while a batch is in flight, so commits never target partitions it no longer owns.
type Consumer struct {
	cl      *kgo.Client
	cfg     ConsumerConfig
	log     *slog.Logger
	metrics *telemetry.ConsumerMetrics

	mu               sync.Mutex
	cancelBatch      context.CancelFunc
	rebalancePending bool
}

func NewConsumer(cfg ConsumerConfig, log *slog.Logger, m *telemetry.ConsumerMetrics) (*Consumer, error) {
	c := &Consumer{cfg: cfg, log: log, metrics: m}
	cl, err := newClient(cfg.Brokers,
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.OnPartitionsCallbackBlocked(c.onRebalanceBlocked),
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onRevoked),
	)
	if err != nil {
		return nil, err
	}
	c.cl = cl
	return c, nil
}

// Producer shares the consumer's client for dead-letter publishing.
func (c *Consumer) Producer() *Producer {
	return &Producer{cl: c.cl, topic: c.cfg.Topic, dlqTopic: c.cfg.DLQTopic}
}

func (c *Consumer) Ping(ctx context.Context) error { return c.cl.Ping(ctx) }

// Close leaves the group so partitions move to other members right away.
func (c *Consumer) Close() { c.cl.CloseAllowingRebalance() }

// Run consumes until ctx is cancelled; in-flight records finish and completed offsets are committed.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	for {
		fetches := c.cl.PollRecords(ctx, c.cfg.MaxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			c.log.ErrorContext(ctx, "fetch failed", "topic", fe.Topic, "partition", fe.Partition, "err", fe.Err)
		}
		if fetches.NumRecords() == 0 {
			c.cl.AllowRebalance()
			if len(fetches.Errors()) > 0 {
				sleep(ctx, time.Second)
			}
			continue
		}
		c.processBatch(ctx, h, fetches)
		c.cl.AllowRebalance()
	}
}

func (c *Consumer) processBatch(ctx context.Context, h Handler, fetches kgo.Fetches) {
	ctx, cancel := c.beginBatch(ctx)
	defer c.endBatch(cancel)

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		done   []*kgo.Record
		rewind = map[string]map[int32]kgo.EpochOffset{}
		sem    = make(chan struct{}, c.cfg.Workers)
	)
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if len(p.Records) == 0 {
			return
		}
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			c.metrics.ActiveWorkers.Inc()
			n := c.processPartition(ctx, h, p.Records)
			c.metrics.ActiveWorkers.Dec()

			mu.Lock()
			defer mu.Unlock()
			if n > 0 {
				done = append(done, p.Records[n-1])
			}
			if n < len(p.Records) {
				next := p.Records[n]
				if rewind[next.Topic] == nil {
					rewind[next.Topic] = map[int32]kgo.EpochOffset{}
				}
				rewind[next.Topic][next.Partition] = kgo.EpochOffset{Epoch: -1, Offset: next.Offset}
			}
		})
	})
	wg.Wait()

	if len(done) > 0 {
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := c.cl.CommitRecords(commitCtx, done...); err != nil {
			c.metrics.CommitErrors.Inc()
			c.log.ErrorContext(ctx, "offset commit failed; records will be redelivered", "err", err)
		}
	}
	// Fetched but unhandled records must be fetched again, or a later commit would skip them.
	if len(rewind) > 0 {
		c.cl.SetOffsets(rewind)
	}
}

// processPartition handles records in order and returns how many completed.
func (c *Consumer) processPartition(ctx context.Context, h Handler, recs []*kgo.Record) int {
	for i, rec := range recs {
		if ctx.Err() != nil {
			return i
		}
		if err := h.Handle(ctx, rec); err != nil {
			c.log.WarnContext(ctx, "record left uncommitted", "partition", rec.Partition, "offset", rec.Offset, "err", err)
			return i
		}
	}
	return len(recs)
}

func (c *Consumer) beginBatch(ctx context.Context) (context.Context, context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ctx, cancel := context.WithCancel(ctx)
	if c.rebalancePending {
		cancel()
	}
	c.cancelBatch = cancel
	return ctx, cancel
}

func (c *Consumer) endBatch(cancel context.CancelFunc) {
	cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelBatch = nil
	c.rebalancePending = false
}

// onRebalanceBlocked fires when the group wants to rebalance mid-batch; workers stop after their current record.
func (c *Consumer) onRebalanceBlocked(context.Context, *kgo.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rebalancePending = true
	if c.cancelBatch != nil {
		c.log.Info("rebalance pending; yielding after in-flight records")
		c.cancelBatch()
	}
}

func (c *Consumer) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	if len(assigned) > 0 {
		c.log.Info("partitions assigned", "partitions", assigned)
	}
}

func (c *Consumer) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	if len(revoked) > 0 {
		c.log.Info("partitions revoked", "partitions", revoked)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
