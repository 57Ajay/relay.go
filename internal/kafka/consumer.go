package kafka

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Handler returns nil once a record is fully handled; an error leaves it uncommitted.
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
type Consumer struct {
	cl  *kgo.Client
	cfg ConsumerConfig
	log *slog.Logger
}

func NewConsumer(cfg ConsumerConfig, log *slog.Logger) (*Consumer, error) {
	cl, err := newClient(cfg.Brokers,
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(cfg.SessionTimeout),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{cl: cl, cfg: cfg, log: log}, nil
}

// Producer shares the consumer's client for dead-letter publishing.
func (c *Consumer) Producer() *Producer {
	return &Producer{cl: c.cl, topic: c.cfg.Topic, dlqTopic: c.cfg.DLQTopic}
}

func (c *Consumer) Ping(ctx context.Context) error { return c.cl.Ping(ctx) }

// Close leaves the group so partitions move to other members right away.
func (c *Consumer) Close() { c.cl.CloseAllowingRebalance() }

func (c *Consumer) Run(ctx context.Context, h Handler) error {
	for {
		fetches := c.cl.PollRecords(ctx, c.cfg.MaxPollRecords)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			c.log.ErrorContext(ctx, "fetch failed", "topic", fe.Topic, "partition", fe.Partition, "err", fe.Err)
		}
		c.processBatch(ctx, h, fetches)
	}
}

func (c *Consumer) processBatch(ctx context.Context, h Handler, fetches kgo.Fetches) {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		done []*kgo.Record
		sem  = make(chan struct{}, c.cfg.Workers)
	)
	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		if len(p.Records) == 0 {
			return
		}
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			n := c.processPartition(ctx, h, p.Records)
			if n > 0 {
				mu.Lock()
				done = append(done, p.Records[n-1])
				mu.Unlock()
			}
		})
	})
	wg.Wait()
	c.commit(ctx, done)
}

// processPartition handles records in order and returns how many completed.
func (c *Consumer) processPartition(ctx context.Context, h Handler, recs []*kgo.Record) int {
	for i, rec := range recs {
		if !c.handle(ctx, h, rec) {
			return i
		}
	}
	return len(recs)
}

func (c *Consumer) handle(ctx context.Context, h Handler, rec *kgo.Record) bool {
	for {
		err := h.Handle(ctx, rec)
		if err == nil {
			return true
		}
		c.log.WarnContext(ctx, "record not handled; retrying", "partition", rec.Partition, "offset", rec.Offset, "err", err)
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Second):
		}
	}
}

func (c *Consumer) commit(ctx context.Context, done []*kgo.Record) {
	if len(done) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := c.cl.CommitRecords(ctx, done...); err != nil {
		c.log.ErrorContext(ctx, "offset commit failed; records will be redelivered", "err", err)
	}
}
