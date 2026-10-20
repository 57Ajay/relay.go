package kafka

import (
	"context"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Handler returns nil once a record is fully handled; an error leaves it uncommitted.
type Handler interface {
	Handle(ctx context.Context, rec *kgo.Record) error
}

type Consumer struct {
	cl       *kgo.Client
	topic    string
	dlqTopic string
}

func NewConsumer(brokers []string, group, topic, dlqTopic string) (*Consumer, error) {
	cl, err := newClient(brokers,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{cl: cl, topic: topic, dlqTopic: dlqTopic}, nil
}

// Producer shares the consumer's client for dead-letter publishing.
func (c *Consumer) Producer() *Producer {
	return &Producer{cl: c.cl, topic: c.topic, dlqTopic: c.dlqTopic}
}

func (c *Consumer) Close() { c.cl.Close() }

func (c *Consumer) Run(ctx context.Context, h Handler) error {
	for {
		fetches := c.cl.PollRecords(ctx, 1000)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			log.Printf("fetch %s/%d: %v", fe.Topic, fe.Partition, fe.Err)
		}

		var done []*kgo.Record
		for _, rec := range fetches.Records() {
			if !c.handle(ctx, h, rec) {
				break
			}
			done = append(done, rec)
		}
		c.commit(ctx, done)
	}
}

func (c *Consumer) handle(ctx context.Context, h Handler, rec *kgo.Record) bool {
	for {
		err := h.Handle(ctx, rec)
		if err == nil {
			return true
		}
		log.Printf("partition=%d offset=%d not handled, retrying: %v", rec.Partition, rec.Offset, err)
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
		log.Printf("offset commit failed; records will be redelivered: %v", err)
	}
}
