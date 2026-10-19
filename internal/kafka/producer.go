// Package kafka: producer
package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/57Ajay/relay/internal/event"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	cl    *kgo.Client
	topic string
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	cl, err := newClient(brokers)
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl, topic: topic}, nil
}

func newClient(brokers []string, opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return cl, nil
}

func (p *Producer) Publish(ctx context.Context, ev event.Event) error {
	val, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	record := kgo.Record{
		Topic: p.topic,
		Key:   []byte(ev.ID),
		Value: val,
	}

	return p.cl.ProduceSync(ctx, &record).FirstErr()
}

func (p *Producer) Close(ctx context.Context) {
	p.cl.Flush(ctx)
	p.cl.Close()
}
