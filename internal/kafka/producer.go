// Package kafka: producer
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/57Ajay/relay/internal/event"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	HeaderDLQReason       = "x-dlq-reason"
	HeaderDLQAttempts     = "x-dlq-attempts"
	HeaderOriginTopic     = "x-origin-topic"
	HeaderOriginPartition = "x-origin-partition"
	HeaderOriginOffset    = "x-origin-offset"
)

type Producer struct {
	cl       *kgo.Client
	topic    string
	dlqTopic string
}

func NewProducer(brokers []string, topic, dlqTopic string) (*Producer, error) {
	cl, err := newClient(brokers)
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl, topic: topic, dlqTopic: dlqTopic}, nil
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

// DeadLetter copies the original key, value and headers to the DLQ and adds why and where from.
func (p *Producer) DeadLetter(ctx context.Context, rec *kgo.Record, reason string, attempts int) error {
	headers := append(make([]kgo.RecordHeader, 0, len(rec.Headers)+5), rec.Headers...)
	headers = append(headers,
		kgo.RecordHeader{Key: HeaderDLQReason, Value: []byte(reason)},
		kgo.RecordHeader{Key: HeaderDLQAttempts, Value: []byte(strconv.Itoa(attempts))},
		kgo.RecordHeader{Key: HeaderOriginTopic, Value: []byte(rec.Topic)},
		kgo.RecordHeader{Key: HeaderOriginPartition, Value: []byte(strconv.Itoa(int(rec.Partition)))},
		kgo.RecordHeader{Key: HeaderOriginOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
	)
	dlq := &kgo.Record{Topic: p.dlqTopic, Key: rec.Key, Value: rec.Value, Headers: headers}
	if err := p.cl.ProduceSync(ctx, dlq).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", p.dlqTopic, err)
	}
	return nil
}

func (p *Producer) Ping(ctx context.Context) error { return p.cl.Ping(ctx) }

func (p *Producer) Close(ctx context.Context) {
	_ = p.cl.Flush(ctx)
	p.cl.Close()
}
