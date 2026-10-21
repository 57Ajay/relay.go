// Package kafka: producer
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/57Ajay/relay/internal/event"
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
	tracer   trace.Tracer
}

// NewProducer builds the API's producer. Deadlines hold even for records already in flight;
// a resulting Kafka duplicate is harmless because consumers deduplicate by event ID.
func NewProducer(brokers []string, topic, dlqTopic string) (*Producer, error) {
	cl, err := newClient(brokers, kgo.AllowIdempotentProduceCancellation())
	if err != nil {
		return nil, err
	}
	return newProducer(cl, topic, dlqTopic), nil
}

func newProducer(cl *kgo.Client, topic, dlqTopic string) *Producer {
	return &Producer{cl: cl, topic: topic, dlqTopic: dlqTopic, tracer: otel.Tracer("relay/kafka")}
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

// Publish returns only after all in-sync replicas acknowledged the record, keyed by event ID.
func (p *Producer) Publish(ctx context.Context, ev event.Event) error {
	val, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	rec := &kgo.Record{Topic: p.topic, Key: []byte(ev.ID), Value: val}

	ctx, span := p.tracer.Start(ctx, p.topic+" publish", trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", p.topic),
			attribute.String("event.id", ev.ID),
		))
	defer span.End()
	otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier{Record: rec})

	if err := p.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("produce to %s: %w", p.topic, err)
	}
	span.SetAttributes(attribute.Int("messaging.kafka.partition", int(rec.Partition)),
		attribute.Int64("messaging.kafka.offset", rec.Offset))
	return nil
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

// HeaderCarrier adapts Kafka record headers for OpenTelemetry propagation.
type HeaderCarrier struct{ Record *kgo.Record }

func (c HeaderCarrier) Get(key string) string {
	for _, h := range c.Record.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c HeaderCarrier) Set(key, value string) {
	for i, h := range c.Record.Headers {
		if h.Key == key {
			c.Record.Headers[i].Value = []byte(value)
			return
		}
	}
	c.Record.Headers = append(c.Record.Headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c HeaderCarrier) Keys() []string {
	keys := make([]string, len(c.Record.Headers))
	for i, h := range c.Record.Headers {
		keys[i] = h.Key
	}
	return keys
}
