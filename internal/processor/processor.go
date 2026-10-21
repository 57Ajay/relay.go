package processor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/57Ajay/relay/internal/event"
	"github.com/57Ajay/relay/internal/kafka"
	"github.com/57Ajay/relay/internal/telemetry"
)

const (
	outcomeProcessed    = "processed"
	outcomeDuplicate    = "duplicate"
	outcomeDeadLettered = "dead_lettered"
	maxReasonLength     = 512
)

var errInjected = errors.New("injected fault")

type Store interface {
	Claim(ctx context.Context, ev event.Event) (bool, error)
	Complete(ctx context.Context, ev event.Event, amount int64) (bool, error)
	MarkFailed(ctx context.Context, ev event.Event, attempts int, reason string) error
}

type DeadLetterer interface {
	DeadLetter(ctx context.Context, rec *kgo.Record, reason string, attempts int) error
}

type Config struct {
	MaxAttempts int
	Backoff     Backoff
	OpTimeout   time.Duration
	FaultRate   float64
}

type Processor struct {
	store   Store
	dlq     DeadLetterer
	cfg     Config
	log     *slog.Logger
	metrics *telemetry.ConsumerMetrics
	tracer  trace.Tracer
}

func New(store Store, dlq DeadLetterer, cfg Config, log *slog.Logger, m *telemetry.ConsumerMetrics) *Processor {
	return &Processor{store: store, dlq: dlq, cfg: cfg, log: log, metrics: m, tracer: otel.Tracer("relay/processor")}
}

// Handle implements kafka.Handler; it only returns an error when ctx is cancelled mid-retry.
func (p *Processor) Handle(ctx context.Context, rec *kgo.Record) error {
	start := time.Now()
	ctx = otel.GetTextMapPropagator().Extract(ctx, kafka.HeaderCarrier{Record: rec})
	ctx, span := p.tracer.Start(ctx, rec.Topic+" process", trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.Int("messaging.kafka.partition", int(rec.Partition)),
			attribute.Int64("messaging.kafka.offset", rec.Offset),
		))
	defer span.End()

	outcome, err := p.handle(ctx, rec)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	span.SetAttributes(attribute.String("relay.outcome", outcome))
	p.metrics.Duration.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
	return nil
}

func (p *Processor) handle(ctx context.Context, rec *kgo.Record) (string, error) {
	ev, err := event.Decode(bytes.NewReader(rec.Value))
	if err == nil {
		err = ev.Validate(time.Now())
	}
	if err != nil {
		var known *event.Event
		if ev.ID != "" {
			known = &ev
		}
		return p.deadLetter(ctx, rec, known, 1, err)
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("event.id", ev.ID), attribute.String("event.type", ev.Type))
	log := p.log.With("event_id", ev.ID, "event_type", ev.Type)
	amount, _ := ev.Amount()

	attempts := 0
	for try := 1; ; try++ {
		outcome, err := p.attempt(ctx, ev, amount)
		if err == nil {
			p.metrics.Processed.WithLabelValues(outcome).Inc()
			if outcome == outcomeProcessed {
				p.metrics.EndToEnd.Observe(time.Since(rec.Timestamp).Seconds())
			}
			return outcome, nil
		}
		class := Classify(err)
		if class != Unavailable {
			attempts++
		}
		if class == Permanent || (class == Transient && attempts >= p.cfg.MaxAttempts) {
			return p.deadLetter(ctx, rec, &ev, attempts, err)
		}
		delay := p.cfg.Backoff.Delay(try)
		p.metrics.Retried.WithLabelValues(class.String()).Inc()
		log.WarnContext(ctx, "attempt failed; retrying", "try", try, "class", class.String(), "retry_in", delay, "err", err)
		if err := sleep(ctx, delay); err != nil {
			return "", fmt.Errorf("retry abandoned after try %d: %w", try, err)
		}
	}
}

func (p *Processor) attempt(ctx context.Context, ev event.Event, amount int64) (outcome string, err error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.OpTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			err = permanentError{fmt.Errorf("panic while processing: %v", r)}
		}
	}()

	claimed, err := p.store.Claim(ctx, ev)
	if err != nil {
		return "", fmt.Errorf("claim: %w", err)
	}
	if !claimed {
		return outcomeDuplicate, nil
	}
	if p.cfg.FaultRate > 0 && rand.Float64() < p.cfg.FaultRate {
		return "", errInjected
	}
	applied, err := p.store.Complete(ctx, ev, amount)
	if err != nil {
		return "", fmt.Errorf("complete: %w", err)
	}
	if !applied {
		return outcomeDuplicate, nil
	}
	return outcomeProcessed, nil
}

// deadLetter reports the record handled only after Kafka acknowledged the dead letter.
func (p *Processor) deadLetter(ctx context.Context, rec *kgo.Record, ev *event.Event, attempts int, cause error) (string, error) {
	reason := cause.Error()
	if len(reason) > maxReasonLength {
		reason = strings.ToValidUTF8(reason[:maxReasonLength], "")
	}
	for try := 1; ; try++ {
		opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.OpTimeout)
		err := p.dlq.DeadLetter(opCtx, rec, reason, attempts)
		cancel()
		if err == nil {
			break
		}
		p.log.ErrorContext(ctx, "dead-letter publish failed; retrying", "offset", rec.Offset, "err", err)
		if err := sleep(ctx, p.cfg.Backoff.Delay(try)); err != nil {
			return "", fmt.Errorf("dead-letter abandoned: %w", err)
		}
	}

	label := "exhausted"
	if Classify(cause) == Permanent {
		label = "permanent"
	}
	p.metrics.Failed.WithLabelValues(label).Inc()
	span := trace.SpanFromContext(ctx)
	span.RecordError(cause)
	span.SetStatus(codes.Error, "dead-lettered: "+reason)
	log := p.log.With("partition", rec.Partition, "offset", rec.Offset, "attempts", attempts, "reason", reason)
	if ev == nil {
		log.ErrorContext(ctx, "undecodable record dead-lettered")
		return outcomeDeadLettered, nil
	}
	log.ErrorContext(ctx, "event dead-lettered", "event_id", ev.ID)

	opCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), p.cfg.OpTimeout)
	defer cancel()
	if err := p.store.MarkFailed(opCtx, *ev, attempts, reason); err != nil {
		log.WarnContext(ctx, "could not record failed status", "event_id", ev.ID, "err", err)
	}
	return outcomeDeadLettered, nil
}
