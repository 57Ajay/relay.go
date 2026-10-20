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

	"github.com/57Ajay/relay/internal/event"
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
	store Store
	dlq   DeadLetterer
	cfg   Config
	log   *slog.Logger
}

func New(store Store, dlq DeadLetterer, cfg Config, log *slog.Logger) *Processor {
	return &Processor{store: store, dlq: dlq, cfg: cfg, log: log}
}

// Handle implements kafka.Handler; it only returns an error when ctx is cancelled mid-retry.
func (p *Processor) Handle(ctx context.Context, rec *kgo.Record) error {
	outcome, err := p.handle(ctx, rec)
	if err != nil {
		return err
	}
	p.log.InfoContext(ctx, "record handled", "partition", rec.Partition, "offset", rec.Offset, "outcome", outcome)
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
	log := p.log.With("event_id", ev.ID, "event_type", ev.Type)
	amount, _ := ev.Amount()

	attempts := 0
	for try := 1; ; try++ {
		outcome, err := p.attempt(ctx, ev, amount)
		if err == nil {
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
