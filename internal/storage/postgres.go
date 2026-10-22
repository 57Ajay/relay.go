// Package storage -> all the psql stuff
package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/57Ajay/relay/internal/event"
	"github.com/57Ajay/relay/migrations"
)

var ErrNotFound = errors.New("event not found")

const migrationLockID = 72_04_2026

type Store struct {
	pool   *pgxpool.Pool
	tracer trace.Tracer
}

type Record struct {
	ID          string       `json:"event_id"`
	Type        string       `json:"event_type"`
	Status      event.Status `json:"status"`
	Attempts    int          `json:"attempts"`
	LastError   string       `json:"last_error,omitempty"`
	OccurredAt  time.Time    `json:"occurred_at"`
	ProcessedAt *time.Time   `json:"processed_at,omitempty"`
	UpdatedAt   time.Time    `json:"updated_at"`
}

type TypeStats struct {
	EventType   string    `json:"event_type"`
	Count       int64     `json:"event_count"`
	AmountTotal int64     `json:"amount_total"`
	LastEventAt time.Time `json:"last_event_at"`
}

// Open creates a pool. asyncCommit skips waiting for the WAL flush; only for writes safe to lose.
func Open(ctx context.Context, url string, maxConns int32, asyncCommit bool) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = maxConns
	if asyncCommit {
		cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "off"
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	return &Store{pool: pool, tracer: otel.Tracer("relay/storage")}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// CREATE TABLE IF NOT EXISTS is not safe under concurrency; replicas starting together migrate one at a time.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	files, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return fmt.Errorf("glob migrations: %w", err)
	}
	for _, file := range files {
		var applied bool
		if err := tx.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", file).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %s: %w", file, err)
		}
		if applied {
			continue
		}

		query, err := migrations.FS.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", file, err)
		}
		if _, err := tx.Exec(ctx, string(query)); err != nil {
			return fmt.Errorf("execute migration %s: %w", file, err)
		}
		if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations (version) VALUES ($1)", file); err != nil {
			return fmt.Errorf("record migration %s: %w", file, err)
		}
	}
	return tx.Commit(ctx)
}

const markPendingSQL = `
INSERT INTO processed_events (event_id, event_type, payload, occurred_at, status)
VALUES ($1, $2, $3, $4, 'pending')
ON CONFLICT (event_id) DO NOTHING`

func (s *Store) MarkPending(ctx context.Context, ev event.Event) (err error) {
	ctx, span := s.span(ctx, "mark_pending")
	defer func() { finish(span, err) }()
	_, err = s.pool.Exec(ctx, markPendingSQL, ev.ID, ev.Type, ev.Payload, ev.OccurredAt)
	return err
}

const claimSQL = `
INSERT INTO processed_events (event_id, event_type, payload, occurred_at, status, attempts)
VALUES ($1, $2, $3, $4, 'processing', 1)
ON CONFLICT (event_id) DO UPDATE
SET status = 'processing', attempts = processed_events.attempts + 1, updated_at = now(),
    event_type = EXCLUDED.event_type, payload = EXCLUDED.payload, occurred_at = EXCLUDED.occurred_at
WHERE processed_events.status <> 'processed'
RETURNING attempts`

// Claim returns false if the event was already processed.
func (s *Store) Claim(ctx context.Context, ev event.Event) (claimed bool, err error) {
	ctx, span := s.span(ctx, "claim")
	defer func() { finish(span, err) }()
	var attempts int
	err = s.pool.QueryRow(ctx, claimSQL, ev.ID, ev.Type, ev.Payload, ev.OccurredAt).Scan(&attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

const completeSQL = `
INSERT INTO processed_events (event_id, event_type, payload, occurred_at, status, attempts, processed_at)
VALUES ($1, $2, $3, $4, 'processed', 1, now())
ON CONFLICT (event_id) DO UPDATE
SET status = 'processed', processed_at = now(), last_error = NULL, updated_at = now(),
    event_type = EXCLUDED.event_type, payload = EXCLUDED.payload, occurred_at = EXCLUDED.occurred_at
WHERE processed_events.status <> 'processed'`

const applyStatsSQL = `
INSERT INTO event_stats (event_type, event_count, amount_total, last_event_at)
VALUES ($1, 1, $2, $3)
ON CONFLICT (event_type) DO UPDATE
SET event_count = event_stats.event_count + 1,
    amount_total = event_stats.amount_total + EXCLUDED.amount_total,
    last_event_at = GREATEST(event_stats.last_event_at, EXCLUDED.last_event_at),
    updated_at = now()`

// Complete returns false if another delivery already completed the event.
func (s *Store) Complete(ctx context.Context, ev event.Event, amount int64) (applied bool, err error) {
	ctx, span := s.span(ctx, "complete")
	defer func() { finish(span, err) }()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, completeSQL, ev.ID, ev.Type, ev.Payload, ev.OccurredAt)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if _, err := tx.Exec(ctx, applyStatsSQL, ev.Type, amount, ev.OccurredAt); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

const markFailedSQL = `
INSERT INTO processed_events (event_id, event_type, payload, occurred_at, status, attempts, last_error)
VALUES ($1, $2, $3, $4, 'failed', $5, $6)
ON CONFLICT (event_id) DO UPDATE
SET status = 'failed', last_error = EXCLUDED.last_error, updated_at = now(),
    attempts = GREATEST(processed_events.attempts, EXCLUDED.attempts)
WHERE processed_events.status <> 'processed'`

func (s *Store) MarkFailed(ctx context.Context, ev event.Event, attempts int, reason string) (err error) {
	ctx, span := s.span(ctx, "mark_failed")
	defer func() { finish(span, err) }()
	_, err = s.pool.Exec(ctx, markFailedSQL, ev.ID, ev.Type, ev.Payload, ev.OccurredAt, attempts, reason)
	return err
}

const getSQL = `
SELECT event_id, event_type, status, attempts, COALESCE(last_error, ''), occurred_at, processed_at, updated_at
FROM processed_events WHERE event_id = $1`

func (s *Store) Get(ctx context.Context, id string) (rec Record, err error) {
	ctx, span := s.span(ctx, "get")
	defer func() { finish(span, err) }()
	err = s.pool.QueryRow(ctx, getSQL, id).Scan(&rec.ID, &rec.Type, &rec.Status, &rec.Attempts,
		&rec.LastError, &rec.OccurredAt, &rec.ProcessedAt, &rec.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	return rec, err
}

func (s *Store) Stats(ctx context.Context) ([]TypeStats, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT event_type, event_count, amount_total, last_event_at FROM event_stats ORDER BY event_type")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (TypeStats, error) {
		var t TypeStats
		err := row.Scan(&t.EventType, &t.Count, &t.AmountTotal, &t.LastEventAt)
		return t, err
	})
}

func (s *Store) span(ctx context.Context, op string) (context.Context, trace.Span) {
	return s.tracer.Start(ctx, "postgres "+op, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("db.system", "postgresql"), attribute.String("db.operation", op)))
}

func finish(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
