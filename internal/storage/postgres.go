// Package storage -> all the psql stuff
package storage

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/57Ajay/relay/internal/event"
	"github.com/57Ajay/relay/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

type TypeStats struct {
	EventType   string `json:"event_type"`
	Count       int64  `json:"event_count"`
	AmountTotal int64  `json:"amount_total"`
}

func Open(ctx context.Context, url string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Migrate applies every embedded migration not yet recorded in
// schema_migrations, all inside one transaction.
func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

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

const saveEventSQL = `
INSERT INTO processed_events (event_id, event_type, payload, occurred_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (event_id) DO NOTHING`

const applyStatsSQL = `
INSERT INTO event_stats (event_type, event_count, amount_total)
VALUES ($1, 1, $2)
ON CONFLICT (event_type) DO UPDATE
SET event_count = event_stats.event_count + 1,
    amount_total = event_stats.amount_total + EXCLUDED.amount_total`

// Complete -> records the event and applies its side effect
// in one transaction; returns false if already applied
func (s *Store) Complete(ctx context.Context, ev event.Event, amount int64) (applied bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, saveEventSQL, ev.ID, ev.Type, ev.Payload, ev.OccurredAt)

	if err != nil {
		return false, err
	}

	if tag.RowsAffected() == 0 {
		return false, nil
	}

	_, err = tx.Exec(ctx, applyStatsSQL, ev.Type, amount)
	if err != nil {
		return false, err
	}

	return true, tx.Commit(ctx)
}

func (s *Store) Stats(ctx context.Context) ([]TypeStats, error) {
	rows, err := s.pool.Query(ctx,
		"SELECT event_type, event_count, amount_total FROM event_stats ORDER BY event_type")
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (TypeStats, error) {
		var t TypeStats
		err := row.Scan(&t.EventType, &t.Count, &t.AmountTotal)
		return t, err
	})
}
