// Package processor
package processor

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/57Ajay/relay/internal/event"
)

type Class int

const (
	Transient Class = iota
	Permanent
	Unavailable
)

func (c Class) String() string {
	switch c {
	case Permanent:
		return "permanent"
	case Unavailable:
		return "unavailable"
	default:
		return "transient"
	}
}

type permanentError struct{ error }

func (e permanentError) Unwrap() error { return e.error }

func Classify(err error) Class {
	var (
		perm    permanentError
		pgErr   *pgconn.PgError
		connErr *pgconn.ConnectError
		netErr  net.Error
	)
	switch {
	case errors.As(err, &perm), errors.Is(err, event.ErrInvalid):
		return Permanent
	case errors.As(err, &pgErr):
		return classifySQLState(pgErr.Code)
	case errors.As(err, &connErr), errors.As(err, &netErr),
		errors.Is(err, context.DeadlineExceeded), pgconn.Timeout(err):
		return Unavailable
	}
	return Transient
}

func classifySQLState(code string) Class {
	switch {
	case strings.HasPrefix(code, "08"), strings.HasPrefix(code, "28"), strings.HasPrefix(code, "3D"),
		strings.HasPrefix(code, "42"), strings.HasPrefix(code, "53"), strings.HasPrefix(code, "57P"),
		strings.HasPrefix(code, "58"), code == "25006":
		return Unavailable
	case strings.HasPrefix(code, "22"), strings.HasPrefix(code, "23"):
		return Permanent
	}
	return Transient
}

type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

// Delay is exponential with equal jitter: half the capped step is fixed, half random.
func (b Backoff) Delay(attempt int) time.Duration {
	step := b.Max
	if attempt < 1 {
		attempt = 1
	}
	if attempt <= 30 {
		if d := b.Base << (attempt - 1); d > 0 && d < b.Max {
			step = d
		}
	}
	half := step / 2
	return half + rand.N(step-half+1)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
