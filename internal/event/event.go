// Package event: the core event types and utils
package event

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrInvalid  = errors.New("invalid event")
	idPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	typePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)
	errAmount   = &ValidationError{Problems: []string{"payload.amount must be an integer between 0 and 10^12"}}
)

const (
	MaxTypeLength = 64
	MaxClockSkew  = 5 * time.Minute
	MaxAmount     = 1_000_000_000_000
)

type Status string

type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "invalid event: " + strings.Join(e.Problems, "; ")
}

func (e *ValidationError) Unwrap() error { return ErrInvalid }

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusProcessed  Status = "processed"
	StatusFailed     Status = "failed"
)

type Event struct {
	ID         string          `json:"event_id"`
	Type       string          `json:"event_type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
}

func Decode(r io.Reader) (Event, error) {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()

	var ev Event
	if err := dec.Decode(&ev); err != nil {
		return Event{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}

	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Event{}, fmt.Errorf("%w: unexpected data after event object", ErrInvalid)
	}

	return ev, nil
}

func (e *Event) Amount() (int64, error) {
	var p struct {
		Amount *json.Number `json:"amount"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return 0, errAmount
	}
	if p.Amount == nil {
		return 0, nil
	}
	n, err := p.Amount.Int64()
	if err != nil || n < 0 || n > MaxAmount {
		return 0, errAmount
	}
	return n, nil
}

func (e *Event) Validate(now time.Time) error {
	var problems []string
	switch {
	case e.ID == "":
		problems = append(problems, "event_id is required")
	case !idPattern.MatchString(e.ID):
		problems = append(problems, "event_id must be 1-128 characters from [A-Za-z0-9._:-]")
	}
	switch {
	case e.Type == "":
		problems = append(problems, "event_type is required")
	case len(e.Type) > MaxTypeLength || !typePattern.MatchString(e.Type):
		problems = append(problems, "event_type must be lowercase and dot-separated, e.g. order.created")
	}
	switch {
	case e.OccurredAt.IsZero():
		problems = append(problems, "occurred_at is required (RFC 3339)")
	case e.OccurredAt.After(now.Add(MaxClockSkew)):
		problems = append(problems, "occurred_at is too far in the future")
	}
	switch {
	case !bytes.HasPrefix(bytes.TrimSpace(e.Payload), []byte("{")):
		problems = append(problems, "payload must be a JSON object")
	case !utf8.Valid(e.Payload) || !jsonbSafe(e.Payload):
		problems = append(problems, "payload must be valid UTF-8 without \\u0000 or unpaired surrogate escapes")
	default:
		if _, err := e.Amount(); err != nil {
			problems = append(problems, errAmount.Problems...)
		}
	}
	if len(problems) > 0 {
		return &ValidationError{Problems: problems}
	}
	return nil
}

// jsonbSafe rejects \u0000 and unpaired surrogate escapes, which Go accepts but jsonb does not.
func jsonbSafe(b []byte) bool {
	high := false
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' || i+1 == len(b) || b[i+1] != 'u' {
			if high {
				return false
			}
			if b[i] == '\\' {
				i++
			}
			continue
		}
		if i+6 > len(b) {
			return false
		}
		r, err := strconv.ParseUint(string(b[i+2:i+6]), 16, 16)
		if err != nil {
			return false
		}
		i += 5
		switch {
		case r == 0, r >= 0xDC00 && r < 0xE000 && !high, r >= 0xD800 && r < 0xDC00 && high:
			return false
		case r >= 0xD800 && r < 0xDC00:
			high = true
		default:
			high = false
		}
	}
	return !high
}

func NewID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return "evt_" + hex.EncodeToString(b)
}
