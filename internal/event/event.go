// Package event: the core event types and utils
package event

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
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
		return Event{}, fmt.Errorf("invalid event, %w", err)
	}

	return ev, nil
}

// Amount returns payload.amount, or 0 when it is absent.
func (e *Event) Amount() (int64, error) {
	var p struct {
		Amount int64 `json:"amount"`
	}
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return 0, fmt.Errorf("invalid amount: %w", err)
	}
	return p.Amount, nil
}
