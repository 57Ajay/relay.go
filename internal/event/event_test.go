package event

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

func valid() Event {
	return Event{
		ID:         "evt_123",
		Type:       "order.created",
		OccurredAt: now.Add(-time.Minute),
		Payload:    json.RawMessage(`{"order_id":"o-1","amount":1299}`),
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Event)
		want   string
	}{
		{"valid", func(*Event) {}, ""},
		{"missing id", func(e *Event) { e.ID = "" }, "event_id is required"},
		{"id with spaces", func(e *Event) { e.ID = "evt 1" }, "event_id must be"},
		{"id too long", func(e *Event) { e.ID = strings.Repeat("a", 129) }, "event_id must be"},
		{"missing type", func(e *Event) { e.Type = "" }, "event_type is required"},
		{"type without namespace", func(e *Event) { e.Type = "created" }, "event_type must be"},
		{"uppercase type", func(e *Event) { e.Type = "Order.Created" }, "event_type must be"},
		{"missing timestamp", func(e *Event) { e.OccurredAt = time.Time{} }, "occurred_at is required"},
		{"future timestamp", func(e *Event) { e.OccurredAt = now.Add(time.Hour) }, "too far in the future"},
		{"array payload", func(e *Event) { e.Payload = json.RawMessage(`[1]`) }, "payload must be a JSON object"},
		{"missing payload", func(e *Event) { e.Payload = nil }, "payload must be a JSON object"},
		{"negative amount", func(e *Event) { e.Payload = json.RawMessage(`{"amount":-1}`) }, "payload.amount"},
		{"nul escape", func(e *Event) { e.Payload = json.RawMessage(`{"s":"a\u0000"}`) }, "unpaired surrogate"},
		{"lone surrogate", func(e *Event) { e.Payload = json.RawMessage(`{"s":"\ud800x"}`) }, "unpaired surrogate"},
		{"surrogate pair", func(e *Event) { e.Payload = json.RawMessage(`{"s":"\ud83d\ude00"}`) }, ""},
		{"escaped backslash", func(e *Event) { e.Payload = json.RawMessage(`{"s":"\\u0000"}`) }, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ev := valid()
			tt.mutate(&ev)
			err := ev.Validate(now)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) || !errors.Is(err, ErrInvalid) {
				t.Fatalf("got %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestDecodeIsStrict(t *testing.T) {
	ok := `{"event_id":"e1","event_type":"a.b","occurred_at":"2026-10-17T10:00:00Z","payload":{}}`
	if _, err := Decode(strings.NewReader(ok)); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
	for name, body := range map[string]string{
		"unknown field": `{"event_id":"e1","extra":true}`,
		"trailing data": ok + `{}`,
		"bad timestamp": `{"occurred_at":"yesterday"}`,
		"not json":      `event`,
	} {
		if _, err := Decode(strings.NewReader(body)); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %v, want ErrInvalid", name, err)
		}
	}
}

func TestAmount(t *testing.T) {
	for payload, want := range map[string]int64{`{"amount":1299}`: 1299, `{"order_id":"x"}`: 0} {
		ev := Event{Payload: json.RawMessage(payload)}
		if got, err := ev.Amount(); err != nil || got != want {
			t.Errorf("%s: got %d, %v", payload, got, err)
		}
	}
	for _, payload := range []string{`{"amount":-5}`, `{"amount":12.5}`, `{"amount":"ten"}`, `{"amount":1000000000001}`} {
		ev := Event{Payload: json.RawMessage(payload)}
		if _, err := ev.Amount(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %v, want ErrInvalid", payload, err)
		}
	}
}

func TestNewID(t *testing.T) {
	a, b := NewID(), NewID()
	if a == b {
		t.Fatalf("NewID returned the same id twice: %s", a)
	}
	ev := valid()
	ev.ID = a
	if len(a) != len("evt_")+32 || ev.Validate(now) != nil {
		t.Fatalf("NewID produced an invalid id: %q", a)
	}
}
