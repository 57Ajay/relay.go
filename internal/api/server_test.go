package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/57Ajay/relay/internal/event"
	"github.com/57Ajay/relay/internal/storage"
)

type fakePublisher struct {
	err       error
	published []event.Event
}

func (p *fakePublisher) Publish(_ context.Context, ev event.Event) error {
	if p.err != nil {
		return p.err
	}
	p.published = append(p.published, ev)
	return nil
}

func (p *fakePublisher) Ping(context.Context) error { return p.err }

type fakeStore struct {
	pingErr error
	pending []string
	records map[string]storage.Record
}

func (s *fakeStore) MarkPending(_ context.Context, ev event.Event) error {
	s.pending = append(s.pending, ev.ID)
	return nil
}

func (s *fakeStore) Get(_ context.Context, id string) (storage.Record, error) {
	if rec, ok := s.records[id]; ok {
		return rec, nil
	}
	return storage.Record{}, storage.ErrNotFound
}

func (s *fakeStore) Stats(context.Context) ([]storage.TypeStats, error) { return nil, nil }
func (s *fakeStore) Ping(context.Context) error                         { return s.pingErr }

func newServer(pub *fakePublisher, store *fakeStore) *Server {
	opts := Options{MaxBodyBytes: 1024, MaxInFlight: 8, PublishTimeout: time.Second}
	return New(pub, store, opts, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func post(srv http.Handler, contentType, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

const validBody = `{"event_type":"order.created","occurred_at":"2026-01-02T15:04:05Z","payload":{"amount":1299}}`

func TestCreateEventAssignsIDAndPublishes(t *testing.T) {
	pub, store := &fakePublisher{}, &fakeStore{}
	rec := post(newServer(pub, store), "application/json; charset=utf-8", validBody)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	var resp map[string]string
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if !strings.HasPrefix(resp["event_id"], "evt_") || resp["status"] != "accepted" {
		t.Fatalf("unexpected response %v", resp)
	}
	if len(pub.published) != 1 || pub.published[0].ID != resp["event_id"] || len(store.pending) != 1 {
		t.Fatalf("published=%v pending=%v", pub.published, store.pending)
	}
}

func TestCreateEventRejectsBadRequests(t *testing.T) {
	tests := map[string]struct {
		contentType, body string
		want              int
	}{
		"malformed json": {"application/json", `{"event_type":`, http.StatusBadRequest},
		"unknown field":  {"application/json", `{"event_type":"a.b","admin":true}`, http.StatusBadRequest},
		"invalid event":  {"application/json", `{"event_type":"Bad","payload":{}}`, http.StatusBadRequest},
		"wrong media":    {"text/plain", validBody, http.StatusUnsupportedMediaType},
		"oversized":      {"application/json", `{"payload":{"pad":"` + strings.Repeat("x", 2048) + `"}}`, http.StatusRequestEntityTooLarge},
	}
	for name, tt := range tests {
		pub := &fakePublisher{}
		if rec := post(newServer(pub, &fakeStore{}), tt.contentType, tt.body); rec.Code != tt.want || len(pub.published) != 0 {
			t.Errorf("%s: status %d (want %d), published %d", name, rec.Code, tt.want, len(pub.published))
		}
	}
}

func TestCreateEventKafkaUnavailable(t *testing.T) {
	store := &fakeStore{}
	rec := post(newServer(&fakePublisher{err: errors.New("no brokers")}, store), "application/json", validBody)
	if rec.Code != http.StatusServiceUnavailable || len(store.pending) != 0 {
		t.Fatalf("status %d, pending %v", rec.Code, store.pending)
	}
}

func TestGetEvent(t *testing.T) {
	store := &fakeStore{records: map[string]storage.Record{"evt_1": {ID: "evt_1", Status: event.StatusProcessed}}}
	srv := newServer(&fakePublisher{}, store)
	for path, want := range map[string]int{"/v1/events/evt_1": http.StatusOK, "/v1/events/missing": http.StatusNotFound} {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != want {
			t.Errorf("%s: status %d, want %d", path, rec.Code, want)
		}
	}
}

func TestReadinessDependsOnlyOnKafka(t *testing.T) {
	cases := []struct {
		pub   *fakePublisher
		store *fakeStore
		want  int
	}{
		{&fakePublisher{}, &fakeStore{pingErr: errors.New("db down")}, http.StatusOK},
		{&fakePublisher{err: errors.New("kafka down")}, &fakeStore{}, http.StatusServiceUnavailable},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		newServer(c.pub, c.store).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if rec.Code != c.want {
			t.Errorf("readyz = %d, want %d (%s)", rec.Code, c.want, rec.Body)
		}
	}
}
