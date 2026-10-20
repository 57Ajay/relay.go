// Package api serves the HTTP ingestion and status endpoints.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/57Ajay/relay/internal/event"
	"github.com/57Ajay/relay/internal/storage"
	"github.com/57Ajay/relay/internal/telemetry"
)

const pendingWriteTimeout = 500 * time.Millisecond

type Publisher interface {
	Publish(ctx context.Context, ev event.Event) error
	Ping(ctx context.Context) error
}

type Store interface {
	MarkPending(ctx context.Context, ev event.Event) error
	Get(ctx context.Context, id string) (storage.Record, error)
	Stats(ctx context.Context) ([]storage.TypeStats, error)
	Ping(ctx context.Context) error
}

type Options struct {
	MaxBodyBytes   int64
	MaxInFlight    int
	PublishTimeout time.Duration
}

type Server struct {
	pub      Publisher
	store    Store
	opts     Options
	log      *slog.Logger
	inflight chan struct{}
	draining atomic.Bool
	handler  http.Handler
}

func New(pub Publisher, store Store, opts Options, log *slog.Logger) *Server {
	s := &Server{
		pub:      pub,
		store:    store,
		opts:     opts,
		log:      log,
		inflight: make(chan struct{}, opts.MaxInFlight),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", s.createEvent)
	mux.HandleFunc("GET /v1/events/{id}", s.getEvent)
	mux.HandleFunc("GET /v1/stats", s.getStats)
	mux.HandleFunc("GET /healthz", telemetry.Healthz)
	// Ingestion only needs Kafka; a database outage must not take the API out of the load balancer.
	mux.Handle("GET /readyz", telemetry.ReadyHandler(&s.draining,
		telemetry.Check{Name: "kafka", Critical: true, Fn: pub.Ping},
		telemetry.Check{Name: "postgres", Fn: store.Ping}))
	s.handler = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.handler.ServeHTTP(w, r) }

// Drain makes /readyz fail so load balancers stop routing new traffic.
func (s *Server) Drain() { s.draining.Store(true) }

func (s *Server) createEvent(w http.ResponseWriter, r *http.Request) {
	select {
	case s.inflight <- struct{}{}:
		defer func() { <-s.inflight }()
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "server overloaded, retry later", nil)
		return
	}

	if mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mt != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "content type must be application/json", nil)
		return
	}
	ev, err := event.Decode(http.MaxBytesReader(w, r.Body, s.opts.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			w.Header().Set("Connection", "close")
			writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d bytes", tooLarge.Limit), nil)
			return
		}
		writeError(w, http.StatusBadRequest, "malformed request body", []string{err.Error()})
		return
	}
	if ev.ID == "" {
		ev.ID = event.NewID()
	}
	if err := ev.Validate(time.Now()); err != nil {
		var problems []string
		if verr, ok := errors.AsType[*event.ValidationError](err); ok {
			problems = verr.Problems
		}
		writeError(w, http.StatusBadRequest, "invalid event", problems)
		return
	}

	// A client hanging up must not cancel the produce: franz-go fails every record buffered behind a cancelled one.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), s.opts.PublishTimeout)
	defer cancel()
	if err := s.pub.Publish(ctx, ev); err != nil {
		s.log.ErrorContext(ctx, "publish failed", "event_id", ev.ID, "err", err)
		writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "event broker unavailable, retry with the same event_id", EventID: ev.ID})
		return
	}

	pctx, pcancel := context.WithTimeout(ctx, pendingWriteTimeout)
	defer pcancel()
	if err := s.store.MarkPending(pctx, ev); err != nil {
		s.log.DebugContext(ctx, "could not record pending status", "event_id", ev.ID, "err", err)
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"event_id": ev.ID, "status": "accepted"})
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request) {
	rec, err := s.store.Get(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, storage.ErrNotFound):
		writeError(w, http.StatusNotFound, "event not found", nil)
	case err != nil:
		s.log.ErrorContext(r.Context(), "status lookup failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "status store unavailable", nil)
	default:
		writeJSON(w, http.StatusOK, rec)
	}
}

func (s *Server) getStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context())
	if err != nil {
		s.log.ErrorContext(r.Context(), "stats lookup failed", "err", err)
		writeError(w, http.StatusServiceUnavailable, "status store unavailable", nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"stats": stats})
}

type errorBody struct {
	Error   string   `json:"error"`
	EventID string   `json:"event_id,omitempty"`
	Details []string `json:"details,omitempty"`
}

func writeError(w http.ResponseWriter, code int, msg string, details []string) {
	writeJSON(w, code, errorBody{Error: msg, Details: details})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
