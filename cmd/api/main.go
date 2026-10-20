package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/57Ajay/relay/internal/config"
	"github.com/57Ajay/relay/internal/event"
	"github.com/57Ajay/relay/internal/kafka"
	"github.com/57Ajay/relay/internal/storage"
	"github.com/57Ajay/relay/internal/telemetry"
)

func main() {
	if len(os.Args) == 3 && os.Args[1] == "probe" {
		os.Exit(telemetry.Probe(os.Args[2]))
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "api:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAPI()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := telemetry.NewLogger(os.Stdout, cfg.ServiceName, cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	producer, err := kafka.NewProducer(cfg.Brokers, cfg.Topic, cfg.DLQTopic)
	if err != nil {
		return err
	}
	defer withTimeout(5*time.Second, producer.Close)

	store, err := storage.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer store.Close()

	var draining atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, r *http.Request) {
		ev, err := event.Decode(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errorBody{Error: "malformed request body", Details: []string{err.Error()}})
			return
		}
		if ev.ID == "" {
			ev.ID = event.NewID()
		}
		if err := ev.Validate(time.Now()); err != nil {
			body := errorBody{Error: "invalid event"}
			if verr, ok := errors.AsType[*event.ValidationError](err); ok {
				body.Details = verr.Problems
			}
			writeJSON(w, http.StatusBadRequest, body)
			return
		}

		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), cfg.PublishTimeout)
		defer cancel()
		if err := producer.Publish(ctx, ev); err != nil {
			log.ErrorContext(ctx, "publish failed", "event_id", ev.ID, "err", err)
			writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "event broker unavailable, retry with the same event_id", EventID: ev.ID})
			return
		}

		pctx, pcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer pcancel()
		if err := store.MarkPending(pctx, ev); err != nil {
			log.WarnContext(ctx, "could not record pending status", "event_id", ev.ID, "err", err)
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"event_id": ev.ID, "status": "accepted"})
	})

	mux.HandleFunc("GET /v1/events/{id}", func(w http.ResponseWriter, r *http.Request) {
		rec, err := store.Get(r.Context(), r.PathValue("id"))
		switch {
		case errors.Is(err, storage.ErrNotFound):
			writeJSON(w, http.StatusNotFound, errorBody{Error: "event not found"})
		case err != nil:
			log.ErrorContext(r.Context(), "status lookup failed", "err", err)
			writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "status store unavailable"})
		default:
			writeJSON(w, http.StatusOK, rec)
		}
	})

	mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) {
		stats, err := store.Stats(r.Context())
		if err != nil {
			log.ErrorContext(r.Context(), "stats lookup failed", "err", err)
			writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "status store unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"stats": stats})
	})

	mux.HandleFunc("GET /healthz", telemetry.Healthz)
	mux.Handle("GET /readyz", telemetry.ReadyHandler(&draining,
		telemetry.Check{Name: "kafka", Critical: true, Fn: producer.Ping},
		telemetry.Check{Name: "postgres", Fn: store.Ping}))

	httpServer := &http.Server{Addr: cfg.HTTPAddr, Handler: mux}
	errCh := make(chan error, 1)
	go func() { errCh <- httpServer.ListenAndServe() }()
	log.Info("api listening", "addr", cfg.HTTPAddr, "topic", cfg.Topic)

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		stop()
	}

	log.Info("shutting down", "drain_delay", cfg.DrainDelay)
	draining.Store(true)
	time.Sleep(cfg.DrainDelay)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http shutdown: %w", err)
	}
	log.Info("api stopped")
	return nil
}

func withTimeout(d time.Duration, fn func(context.Context)) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	fn(ctx)
}

type errorBody struct {
	Error   string   `json:"error"`
	EventID string   `json:"event_id,omitempty"`
	Details []string `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
