package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/57Ajay/relay/internal/config"
	"github.com/57Ajay/relay/internal/event"
	"github.com/57Ajay/relay/internal/kafka"
	"github.com/57Ajay/relay/internal/storage"
)

func main() {
	cfg, err := config.LoadAPI()
	if err != nil {
		log.Fatal("config: ", err)
	}
	ctx := context.Background()

	producer, err := kafka.NewProducer(cfg.Brokers, cfg.Topic, cfg.DLQTopic)
	if err != nil {
		log.Fatal("failed to start the producer: ", err)
	}
	defer producer.Close(ctx)

	store, err := storage.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		log.Fatal("failed to open the store: ", err)
	}
	defer store.Close()

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
			log.Printf("publish %s: %v", ev.ID, err)
			writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "event broker unavailable, retry with the same event_id", EventID: ev.ID})
			return
		}

		pctx, pcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer pcancel()
		if err := store.MarkPending(pctx, ev); err != nil {
			log.Printf("mark pending %s: %v", ev.ID, err)
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"event_id": ev.ID, "status": "accepted"})
	})

	mux.HandleFunc("GET /v1/events/{id}", func(w http.ResponseWriter, r *http.Request) {
		rec, err := store.Get(r.Context(), r.PathValue("id"))
		switch {
		case errors.Is(err, storage.ErrNotFound):
			writeJSON(w, http.StatusNotFound, errorBody{Error: "event not found"})
		case err != nil:
			log.Printf("get event: %v", err)
			writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "status store unavailable"})
		default:
			writeJSON(w, http.StatusOK, rec)
		}
	})

	mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) {
		stats, err := store.Stats(r.Context())
		if err != nil {
			log.Printf("stats: %v", err)
			writeJSON(w, http.StatusServiceUnavailable, errorBody{Error: "status store unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"stats": stats})
	})

	log.Printf("api listening on %s", cfg.HTTPAddr)
	log.Fatal(http.ListenAndServe(cfg.HTTPAddr, mux))
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
