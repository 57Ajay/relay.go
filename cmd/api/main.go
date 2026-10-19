package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/57Ajay/relay/internal/event"
	"github.com/57Ajay/relay/internal/kafka"
	"github.com/57Ajay/relay/internal/storage"
)

func main() {
	ctx := context.Background()

	producer, err := kafka.NewProducer([]string{"localhost:9094"}, "events")
	if err != nil {
		log.Fatal("failed to start the producer: ", err)
	}
	defer producer.Close(ctx)

	store, err := storage.Open(ctx, "postgres://relay:relay@localhost:5432/relay?sslmode=disable", 10)
	if err != nil {
		log.Fatal("failed to open the store: ", err)
	}
	defer store.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", func(w http.ResponseWriter, r *http.Request) {
		ev, err := event.Decode(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
			return
		}

		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Second)
		defer cancel()
		if err := producer.Publish(ctx, ev); err != nil {
			log.Printf("publish %s: %v", ev.ID, err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "event broker unavailable"})
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
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "event not found"})
		case err != nil:
			log.Printf("get event: %v", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "status store unavailable"})
		default:
			writeJSON(w, http.StatusOK, rec)
		}
	})

	mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) {
		stats, err := store.Stats(r.Context())
		if err != nil {
			log.Printf("stats: %v", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "status store unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"stats": stats})
	})

	log.Fatal(http.ListenAndServe(":8080", mux))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
