package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

type Check struct {
	Name     string
	Critical bool
	Fn       func(context.Context) error
}

func Healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ReadyHandler reports every check; only critical ones (or draining) make it unready.
func ReadyHandler(draining *atomic.Bool, checks ...Check) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		ready := !draining.Load()
		status := map[string]string{}
		for _, c := range checks {
			status[c.Name] = "ok"
			if err := c.Fn(ctx); err != nil {
				status[c.Name] = "unavailable"
				ready = ready && !c.Critical
			}
		}
		code := http.StatusOK
		if !ready {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]any{"ready": ready, "draining": draining.Load(), "checks": status})
	}
}

func OpsMux(draining *atomic.Bool, checks ...Check) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", Healthz)
	mux.Handle("GET /readyz", ReadyHandler(draining, checks...))
	return mux
}

// Probe is used as a container health check, since distroless images ship no curl.
func Probe(url string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 1
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
