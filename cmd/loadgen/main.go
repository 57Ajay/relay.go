// Command loadgen drives the API at a target rate and, given -db, waits for the
// consumers to finish and reports processing throughput and end-to-end latency
// measured inside PostgreSQL (occurred_at is stamped at send time).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

var eventTypes = []string{"order.created", "order.paid", "order.shipped", "order.cancelled"}

type result struct {
	latencies []time.Duration
	accepted  atomic.Int64
	rejected  atomic.Int64
	errored   atomic.Int64
	mu        sync.Mutex
}

func main() {
	var (
		url         = flag.String("url", "http://localhost:8080", "API base URL")
		rate        = flag.Int("rate", 500, "target events per second (0 = as fast as possible)")
		duration    = flag.Duration("duration", 30*time.Second, "how long to send")
		concurrency = flag.Int("concurrency", 32, "concurrent HTTP clients")
		payloadSize = flag.Int("payload-bytes", 256, "approximate payload size")
		dbURL       = flag.String("db", "", "PostgreSQL URL; when set, wait for processing and report it")
		wait        = flag.Duration("wait", 5*time.Minute, "maximum time to wait for processing")
	)
	flag.Parse()
	if _, err := http.NewRequestWithContext(context.Background(), http.MethodPost, *url+"/v1/events", nil); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen: invalid -url:", err)
		os.Exit(2)
	}

	runID := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	fmt.Printf("run %s: rate=%d/s duration=%s concurrency=%d payload=%dB\n", runID, *rate, *duration, *concurrency, *payloadSize)

	res, elapsed := send(*url, runID, *rate, *duration, *concurrency, *payloadSize)
	accepted := res.accepted.Load()
	fmt.Printf("\ningest:  accepted=%d rejected=%d errors=%d in %s (%.0f accepted/s)\n",
		accepted, res.rejected.Load(), res.errored.Load(), elapsed.Round(time.Millisecond), float64(accepted)/elapsed.Seconds())
	slices.Sort(res.latencies)
	fmt.Printf("http latency: p50=%s p95=%s p99=%s max=%s\n",
		pct(res.latencies, 0.50), pct(res.latencies, 0.95), pct(res.latencies, 0.99), pct(res.latencies, 1))

	if *dbURL != "" && accepted > 0 {
		if err := report(*dbURL, runID, accepted, *wait); err != nil {
			fmt.Fprintln(os.Stderr, "loadgen:", err)
			os.Exit(1)
		}
	}
}

func send(url, runID string, rate int, duration time.Duration, concurrency, payloadSize int) (*result, time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	tokens := make(chan int64)
	go func() {
		defer close(tokens)
		var tick <-chan time.Time
		if rate > 0 {
			t := time.NewTicker(time.Second / time.Duration(rate))
			defer t.Stop()
			tick = t.C
		}
		for n := int64(0); ; n++ {
			if tick != nil {
				select {
				case <-tick:
				case <-ctx.Done():
					return
				}
			}
			select {
			case tokens <- n:
			case <-ctx.Done():
				return
			}
		}
	}()

	res := &result{}
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: concurrency}}
	padding := strings.Repeat("x", max(payloadSize-64, 0))
	start := time.Now()
	var wg sync.WaitGroup
	for range concurrency {
		wg.Go(func() {
			var local []time.Duration
			for n := range tokens {
				body, _ := json.Marshal(map[string]any{
					"event_id":    fmt.Sprintf("%s-%d", runID, n),
					"event_type":  eventTypes[n%int64(len(eventTypes))],
					"occurred_at": time.Now().UTC().Format(time.RFC3339Nano),
					"payload":     map[string]any{"order_id": fmt.Sprintf("ord-%d", n), "amount": rand.IntN(10_000), "note": padding},
				})
				req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, url+"/v1/events", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				t0 := time.Now()
				resp, err := client.Do(req)
				local = append(local, time.Since(t0))
				switch {
				case err != nil:
					res.errored.Add(1)
				case resp.StatusCode == http.StatusAccepted:
					res.accepted.Add(1)
				default:
					res.rejected.Add(1)
				}
				if resp != nil {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
			}
			res.mu.Lock()
			res.latencies = append(res.latencies, local...)
			res.mu.Unlock()
		})
	}
	wg.Wait()
	return res, time.Since(start)
}

func report(dbURL, runID string, accepted int64, wait time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	conn, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()

	prefix := runID + "-%"
	fmt.Printf("waiting for %d events to be processed...\n", accepted)
	for {
		var done int64
		err := conn.QueryRow(ctx, `SELECT count(*) FROM processed_events
			WHERE event_id LIKE $1 AND status IN ('processed', 'failed')`, prefix).Scan(&done)
		if err != nil {
			return err
		}
		if done >= accepted {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out with %d/%d processed", done, accepted)
		case <-time.After(500 * time.Millisecond):
		}
	}

	var count int64
	var span, p50, p95, p99 float64
	err = conn.QueryRow(ctx, `
		SELECT count(*),
		       COALESCE(EXTRACT(EPOCH FROM max(processed_at) - min(occurred_at)), 0),
		       COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM processed_at - occurred_at)), 0),
		       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM processed_at - occurred_at)), 0),
		       COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM processed_at - occurred_at)), 0)
		FROM processed_events WHERE event_id LIKE $1 AND status = 'processed'`, prefix).
		Scan(&count, &span, &p50, &p95, &p99)
	if err != nil {
		return err
	}
	if count == 0 || span <= 0 {
		fmt.Println("process: no events were processed (all dead-lettered?)")
		return nil
	}
	fmt.Printf("process: %d events in %.2fs (%.0f processed/s)\n", count, span, float64(count)/span)
	fmt.Printf("end-to-end latency (send -> committed): p50=%s p95=%s p99=%s\n", secs(p50), secs(p95), secs(p99))
	return nil
}

func pct(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)-1))
	return sorted[i].Round(10 * time.Microsecond)
}

func secs(s float64) time.Duration {
	return time.Duration(s * float64(time.Second)).Round(100 * time.Microsecond)
}
