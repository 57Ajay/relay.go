package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/57Ajay/relay/internal/api"
	"github.com/57Ajay/relay/internal/config"
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

	// The API only writes best-effort "pending" rows; the consumer's writes stay fully durable.
	store, err := storage.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns, true)
	if err != nil {
		return err
	}
	defer store.Close()

	srv := api.New(producer, store, telemetry.NewRegistry(), api.Options{
		MaxBodyBytes:   cfg.MaxBodyBytes,
		MaxInFlight:    cfg.MaxInFlight,
		PublishTimeout: cfg.PublishTimeout,
	}, log.With("component", "api"))
	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      cfg.PublishTimeout + 10*time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}

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
	srv.Drain()
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
