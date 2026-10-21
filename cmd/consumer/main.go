package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/57Ajay/relay/internal/config"
	"github.com/57Ajay/relay/internal/kafka"
	"github.com/57Ajay/relay/internal/processor"
	"github.com/57Ajay/relay/internal/storage"
	"github.com/57Ajay/relay/internal/telemetry"
)

func main() {
	if len(os.Args) == 3 && os.Args[1] == "probe" {
		os.Exit(telemetry.Probe(os.Args[2]))
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "consumer:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadConsumer()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	log := telemetry.NewLogger(os.Stdout, cfg.ServiceName, cfg.LogLevel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.InitTracing(ctx, cfg.ServiceName, cfg.OTLPEndpoint, cfg.TraceRatio)
	if err != nil {
		return err
	}
	defer withTimeout(5*time.Second, func(ctx context.Context) { _ = shutdownTracing(ctx) })

	store, err := storage.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns, false)
	if err != nil {
		return err
	}
	defer store.Close()
	if cfg.Migrate {
		if err := migrate(ctx, store, log); err != nil {
			return err
		}
	}

	reg := telemetry.NewRegistry()
	metrics := telemetry.NewConsumerMetrics(reg)
	consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:        cfg.Brokers,
		Group:          cfg.GroupID,
		Topic:          cfg.Topic,
		DLQTopic:       cfg.DLQTopic,
		Workers:        cfg.Workers,
		MaxPollRecords: cfg.MaxPollRecords,
		SessionTimeout: cfg.SessionTimeout,
	}, log.With("component", "kafka"), metrics)
	if err != nil {
		return err
	}
	proc := processor.New(store, consumer.Producer(), processor.Config{
		MaxAttempts: cfg.MaxAttempts,
		Backoff:     processor.Backoff{Base: cfg.RetryBaseDelay, Max: cfg.RetryMaxDelay},
		OpTimeout:   cfg.OpTimeout,
		FaultRate:   cfg.FaultRate,
	}, log.With("component", "processor"), metrics)

	var draining atomic.Bool
	ops := &http.Server{
		Addr: cfg.OpsAddr,
		Handler: telemetry.OpsMux(reg, &draining,
			telemetry.Check{Name: "kafka", Critical: true, Fn: consumer.Ping},
			telemetry.Check{Name: "postgres", Critical: true, Fn: store.Ping}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := ops.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Error("ops server failed", "err", err)
		}
	}()

	log.Info("consumer started", "group", cfg.GroupID, "topic", cfg.Topic, "workers", cfg.Workers)
	runErr := consumer.Run(ctx, proc)
	stop()

	draining.Store(true)
	log.Info("leaving consumer group")
	consumer.Close()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = ops.Shutdown(shutdownCtx)
	log.Info("consumer stopped")
	return runErr
}

// migrate retries while PostgreSQL is still starting.
func migrate(ctx context.Context, store *storage.Store, log *slog.Logger) error {
	for attempt := 1; ; attempt++ {
		err := store.Migrate(ctx)
		if err == nil {
			return nil
		}
		if attempt == 10 || ctx.Err() != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		log.Warn("database not ready; retrying migration", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
		case <-time.After(3 * time.Second):
		}
	}
}

func withTimeout(d time.Duration, fn func(context.Context)) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	fn(ctx)
}
