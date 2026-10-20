package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/57Ajay/relay/internal/config"
	"github.com/57Ajay/relay/internal/kafka"
	"github.com/57Ajay/relay/internal/processor"
	"github.com/57Ajay/relay/internal/storage"
)

func main() {
	cfg, err := config.LoadConsumer()
	if err != nil {
		log.Fatal("config: ", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := storage.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		log.Fatal("failed to open the store: ", err)
	}
	defer store.Close()
	if cfg.Migrate {
		if err := store.Migrate(ctx); err != nil {
			log.Fatal("migrate: ", err)
		}
	}

	consumer, err := kafka.NewConsumer(cfg.Brokers, cfg.GroupID, cfg.Topic, cfg.DLQTopic)
	if err != nil {
		log.Fatal("failed to create consumer: ", err)
	}
	defer consumer.Close()

	proc := processor.New(store, consumer.Producer(), processor.Config{
		MaxAttempts: cfg.MaxAttempts,
		Backoff:     processor.Backoff{Base: cfg.RetryBaseDelay, Max: cfg.RetryMaxDelay},
		OpTimeout:   cfg.OpTimeout,
		FaultRate:   cfg.FaultRate,
	}, slog.Default())

	log.Println("consumer started")
	if err := consumer.Run(ctx, proc); err != nil {
		log.Println("consumer failed: ", err)
	}
	log.Println("consumer stopped")
}
