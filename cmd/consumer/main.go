package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/57Ajay/relay/internal/kafka"
	"github.com/57Ajay/relay/internal/processor"
	"github.com/57Ajay/relay/internal/storage"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := storage.Open(ctx, "postgres://relay:relay@localhost:5432/relay?sslmode=disable", 10)
	if err != nil {
		log.Fatal("failed to open the store: ", err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		log.Fatal("migrate: ", err)
	}

	consumer, err := kafka.NewConsumer([]string{"localhost:9094"}, "relay-consumers", "events", "events.dlq")
	if err != nil {
		log.Fatal("failed to create consumer: ", err)
	}
	defer consumer.Close()

	faultRate, _ := strconv.ParseFloat(os.Getenv("FAULT_RATE"), 64)
	proc := processor.New(store, consumer.Producer(), processor.Config{
		MaxAttempts: 5,
		Backoff:     processor.Backoff{Base: 100 * time.Millisecond, Max: 5 * time.Second},
		OpTimeout:   5 * time.Second,
		FaultRate:   faultRate,
	}, slog.Default())

	log.Println("consumer started")
	if err := consumer.Run(ctx, proc); err != nil {
		log.Println("consumer failed: ", err)
	}
	log.Println("consumer stopped")
}
