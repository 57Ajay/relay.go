package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/57Ajay/relay/internal/event"
	"github.com/57Ajay/relay/internal/kafka"
	"github.com/57Ajay/relay/internal/storage"
	"github.com/twmb/franz-go/pkg/kgo"
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

	consumer, err := kafka.NewConsumer([]string{"localhost:9094"}, "relay-consumers", "events")
	if err != nil {
		log.Fatal("failed to create consumer: ", err)
	}
	defer consumer.Close()

	log.Println("consumer started")
	if err := consumer.Run(ctx, handler{store: store}); err != nil {
		log.Println("consumer failed: ", err)
	}
	log.Println("consumer stopped")
}

type handler struct {
	store *storage.Store
}

func (h handler) Handle(ctx context.Context, r *kgo.Record) error {
	ev, err := event.Decode(bytes.NewReader(r.Value))
	if err != nil {
		log.Printf("partition=%d offset=%d undecodable, skipping: %v", r.Partition, r.Offset, err)
		return nil
	}
	if err := ev.Validate(time.Now()); err != nil {
		log.Printf("partition=%d offset=%d event=%s invalid, skipping: %v", r.Partition, r.Offset, ev.ID, err)
		return nil
	}
	amount, _ := ev.Amount()
	claimed, err := h.store.Claim(ctx, ev)
	if err != nil {
		return err
	}
	if !claimed {
		log.Printf("partition=%d offset=%d event=%s already processed, skipping duplicate", r.Partition, r.Offset, ev.ID)
		return nil
	}
	applied, err := h.store.Complete(ctx, ev, amount)
	if err != nil {
		return err
	}
	if !applied {
		log.Printf("partition=%d offset=%d event=%s already saved, skipping duplicate", r.Partition, r.Offset, ev.ID)
		return nil
	}
	log.Printf("partition=%d offset=%d event=%s saved", r.Partition, r.Offset, ev.ID)
	return nil
}
