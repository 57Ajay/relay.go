package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/57Ajay/relay/internal/event"
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

	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9094"),
		kgo.ConsumerGroup("relay-consumers"),
		kgo.ConsumeTopics("events"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatal("failed to create client: ", err)
	}
	defer client.Close()

	log.Println("consumer started")
	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		for _, fe := range fetches.Errors() {
			log.Printf("fetch %s/%d: %v", fe.Topic, fe.Partition, fe.Err)
		}

		fetches.EachRecord(func(r *kgo.Record) {
			ev, err := event.Decode(bytes.NewReader(r.Value))
			if err != nil {
				log.Printf("partition=%d offset=%d undecodable: %v", r.Partition, r.Offset, err)
				return
			}
			amount, err := ev.Amount()
			if err != nil {
				log.Printf("partition=%d offset=%d event=%s: %v", r.Partition, r.Offset, ev.ID, err)
				return
			}
			if err := store.Save(ctx, ev, amount); err != nil {
				log.Printf("partition=%d offset=%d event=%s save failed: %v", r.Partition, r.Offset, ev.ID, err)
				return
			}
			log.Printf("partition=%d offset=%d event=%s saved", r.Partition, r.Offset, ev.ID)
		})
	}
	log.Println("consumer stopped")
}
