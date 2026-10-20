package config

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func env(vars map[string]string) lookupFunc {
	return func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
}

func TestConsumerDefaults(t *testing.T) {
	cfg, err := loadConsumer(env(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Topic != "events" || cfg.DLQTopic != "events.dlq" || cfg.GroupID != "relay-consumers" {
		t.Errorf("unexpected topics/group: %+v", cfg)
	}
	if cfg.MaxAttempts != 5 || cfg.Workers != 4 || !cfg.Migrate {
		t.Errorf("unexpected processing defaults: %+v", cfg)
	}
}

func TestAPIOverrides(t *testing.T) {
	cfg, err := loadAPI(env(map[string]string{
		"KAFKA_BROKERS":   " kafka-0:9092, kafka-1:9092 ,",
		"MAX_BODY_BYTES":  "2048",
		"PUBLISH_TIMEOUT": "2s",
		"SERVICE_NAME":    "api-test",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !slices.Equal(cfg.Brokers, []string{"kafka-0:9092", "kafka-1:9092"}) {
		t.Errorf("brokers = %v", cfg.Brokers)
	}
	if cfg.MaxBodyBytes != 2048 || cfg.PublishTimeout != 2*time.Second || cfg.ServiceName != "api-test" {
		t.Errorf("overrides not applied: %+v", cfg)
	}
}

func TestInvalidValuesAreAllReported(t *testing.T) {
	_, err := loadConsumer(env(map[string]string{
		"WORKERS":          "many",
		"MAX_ATTEMPTS":     "0",
		"RETRY_BASE_DELAY": "10s",
		"RETRY_MAX_DELAY":  "1s",
		"FAULT_RATE":       "1.5",
		"KAFKA_DLQ_TOPIC":  "events",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"WORKERS", "MAX_ATTEMPTS", "RETRY_BASE_DELAY", "FAULT_RATE", "KAFKA_DLQ_TOPIC"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}
