package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Common struct {
	ServiceName string
	LogLevel    string
	Brokers     []string
	Topic       string
	DLQTopic    string
	DatabaseURL string
	DBMaxConns  int32
}

type API struct {
	Common
	HTTPAddr        string
	MaxBodyBytes    int64
	MaxInFlight     int
	PublishTimeout  time.Duration
	DrainDelay      time.Duration
	ShutdownTimeout time.Duration
}

type Consumer struct {
	Common
	GroupID        string
	OpsAddr        string
	Workers        int
	MaxPollRecords int
	SessionTimeout time.Duration
	MaxAttempts    int
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	OpTimeout      time.Duration
	FaultRate      float64
	Migrate        bool
}

type lookupFunc func(string) (string, bool)

func LoadAPI() (API, error)           { return loadAPI(os.LookupEnv) }
func LoadConsumer() (Consumer, error) { return loadConsumer(os.LookupEnv) }

func loadAPI(lookup lookupFunc) (API, error) {
	l := &loader{lookup: lookup}
	cfg := API{
		Common:          l.common("relay-api"),
		HTTPAddr:        l.str("HTTP_ADDR", ":8080"),
		MaxBodyBytes:    int64(parse(l, "MAX_BODY_BYTES", 64<<10, strconv.Atoi)),
		MaxInFlight:     parse(l, "MAX_IN_FLIGHT", 1024, strconv.Atoi),
		PublishTimeout:  parse(l, "PUBLISH_TIMEOUT", 5*time.Second, time.ParseDuration),
		DrainDelay:      parse(l, "DRAIN_DELAY", 0, time.ParseDuration),
		ShutdownTimeout: parse(l, "SHUTDOWN_TIMEOUT", 15*time.Second, time.ParseDuration),
	}
	l.check(cfg.MaxBodyBytes >= 1024, "MAX_BODY_BYTES must be at least 1024")
	l.check(cfg.MaxInFlight > 0, "MAX_IN_FLIGHT must be positive")
	l.check(cfg.PublishTimeout > 0, "PUBLISH_TIMEOUT must be positive")
	l.check(cfg.DrainDelay >= 0, "DRAIN_DELAY must not be negative")
	l.check(cfg.ShutdownTimeout > 0, "SHUTDOWN_TIMEOUT must be positive")
	return cfg, l.err()
}

func loadConsumer(lookup lookupFunc) (Consumer, error) {
	l := &loader{lookup: lookup}
	cfg := Consumer{
		Common:         l.common("relay-consumer"),
		GroupID:        l.str("KAFKA_GROUP_ID", "relay-consumers"),
		OpsAddr:        l.str("OPS_ADDR", ":9090"),
		Workers:        parse(l, "WORKERS", 4, strconv.Atoi),
		MaxPollRecords: parse(l, "MAX_POLL_RECORDS", 1000, strconv.Atoi),
		SessionTimeout: parse(l, "KAFKA_SESSION_TIMEOUT", 15*time.Second, time.ParseDuration),
		MaxAttempts:    parse(l, "MAX_ATTEMPTS", 5, strconv.Atoi),
		RetryBaseDelay: parse(l, "RETRY_BASE_DELAY", 100*time.Millisecond, time.ParseDuration),
		RetryMaxDelay:  parse(l, "RETRY_MAX_DELAY", 5*time.Second, time.ParseDuration),
		OpTimeout:      parse(l, "OP_TIMEOUT", 5*time.Second, time.ParseDuration),
		FaultRate:      parse(l, "FAULT_RATE", 0.0, parseFloat),
		Migrate:        parse(l, "DB_MIGRATE", true, strconv.ParseBool),
	}
	l.check(cfg.Workers > 0, "WORKERS must be positive")
	l.check(cfg.MaxPollRecords > 0, "MAX_POLL_RECORDS must be positive")
	l.check(cfg.SessionTimeout >= 6*time.Second, "KAFKA_SESSION_TIMEOUT must be at least 6s (broker minimum)")
	l.check(cfg.MaxAttempts > 0, "MAX_ATTEMPTS must be positive")
	l.check(cfg.RetryBaseDelay > 0 && cfg.RetryBaseDelay <= cfg.RetryMaxDelay,
		"RETRY_BASE_DELAY must be positive and not exceed RETRY_MAX_DELAY")
	l.check(cfg.OpTimeout > 0, "OP_TIMEOUT must be positive")
	l.check(cfg.FaultRate >= 0 && cfg.FaultRate <= 1, "FAULT_RATE must be between 0 and 1")
	return cfg, l.err()
}

type loader struct {
	lookup lookupFunc
	errs   []error
}

func (l *loader) common(service string) Common {
	c := Common{
		ServiceName: l.str("SERVICE_NAME", service),
		LogLevel:    l.str("LOG_LEVEL", "info"),
		Brokers:     l.list("KAFKA_BROKERS", "localhost:9094"),
		Topic:       l.str("KAFKA_TOPIC", "events"),
		DLQTopic:    l.str("KAFKA_DLQ_TOPIC", "events.dlq"),
		DatabaseURL: l.str("DATABASE_URL", "postgres://relay:relay@localhost:5432/relay?sslmode=disable"),
		DBMaxConns:  int32(parse(l, "DB_MAX_CONNS", 10, strconv.Atoi)),
	}
	l.check(len(c.Brokers) > 0, "KAFKA_BROKERS must list at least one broker")
	l.check(c.Topic != c.DLQTopic, "KAFKA_TOPIC and KAFKA_DLQ_TOPIC must differ")
	l.check(c.DBMaxConns > 0, "DB_MAX_CONNS must be positive")
	return c
}

func (l *loader) str(key, def string) string {
	if v, ok := l.lookup(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

func (l *loader) list(key, def string) []string {
	var out []string
	for part := range strings.SplitSeq(l.str(key, def), ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func (l *loader) check(ok bool, msg string) {
	if !ok {
		l.errs = append(l.errs, errors.New(msg))
	}
}

func (l *loader) err() error { return errors.Join(l.errs...) }

func parse[T any](l *loader, key string, def T, fn func(string) (T, error)) T {
	raw := l.str(key, "")
	if raw == "" {
		return def
	}
	v, err := fn(raw)
	if err != nil {
		l.errs = append(l.errs, fmt.Errorf("%s: invalid value %q", key, raw))
		return def
	}
	return v
}

func parseFloat(s string) (float64, error) { return strconv.ParseFloat(s, 64) }
