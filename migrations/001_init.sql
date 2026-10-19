CREATE TABLE processed_events (
    event_id     TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    occurred_at  TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_processed_events_type_occurred_at
    ON processed_events (event_type, occurred_at DESC);

CREATE INDEX idx_processed_events_processed_at
    ON processed_events (processed_at);

CREATE TABLE event_stats (
    event_type   TEXT PRIMARY KEY,
    event_count  BIGINT NOT NULL DEFAULT 0,
    amount_total BIGINT NOT NULL DEFAULT 0
);

