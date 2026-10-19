CREATE TABLE processed_events (
    event_id     TEXT PRIMARY KEY,
    event_type   TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    status       TEXT        NOT NULL CHECK (status IN ('pending', 'processing', 'processed', 'failed')),
    attempts     INTEGER     NOT NULL DEFAULT 0,
    last_error   TEXT,
    occurred_at  TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX processed_events_unfinished_idx
    ON processed_events (status, updated_at)
    WHERE status <> 'processed';

CREATE TABLE event_stats (
    event_type    TEXT PRIMARY KEY,
    event_count   BIGINT      NOT NULL DEFAULT 0,
    amount_total  BIGINT      NOT NULL DEFAULT 0,
    last_event_at TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
