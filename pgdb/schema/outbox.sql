CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE outbox_event_status AS ENUM (
    'pending',
    'sent',
    'failed'
);

CREATE TABLE outbox_events (
    id       UUID   PRIMARY KEY NOT NULL DEFAULT uuid_generate_v4(),
    seq      BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,

    topic    TEXT   NOT NULL,
    key      TEXT   NOT NULL,
    type     TEXT   NOT NULL,
    version  INT    NOT NULL,
    producer TEXT   NOT NULL,
    payload  JSONB  NOT NULL,

    status          outbox_event_status NOT NULL DEFAULT 'pending', -- pending | sent | failed
    attempts        INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),

    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    sent_at       TIMESTAMPTZ
);

CREATE INDEX idx_outbox_pending
    ON outbox_events (next_retry_at, seq)
    WHERE status = 'pending';

CREATE INDEX idx_outbox_pending_by_key
    ON outbox_events (key, next_retry_at, seq)
    WHERE status = 'pending';

CREATE INDEX idx_outbox_sent_at
    ON outbox_events (sent_at)
    WHERE status = 'sent';

CREATE TABLE outbox_key_locks (
    key        TEXT PRIMARY KEY,
    owner      TEXT NOT NULL,
    locked_at  TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    stale_at  TIMESTAMPTZ NOT NULL DEFAULT ((now() AT TIME ZONE 'UTC') + interval '6 hours')
);

CREATE INDEX idx_outbox_key_locks_locked_at
    ON outbox_key_locks (locked_at);

CREATE TABLE outbox_key_state (
    key           TEXT PRIMARY KEY,
    blocked_until TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

