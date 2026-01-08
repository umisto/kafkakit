CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE inbox_event_status AS ENUM (
    'pending',
    'processed',
    'failed'
);

CREATE TABLE inbox_events (
    id       UUID   PRIMARY KEY NOT NULL,
    seq      BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL,

    topic    TEXT   NOT NULL,
    key      TEXT   NOT NULL,
    type     TEXT   NOT NULL,
    version  INT    NOT NULL,
    producer TEXT   NOT NULL,
    payload  JSONB  NOT NULL,

    status          inbox_event_status NOT NULL DEFAULT 'pending', -- pending | processed | failed
    attempts        INT NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),

    created_at    TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),

    kafka_partition  INT,
    kafka_offset     BIGINT,

    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    processed_at  TIMESTAMPTZ
);

CREATE UNIQUE INDEX ux_inbox_kafka_msg
    ON inbox_events (topic, kafka_partition, kafka_offset)
    WHERE kafka_partition IS NOT NULL AND kafka_offset IS NOT NULL;

CREATE INDEX idx_inbox_pending
    ON inbox_events (next_retry_at, seq)
    WHERE status = 'pending';

CREATE INDEX idx_inbox_pending_by_key
    ON inbox_events (key, next_retry_at, seq)
    WHERE status = 'pending';

CREATE TABLE inbox_key_locks (
    key       TEXT PRIMARY KEY,
    owner     TEXT NOT NULL,
    locked_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    stale_at  TIMESTAMPTZ NOT NULL DEFAULT ((now() AT TIME ZONE 'UTC') + interval '5 minute')
);

CREATE INDEX idx_inbox_key_locks_locked_at
    ON inbox_key_locks (locked_at);

CREATE TABLE inbox_key_state (
    key           TEXT PRIMARY KEY,
    blocked_until TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE INDEX idx_inbox_key_state_blocked_until
    ON inbox_key_state (blocked_until);
