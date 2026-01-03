-- name: CreateOutboxEvent :one
INSERT INTO outbox_events (
    id,
    topic,
    type,
    version,
    key,
    producer,
    payload,
    status,
    attempts,
    next_retry_at,
    sent_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9,  $10, $11
)
ON CONFLICT (id) DO NOTHING
RETURNING *;

-- name: GetOutboxEventByID :one
SELECT *
FROM outbox_events
WHERE id = $1;

-- name: GetPendingOutboxEvents :many
WITH picked AS (
    SELECT id
    FROM outbox_events
    WHERE status = 'pending'
      AND (next_retry_at IS NULL OR next_retry_at <= now() AT TIME ZONE 'UTC')
    ORDER BY seq ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
),
updated AS (
    UPDATE outbox_events o
    SET status = 'processing'
    FROM picked p
    WHERE o.id = p.id
    RETURNING o.*
)
SELECT *
FROM updated
ORDER BY seq ASC;


-- name: MarkOutboxEventsAsSent :many
UPDATE outbox_events
SET
    status = 'sent',
    sent_at = now() AT TIME ZONE 'UTC'
WHERE id = ANY(sqlc.arg(ids)::uuid[])
RETURNING *;

-- name: MarkOutboxEventsAsFailed :many
UPDATE outbox_events
SET
    status = 'failed',
    next_retry_at = NULL
WHERE id = ANY(sqlc.arg(ids)::uuid[])
RETURNING *;

-- name: MarkOutboxEventsAsPending :many
UPDATE outbox_events
SET
    status = 'pending',
    attempts = attempts + 1,
    next_retry_at = sqlc.arg(next_retry_at)::timestamptz
WHERE id = ANY(sqlc.arg(ids)::uuid[])
RETURNING *;