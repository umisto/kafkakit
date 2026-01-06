-- name: CreateInboxEvent :one
INSERT INTO inbox_events (
    id,
    topic,
    key,
    type,
    version,
    producer,
    payload,
    status,
    attempts,
    next_retry_at,
    processed_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9,  $10, $11
)
ON CONFLICT (id) DO NOTHING
RETURNING *;

-- name: GetInboxEventByID :one
SELECT *
FROM inbox_events
WHERE id = $1;

-- name: GetPendingInboxEvents :many
WITH picked AS (
    SELECT id
    FROM inbox_events
    WHERE status = 'pending'
      AND (next_retry_at IS NULL OR next_retry_at <= now() AT TIME ZONE 'UTC')
    ORDER BY seq ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
),
updated AS (
    UPDATE inbox_events i
    SET status = 'processing'
    FROM picked p
    WHERE i.id = p.id
    RETURNING i.*
)
SELECT *
FROM updated
ORDER BY seq ASC;


-- name: MarkInboxEventsAsProcessed :many
UPDATE inbox_events
SET
    status = 'processed',
    processed_at = now() AT TIME ZONE 'UTC'
WHERE id = ANY(sqlc.arg(ids)::uuid[])
RETURNING *;

-- name: MarkInboxEventsAsFailed :many
UPDATE inbox_events
SET
    status = 'failed',
    next_retry_at = NULL
WHERE id = ANY(sqlc.arg(ids)::uuid[])
RETURNING *;

-- name: MarkInboxEventsAsPending :many
UPDATE inbox_events
SET
    status = 'pending',
    attempts = attempts + 1,
    next_retry_at = sqlc.arg(next_retry_at)::timestamptz
WHERE id = ANY(sqlc.arg(ids)::uuid[])
RETURNING *;

-- name: UpdateInboxEventStatus :one
UPDATE inbox_events
SET status = sqlc.arg(status)::inbox_event_status
WHERE id = sqlc.arg(id)::uuid
RETURNING *;

