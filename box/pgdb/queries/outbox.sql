-- name: CreateOutboxEvent :one
INSERT INTO outbox_events (
    id, topic, key, type, version, producer, payload
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
)
RETURNING *;

-- name: GetOutboxEventByID :one
SELECT *
FROM outbox_events
WHERE id = $1;

-- name: PickPendingOutboxKey :one
SELECT e.key
FROM outbox_events e
    LEFT JOIN outbox_key_state ks ON ks.key = e.key
    LEFT JOIN outbox_key_locks kl ON kl.key = e.key
WHERE e.status = 'pending'
    AND e.next_retry_at <= (now() AT TIME ZONE 'UTC')
    AND (ks.blocked_until IS NULL OR ks.blocked_until <= (now() AT TIME ZONE 'UTC'))
    AND (kl.key IS NULL OR kl.stale_at <= (now() AT TIME ZONE 'UTC'))
ORDER BY e.seq ASC
LIMIT 1;

-- name: GetPendingOutboxEventsByKey :many
SELECT *
FROM outbox_events
WHERE status = 'pending'
  AND key = $1
  AND next_retry_at <= (now() AT TIME ZONE 'UTC')
ORDER BY seq ASC
LIMIT $2
FOR UPDATE SKIP LOCKED;

-- name: MarkOutboxEventsAsSent :many
UPDATE outbox_events
SET
    status = 'sent',
    attempts = attempts + 1,
    last_attempt_at = (now() AT TIME ZONE 'UTC'),
    sent_at = (now() AT TIME ZONE 'UTC')
WHERE id = ANY(sqlc.arg(ids)::uuid[])
RETURNING *;

-- name: MarkOutboxEventsAsFailed :many
UPDATE outbox_events
SET
    status = 'failed',
    attempts = attempts + 1,
    last_attempt_at = (now() AT TIME ZONE 'UTC')
WHERE id = ANY(sqlc.arg(ids)::uuid[])
RETURNING *;

-- name: MarkOutboxEventAsPending :one
UPDATE outbox_events
SET
    status = 'pending',
    attempts = attempts + 1,
    last_attempt_at = (now() AT TIME ZONE 'UTC'),
    next_retry_at = sqlc.arg(next_retry_at)::timestamptz
WHERE id = $1
RETURNING *;
