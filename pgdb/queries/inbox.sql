-- name: CreateInboxEvent :one
INSERT INTO inbox_events (
    id, topic, key, type, version, producer, payload,
    kafka_partition, kafka_offset
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9
)
ON CONFLICT (topic, kafka_partition, kafka_offset)
WHERE kafka_partition IS NOT NULL AND kafka_offset IS NOT NULL
DO NOTHING
RETURNING *;

-- name: GetInboxEventByID :one
SELECT *
FROM inbox_events
WHERE id = $1;

-- name: PickPendingInboxKey :one
SELECT e.key
FROM inbox_events e
LEFT JOIN inbox_key_state ks ON ks.key = e.key
LEFT JOIN inbox_key_locks kl ON kl.key = e.key
WHERE e.status = 'pending'
    AND e.next_retry_at <= (now() AT TIME ZONE 'UTC')
    AND (ks.blocked_until IS NULL OR ks.blocked_until <= (now() AT TIME ZONE 'UTC'))
    AND (kl.key IS NULL OR kl.stale_at <= (now() AT TIME ZONE 'UTC'))
ORDER BY e.seq ASC
    LIMIT 1;

-- name: GetPendingInboxEventsByKey :many
SELECT *
FROM inbox_events
WHERE status = 'pending'
  AND key = $1
  AND next_retry_at <= (now() AT TIME ZONE 'UTC')
ORDER BY seq ASC
LIMIT $2
FOR UPDATE SKIP LOCKED;

-- name: MarkInboxEventsAsProcessed :many
UPDATE inbox_events
SET
    status = 'processed',
    processed_at = (now() AT TIME ZONE 'UTC')
WHERE id = ANY(sqlc.arg(ids)::uuid[])
    RETURNING *;

-- name: MarkInboxEventsAsFailed :many
UPDATE inbox_events
SET
    status = 'failed',
    attempts = attempts + 1,
    last_attempt_at = (now() AT TIME ZONE 'UTC')
WHERE id = ANY(sqlc.arg(ids)::uuid[])
    RETURNING *;

-- name: MarkInboxEventsAsPending :many
UPDATE inbox_events
SET
    status = 'pending',
    attempts = attempts + 1,
    last_attempt_at = (now() AT TIME ZONE 'UTC'),
    next_retry_at = sqlc.arg(next_retry_at)::timestamptz
WHERE id = ANY(sqlc.arg(ids)::uuid[])
    RETURNING *;
