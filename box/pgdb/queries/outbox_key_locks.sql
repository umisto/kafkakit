-- name: TryLockOutboxKey :one
INSERT INTO outbox_key_locks(key, owner, locked_at, stale_at)
SELECT
    sqlc.arg(key)::text,
    sqlc.arg(owner)::text,
    (now() AT TIME ZONE 'UTC'),
    sqlc.arg(stale_at)::timestamptz
WHERE NOT EXISTS (
    SELECT 1
    FROM outbox_key_state ks
    WHERE ks.key = sqlc.arg(key)::text
        AND ks.blocked_until IS NOT NULL
        AND ks.blocked_until > (now() AT TIME ZONE 'UTC')
)
ON CONFLICT (key) DO UPDATE
SET owner     = EXCLUDED.owner,
    locked_at = EXCLUDED.locked_at,
    stale_at  = EXCLUDED.stale_at
WHERE outbox_key_locks.stale_at <= (now() AT TIME ZONE 'UTC')
RETURNING key, owner, locked_at, stale_at;

-- name: DeleteOutboxKey :exec
DELETE FROM outbox_key_locks
WHERE "key" = sqlc.arg(key)::text
  AND owner = sqlc.arg(owner)::text;

-- name: DeleteStaleOutboxKeyLocks :exec
DELETE FROM outbox_key_locks
WHERE stale_at <= (now() AT TIME ZONE 'UTC');
