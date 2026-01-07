-- name: TryLockOutboxKey :one
INSERT INTO outbox_key_locks(key, owner, locked_at, stale_at)
VALUES (
    sqlc.arg(key)::text,
    sqlc.arg(owner)::text,
    (now() AT TIME ZONE 'UTC'),
    sqlc.arg(stale_at)::timestamptz
)
ON CONFLICT (key) DO UPDATE
SET owner     = EXCLUDED.owner,
    locked_at = EXCLUDED.locked_at,
    stale_at  = EXCLUDED.stale_at
WHERE outbox_key_locks.stale_at <= (now() AT TIME ZONE 'UTC')
RETURNING key, owner, locked_at, stale_at;

-- name: UnlockOutboxKey :exec
DELETE FROM outbox_key_locks
WHERE key = sqlc.arg(key)::text
    AND owner = sqlc.arg(owner)::text;

-- name: DeleteStaleOutboxKeyLocks :exec
DELETE FROM outbox_key_locks
WHERE stale_at <= (now() AT TIME ZONE 'UTC');

-- name: GetOutboxKeyState :one
SELECT key, blocked_until, updated_at
FROM outbox_key_state
WHERE key = sqlc.arg(key)::text;

-- name: UpsertOutboxKeyStateBlock :exec
INSERT INTO outbox_key_state(key, blocked_until, updated_at)
VALUES (
    sqlc.arg(key)::text,
    sqlc.arg(blocked_until)::timestamptz,
    (now() AT TIME ZONE 'UTC')
)
ON CONFLICT (key) DO UPDATE
SET blocked_until = EXCLUDED.blocked_until,
    updated_at = (now() AT TIME ZONE 'UTC');

-- name: ClearOutboxKeyStateBlock :exec
UPDATE outbox_key_state
SET blocked_until = NULL,
    updated_at = (now() AT TIME ZONE 'UTC')
WHERE key = sqlc.arg(key)::text;

