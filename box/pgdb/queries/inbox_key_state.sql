-- name: GetInboxKeyState :one
SELECT key, blocked_until, updated_at
FROM inbox_key_state
WHERE key = sqlc.arg(key)::text;

-- name: UpsertInboxKeyStateBlock :exec
INSERT INTO inbox_key_state(key, blocked_until, updated_at)
VALUES (
    sqlc.arg(key)::text,
    sqlc.arg(blocked_until)::timestamptz,
    (now() AT TIME ZONE 'UTC')
)
ON CONFLICT (key) DO UPDATE
SET blocked_until = EXCLUDED.blocked_until,
    updated_at    = (now() AT TIME ZONE 'UTC');

-- name: ClearInboxKeyStateBlock :exec
UPDATE inbox_key_state
SET blocked_until = NULL,
    updated_at    = (now() AT TIME ZONE 'UTC')
WHERE key = sqlc.arg(key)::text;

-- name: ListBlockedInboxKeys :many
SELECT key, blocked_until, updated_at
FROM inbox_key_state
WHERE blocked_until IS NOT NULL
    AND blocked_until > (now() AT TIME ZONE 'UTC')
ORDER BY blocked_until ASC;
