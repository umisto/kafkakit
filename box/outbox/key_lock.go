package outbox

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/netbill/evebox/box/pgdb"
)

func (b Box) LockOutboxKey(
	ctx context.Context,
	key string,
	owner string,
	nextRetryAt time.Time,
) (bool, error) {
	_, err := b.queries(ctx).TryLockOutboxKey(ctx, pgdb.TryLockOutboxKeyParams{
		Key:     key,
		Owner:   owner,
		StaleAt: nextRetryAt,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (b Box) DeleteOutboxKey(
	ctx context.Context,
	key string,
	owner string,
) error {
	return b.queries(ctx).DeleteOutboxKey(ctx, pgdb.DeleteOutboxKeyParams{
		Key:   key,
		Owner: owner,
	})
}
