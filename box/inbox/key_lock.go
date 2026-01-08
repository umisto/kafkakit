package inbox

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/netbill/evebox/box/pgdb"
)

func (b Box) LockInboxKey(
	ctx context.Context,
	key string,
	owner string,
	nextRetryAt time.Time,
) (bool, error) {
	_, err := b.queries(ctx).TryLockInboxKey(ctx, pgdb.TryLockInboxKeyParams{
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

func (b Box) UnlockInboxKey(
	ctx context.Context,
	key string,
	owner string,
) error {
	return b.queries(ctx).UnlockInboxKey(ctx, pgdb.UnlockInboxKeyParams{
		Key:   key,
		Owner: owner,
	})
}
