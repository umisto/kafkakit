package inbox

import (
	"context"
	"database/sql"
	"time"

	"github.com/netbill/evebox/pgdb"
)

func (b Box) LockInboxKey(
	ctx context.Context,
	key string,
	owner string,
	ttl time.Duration,
) (bool, error) {
	_, err := b.queries(ctx).TryLockInboxKey(ctx, pgdb.TryLockInboxKeyParams{
		Key:     key,
		Owner:   owner,
		StaleAt: time.Now().UTC().Add(ttl),
	})
	if err == sql.ErrNoRows {
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
