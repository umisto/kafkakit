package inbox

import (
	"context"
	"time"

	"github.com/netbill/evebox/box/pgdb"
)

func (b Box) BlockInboxKeyUntil(
	ctx context.Context,
	key string,
	blockedUntil time.Time,
) error {
	return b.queries(ctx).UpsertInboxKeyStateBlock(ctx, pgdb.UpsertInboxKeyStateBlockParams{
		Key:          key,
		BlockedUntil: blockedUntil,
	})
}

func (b Box) UnblockInboxKey(
	ctx context.Context,
	key string,
) error {
	return b.queries(ctx).ClearInboxKeyStateBlock(ctx, key)
}
