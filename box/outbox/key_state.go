package outbox

import (
	"context"
	"time"

	"github.com/netbill/evebox/box/pgdb"
)

func (b Box) BlockOutboxKeyUntil(
	ctx context.Context,
	key string,
	blockedUntil time.Time,
) error {
	return b.queries(ctx).UpsertOutboxKeyStateBlock(ctx, pgdb.UpsertOutboxKeyStateBlockParams{
		Key:          key,
		BlockedUntil: blockedUntil,
	})
}

func (b Box) UnblockOutboxKey(
	ctx context.Context,
	key string,
) error {
	return b.queries(ctx).ClearOutboxKeyStateBlock(ctx, key)
}
