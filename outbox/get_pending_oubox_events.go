package outbox

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/netbill/evebox/pgdb"
)

func (b Box) GetPendingOutboxEvents(
	ctx context.Context,
	owner string,
	limit int32,
) (key string, events []Event, err error) {

	key, err = b.queries(ctx).PickPendingOutboxKey(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}

	_, err = b.queries(ctx).TryLockOutboxKey(ctx, pgdb.TryLockOutboxKeyParams{
		Key:     key,
		Owner:   owner,
		StaleAt: time.Now().UTC().Add(6 * time.Hour),
	})
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}

	rows, err := b.queries(ctx).GetPendingOutboxEventsByKey(ctx, pgdb.GetPendingOutboxEventsByKeyParams{
		Key:   key,
		Limit: limit,
	})
	if err != nil {
		return "", nil, err
	}

	if len(rows) == 0 {
		return key, nil, nil
	}

	events = make([]Event, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertOutboxEvent(row))
	}

	return key, events, nil
}
