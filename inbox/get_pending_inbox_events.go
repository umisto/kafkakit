package inbox

import (
	"context"
	"database/sql"
	"time"

	"github.com/netbill/evebox/pgdb"
)

func (b Box) GetPendingInboxEvents(
	ctx context.Context,
	owner string,
	limit int32,
) (key string, events []InboxEvent, err error) {
	key, err = b.queries(ctx).PickPendingInboxKey(ctx)
	if err == sql.ErrNoRows {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}

	_, err = b.queries(ctx).TryLockInboxKey(ctx, pgdb.TryLockInboxKeyParams{
		Key:     key,
		Owner:   owner,
		StaleAt: time.Now().UTC().Add(6 * time.Hour),
	})
	if err == sql.ErrNoRows {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, err
	}

	rows, err := b.queries(ctx).GetPendingInboxEventsByKey(ctx, pgdb.GetPendingInboxEventsByKeyParams{
		Key:   key,
		Limit: limit,
	})
	if err != nil {
		return "", nil, err
	}

	if len(rows) == 0 {
		return key, nil, nil
	}

	events = make([]InboxEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertInboxEvent(row))
	}

	return key, events, nil
}
