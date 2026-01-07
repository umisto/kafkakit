package inbox

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/evebox/pgdb"
)

func (b Box) MarkInboxEventsAsProcessed(
	ctx context.Context,
	ids ...uuid.UUID,
) ([]InboxEvent, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := b.queries(ctx).MarkInboxEventsAsProcessed(ctx, ids)
	if err != nil {
		return nil, err
	}

	events := make([]InboxEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertInboxEvent(row))
	}

	return events, nil
}

func (b Box) MarkInboxEventsAsFailed(
	ctx context.Context,
	ids ...uuid.UUID,
) ([]InboxEvent, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := b.queries(ctx).MarkInboxEventsAsFailed(ctx, ids)
	if err != nil {
		return nil, err
	}

	events := make([]InboxEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertInboxEvent(row))
	}

	return events, nil
}

func (b Box) MarkInboxEventsAsPending(
	ctx context.Context,
	nextRetryAt time.Time,
	ids ...uuid.UUID,
) ([]InboxEvent, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := b.queries(ctx).MarkInboxEventsAsPending(ctx, pgdb.MarkInboxEventsAsPendingParams{
		Ids:         ids,
		NextRetryAt: nextRetryAt,
	})
	if err != nil {
		return nil, err
	}

	events := make([]InboxEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertInboxEvent(row))
	}

	return events, nil
}
