package outbox

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/evebox/pgdb"
)

func (b Box) MarkOutboxEventsAsSent(
	ctx context.Context,
	ids ...uuid.UUID,
) ([]OutboxEvent, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := b.queries(ctx).MarkOutboxEventsAsSent(ctx, ids)
	if err != nil {
		return nil, err
	}

	events := make([]OutboxEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertOutboxEvent(row))
	}

	return events, nil
}

func (b Box) MarkOutboxEventsAsPending(
	ctx context.Context,
	nextRetryAt time.Time,
	ids ...uuid.UUID,
) ([]OutboxEvent, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := b.queries(ctx).MarkOutboxEventsAsPending(ctx, pgdb.MarkOutboxEventsAsPendingParams{
		Ids:         ids,
		NextRetryAt: nextRetryAt,
	})
	if err != nil {
		return nil, err
	}

	events := make([]OutboxEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertOutboxEvent(row))
	}

	return events, nil
}

func (b Box) MarkOutboxEventsAsFailed(
	ctx context.Context,
	ids ...uuid.UUID,
) ([]OutboxEvent, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := b.queries(ctx).MarkOutboxEventsAsFailed(ctx, ids)
	if err != nil {
		return nil, err
	}

	events := make([]OutboxEvent, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertOutboxEvent(row))
	}

	return events, nil
}
