package outbox

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
	"github.com/netbill/evebox/pgdb"
)

func (b Box) CreateOutboxEvent(
	ctx context.Context,
	event OutboxEvent,
) (OutboxEvent, error) {
	row, err := b.queries(ctx).CreateOutboxEvent(ctx, pgdb.CreateOutboxEventParams{
		ID:       event.ID,
		Topic:    event.Topic,
		Key:      event.Key,
		Type:     event.Type,
		Version:  event.Version,
		Producer: event.Producer,
		Payload:  event.Payload,
	})
	if err != nil {
		return OutboxEvent{}, err
	}
	return convertOutboxEvent(row), nil
}

func (b Box) GetOutboxEvent(
	ctx context.Context,
	id uuid.UUID,
) (OutboxEvent, error) {
	row, err := b.queries(ctx).GetOutboxEventByID(ctx, id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return OutboxEvent{}, nil
	case err != nil:
		return OutboxEvent{}, err
	}
	return convertOutboxEvent(row), nil
}
