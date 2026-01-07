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
	event Event,
) (Event, error) {
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
		return Event{}, err
	}
	return convertOutboxEvent(row), nil
}

func (b Box) GetOutboxEvent(
	ctx context.Context,
	id uuid.UUID,
) (Event, error) {
	row, err := b.queries(ctx).GetOutboxEventByID(ctx, id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Event{}, nil
	case err != nil:
		return Event{}, err
	}
	return convertOutboxEvent(row), nil
}
