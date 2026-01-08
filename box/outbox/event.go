package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/evebox/box/pgdb"
	"github.com/netbill/evebox/header"
	"github.com/segmentio/kafka-go"
)

func (b Box) CreateOutboxEvent(
	ctx context.Context,
	message kafka.Message,
) (Event, error) {
	headers := map[string][]byte{}
	for _, h := range message.Headers {
		headers[h.Key] = h.Value
	}

	eventIDBytes, ok := headers[header.EventID]
	if !ok {
		return Event{}, fmt.Errorf("missing %s header", header.EventID)
	}
	eventID, err := uuid.ParseBytes(eventIDBytes)
	if err != nil {
		return Event{}, fmt.Errorf("invalid %s header", header.EventID)
	}

	eventTypeBytes, ok := headers[header.EventType]
	if !ok {
		return Event{}, fmt.Errorf("missing %s header", header.EventType)
	}

	eventVersionBytes, ok := headers[header.EventVersion]
	if !ok {
		return Event{}, fmt.Errorf("missing %s header", header.EventVersion)
	}
	v64, err := strconv.ParseInt(string(eventVersionBytes), 10, 32)
	if err != nil {
		return Event{}, fmt.Errorf("invalid %s header", header.EventVersion)
	}
	eventVersion := int32(v64)

	producerBytes, ok := headers[header.Producer]
	if !ok {
		return Event{}, fmt.Errorf("missing %s header", header.Producer)
	}

	row, err := b.queries(ctx).CreateOutboxEvent(ctx, pgdb.CreateOutboxEventParams{
		ID:       eventID,
		Topic:    message.Topic,
		Key:      string(message.Key),
		Type:     string(eventTypeBytes),
		Version:  eventVersion,
		Producer: string(producerBytes),
		Payload:  message.Value,
	})
	if err != nil {
		return Event{}, err
	}

	return convertOutboxEvent(row), nil
}

func (b Box) GetPendingOutboxKey(
	ctx context.Context,
) (string, error) {
	key, err := b.queries(ctx).PickPendingOutboxKey(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	return key, nil
}

func (b Box) GetPendingOutboxEvents(
	ctx context.Context,
	key string,
	limit int32,
) ([]Event, error) {
	events, err := b.queries(ctx).GetPendingOutboxEventsByKey(ctx, pgdb.GetPendingOutboxEventsByKeyParams{
		Key:   key,
		Limit: limit,
	})
	if err != nil {
		return nil, err
	}

	if len(events) == 0 {
		return nil, nil
	}

	result := make([]Event, 0, len(events))
	for _, event := range events {
		result = append(result, convertOutboxEvent(event))
	}

	return result, nil
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

func (b Box) MarkOutboxEventsAsSent(
	ctx context.Context,
	ids ...uuid.UUID,
) ([]Event, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := b.queries(ctx).MarkOutboxEventsAsSent(ctx, ids)
	if err != nil {
		return nil, err
	}

	events := make([]Event, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertOutboxEvent(row))
	}

	return events, nil
}

func (b Box) MarkOutboxEventsAsFailed(
	ctx context.Context,
	ids ...uuid.UUID,
) ([]Event, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := b.queries(ctx).MarkOutboxEventsAsFailed(ctx, ids)
	if err != nil {
		return nil, err
	}

	events := make([]Event, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertOutboxEvent(row))
	}

	return events, nil
}

func (b Box) MarkOutboxEventAsPending(
	ctx context.Context,
	nextRetryAt time.Time,
	ID uuid.UUID,
) (Event, error) {
	row, err := b.queries(ctx).MarkOutboxEventAsPending(ctx, pgdb.MarkOutboxEventAsPendingParams{
		ID:          ID,
		NextRetryAt: nextRetryAt,
	})
	if err != nil {
		return Event{}, err
	}

	return convertOutboxEvent(row), nil
}
