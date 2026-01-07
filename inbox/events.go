package inbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/evebox/header"
	"github.com/netbill/evebox/pgdb"
	"github.com/segmentio/kafka-go"
)

func (b Box) CreateInboxEvent(
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

	res, err := b.queries(ctx).CreateInboxEvent(ctx, pgdb.CreateInboxEventParams{
		ID:             eventID,
		Topic:          message.Topic,
		Key:            string(message.Key),
		Type:           string(eventTypeBytes),
		Version:        eventVersion,
		Producer:       string(producerBytes),
		Payload:        message.Value,
		KafkaPartition: sql.NullInt32{Int32: int32(message.Partition), Valid: true},
		KafkaOffset:    sql.NullInt64{Int64: message.Offset, Valid: true},
	})
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Event{}, nil
	case err != nil:
		return Event{}, err
	}

	return convertInboxEvent(res), nil
}

func (b Box) PickPendingInboxKey(
	ctx context.Context,
) (string, error) {
	key, err := b.queries(ctx).PickPendingInboxKey(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return key, nil
}

func (b Box) GetPendingInboxEvents(
	ctx context.Context,
	key string,
	limit int32,
) ([]Event, error) {
	rows, err := b.queries(ctx).GetPendingInboxEventsByKey(ctx, pgdb.GetPendingInboxEventsByKeyParams{
		Key:   key,
		Limit: limit,
	})
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		return nil, nil
	}

	events := make([]Event, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertInboxEvent(row))
	}

	return events, nil
}

func (b Box) GetInboxEvent(
	ctx context.Context,
	ID uuid.UUID,
) (Event, error) {
	res, err := b.queries(ctx).GetInboxEventByID(ctx, ID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Event{}, nil
	case err != nil:
		return Event{}, err
	}

	return convertInboxEvent(res), nil
}

func (b Box) MarkInboxEventsAsProcessed(
	ctx context.Context,
	ids ...uuid.UUID,
) ([]Event, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := b.queries(ctx).MarkInboxEventsAsProcessed(ctx, ids)
	if err != nil {
		return nil, err
	}

	events := make([]Event, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertInboxEvent(row))
	}

	return events, nil
}

func (b Box) MarkInboxEventsAsFailed(
	ctx context.Context,
	ids ...uuid.UUID,
) ([]Event, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rows, err := b.queries(ctx).MarkInboxEventsAsFailed(ctx, ids)
	if err != nil {
		return nil, err
	}

	events := make([]Event, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertInboxEvent(row))
	}

	return events, nil
}

func (b Box) MarkInboxEventsAsPending(
	ctx context.Context,
	nextRetryAt time.Time,
	ids ...uuid.UUID,
) ([]Event, error) {
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

	events := make([]Event, 0, len(rows))
	for _, row := range rows {
		events = append(events, convertInboxEvent(row))
	}

	return events, nil
}
