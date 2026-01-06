package box

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/evebox/box/pgdb"
	"github.com/netbill/evebox/header"
	"github.com/segmentio/kafka-go"
)

const (
	OutboxStatusPending    = "pending"
	OutboxStatusProcessing = "processing"
	OutboxStatusSent       = "sent"
	OutboxStatusFailed     = "failed"
)

type OutboxEvent struct {
	ID          uuid.UUID
	Seq         int64
	Topic       string
	Key         string
	Type        string
	Version     int32
	Producer    string
	Payload     json.RawMessage
	Status      string
	Attempts    int32
	CreatedAt   time.Time
	NextRetryAt *time.Time
	SentAt      *time.Time
}

func (e OutboxEvent) ToMessage() kafka.Message {
	return kafka.Message{
		Topic: e.Topic,
		Key:   []byte(e.Key),
		Value: e.Payload,
		Headers: []kafka.Header{
			{
				Key:   header.EventID,
				Value: []byte(e.ID.String()),
			},
			{
				Key:   header.EventType,
				Value: []byte(e.Type),
			},
			{
				Key:   header.EventVersion,
				Value: []byte(strconv.FormatInt(int64(e.Version), 10)),
			},
			{
				Key:   header.Producer,
				Value: []byte(e.Producer),
			},
			{
				Key:   header.ContentType,
				Value: []byte("application/json"),
			},
		},
	}
}

func (e OutboxEvent) IsNil() bool {
	return e.ID == uuid.Nil
}

func (b Box) CreateOutboxEvent(
	ctx context.Context,
	message kafka.Message,
) (OutboxEvent, error) {
	key := message.Key
	topic := message.Topic
	value := message.Value

	headers := map[string][]byte{}
	for _, h := range message.Headers {
		headers[h.Key] = h.Value
	}

	eventIDByte, ok := headers[header.EventID]
	if !ok {
		return OutboxEvent{}, fmt.Errorf("missing %s header", header.EventID)
	}
	eventID, err := uuid.ParseBytes(eventIDByte)
	if err != nil {
		return OutboxEvent{}, fmt.Errorf("invalid %s header", header.EventID)
	}

	eventType, ok := headers[header.EventType]
	if !ok {
		return OutboxEvent{}, fmt.Errorf("missing %s header", header.EventType)
	}

	eventVersionBytes, ok := headers[header.EventVersion]
	if !ok {
		return OutboxEvent{}, fmt.Errorf("missing %s header", header.EventVersion)
	}
	v64, err := strconv.ParseInt(string(eventVersionBytes), 10, 32)
	if err != nil {
		return OutboxEvent{}, fmt.Errorf("invalid %s header", header.EventVersion)
	}
	eventVersion := int32(v64)

	producer, ok := headers[header.Producer]
	if !ok {
		return OutboxEvent{}, fmt.Errorf("missing %s header", header.Producer)
	}

	stmt := pgdb.CreateOutboxEventParams{
		ID:          eventID,
		Topic:       topic,
		Key:         string(key),
		Type:        string(eventType),
		Version:     eventVersion,
		Producer:    string(producer),
		Payload:     value,
		Status:      OutboxStatusPending,
		NextRetryAt: sql.NullTime{Valid: true, Time: time.Now().UTC()},
	}

	res, err := b.queries(ctx).CreateOutboxEvent(ctx, stmt)
	if err != nil {
		return OutboxEvent{}, fmt.Errorf("create outbox event: %w", err)
	}

	return pgdbOutboxEvent(res), nil
}

func (b Box) GetOutboxEventByID(ctx context.Context, id uuid.UUID) (OutboxEvent, error) {
	res, err := b.queries(ctx).GetOutboxEventByID(ctx, id)
	if err != nil {
		return OutboxEvent{}, fmt.Errorf("get outbox event by id: %w", err)
	}

	return pgdbOutboxEvent(res), nil
}

func (b Box) GetPendingOutboxEvents(ctx context.Context, limit int32) ([]OutboxEvent, error) {
	res, err := b.queries(ctx).GetPendingOutboxEvents(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("get pending outbox events: %w", err)
	}

	events := make([]OutboxEvent, 0, len(res))
	for _, e := range res {
		ele := OutboxEvent{
			ID:        e.ID,
			Seq:       e.Seq,
			Topic:     e.Topic,
			Key:       e.Key,
			Type:      e.Type,
			Version:   e.Version,
			Producer:  e.Producer,
			Payload:   e.Payload,
			Status:    string(e.Status),
			Attempts:  e.Attempts,
			CreatedAt: e.CreatedAt,
		}

		if e.NextRetryAt.Valid {
			ele.NextRetryAt = &e.NextRetryAt.Time
		}

		if e.SentAt.Valid {
			ele.SentAt = &e.SentAt.Time
		}

		events = append(events, ele)
	}

	return events, nil
}

func (b Box) MarkOutboxEventsSent(ctx context.Context, ids []uuid.UUID) ([]OutboxEvent, error) {
	res, err := b.queries(ctx).MarkOutboxEventsAsSent(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("mark outbox events as sent: %w", err)
	}

	events := make([]OutboxEvent, 0, len(res))
	for _, e := range res {
		events = append(events, pgdbOutboxEvent(e))
	}

	return events, nil
}

func (b Box) MarkOutboxEventsAsFailed(ctx context.Context, ids []uuid.UUID) ([]OutboxEvent, error) {
	res, err := b.queries(ctx).MarkOutboxEventsAsFailed(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("mark outbox events as failed: %w", err)
	}

	events := make([]OutboxEvent, 0, len(res))
	for _, e := range res {
		events = append(events, pgdbOutboxEvent(e))
	}

	return events, nil
}

func (b Box) MarkOutboxEventsAsPending(ctx context.Context, ids []uuid.UUID, delay time.Duration) ([]OutboxEvent, error) {
	res, err := b.queries(ctx).MarkOutboxEventsAsPending(ctx, pgdb.MarkOutboxEventsAsPendingParams{
		Ids:         ids,
		NextRetryAt: time.Now().UTC().Add(delay),
	})
	if err != nil {
		return nil, fmt.Errorf("delay outbox events retry: %w", err)
	}

	events := make([]OutboxEvent, 0, len(res))
	for _, e := range res {
		events = append(events, pgdbOutboxEvent(e))
	}

	return events, nil
}

func pgdbOutboxEvent(e pgdb.OutboxEvent) OutboxEvent {
	res := OutboxEvent{
		ID:        e.ID,
		Seq:       e.Seq,
		Topic:     e.Topic,
		Key:       e.Key,
		Type:      e.Type,
		Version:   e.Version,
		Producer:  e.Producer,
		Payload:   e.Payload,
		Status:    string(e.Status),
		Attempts:  e.Attempts,
		CreatedAt: e.CreatedAt,
	}

	if e.NextRetryAt.Valid {
		res.NextRetryAt = &e.NextRetryAt.Time
	}

	if e.SentAt.Valid {
		res.SentAt = &e.SentAt.Time
	}

	return res
}
