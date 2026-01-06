package box

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/evebox/box/pgdb"
	"github.com/netbill/evebox/header"
	"github.com/netbill/logium"
	"github.com/segmentio/kafka-go"
)

type InboxStatus string

func (e InboxStatus) String() string {
	return string(e)
}

func (e InboxStatus) IsValid() bool {
	switch e {
	case InboxStatusPending, InboxStatusProcessed, InboxStatusProcessing, InboxStatusFailed:
		return true
	default:
		return false
	}
}

func (e InboxStatus) pgdb() pgdb.InboxEventStatus {
	return pgdb.InboxEventStatus(e.String())
}

const (
	InboxStatusPending    InboxStatus = "pending"
	InboxStatusProcessed  InboxStatus = "processed"
	InboxStatusProcessing InboxStatus = "processing"
	InboxStatusFailed     InboxStatus = "failed"
)

type InboxEvent struct {
	ID          uuid.UUID
	Seq         int64
	Topic       string
	Key         string
	Type        string
	Version     int32
	Producer    string
	Payload     json.RawMessage
	Status      InboxStatus
	Attempts    int32
	CreatedAt   time.Time
	NextRetryAt *time.Time
	ProcessedAt *time.Time
}

func (e InboxEvent) ToMessage() kafka.Message {
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

func (e InboxEvent) IsNil() bool {
	return e.ID == uuid.Nil
}

func (b Box) CreateInboxEvent(
	ctx context.Context,
	message kafka.Message,
) (InboxEvent, error) {
	key := message.Key
	topic := message.Topic
	value := message.Value

	headers := map[string][]byte{}
	for _, h := range message.Headers {
		headers[h.Key] = h.Value
	}

	eventIDByte, ok := headers[header.EventID]
	if !ok {
		return InboxEvent{}, fmt.Errorf("missing %s header", header.EventID)
	}
	eventID, err := uuid.ParseBytes(eventIDByte)
	if err != nil {
		return InboxEvent{}, fmt.Errorf("invalid %s header", header.EventID)
	}

	eventType, ok := headers[header.EventType]
	if !ok {
		return InboxEvent{}, fmt.Errorf("missing %s header", header.EventType)
	}

	eventVersionBytes, ok := headers[header.EventVersion]
	if !ok {
		return InboxEvent{}, fmt.Errorf("missing %s header", header.EventVersion)
	}
	v64, err := strconv.ParseInt(string(eventVersionBytes), 10, 32)
	if err != nil {
		return InboxEvent{}, fmt.Errorf("invalid %s header", header.EventVersion)
	}
	eventVersion := int32(v64)

	producer, ok := headers[header.Producer]
	if !ok {
		return InboxEvent{}, fmt.Errorf("missing %s header", header.Producer)
	}

	stmt := pgdb.CreateInboxEventParams{
		ID:          eventID,
		Topic:       topic,
		Key:         string(key),
		Type:        string(eventType),
		Version:     eventVersion,
		Producer:    string(producer),
		Payload:     value,
		Status:      InboxStatusPending.pgdb(),
		NextRetryAt: sql.NullTime{Time: time.Now().UTC(), Valid: true},
	}

	res, err := b.queries(ctx).CreateInboxEvent(ctx, stmt)
	if err != nil {
		return InboxEvent{}, err
	}

	return pgdbInboxEvent(res), nil
}

func (b Box) GetInboxEventByID(
	ctx context.Context,
	id uuid.UUID,
) (InboxEvent, error) {
	res, err := b.queries(ctx).GetInboxEventByID(ctx, id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return InboxEvent{}, nil
	case err != nil:
		return InboxEvent{}, err
	}

	return pgdbInboxEvent(res), nil
}

func (b Box) GetPendingInboxEvents(
	ctx context.Context,
	limit int32,
) ([]InboxEvent, error) {
	res, err := b.queries(ctx).GetPendingInboxEvents(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("get pending inbox events: %w", err)
	}

	events := make([]InboxEvent, 0, len(res))
	for _, e := range res {
		ele := InboxEvent{
			ID:        e.ID,
			Seq:       e.Seq,
			Topic:     e.Topic,
			Key:       e.Key,
			Type:      e.Type,
			Version:   e.Version,
			Producer:  e.Producer,
			Payload:   e.Payload,
			Status:    InboxStatus(e.Status),
			Attempts:  e.Attempts,
			CreatedAt: e.CreatedAt,
		}

		if e.NextRetryAt.Valid {
			ele.NextRetryAt = &e.NextRetryAt.Time
		}
		if e.ProcessedAt.Valid {
			ele.ProcessedAt = &e.ProcessedAt.Time
		}

		events = append(events, ele)
	}

	return events, nil
}

func (b Box) MarkInboxEventsAsProcessed(
	ctx context.Context,
	ids []uuid.UUID,
) ([]InboxEvent, error) {
	res, err := b.queries(ctx).MarkInboxEventsAsProcessed(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("mark inbox events as processed: %w", err)
	}

	events := make([]InboxEvent, 0, len(res))
	for _, e := range res {
		events = append(events, pgdbInboxEvent(e))
	}

	return events, nil
}

func (b Box) MarkInboxEventsAsFailed(
	ctx context.Context,
	ids []uuid.UUID,
) ([]InboxEvent, error) {
	res, err := b.queries(ctx).MarkInboxEventsAsFailed(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("mark inbox events as failed: %w", err)
	}

	events := make([]InboxEvent, 0, len(res))
	for _, e := range res {
		events = append(events, pgdbInboxEvent(e))
	}

	return events, nil
}

func (b Box) MarkInboxEventsAsPending(
	ctx context.Context,
	ids []uuid.UUID,
	delay time.Duration,
) ([]InboxEvent, error) {
	res, err := b.queries(ctx).MarkInboxEventsAsPending(ctx, pgdb.MarkInboxEventsAsPendingParams{
		Ids:         ids,
		NextRetryAt: time.Now().UTC().Add(delay),
	})
	if err != nil {
		return nil, fmt.Errorf("delay inbox events: %w", err)
	}

	events := make([]InboxEvent, 0, len(res))
	for _, e := range res {
		events = append(events, pgdbInboxEvent(e))
	}

	return events, nil
}

func (b Box) UpdateInboxEventStatus(
	ctx context.Context,
	id uuid.UUID,
	status InboxStatus,
) (InboxEvent, error) {
	res, err := b.queries(ctx).UpdateInboxEventStatus(ctx, pgdb.UpdateInboxEventStatusParams{
		ID:     id,
		Status: pgdb.InboxEventStatus(status),
	})
	if err != nil {
		return InboxEvent{}, fmt.Errorf("update inbox event status: %w", err)
	}

	return pgdbInboxEvent(res), nil
}

type InboxHandler func(ctx context.Context, event InboxEvent) InboxStatus

func (b Box) WriteAndHandle(
	ctx context.Context,
	log logium.Logger,
	message kafka.Message,
	handler InboxHandler,
) error {
	return b.Transaction(ctx, func(ctx context.Context) error {
		eventInBox, err := b.CreateInboxEvent(ctx, message)
		if err != nil {
			log.Errorf("failed to upsert inbox event for account %s: %v", string(message.Key), err)
			return err
		}

		if _, err = b.UpdateInboxEventStatus(ctx, eventInBox.ID, handler(ctx, eventInBox)); err != nil {
			log.Errorf("failed to update inbox event status for key %s, id: %s, error: %v", eventInBox.Key, eventInBox.ID, err)
			return err
		}

		return nil
	})
}

func pgdbInboxEvent(e pgdb.InboxEvent) InboxEvent {
	res := InboxEvent{
		ID:        e.ID,
		Seq:       e.Seq,
		Topic:     e.Topic,
		Key:       e.Key,
		Type:      e.Type,
		Version:   e.Version,
		Producer:  e.Producer,
		Payload:   e.Payload,
		Status:    InboxStatus(e.Status),
		Attempts:  e.Attempts,
		CreatedAt: e.CreatedAt,
	}
	if e.NextRetryAt.Valid {
		res.NextRetryAt = &e.NextRetryAt.Time
	}
	if e.ProcessedAt.Valid {
		res.ProcessedAt = &e.ProcessedAt.Time
	}

	return res
}
