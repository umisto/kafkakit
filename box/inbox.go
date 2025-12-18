package box

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/chains-lab/kafkakit/box/pgdb"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const (
	InboxStatusPending   = "pending"
	InboxStatusProcessed = "processed"
	InboxStatusFailed    = "failed"
)

type InboxEvent struct {
	ID          uuid.UUID
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
	ProcessedAt *time.Time
}

func (e InboxEvent) ToMessage() kafka.Message {
	return kafka.Message{
		Topic: e.Topic,
		Key:   []byte(e.Key),
		Value: e.Payload,
		Headers: []kafka.Header{
			{
				Key:   "event_id",
				Value: []byte(e.ID.String()),
			},
			{
				Key:   "event_type",
				Value: []byte(e.Type),
			},
			{
				Key:   "event_version",
				Value: []byte(strconv.FormatInt(int64(e.Version), 10)),
			},
			{
				Key:   "producer",
				Value: []byte(e.Producer),
			},
			{
				Key:   "content_type",
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
	status string,
	message kafka.Message,
) (InboxEvent, error) {
	key := message.Key
	topic := message.Topic
	value := message.Value

	headers := map[string][]byte{}
	for _, h := range message.Headers {
		headers[h.Key] = h.Value
	}

	eventIDByte, ok := headers["event_id"]
	if !ok {
		return InboxEvent{}, errors.New("missing event_id header")
	}
	eventID, err := uuid.ParseBytes(eventIDByte)
	if err != nil {
		return InboxEvent{}, errors.New("invalid event_id header")
	}

	eventType, ok := headers["event_type"]
	if !ok {
		return InboxEvent{}, errors.New("missing event_type header")
	}

	eventVersionBytes, ok := headers["event_version"]
	if !ok {
		return InboxEvent{}, errors.New("missing event_version header")
	}

	producer, ok := headers["producer"]
	if !ok {
		return InboxEvent{}, errors.New("missing producer header")
	}

	var eventVersion int32
	if err := json.Unmarshal(eventVersionBytes, &eventVersion); err != nil {
		return InboxEvent{}, errors.New("invalid event_version header")
	}

	stmt := pgdb.CreateInboxEventParams{
		ID:       eventID,
		Topic:    topic,
		Key:      string(key),
		Type:     string(eventType),
		Version:  eventVersion,
		Producer: string(producer),
		Payload:  value,
		Status:   pgdb.InboxEventStatus(status),
	}

	switch status {
	case InboxStatusPending:
		stmt.NextRetryAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}

	case InboxStatusProcessed:
		stmt.ProcessedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
		stmt.Attempts = 1
	}

	res, err := b.queries.CreateInboxEvent(ctx, stmt)
	if err != nil {
		return InboxEvent{}, err
	}

	return pgdbInboxEvent(res), nil
}

func (b Box) GetInboxEventByID(
	ctx context.Context,
	id uuid.UUID,
) (InboxEvent, error) {
	res, err := b.queries.GetInboxEventByID(ctx, id)
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
	res, err := b.queries.GetPendingInboxEvents(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("get pending inbox events: %w", err)
	}

	events := make([]InboxEvent, 0, len(res))
	for _, e := range res {
		events = append(events, pgdbInboxEvent(e))
	}

	return events, nil
}

func (b Box) MarkInboxEventsAsProcessed(
	ctx context.Context,
	ids []uuid.UUID,
) ([]InboxEvent, error) {
	res, err := b.queries.MarkInboxEventsAsProcessed(ctx, ids)
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
	res, err := b.queries.MarkInboxEventsAsFailed(ctx, ids)
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
	res, err := b.queries.MarInboxEventsAsPending(ctx, pgdb.MarInboxEventsAsPendingParams{
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

func pgdbInboxEvent(e pgdb.InboxEvent) InboxEvent {
	res := InboxEvent{
		ID:        e.ID,
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
	if e.ProcessedAt.Valid {
		res.ProcessedAt = &e.ProcessedAt.Time
	}

	return res
}
