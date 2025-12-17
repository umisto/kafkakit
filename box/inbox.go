package box

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/chains-lab/kafkakit"
	"github.com/chains-lab/kafkakit/box/pgdb"
	"github.com/google/uuid"
)

const (
	InboxEventStatusPending   = "pending"
	InboxEventStatusProcessed = "processed"
	InboxEventStatusFailed    = "failed"
)

type InboxEvent struct {
	ID           uuid.UUID
	Topic        string
	EventType    string
	EventVersion uint
	Key          string
	Payload      json.RawMessage
	Status       string
	Attempts     uint
	CreatedAt    time.Time
	NextRetryAt  *time.Time
	ProcessedAt  *time.Time
}

func (e InboxEvent) ToMessage() kafkakit.Message {
	return kafkakit.Message{
		Topic:        e.Topic,
		EventType:    e.EventType,
		EventVersion: e.EventVersion,
		Key:          e.Key,
		Payload:      e.Payload,
	}
}

func (e InboxEvent) IsNil() bool {
	return e.ID == uuid.Nil
}

type CreateInboxEventParams struct {
	Event   kafkakit.Message
	Status  string
	RetryAt time.Duration
}

func (r *Repository) CreateInboxEvent(
	ctx context.Context,
	params CreateInboxEventParams,
) (InboxEvent, error) {
	stmt := pgdb.InboxEvent{
		ID:           uuid.New(),
		Topic:        params.Event.Topic,
		EventType:    params.Event.EventType,
		EventVersion: params.Event.EventVersion,
		Key:          params.Event.Key,
		Payload:      params.Event.Payload,
		Status:       params.Status,
		CreatedAt:    time.Now().UTC(),
	}

	switch params.Status {
	case InboxEventStatusPending:
		stmt.NextRetryAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
	case InboxEventStatusProcessed:
		stmt.ProcessedAt = sql.NullTime{Time: time.Now().UTC(), Valid: true}
	}

	res, err := r.Inbox().Insert(ctx, stmt)
	if err != nil {
		return InboxEvent{}, err
	}

	return pgdbInboxEvent(res), nil
}

func (r *Repository) GetInboxEvent(
	ctx context.Context,
	id uuid.UUID,
) (InboxEvent, error) {
	res, err := r.Inbox().FilterID(id).Get(ctx)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return InboxEvent{}, nil
	case err != nil:
		return InboxEvent{}, err
	}

	return pgdbInboxEvent(res), nil
}

func (r *Repository) GetInboxEventByKey(
	ctx context.Context,
	key string,
	topic string,
	eventType string,
) (InboxEvent, error) {
	res, err := r.Inbox().
		FilterKey(key).
		FilterTopic(topic).
		FilterEventType(eventType).
		Get(ctx)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return InboxEvent{}, nil
	case err != nil:
		return InboxEvent{}, err
	}

	return pgdbInboxEvent(res), nil
}

func (r *Repository) GetPendingInboxEvents(
	ctx context.Context,
	limit uint,
) ([]InboxEvent, error) {
	rows, err := r.Inbox().New().
		FilterStatus(InboxEventStatusPending).
		Page(limit, 0).
		Select(ctx)
	if err != nil {
		return nil, err
	}

	events := make([]InboxEvent, len(rows))
	for i, e := range rows {
		events[i] = pgdbInboxEvent(e)
	}

	return events, nil
}

func (r *Repository) DelayInboxEvents(
	ctx context.Context,
	ids []uuid.UUID,
	delay time.Duration,
) error {
	_, err := r.Inbox().FilterID(ids...).AddAttempts().UpdateNextRetryAt(sql.NullTime{
		Time:  time.Now().UTC().Add(delay),
		Valid: true,
	}).UpdateOne(ctx)
	return err
}

func (r *Repository) MarkInboxEventsAsProcessed(
	ctx context.Context,
	ids []uuid.UUID,
) error {
	_, err := r.Inbox().FilterID(ids...).AddAttempts().UpdateStatus(InboxEventStatusProcessed).UpdateOne(ctx)
	return err
}

func (r *Repository) MarkInboxEventsAsFailed(
	ctx context.Context,
	ids []uuid.UUID,
) error {
	_, err := r.Inbox().FilterID(ids...).AddAttempts().UpdateStatus(InboxEventStatusFailed).UpdateOne(ctx)
	return err
}

func pgdbInboxEvent(e pgdb.InboxEvent) InboxEvent {
	res := InboxEvent{
		ID:           e.ID,
		Topic:        e.Topic,
		EventType:    e.EventType,
		EventVersion: e.EventVersion,
		Key:          e.Key,
		Payload:      e.Payload,
		Status:       e.Status,
		Attempts:     e.Attempts,
		CreatedAt:    e.CreatedAt,
	}
	if e.NextRetryAt.Valid {
		res.NextRetryAt = &e.NextRetryAt.Time
	}
	if e.ProcessedAt.Valid {
		res.ProcessedAt = &e.ProcessedAt.Time
	}

	return res
}
