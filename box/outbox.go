package box

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/chains-lab/kafkakit/box/pgdb"
	"github.com/google/uuid"
)

const (
	OutboxStatusPending = "pending"
	OutboxStatusSent    = "sent"
	OutboxStatusFailed  = "failed"
)

func (r *Repository) CreateOutboxEvent(ctx context.Context, event contracts.Message) error {
	payloadBytes, err := json.Marshal(event.Payload)
	if err != nil {
		return err
	}

	now := time.Now().UTC()

	row := pgdb.OutboxEvent{
		ID:           uuid.New(),
		Topic:        event.Topic,
		EventType:    event.EventType,
		EventVersion: int(event.EventVersion),
		Key:          event.Key,
		Payload:      payloadBytes,

		Status:      "pending",
		Attempts:    0,
		NextRetryAt: now,

		CreatedAt: now,
		SentAt:    nil,
	}

	return r.sql.outbox.Insert(ctx, row)
}

func (r *Repository) GetPendingOutboxEvents(ctx context.Context, limit int32) ([]contracts.OutboxEvent, error) {
	now := time.Now().UTC()

	rows, err := r.sql.outbox.New().
		FilterStatus("pending").
		FilterReadyToSend(now).
		OrderByCreatedAtAsc().
		Page(uint64(limit), 0).
		Select(ctx)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return []contracts.OutboxEvent{}, nil
	case err != nil:
		return []contracts.OutboxEvent{}, err
	}

	events := make([]contracts.OutboxEvent, 0, len(rows))
	for _, e := range rows {
		events = append(events, contracts.OutboxEvent{
			ID:           e.ID,
			Topic:        e.Topic,
			EventType:    e.EventType,
			EventVersion: e.EventVersion,
			Key:          e.Key,
			Payload:      e.Payload,
		})
	}

	return events, nil
}

func (r *Repository) MarkOutboxEventsSent(ctx context.Context, ids []uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}

	now := time.Now().UTC()

	return r.sql.outbox.Transaction(ctx, func(ctx context.Context) error {
		for _, id := range ids {
			_, err := r.sql.outbox.New().
				FilterID(id).
				UpdateStatus("sent").
				UpdateSentAt(now).
				Update(ctx)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *Repository) DelayOutboxEvents(ctx context.Context, ids []uuid.UUID, delay time.Duration) error {
	if len(ids) == 0 {
		return nil
	}

	next := time.Now().UTC().Add(delay)

	return r.sql.outbox.Transaction(ctx, func(ctx context.Context) error {
		for _, id := range ids {
			_, err := r.sql.outbox.New().
				FilterID(id).
				AddAttempts().
				UpdateStatus("pending").
				UpdateNextRetryAt(next).
				Update(ctx)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
