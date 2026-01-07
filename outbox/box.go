package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/evebox/header"
	"github.com/netbill/evebox/pgdb"
	"github.com/netbill/pgx"
	"github.com/segmentio/kafka-go"
)

type Box struct {
	db *sql.DB
}

func New(db *sql.DB) Box {
	return Box{
		db: db,
	}
}

func (b Box) queries(ctx context.Context) *pgdb.Queries {
	return pgdb.New(pgx.Exec(b.db, ctx))
}

func (b Box) Transaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return pgx.Transaction(b.db, ctx, fn)
}

type EventStatus string

func (e EventStatus) String() string {
	return string(e)
}

func (e EventStatus) IsValid() bool {
	switch e {
	case StatusPending, StatusSent, StatusFailed:
		return true
	default:
		return false
	}
}

func (e EventStatus) pgdb() pgdb.OutboxEventStatus {
	return pgdb.OutboxEventStatus(e.String())
}

const (
	StatusPending EventStatus = "pending"
	StatusSent    EventStatus = "sent"
	StatusFailed  EventStatus = "failed"
)

type Event struct {
	ID            uuid.UUID
	Seq           int64
	Topic         string
	Key           string
	Type          string
	Version       int32
	Producer      string
	Payload       json.RawMessage
	Status        EventStatus
	Attempts      int32
	LastAttemptAt *time.Time
	CreatedAt     time.Time
	NextRetryAt   time.Time
	SentAt        *time.Time
}

func convertOutboxEvent(oe pgdb.OutboxEvent) Event {
	res := Event{
		ID:          oe.ID,
		Seq:         oe.Seq,
		Topic:       oe.Topic,
		Key:         oe.Key,
		Type:        oe.Type,
		Version:     oe.Version,
		Producer:    oe.Producer,
		Payload:     oe.Payload,
		Status:      EventStatus(oe.Status),
		Attempts:    oe.Attempts,
		CreatedAt:   oe.CreatedAt,
		NextRetryAt: oe.NextRetryAt,
	}
	if oe.LastAttemptAt.Valid {
		val := oe.LastAttemptAt.Time
		res.LastAttemptAt = &val
	}
	if oe.SentAt.Valid {
		val := oe.SentAt.Time
		res.SentAt = &val
	}
	return res
}

func (e Event) ToMessage() kafka.Message {
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
