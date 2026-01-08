package inbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/evebox/box/pgdb"
	"github.com/netbill/evebox/header"
	"github.com/netbill/pgx"
	"github.com/segmentio/kafka-go"
)

type EventStatus string

func (e EventStatus) String() string {
	return string(e)
}

func (e EventStatus) IsValid() bool {
	switch e {
	case EventStatusPending, EventStatusProcessed, EventStatusFailed:
		return true
	default:
		return false
	}
}

func (e EventStatus) pgdb() pgdb.InboxEventStatus {
	return pgdb.InboxEventStatus(e.String())
}

const (
	EventStatusPending   EventStatus = "pending"
	EventStatusProcessed EventStatus = "processed"
	EventStatusFailed    EventStatus = "failed"
)

type Event struct {
	ID             uuid.UUID
	Seq            int64
	Topic          string
	Key            string
	Type           string
	Version        int32
	Producer       string
	Payload        json.RawMessage
	Status         EventStatus
	Attempts       int32
	LastError      *string
	LastAttemptAt  time.Time
	CreatedAt      time.Time
	KafkaPartition *int32
	KafkaOffset    *int64
	NextRetryAt    time.Time
	ProcessedAt    *time.Time
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

func (e Event) IsNil() bool {
	return e.ID == uuid.Nil
}

func convertInboxEvent(ie pgdb.InboxEvent) Event {
	res := Event{
		ID:            ie.ID,
		Seq:           ie.Seq,
		Topic:         ie.Topic,
		Key:           ie.Key,
		Type:          ie.Type,
		Version:       ie.Version,
		Producer:      ie.Producer,
		Payload:       ie.Payload,
		Status:        EventStatus(ie.Status),
		Attempts:      ie.Attempts,
		LastAttemptAt: ie.LastAttemptAt,
		CreatedAt:     ie.CreatedAt,
		NextRetryAt:   ie.NextRetryAt,
	}
	if ie.KafkaPartition.Valid {
		val := ie.KafkaPartition.Int32
		res.KafkaPartition = &val
	}
	if ie.KafkaOffset.Valid {
		val := ie.KafkaOffset.Int64
		res.KafkaOffset = &val
	}
	if ie.ProcessedAt.Valid {
		val := ie.ProcessedAt.Time
		res.ProcessedAt = &val
	}
	return res
}

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
