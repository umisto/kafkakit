package outbox

import (
	"encoding/json"
	"time"

	"github.com/chains-lab/kafkakit"
	"github.com/google/uuid"
)

func (e Event) ToMessage() kafkakit.Message {
	return kafkakit.Message{
		Topic:        e.Topic,
		EventType:    e.EventType,
		EventVersion: e.EventVersion,
		Key:          e.Key,
		Payload:      e.Payload,
	}
}

type Event struct {
	ID           uuid.UUID
	Topic        string
	EventType    string
	EventVersion uint
	Key          string
	Payload      json.RawMessage
	Status       string
	Attempts     uint
	NextRetryAt  time.Time
	CreatedAt    time.Time
	SentAt       *time.Time
}
