package inbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/netbill/evebox/header"
	"github.com/netbill/evebox/pgdb"
	"github.com/segmentio/kafka-go"
)

func (b Box) CreateInboxEvent(
	ctx context.Context,
	message kafka.Message,
) (InboxEvent, error) {
	headers := map[string][]byte{}
	for _, h := range message.Headers {
		headers[h.Key] = h.Value
	}

	eventIDBytes, ok := headers[header.EventID]
	if !ok {
		return InboxEvent{}, fmt.Errorf("missing %s header", header.EventID)
	}
	eventID, err := uuid.ParseBytes(eventIDBytes)
	if err != nil {
		return InboxEvent{}, fmt.Errorf("invalid %s header", header.EventID)
	}

	eventTypeBytes, ok := headers[header.EventType]
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

	producerBytes, ok := headers[header.Producer]
	if !ok {
		return InboxEvent{}, fmt.Errorf("missing %s header", header.Producer)
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
		return InboxEvent{}, nil
	case err != nil:
		return InboxEvent{}, err
	}

	return convertInboxEvent(res), nil
}
