package inbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/evebox/box/pgdb"
	"github.com/segmentio/kafka-go"
)

type Handler func(ctx context.Context, event Event) EventStatus

func (b Box) WriteAndHandle(
	ctx context.Context,
	message kafka.Message,
	owner string,
	handler Handler,
) error {
	return b.Transaction(ctx, func(ctx context.Context) error {
		event, err := b.CreateInboxEvent(ctx, message)
		if err != nil {
			return err
		}
		if event.ID == uuid.Nil {
			return nil
		}

		_, err = b.queries(ctx).TryLockInboxKey(ctx, pgdb.TryLockInboxKeyParams{
			Key:     event.Key,
			Owner:   owner,
			StaleAt: time.Now().UTC().Add(6 * time.Hour),
		})
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}

		defer func() {
			_ = b.queries(ctx).UnlockInboxKey(ctx, pgdb.UnlockInboxKeyParams{
				Key:   event.Key,
				Owner: owner,
			})
		}()

		status := handler(ctx, event)

		switch status {
		case EventStatusPending:
			next := time.Now().UTC().Add(30 * time.Second)

			if _, err = b.MarkInboxEventsAsPending(ctx, next, event.ID); err != nil {
				return err
			}

			if err = b.BlockInboxKeyUntil(ctx, event.Key, next); err != nil {
				return err
			}

			return nil

		case EventStatusProcessed:
			_, err = b.MarkInboxEventsAsProcessed(ctx, event.ID)
			return err

		case EventStatusFailed:
			_, err = b.MarkInboxEventsAsFailed(ctx, event.ID)
			return err

		default:
			return fmt.Errorf("unknown event status: %s", status)
		}
	})
}
