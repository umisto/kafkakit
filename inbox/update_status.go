package inbox

import (
	"context"
	"database/sql"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/evebox/pgdb"
	"github.com/segmentio/kafka-go"
)

type InboxHandler func(ctx context.Context, event InboxEvent) error

func (b Box) WriteAndHandle(
	ctx context.Context,
	message kafka.Message,
	owner string,
	handler InboxHandler,
) error {
	event, err := b.CreateInboxEvent(ctx, message)
	if err != nil {
		return err
	}

	if event.ID == uuid.Nil {
		return nil
	}

	return b.Transaction(ctx, func(ctx context.Context) error {
		_, err = b.queries(ctx).TryLockInboxKey(ctx, pgdb.TryLockInboxKeyParams{
			Key:     event.Key,
			Owner:   owner,
			StaleAt: time.Now().UTC().Add(6 * time.Hour),
		})
		if err == sql.ErrNoRows {
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

		if err = handler(ctx, event); err != nil {
			_, err = b.MarkInboxEventsAsPending(
				ctx,
				time.Now().UTC().Add(30*time.Second),
				event.ID,
			)
			return err
		}

		_, err = b.MarkInboxEventsAsProcessed(ctx, event.ID)
		return err
	})
}
