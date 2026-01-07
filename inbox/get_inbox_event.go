package inbox

import (
	"context"
	"database/sql"
	"errors"

	"github.com/google/uuid"
)

func (b Box) GetInboxEvent(
	ctx context.Context,
	ID uuid.UUID,
) (InboxEvent, error) {
	res, err := b.queries(ctx).GetInboxEventByID(ctx, ID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return InboxEvent{}, nil
	case err != nil:
		return InboxEvent{}, err
	}

	return convertInboxEvent(res), nil
}
