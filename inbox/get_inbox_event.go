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
) (Event, error) {
	res, err := b.queries(ctx).GetInboxEventByID(ctx, ID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Event{}, nil
	case err != nil:
		return Event{}, err
	}

	return convertInboxEvent(res), nil
}
