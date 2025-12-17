package pgdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
)

const outboxEventsTable = "outbox_events"

type OutboxEvent struct {
	ID           uuid.UUID `db:"id"`
	Topic        string    `db:"topic"`
	EventType    string    `db:"event_type"`
	EventVersion int       `db:"event_version"`
	Key          string    `db:"key"`
	Payload      []byte    `db:"payload"`

	Status      string       `db:"status"`
	Attempts    int          `db:"attempts"`
	CreatedAt   time.Time    `db:"created_at"`
	NextRetryAt sql.NullTime `db:"next_retry_at"`
	SentAt      sql.NullTime `db:"sent_at"`
}

type OutboxEventsQ struct {
	db DBTX

	selector sq.SelectBuilder
	inserter sq.InsertBuilder
	updater  sq.UpdateBuilder
	deleter  sq.DeleteBuilder
	counter  sq.SelectBuilder
}

func NewOutboxEventsQ(db DBTX) OutboxEventsQ {
	builder := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	return OutboxEventsQ{
		db: db,

		selector: builder.Select("*").From(outboxEventsTable),
		inserter: builder.Insert(outboxEventsTable),
		updater:  builder.Update(outboxEventsTable),
		deleter:  builder.Delete(outboxEventsTable),
		counter:  builder.Select("COUNT(*) AS count").From(outboxEventsTable),
	}
}

func (q OutboxEventsQ) Insert(ctx context.Context, input OutboxEvent) error {
	values := map[string]interface{}{
		"id":            input.ID,
		"topic":         input.Topic,
		"event_type":    input.EventType,
		"event_version": input.EventVersion,
		"key":           input.Key,
		"payload":       input.Payload,
		"status":        input.Status,
		"attempts":      input.Attempts,
		"next_retry_at": input.NextRetryAt,
		"created_at":    input.CreatedAt,
		"sent_at":       input.SentAt,
	}

	query, args, err := q.inserter.SetMap(values).ToSql()
	if err != nil {
		return fmt.Errorf("building insert query for %s: %w", outboxEventsTable, err)
	}

	_, err = q.db.ExecContext(ctx, query, args...)
	return err
}

func (q OutboxEventsQ) Update(ctx context.Context) error {
	query, args, err := q.updater.ToSql()
	if err != nil {
		return fmt.Errorf("building update query for %s: %w", outboxEventsTable, err)
	}

	_, err = q.db.ExecContext(ctx, query, args...)
	return err
}

func (q OutboxEventsQ) UpdateStatus(status string) OutboxEventsQ {
	q.updater = q.updater.Set("status", status)
	return q
}

func (q OutboxEventsQ) AddAttempts() OutboxEventsQ {
	q.updater = q.updater.Set("attempts", sq.Expr("attempts + 1"))
	return q
}

func (q OutboxEventsQ) UpdateAttempts(attempts int) OutboxEventsQ {
	q.updater = q.updater.Set("attempts", attempts)
	return q
}

func (q OutboxEventsQ) UpdateNextRetryAt(t time.Time) OutboxEventsQ {
	q.updater = q.updater.Set("next_retry_at", t)
	return q
}

func (q OutboxEventsQ) UpdateSentAt(sentAt sql.NullTime) OutboxEventsQ {
	q.updater = q.updater.Set("sent_at", sentAt)
	return q
}

func (q OutboxEventsQ) Get(ctx context.Context) (OutboxEvent, error) {
	query, args, err := q.selector.Limit(1).ToSql()
	if err != nil {
		return OutboxEvent{}, fmt.Errorf("building get query for %s: %w", outboxEventsTable, err)
	}

	row := q.db.QueryRowContext(ctx, query, args...)

	var e OutboxEvent
	err = row.Scan(
		&e.ID,
		&e.Topic,
		&e.EventType,
		&e.EventVersion,
		&e.Key,
		&e.Payload,
		&e.Status,
		&e.Attempts,
		&e.CreatedAt,
		&e.NextRetryAt,
		&e.SentAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return OutboxEvent{}, nil
		}
		return OutboxEvent{}, err
	}

	return e, nil
}

func (q OutboxEventsQ) Select(ctx context.Context) ([]OutboxEvent, error) {
	query, args, err := q.selector.ToSql()
	if err != nil {
		return nil, fmt.Errorf("building select query for %s: %w", outboxEventsTable, err)
	}

	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		err = rows.Scan(
			&e.ID,
			&e.Topic,
			&e.EventType,
			&e.EventVersion,
			&e.Key,
			&e.Payload,
			&e.Status,
			&e.Attempts,
			&e.CreatedAt,
			&e.NextRetryAt,
			&e.SentAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning outbox event: %w", err)
		}
		out = append(out, e)
	}

	return out, nil
}

func (q OutboxEventsQ) Delete(ctx context.Context) error {
	query, args, err := q.deleter.ToSql()
	if err != nil {
		return fmt.Errorf("building delete query for %s: %w", outboxEventsTable, err)
	}

	_, err = q.db.ExecContext(ctx, query, args...)
	return err
}

func (q OutboxEventsQ) FilterID(id uuid.UUID) OutboxEventsQ {
	q.selector = q.selector.Where(sq.Eq{"id": id})
	q.counter = q.counter.Where(sq.Eq{"id": id})
	q.deleter = q.deleter.Where(sq.Eq{"id": id})
	q.updater = q.updater.Where(sq.Eq{"id": id})
	return q
}

func (q OutboxEventsQ) FilterStatus(status string) OutboxEventsQ {
	q.selector = q.selector.Where(sq.Eq{"status": status})
	q.counter = q.counter.Where(sq.Eq{"status": status})
	q.deleter = q.deleter.Where(sq.Eq{"status": status})
	q.updater = q.updater.Where(sq.Eq{"status": status})
	return q
}

func (q OutboxEventsQ) FilterReadyToSend(now time.Time) OutboxEventsQ {
	q.selector = q.selector.Where(sq.LtOrEq{"next_retry_at": now})
	q.counter = q.counter.Where(sq.LtOrEq{"next_retry_at": now})
	return q
}

func (q OutboxEventsQ) Page(limit, offset uint) OutboxEventsQ {
	q.selector = q.selector.Limit(uint64(limit)).Offset(uint64(offset))
	return q
}

func (q OutboxEventsQ) OrderCreatedAt(ascending bool) OutboxEventsQ {
	if ascending {
		q.selector = q.selector.OrderBy("created_at ASC")
	} else {
		q.selector = q.selector.OrderBy("created_at DESC")
	}
	return q
}

func (q OutboxEventsQ) Count(ctx context.Context) (uint64, error) {
	query, args, err := q.counter.ToSql()
	if err != nil {
		return 0, fmt.Errorf("building count query for %s: %w", outboxEventsTable, err)
	}

	var count uint64
	err = q.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}
