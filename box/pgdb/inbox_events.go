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

const inboxEventsTable = "inbox_events"

type InboxEvent struct {
	ID           uuid.UUID `db:"id"`
	Topic        string    `db:"topic"`
	EventType    string    `db:"event_type"`
	EventVersion uint      `db:"event_version"`
	Key          string    `db:"key"`
	Payload      []byte    `db:"payload"`

	Status      string       `db:"status"`
	Attempts    uint         `db:"attempts"`
	CreatedAt   time.Time    `db:"created_at"`
	NextRetryAt sql.NullTime `db:"next_retry_at"`
	ProcessedAt sql.NullTime `db:"processed_at"`
}

type InboxEventsQ struct {
	db DBTX

	selector sq.SelectBuilder
	inserter sq.InsertBuilder
	updater  sq.UpdateBuilder
	deleter  sq.DeleteBuilder
	counter  sq.SelectBuilder
}

func NewInboxEventsQ(db DBTX) InboxEventsQ {
	builder := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	return InboxEventsQ{
		db: db,

		selector: builder.Select("*").From(inboxEventsTable),
		inserter: builder.Insert(inboxEventsTable),
		updater:  builder.Update(inboxEventsTable),
		deleter:  builder.Delete(inboxEventsTable),
		counter:  builder.Select("COUNT(*) AS count").From(inboxEventsTable),
	}
}

func (q InboxEventsQ) New() InboxEventsQ {
	return NewInboxEventsQ(q.db)
}

func (q InboxEventsQ) WithTx(tx *sql.Tx) InboxEventsQ {
	q.db = tx
	return q
}

func (q InboxEventsQ) Insert(ctx context.Context, input InboxEvent) (InboxEvent, error) {
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
		"processed_at":  input.ProcessedAt,
	}

	query, args, err := q.inserter.
		SetMap(values).
		Suffix("RETURNING id, topic, event_type, event_version, key, payload, status, attempts, next_retry_at, created_at, processed_at").
		ToSql()
	if err != nil {
		return InboxEvent{}, fmt.Errorf("building insert query for %s: %w", inboxEventsTable, err)
	}

	var out InboxEvent
	err = q.db.QueryRowContext(ctx, query, args...).Scan(
		&out.ID,
		&out.Topic,
		&out.EventType,
		&out.EventVersion,
		&out.Key,
		&out.Payload,
		&out.Status,
		&out.Attempts,
		&out.CreatedAt,
		&out.NextRetryAt,
		&out.ProcessedAt,
	)
	if err != nil {
		return InboxEvent{}, err
	}

	return out, nil
}

func (q InboxEventsQ) UpdateOne(ctx context.Context) (InboxEvent, error) {
	query, args, err := q.updater.
		Suffix("RETURNING id, topic, event_type, event_version, key, payload, status, attempts, next_retry_at, created_at, processed_at").
		ToSql()
	if err != nil {
		return InboxEvent{}, fmt.Errorf("building update query for %s: %w", inboxEventsTable, err)
	}

	var out InboxEvent
	err = q.db.QueryRowContext(ctx, query, args...).Scan(
		&out.ID,
		&out.Topic,
		&out.EventType,
		&out.EventVersion,
		&out.Key,
		&out.Payload,
		&out.Status,
		&out.Attempts,
		&out.CreatedAt,
		&out.NextRetryAt,
		&out.ProcessedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return InboxEvent{}, nil
		}
		return InboxEvent{}, err
	}

	return out, nil
}

func (q InboxEventsQ) UpdateMany(ctx context.Context) ([]InboxEvent, error) {
	query, args, err := q.updater.
		Suffix("RETURNING id, topic, event_type, event_version, key, payload, status, attempts, next_retry_at, created_at, processed_at").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("building update query for %s: %w", inboxEventsTable, err)
	}

	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]InboxEvent, 0, 16)
	for rows.Next() {
		var e InboxEvent
		if err := rows.Scan(
			&e.ID,
			&e.Topic,
			&e.EventType,
			&e.EventVersion,
			&e.Key,
			&e.Payload,
			&e.Status,
			&e.Attempts,
			&e.NextRetryAt,
			&e.CreatedAt,
			&e.ProcessedAt,
		); err != nil {
			return nil, fmt.Errorf("scanning updated inbox event: %w", err)
		}
		out = append(out, e)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func (q InboxEventsQ) UpdateStatus(status string) InboxEventsQ {
	q.updater = q.updater.Set("status", status)
	return q
}

func (q InboxEventsQ) AddAttempts() InboxEventsQ {
	q.updater = q.updater.Set("attempts", sq.Expr("attempts + 1"))
	return q
}

func (q InboxEventsQ) UpdateAttempts(attempts int) InboxEventsQ {
	q.updater = q.updater.Set("attempts", attempts)
	return q
}

func (q InboxEventsQ) UpdateNextRetryAt(t sql.NullTime) InboxEventsQ {
	q.updater = q.updater.Set("next_retry_at", t)
	return q
}

func (q InboxEventsQ) UpdateProcessedAt(processedAt sql.NullTime) InboxEventsQ {
	q.updater = q.updater.Set("processed_at", processedAt)
	return q
}

func (q InboxEventsQ) Get(ctx context.Context) (InboxEvent, error) {
	query, args, err := q.selector.Limit(1).ToSql()
	if err != nil {
		return InboxEvent{}, fmt.Errorf("building get query for %s: %w", inboxEventsTable, err)
	}

	row := q.db.QueryRowContext(ctx, query, args...)

	var e InboxEvent
	err = row.Scan(
		&e.ID,
		&e.Topic,
		&e.EventType,
		&e.EventVersion,
		&e.Key,
		&e.Payload,
		&e.Status,
		&e.Attempts,
		&e.NextRetryAt,
		&e.CreatedAt,
		&e.ProcessedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return InboxEvent{}, nil
		}
		return InboxEvent{}, err
	}

	return e, nil
}

func (q InboxEventsQ) Select(ctx context.Context) ([]InboxEvent, error) {
	query, args, err := q.selector.ToSql()
	if err != nil {
		return nil, fmt.Errorf("building select query for %s: %w", inboxEventsTable, err)
	}

	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []InboxEvent
	for rows.Next() {
		var e InboxEvent
		err = rows.Scan(
			&e.ID,
			&e.Topic,
			&e.EventType,
			&e.EventVersion,
			&e.Key,
			&e.Payload,
			&e.Status,
			&e.Attempts,
			&e.NextRetryAt,
			&e.CreatedAt,
			&e.ProcessedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning inbox event: %w", err)
		}
		out = append(out, e)
	}

	return out, nil
}

func (q InboxEventsQ) Delete(ctx context.Context) error {
	query, args, err := q.deleter.ToSql()
	if err != nil {
		return fmt.Errorf("building delete query for %s: %w", inboxEventsTable, err)
	}

	_, err = q.db.ExecContext(ctx, query, args...)
	return err
}

func (q InboxEventsQ) FilterID(id ...uuid.UUID) InboxEventsQ {
	q.selector = q.selector.Where(sq.Eq{"id": id})
	q.counter = q.counter.Where(sq.Eq{"id": id})
	q.deleter = q.deleter.Where(sq.Eq{"id": id})
	q.updater = q.updater.Where(sq.Eq{"id": id})
	return q
}

func (q InboxEventsQ) FilterTopic(topic string) InboxEventsQ {
	q.selector = q.selector.Where(sq.Eq{"topic": topic})
	q.counter = q.counter.Where(sq.Eq{"topic": topic})
	q.deleter = q.deleter.Where(sq.Eq{"topic": topic})
	q.updater = q.updater.Where(sq.Eq{"topic": topic})
	return q
}

func (q InboxEventsQ) FilterEventType(eventType string) InboxEventsQ {
	q.selector = q.selector.Where(sq.Eq{"event_type": eventType})
	q.counter = q.counter.Where(sq.Eq{"event_type": eventType})
	q.deleter = q.deleter.Where(sq.Eq{"event_type": eventType})
	q.updater = q.updater.Where(sq.Eq{"event_type": eventType})
	return q
}

func (q InboxEventsQ) FilterKey(key string) InboxEventsQ {
	q.selector = q.selector.Where(sq.Eq{"key": key})
	q.counter = q.counter.Where(sq.Eq{"key": key})
	q.deleter = q.deleter.Where(sq.Eq{"key": key})
	q.updater = q.updater.Where(sq.Eq{"key": key})
	return q
}

func (q InboxEventsQ) FilterStatus(status string) InboxEventsQ {
	q.selector = q.selector.Where(sq.Eq{"status": status})
	q.counter = q.counter.Where(sq.Eq{"status": status})
	q.deleter = q.deleter.Where(sq.Eq{"status": status})
	q.updater = q.updater.Where(sq.Eq{"status": status})
	return q
}

func (q InboxEventsQ) FilterReadyToProcess(now time.Time) InboxEventsQ {
	q.selector = q.selector.Where(sq.LtOrEq{"next_retry_at": now})
	q.counter = q.counter.Where(sq.LtOrEq{"next_retry_at": now})
	return q
}

func (q InboxEventsQ) Page(limit, offset uint) InboxEventsQ {
	q.selector = q.selector.Limit(uint64(limit)).Offset(uint64(offset))
	return q
}

func (q InboxEventsQ) OrderCreatedAt(ascending bool) InboxEventsQ {
	if ascending {
		q.selector = q.selector.OrderBy("created_at ASC")
	} else {
		q.selector = q.selector.OrderBy("created_at DESC")
	}
	return q
}

func (q InboxEventsQ) Count(ctx context.Context) (uint64, error) {
	query, args, err := q.counter.ToSql()
	if err != nil {
		return 0, fmt.Errorf("building count query for %s: %w", inboxEventsTable, err)
	}

	var count uint64
	err = q.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}
