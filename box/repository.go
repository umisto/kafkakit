package box

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/chains-lab/kafkakit/box/pgdb"
)

type Repository struct {
	db *sql.DB
}

type Queries struct {
	Inbox  pgdb.InboxEventsQ
	Outbox pgdb.OutboxEventsQ
}

func New(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Inbox() pgdb.InboxEventsQ {
	return pgdb.NewInboxEventsQ(r.db)
}

func (r *Repository) Outbox() pgdb.OutboxEventsQ {
	return pgdb.NewOutboxEventsQ(r.db)
}

func (r *Repository) qWithTx(tx *sql.Tx) Queries {
	return Queries{
		Inbox:  pgdb.NewInboxEventsQ(tx),
		Outbox: pgdb.NewOutboxEventsQ(tx),
	}
}

func (r *Repository) Transaction(ctx context.Context, fn func(q Queries) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	q := r.qWithTx(tx)

	if err = fn(q); err != nil {
		_ = tx.Rollback()
		return err
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}
