package box

import (
	"context"
	"database/sql"

	"github.com/netbill/kafkakit/box/pgdb"
	"github.com/netbill/pgx"
)

type Box struct {
	queries *pgdb.Queries
	db      *sql.DB
}

func New(db *sql.DB) Box {
	return Box{
		queries: pgdb.New(db),
		db:      db,
	}
}

func (b Box) Transaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return pgx.Transaction(b.db, ctx, fn)
}
