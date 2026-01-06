package box

import (
	"context"
	"database/sql"

	"github.com/netbill/evebox/box/pgdb"
	"github.com/netbill/pgx"
)

type Box struct {
	db *sql.DB
}

func New(db *sql.DB) Box {
	return Box{
		db: db,
	}
}

func (b Box) queries(ctx context.Context) *pgdb.Queries {
	return pgdb.New(pgx.Exec(b.db, ctx))
}

func (b Box) Transaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return pgx.Transaction(b.db, ctx, fn)
}
