package uow

import (
	"context"
	"database/sql"
)

type TransactionalDb interface {
	// Begin a transaction
	Begin(ctx context.Context, opt ...*sql.TxOptions) (db interface{}, err error)
}

type DbFactory func(ctx context.Context, key string) TransactionalDb