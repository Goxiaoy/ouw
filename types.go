package uow

import (
	"context"
	"database/sql"
)

type TransactionalDb interface {
	// Begin a transaction
	Begin(opt ...*sql.TxOptions) (db Txn, err error)
}

type Txn interface {
	Commit() error
	Rollback() error
}

// DbFactory resolve transactional db by database keys
type DbFactory func(ctx context.Context, keys ...string) (TransactionalDb, error)
