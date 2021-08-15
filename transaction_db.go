package uow

import (
	"context"
	"database/sql"
)

type TransactionalDb interface {
	// Begin a transaction
	Begin(ctx context.Context, opt ...*sql.TxOptions) (db Txn, err error)
}

type Txn interface {
	Commit() error
	Rollback() error
}

// DbFactory resolve transactional db by database kind (like redis,mysql,sqlite) and key (usually business name)
type DbFactory func(ctx context.Context, kind, key string) TransactionalDb
