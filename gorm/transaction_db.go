package gorm

import (
	"context"
	"database/sql"
	"github.com/goxiaoy/ouw"
	"gorm.io/gorm"
)

type TransactionDb struct {
	db *gorm.DB
}

func NewTransactionDb(db *gorm.DB) ouw.TransactionalDb {
	return &TransactionDb{
		db: db,
	}
}

func (t *TransactionDb) Begin(ctx context.Context, opt ...*sql.TxOptions) (db interface{}, err error) {
	return t.db.Begin(opt...), nil
}
