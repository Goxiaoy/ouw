package gorm

import (
	"context"
	"database/sql"
	"github.com/goxiaoy/uow"
	"gorm.io/gorm"
)

type TransactionDb struct {
	*gorm.DB
}

// NewTransactionDb create a wrapper which implements uow.Txn
func NewTransactionDb(db *gorm.DB) *TransactionDb {
	return &TransactionDb{
		db,
	}
}

func (t *TransactionDb) Commit() error {
	return t.DB.Commit().Error
}

func (t *TransactionDb) Rollback() error {
	return t.DB.Rollback().Error
}

func (t *TransactionDb) Begin(ctx context.Context, opt ...*sql.TxOptions) (db uow.Txn, err error) {
	tx := t.DB.Begin(opt...)
	return NewTransactionDb(tx), tx.Error
}
