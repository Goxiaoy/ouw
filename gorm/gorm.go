package gorm

import (
	"database/sql"
	"fmt"
	"github.com/go-saas/uow"
	"gorm.io/gorm"
)

type TransactionDb struct {
	*gorm.DB

	commitFunc   func() error
	rollbackFunc func() error
}

var (
	_ uow.TransactionalDb = (*TransactionDb)(nil)
	_ uow.Txn             = (*TransactionDb)(nil)
)

// NewTransactionDb create a wrapper which implements uow.Txn
func NewTransactionDb(db *gorm.DB) *TransactionDb {
	return &TransactionDb{
		DB: db,
	}
}

func (t *TransactionDb) Commit() error {
	if t.commitFunc != nil {
		return t.commitFunc()
	}
	return t.DB.Commit().Error
}

func (t *TransactionDb) Rollback() error {
	if t.rollbackFunc != nil {
		return t.rollbackFunc()
	}
	return t.DB.Rollback().Error
}

func (t *TransactionDb) Begin(opt ...*sql.TxOptions) (uow.Txn, error) {
	var err error
	db := t.DB
	if committer, ok := db.Statement.ConnPool.(gorm.TxCommitter); ok && committer != nil {
		// nested transaction
		rollbackFunc := func() error { return nil }
		if !db.DisableNestedTransaction {
			//create save point
			err = db.SavePoint(fmt.Sprintf("sp%p", t)).Error
			if err != nil {
				return nil, err
			}
			rollbackFunc = func() error {
				return db.RollbackTo(fmt.Sprintf("sp%p", t)).Error
			}
		}
		//NewDB or not??
		ret := NewTransactionDb(db.Session(&gorm.Session{NewDB: true}))
		ret.rollbackFunc = rollbackFunc
		ret.commitFunc = func() error {
			return nil
		}
		return ret, nil
	}
	tx := t.DB.Begin(opt...)
	return NewTransactionDb(tx), tx.Error
}
