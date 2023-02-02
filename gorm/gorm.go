package gorm

import (
	"database/sql"
	"fmt"
	"github.com/go-saas/uow"
	"gorm.io/gorm"
)

type (
	RollbackFunc  func() error
	CommitFunc    func() error
	TransactionDb struct {
		*gorm.DB

		commitFunc   CommitFunc
		rollbackFunc RollbackFunc
	}
)

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
	// see https://github.com/go-gorm/gorm/blob/f3c6fc253356919e8ebbcf7bc50e8c7fe88802aa/finisher_api.go#L615-L655
	if committer, ok := db.Statement.ConnPool.(gorm.TxCommitter); ok && committer != nil {
		var rollback RollbackFunc
		var commitFunc CommitFunc
		if !db.DisableNestedTransaction {
			// nested transaction
			//create save point
			err = db.SavePoint(fmt.Sprintf("sp%p", t)).Error
			if err != nil {
				return nil, err
			}
			rollback = func() error {
				return db.RollbackTo(fmt.Sprintf("sp%p", t)).Error
			}
			//nested level do not need to commit
			commitFunc = func() error {
				return nil
			}
		}
		// TODO NewDB or not??
		ret := NewTransactionDb(db.Session(&gorm.Session{NewDB: true}))
		ret.rollbackFunc = rollback
		ret.commitFunc = commitFunc
		return ret, nil
	} else {
		tx := db.Begin(opt...)
		return NewTransactionDb(tx), tx.Error
	}
}
