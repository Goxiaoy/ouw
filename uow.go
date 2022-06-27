package uow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrUnitOfWorkNotFound = errors.New("unit of work not found, please wrap with manager.WithNew")
)

type unitOfWork struct {
	id      string
	parent  *unitOfWork
	factory DbFactory
	// db can be any kind of client
	db        map[string]Txn
	mtx       sync.Mutex
	opt       []*sql.TxOptions
	formatter KeyFormatter
}

func newUnitOfWork(id string, parent *unitOfWork, factory DbFactory, formatter KeyFormatter, opt ...*sql.TxOptions) *unitOfWork {
	return &unitOfWork{
		id:        id,
		parent:    parent,
		factory:   factory,
		formatter: formatter,
		db:        make(map[string]Txn),
		opt:       opt,
	}
}

func (u *unitOfWork) commit() error {
	for _, db := range u.db {
		err := db.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (u *unitOfWork) rollback() error {
	var errs []string
	for _, db := range u.db {
		err := db.Rollback()
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, "\n"))
	} else {
		return nil
	}
}

func (u *unitOfWork) GetId() string {
	return u.id
}

func (u *unitOfWork) GetTxDb(ctx context.Context, keys ...string) (tx Txn, err error) {
	u.mtx.Lock()
	defer u.mtx.Unlock()
	key := u.formatter(keys...)
	if tx, ok := u.db[key]; ok {
		return tx, nil
	}

	//find from parent
	if u.parent != nil {
		return u.parent.GetTxDb(ctx, keys...)
	}
	// no parent
	// using factory
	db, err := u.getFactory()(ctx, keys...)
	if err != nil {
		return nil, err
	}
	//begin new transaction
	tx, err = db.Begin(ctx, u.opt...)
	if err != nil {
		return nil, err
	}
	u.db[key] = tx
	return
}

func (u *unitOfWork) getFactory() DbFactory {
	return func(ctx context.Context, keys ...string) (TransactionalDb, error) {
		//find from current
		if tx, ok := u.db[u.formatter(keys...)]; ok {
			if tdb, ok := tx.(TransactionalDb); ok {
				return tdb, nil
			}
		}
		//find from parent
		if u.parent != nil {
			return u.parent.getFactory()(ctx, keys...)
		}
		return u.factory(ctx, keys...)
	}
}

// WithUnitOfWork wrap a function into current unit of work. Automatically rollback if function returns error
func withUnitOfWork(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	uow, ok := FromCurrentUow(ctx)
	if !ok {
		return ErrUnitOfWorkNotFound
	}
	panicked := true
	defer func() {
		if panicked || err != nil {
			if rerr := uow.rollback(); rerr != nil {
				err = fmt.Errorf("rolling back transaction fail: %s\n %w ", rerr.Error(), err)
			}
		}
	}()
	if err = fn(ctx); err != nil {
		panicked = false
		return
	}
	panicked = false
	if rerr := uow.commit(); rerr != nil {
		return fmt.Errorf("committing transaction fail: %w", rerr)
	}
	return nil
}
