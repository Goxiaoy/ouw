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

type UnitOfWork interface {
	Commit() error
	Rollback() error
	GetTxDb(ctx context.Context, key string) (tx interface{}, err error)
}

var _ UnitOfWork = (*unitOfWork)(nil)

type unitOfWork struct {
	factory DbFactory
	// db can be any client
	db  map[string]interface{}
	mtx sync.Mutex
	opt []*sql.TxOptions
}

func NewUnitOfWork(factory DbFactory, opt ...*sql.TxOptions) UnitOfWork {
	return &unitOfWork{
		factory:   factory,
		db:  make(map[string]interface{}),
		opt: opt,
	}
}

func (u *unitOfWork) Commit() error {
	for _, db := range u.db {
		tx, ok := db.(sql.Tx)
		if ok {
			err := tx.Commit()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (u *unitOfWork) Rollback() error {
	var errs []string
	for _, db := range u.db {
		tx, ok := db.(sql.Tx)
		if ok {
			err := tx.Rollback()
			if err != nil {
				errs = append(errs, err.Error())
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf(strings.Join(errs, "\n"))
	} else {
		return nil
	}
}

func (u *unitOfWork) GetTxDb(ctx context.Context, key string) (tx interface{}, err error) {
	u.mtx.Lock()
	defer u.mtx.Unlock()
	db := u.factory(ctx,key)
	tx, ok := u.db[key]
	if ok {
		return tx, nil
	}
	tx, err = db.Begin(ctx, u.opt...)
	if err != nil {
		return nil, err
	}
	u.db[key] = tx
	return
}

// WithUnitOfWork wrap a function into current unit of work. Automatically rollback if function returns error
func withUnitOfWork(ctx context.Context, fn func(ctx context.Context) error) error {
	uow, ok := FromCurrentUow(ctx)
	if !ok {
		return ErrUnitOfWorkNotFound
	}
	defer func() {
		if v := recover(); v != nil {
			uow.Rollback()
			panic(v)
		}
	}()
	if err := fn(ctx); err != nil {
		if rerr := uow.Rollback(); rerr != nil {
			err = fmt.Errorf("rolling back transaction: %w", rerr)
		}
		return err
	}
	if err := uow.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}
