package ouw

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
)

type UnitOfWork interface {
	Commit() error
	Rollback() error
	GetTxDb(ctx context.Context, key DbKey) (tx interface{}, err error)
}

var _ UnitOfWork = (*unitOfWork)(nil)

type unitOfWork struct {
	m Manager
	// db can be any client
	db  map[DbKey]interface{}
	mtx sync.Mutex
}

func NewUnitOfWork(m Manager) UnitOfWork {
	return &unitOfWork{
		m:  m,
		db: make(map[DbKey]interface{}),
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

func (u *unitOfWork) GetTxDb(ctx context.Context, key DbKey) (tx interface{}, err error) {
	u.mtx.Lock()
	defer u.mtx.Unlock()
	db, ok := u.m.Resolve(ctx, key)
	if !ok {
		return nil, ErrDbKeyNotFound
	}
	tx, ok = u.db[key]
	if ok {
		return tx, nil
	}
	tx, err = db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	u.db[key] = tx
	return
}

// WithUnitOfWork wrap a function into a unit of work. Automatically rollback if function returns error
func WithUnitOfWork(ctx context.Context, uow UnitOfWork, fn func(ctx context.Context) error) error {
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
