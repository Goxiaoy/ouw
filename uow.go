package uow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	orderedmap "github.com/elliotchance/orderedmap/v2"
	"strings"
	"sync"
)

var (
	ErrUnitOfWorkNotFound = errors.New("unit of work not found, please wrap with manager.WithNew")
)

type UnitOfWork struct {
	id            string
	parent        *UnitOfWork
	factory       DbFactory
	disableNested bool
	// db can be any kind of client
	db        *orderedmap.OrderedMap[string, Txn]
	mtx       sync.Mutex
	opt       []*sql.TxOptions
	formatter KeyFormatter
}

func newUnitOfWork(id string, disableNested bool, parent *UnitOfWork, factory DbFactory, formatter KeyFormatter, opt ...*sql.TxOptions) *UnitOfWork {
	return &UnitOfWork{
		id:            id,
		parent:        parent,
		factory:       factory,
		disableNested: disableNested,
		formatter:     formatter,
		db:            orderedmap.NewOrderedMap[string, Txn](),
		opt:           opt,
	}
}

func (u *UnitOfWork) Commit() error {
	for el := u.db.Back(); el != nil; el = el.Prev() {
		err := el.Value.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (u *UnitOfWork) Rollback() error {
	var errs []string
	for el := u.db.Back(); el != nil; el = el.Prev() {
		err := el.Value.Rollback()
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

func (u *UnitOfWork) GetId() string {
	return u.id
}

func (u *UnitOfWork) GetTxDb(ctx context.Context, keys ...string) (tx Txn, err error) {
	u.mtx.Lock()
	defer u.mtx.Unlock()
	key := u.formatter(keys...)
	if tx, ok := u.db.Get(key); ok {
		return tx, nil
	}

	//find from parent, no not begin new
	if u.parent != nil && u.disableNested {
		return u.parent.GetTxDb(ctx, keys...)
	}

	// using factory
	db, err := u.getFactory()(ctx, keys...)
	if err != nil {
		return nil, err
	}
	//begin new transaction
	tx, err = db.Begin(u.opt...)
	if err != nil {
		return nil, err
	}
	u.db.Set(key, tx)
	return
}

func (u *UnitOfWork) getFactory() DbFactory {
	return func(ctx context.Context, keys ...string) (TransactionalDb, error) {
		//find from current
		if tx, ok := u.db.Get(u.formatter(keys...)); ok {
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

func WithUnitOfWork(ctx context.Context, u *UnitOfWork, fn func(ctx context.Context) error) (err error) {
	ctx = NewCurrentUow(ctx, u)
	return WithCurrentUnitOfWork(ctx, fn)
}

// WithCurrentUnitOfWork wrap a function into current unit of work. Automatically Rollback if function returns error
func WithCurrentUnitOfWork(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	uow, ok := FromCurrentUow(ctx)
	if !ok {
		return ErrUnitOfWorkNotFound
	}
	panicked := true
	defer func() {
		if panicked || err != nil {
			if rerr := uow.Rollback(); rerr != nil {
				err = fmt.Errorf("rolling back transaction fail: %s\n %w ", rerr.Error(), err)
			}
		}
	}()
	if err = fn(ctx); err != nil {
		panicked = false
		return
	}
	panicked = false
	if rerr := uow.Commit(); rerr != nil {
		return fmt.Errorf("committing transaction fail: %w", rerr)
	}
	return nil
}
