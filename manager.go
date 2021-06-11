package uow

import (
	"context"
	"database/sql"
)

type Manager interface {
	// WithNew create a new unit of work and execute [fn] with this unit of work
	WithNew(ctx context.Context, fn func(ctx context.Context) error, opt ...*sql.TxOptions) error
}

type manager struct {
	factory DbFactory
}

func NewManager(factory DbFactory) Manager {
	return &manager{
		factory: factory,
	}
}

func (m *manager) WithNew(ctx context.Context, fn func(ctx context.Context) error, opt ...*sql.TxOptions) error {
	//get current for nested
	current, ok := FromCurrentUow(ctx)
	factory := m.factory
	if ok {
		factory = func(ctx context.Context, key string) TransactionalDb {
			//TODO lock?
			tx, ok := current.db[key]
			if ok {
				tdb, ok := tx.(TransactionalDb)
				if ok {
					return tdb
				}
			}
			return m.factory(ctx, key)
		}
	}
	uow := m.createNewUintOfWork(factory, opt...)
	newCtx := newCurrentUow(ctx, uow)
	return withUnitOfWork(newCtx, fn)
}

func (m *manager) createNewUintOfWork(factory DbFactory, opt ...*sql.TxOptions) *UnitOfWork {
	return NewUnitOfWork(factory, opt...)
}
