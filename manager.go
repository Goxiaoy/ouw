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
	uow := m.createNewUintOfWork(opt...)
	newCtx := newCurrentUow(ctx, uow)
	return withUnitOfWork(newCtx, fn)
}

func (m *manager) createNewUintOfWork(opt ...*sql.TxOptions) UnitOfWork {
	return NewUnitOfWork(m.factory, opt...)
}
