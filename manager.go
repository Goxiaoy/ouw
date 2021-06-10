package ouw

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

var (
	ErrDuplicateKey  = errors.New("duplicate dbKey")
	ErrDbKeyNotFound = errors.New("dbKey not found")
)

type Manager interface {
	Register(key string, f DbFactory) (err error)
	RegisterIfNot(key string, f DbFactory)
	Resolve(ctx context.Context, key string) (db TransactionalDb, ok bool)
	// WithNew create a new unit of work and execute [fn] with this unit of work
	WithNew(ctx context.Context, fn func(ctx context.Context) error, opt ...*sql.TxOptions) error
}

type manager struct {
	mtx sync.Mutex
	db  map[string]DbFactory
}

func NewManager() Manager {
	return &manager{
		db: make(map[string]DbFactory),
	}
}

func (m *manager) Register(key string, f DbFactory) (err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	_, ok := m.db[key]
	if ok {
		return ErrDuplicateKey
	}
	m.db[key] = f
	return nil
}

func (m *manager) RegisterIfNot(key string, f DbFactory) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	_, ok := m.db[key]
	if !ok {
		m.db[key] = f
	}
}

func (m *manager) Resolve(ctx context.Context, key string) (db TransactionalDb, ok bool) {
	f, ok := m.db[key]
	if !ok {
		return
	}
	db = f(ctx, key)
	return
}

func (m *manager) WithNew(ctx context.Context, fn func(ctx context.Context) error, opt ...*sql.TxOptions) error {
	uow := m.createNewUintOfWork(opt...)
	newCtx := newCurrentUow(ctx, uow)
	return withUnitOfWork(newCtx, fn)
}

func (m *manager) createNewUintOfWork(opt ...*sql.TxOptions) UnitOfWork {
	return NewUnitOfWork(m, opt...)
}
