package ouw

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrDuplicateKey  = errors.New("duplicate dbKey")
	ErrDbKeyNotFound = errors.New("dbKey not found")
)

// DbKey usually represents the connection string of this db
type DbKey string

type CancelFunc func(context.Context) context.Context

type TransactionalDb interface {
	// Begin a transaction
	Begin(ctx context.Context) (db interface{}, err error)
}

type DbFactory func(ctx context.Context, key DbKey) TransactionalDb

type Manager interface {
	Register(key DbKey, f DbFactory) (err error)
	RegisterIfNot(key DbKey, f DbFactory)
	// Begin a unit of work
	Begin(ctx context.Context) (context.Context, CancelFunc)
	Resolve(ctx context.Context, key DbKey) (db TransactionalDb, ok bool)
}

type manager struct {
	mtx sync.Mutex
	db  map[DbKey]DbFactory
}

func NewManager() Manager {
	return &manager{
		db: make(map[DbKey]DbFactory),
	}
}

func (m *manager) Register(key DbKey, f DbFactory) (err error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	_, ok := m.db[key]
	if ok {
		return ErrDuplicateKey
	}
	m.db[key] = f
	return nil
}

func (m *manager) RegisterIfNot(key DbKey, f DbFactory) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	_, ok := m.db[key]
	if !ok {
		m.db[key] = f
	}
}

func (m *manager) Begin(ctx context.Context) (context.Context, CancelFunc) {
	current, _ := FromCurrentUow(ctx)
	uow := m.createNewUintOfWork()
	newCtx := NewCurrentUow(ctx, uow)
	return newCtx, func(ctx context.Context) context.Context {
		return NewCurrentUow(ctx, current)
	}
}

func (m *manager) Resolve(ctx context.Context, key DbKey) (db TransactionalDb, ok bool) {
	f, ok := m.db[key]
	if !ok {
		return
	}
	db = f(ctx, key)
	return
}

func (m *manager) createNewUintOfWork() UnitOfWork {
	return NewUnitOfWork(m)
}
