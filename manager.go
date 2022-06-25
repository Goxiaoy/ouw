package uow

import (
	"context"
	"database/sql"
	"strings"
)

type Manager interface {
	// WithNew create a new unit of work and execute [fn] with this unit of work
	WithNew(ctx context.Context, fn func(ctx context.Context) error, opt ...*sql.TxOptions) error
}

type KeyFormatter func(keys ...string) string

var (
	DefaultKeyFormatter KeyFormatter = func(keys ...string) string {
		return strings.Join(keys, "/")
	}
)

type manager struct {
	cfg     *Config
	factory DbFactory
}

type Config struct {
	NestedTransaction bool
	Formatter         KeyFormatter
}

type Option func(*Config)

func WithNestedNestedTransaction() Option {
	return func(config *Config) {
		config.NestedTransaction = true
	}
}

func WithKeyFormatter(f KeyFormatter) Option {
	return func(config *Config) {
		config.Formatter = f
	}
}

func NewManager(factory DbFactory, opts ...Option) Manager {
	cfg := &Config{
		Formatter: DefaultKeyFormatter,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return &manager{
		cfg:     cfg,
		factory: factory,
	}
}

func (m *manager) WithNew(ctx context.Context, fn func(ctx context.Context) error, opt ...*sql.TxOptions) error {
	factory := m.factory
	//get current for nested
	if m.cfg.NestedTransaction {
		current, ok := FromCurrentUow(ctx)
		if ok {
			factory = func(ctx context.Context, keys ...string) TransactionalDb {
				tx, ok := current.db[m.cfg.Formatter(keys...)]
				if ok {
					tdb, ok := tx.(TransactionalDb)
					if ok {
						return tdb
					}
				}
				//fall back to parent factory
				return m.factory(ctx, keys...)
			}
		}
	}
	uow := m.createNewUintOfWork(factory, opt...)
	newCtx := newCurrentUow(ctx, uow)
	return withUnitOfWork(newCtx, fn)
}

func (m *manager) createNewUintOfWork(factory DbFactory, opt ...*sql.TxOptions) *UnitOfWork {
	return NewUnitOfWork(factory, m.cfg.Formatter, opt...)
}
