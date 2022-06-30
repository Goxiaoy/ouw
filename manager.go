package uow

import (
	"context"
	"database/sql"
	"github.com/google/uuid"
	"strings"
)

type Manager interface {
	CreateNew(ctx context.Context, opt ...*sql.TxOptions) (*UnitOfWork, error)
	// WithNew create a new unit of work and execute [fn] with this unit of work
	WithNew(ctx context.Context, fn func(ctx context.Context) error, opt ...*sql.TxOptions) error
}

type KeyFormatter func(keys ...string) string

var (
	DefaultKeyFormatter KeyFormatter = func(keys ...string) string {
		return strings.Join(keys, "/")
	}
)

type IdGenerator func(ctx context.Context) string

var (
	DefaultIdGenerator IdGenerator = func(ctx context.Context) string {
		return uuid.New().String()
	}
)

type manager struct {
	cfg     *Config
	factory DbFactory
}

var _ Manager = (*manager)(nil)

type Config struct {
	DisableNestedTransaction bool
	formatter                KeyFormatter
	idGen                    IdGenerator
}

type Option func(*Config)

func WithDisableNestedNestedTransaction() Option {
	return func(config *Config) {
		config.DisableNestedTransaction = true
	}
}
func WithKeyFormatter(f KeyFormatter) Option {
	return func(config *Config) {
		config.formatter = f
	}
}

func WithIdGenerator(idGen IdGenerator) Option {
	return func(config *Config) {
		config.idGen = idGen
	}
}

func NewManager(factory DbFactory, opts ...Option) Manager {
	cfg := &Config{
		formatter: DefaultKeyFormatter,
		idGen:     DefaultIdGenerator,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return &manager{
		cfg:     cfg,
		factory: factory,
	}
}

func (m *manager) CreateNew(ctx context.Context, opt ...*sql.TxOptions) (*UnitOfWork, error) {
	factory := m.factory
	//get current for nested
	var parent *UnitOfWork
	if current, ok := FromCurrentUow(ctx); ok {
		parent = current
	}
	if parent != nil {
		//first level uow will use default factory, others will find from parent
		factory = nil
	}
	uow := newUnitOfWork(m.cfg.idGen(ctx), m.cfg.DisableNestedTransaction, parent, factory, m.cfg.formatter, opt...)
	return uow, nil
}

func (m *manager) WithNew(ctx context.Context, fn func(ctx context.Context) error, opt ...*sql.TxOptions) error {
	uow, err := m.CreateNew(ctx, opt...)
	if err != nil {
		return err
	}
	return WithUnitOfWork(ctx, uow, fn)
}
