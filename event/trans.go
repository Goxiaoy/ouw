package event

import (
	"context"
	"database/sql"
	"github.com/go-saas/uow"
	"sync"
)

type Transactional struct {
	ctx      context.Context
	producer Producer
	events   []Event
	sync.Mutex
}

func NewTransactional(ctx context.Context, producer Producer) *Transactional {
	return &Transactional{
		ctx:      ctx,
		producer: producer,
	}
}

var (
	_ uow.TransactionalDb = (*Transactional)(nil)
	_ uow.Txn             = (*Transactional)(nil)
)

func (t *Transactional) Commit() error {
	if len(t.events) == 0 {
		return nil
	}
	return t.producer.BatchSend(t.ctx, t.events)
}

func (t *Transactional) Rollback() error {
	//can not perform rollback
	return nil
}

func (t *Transactional) Begin(opt ...*sql.TxOptions) (db uow.Txn, err error) {
	return NewTransactional(t.ctx, t.producer), nil
}

func (t *Transactional) Send(msg ...Event) error {
	t.Lock()
	defer t.Unlock()
	t.events = append(t.events, msg...)
	return nil
}

type TransactionalProducer struct {
	wrap Producer
	keys []string
}

func NewTransactionalProducer(wrap Producer, keys []string) *TransactionalProducer {
	return &TransactionalProducer{wrap: wrap, keys: keys}
}

func (t *TransactionalProducer) Close() error {
	return t.wrap.Close()
}

func (t *TransactionalProducer) Send(ctx context.Context, msg Event) error {
	if u, ok := uow.FromCurrentUow(ctx); ok {
		//resolve Transactional from unit of work
		tx, err := u.GetTxDb(ctx, t.keys...)
		if err != nil {
			return err
		}
		return tx.(*Transactional).Send(msg)
	} else {
		return t.wrap.Send(ctx, msg)
	}
}

func (t *TransactionalProducer) BatchSend(ctx context.Context, msg []Event) error {
	if u, ok := uow.FromCurrentUow(ctx); ok {
		//resolve Transactional from unit of work
		tx, err := u.GetTxDb(ctx, t.keys...)
		if err != nil {
			return err
		}
		return tx.(*Transactional).Send(msg...)
	} else {
		return t.wrap.BatchSend(ctx, msg)
	}
}

var _ Producer = (*TransactionalProducer)(nil)
