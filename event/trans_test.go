package event

import (
	"context"
	"fmt"
	"github.com/go-saas/uow"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

type headerCarrier http.Header

// Get returns the value associated with the passed key.
func (hc headerCarrier) Get(key string) string {
	return http.Header(hc).Get(key)
}

// Set stores the key-value pair.
func (hc headerCarrier) Set(key string, value string) {
	http.Header(hc).Set(key, value)
}

// Keys lists the keys stored in this carrier.
func (hc headerCarrier) Keys() []string {
	keys := make([]string, 0, len(hc))
	for k := range http.Header(hc) {
		keys = append(keys, k)
	}
	return keys
}

type Message struct {
	header headerCarrier
	key    string
	value  []byte
}

var (
	_ Event = (*Message)(nil)
)

func (m *Message) Key() string {
	return m.key
}

func (m *Message) Header() Header {
	return m.header
}

func (m *Message) Value() []byte {
	return m.value
}

func NewMessage(key string, value []byte) Event {
	return &Message{
		key:    key,
		value:  value,
		header: headerCarrier{},
	}
}

type producer struct {
}

func (p *producer) Close() error {
	return nil
}

func (p *producer) Send(ctx context.Context, msg Event) error {
	return p.BatchSend(ctx, []Event{msg})
}

func (p *producer) BatchSend(ctx context.Context, msg []Event) error {
	for _, event := range msg {
		fmt.Printf("%s \n", event.Key())
	}
	return nil
}

var _ Producer = (*producer)(nil)

func TestUow(t *testing.T) {
	p := &producer{}
	mgr := uow.NewManager(func(ctx context.Context, keys ...string) (uow.TransactionalDb, error) {
		if keys[0] == "event" {
			return NewTransactional(ctx, p), nil
		}
		panic("not found")
	})
	transP := NewTransactionalProducer(p, []string{"event"})
	err := mgr.WithNew(context.Background(), func(ctx context.Context) error {
		if err := transP.Send(ctx, NewMessage("1", nil)); err != nil {
			return err
		}
		if err := transP.Send(ctx, NewMessage("2", nil)); err != nil {
			return err
		}
		if err := transP.Send(ctx, NewMessage("3", nil)); err != nil {
			return err
		}
		return nil
	})
	assert.NoError(t, err)
}
