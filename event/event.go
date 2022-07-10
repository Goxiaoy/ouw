package event

import (
	"context"
	"io"
)

type Header interface {
	Get(key string) string
	Set(key string, value string)
	Keys() []string
}

type Event interface {
	Header() Header
	Key() string
	Value() []byte
}

type Producer interface {
	io.Closer
	Send(ctx context.Context, msg Event) error
	BatchSend(ctx context.Context, msg []Event) error
}
