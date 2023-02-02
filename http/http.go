package http

import (
	"context"
	"database/sql"
	"github.com/go-saas/uow"
	"net/http"
)

var (
	SafeMethods = []string{"GET", "HEAD", "OPTIONS", "TRACE"}
)

func contains(vals []string, s string) bool {
	for _, v := range vals {
		if v == s {
			return true
		}
	}

	return false
}

// SkipFunc identity whether a request should skip run into unit of work
type SkipFunc func(r *http.Request) bool

// EncodeErrorFunc how to encode error when a unit of work rollback
type EncodeErrorFunc func(http.ResponseWriter, *http.Request, error)

type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

type option struct {
	skip       SkipFunc
	txOpt      []*sql.TxOptions
	errEncoder EncodeErrorFunc
}

type Option func(*option)

// WithSkip change the skip unit of work function. default will skip SafeMethods like "GET", "HEAD", "OPTIONS", "TRACE"
func WithSkip(f SkipFunc) Option {
	return func(o *option) {
		o.skip = f
	}
}

func WithTxOpt(txOpt ...*sql.TxOptions) Option {
	return func(o *option) {
		o.txOpt = txOpt
	}
}

// WithErrorEncoder error encoder. default will not encode any error
func WithErrorEncoder(f EncodeErrorFunc) Option {
	return func(o *option) {
		o.errEncoder = f
	}
}

// Uow wrap HandlerFunc with unit of work
func Uow(mgr uow.Manager, handler HandlerFunc, opts ...Option) http.Handler {
	opt := &option{
		skip: func(r *http.Request) bool {
			return contains(SafeMethods, r.Method)
		},
		errEncoder: func(w http.ResponseWriter, r *http.Request, err error) {
			//skip
		},
	}
	for _, o := range opts {
		o(opt)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if opt.skip(r) {
			err := handler(w, r)
			opt.errEncoder(w, r, err)
			return
		}
		//run into unit of work
		err := mgr.WithNew(r.Context(), func(ctx context.Context) error {
			return handler(w, r.WithContext(ctx))
		}, opt.txOpt...)
		opt.errEncoder(w, r, err)
		return
	})
}
