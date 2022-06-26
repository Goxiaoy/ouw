package kratos

import (
	"context"
	"database/sql"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/go-kratos/kratos/v2/transport/http"
	"github.com/go-saas/uow"
	"strings"
)

var (
	safeMethods = []string{"GET", "HEAD", "OPTIONS", "TRACE"}
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
type SkipFunc func(ctx context.Context, req interface{}) bool

type option struct {
	skip  SkipFunc
	txOpt []*sql.TxOptions
	l     log.Logger
}

type Option func(*option)

// WithSkip change the skip unit of work function.
//
// default request will skip operation method prefixed by "get" and "list" (case-insensitive)
// default http request will skip safeMethods like "GET", "HEAD", "OPTIONS", "TRACE"
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

func WithLogger(l log.Logger) Option {
	return func(o *option) {
		o.l = l
	}
}

func Uow(um uow.Manager, opts ...Option) middleware.Middleware {
	opt := &option{
		l: log.GetLogger(),
	}
	for _, o := range opts {
		o(opt)
	}
	logger := log.NewHelper(opt.l)

	skip := func(ctx context.Context, req interface{}) bool {
		if t, ok := transport.FromServerContext(ctx); ok {
			//resolve by operation
			if len(t.Operation()) > 0 && skipOperation(t.Operation()) {
				logger.Debugf("safe operation %s. skip uow", t.Operation())
				return true
			}
			// can not identify
			if ht, ok := t.(*http.Transport); ok {
				if contains(safeMethods, ht.Request().Method) {
					//safe method skip unit of work
					logger.Debugf("safe method %s. skip uow", ht.Request().Method)
					return true
				}
			}
			return false
		}
		return false
	}
	opt.skip = skip
	return func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			if opt.skip(ctx, req) {
				return next(ctx, req)
			}
			var res interface{}
			var err error
			// wrap into new unit of work
			logger.Debugf("run into unit of work")
			err = um.WithNew(ctx, func(ctx context.Context) error {
				var err error
				res, err = next(ctx, req)
				return err
			})
			return res, err
		}
	}
}

//useOperation return true if operation action not start with "get" and "list" (case-insensitive)
func skipOperation(operation string) bool {
	s := strings.Split(operation, "/")
	act := strings.ToLower(s[len(s)-1])
	return strings.HasPrefix(act, "get") || strings.HasPrefix(act, "list")
}
