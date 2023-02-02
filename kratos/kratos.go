package kratos

import (
	"context"
	"database/sql"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/middleware/selector"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/go-kratos/kratos/v2/transport/http"
	"github.com/go-saas/uow"
	uhttp "github.com/go-saas/uow/http"
	"strings"
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
	skip    SkipFunc
	txOpt   []*sql.TxOptions
	skipOps []string
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

// WithForceSkipOp use selector.Server to skip operation
func WithForceSkipOp(ops ...string) Option {
	return func(o *option) {
		o.skipOps = ops
	}
}

func WithTxOpt(txOpt ...*sql.TxOptions) Option {
	return func(o *option) {
		o.txOpt = txOpt
	}
}

func DefaultSkip() func(ctx context.Context, req interface{}) bool {
	return func(ctx context.Context, req interface{}) bool {
		if t, ok := transport.FromServerContext(ctx); ok {
			//resolve by operation
			if len(t.Operation()) > 0 && skipOperation(t.Operation()) {
				log.Debugf("[uow] safe operation %s. skip uow", t.Operation())
				return true
			}
			// can not identify
			if ht, ok := t.(*http.Transport); ok {
				if contains(uhttp.SafeMethods, ht.Request().Method) {
					//safe method skip unit of work
					log.Debugf("[uow] safe method %s. skip uow", ht.Request().Method)
					return true
				}
			}
			return false
		}
		return false
	}
}

// Uow server unit of work middleware
func Uow(um uow.Manager, opts ...Option) middleware.Middleware {
	opt := &option{
		skip: DefaultSkip(),
	}
	for _, o := range opts {
		o(opt)
	}
	return selector.Server(func(next middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			if opt.skip(ctx, req) {
				return next(ctx, req)
			}
			var res interface{}
			var err error
			// wrap into new unit of work
			log.Debugf("[uow] run into unit of work")
			err = um.WithNew(ctx, func(ctx context.Context) error {
				var err error
				res, err = next(ctx, req)
				return err
			})
			return res, err
		}
	}).Match(func(ctx context.Context, operation string) bool {
		return !contains(opt.skipOps, operation)
	}).Build()
}

// useOperation return true if operation action not start with "get" and "list" (case-insensitive)
func skipOperation(operation string) bool {
	s := strings.Split(operation, "/")
	act := strings.ToLower(s[len(s)-1])
	return strings.HasPrefix(act, "get") || strings.HasPrefix(act, "list")
}
