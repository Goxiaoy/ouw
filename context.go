package uow

import "context"

type unitOfWorkKey string

var (
	current unitOfWorkKey = "current"
)

func newCurrentUow(ctx context.Context, u *UnitOfWork) context.Context {
	return context.WithValue(ctx, current, u)
}

func FromCurrentUow(ctx context.Context) (u *UnitOfWork, ok bool) {
	u, ok = ctx.Value(current).(*UnitOfWork)
	return
}
