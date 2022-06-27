package uow

import "context"

type unitOfWorkKey string

var (
	current unitOfWorkKey = "current"
)

func newCurrentUow(ctx context.Context, u *unitOfWork) context.Context {
	return context.WithValue(ctx, current, u)
}

func FromCurrentUow(ctx context.Context) (u *unitOfWork, ok bool) {
	u, ok = ctx.Value(current).(*unitOfWork)
	return
}
