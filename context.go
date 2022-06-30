package uow

import "context"

type unitOfWorkKey string

var (
	currentKey unitOfWorkKey = "current"
)

func NewCurrentUow(ctx context.Context, u *UnitOfWork) context.Context {
	return context.WithValue(ctx, currentKey, u)
}

func FromCurrentUow(ctx context.Context) (u *UnitOfWork, ok bool) {
	u, ok = ctx.Value(currentKey).(*UnitOfWork)
	return
}
