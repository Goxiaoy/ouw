package mock

import (
	"context"
	"database/sql"
	"log"
)

type nonTransactionDb struct {
}

func (n *nonTransactionDb) Begin(ctx context.Context, opt ...*sql.TxOptions) (db interface{}, err error) {
	return n, nil
}

func (n *nonTransactionDb) Commit() error {
	log.Println("Commit")
	return nil
}

func (n *nonTransactionDb) Rollback() error {
	log.Println("Rollback")
	return nil
}
