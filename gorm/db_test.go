package gorm

import (
	"context"
	"fmt"
	"github.com/go-saas/uow"
	"github.com/mattn/go-sqlite3"
	sqldblogger "github.com/simukti/sqldb-logger"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"os"
	"testing"
)

type post struct {
	gorm.Model
}

var (
	client         *gorm.DB
	clientResolver func(ctx context.Context) *gorm.DB
)

type L struct {
}

func (l L) Log(ctx context.Context, level sqldblogger.Level, msg string, data map[string]interface{}) {
	fmt.Println(msg)
	fmt.Printf("%+v", data)
}

func TestMain(m *testing.M) {
	var err error
	db := sqldblogger.OpenDriver("file:test.DB?cache=shared&mode=memory", &sqlite3.SQLiteDriver{}, L{})

	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)

	client, err = gorm.Open(&sqlite.Dialector{
		DriverName: sqlite.DriverName,
		Conn:       db,
	}, &gorm.Config{SkipDefaultTransaction: true, DisableNestedTransaction: false})
	if err != nil {
		panic(err)
	}

	err = client.AutoMigrate(&post{})
	if err != nil {
		panic(err)
	}

	clientResolver = func(ctx context.Context) *gorm.DB {
		u, ok := uow.FromCurrentUow(ctx)
		if !ok {
			panic("can not find uow")
		}
		db, err := u.GetTxDb(ctx)
		if err != nil {
			panic(err)
		}
		return db.(*TransactionDb).DB
	}

	exitCode := m.Run()
	os.Exit(exitCode)
}

func TestCommit(t *testing.T) {
	mgr := uow.NewManager(func(ctx context.Context, keys ...string) (uow.TransactionalDb, error) {
		return NewTransactionDb(client), nil
	})

	//run function with unit of work and commit
	err := mgr.WithNew(context.Background(), func(ctx context.Context) error {
		err := clientResolver(ctx).Create(&post{gorm.Model{ID: 1001}}).Error
		assert.NoError(t, err)
		return err
	})
	assert.NoError(t, err)

	p := &post{}
	err = client.Find(p, "id = ?", 1001).Error
	assert.NoError(t, err)
	assert.Equal(t, uint(1001), p.ID)
}

func TestRollback(t *testing.T) {
	mgr := uow.NewManager(func(ctx context.Context, keys ...string) (uow.TransactionalDb, error) {
		return NewTransactionDb(client), nil
	})
	//run function with unit of work and rollback
	err := mgr.WithNew(context.Background(), func(ctx context.Context) error {

		err := clientResolver(ctx).Create(&post{gorm.Model{ID: 1000}}).Error
		assert.NoError(t, err)
		//just return fake err to trigger transaction rollback
		return fmt.Errorf("fake error")
	})
	assert.Error(t, err)

	//Sqlite tx is not rollback immediately
	//p := &post{}
	//err = client.Find(p, "id = ?", 1000).Error
	//assert.ErrorIs(t, err, gorm.ErrRecordNotFound)

	fakeError := fmt.Errorf("fake error")
	//run function with unit of work and panic rollback

	assert.PanicsWithValue(t, fakeError, func() {
		err = mgr.WithNew(context.Background(), func(ctx context.Context) error {
			err := clientResolver(ctx).Create(&post{gorm.Model{ID: 2000}}).Error
			assert.NoError(t, err)
			//just return fake err to trigger transaction rollback
			panic(fakeError)
		})
	})

	//p := &post{}
	//err = client.Find(p, "id = ?", 2000).Error
	//assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
}

func TestGormNested(t *testing.T) {
	err := client.Transaction(func(client *gorm.DB) error {
		err := client.Create(&post{gorm.Model{ID: 3002}}).Error
		if err != nil {
			return err
		}
		return client.Transaction(func(client *gorm.DB) error {
			err := client.Create(&post{gorm.Model{ID: 3004}}).Error
			if err != nil {
				return err
			}
			return client.Transaction(func(client *gorm.DB) error {
				return client.Create(&post{gorm.Model{ID: 3005}}).Error
			})
		})
	})
	if err != nil {
		panic(err)
	}
}

func TestNested(t *testing.T) {

	mgr := uow.NewManager(func(ctx context.Context, keys ...string) (uow.TransactionalDb, error) {
		return NewTransactionDb(client), nil
	})

	//level 1
	err := mgr.WithNew(context.Background(), func(ctx context.Context) error {

		err := clientResolver(ctx).Create(&post{gorm.Model{ID: 1002}}).Error
		assert.NoError(t, err)

		//level 2
		err = mgr.WithNew(ctx, func(ctx context.Context) error {
			err := clientResolver(ctx).Create(&post{gorm.Model{ID: 1003}}).Error
			assert.NoError(t, err)

			//level 3
			err = mgr.WithNew(ctx, func(ctx context.Context) error {
				err := clientResolver(ctx).Create(&post{gorm.Model{ID: 1004}}).Error
				assert.NoError(t, err)
				return err
			})
			assert.NoError(t, err)

			return err
		})
		assert.NoError(t, err)

		return err
	})
	assert.NoError(t, err)
}

func TestNestedDisable(t *testing.T) {

	mgr := uow.NewManager(func(ctx context.Context, keys ...string) (uow.TransactionalDb, error) {
		return NewTransactionDb(client), nil
	}, uow.WithDisableNestedNestedTransaction())

	//level 1
	err := mgr.WithNew(context.Background(), func(ctx context.Context) error {

		err := clientResolver(ctx).Create(&post{gorm.Model{ID: 4002}}).Error
		assert.NoError(t, err)

		//level 2
		err = mgr.WithNew(ctx, func(ctx context.Context) error {
			err := clientResolver(ctx).Create(&post{gorm.Model{ID: 4003}}).Error
			assert.NoError(t, err)

			//level 3
			err = mgr.WithNew(ctx, func(ctx context.Context) error {
				err := clientResolver(ctx).Create(&post{gorm.Model{ID: 4004}}).Error
				assert.NoError(t, err)
				return err
			})
			assert.NoError(t, err)

			return err
		})
		assert.NoError(t, err)

		return err
	})
	assert.NoError(t, err)
}
