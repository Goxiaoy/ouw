package dtmcli

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/dtm-labs/logger"
	"github.com/go-saas/uow"
)

type BarrierDbFunc func(ctx context.Context, u *uow.UnitOfWork) *sql.Tx

// CallUow dtm barrier with unit of work. see dtmcli.BranchBarrier.Call
func CallUow(ctx context.Context, mgr uow.Manager, bb *dtmcli.BranchBarrier, barrierDbFunc BarrierDbFunc, fn func(ctx context.Context) error, opt ...*sql.TxOptions) (rerr error) {
	u, err := mgr.CreateNew(ctx, opt...)
	if err != nil {
		return err
	}
	//push into context
	ctx = uow.NewCurrentUow(ctx, u)
	//already transactional ,barrier db is managed by uow now
	barrierDb := barrierDbFunc(ctx, u)

	//dtmcli.BranchBarrier.newBarrierID()
	bb.BarrierID++
	bid := fmt.Sprintf("%02d", bb.BarrierID)

	defer dtmimp.DeferDo(&rerr, func() error {
		return u.Commit()
	}, func() error {
		return u.Rollback()
	})

	originOp := map[string]string{
		dtmimp.OpCancel:     dtmimp.OpTry,    // tcc
		dtmimp.OpCompensate: dtmimp.OpAction, // saga
		dtmimp.OpRollback:   dtmimp.OpAction, // workflow
	}[bb.Op]

	originAffected, oerr := dtmimp.InsertBarrier(barrierDb, bb.TransType, bb.Gid, bb.BranchID, originOp, bid, bb.Op, bb.DBType, bb.BarrierTableName)
	currentAffected, rerr := dtmimp.InsertBarrier(barrierDb, bb.TransType, bb.Gid, bb.BranchID, bb.Op, bid, bb.Op, bb.DBType, bb.BarrierTableName)
	logger.Debugf("originAffected: %d currentAffected: %d", originAffected, currentAffected)

	if rerr == nil && bb.Op == dtmimp.MsgDoOp && currentAffected == 0 { // for msg's DoAndSubmit, repeated insert should be rejected.
		return dtmcli.ErrDuplicated
	}

	if rerr == nil {
		rerr = oerr
	}

	if (bb.Op == dtmimp.OpCancel || bb.Op == dtmimp.OpCompensate || bb.Op == dtmimp.OpRollback) && originAffected > 0 || // null compensate
		currentAffected == 0 { // repeated request or dangled request
		return
	}
	if rerr == nil {
		rerr = fn(ctx)
	}
	return
}
