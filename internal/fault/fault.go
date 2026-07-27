// Package fault defines named internal fault points used by integration and
// crash-recovery tests. Production components default to None.
package fault

import (
	"context"
	"errors"
	"fmt"
)

type Point string

const (
	ProbeReturn                   Point = "probe_return"
	ClaimExecutionLock            Point = "claim_execution_lock"
	ClaimBeforeJournal            Point = "claim_before_journal"
	ClaimBeforeCommit             Point = "claim_before_commit"
	HandlerStart                  Point = "handler_start"
	HandlerReturn                 Point = "handler_return"
	SettleAfterFence              Point = "settle_after_fence"
	SettleAfterAttempt            Point = "settle_after_attempt"
	SettleAfterEvents             Point = "settle_after_events"
	SettleAfterChildren           Point = "settle_after_children"
	SettleBeforeCommitFunction    Point = "settle_before_commit_function"
	SettleAfterCommitFunction     Point = "settle_after_commit_function"
	SettleBeforeCommit            Point = "settle_before_commit"
	SettleCommitAmbiguous         Point = "settle_commit_ambiguous"
	CoordinatorAfterHandler       Point = "coordinator_after_handler"
	CoordinatorBeforeInboxAdvance Point = "coordinator_before_inbox_advance"
	PlanAfterClaim                Point = "plan_after_claim"
	PlanAfterEvaluate             Point = "plan_after_evaluate"
	PlanBeforeCommit              Point = "plan_before_commit"
	RenewBeforeResult             Point = "renew_before_result"
	MaintenanceAfterProbe         Point = "maintenance_after_probe"
	NotifyConnect                 Point = "notify_connect"
	NotifyBeforeWait              Point = "notify_before_wait"
	MigrationEachUnit             Point = "migration_each_unit"
	IngressBeforeJournal          Point = "ingress_before_journal"
	IngressBeforeCommit           Point = "ingress_before_commit"
	IngressCommitAmbiguous        Point = "ingress_commit_ambiguous"
	ClaimCommitAmbiguous          Point = "claim_commit_ambiguous"
)

var ErrInjected = errors.New("flow: injected fault")

type Hook interface {
	Hit(context.Context, Point) error
}

type None struct{}

func (None) Hit(context.Context, Point) error { return nil }

type Func func(context.Context, Point) error

func (f Func) Hit(ctx context.Context, point Point) error {
	if f == nil {
		return nil
	}
	return f(ctx, point)
}

func Injected(point Point) error { return fmt.Errorf("%w at %s", ErrInjected, point) }
