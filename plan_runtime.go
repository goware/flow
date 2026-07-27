package flow

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/store"
)

func (r *Runtime) runPlanScheduler(ctx context.Context) {
	keys := r.registry.planKeys()
	kinds := make([]store.PlanKind, len(keys))
	for index, key := range keys {
		kinds[index] = store.PlanKind{Name: key.name, Version: key.version}
	}
	for {
		if ctx.Err() != nil {
			return
		}
		seen := r.wake.snapshot()
		if len(kinds) == 0 {
			r.wake.wait(ctx, seen, r.pollInterval)
			continue
		}
		candidates, err := r.store.ProbeDirtyPlans(ctx, kinds, 64)
		if err != nil || len(candidates) == 0 {
			r.wake.wait(ctx, seen, r.pollInterval)
			continue
		}
		progress := false
		for _, candidate := range candidates {
			definition, ok := r.registry.plan(candidate.Name, candidate.Version)
			if !ok {
				continue
			}
			if r.reconcilePlan(ctx, candidate, definition) {
				progress = true
			}
		}
		if !progress {
			r.wake.wait(ctx, seen, r.pollInterval)
		}
	}
}

func (r *Runtime) reconcilePlan(ctx context.Context, candidate store.PlanCandidate, definition erasedPlan) bool {
	started := time.Now()
	semantic, err := r.store.BeginSemantic(ctx, candidate.ExecutionID, store.LockSkipLocked)
	if errors.Is(err, store.ErrLockUnavailable) {
		return false
	}
	if err != nil {
		return false
	}
	defer semantic.Rollback(context.WithoutCancel(ctx))
	snapshot, err := r.store.LoadPlanSnapshot(ctx, semantic)
	if err != nil {
		return false
	}
	args, err := definition.def.Args.Decode(snapshot.Input)
	if err != nil {
		_ = r.failPlan(ctx, candidate.ExecutionID, semantic, "argument_decode", "plan arguments do not match the registered definition")
		return true
	}
	plan := newPlan(snapshot)
	invokeErr, panicked := invokePlan(definition, plan, args)
	if invokeErr != nil || panicked {
		reason := "plan function panicked"
		if invokeErr != nil && !panicked {
			reason = safeErrorMessage(invokeErr)
		}
		_ = r.failPlan(ctx, candidate.ExecutionID, semantic, "plan_panic", reason)
		return true
	}
	request, err := buildPlanReconciliation(snapshot, plan)
	if err != nil {
		_ = r.failPlan(ctx, candidate.ExecutionID, semantic, "plan_defect", safeErrorMessage(err))
		return true
	}
	result, err := r.store.ReconcilePlanLocked(ctx, semantic, request)
	if err != nil {
		if errors.Is(err, flowerr.ErrConflict) || errors.Is(err, flowerr.ErrInvalid) || errors.Is(err, flowerr.ErrInvalidState) {
			_ = r.failPlan(ctx, candidate.ExecutionID, semantic, "plan_defect", safeErrorMessage(err))
			return true
		}
		return false
	}
	if err := semantic.Commit(ctx); err != nil {
		return false
	}
	r.observe(context.Background(), Observation{
		Kind: ObservationPlan, Operation: "reconcile", Outcome: result.Status,
		ExecutionID: ExecutionID(candidate.ExecutionID.String()), Name: candidate.Name,
		Version: candidate.Version, Count: int64(result.Created), Duration: time.Since(started), Worker: r.replicaName(),
	})
	if result.Created > 0 || result.Skipped > 0 {
		r.wake.signal()
	}
	return true
}

func invokePlan(definition erasedPlan, plan *Plan, args any) (err error, panicked bool) {
	defer func() {
		if recover() != nil {
			err = errors.New("plan panicked")
			panicked = true
		}
	}()
	return definition.invoke(plan, args), false
}

func (r *Runtime) failPlan(ctx context.Context, id uuid.UUID, semantic *store.SemanticTx, code, reason string) error {
	if semantic == nil {
		return errors.New("plan semantic transaction is unavailable")
	}
	_ = semantic.Rollback(context.WithoutCancel(ctx))
	failureTx, err := r.store.BeginSemantic(ctx, id, store.LockBlocking)
	if err != nil {
		return err
	}
	defer failureTx.Rollback(context.WithoutCancel(ctx))
	if err := r.store.FailPlanLocked(ctx, failureTx, code, reason); err != nil {
		return err
	}
	if err := failureTx.Commit(ctx); err != nil {
		return err
	}
	r.observe(context.Background(), Observation{
		Kind: ObservationPlan, Operation: "reconcile", Outcome: "failed", ExecutionID: ExecutionID(id.String()), Worker: r.replicaName(),
	})
	return nil
}
