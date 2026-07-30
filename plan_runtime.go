package flow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/store"
)

const maxPlanInvocationsPerTransaction = 10_000

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
	if err := r.faults.Hit(ctx, fault.PlanAfterClaim); err != nil {
		return false
	}
	snapshot, err := r.store.LoadPlanSnapshot(ctx, semantic)
	if err != nil {
		return false
	}
	args, err := definition.def.Args.Decode(snapshot.Input)
	if err != nil {
		_ = r.failPlan(ctx, candidate.ExecutionID, semantic, "argument_decode", "plan arguments do not match the registered definition")
		return true
	}
	request, invocations, invokeErr, panicked := r.evaluatePlanFixedPointLocked(ctx, semantic, &snapshot, definition, args)
	if invokeErr != nil || panicked {
		reason := "plan function panicked"
		if invokeErr != nil && !panicked {
			reason = safeErrorMessage(invokeErr)
		}
		_ = r.failPlan(ctx, candidate.ExecutionID, semantic, "plan_panic", reason)
		return true
	}
	if err := r.faults.Hit(ctx, fault.PlanAfterEvaluate); err != nil {
		return false
	}
	result, err := r.store.ReconcilePlanLocked(ctx, semantic, request)
	if err != nil {
		if errors.Is(err, flowerr.ErrConflict) || errors.Is(err, flowerr.ErrInvalid) || errors.Is(err, flowerr.ErrInvalidState) {
			_ = r.failPlan(ctx, candidate.ExecutionID, semantic, "plan_defect", safeErrorMessage(err))
			return true
		}
		return false
	}
	if err := r.faults.Hit(ctx, fault.PlanBeforeCommit); err != nil {
		return false
	}
	if err := semantic.Commit(ctx); err != nil {
		return false
	}
	r.observe(context.Background(), Observation{
		Kind: ObservationPlan, Operation: "reconcile", Outcome: result.Status,
		ExecutionID: ExecutionID(candidate.ExecutionID.String()), Name: candidate.Name,
		Version: candidate.Version, Count: int64(invocations), Duration: time.Since(started), Worker: r.replicaName(),
	})
	if result.Created > 0 || result.Skipped > 0 {
		r.wake.signal()
	}
	return true
}

func (r *Runtime) evaluatePlanFixedPointLocked(
	ctx context.Context,
	semantic *store.SemanticTx,
	snapshot *store.PlanSnapshot,
	definition erasedPlan,
	args any,
) (store.PlanReconciliation, int, error, bool) {
	invocations := 0
	request := store.PlanReconciliation{
		ExpectedRevision: snapshot.Revision, ConsumedThrough: snapshot.JournalThrough,
	}
	for {
		plan, err, panicked := r.evaluateCompletePlanLocked(ctx, semantic, snapshot, definition, args, &invocations)
		if err != nil || panicked {
			return store.PlanReconciliation{}, invocations, err, panicked
		}
		delta, err := buildPlanReconciliation(*snapshot, plan)
		if err != nil {
			return store.PlanReconciliation{}, invocations, err, false
		}
		request.WaitingReads = delta.WaitingReads
		request.WaitingOn = append([]string(nil), delta.WaitingOn...)
		if len(delta.Commands) == 0 {
			request.Quiescent = true
			return request, invocations, nil, false
		}
		request.Commands = append(request.Commands, delta.Commands...)
		immediate, expired := applyProvisionalPlanCommands(snapshot, delta.Commands)
		request.ImmediateExpired = append(request.ImmediateExpired, expired...)
		if immediate == 0 {
			request.Quiescent = false
			return request, invocations, nil, false
		}
	}
}

func (r *Runtime) evaluateCompletePlanLocked(
	ctx context.Context,
	semantic *store.SemanticTx,
	snapshot *store.PlanSnapshot,
	definition erasedPlan,
	args any,
	invocations *int,
) (*Plan, error, bool) {
	for {
		*invocations++
		if *invocations > maxPlanInvocationsPerTransaction {
			return nil, fmt.Errorf("%w: plan evaluation exceeded invocation guard", flowerr.ErrInvalidState), false
		}
		plan := newPlan(*snapshot)
		invokeErr, panicked := invokePlan(definition, plan, args)
		if invokeErr != nil || panicked {
			return plan, invokeErr, panicked
		}
		selectors, values := plan.missingSelectors(), plan.missingValues()
		if len(selectors) == 0 && len(values) == 0 {
			if r.planVerification {
				(*invocations)++
				if *invocations > maxPlanInvocationsPerTransaction {
					return nil, fmt.Errorf("%w: plan evaluation exceeded invocation guard", flowerr.ErrInvalidState), false
				}
				second := newPlan(*snapshot)
				secondErr, secondPanicked := invokePlan(definition, second, args)
				if secondErr != nil || secondPanicked {
					return second, secondErr, secondPanicked
				}
				if !second.complete() {
					return nil, fmt.Errorf("%w: identical plan snapshot consulted different unloaded values", flowerr.ErrInvalidState), false
				}
				firstFingerprint, err := planEvaluationFingerprint(plan)
				if err != nil {
					return nil, err, false
				}
				secondFingerprint, err := planEvaluationFingerprint(second)
				if err != nil {
					return nil, err, false
				}
				if !bytes.Equal(firstFingerprint.Bytes, secondFingerprint.Bytes) {
					return nil, fmt.Errorf("%w: plan produced different decisions for an identical snapshot", flowerr.ErrInvalidState), false
				}
			}
			return plan, nil, false
		}
		if len(selectors) > 0 {
			events, err := r.store.LoadPlanEventsLocked(ctx, semantic, snapshot.JournalThrough, selectors)
			if err != nil {
				return nil, err, false
			}
			snapshot.Events = append(snapshot.Events, events...)
			snapshot.LoadedSelectors = append(snapshot.LoadedSelectors, selectors...)
		}
		if len(values) > 0 {
			results, err := r.store.LoadPlanCommandResultsLocked(ctx, semantic, values)
			if err != nil {
				return nil, err, false
			}
			for index := range snapshot.Commands {
				value, requested := results[snapshot.Commands[index].ID]
				if !requested {
					continue
				}
				snapshot.Commands[index].Result = value
				snapshot.Commands[index].ResultLoaded = true
			}
			if len(results) != len(values) {
				return nil, fmt.Errorf("%w: a requested successful plan result is unavailable", flowerr.ErrInvalidState), false
			}
		}
	}
}

func applyProvisionalPlanCommands(snapshot *store.PlanSnapshot, commands []store.CommandCreate) (int, []uuid.UUID) {
	states := make(map[uuid.UUID]string, len(snapshot.Commands)+len(commands))
	for _, command := range snapshot.Commands {
		states[command.ID] = command.State
	}
	provisional := make(map[uuid.UUID]*store.PlanCommandSnapshot, len(commands))
	creates := make(map[uuid.UUID]store.CommandCreate, len(commands))
	for _, command := range commands {
		state := "ready"
		if len(command.Dependencies) > 0 || len(command.Waits) > 0 {
			state = "pending"
		}
		item := store.PlanCommandSnapshot{
			ID: command.ID, Key: command.Key, Name: command.Name, Version: command.Version,
			Origin: command.Origin, State: state, Args: command.Args.BytesCopy(),
			FailureScope: command.FailureScope, DeclarationFingerprint: append([]byte(nil), command.DeclarationFingerprint[:]...),
		}
		provisional[command.ID], creates[command.ID], states[command.ID] = &item, command, state
	}
	for {
		progressed := false
		for id, command := range creates {
			item := provisional[id]
			if isPublicTerminal(item.State) {
				continue
			}
			allSatisfied, impossible := true, false
			for _, group := range command.Dependencies {
				state := provisionalDependencyState(group, states)
				if state == "unsatisfiable" {
					impossible = true
					break
				}
				if state != "satisfied" {
					allSatisfied = false
				}
			}
			if impossible {
				item.State, states[id], progressed = "skipped", "skipped", true
				continue
			}
			if allSatisfied && len(command.Waits) == 0 {
				state := "ready"
				if snapshot.DeadlineAt != nil && !snapshot.DecisionAt.Add(command.InitialDelay).Before(*snapshot.DeadlineAt) {
					state, progressed = "expired", true
				}
				item.State, states[id] = state, state
			}
		}
		if !progressed {
			break
		}
	}
	immediate := 0
	var expired []uuid.UUID
	for _, command := range commands {
		item := *provisional[command.ID]
		switch item.State {
		case "skipped":
			item.FailureCode = "dependency_unsatisfiable"
			item.FailureMessage = "a command dependency became unsatisfiable"
			immediate++
		case "expired":
			item.FailureCode = "initial_schedule_expired"
			item.FailureMessage = "first eligible time is not before the execution deadline"
			expired = append(expired, command.ID)
			immediate++
			if command.Required {
				snapshot.Status = "failing"
			}
		default:
			snapshot.OpenCommands++
		}
		snapshot.Commands = append(snapshot.Commands, item)
		snapshot.CommandCount++
	}
	return immediate, expired
}

func provisionalDependencyState(group store.DependencyGroupCreate, states map[uuid.UUID]string) string {
	succeeded, unsuccessful, terminal := 0, 0, 0
	for _, member := range group.Members {
		state := states[member.CommandID]
		if state == "succeeded" {
			succeeded++
			terminal++
		} else if isPublicTerminal(state) {
			unsuccessful++
			terminal++
		}
	}
	switch group.Kind {
	case "all_succeeded":
		if unsuccessful > 0 {
			return "unsatisfiable"
		}
		if succeeded == len(group.Members) {
			return "satisfied"
		}
	case "all_settled":
		if terminal == len(group.Members) {
			return "satisfied"
		}
	case "all_failed":
		if succeeded > 0 {
			return "unsatisfiable"
		}
		if unsuccessful == len(group.Members) {
			return "satisfied"
		}
	}
	return "unresolved"
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
