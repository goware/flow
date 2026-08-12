package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/failure"
	"github.com/goware/flow/internal/fault"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/store/journalcodec"
	"github.com/goware/flow/internal/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	commandProbeFactor       = 4
	maxCommandProbe          = 256
	maxCommandRoundsPerTurn  = maxCommandProbe + 1
	maxConcurrentClaims      = 8
	claimMaintenanceHeadroom = 2
	maxCommandResultBytes    = 256 << 10
	settlementAttempts       = 3
)

var errCommitPanicked = errors.New("command commit function panicked")

func (r *Runtime) runCommandScheduler(ctx context.Context) {
	slots := newCommandSlots(r.workerConcurrency, r.queueConcurrency)
	queueTurn := 0
	keys := r.registry.workerKeys()
	kinds := make([]store.CommandKind, len(keys))
	for index, key := range keys {
		kinds[index] = store.CommandKind{Name: key.name, Version: key.version}
	}
	var continuationAfter *store.CommandProbeCursor
	revisitHead := false
	for {
		if ctx.Err() != nil {
			return
		}
		seen := r.wake.snapshot()
		free := slots.free()
		if free == 0 || len(kinds) == 0 {
			r.wake.wait(ctx, seen, r.pollInterval)
			continue
		}
		progress := false
		excludedRuns := make(map[uuid.UUID]struct{})
		excludedQueues := make(map[string]struct{})
		probeAfter := continuationAfter
		headRevisit := revisitHead && continuationAfter != nil
		resumeAfter := continuationAfter
		if headRevisit {
			probeAfter = nil
			revisitHead = false
		}
		reachedEnd := false
		for rounds := 0; rounds < maxCommandRoundsPerTurn && slots.free() > 0 && ctx.Err() == nil; rounds++ {
			free = slots.free()
			limit := min(maxCommandProbe, max(free, free*commandProbeFactor))
			atExclusionCap := len(excludedRuns)+len(excludedQueues) >= maxCommandProbe
			started := time.Now()
			candidates, err := r.store.ProbeCommandsExcluding(ctx, kinds, limit,
				runIDs(excludedRuns), queueNames(excludedQueues), probeAfter)
			if err == nil {
				err = r.faults.Hit(ctx, fault.ProbeReturn)
			}
			r.observe(ctx, Observation{
				Kind: ObservationClaim, Operation: "probe", Outcome: outcomeForError(err),
				Count: int64(len(candidates)), Duration: time.Since(started), Worker: r.replicaName(),
			})
			if err != nil || len(candidates) == 0 {
				if err == nil {
					reachedEnd = true
				}
				break
			}

			ordered := fairQueueCandidates(candidates, &queueTurn)
			if atExclusionCap {
				// With the bounded exclusion set full, inspect exactly the earliest
				// remaining database-ordered candidate. If it is blocked, advancing
				// past that known candidate rotates beyond the stable prefix without
				// skipping any unexamined work.
				ordered = candidates[:1]
			}
			selected := make([]store.CommandCandidate, 0, free)
			queueExclusionsBeforeSelection := len(excludedQueues)
			for _, candidate := range ordered {
				if slots.free() == 0 {
					break
				}
				reserved, laneFull := slots.reserve(candidate.Queue)
				if reserved {
					selected = append(selected, candidate)
				} else if laneFull {
					excludedQueues[candidate.Queue] = struct{}{}
				}
			}
			if len(selected) == 0 {
				if len(excludedQueues) > queueExclusionsBeforeSelection {
					if atExclusionCap {
						probeAfter = commandProbeCursor(candidates[0])
						clear(excludedRuns)
						clear(excludedQueues)
					}
					continue
				}
				break
			}
			if ctx.Err() != nil {
				for _, candidate := range selected {
					slots.release(candidate.Queue)
				}
				return
			}
			exclusionsBefore := len(excludedRuns) + len(excludedQueues)
			roundProgress := false
			roundCommands := 0
			groups := groupCandidatesByRun(selected)
			for _, claimedGroup := range r.claimRunGroups(ctx, groups) {
				group, result, claimErr := claimedGroup.candidates, claimedGroup.result, claimedGroup.err
				claimedIDs := make(map[uuid.UUID]struct{}, len(result.Commands))
				for _, command := range result.Commands {
					claimedIDs[command.CommandID] = struct{}{}
				}
				for _, candidate := range group {
					if _, claimed := claimedIDs[candidate.CommandID]; !claimed {
						slots.release(candidate.Queue)
					}
				}
				if result.Progressed {
					progress = true
					if claimErr == nil {
						roundProgress = true
					}
				}
				if !result.Progressed && len(result.Commands) == 0 && len(group) > 0 {
					excludedRuns[group[0].RunID] = struct{}{}
				}
				if claimErr != nil || len(result.Commands) == 0 {
					continue
				}
				for _, command := range result.Commands {
					worker, ok := r.registry.worker(command.Name, command.Version)
					if !ok {
						slots.release(command.Queue)
						continue
					}
					progress = true
					roundCommands++
					r.workerGroup.Add(1)
					go r.executeClaim(worker, command, slots)
				}
			}
			if atExclusionCap {
				if !roundProgress && roundCommands == 0 {
					probeAfter = commandProbeCursor(candidates[0])
					clear(excludedRuns)
					clear(excludedQueues)
				}
				continue
			}
			if len(excludedRuns)+len(excludedQueues) == exclusionsBefore {
				break
			}
		}
		if headRevisit {
			// A head-revisit turn is intentionally bounded too. Resume the saved
			// tail cursor on the next turn regardless of how far the head sweep got.
			continuationAfter = resumeAfter
		} else if reachedEnd {
			continuationAfter = nil
			revisitHead = false
		} else {
			continuationAfter = probeAfter
			revisitHead = continuationAfter != nil
		}
		if !progress {
			r.wake.wait(ctx, seen, r.pollInterval)
		}
	}
}

func commandProbeCursor(candidate store.CommandCandidate) *store.CommandProbeCursor {
	return &store.CommandProbeCursor{
		NextRunAt: candidate.NextRunAt,
		Queue:     candidate.Queue,
		CommandID: candidate.CommandID,
	}
}

func runIDs(runs map[uuid.UUID]struct{}) []uuid.UUID {
	result := make([]uuid.UUID, 0, len(runs))
	for runID := range runs {
		result = append(result, runID)
	}
	return result
}

func queueNames(queues map[string]struct{}) []string {
	result := make([]string, 0, len(queues))
	for queue := range queues {
		result = append(result, queue)
	}
	return result
}

type commandGroupClaim struct {
	candidates []store.CommandCandidate
	result     store.ClaimBatchResult
	err        error
}

// claimRunGroups runs at most one transaction per run and waits
// for the complete selected set before the scheduler probes again. Worker
// accounting remains scheduler-owned after this function returns, so Run
// cannot begin waiting while a claim goroutine might still call WaitGroup.Add.
func (r *Runtime) claimRunGroups(ctx context.Context, groups [][]store.CommandCandidate) []commandGroupClaim {
	results := make([]commandGroupClaim, len(groups))
	if len(groups) == 0 {
		return results
	}
	jobs := make(chan int)
	var group sync.WaitGroup
	workers := min(len(groups), claimConcurrencyLimit(r.workerConcurrency, int(r.db.Conn.Config().MaxConns)))
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			for index := range jobs {
				results[index].candidates = groups[index]
				if ctx.Err() != nil {
					results[index].err = ctx.Err()
					continue
				}
				results[index].result, results[index].err = r.claimRunGroup(ctx, groups[index])
			}
		}()
	}
	for index := range groups {
		jobs <- index
	}
	close(jobs)
	group.Wait()
	return results
}

func (r *Runtime) claimRunGroup(
	ctx context.Context,
	group []store.CommandCandidate,
) (store.ClaimBatchResult, error) {
	started := time.Now()
	result, err := r.store.ClaimCommands(ctx, group, r.commandLease, r.replicaName(), r.faults)
	for index := range result.Commands {
		window := max(time.Duration(0), result.Commands[index].LeaseExpiresAt.Sub(result.Commands[index].DBNow))
		result.Commands[index].LocalLeaseExpiresAt = started.Add(window)
	}
	if err != nil && len(result.Commands) > 0 {
		resolveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), min(5*time.Second, max(100*time.Millisecond, r.commandLease/2)))
		defer cancel()
		possiblyOwned := result.Commands[:0]
		for _, command := range result.Commands {
			ownership, resolveErr := r.store.ResolveCommandAttempt(resolveCtx, command.CommandID, command.AttemptID, command.LeaseToken)
			if resolveErr != nil || ownership == store.AttemptOwnershipStillOwned {
				// A resolver failure cannot prove the commit rolled back. Retain the
				// prepared fence and transfer it to worker accounting so a possibly
				// committed running attempt is never silently abandoned.
				possiblyOwned = append(possiblyOwned, command)
			}
		}
		result.Commands = possiblyOwned
		if len(possiblyOwned) > 0 {
			err = nil
		}
	}
	r.observe(ctx, Observation{
		Kind: ObservationClaim, Operation: "claim", Outcome: outcomeForError(err),
		RunID: RunID(group[0].RunID.String()), Count: int64(len(result.Commands)),
		Duration: time.Since(started), Worker: r.replicaName(),
	})
	return result, err
}

func claimConcurrencyLimit(workerConcurrency, poolCapacity int) int {
	limit := min(maxConcurrentClaims, max(1, workerConcurrency))
	if poolCapacity > claimMaintenanceHeadroom {
		limit = min(limit, poolCapacity-claimMaintenanceHeadroom)
	} else {
		limit = 1
	}
	return max(1, limit)
}

type commandSlots struct {
	global chan struct{}
	mu     sync.Mutex
	limits map[string]int
	active map[string]int
}

func newCommandSlots(global int, limits map[string]int) *commandSlots {
	return &commandSlots{
		global: make(chan struct{}, global), limits: cloneIntMap(limits), active: make(map[string]int),
	}
}

func (slots *commandSlots) free() int { return cap(slots.global) - len(slots.global) }

func (slots *commandSlots) reserve(queue string) (reserved, laneFull bool) {
	slots.mu.Lock()
	defer slots.mu.Unlock()
	if limit := slots.limits[queue]; limit > 0 && slots.active[queue] >= limit {
		return false, true
	}
	select {
	case slots.global <- struct{}{}:
		slots.active[queue]++
		return true, false
	default:
		return false, false
	}
}

func (slots *commandSlots) release(queue string) {
	slots.mu.Lock()
	if slots.active[queue] > 1 {
		slots.active[queue]--
	} else {
		delete(slots.active, queue)
	}
	slots.mu.Unlock()
	<-slots.global
}

func fairQueueCandidates(candidates []store.CommandCandidate, turn *int) []store.CommandCandidate {
	if len(candidates) < 2 {
		return candidates
	}
	byQueue := make(map[string][]store.CommandCandidate)
	queues := make([]string, 0)
	for _, candidate := range candidates {
		if _, exists := byQueue[candidate.Queue]; !exists {
			queues = append(queues, candidate.Queue)
		}
		byQueue[candidate.Queue] = append(byQueue[candidate.Queue], candidate)
	}
	if len(queues) < 2 {
		return candidates
	}
	sort.Strings(queues)
	start := 0
	if turn != nil {
		start = *turn % len(queues)
		*turn = (start + 1) % len(queues)
	}
	ordered := make([]store.CommandCandidate, 0, len(candidates))
	for offset := 0; len(ordered) < len(candidates); offset++ {
		for queueOffset := range len(queues) {
			queue := queues[(start+queueOffset)%len(queues)]
			if offset < len(byQueue[queue]) {
				ordered = append(ordered, byQueue[queue][offset])
			}
		}
	}
	return ordered
}

func groupCandidatesByRun(candidates []store.CommandCandidate) [][]store.CommandCandidate {
	groups := make([][]store.CommandCandidate, 0, len(candidates))
	indexes := make(map[uuid.UUID]int, len(candidates))
	for _, candidate := range candidates {
		index, exists := indexes[candidate.RunID]
		if !exists {
			index = len(groups)
			indexes[candidate.RunID] = index
			groups = append(groups, nil)
		}
		groups[index] = append(groups[index], candidate)
	}
	return groups
}

func (r *Runtime) executeClaim(worker erasedWorker, claim store.ClaimedCommand, slots *commandSlots) {
	defer r.workerGroup.Done()
	baseCtx, cancelCause := context.WithCancelCause(context.Background())
	workerCtx := baseCtx
	cancelDeadline := func() {}
	if remaining, ok := commandAttemptRemaining(claim); ok {
		var cancel context.CancelFunc
		workerCtx, cancel = context.WithTimeoutCause(baseCtx, max(0, remaining), errAttemptTimeout)
		cancelDeadline = cancel
	}
	localLeaseExpiry := claim.LocalLeaseExpiresAt
	if localLeaseExpiry.IsZero() {
		localLeaseExpiry = time.Now().Add(max(time.Duration(0), claim.LeaseExpiresAt.Sub(claim.DBNow)))
	}
	r.active.register(activeCommand{
		commandID: claim.CommandID, attemptID: claim.AttemptID, token: claim.LeaseToken,
		localExpiry: localLeaseExpiry, cancel: cancelCause,
	})
	r.mu.RLock()
	stopping := r.lifecycle == runtimeStopping || r.lifecycle == runtimeStopped
	r.mu.RUnlock()
	if stopping {
		r.active.cancelAttempt(claim.CommandID, claim.AttemptID, errRuntimeShutdown)
	}
	defer func() {
		cancelDeadline()
		cancelCause(nil)
		r.active.unregister(claim.CommandID, claim.AttemptID)
		slots.release(claim.Queue)
		r.wake.signal()
	}()

	args, err := worker.command.Args.Decode(claim.Args)
	if err != nil {
		r.concludeClaim(context.Background(), claim, classifiedConclusion{
			class: retrypolicy.ClassPermanent, code: "argument_decode", message: "stored command arguments do not match the registered definition",
		})
		return
	}
	info := CommandInfo{
		RunID: RunID(claim.RunID.String()), RunKey: claim.RunKey, CommandID: CommandID(claim.CommandID.String()),
		CommandKey: claim.CommandKey, Name: claim.Name, Version: claim.Version,
		CreatedAt: claim.CreatedAt, BudgetStartedAt: claim.BudgetStartedAt,
		Attempt: claim.Attempt, AttemptStartedAt: claim.DBNow,
	}
	scope := &workScope{args: args, info: info}
	if len(claim.EventInputs) > 0 {
		var duplicate bool
		scope.state.eventInputs, duplicate = claimedEventInputSnapshots(claim.EventInputs)
		if duplicate {
			r.concludeClaim(context.Background(), claim, classifiedConclusion{
				class: retrypolicy.ClassPermanent, code: "event_input_decode", message: "claimed command contains duplicate event inputs",
			})
			return
		}
	}
	if err := r.faults.Hit(workerCtx, fault.HandlerStart); err != nil {
		r.concludeClaim(workerCtx, claim, classifiedConclusion{class: retrypolicy.ClassInterrupted, code: "handler_start_interrupted", message: "handler start was interrupted"})
		return
	}
	started := time.Now()
	result, workerErr, panicked := invokeWorker(workerCtx, worker, scope)
	if hookErr := r.faults.Hit(workerCtx, fault.HandlerReturn); hookErr != nil {
		workerErr = hookErr
	}
	r.observe(context.Background(), Observation{
		Kind: ObservationAttempt, Operation: "handler", Outcome: outcomeForError(workerErr),
		RunID: info.RunID, CommandID: info.CommandID, CommandKey: info.CommandKey,
		Name: info.Name, Version: info.Version, Queue: claim.Queue, Worker: r.replicaName(), Duration: time.Since(started),
	})
	if cause := context.Cause(workerCtx); cause != nil {
		r.concludeClaim(context.Background(), claim, classifyWorkerError(cause, false))
		return
	}
	if panicked || workerErr != nil || scope.state.firstError != nil {
		if scope.state.firstError != nil {
			workerErr = scope.state.firstError
		}
		r.concludeClaim(context.Background(), claim, classifyWorkerError(workerErr, panicked))
		return
	}
	encoded, err := worker.command.Result.Encode(result, maxCommandResultBytes)
	if err != nil {
		r.concludeClaim(context.Background(), claim, classifiedConclusion{
			class: retrypolicy.ClassPermanent, code: "result_encode", message: "worker result is invalid or exceeds the result limit",
		})
		return
	}
	events, children, err := prepareWorkerDecision(scope, claim)
	if err != nil {
		r.concludeClaim(context.Background(), claim, classifiedConclusion{
			class: retrypolicy.ClassPermanent, code: "invalid_decision", message: safeErrorMessage(err),
		})
		return
	}
	commit := func(tx pgx.Tx) error { return nil }
	if worker.commit != nil {
		commit = func(tx pgx.Tx) (resultErr error) {
			defer func() {
				if recover() != nil {
					resultErr = errCommitPanicked
				}
			}()
			commitErr := worker.commit(workerCtx, tx, args, result, info)
			if scope.state.firstError != nil {
				return scope.state.firstError
			}
			return commitErr
		}
	} else {
		commit = nil
	}
	for attempt := 0; attempt < settlementAttempts; attempt++ {
		settleResult, settleErr := r.store.SettleCommandSuccess(context.Background(), store.CommandSuccess{
			Claim: claim, Result: encoded, Events: events, Children: children, Commit: commit,
		}, r.faults)
		if settleErr == nil {
			switch settleResult.Status {
			case "succeeded":
				for _, event := range events {
					r.observe(context.Background(), Observation{
						Kind: ObservationEvent, Operation: "settle", Outcome: "accepted",
						RunID: info.RunID, CommandID: info.CommandID, CommandKey: info.CommandKey,
						Name: event.Name, Worker: r.replicaName(),
					})
				}
				r.observe(context.Background(), Observation{
					Kind: ObservationAttempt, Operation: "settle", Outcome: "succeeded",
					RunID: info.RunID, CommandID: info.CommandID, CommandKey: info.CommandKey,
					Name: info.Name, Version: info.Version, Queue: claim.Queue, Worker: r.replicaName(), Count: int64(len(events)),
				})
				return
			case "expired":
				r.observe(context.Background(), Observation{
					Kind: ObservationAttempt, Operation: "settle", Outcome: "expired",
					RunID: info.RunID, CommandID: info.CommandID, CommandKey: info.CommandKey,
					Name: info.Name, Version: info.Version, Queue: claim.Queue, Worker: r.replicaName(),
				})
				return
			default:
				settleErr = newError(ErrInvalidState, "settle", "status", settleResult.Status, "successful settlement returned an unknown status")
			}
		}
		var commitErr *store.CommitFunctionError
		if errors.As(settleErr, &commitErr) {
			if errors.Is(commitErr.Err, errCommitPanicked) {
				r.concludeClaim(context.Background(), claim, classifyWorkerError(commitErr.Err, true))
				return
			}
			if errors.Is(commitErr.Err, ErrConflict) || errors.Is(commitErr.Err, ErrInvalid) ||
				errors.Is(commitErr.Err, ErrInvalidState) || errors.Is(commitErr.Err, ErrPayloadTooLarge) {
				r.concludeClaim(context.Background(), claim, classifiedConclusion{
					class: retrypolicy.ClassPermanent, code: "invalid_decision", message: safeErrorMessage(commitErr.Err),
				})
				return
			}
			r.concludeClaim(context.Background(), claim, classifyWorkerError(commitErr.Err, false))
			return
		}
		if errors.Is(settleErr, ErrConflict) || errors.Is(settleErr, ErrInvalid) ||
			errors.Is(settleErr, ErrInvalidState) || errors.Is(settleErr, ErrPayloadTooLarge) {
			r.concludeClaim(context.Background(), claim, classifiedConclusion{
				class: retrypolicy.ClassPermanent, code: "invalid_decision", message: safeErrorMessage(settleErr),
			})
			return
		}
		ownership, resolveErr := r.store.ResolveCommandAttempt(context.Background(), claim.CommandID, claim.AttemptID, claim.LeaseToken)
		if resolveErr == nil && ownership == store.AttemptOwnershipConcluded {
			return
		}
		if resolveErr == nil && ownership == store.AttemptOwnershipLost || errors.Is(settleErr, ErrLeaseLost) || errors.Is(settleErr, ErrTerminal) {
			return
		}
		if attempt+1 < settlementAttempts {
			time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
		}
	}
	r.observe(context.Background(), Observation{
		Kind: ObservationAttempt, Operation: "settle", Outcome: "error",
		RunID: info.RunID, CommandID: info.CommandID, CommandKey: info.CommandKey,
		Name: info.Name, Version: info.Version, Queue: claim.Queue, Worker: r.replicaName(),
	})
}

func claimedEventInputSnapshots(inputs []store.ClaimedEventInput) (map[string]eventInputSnapshot, bool) {
	snapshots := make(map[string]eventInputSnapshot, len(inputs))
	for _, input := range inputs {
		identity := input.Name + "\x00" + input.Key
		if _, duplicate := snapshots[identity]; duplicate {
			return nil, true
		}
		// Claim materialization allocated this payload from the immutable journal
		// body. Ownership transfers directly into the private attempt snapshot.
		snapshots[identity] = eventInputSnapshot{position: input.Position, payload: input.Payload}
	}
	return snapshots, false
}

func prepareWorkerDecision(scope *workScope, claim store.ClaimedCommand) ([]store.ApplicationEvent, []store.CommandCreate, error) {
	if err := validateDecisionCommands(scope.state.decision); err != nil {
		return nil, nil, err
	}
	stagedEvents := scope.state.decision.orderedEvents()
	events := make([]store.ApplicationEvent, 0, len(stagedEvents))
	for _, staged := range stagedEvents {
		body, err := canonical.Marshal(journalcodec.ApplicationEventBody{
			V: journalcodec.ApplicationEventBodyVersion, Payload: json.RawMessage(staged.payload.BytesCopy()),
		}, 0)
		if err != nil {
			return nil, nil, newError(ErrInvalid, "settle", "event", staged.key, "event body cannot be journaled")
		}
		events = append(events, store.ApplicationEvent{
			ID: uuid.New(), Name: staged.definition.Name, Key: staged.key, Body: body,
		})
	}
	stagedCommands := scope.state.decision.orderedCommands()
	children := make([]store.CommandCreate, 0, len(stagedCommands))
	for _, staged := range stagedCommands {
		child, err := prepareCommand(uuid.New(), staged.key, staged.definition, staged.defaults, staged.args)
		if err != nil {
			return nil, nil, err
		}
		child.ParentCommandID = cloneUUIDPointer(claim.CommandID)
		if staged.startAfter > 0 {
			child.InitialDelay = staged.startAfter
		}
		for _, wait := range staged.waits {
			child.Waits = append(child.Waits, store.EventWaitCreate{Name: wait.name, Key: wait.key})
		}
		child.Within = staged.within
		child.DeclarationFingerprint, err = commandDeclarationFingerprint(child)
		if err != nil {
			return nil, nil, err
		}
		children = append(children, child)
	}
	return events, children, nil
}

func cloneUUIDPointer(value uuid.UUID) *uuid.UUID {
	copy := value
	return &copy
}

func invokeWorker(ctx context.Context, worker erasedWorker, scope *workScope) (result any, err error, panicked bool) {
	defer func() {
		if recover() != nil {
			result = nil
			err = errors.New("worker panicked")
			panicked = true
		}
	}()
	result, err = worker.invoke(ctx, scope)
	return result, err, false
}

type classifiedConclusion struct {
	class         retrypolicy.ErrorClass
	explicitDelay *time.Duration
	code          string
	message       string
}

func classifyWorkerError(err error, panicked bool) classifiedConclusion {
	if panicked {
		return classifiedConclusion{class: retrypolicy.ClassPanic, code: "panic", message: "worker panicked"}
	}
	switch {
	case errors.Is(err, ErrLeaseLost):
		return classifiedConclusion{class: retrypolicy.ClassLeaseLost, code: "lease_lost", message: "command lease was lost"}
	case errors.Is(err, errRuntimeShutdown):
		return classifiedConclusion{class: retrypolicy.ClassInterrupted, code: "shutdown", message: "runtime shutdown interrupted the attempt"}
	case errors.Is(err, errAttemptTimeout), errors.Is(err, context.DeadlineExceeded):
		return classifiedConclusion{class: retrypolicy.ClassTimeout, code: "attempt_timeout", message: "command attempt timed out"}
	case failure.IsPermanent(err):
		return classifiedConclusion{class: retrypolicy.ClassPermanent, code: "permanent", message: safeErrorMessage(err)}
	}
	if delay, ok := failure.RetryDelay(err); ok {
		if delay <= 0 {
			return classifiedConclusion{class: retrypolicy.ClassPermanent, code: "invalid_retry_after", message: "retry delay must be positive"}
		}
		return classifiedConclusion{class: retrypolicy.ClassRetryAfter, explicitDelay: &delay, code: "retry_after", message: safeErrorMessage(err)}
	}
	if errors.Is(err, context.Canceled) {
		return classifiedConclusion{class: retrypolicy.ClassInterrupted, code: "interrupted", message: "command attempt was interrupted"}
	}
	return classifiedConclusion{class: retrypolicy.ClassRetryable, code: "worker_error", message: safeErrorMessage(err)}
}

func (r *Runtime) concludeClaim(ctx context.Context, claim store.ClaimedCommand, conclusion classifiedConclusion) {
	for attempt := 0; attempt < settlementAttempts; attempt++ {
		result, err := r.store.SettleCommandConclusion(ctx, store.CommandConclusion{
			Claim: claim, Classification: conclusion.class, ExplicitDelay: conclusion.explicitDelay,
			Failure: failure.Value{Code: conclusion.code, Message: conclusion.message},
		}, r.faults)
		if err == nil {
			if result.Retry {
				r.wake.signal()
			}
			r.observe(context.Background(), Observation{
				Kind: ObservationAttempt, Operation: "conclude", Outcome: result.Status,
				RunID: RunID(claim.RunID.String()), CommandID: CommandID(claim.CommandID.String()),
				CommandKey: claim.CommandKey, Name: claim.Name, Version: claim.Version, Queue: claim.Queue, Worker: r.replicaName(),
			})
			return
		}
		ownership, resolveErr := r.store.ResolveCommandAttempt(context.Background(), claim.CommandID, claim.AttemptID, claim.LeaseToken)
		if resolveErr == nil && ownership != store.AttemptOwnershipStillOwned {
			return
		}
		if errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrTerminal) {
			return
		}
		if attempt+1 < settlementAttempts {
			time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
		}
	}
	r.observe(context.Background(), Observation{
		Kind: ObservationAttempt, Operation: "conclude", Outcome: "error",
		RunID: RunID(claim.RunID.String()), CommandID: CommandID(claim.CommandID.String()),
		CommandKey: claim.CommandKey, Name: claim.Name, Version: claim.Version, Queue: claim.Queue, Worker: r.replicaName(),
	})
}

func commandAttemptRemaining(claim store.ClaimedCommand) (time.Duration, bool) {
	var deadline time.Time
	if claim.AttemptTimeout > 0 {
		deadline = claim.DBNow.Add(claim.AttemptTimeout)
	}
	if claim.RetryMaxElapsed != nil {
		candidate := claim.BudgetStartedAt.Add(*claim.RetryMaxElapsed)
		if deadline.IsZero() || candidate.Before(deadline) {
			deadline = candidate
		}
	}
	if claim.RunDeadline != nil && (deadline.IsZero() || claim.RunDeadline.Before(deadline)) {
		deadline = *claim.RunDeadline
	}
	if deadline.IsZero() {
		return 0, false
	}
	return deadline.Sub(claim.DBNow), true
}

func safeErrorMessage(err error) string {
	if err == nil {
		return "worker returned an error"
	}
	message := err.Error()
	if len(message) > 1024 {
		message = message[:1024]
	}
	return message
}

func outcomeForError(err error) string {
	if err == nil {
		return "ok"
	}
	return "error"
}

func (r *Runtime) wakeCommands() { r.wake.signal() }

func unexpectedWorkerError(name string, version int) error {
	return fmt.Errorf("worker %s/%d is not registered", name, version)
}
