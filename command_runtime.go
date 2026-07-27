package flow

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/failure"
	"github.com/goware/flow/internal/fault"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store"
	"github.com/jackc/pgx/v5"
)

const (
	commandProbeFactor    = 4
	maxCommandProbe       = 256
	maxCommandResultBytes = 256 << 10
	settlementAttempts    = 3
)

func (r *Runtime) runCommandScheduler(ctx context.Context) {
	slots := newCommandSlots(r.workerConcurrency, r.queueConcurrency)
	queueTurn := 0
	keys := r.registry.workerKeys()
	kinds := make([]store.CommandKind, len(keys))
	for index, key := range keys {
		kinds[index] = store.CommandKind{Name: key.name, Version: key.version}
	}
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
		limit := min(maxCommandProbe, max(free, free*commandProbeFactor))
		started := time.Now()
		candidates, err := r.store.ProbeCommands(ctx, kinds, limit)
		if err == nil {
			err = r.faults.Hit(ctx, fault.ProbeReturn)
		}
		r.observe(ctx, Observation{
			Kind: ObservationClaim, Operation: "probe", Outcome: outcomeForError(err),
			Count: int64(len(candidates)), Duration: time.Since(started), Worker: r.replicaName(),
		})
		if err != nil {
			r.wake.wait(ctx, seen, r.pollInterval)
			continue
		}
		progress := false
		selected := make([]store.CommandCandidate, 0, free)
		for _, candidate := range fairQueueCandidates(candidates, &queueTurn) {
			if slots.reserve(candidate.Queue) {
				selected = append(selected, candidate)
			}
			if slots.free() == 0 {
				break
			}
		}
		if ctx.Err() != nil {
			for _, candidate := range selected {
				slots.release(candidate.Queue)
			}
			return
		}
		for _, group := range groupCandidatesByExecution(selected) {
			claimStarted := time.Now()
			result, claimErr := r.store.ClaimCommands(ctx, group, r.commandLease, r.replicaName(), r.faults)
			if claimErr != nil && len(result.Commands) > 0 {
				confirmed := result.Commands[:0]
				for _, command := range result.Commands {
					ownership, resolveErr := r.store.ResolveCommandAttempt(ctx, command.CommandID, command.AttemptID, command.LeaseToken)
					if resolveErr == nil && ownership == store.AttemptOwnershipStillOwned {
						confirmed = append(confirmed, command)
					}
				}
				result.Commands = confirmed
				if len(confirmed) > 0 {
					claimErr = nil
				}
			}
			r.observe(ctx, Observation{
				Kind: ObservationClaim, Operation: "claim", Outcome: outcomeForError(claimErr),
				ExecutionID: ExecutionID(group[0].ExecutionID.String()), Count: int64(len(result.Commands)),
				Duration: time.Since(claimStarted), Worker: r.replicaName(),
			})
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
				r.workerGroup.Add(1)
				go r.executeClaim(worker, command, slots)
			}
		}
		if !progress {
			r.wake.wait(ctx, seen, r.pollInterval)
		}
	}
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

func (slots *commandSlots) reserve(queue string) bool {
	slots.mu.Lock()
	defer slots.mu.Unlock()
	if limit := slots.limits[queue]; limit > 0 && slots.active[queue] >= limit {
		return false
	}
	select {
	case slots.global <- struct{}{}:
		slots.active[queue]++
		return true
	default:
		return false
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

func groupCandidatesByExecution(candidates []store.CommandCandidate) [][]store.CommandCandidate {
	groups := make([][]store.CommandCandidate, 0, len(candidates))
	indexes := make(map[uuid.UUID]int, len(candidates))
	for _, candidate := range candidates {
		index, exists := indexes[candidate.ExecutionID]
		if !exists {
			index = len(groups)
			indexes[candidate.ExecutionID] = index
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
	localLeaseExpiry := time.Now().Add(max(0, claim.LeaseExpiresAt.Sub(claim.DBNow)))
	r.active.register(activeCommand{
		commandID: claim.CommandID, attemptID: claim.AttemptID, token: claim.LeaseToken,
		localExpiry: localLeaseExpiry, cancel: cancelCause,
	})
	r.mu.RLock()
	stopping := r.lifecycle == runtimeStopping || r.lifecycle == runtimeStopped
	r.mu.RUnlock()
	if stopping {
		cancelCause(errRuntimeShutdown)
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
		r.concludeClaim(workerCtx, claim, classifiedConclusion{
			class: retrypolicy.ClassPermanent, code: "argument_decode", message: "stored command arguments do not match the registered definition",
		})
		return
	}
	info := CommandInfo{
		ExecutionID: ExecutionID(claim.ExecutionID.String()), CommandID: CommandID(claim.CommandID.String()),
		CommandKey: claim.CommandKey, Name: claim.Name, Version: claim.Version,
		CreatedAt: claim.CreatedAt, BudgetStartedAt: claim.BudgetStartedAt,
		Attempt: claim.Attempt, AttemptStartedAt: claim.DBNow,
	}
	scope := &workScope{args: args, info: info}
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
		ExecutionID: info.ExecutionID, CommandID: info.CommandID, CommandKey: info.CommandKey,
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
	commit := func(tx pgx.Tx) error { return nil }
	if worker.commit != nil {
		commit = func(tx pgx.Tx) error { return worker.commit(workerCtx, tx, args, result, info) }
	} else {
		commit = nil
	}
	for attempt := 0; attempt < settlementAttempts; attempt++ {
		_, settleErr := r.store.SettleCommandSuccess(context.Background(), store.CommandSuccess{
			Claim: claim, Result: encoded, Commit: commit,
		}, r.faults)
		if settleErr == nil {
			r.observe(context.Background(), Observation{
				Kind: ObservationAttempt, Operation: "settle", Outcome: "succeeded",
				ExecutionID: info.ExecutionID, CommandID: info.CommandID, CommandKey: info.CommandKey,
				Name: info.Name, Version: info.Version, Queue: claim.Queue, Worker: r.replicaName(),
			})
			return
		}
		var commitErr *store.CommitFunctionError
		if errors.As(settleErr, &commitErr) {
			r.concludeClaim(context.Background(), claim, classifyWorkerError(commitErr.Err, false))
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
			ErrorCode: conclusion.code, ErrorMessage: conclusion.message,
		}, r.faults)
		if err == nil {
			if result.Retry {
				r.wake.signal()
			}
			r.observe(context.Background(), Observation{
				Kind: ObservationAttempt, Operation: "conclude", Outcome: result.Status,
				ExecutionID: ExecutionID(claim.ExecutionID.String()), CommandID: CommandID(claim.CommandID.String()),
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
}

func commandAttemptRemaining(claim store.ClaimedCommand) (time.Duration, bool) {
	var deadline time.Time
	if claim.AttemptTimeout > 0 {
		deadline = claim.DBNow.Add(claim.AttemptTimeout)
	}
	if policy, err := retrypolicy.PublicFromCanonical(claim.RetryPolicy); err == nil {
		value := retrypolicy.ValueOf(policy)
		if value.MaxElapsed != nil {
			candidate := claim.BudgetStartedAt.Add(*value.MaxElapsed)
			if deadline.IsZero() || candidate.Before(deadline) {
				deadline = candidate
			}
		}
	}
	if claim.ExecutionDeadline != nil && (deadline.IsZero() || claim.ExecutionDeadline.Before(deadline)) {
		deadline = *claim.ExecutionDeadline
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
