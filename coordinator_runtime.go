package flow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/store/journalcodec"
)

func (r *Runtime) runCoordinatorScheduler(ctx context.Context) {
	keys := r.registry.coordinatorKeys()
	kinds := make([]store.CoordinatorKind, len(keys))
	for i, key := range keys {
		kinds[i] = store.CoordinatorKind{Name: key.name, Version: key.version}
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
		candidates, err := r.store.ProbeCoordinators(ctx, kinds, 64)
		if err != nil || len(candidates) == 0 {
			r.wake.wait(ctx, seen, r.pollInterval)
			continue
		}
		progress := false
		for _, candidate := range candidates {
			definition, ok := r.registry.coordinator(candidate.Name, candidate.Version)
			if !ok {
				continue
			}
			selectors := coordinatorStoreSelectors(definition)
			result, claimErr := r.store.ClaimCoordinator(ctx, candidate, selectors, r.commandLease, r.replicaName(), r.faults)
			if result.Progressed {
				progress = true
			}
			if claimErr != nil || result.Coordinator == nil {
				continue
			}
			claim := *result.Coordinator
			r.coordinatorGroup.Add(1)
			// One scheduler goroutine runs one handler at a time. Running exactly
			// coordinatorConcurrency schedulers is the capacity bound; do not
			// detach an unbounded goroutine per claimed delivery.
			r.executeCoordinatorClaim(definition, claim)
		}
		if !progress {
			r.wake.wait(ctx, seen, r.pollInterval)
		}
	}
}

func coordinatorStoreSelectors(definition erasedCoordinator) []store.CoordinatorSelector {
	result := make([]store.CoordinatorSelector, 0, len(definition.handlers))
	for _, handler := range definition.handlers {
		if handler.selector.kind == coordinatorStart {
			continue
		}
		result = append(result, store.CoordinatorSelector{
			Namespace: handler.selector.namespace, Name: handler.selector.name, Version: handler.selector.version,
			Outcome: handler.selector.kind == coordinatorOutcome,
		})
	}
	return result
}

func (r *Runtime) executeCoordinatorClaim(definition erasedCoordinator, claim store.ClaimedCoordinator) {
	defer r.coordinatorGroup.Done()
	ctx, cancel := context.WithCancelCause(context.Background())
	r.activeCoordinators.register(activeCoordinator{
		coordinatorID: claim.CoordinatorID, attemptID: claim.AttemptID, token: claim.LeaseToken,
		localExpiry: time.Now().Add(max(0, claim.LeaseExpiresAt.Sub(claim.DBNow))), cancel: cancel,
	})
	defer func() {
		cancel(nil)
		r.activeCoordinators.unregister(claim.CoordinatorID, claim.AttemptID)
		r.wake.signal()
	}()
	r.mu.RLock()
	stopping := r.lifecycle == runtimeStopping || r.lifecycle == runtimeStopped
	r.mu.RUnlock()
	if stopping {
		cancel(errRuntimeShutdown)
	}

	state, err := definition.stateDef.State.Decode(claim.State)
	if err != nil {
		r.concludeCoordinator(claim, classifiedConclusion{class: "permanent", code: "state_decode", message: "coordinator state does not match definition"})
		return
	}
	scope := &coordinatorScope{state: state}
	ctx = withAttemptScope(ctx, &scope.scope)
	handler, received, err := coordinatorHandlerForClaim(definition, claim)
	if err != nil {
		r.concludeCoordinator(claim, classifiedConclusion{class: "permanent", code: "delivery_decode", message: safeErrorMessage(err)})
		return
	}
	started := time.Now()
	var handlerErr error
	var panicked bool
	if handler != nil {
		handlerErr, panicked = invokeCoordinator(ctx, *handler, scope, received)
	}
	if hookErr := r.faults.Hit(ctx, fault.CoordinatorAfterHandler); hookErr != nil {
		handlerErr = hookErr
	}
	r.observe(context.Background(), Observation{
		Kind: ObservationCoordinator, Operation: "handler", Outcome: outcomeForError(handlerErr),
		ExecutionID: ExecutionID(claim.ExecutionID.String()), Name: claim.Name, Version: claim.Version,
		Worker: r.replicaName(), Duration: time.Since(started),
	})
	if cause := context.Cause(ctx); cause != nil {
		r.concludeCoordinator(claim, classifyWorkerError(cause, false))
		return
	}
	if panicked || handlerErr != nil || scope.scope.firstError != nil {
		if scope.scope.firstError != nil {
			handlerErr = scope.scope.firstError
		}
		r.concludeCoordinator(claim, classifyWorkerError(handlerErr, panicked))
		return
	}
	encodedState, err := definition.stateDef.State.Encode(scope.state, maxCoordinatorStateBytes)
	if err != nil {
		r.concludeCoordinator(claim, classifiedConclusion{class: "permanent", code: "state_encode", message: "coordinator state is invalid or too large"})
		return
	}
	if terminal := scope.scope.terminal; terminal != nil && !bytes.Equal(terminal.state.Bytes, encodedState.Bytes) {
		r.concludeCoordinator(claim, classifiedConclusion{class: "permanent", code: "invalid_decision", message: "coordinator state changed after terminal decision"})
		return
	}
	events, children, err := prepareCoordinatorDecision(scope)
	if err != nil {
		r.concludeCoordinator(claim, classifiedConclusion{class: "permanent", code: "invalid_decision", message: safeErrorMessage(err)})
		return
	}
	request := store.CoordinatorSuccess{Claim: claim, State: encodedState, Events: events, Children: children}
	if terminal := scope.scope.terminal; terminal != nil {
		switch terminal.kind {
		case coordinatorSucceeded:
			request.Terminal = "succeeded"
		case coordinatorFailed:
			request.Terminal, request.Reason = "failed", safeErrorMessage(terminal.reason)
		}
	}
	for attempt := 0; attempt < settlementAttempts; attempt++ {
		result, settleErr := r.store.SettleCoordinatorSuccess(context.Background(), request, r.faults)
		if settleErr == nil {
			for _, event := range events {
				r.observe(context.Background(), Observation{
					Kind: ObservationEvent, Operation: "settle", Outcome: "accepted",
					ExecutionID: ExecutionID(claim.ExecutionID.String()), Name: event.Name, Worker: r.replicaName(),
				})
			}
			r.observe(context.Background(), Observation{Kind: ObservationCoordinator, Operation: "settle", Outcome: result.Status,
				ExecutionID: ExecutionID(claim.ExecutionID.String()), Name: claim.Name, Version: claim.Version, Worker: r.replicaName(), Count: int64(len(events))})
			return
		}
		if errors.Is(settleErr, ErrLeaseLost) || errors.Is(settleErr, ErrTerminal) {
			return
		}
		if errors.Is(settleErr, ErrConflict) || errors.Is(settleErr, ErrInvalid) || errors.Is(settleErr, ErrInvalidState) {
			r.concludeCoordinator(claim, classifiedConclusion{class: "permanent", code: "invalid_decision", message: safeErrorMessage(settleErr)})
			return
		}
		if attempt+1 < settlementAttempts {
			time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
		}
	}
}

func coordinatorHandlerForClaim(definition erasedCoordinator, claim store.ClaimedCoordinator) (*erasedCoordinatorHandler, any, error) {
	if claim.Delivery.Start {
		handler, ok := definition.handlers[coordinatorSelector{kind: coordinatorStart}.key()]
		if !ok {
			return nil, nil, nil
		}
		return &handler, nil, nil
	}
	selector := coordinatorSelector{kind: coordinatorEvent, namespace: claim.Delivery.Namespace,
		name: claim.Delivery.Name}
	if claim.Delivery.TerminalStatus != "" {
		outcome := coordinatorSelector{kind: coordinatorOutcome, namespace: "command_terminal",
			name: claim.Delivery.CommandName, version: claim.Delivery.CommandVersion}
		if _, ok := definition.handlers[outcome.key()]; ok {
			selector = outcome
		}
	}
	handler, ok := definition.handlers[selector.key()]
	if !ok {
		return nil, nil, newError(ErrInvalidState, "coordinate", "delivery", claim.DeliveryKey, "selected delivery has no registered handler")
	}
	if handler.decode == nil || claim.Delivery.EventID == nil || claim.Delivery.Position == nil {
		return nil, nil, newError(ErrInvalidState, "coordinate", "delivery", claim.DeliveryKey, "selected delivery is incomplete")
	}
	value, err := handler.decode(coordinatorReceivedData{
		eventID: EventID(claim.Delivery.EventID.String()), key: claim.Delivery.Key,
		position: JournalPosition(*claim.Delivery.Position), recordedAt: claim.Delivery.RecordedAt,
		body: claim.Delivery.Body, status: CommandStatus(claim.Delivery.TerminalStatus),
		result: claim.Delivery.CommandResult, failure: claim.Delivery.CommandFailure,
	})
	if err != nil {
		return nil, nil, err
	}
	return &handler, value, nil
}

func invokeCoordinator(ctx context.Context, handler erasedCoordinatorHandler, scope *coordinatorScope, received any) (err error, panicked bool) {
	defer func() {
		if recover() != nil {
			err, panicked = errors.New("coordinator panicked"), true
		}
	}()
	return handler.invoke(ctx, scope, received), false
}

func prepareCoordinatorDecision(scope *coordinatorScope) ([]store.ApplicationEvent, []store.CommandCreate, error) {
	stagedEvents := scope.scope.decision.orderedEvents()
	events := make([]store.ApplicationEvent, 0, len(stagedEvents))
	for _, staged := range stagedEvents {
		body, err := canonical.Marshal(journalcodec.ApplicationEventBody{
			V: 1, Payload: json.RawMessage(staged.payload.BytesCopy()),
		}, 0)
		if err != nil {
			return nil, nil, newError(ErrInvalid, "settle", "event", staged.key, "event body cannot be journaled")
		}
		events = append(events, store.ApplicationEvent{
			ID: uuid.New(), Name: staged.definition.Name, Key: staged.key, Body: body,
		})
	}
	children := make([]store.CommandCreate, 0, len(scope.scope.decision.commands))
	for _, staged := range scope.scope.decision.orderedCommands() {
		child, err := prepareCommand(uuid.New(), staged.key, staged.definition, staged.defaults, staged.args, "coordinator_command")
		if err != nil {
			return nil, nil, err
		}
		child.Required = staged.required
		if staged.startAfter > 0 {
			child.ScheduleKind, child.InitialDelay = "execute_delay", staged.startAfter
		}
		declaration, err := canonical.Marshal(struct {
			V            int             `json:"v"`
			Key          string          `json:"key"`
			Name         string          `json:"name"`
			Version      int             `json:"version"`
			Args         json.RawMessage `json:"args"`
			Origin       string          `json:"origin"`
			Required     bool            `json:"required"`
			StartAfterMS int64           `json:"start_after_ms,omitempty"`
		}{V: 1, Key: child.Key, Name: child.Name, Version: child.Version, Args: json.RawMessage(child.Args.BytesCopy()),
			Origin: child.Origin, Required: child.Required, StartAfterMS: child.InitialDelay.Milliseconds()}, 0)
		if err != nil {
			return nil, nil, newError(ErrInvalid, "settle", "command", child.Key, "declaration cannot be canonicalized")
		}
		child.DeclarationFingerprint = declaration.Digest
		children = append(children, child)
	}
	return events, children, nil
}

func (r *Runtime) concludeCoordinator(claim store.ClaimedCoordinator, conclusion classifiedConclusion) {
	for attempt := 0; attempt < settlementAttempts; attempt++ {
		result, err := r.store.SettleCoordinatorConclusion(context.Background(), store.CoordinatorConclusion{
			Claim: claim, Classification: conclusion.class, ExplicitDelay: conclusion.explicitDelay,
			ErrorCode: conclusion.code, ErrorMessage: conclusion.message,
		}, r.faults)
		if err == nil {
			if result.Retry {
				r.wake.signal()
			}
			r.observe(context.Background(), Observation{Kind: ObservationCoordinator, Operation: "conclude", Outcome: result.Status,
				ExecutionID: ExecutionID(claim.ExecutionID.String()), Name: claim.Name, Version: claim.Version, Worker: r.replicaName()})
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
