package flow

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/store/journalcodec"
	"github.com/goware/flow/internal/testengine"
)

func init() { testengine.Run = runTestEngine }

func runTestEngine(value any, request testengine.Request) (testengine.Result, error) {
	registration, ok := value.(Registration)
	if !ok || registration == nil {
		return testengine.Result{}, newError(ErrInvalid, "test", "registration", "", "value is not a Flow registration")
	}
	data := registration.flowRegistration()
	if data.validation != nil {
		return testengine.Result{}, newError(ErrInvalid, "test", "registration", data.name, data.validation.Error())
	}
	switch request.Operation {
	case testengine.Worker:
		worker, ok := data.value.(erasedWorker)
		if !ok || data.kind != workerRegistrationKind {
			return testengine.Result{}, newError(ErrInvalid, "test", "worker", data.name, "registration is not a worker")
		}
		return testWorker(worker, request)
	case testengine.Commit:
		worker, ok := data.value.(erasedWorker)
		if !ok || data.kind != workerRegistrationKind {
			return testengine.Result{}, newError(ErrInvalid, "test", "commit", data.name, "registration is not a worker")
		}
		return testCommit(worker, request)
	case testengine.Coordinator:
		coordinator, ok := data.value.(erasedCoordinator)
		if !ok || data.kind != coordinatorRegistrationKind {
			return testengine.Result{}, newError(ErrInvalid, "test", "coordinator", data.name, "registration is not a coordinator")
		}
		return testCoordinator(coordinator, request)
	default:
		return testengine.Result{}, newError(ErrInvalid, "test", "operation", string(request.Operation), "unknown test operation")
	}
}

func testWorker(worker erasedWorker, request testengine.Request) (testengine.Result, error) {
	args, err := worker.command.Args.Decode(request.Args)
	if err != nil {
		return testengine.Result{}, newError(ErrInvalid, "test", "worker arguments", worker.command.Name, "arguments do not match definition")
	}
	scope := &workScope{args: args, info: testCommandInfo(request.Info)}
	value, handlerErr, panicked := invokeWorker(withAttemptScope(request.Context, &scope.state), worker, scope)
	if handlerErr == nil && !panicked {
		handlerErr = validateDecisionCommands(scope.state.decision)
	}
	if scope.state.firstError != nil {
		handlerErr = scope.state.firstError
	}
	result := testengine.Result{HandlerError: handlerErr, Panicked: panicked}
	if handlerErr == nil && !panicked {
		encoded, err := worker.command.Result.Encode(value, maxCommandResultBytes)
		if err != nil {
			return testengine.Result{}, newError(ErrInvalid, "test", "worker result", worker.command.Name, "result does not match definition")
		}
		result.Value = json.RawMessage(encoded.BytesCopy())
	}
	result.Commands = testDecision(scope.state.decision)
	result.Events = testEvents(scope.state.decision)
	return result, nil
}

func testCommit(worker erasedWorker, request testengine.Request) (testengine.Result, error) {
	if worker.commit == nil {
		return testengine.Result{}, nil
	}
	tx, ok := request.Tx.(Tx)
	if !ok || tx == nil {
		return testengine.Result{}, newError(ErrInvalid, "test", "commit transaction", worker.command.Name, "transaction double does not implement flow.Tx")
	}
	args, err := worker.command.Args.Decode(request.Args)
	if err != nil {
		return testengine.Result{}, newError(ErrInvalid, "test", "commit arguments", worker.command.Name, "arguments do not match definition")
	}
	result, err := worker.command.Result.Decode(request.Result)
	if err != nil {
		return testengine.Result{}, newError(ErrInvalid, "test", "commit result", worker.command.Name, "result does not match definition")
	}
	scope := scopeState{}
	err = worker.commit(withAttemptScope(request.Context, &scope), tx, args, result, testCommandInfo(request.Info))
	if scope.firstError != nil {
		return testengine.Result{}, scope.firstError
	}
	return testengine.Result{}, err
}

func testCoordinator(definition erasedCoordinator, request testengine.Request) (testengine.Result, error) {
	state, err := definition.stateDef.State.Decode(request.State)
	if err != nil {
		return testengine.Result{}, newError(ErrInvalid, "test", "coordinator state", definition.name, "state does not match definition")
	}
	scope := &coordinatorScope{state: state}
	selector := coordinatorSelector{kind: coordinatorStart}
	var received any
	if request.DeliveryKind != "start" {
		kind := coordinatorEvent
		if request.DeliveryKind == "outcome" {
			kind = coordinatorOutcome
		}
		selector = coordinatorSelector{kind: kind, namespace: request.DeliveryNamespace,
			name: request.DeliveryName}
		if kind == coordinatorOutcome {
			selector.version = request.DeliveryCommandVersion
		}
		handler, ok := definition.handlers[selector.key()]
		if !ok {
			return testengine.Result{}, newError(ErrNotFound, "test", "coordinator handler", selector.key(), "handler is not registered")
		}
		var body canonical.Value
		if kind == coordinatorOutcome && request.DeliveryStatus == string(StatusSucceeded) {
			body, err = canonical.Marshal(journalcodec.CommandSucceededBody{V: 1, CommandKey: request.DeliveryKey,
				Result: request.DeliveryResult}, 0)
		} else {
			body, err = canonical.Marshal(journalcodec.ApplicationEventBody{V: 1, Payload: request.DeliveryPayload}, 0)
		}
		if err != nil {
			return testengine.Result{}, err
		}
		failureBytes, _ := json.Marshal(CommandFailure{Code: request.DeliveryFailureCode, Message: request.DeliveryFailureMessage})
		received, err = handler.decode(coordinatorReceivedData{
			eventID: EventID(uuid.NewString()), key: request.DeliveryKey,
			position: JournalPosition(request.DeliveryPosition), recordedAt: request.DeliveryRecordedAt,
			body: body.Bytes, status: CommandStatus(request.DeliveryStatus), result: request.DeliveryResult,
			failure: failureBytes,
		})
		if err != nil {
			return testengine.Result{}, err
		}
	}
	handler, ok := definition.handlers[selector.key()]
	if !ok {
		if request.DeliveryKind == "start" {
			return testengine.Result{State: append([]byte(nil), request.State...)}, nil
		}
		return testengine.Result{}, newError(ErrNotFound, "test", "coordinator handler", selector.key(), "handler is not registered")
	}
	handlerErr, panicked := invokeCoordinator(withAttemptScope(request.Context, &scope.scope), handler, scope, received)
	if handlerErr == nil && !panicked {
		handlerErr = validateDecisionCommands(scope.scope.decision)
	}
	if scope.scope.firstError != nil {
		handlerErr = scope.scope.firstError
	}
	encoded, encodeErr := definition.stateDef.State.Encode(scope.state, maxCoordinatorStateBytes)
	if encodeErr != nil {
		return testengine.Result{}, encodeErr
	}
	result := testengine.Result{State: json.RawMessage(encoded.BytesCopy()), HandlerError: handlerErr, Panicked: panicked}
	result.Commands = testDecision(scope.scope.decision)
	result.Events = testEvents(scope.scope.decision)
	if terminal := scope.scope.terminal; terminal != nil {
		result.Terminal = string(terminal.kind)
		if terminal.reason != nil {
			result.TerminalReason = terminal.reason.Error()
		}
	}
	return result, nil
}

func testCommandInfo(value testengine.Info) CommandInfo {
	return CommandInfo{ExecutionID: ExecutionID(value.ExecutionID), CommandID: CommandID(value.CommandID),
		CommandKey: value.CommandKey, Name: value.Name, Version: value.Version, CreatedAt: value.CreatedAt,
		BudgetStartedAt: value.BudgetStartedAt, Attempt: value.Attempt, AttemptStartedAt: value.AttemptStartedAt}
}

func testDecision(decision decisionState) []testengine.StagedCommand {
	commands := make([]testengine.StagedCommand, 0, len(decision.commands))
	for _, command := range decision.orderedCommands() {
		item := testengine.StagedCommand{Key: command.key, Name: command.definition.Name,
			Version: command.definition.Version, Args: json.RawMessage(command.args.BytesCopy()),
			Required: command.required, StartAfter: command.startAfter, Within: command.within}
		for _, wait := range command.waits {
			item.Waits = append(item.Waits, testengine.EventWait{Name: wait.name, Key: wait.key})
		}
		commands = append(commands, item)
	}
	return commands
}

func testEvents(decision decisionState) []testengine.StagedEvent {
	events := make([]testengine.StagedEvent, 0, len(decision.events))
	for _, event := range decision.orderedEvents() {
		events = append(events, testengine.StagedEvent{Name: event.definition.Name, Key: event.key,
			Payload: json.RawMessage(event.payload.BytesCopy())})
	}
	return events
}
