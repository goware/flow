package flow

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/store"
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
	case testengine.Plan:
		plan, ok := data.value.(erasedPlan)
		if !ok || data.kind != planRegistrationKind {
			return testengine.Result{}, newError(ErrInvalid, "test", "plan", data.name, "registration is not a plan")
		}
		return testPlan(plan, request)
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
	scope.state.results = testDependencies(request.Dependencies)
	value, handlerErr, panicked := invokeWorker(request.Context, worker, scope)
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
	result.Events, result.Commands = testDecision(scope.state.decision)
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
	return testengine.Result{}, worker.commit(request.Context, tx, args, result, testCommandInfo(request.Info))
}

func testPlan(definition erasedPlan, request testengine.Request) (testengine.Result, error) {
	executionID, err := testUUID(request.ExecutionID)
	if err != nil {
		return testengine.Result{}, err
	}
	args, err := definition.def.Args.Decode(request.Args)
	if err != nil {
		return testengine.Result{}, newError(ErrInvalid, "test", "plan arguments", definition.def.Name, "arguments do not match definition")
	}
	snapshot := store.PlanSnapshot{ExecutionID: executionID, Status: request.Status, MaxCommands: request.MaxCommands,
		JournalThrough: request.JournalThrough}
	if snapshot.Status == "" {
		snapshot.Status = "running"
	}
	if snapshot.MaxCommands == 0 {
		snapshot.MaxCommands = defaultMaxCommandsPerExecution
	}
	for _, command := range request.Commands {
		id, err := testUUID(command.ID)
		if err != nil {
			return testengine.Result{}, err
		}
		snapshot.Commands = append(snapshot.Commands, store.PlanCommandSnapshot{
			ID: id, Key: command.Key, Name: command.Name, Version: command.Version, Origin: command.Origin,
			State: command.State, Result: append([]byte(nil), command.Result...), ResultLoaded: true,
			FailureCode: command.FailureCode, FailureMessage: command.FailureMessage,
		})
	}
	loaded := make(map[store.PlanEventSelector]struct{})
	for _, event := range request.Events {
		snapshot.Events = append(snapshot.Events, store.PlanEventSnapshot{
			Position: event.Position, Namespace: event.Namespace, Name: event.Name,
			Version: event.Version, Key: event.Key, Payload: append([]byte(nil), event.Payload...),
		})
		loaded[store.PlanEventSelector{Namespace: event.Namespace, Name: event.Name, Version: event.Version}] = struct{}{}
	}
	for selector := range loaded {
		snapshot.LoadedSelectors = append(snapshot.LoadedSelectors, selector)
	}
	for _, selector := range request.KnownEvents {
		value := store.PlanEventSelector{Namespace: selector.Namespace, Name: selector.Name, Version: selector.Version}
		if _, exists := loaded[value]; !exists {
			snapshot.LoadedSelectors = append(snapshot.LoadedSelectors, value)
		}
	}
	plan := newPlan(snapshot)
	if err := definition.invoke(plan, args); err != nil {
		return testengine.Result{}, err
	}
	if err := plan.validate(); err != nil {
		return testengine.Result{}, err
	}
	if snapshot.MaxCommands > 0 {
		created := len(snapshot.Commands)
		for key := range plan.declarations {
			if _, exists := plan.snapshot.commands[key]; !exists {
				created++
			}
		}
		if created > snapshot.MaxCommands {
			return testengine.Result{}, newError(ErrInvalid, "test", "plan command ceiling", definition.def.Name, "declarations exceed the execution command ceiling")
		}
	}
	result := testengine.Result{WaitingReads: plan.waitingReads}
	for _, key := range plan.order {
		decl := plan.declarations[key]
		item := testengine.Declaration{Key: key, Name: decl.command.Name, Version: decl.command.Version,
			Args: json.RawMessage(decl.args.BytesCopy()), Required: decl.required, Within: decl.within, Delay: decl.delay}
		for _, group := range decl.groups {
			item.Dependencies = append(item.Dependencies, append([]string(nil), group.keys...))
		}
		for _, wait := range decl.waits {
			item.Waits = append(item.Waits, fmt.Sprintf("%s:%s:%d", wait.namespace, wait.name, wait.version))
		}
		result.Declarations = append(result.Declarations, item)
	}
	fingerprint, err := planEvaluationFingerprint(plan)
	if err != nil {
		return testengine.Result{}, err
	}
	var decoded struct {
		Reads []testengine.Read `json:"reads"`
	}
	if err := json.Unmarshal(fingerprint.Bytes, &decoded); err != nil {
		return testengine.Result{}, err
	}
	result.Reads = decoded.Reads
	return result, nil
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
			name: request.DeliveryName, version: request.DeliveryVersion}
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
	handlerErr, panicked := invokeCoordinator(request.Context, handler, scope, received)
	if scope.scope.firstError != nil {
		handlerErr = scope.scope.firstError
	}
	encoded, encodeErr := definition.stateDef.State.Encode(scope.state, maxCoordinatorStateBytes)
	if encodeErr != nil {
		return testengine.Result{}, encodeErr
	}
	result := testengine.Result{State: json.RawMessage(encoded.BytesCopy()), HandlerError: handlerErr, Panicked: panicked}
	result.Events, result.Commands = testDecision(scope.scope.decision)
	if terminal := scope.scope.terminal; terminal != nil {
		result.Terminal, result.ResultRef = string(terminal.kind), terminal.resultRef
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

func testDependencies(values []testengine.Dependency) resultSourceState {
	result := resultSourceState{restricted: true, values: make(map[string]resultSourceValue, len(values))}
	for _, value := range values {
		item := resultSourceValue{name: value.Name, version: value.Version, status: CommandStatus(value.Status),
			result: append([]byte(nil), value.Result...)}
		if value.FailureCode != "" {
			item.failure = &CommandFailure{Code: value.FailureCode, Message: value.FailureMessage}
		}
		result.values[value.Key] = item
	}
	return result
}

func testDecision(decision decisionState) ([]testengine.StagedEvent, []testengine.StagedCommand) {
	events := make([]testengine.StagedEvent, 0, len(decision.events))
	for _, event := range decision.orderedEvents() {
		events = append(events, testengine.StagedEvent{Name: event.definition.Name, Version: event.definition.Version,
			Key: event.key, Payload: json.RawMessage(event.payload.BytesCopy())})
	}
	commands := make([]testengine.StagedCommand, 0, len(decision.commands))
	for _, command := range decision.orderedCommands() {
		commands = append(commands, testengine.StagedCommand{Key: command.key, Name: command.definition.Name,
			Version: command.definition.Version, Args: json.RawMessage(command.args.BytesCopy()),
			Required: command.required, StartAfter: command.startAfter})
	}
	return events, commands
}

func testUUID(value string) (uuid.UUID, error) {
	if value == "" {
		return uuid.New(), nil
	}
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, newError(ErrInvalid, "test", "identifier", value, "invalid UUID")
	}
	return id, nil
}
