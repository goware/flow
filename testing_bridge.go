package flow

import (
	"encoding/json"
	"slices"

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
		if !ok {
			return testengine.Result{}, newError(ErrInvalid, "test", "worker", data.name, "registration is not a worker")
		}
		return testWorker(worker, request)
	case testengine.Commit:
		worker, ok := data.value.(erasedWorker)
		if !ok {
			return testengine.Result{}, newError(ErrInvalid, "test", "commit", data.name, "registration is not a worker")
		}
		return testCommit(worker, request)
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
	if len(request.EventInputs) > maxCommandEventWaits {
		return testengine.Result{}, newError(ErrInvalid, "test", "event inputs", worker.command.Name, "command exceeds the 256 event-wait limit")
	}
	if len(request.EventInputs) > 0 {
		scope.state.eventInputs = make(map[string]eventInputSnapshot, len(request.EventInputs))
		for _, input := range request.EventInputs {
			if input.Name == "" || input.Key == "" || input.Position < 1 {
				return testengine.Result{}, newError(ErrInvalid, "test", "event input", input.Key, "event input is incomplete")
			}
			identity := input.Name + "\x00" + input.Key
			if prior, exists := scope.state.eventInputs[identity]; exists {
				if slices.Equal(prior.payload, input.Payload) {
					continue
				}
				return testengine.Result{}, newError(ErrConflict, "test", "event input", input.Key, "event input identity differs")
			}
			scope.state.eventInputs[identity] = eventInputSnapshot{position: input.Position, payload: slices.Clone(input.Payload)}
		}
	}
	value, handlerErr, panicked := invokeWorker(request.Context, worker, scope)
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
	return testengine.Result{}, worker.commit(request.Context, tx, args, result, testCommandInfo(request.Info))
}

func testCommandInfo(value testengine.Info) CommandInfo {
	return CommandInfo{RunID: RunID(value.RunID), RunKey: value.RunKey, Definition: value.Definition,
		CommandID: CommandID(value.CommandID), CommandKey: value.CommandKey, Name: value.Name, Version: value.Version, CreatedAt: value.CreatedAt,
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
