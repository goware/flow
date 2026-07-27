package flow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/definition"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Tx interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Commit[A, R any] struct {
	Args   A
	Result R
	Info   CommandInfo
}

type Work[A any] struct {
	Args  A
	info  CommandInfo
	scope *scopeState
}

func (w *Work[A]) Info() CommandInfo {
	if w == nil {
		return CommandInfo{}
	}
	return w.info
}

func (w *Work[A]) flowScope() *scopeState {
	if w == nil {
		return nil
	}
	return w.scope
}

func (w *Work[A]) flowResultSource() *resultSourceState {
	if w == nil || w.scope == nil {
		return nil
	}
	return &w.scope.results
}

type Scope interface {
	flowScope() *scopeState
}

type ResultSource interface {
	flowResultSource() *resultSourceState
}

type CoordinatorScope interface {
	Scope
	flowCoordinatorScope()
}

type WorkerOption[A, R any] interface {
	applyWorker(*workerOptionState[A, R])
}

type workerOptionState[A, R any] struct {
	commit    func(context.Context, Tx, Commit[A, R]) error
	commitSet bool
	errs      []error
}

type workerOptionFunc[A, R any] func(*workerOptionState[A, R])

func (f workerOptionFunc[A, R]) applyWorker(state *workerOptionState[A, R]) { f(state) }

func WithCommit[A, R any](fn func(context.Context, Tx, Commit[A, R]) error) WorkerOption[A, R] {
	return workerOptionFunc[A, R](func(state *workerOptionState[A, R]) {
		if state.commitSet {
			state.errs = append(state.errs, errors.New("commit function configured more than once"))
			return
		}
		state.commitSet = true
		if fn == nil {
			state.errs = append(state.errs, errors.New("commit function must not be nil"))
			return
		}
		state.commit = fn
	})
}

type Registration interface {
	flowRegistration() registrationData
}

type registrationKind string

const (
	workerRegistrationKind      registrationKind = "worker"
	planRegistrationKind        registrationKind = "plan"
	coordinatorRegistrationKind registrationKind = "coordinator"
)

type registrationData struct {
	kind       registrationKind
	name       string
	version    int
	value      any
	validation error
}

type erasedWorker struct {
	command  *definition.Command
	defaults commandDefaults
	invoke   func(context.Context, *workScope) (any, error)
	commit   func(context.Context, Tx, any, any, CommandInfo) error
}

type workScope struct {
	args  any
	info  CommandInfo
	state scopeState
}

type workerRegistration struct {
	data registrationData
}

func (r workerRegistration) flowRegistration() registrationData { return r.data }

func Handle[A, R any](
	cmd Command[A, R],
	worker func(context.Context, *Work[A]) (R, error),
	opts ...WorkerOption[A, R],
) Registration {
	state := workerOptionState[A, R]{}
	for _, option := range opts {
		if option == nil {
			state.errs = append(state.errs, errors.New("nil worker option"))
			continue
		}
		option.applyWorker(&state)
	}
	validation := errors.Join(cmd.err, errors.Join(state.errs...))
	if cmd.def == nil {
		validation = errors.Join(validation, errors.New("zero command definition"))
	}
	if worker == nil {
		validation = errors.Join(validation, errors.New("worker function must not be nil"))
	}

	erased := erasedWorker{command: cmd.def, defaults: cmd.defaults}
	if worker != nil {
		erased.invoke = func(ctx context.Context, scope *workScope) (any, error) {
			args, ok := scope.args.(A)
			if !ok {
				return nil, newError(ErrInvalid, "invoke", "command", scope.info.CommandKey, "argument type mismatch")
			}
			work := &Work[A]{Args: args, info: scope.info, scope: &scope.state}
			return worker(ctx, work)
		}
	}
	if state.commit != nil {
		erased.commit = func(ctx context.Context, tx Tx, args, result any, info CommandInfo) error {
			typedArgs, argsOK := args.(A)
			typedResult, resultOK := result.(R)
			if !argsOK || !resultOK {
				return newError(ErrInvalid, "commit", "command", info.CommandKey, "durable type mismatch")
			}
			return state.commit(ctx, tx, Commit[A, R]{Args: typedArgs, Result: typedResult, Info: info})
		}
	}

	name, version := "", 0
	if cmd.def != nil {
		name, version = cmd.def.Name, cmd.def.Version
	}
	return workerRegistration{data: registrationData{
		kind:       workerRegistrationKind,
		name:       name,
		version:    version,
		value:      erased,
		validation: validation,
	}}
}

type scopeState struct {
	firstError error
	results    resultSourceState
	terminal   *coordinatorTerminal
	decision   decisionState
}

type resultSourceState struct {
	restricted bool
	values     map[string]resultSourceValue
}

type resultSourceValue struct {
	name    string
	version int
	status  CommandStatus
	result  []byte
	failure *CommandFailure
}

type stagedEvent struct {
	definition *definition.Event
	key        string
	payload    canonical.Value
}

type stagedCommand struct {
	definition *definition.Command
	defaults   commandDefaults
	key        string
	args       canonical.Value
	required   bool
	startAfter time.Duration
}

type decisionState struct {
	events       map[string]stagedEvent
	eventOrder   []string
	commands     map[string]stagedCommand
	commandOrder []string
}

func (s *scopeState) poison(err error) {
	if s != nil && s.firstError == nil {
		s.firstError = err
	}
}

// SpawnOption configures one child command staged by Spawn. The interface is
// sealed so durable command decisions remain canonical and inspectable.
type SpawnOption interface{ applySpawn(*spawnOptionState) }

type spawnOptionState struct {
	required      bool
	optionalSet   bool
	startAfter    time.Duration
	startAfterSet bool
	errs          []error
}

type spawnOptionFunc func(*spawnOptionState)

func (f spawnOptionFunc) applySpawn(state *spawnOptionState) { f(state) }

func Optional() SpawnOption {
	return spawnOptionFunc(func(state *spawnOptionState) {
		if state.optionalSet {
			state.errs = append(state.errs, errors.New("optional configured more than once"))
			return
		}
		state.optionalSet = true
		state.required = false
	})
}

func StartAfter(delay time.Duration) SpawnOption {
	return spawnOptionFunc(func(state *spawnOptionState) {
		if state.startAfterSet {
			state.errs = append(state.errs, errors.New("start delay configured more than once"))
			return
		}
		state.startAfterSet = true
		if delay < time.Millisecond {
			state.errs = append(state.errs, errors.New("start delay must be at least one millisecond"))
			return
		}
		state.startAfter = delay
	})
}

// Emit stages an additional typed application fact. It becomes durable only
// if the enclosing handler decision settles successfully.
func Emit[T any](scope Scope, event Event[T], key string, payload T) error {
	state, err := usableScope(scope, "emit")
	if err != nil {
		return err
	}
	if event.def == nil || event.def.Namespace != "application" || event.err != nil {
		err = newError(ErrInvalid, "emit", "event", key, "invalid application event definition")
		state.poison(err)
		return err
	}
	if err = validateStableKey(key, maxCommandKeyBytes, "event"); err != nil {
		state.poison(err)
		return err
	}
	encoded, err := encodeDefinitionValue(event.def.Payload, payload, maxApplicationEventBytes, "event payload")
	if err != nil {
		state.poison(err)
		return err
	}
	identity := event.def.Name + "\x00" + key
	if state.decision.events == nil {
		state.decision.events = make(map[string]stagedEvent)
	}
	if prior, exists := state.decision.events[identity]; exists {
		if prior.definition.Version == event.def.Version && bytes.Equal(prior.payload.Bytes, encoded.Bytes) {
			return nil
		}
		err = newError(ErrConflict, "emit", "event", key, "event identity was staged with different content")
		state.poison(err)
		return err
	}
	state.decision.events[identity] = stagedEvent{definition: event.def, key: key, payload: encoded}
	state.decision.eventOrder = append(state.decision.eventOrder, identity)
	return nil
}

// Spawn stages a bounded asynchronous child command. It never invokes a
// worker inline and performs no database work before successful settlement.
func Spawn[A, R any](scope Scope, key string, cmd Command[A, R], args A, opts ...SpawnOption) error {
	state, err := usableScope(scope, "spawn")
	if err != nil {
		return err
	}
	if cmd.def == nil || cmd.err != nil {
		err = newError(ErrInvalid, "spawn", "command", key, "invalid command definition")
		state.poison(err)
		return err
	}
	if err = validateStableKey(key, maxCommandKeyBytes, "command"); err != nil {
		state.poison(err)
		return err
	}
	options := spawnOptionState{required: true}
	for _, option := range opts {
		if option == nil {
			options.errs = append(options.errs, errors.New("nil spawn option"))
			continue
		}
		option.applySpawn(&options)
	}
	if optionErr := errors.Join(options.errs...); optionErr != nil {
		err = newError(ErrInvalid, "spawn", "command", key, optionErr.Error())
		state.poison(err)
		return err
	}
	encoded, err := encodeDefinitionValue(cmd.def.Args, args, maxCommandArgumentBytes, "command arguments")
	if err != nil {
		state.poison(err)
		return err
	}
	staged := stagedCommand{
		definition: cmd.def, defaults: cmd.defaults, key: key, args: encoded,
		required: options.required, startAfter: options.startAfter,
	}
	if state.decision.commands == nil {
		state.decision.commands = make(map[string]stagedCommand)
	}
	if prior, exists := state.decision.commands[key]; exists {
		if equivalentStagedCommand(prior, staged) {
			return nil
		}
		err = newError(ErrConflict, "spawn", "command", key, "command key was staged with a different declaration")
		state.poison(err)
		return err
	}
	state.decision.commands[key] = staged
	state.decision.commandOrder = append(state.decision.commandOrder, key)
	return nil
}

func usableScope(scope Scope, operation string) (*scopeState, error) {
	if scope == nil || scope.flowScope() == nil {
		return nil, newError(ErrInvalidState, operation, "scope", "", "scope is unavailable")
	}
	state := scope.flowScope()
	if state.firstError != nil {
		return state, state.firstError
	}
	return state, nil
}

func equivalentStagedCommand(a, b stagedCommand) bool {
	if a.definition == nil || b.definition == nil {
		return false
	}
	if a.definition.Name != b.definition.Name || a.definition.Version != b.definition.Version ||
		a.required != b.required || a.startAfter != b.startAfter ||
		a.defaults.queue != b.defaults.queue || a.defaults.attemptTimeout != b.defaults.attemptTimeout ||
		!bytes.Equal(a.args.Bytes, b.args.Bytes) {
		return false
	}
	left, leftErr := canonical.Marshal(a.defaults.retryPolicy, 0)
	right, rightErr := canonical.Marshal(b.defaults.retryPolicy, 0)
	return leftErr == nil && rightErr == nil && bytes.Equal(left.Bytes, right.Bytes)
}

func (state *decisionState) orderedEvents() []stagedEvent {
	if state == nil || len(state.events) == 0 {
		return nil
	}
	keys := state.eventOrder
	result := make([]stagedEvent, 0, len(keys))
	for _, key := range keys {
		result = append(result, state.events[key])
	}
	return result
}

func (state *decisionState) orderedCommands() []stagedCommand {
	if state == nil || len(state.commands) == 0 {
		return nil
	}
	keys := append([]string(nil), state.commandOrder...)
	sort.Strings(keys)
	result := make([]stagedCommand, 0, len(keys))
	for _, key := range keys {
		result = append(result, state.commands[key])
	}
	return result
}

func ResultOf[A, R any](source ResultSource, key string, cmd Command[A, R]) (R, error) {
	var zero R
	value, err := lookupResultSource(source, key, cmd.def, true)
	if err != nil {
		return zero, Permanent(err)
	}
	decoded, err := cmd.def.Result.Decode(value.result)
	if err != nil {
		return zero, Permanent(newError(ErrInvalidState, "result", "command", key, "stored result cannot be decoded"))
	}
	result, ok := decoded.(R)
	if !ok {
		return zero, Permanent(newError(ErrInvalidState, "result", "command", key, "stored result has an incompatible type"))
	}
	return result, nil
}

func OutcomeOf[A, R any](source ResultSource, key string, cmd Command[A, R]) (CommandOutcome[R], error) {
	var result CommandOutcome[R]
	value, err := lookupResultSource(source, key, cmd.def, false)
	if err != nil {
		return result, Permanent(err)
	}
	result.Status = value.status
	if value.failure != nil {
		copy := *value.failure
		result.Failure = &copy
	}
	if value.status == StatusSucceeded {
		decoded, decodeErr := cmd.def.Result.Decode(value.result)
		if decodeErr != nil {
			return CommandOutcome[R]{}, Permanent(newError(ErrInvalidState, "outcome", "command", key, "stored result cannot be decoded"))
		}
		result.Result = decoded.(R)
	}
	return result, nil
}

func lookupResultSource(source ResultSource, key string, command *definition.Command, successOnly bool) (resultSourceValue, error) {
	if source == nil || source.flowResultSource() == nil {
		return resultSourceValue{}, newError(ErrInvalidState, "read", "result source", key, "result source is unavailable")
	}
	if command == nil {
		return resultSourceValue{}, newError(ErrInvalid, "read", "command", key, "invalid command definition")
	}
	state := source.flowResultSource()
	value, exists := state.values[key]
	if !exists {
		reason := "command is not an available dependency"
		if !state.restricted {
			reason = "command does not exist in the snapshot"
		}
		return resultSourceValue{}, newError(ErrNotFound, "read", "command", key, reason)
	}
	if value.name != command.Name || value.version != command.Version {
		return resultSourceValue{}, newError(ErrConflict, "read", "command", key, fmt.Sprintf("definition differs from %s/%d", value.name, value.version))
	}
	if value.status == "" {
		return resultSourceValue{}, newError(ErrInvalidState, "read", "command", key, "command is not terminal")
	}
	if successOnly && value.status != StatusSucceeded {
		return resultSourceValue{}, newError(ErrInvalidState, "read", "command", key, "command has no successful result")
	}
	return value, nil
}
