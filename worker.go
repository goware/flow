package flow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/definition"
	retrypolicy "github.com/goware/flow/internal/retry"
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

// Work is the attempt-local command scope passed to a worker. It represents
// one invocation of one claimed command, not the whole Run and not the
// immutable Command definition. Args contains the command's typed arguments;
// Info returns its durable run, command, and attempt identity. The private
// scope backs Enqueue, Emit, and GetEventValue for the decision being built by
// this invocation.
//
// A fresh Work is created for every command attempt. It is valid only during
// the worker call and must not be retained or used concurrently.
type Work[A any] struct {
	Args  A
	info  CommandInfo
	scope *scopeState
}

// Info returns immutable identity and timing information for the claimed
// command and its current attempt.
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

type registrationData struct {
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
		name:       name,
		version:    version,
		value:      erased,
		validation: validation,
	}}
}

type scopeState struct {
	firstError  error
	decision    decisionState
	eventInputs map[string]eventInputSnapshot
}

type eventInputSnapshot struct {
	position int64
	payload  []byte
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
	startAfter time.Duration
	waits      []commandEventWait
	within     time.Duration
}

type commandEventWait struct {
	eventReference
	key string
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

// Emit stages an application event in a worker decision. It
// performs no database work and becomes durable only when the enclosing
// decision settles successfully.
func Emit[W, T any](work *Work[W], event Event[T], key string, payload T) error {
	state, err := usableWork(work, "emit")
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
		if bytes.Equal(prior.payload.Bytes, encoded.Bytes) {
			return nil
		}
		err = newError(ErrConflict, "emit", "event", key, "event identity was staged with different content")
		state.poison(err)
		return err
	}
	if len(state.decision.events) >= maxStagedApplicationEvents {
		err = newError(ErrInvalid, "emit", "event", key, "decision exceeds the 256 staged-event limit")
		state.poison(err)
		return err
	}
	state.decision.events[identity] = stagedEvent{definition: event.def, key: key, payload: encoded}
	state.decision.eventOrder = append(state.decision.eventOrder, identity)
	return nil
}

// Enqueue requests a command from a worker. It never invokes
// the worker inline; the command is staged in the enclosing durable decision.
func Enqueue[W, A, R any](work *Work[W], key string, cmd Command[A, R], args A) *StagedCommand {
	state, err := usableWork(work, "enqueue")
	node := &StagedCommand{scope: state, key: key}
	if err != nil {
		return node
	}
	if cmd.def == nil || cmd.err != nil {
		err = newError(ErrInvalid, "enqueue", "command", key, "invalid command definition")
		state.poison(err)
		return node
	}
	if err = validateStableKey(key, maxCommandKeyBytes, "command"); err != nil {
		state.poison(err)
		return node
	}
	encoded, err := encodeDefinitionValue(cmd.def.Args, args, maxCommandArgumentBytes, "command arguments")
	if err != nil {
		state.poison(err)
		return node
	}
	staged := stagedCommand{definition: cmd.def, defaults: cmd.defaults, key: key, args: encoded}
	if state.decision.commands == nil {
		state.decision.commands = make(map[string]stagedCommand)
	}
	if prior, exists := state.decision.commands[key]; exists {
		if equivalentStagedCommandIdentity(prior, staged) {
			return node
		}
		err = newError(ErrConflict, "enqueue", "command", key, "command key was staged with a different declaration")
		state.poison(err)
		return node
	}
	state.decision.commands[key] = staged
	state.decision.commandOrder = append(state.decision.commandOrder, key)
	return node
}

func equivalentStagedCommandIdentity(a, b stagedCommand) bool {
	left, right := a, b
	left.startAfter, right.startAfter = 0, 0
	left.waits, right.waits = nil, nil
	left.within, right.within = 0, 0
	return equivalentStagedCommand(left, right)
}

func usableWork[W any](work *Work[W], operation string) (*scopeState, error) {
	if work == nil || work.flowScope() == nil {
		return nil, newError(ErrInvalidState, operation, "work", "", "work is unavailable")
	}
	state := work.flowScope()
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
		a.startAfter != b.startAfter || a.within != b.within ||
		!equivalentCommandDefaults(a.defaults, b.defaults) ||
		!bytes.Equal(a.args.Bytes, b.args.Bytes) || !slices.Equal(a.waits, b.waits) {
		return false
	}
	return true
}

func equivalentCommandDefaults(a, b commandDefaults) bool {
	if a.queue != b.queue || a.attemptTimeout != b.attemptTimeout || a.recoveryLease != b.recoveryLease {
		return false
	}
	left, leftErr := retrypolicy.CanonicalPublic(a.retryPolicy)
	right, rightErr := retrypolicy.CanonicalPublic(b.retryPolicy)
	return leftErr == nil && rightErr == nil && bytes.Equal(left.Bytes, right.Bytes)
}

func addCommandEventWait(waits []commandEventWait, wait commandEventWait) []commandEventWait {
	if slices.Contains(waits, wait) {
		return waits
	}
	waits = append(waits, wait)
	sort.Slice(waits, func(i, j int) bool {
		if waits[i].namespace != waits[j].namespace {
			return waits[i].namespace < waits[j].namespace
		}
		if waits[i].name != waits[j].name {
			return waits[i].name < waits[j].name
		}
		return waits[i].key < waits[j].key
	})
	return waits
}

func validateDecisionCommands(state decisionState) error {
	for _, command := range state.commands {
		if command.within > 0 && len(command.waits) == 0 {
			return newError(ErrInvalid, "enqueue", "within", command.key, "Within requires WaitFor")
		}
		if len(command.waits) > maxCommandEventWaits {
			return newError(ErrInvalid, "enqueue", "wait", command.key, "command exceeds the 256 event-wait limit")
		}
	}
	return nil
}

func (state *decisionState) orderedEvents() []stagedEvent {
	if state == nil || len(state.events) == 0 {
		return nil
	}
	keys := append([]string(nil), state.eventOrder...)
	sort.Strings(keys)
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

// GetEventValue returns the typed value attached to an exact event gate
// materialized for the current command. The value is already in memory when
// the worker starts; this function does not wait or query the database.
// found=false reports ordinary absence without poisoning the worker decision.
func GetEventValue[W, T any](work *Work[W], event Event[T], key string) (T, bool, error) {
	var zero T
	state, err := usableWork(work, "get event value")
	if err != nil {
		return zero, false, err
	}
	if event.def == nil || event.def.Namespace != "application" || event.err != nil {
		err = newError(ErrInvalid, "get", "event value", key, "invalid application event definition")
		state.poison(err)
		return zero, false, err
	}
	if err = validateStableKey(key, maxCommandKeyBytes, "event"); err != nil {
		state.poison(err)
		return zero, false, err
	}
	input, ok := state.eventInputs[event.def.Name+"\x00"+key]
	if !ok {
		return zero, false, nil
	}
	decoded, err := event.def.Payload.Decode(input.payload)
	if err != nil {
		err = newError(ErrInvalidState, "get", "event value", key, "stored event payload cannot be decoded")
		state.poison(err)
		return zero, false, err
	}
	result, ok := decoded.(T)
	if !ok {
		err = newError(ErrInvalidState, "get", "event value", key, "stored event payload has an incompatible type")
		state.poison(err)
		return zero, false, err
	}
	return result, true, nil
}

func ResultOf[A, R any](trace RunTrace, key string, cmd Command[A, R]) (R, error) {
	var zero R
	value, err := lookupTraceResult(trace, key, cmd.def)
	if err != nil {
		return zero, NoRetry(err)
	}
	decoded, err := cmd.def.Result.Decode(value.Result)
	if err != nil {
		return zero, NoRetry(newError(ErrInvalidState, "result", "command", key, "stored result cannot be decoded"))
	}
	result, ok := decoded.(R)
	if !ok {
		return zero, NoRetry(newError(ErrInvalidState, "result", "command", key, "stored result has an incompatible type"))
	}
	return result, nil
}

func lookupTraceResult(trace RunTrace, key string, command *definition.Command) (TraceCommand, error) {
	if command == nil {
		return TraceCommand{}, newError(ErrInvalid, "read", "command", key, "invalid command definition")
	}
	for _, value := range trace.Commands {
		if value.Key != key {
			continue
		}
		if value.Name != command.Name || value.Version != command.Version {
			return TraceCommand{}, newError(ErrConflict, "read", "command", key, fmt.Sprintf("definition differs from %s/%d", value.Name, value.Version))
		}
		if value.Status != CommandStatusSucceeded {
			return TraceCommand{}, newError(ErrInvalidState, "read", "command", key, "command has no successful result")
		}
		return value, nil
	}
	return TraceCommand{}, newError(ErrNotFound, "read", "command", key, "command does not exist in the trace")
}
