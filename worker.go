package flow

import (
	"context"
	"errors"

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
}

type resultSourceState struct{}

func (s *scopeState) poison(err error) {
	if s != nil && s.firstError == nil {
		s.firstError = err
	}
}
