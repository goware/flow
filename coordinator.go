package flow

import (
	"context"
	"errors"
	"fmt"
)

type Coordination[S any] struct {
	State S
	scope *scopeState
}

func (c *Coordination[S]) flowScope() *scopeState {
	if c == nil {
		return nil
	}
	return c.scope
}

func (c *Coordination[S]) flowCoordinatorScope() {}

type Handler[S any] interface {
	flowCoordinatorHandler() coordinatorHandler[S]
}

type coordinatorHandlerKind string

const (
	coordinatorStart   coordinatorHandlerKind = "start"
	coordinatorEvent   coordinatorHandlerKind = "event"
	coordinatorOutcome coordinatorHandlerKind = "outcome"
)

type coordinatorSelector struct {
	kind      coordinatorHandlerKind
	namespace string
	name      string
	version   int
}

func (s coordinatorSelector) key() string {
	if s.kind == coordinatorStart {
		return "start"
	}
	return fmt.Sprintf("%s:%s:%s:%d", s.kind, s.namespace, s.name, s.version)
}

func (s coordinatorSelector) nameVersion() string {
	return fmt.Sprintf("%s:%d", s.name, s.version)
}

type coordinatorHandler[S any] struct {
	selector coordinatorSelector
	invoke   func(context.Context, *Coordination[S], any) error
	err      error
}

func (h coordinatorHandler[S]) flowCoordinatorHandler() coordinatorHandler[S] { return h }

func OnStart[S any](handler func(context.Context, *Coordination[S]) error) Handler[S] {
	value := coordinatorHandler[S]{selector: coordinatorSelector{kind: coordinatorStart}}
	if handler == nil {
		value.err = errors.New("coordinator start handler must not be nil")
		return value
	}
	value.invoke = func(ctx context.Context, coordination *Coordination[S], _ any) error {
		return handler(ctx, coordination)
	}
	return value
}

func On[S, T any](event Event[T], handler func(context.Context, *Coordination[S], Received[T]) error) Handler[S] {
	ref := event.flowEventName()
	value := coordinatorHandler[S]{selector: coordinatorSelector{
		kind: coordinatorEvent, namespace: ref.namespace, name: ref.name, version: ref.version,
	}, err: event.err}
	if event.def == nil {
		value.err = errors.Join(value.err, errors.New("zero event definition"))
	}
	if handler == nil {
		value.err = errors.Join(value.err, errors.New("coordinator event handler must not be nil"))
		return value
	}
	value.invoke = func(ctx context.Context, coordination *Coordination[S], received any) error {
		typed, ok := received.(Received[T])
		if !ok {
			return newError(ErrInvalid, "coordinate", "event", ref.name, "payload type mismatch")
		}
		return handler(ctx, coordination, typed)
	}
	return value
}

func OnOutcome[S, A, R any](
	command Command[A, R],
	handler func(context.Context, *Coordination[S], Received[CommandOutcome[R]]) error,
) Handler[S] {
	name, version := command.Name(), command.Version()
	value := coordinatorHandler[S]{selector: coordinatorSelector{
		kind: coordinatorOutcome, namespace: "command_terminal", name: name, version: version,
	}, err: command.err}
	if command.def == nil {
		value.err = errors.Join(value.err, errors.New("zero command definition"))
	}
	if handler == nil {
		value.err = errors.Join(value.err, errors.New("coordinator outcome handler must not be nil"))
		return value
	}
	value.invoke = func(ctx context.Context, coordination *Coordination[S], received any) error {
		typed, ok := received.(Received[CommandOutcome[R]])
		if !ok {
			return newError(ErrInvalid, "coordinate", "command", name, "outcome type mismatch")
		}
		return handler(ctx, coordination, typed)
	}
	return value
}

type erasedCoordinator struct {
	name     string
	version  int
	stateDef any
	handlers map[string]erasedCoordinatorHandler
}

type erasedCoordinatorHandler struct {
	selector coordinatorSelector
	invoke   func(context.Context, *coordinatorScope, any) error
}

type coordinatorScope struct {
	state any
	scope scopeState
}

func (c Coordinator[S]) flowRegistration() registrationData {
	name, version := "", 0
	if c.def != nil {
		name, version = c.def.Name, c.def.Version
	}
	erased := erasedCoordinator{
		name: name, version: version, stateDef: c.def,
		handlers: make(map[string]erasedCoordinatorHandler, len(c.handlers)),
	}
	for _, handler := range c.handlers {
		h := handler
		erased.handlers[h.selector.key()] = erasedCoordinatorHandler{
			selector: h.selector,
			invoke: func(ctx context.Context, scope *coordinatorScope, payload any) error {
				state, ok := scope.state.(S)
				if !ok {
					return newError(ErrInvalid, "coordinate", "state", name, "state type mismatch")
				}
				coordination := &Coordination[S]{State: state, scope: &scope.scope}
				if err := h.invoke(ctx, coordination, payload); err != nil {
					return err
				}
				scope.state = coordination.State
				return nil
			},
		}
	}
	validation := c.err
	if c.def == nil {
		validation = errors.Join(validation, errors.New("zero coordinator definition"))
	}
	return registrationData{
		kind: coordinatorRegistrationKind, name: name, version: version,
		value: erased, validation: validation,
	}
}

type coordinatorTerminalKind string

const (
	coordinatorSucceeded coordinatorTerminalKind = "succeeded"
	coordinatorFailed    coordinatorTerminalKind = "failed"
)

type coordinatorTerminal struct {
	kind      coordinatorTerminalKind
	resultRef string
	reason    error
}

func SucceedExecution(scope CoordinatorScope, resultRef string) error {
	state, err := validCoordinatorScope(scope)
	if err != nil {
		return err
	}
	return state.setTerminal(coordinatorTerminal{kind: coordinatorSucceeded, resultRef: resultRef})
}

func FailExecution(scope CoordinatorScope, reason error) error {
	state, err := validCoordinatorScope(scope)
	if err != nil {
		return err
	}
	if reason == nil {
		reason = errors.New("coordinator failed")
	}
	return state.setTerminal(coordinatorTerminal{kind: coordinatorFailed, reason: reason})
}

func validCoordinatorScope(value CoordinatorScope) (*scopeState, error) {
	if value == nil || value.flowScope() == nil {
		return nil, newError(ErrInvalidState, "complete", "coordinator", "", "scope is not active")
	}
	return value.flowScope(), nil
}

func (s *scopeState) setTerminal(terminal coordinatorTerminal) error {
	if s.terminal == nil {
		s.terminal = &terminal
		return nil
	}
	reasonsEqual := (s.terminal.reason == nil && terminal.reason == nil) ||
		errors.Is(s.terminal.reason, terminal.reason) || errors.Is(terminal.reason, s.terminal.reason)
	if s.terminal.kind == terminal.kind && s.terminal.resultRef == terminal.resultRef && reasonsEqual {
		return nil
	}
	err := newError(ErrConflict, "complete", "coordinator", "", "terminal decision already staged")
	s.poison(err)
	return err
}
