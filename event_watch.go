package flow

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/goware/flow/internal/store/journalcodec"
	"github.com/goware/flow/internal/uuid"
)

// EventWatch observes future durable application events of one definition in
// one run. It is a broadcast reader: it does not consume or acknowledge an
// event and holds no database connection while waiting.
//
// Call Next sequentially. Concurrent Next calls return ErrInvalidState.
// Always call Close; it is safe to call more than once. An EventWatch must not
// be copied.
type EventWatch[T any] struct {
	runtime *Runtime
	event   Event[T]
	runID   uuid.UUID
	cursor  int64

	closeOnce sync.Once
	closed    atomic.Bool
	inNext    atomic.Bool
	lifetime  context.Context
	cancel    context.CancelFunc
}

// Watch starts after the run's current journal head. It requires notifications
// on every runtime that may write the watched run. Establish the watch before
// reading the application's projection to close the application read race.
// A watch may be created before runtime.Run starts; until the listener starts
// and performs its catch-up wake, callers must bound Next with a context.
func (event Event[T]) Watch(ctx context.Context, runtime *Runtime, id RunID) (*EventWatch[T], error) {
	if event.err != nil || event.def == nil || event.def.Namespace != "application" {
		return nil, newError(ErrInvalid, "watch", "event", eventName(event.def), "invalid event definition")
	}
	if runtime == nil {
		return nil, newError(ErrInvalid, "watch", "runtime", "", "runtime is nil")
	}
	runID, err := parseRunID(id)
	if err != nil {
		return nil, err
	}
	runtime.mu.RLock()
	closed := runtime.closed || runtime.lifecycle == runtimeStopping || runtime.lifecycle == runtimeStopped
	notifications := runtime.notifications
	runtime.mu.RUnlock()
	if closed {
		return nil, newError(ErrClosed, "watch", "runtime", "", "runtime is closed")
	}
	if !notifications {
		return nil, newError(ErrInvalid, "watch", "runtime", "", "event watches require notifications")
	}
	runtimeLifetime, registered := runtime.eventWakes.register(runID)
	if !registered {
		return nil, newError(ErrClosed, "watch", "runtime", "", "runtime is closed")
	}
	lifetime, cancel := context.WithCancel(runtimeLifetime)
	watch := &EventWatch[T]{runtime: runtime, event: event, runID: runID, lifetime: lifetime, cancel: cancel}
	queryCtx, cancelQuery := context.WithCancel(ctx)
	stopLifetimeCancel := context.AfterFunc(lifetime, cancelQuery)
	cursor, err := runtime.store.OpenEventWatch(queryCtx, runID)
	stopLifetimeCancel()
	cancelQuery()
	if err != nil {
		if lifetime.Err() != nil {
			watch.Close()
			return nil, newError(ErrClosed, "watch", "runtime", "", "runtime is closed")
		}
		watch.Close()
		return nil, err
	}
	if isTerminalStoreRunStatus(cursor.Status) {
		watch.Close()
		return nil, newError(ErrTerminal, "watch", "run", string(id), "run is terminal")
	}
	if !isActiveStoreRunStatus(cursor.Status) {
		watch.Close()
		return nil, newError(ErrInvalidState, "watch", "run", string(id), "stored run status is unknown")
	}
	if _, active := runtime.eventWakes.snapshot(runID); !active {
		watch.Close()
		return nil, newError(ErrClosed, "watch", "runtime", "", "runtime is closed")
	}
	watch.cursor = cursor.Position
	return watch, nil
}

// Next returns the next matching durable application event after the watch
// baseline. It waits only for notification/reconnect hints, Close, runtime
// shutdown, or ctx; it performs no periodic polling.
func (watch *EventWatch[T]) Next(ctx context.Context) (string, T, error) {
	var zero T
	if watch == nil || watch.runtime == nil {
		return "", zero, newError(ErrInvalid, "next", "event watch", "", "watch is nil")
	}
	if watch.closed.Load() {
		return "", zero, newError(ErrClosed, "next", "event watch", "", "watch is closed")
	}
	if !watch.inNext.CompareAndSwap(false, true) {
		return "", zero, newError(ErrInvalidState, "next", "event watch", "", "concurrent Next calls are invalid")
	}
	defer watch.inNext.Store(false)

	for {
		ready, active := watch.runtime.eventWakes.snapshot(watch.runID)
		if !active || watch.closed.Load() {
			return "", zero, newError(ErrClosed, "next", "event watch", "", "watch is closed")
		}
		queryCtx, cancelQuery := context.WithCancel(ctx)
		stopLifetimeCancel := context.AfterFunc(watch.lifetime, cancelQuery)
		result, err := watch.runtime.store.ReadEventWatch(queryCtx, watch.runID, watch.cursor, watch.event.def.Name)
		stopLifetimeCancel()
		cancelQuery()
		if err != nil {
			if watch.lifetime.Err() != nil {
				return "", zero, newError(ErrClosed, "next", "event watch", "", "watch is closed")
			}
			return "", zero, err
		}
		if watch.closed.Load() {
			return "", zero, newError(ErrClosed, "next", "event watch", "", "watch is closed")
		}
		if _, active := watch.runtime.eventWakes.snapshot(watch.runID); !active {
			return "", zero, newError(ErrClosed, "next", "event watch", "", "runtime is closed")
		}
		if !result.RunFound {
			return "", zero, newError(ErrTerminal, "next", "run", watch.runID.String(), "watched run no longer exists")
		}
		if result.Found {
			decoded, err := journalcodec.DecodeApplicationEvent(result.Body)
			if err != nil {
				return "", zero, newError(ErrInvalidState, "decode", "event", watch.event.def.Name, "stored event body is invalid")
			}
			value, err := watch.event.def.Payload.Decode(decoded.Payload)
			if err != nil {
				return "", zero, newError(ErrInvalidState, "decode", "event payload", watch.event.def.Name, "stored payload does not match its definition")
			}
			payload, ok := value.(T)
			if !ok {
				return "", zero, newError(ErrInvalidState, "decode", "event payload", watch.event.def.Name, "stored payload has an incompatible type")
			}
			watch.cursor = result.Position
			return result.Key, payload, nil
		}
		if isTerminalStoreRunStatus(result.Status) {
			return "", zero, newError(ErrTerminal, "next", "run", watch.runID.String(), "run is terminal")
		}
		if !isActiveStoreRunStatus(result.Status) {
			return "", zero, newError(ErrInvalidState, "next", "run", watch.runID.String(), "stored run status is unknown")
		}
		select {
		case <-ctx.Done():
			return "", zero, ctx.Err()
		case <-watch.lifetime.Done():
			return "", zero, newError(ErrClosed, "next", "event watch", "", "watch is closed")
		case <-ready:
		}
	}
}

// Close removes the local registration and unblocks a waiting Next.
func (watch *EventWatch[T]) Close() {
	if watch == nil {
		return
	}
	watch.closeOnce.Do(func() {
		watch.closed.Store(true)
		if watch.cancel != nil {
			watch.cancel()
		}
		if watch.runtime != nil {
			watch.runtime.eventWakes.unregister(watch.runID)
		}
	})
}

func isActiveStoreRunStatus(status string) bool {
	return status == string(RunStatusRunning) || status == string(RunStatusFailing)
}

func isTerminalStoreRunStatus(status string) bool {
	return status == string(RunStatusSucceeded) || status == string(RunStatusFailed) ||
		status == string(RunStatusCancelled) || status == string(RunStatusExpired)
}
