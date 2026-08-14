package flow

import (
	"context"
	"sync"

	"github.com/goware/flow/internal/uuid"
)

// eventWakeHub coalesces run-scoped notification hints. It owns no goroutines:
// EventWatch.Next blocks the caller's goroutine on the returned channel.
type eventWakeHub struct {
	mu      sync.Mutex
	entries map[uuid.UUID]*eventWakeEntry
	closed  bool
	ctx     context.Context
	cancel  context.CancelFunc
}

type eventWakeEntry struct {
	ready    chan struct{}
	watchers int
}

func newEventWakeHub() *eventWakeHub {
	ctx, cancel := context.WithCancel(context.Background())
	return &eventWakeHub{entries: make(map[uuid.UUID]*eventWakeEntry), ctx: ctx, cancel: cancel}
}

func (hub *eventWakeHub) register(runID uuid.UUID) (context.Context, bool) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed {
		return nil, false
	}
	entry := hub.entries[runID]
	if entry == nil {
		entry = &eventWakeEntry{ready: make(chan struct{})}
		hub.entries[runID] = entry
	}
	entry.watchers++
	return hub.ctx, true
}

func (hub *eventWakeHub) unregister(runID uuid.UUID) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	entry := hub.entries[runID]
	if entry == nil {
		return
	}
	entry.watchers--
	if entry.watchers == 0 {
		delete(hub.entries, runID)
	}
}

func (hub *eventWakeHub) snapshot(runID uuid.UUID) (<-chan struct{}, bool) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed {
		return nil, false
	}
	entry := hub.entries[runID]
	if entry == nil {
		return nil, false
	}
	return entry.ready, true
}

func (hub *eventWakeHub) signal(runID uuid.UUID) {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed {
		return
	}
	entry := hub.entries[runID]
	if entry == nil {
		return
	}
	close(entry.ready)
	entry.ready = make(chan struct{})
}

func (hub *eventWakeHub) signalAll() {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	if hub.closed {
		return
	}
	for _, entry := range hub.entries {
		close(entry.ready)
		entry.ready = make(chan struct{})
	}
}

func (hub *eventWakeHub) close() {
	hub.mu.Lock()
	if hub.closed {
		hub.mu.Unlock()
		return
	}
	hub.closed = true
	for _, entry := range hub.entries {
		close(entry.ready)
	}
	clear(hub.entries)
	hub.mu.Unlock()
	hub.cancel()
}
