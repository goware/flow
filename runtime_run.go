package flow

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/store"
)

type runtimeLifecycle uint8

const (
	runtimeCreated runtimeLifecycle = iota
	runtimeRunning
	runtimeStopping
	runtimeStopped
)

var errRuntimeShutdown = errors.New("flow runtime is stopping")
var errAttemptTimeout = errors.New("flow command attempt timed out")

type wakeHub struct {
	mu         sync.Mutex
	generation uint64
	ready      chan struct{}
}

func newWakeHub() *wakeHub { return &wakeHub{ready: make(chan struct{}, 1)} }

func (hub *wakeHub) signal() {
	if hub == nil {
		return
	}
	hub.mu.Lock()
	hub.generation++
	hub.mu.Unlock()
	select {
	case hub.ready <- struct{}{}:
	default:
	}
}

func (hub *wakeHub) snapshot() uint64 {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return hub.generation
}

func (hub *wakeHub) wait(ctx context.Context, seen uint64, interval time.Duration) {
	hub.mu.Lock()
	changed := hub.generation != seen
	hub.mu.Unlock()
	if changed {
		return
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-hub.ready:
	case <-timer.C:
	}
}

type activeCommand struct {
	commandID   uuid.UUID
	attemptID   uuid.UUID
	token       uuid.UUID
	localExpiry time.Time
	cancel      context.CancelCauseFunc
}

type activeCommands struct {
	mu     sync.Mutex
	values map[uuid.UUID]activeCommand
}

func newActiveCommands() *activeCommands {
	return &activeCommands{values: make(map[uuid.UUID]activeCommand)}
}

func (active *activeCommands) register(command activeCommand) {
	active.mu.Lock()
	active.values[command.commandID] = command
	active.mu.Unlock()
}

func (active *activeCommands) unregister(commandID, attemptID uuid.UUID) {
	active.mu.Lock()
	command, exists := active.values[commandID]
	if exists && command.attemptID == attemptID {
		delete(active.values, commandID)
	}
	active.mu.Unlock()
}

func (active *activeCommands) snapshot() []activeCommand {
	active.mu.Lock()
	defer active.mu.Unlock()
	result := make([]activeCommand, 0, len(active.values))
	for _, command := range active.values {
		result = append(result, command)
	}
	return result
}

func (active *activeCommands) renewed(commandID uuid.UUID, expiresAt time.Time) {
	active.mu.Lock()
	command, exists := active.values[commandID]
	if exists {
		command.localExpiry = expiresAt
		active.values[commandID] = command
	}
	active.mu.Unlock()
}

func (active *activeCommands) cancelExpired() {
	now := time.Now()
	active.mu.Lock()
	defer active.mu.Unlock()
	for _, command := range active.values {
		if !now.Before(command.localExpiry) {
			command.cancel(ErrLeaseLost)
		}
	}
}

func (active *activeCommands) cancelUnrenewed(renewed map[uuid.UUID]struct{}) {
	active.mu.Lock()
	defer active.mu.Unlock()
	for id, command := range active.values {
		if _, ok := renewed[id]; !ok {
			command.cancel(ErrLeaseLost)
		}
	}
}

func (active *activeCommands) cancelAll(cause error) {
	active.mu.Lock()
	defer active.mu.Unlock()
	for _, command := range active.values {
		command.cancel(cause)
	}
}

func waitGroupContext(ctx context.Context, group *sync.WaitGroup) bool {
	done := make(chan struct{})
	go func() {
		group.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *Runtime) Run(ctx context.Context) error {
	if r == nil {
		return newError(ErrInvalid, "run", "runtime", "", "runtime is nil")
	}
	r.mu.Lock()
	if r.lifecycle != runtimeCreated || r.closed {
		r.mu.Unlock()
		return newError(ErrInvalidState, "run", "runtime", "", "runtime may run only once")
	}
	r.registry.freeze()
	r.lifecycle = runtimeRunning
	r.runDone = make(chan struct{})
	runCtx, cancel := context.WithCancel(context.Background())
	r.runCancel = cancel
	r.mu.Unlock()

	watcherDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-watcherDone:
		}
	}()

	serviceCtx, stopServices := context.WithCancel(context.Background())
	var services sync.WaitGroup
	services.Add(2 + r.planConcurrency)
	go func() {
		defer services.Done()
		r.runLeaseManager(serviceCtx)
	}()
	go func() {
		defer services.Done()
		r.runMaintenance(serviceCtx)
	}()
	for range r.planConcurrency {
		go func() {
			defer services.Done()
			r.runPlanScheduler(serviceCtx)
		}()
	}

	r.observe(context.Background(), Observation{Kind: ObservationRuntime, Operation: "run", Outcome: "started", Worker: r.replicaName()})
	r.runCommandScheduler(runCtx)

	r.mu.Lock()
	if r.lifecycle == runtimeRunning {
		r.lifecycle = runtimeStopping
	}
	r.mu.Unlock()
	graceCtx, cancelGrace := context.WithTimeout(context.Background(), r.shutdownGrace)
	graceful := waitGroupContext(graceCtx, &r.workerGroup)
	cancelGrace()
	if !graceful {
		r.active.cancelAll(errRuntimeShutdown)
		settleCtx, cancelSettle := context.WithTimeout(context.Background(), min(2*time.Second, max(100*time.Millisecond, r.commandLease/2)))
		_ = waitGroupContext(settleCtx, &r.workerGroup)
		cancelSettle()
	}
	stopServices()
	services.Wait()
	close(watcherDone)

	r.mu.Lock()
	r.lifecycle = runtimeStopped
	r.closed = true
	r.runCancel = nil
	close(r.runDone)
	r.mu.Unlock()
	r.observe(context.Background(), Observation{Kind: ObservationRuntime, Operation: "run", Outcome: "stopped", Worker: r.replicaName()})
	return nil
}

func (r *Runtime) Stop(ctx context.Context) error {
	if r == nil {
		return newError(ErrInvalid, "stop", "runtime", "", "runtime is nil")
	}
	r.mu.Lock()
	switch r.lifecycle {
	case runtimeCreated:
		r.lifecycle = runtimeStopped
		r.closed = true
		if r.runDone == nil {
			r.runDone = make(chan struct{})
			close(r.runDone)
		}
	case runtimeRunning:
		r.lifecycle = runtimeStopping
		r.runCancel()
	case runtimeStopping:
	case runtimeStopped:
	}
	done := r.runDone
	r.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runtime) runLeaseManager(ctx context.Context) {
	interval := max(10*time.Millisecond, r.commandLease/3)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		current := r.active.snapshot()
		if len(current) == 0 {
			continue
		}
		renewals := make([]store.LeaseRenewal, len(current))
		for index, command := range current {
			renewals[index] = store.LeaseRenewal{CommandID: command.commandID, AttemptID: command.attemptID, Token: command.token}
		}
		if err := r.faults.Hit(ctx, fault.RenewBeforeResult); err != nil {
			r.active.cancelExpired()
			continue
		}
		renewed, err := r.store.RenewCommandLeases(ctx, renewals, r.commandLease)
		if err != nil {
			r.active.cancelExpired()
			r.observe(ctx, Observation{Kind: ObservationLease, Operation: "renew", Outcome: "error", Count: int64(len(current))})
			continue
		}
		renewedSet := make(map[uuid.UUID]struct{}, len(renewed))
		for _, lease := range renewed {
			renewedSet[lease.CommandID] = struct{}{}
			r.active.renewed(lease.CommandID, time.Now().Add(r.commandLease))
		}
		r.active.cancelUnrenewed(renewedSet)
		r.observe(ctx, Observation{Kind: ObservationLease, Operation: "renew", Outcome: "ok", Count: int64(len(renewed))})
	}
}

func (r *Runtime) runMaintenance(ctx context.Context) {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		progress := false
		if ids, err := r.store.ProbeExpiredExecutions(ctx, 64); err == nil {
			_ = r.faults.Hit(ctx, fault.MaintenanceAfterProbe)
			for _, id := range ids {
				changed, expireErr := r.store.ExpireExecution(ctx, id, "execution deadline reached")
				if expireErr == nil && changed {
					progress = true
				}
			}
		}
		if waits, err := r.store.ProbeExpiredCommandWaits(ctx, 128); err == nil {
			_ = r.faults.Hit(ctx, fault.MaintenanceAfterProbe)
			for _, candidate := range waits {
				changed, expireErr := r.store.ExpireCommandWait(ctx, candidate)
				if expireErr == nil && changed {
					progress = true
				}
			}
		}
		if leases, err := r.store.ProbeExpiredCommandLeases(ctx, 128); err == nil {
			_ = r.faults.Hit(ctx, fault.MaintenanceAfterProbe)
			for _, candidate := range leases {
				changed, recoverErr := r.store.RecoverExpiredCommandLease(ctx, candidate)
				if recoverErr == nil && changed {
					progress = true
				}
			}
		}
		if progress {
			r.wake.signal()
		}
	}
}

func (r *Runtime) replicaName() string {
	return "runtime-" + r.instanceID.String()
}
