package flow

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/store"
	"github.com/jackc/pgx/v5"
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

const (
	maintenanceExecutionPage = 64
	maintenanceWaitPage      = 128
	maintenanceLeasePage     = 128
	maintenanceDrainPasses   = 8
)

type wakeHub struct {
	mu         sync.Mutex
	generation uint64
	ready      chan struct{}
}

func newWakeHub() *wakeHub { return &wakeHub{ready: make(chan struct{})} }

func (hub *wakeHub) signal() {
	if hub == nil {
		return
	}
	hub.mu.Lock()
	hub.generation++
	close(hub.ready)
	hub.ready = make(chan struct{})
	hub.mu.Unlock()
}

func (hub *wakeHub) snapshot() uint64 {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return hub.generation
}

func (hub *wakeHub) wait(ctx context.Context, seen uint64, interval time.Duration) {
	hub.mu.Lock()
	changed := hub.generation != seen
	ready := hub.ready
	hub.mu.Unlock()
	if changed {
		return
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-ready:
	case <-timer.C:
	}
}

type activeCommand struct {
	commandID   uuid.UUID
	attemptID   uuid.UUID
	token       uuid.UUID
	localExpiry time.Time
	cancel      context.CancelCauseFunc
	cancelled   bool
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
		if command.cancelled {
			continue
		}
		result = append(result, command)
	}
	return result
}

func (active *activeCommands) renewed(commandID, attemptID uuid.UUID, expiresAt time.Time) bool {
	active.mu.Lock()
	defer active.mu.Unlock()
	command, exists := active.values[commandID]
	if exists && command.attemptID == attemptID && !command.cancelled {
		command.localExpiry = expiresAt
		active.values[commandID] = command
		return true
	}
	return false
}

func (active *activeCommands) cancelExpired() int {
	now := time.Now()
	active.mu.Lock()
	defer active.mu.Unlock()
	cancelled := 0
	for id, command := range active.values {
		if !command.cancelled && !now.Before(command.localExpiry) {
			command.cancelled = true
			active.values[id] = command
			command.cancel(ErrLeaseLost)
			cancelled++
		}
	}
	return cancelled
}

func (active *activeCommands) cancelLost(commandID, attemptID uuid.UUID) bool {
	active.mu.Lock()
	defer active.mu.Unlock()
	command, exists := active.values[commandID]
	if !exists || command.attemptID != attemptID || command.cancelled {
		return false
	}
	command.cancelled = true
	active.values[commandID] = command
	command.cancel(ErrLeaseLost)
	return true
}

func (active *activeCommands) cancelAll(cause error) {
	active.mu.Lock()
	defer active.mu.Unlock()
	for id, command := range active.values {
		if command.cancelled {
			continue
		}
		command.cancelled = true
		active.values[id] = command
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
	r.observations.run()

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
	serviceCount := 3
	if r.notifications {
		serviceCount++
	}
	services.Add(serviceCount)
	go func() {
		defer services.Done()
		r.runLeaseManager(serviceCtx)
	}()
	go func() {
		defer services.Done()
		r.runMaintenance(serviceCtx)
	}()
	go func() {
		defer services.Done()
		r.runLeaseWatchdog(serviceCtx)
	}()
	if r.notifications {
		go func() {
			defer services.Done()
			r.runNotificationListener(serviceCtx)
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
	r.observations.close()
	return nil
}

// runNotificationListener owns exactly one session-capable connection outside
// the application pool. Hints only reduce latency: every connect performs a
// broad catch-up wake, and every scheduler retains its correctness poll.
func (r *Runtime) runNotificationListener(ctx context.Context) {
	const (
		initialBackoff = 50 * time.Millisecond
		maxBackoff     = time.Second
	)
	backoff := initialBackoff
	channel := r.store.NotificationChannel()
	listenSQL := "LISTEN " + pgx.Identifier{channel}.Sanitize()
	for ctx.Err() == nil {
		if err := r.faults.Hit(ctx, fault.NotifyConnect); err != nil {
			r.observe(ctx, Observation{Kind: ObservationRuntime, Operation: "notify_listener", Outcome: "connect_error"})
			if !waitContext(ctx, backoff) {
				return
			}
			backoff = min(maxBackoff, backoff*2)
			continue
		}
		config := r.db.Conn.Config().ConnConfig.Copy()
		if config.RuntimeParams == nil {
			config.RuntimeParams = make(map[string]string)
		}
		config.RuntimeParams["application_name"] = "flow-listener-" + r.instanceID.String()
		conn, err := pgx.ConnectConfig(ctx, config)
		if err == nil {
			_, err = conn.Exec(ctx, listenSQL)
		}
		if err != nil {
			if conn != nil {
				_ = conn.Close(context.WithoutCancel(ctx))
			}
			r.observe(ctx, Observation{Kind: ObservationRuntime, Operation: "notify_listener", Outcome: "connect_error"})
			if !waitContext(ctx, backoff) {
				return
			}
			backoff = min(maxBackoff, backoff*2)
			continue
		}

		backoff = initialBackoff
		// LISTEN begins only after its statement commits. Wake immediately to
		// close the commit-before-LISTEN and reconnect windows.
		r.wake.signal()
		r.observe(ctx, Observation{Kind: ObservationRuntime, Operation: "notify_listener", Outcome: "listening"})
		for ctx.Err() == nil {
			if err := r.faults.Hit(ctx, fault.NotifyBeforeWait); err != nil {
				break
			}
			notification, waitErr := conn.WaitForNotification(ctx)
			if waitErr != nil {
				break
			}
			r.wake.signal()
			if _, valid := store.ParseNotificationHint(notification.Payload); valid {
				r.observe(ctx, Observation{Kind: ObservationRuntime, Operation: "notify_hint", Outcome: "received"})
			} else {
				// Unknown versions and malformed hints are never interpreted as
				// work. A bounded broad wake is forward-compatible and safe.
				r.observe(ctx, Observation{Kind: ObservationRuntime, Operation: "notify_hint", Outcome: "broad_wake"})
			}
		}
		_ = conn.Close(context.WithoutCancel(ctx))
		if ctx.Err() == nil {
			r.observe(ctx, Observation{Kind: ObservationRuntime, Operation: "notify_listener", Outcome: "reconnecting"})
			if !waitContext(ctx, backoff) {
				return
			}
			backoff = min(maxBackoff, backoff*2)
		}
	}
}

func waitContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
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
		started := time.Now()
		renewCtx, cancel := context.WithTimeout(ctx, commandRenewalTimeout(r.commandLease))
		err := r.faults.Hit(renewCtx, fault.RenewBeforeResult)
		var results []store.LeaseRenewalResult
		if err == nil {
			results, err = r.store.RenewCommandLeases(renewCtx, renewals, r.commandLease)
		}
		cancel()
		duration := time.Since(started)
		if err != nil {
			r.observe(ctx, Observation{Kind: ObservationLease, Operation: "renew", Outcome: "error",
				Count: int64(len(current)), Duration: duration, Worker: r.replicaName()})
			continue
		}
		counts := map[store.LeaseRenewalOutcome]int64{
			store.LeaseRenewed: 0, store.LeaseLost: 0, store.LeaseUncertain: 0,
		}
		localLost := int64(0)
		for _, result := range results {
			counts[result.Outcome]++
			switch result.Outcome {
			case store.LeaseRenewed:
				if result.LeaseExpiresAt == nil {
					continue
				}
				r.active.renewed(result.CommandID, result.AttemptID, started.Add(r.commandLease))
			case store.LeaseLost:
				if r.active.cancelLost(result.CommandID, result.AttemptID) {
					localLost++
				}
			case store.LeaseUncertain:
			}
		}
		outcome := "ok"
		if counts[store.LeaseLost]+counts[store.LeaseUncertain] > 0 {
			outcome = "partial"
		}
		r.observe(ctx, Observation{Kind: ObservationLease, Operation: "renew", Outcome: outcome,
			Count: int64(len(current)), Duration: duration, Worker: r.replicaName()})
		for _, resultOutcome := range []store.LeaseRenewalOutcome{store.LeaseRenewed, store.LeaseLost, store.LeaseUncertain} {
			if counts[resultOutcome] == 0 {
				continue
			}
			r.observe(ctx, Observation{Kind: ObservationLease, Operation: "renew_result", Outcome: string(resultOutcome),
				Count: counts[resultOutcome], Worker: r.replicaName()})
		}
		if localLost > 0 {
			r.observe(ctx, Observation{Kind: ObservationLease, Operation: "local_cancel", Outcome: "lost",
				Count: localLost, Worker: r.replicaName()})
		}
	}
}

func commandRenewalTimeout(lease time.Duration) time.Duration {
	return max(10*time.Millisecond, min(5*time.Second, lease/6))
}

func leaseWatchdogInterval(lease time.Duration) time.Duration {
	return max(10*time.Millisecond, min(time.Second, lease/6))
}

func (r *Runtime) runLeaseWatchdog(ctx context.Context) {
	ticker := time.NewTicker(leaseWatchdogInterval(r.commandLease))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if cancelled := r.active.cancelExpired(); cancelled > 0 {
				r.observe(ctx, Observation{Kind: ObservationLease, Operation: "local_cancel", Outcome: "expired",
					Count: int64(cancelled), Worker: r.replicaName()})
			}
		}
	}
}

func (r *Runtime) runMaintenance(ctx context.Context) {
	timer := time.NewTimer(r.pollInterval)
	defer timer.Stop()
	consecutiveDrainPasses := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		result := r.runMaintenancePass(ctx)
		if result.progressed {
			r.wake.signal()
		}
		delay := r.pollInterval
		if result.progressed && result.saturated {
			consecutiveDrainPasses++
			delay = min(r.pollInterval, time.Millisecond)
			if consecutiveDrainPasses >= maintenanceDrainPasses {
				consecutiveDrainPasses = 0
				delay = min(r.pollInterval, 25*time.Millisecond)
			}
		} else {
			consecutiveDrainPasses = 0
		}
		timer.Reset(delay)
	}
}

type maintenancePassResult struct {
	progressed bool
	saturated  bool
}

func (r *Runtime) runMaintenancePass(ctx context.Context) maintenancePassResult {
	var result maintenancePassResult

	started := time.Now()
	executions, err := r.store.ProbeExpiredExecutions(ctx, maintenanceExecutionPage)
	r.observeMaintenanceProbe(ctx, "deadline_probe", len(executions), err, started)
	if err == nil {
		result.saturated = len(executions) == maintenanceExecutionPage
		changed, transitionErr := r.runExecutionDeadlinePage(ctx, executions)
		result.progressed = result.progressed || changed > 0
		if len(executions) > 0 {
			r.observeMaintenanceTransition(ctx, "deadline", len(executions), changed, transitionErr, started)
		}
	}

	started = time.Now()
	waits, err := r.store.ProbeExpiredCommandWaits(ctx, maintenanceWaitPage)
	r.observeMaintenanceProbe(ctx, "wait_expiry_probe", len(waits), err, started)
	if err == nil {
		result.saturated = result.saturated || len(waits) == maintenanceWaitPage
		changed, transitionErr := r.runWaitExpiryPage(ctx, waits)
		result.progressed = result.progressed || changed > 0
		if len(waits) > 0 {
			r.observeMaintenanceTransition(ctx, "wait_expiry", len(waits), changed, transitionErr, started)
		}
	}

	started = time.Now()
	leases, err := r.store.ProbeExpiredCommandLeases(ctx, maintenanceLeasePage)
	r.observeMaintenanceProbe(ctx, "lease_recovery_probe", len(leases), err, started)
	if err == nil {
		result.saturated = result.saturated || len(leases) == maintenanceLeasePage
		changed, transitionErr := r.runLeaseRecoveryPage(ctx, leases)
		result.progressed = result.progressed || changed > 0
		if len(leases) > 0 {
			r.observeMaintenanceTransition(ctx, "lease_recovery", len(leases), changed, transitionErr, started)
		}
	}
	if result.saturated {
		outcome := "blocked"
		if result.progressed {
			outcome = "drain"
		}
		r.observe(ctx, Observation{Kind: ObservationRuntime, Operation: "maintenance_pass", Outcome: outcome,
			Count: 1, Worker: r.replicaName()})
	}

	return result
}

func (r *Runtime) runExecutionDeadlinePage(ctx context.Context, candidates []uuid.UUID) (int, error) {
	if len(candidates) > 0 {
		if err := r.faults.Hit(ctx, fault.MaintenanceAfterProbe); err != nil {
			return 0, err
		}
	}
	changed := 0
	var firstErr error
	for _, id := range candidates {
		progressed, err := r.store.ExpireExecution(ctx, id, "execution deadline reached")
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if progressed {
			changed++
		}
	}
	return changed, firstErr
}

func (r *Runtime) runWaitExpiryPage(ctx context.Context, candidates []store.ExpiredWaitCandidate) (int, error) {
	if len(candidates) > 0 {
		if err := r.faults.Hit(ctx, fault.MaintenanceAfterProbe); err != nil {
			return 0, err
		}
	}
	changed := 0
	var firstErr error
	for _, candidate := range candidates {
		progressed, err := r.store.ExpireCommandWait(ctx, candidate)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if progressed {
			changed++
		}
	}
	return changed, firstErr
}

func (r *Runtime) runLeaseRecoveryPage(ctx context.Context, candidates []store.ExpiredLeaseCandidate) (int, error) {
	if len(candidates) > 0 {
		if err := r.faults.Hit(ctx, fault.MaintenanceAfterProbe); err != nil {
			return 0, err
		}
	}
	changed := 0
	var firstErr error
	for _, candidate := range candidates {
		progressed, err := r.store.RecoverExpiredCommandLease(ctx, candidate)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if progressed {
			changed++
		}
	}
	return changed, firstErr
}

func (r *Runtime) observeMaintenanceProbe(ctx context.Context, operation string, count int, err error, started time.Time) {
	if err == nil && count == 0 {
		return
	}
	r.observe(ctx, Observation{Kind: ObservationRuntime, Operation: operation, Outcome: outcomeForError(err),
		Count: int64(count), Duration: time.Since(started), Worker: r.replicaName()})
}

func (r *Runtime) observeMaintenanceTransition(
	ctx context.Context,
	operation string,
	attempted int,
	changed int,
	err error,
	started time.Time,
) {
	outcome := "ok"
	count := changed
	if err != nil {
		outcome = "error"
		if changed > 0 {
			outcome = "partial"
		}
	} else if attempted > 0 && changed == 0 {
		outcome = "noop"
		count = attempted
	}
	r.observe(ctx, Observation{Kind: ObservationRuntime, Operation: operation, Outcome: outcome,
		Count: int64(count), Duration: time.Since(started), Worker: r.replicaName()})
}

func (r *Runtime) replicaName() string {
	return "runtime-" + r.instanceID.String()
}
