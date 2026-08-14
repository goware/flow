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
	maintenanceRunPage     = 64
	maintenanceWaitPage    = 128
	maintenanceLeasePage   = 128
	maintenanceDrainPasses = 8
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
	commandID     uuid.UUID
	attemptID     uuid.UUID
	token         uuid.UUID
	leaseDuration time.Duration
	localExpiry   time.Time
	nextRenewAt   time.Time
	renewing      bool
	retryRenewal  bool
	cancel        context.CancelCauseFunc
	cancelled     bool
}

type activeCommands struct {
	mu      sync.Mutex
	values  map[uuid.UUID]activeCommand
	changed chan struct{}
}

func newActiveCommands() *activeCommands {
	return &activeCommands{values: make(map[uuid.UUID]activeCommand), changed: make(chan struct{})}
}

func (active *activeCommands) signalLocked() {
	close(active.changed)
	active.changed = make(chan struct{})
}

func (active *activeCommands) register(command activeCommand) {
	active.mu.Lock()
	if command.leaseDuration > 0 && command.nextRenewAt.IsZero() {
		command.nextRenewAt = command.localExpiry.Add(-2 * command.leaseDuration / 3)
	}
	active.values[command.commandID] = command
	active.signalLocked()
	active.mu.Unlock()
}

func (active *activeCommands) unregister(commandID, attemptID uuid.UUID) {
	active.mu.Lock()
	command, exists := active.values[commandID]
	if exists && command.attemptID == attemptID {
		delete(active.values, commandID)
		active.signalLocked()
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

func (active *activeCommands) cancelExpired(now time.Time) int {
	active.mu.Lock()
	cancelled := 0
	for id, command := range active.values {
		if !command.cancelled && !command.renewing && !now.Before(command.localExpiry) {
			command.cancelled = true
			active.values[id] = command
			command.cancel(ErrLeaseLost)
			cancelled++
		}
	}
	if cancelled > 0 {
		active.signalLocked()
	}
	active.mu.Unlock()
	return cancelled
}

func (active *activeCommands) cancelAttempt(commandID, attemptID uuid.UUID, cause error) bool {
	active.mu.Lock()
	defer active.mu.Unlock()
	command, exists := active.values[commandID]
	if !exists || command.attemptID != attemptID || command.cancelled {
		return false
	}
	command.cancelled = true
	active.values[commandID] = command
	command.cancel(cause)
	active.signalLocked()
	return true
}

func (active *activeCommands) cancelLost(commandID, attemptID uuid.UUID) bool {
	return active.cancelAttempt(commandID, attemptID, ErrLeaseLost)
}

func (active *activeCommands) cancelAll(cause error) {
	active.mu.Lock()
	changed := false
	for id, command := range active.values {
		if command.cancelled {
			continue
		}
		command.cancelled = true
		active.values[id] = command
		command.cancel(cause)
		changed = true
	}
	if changed {
		active.signalLocked()
	}
	active.mu.Unlock()
}

func (active *activeCommands) nextRenewal() (<-chan struct{}, time.Time, bool) {
	active.mu.Lock()
	defer active.mu.Unlock()
	var earliest time.Time
	found := false
	for _, command := range active.values {
		if command.cancelled || command.renewing || command.leaseDuration <= 0 {
			continue
		}
		if !found || command.nextRenewAt.Before(earliest) {
			earliest, found = command.nextRenewAt, true
		}
	}
	return active.changed, earliest, found
}

func (active *activeCommands) nextExpiry() (<-chan struct{}, time.Time, bool) {
	active.mu.Lock()
	defer active.mu.Unlock()
	var earliest time.Time
	found := false
	for _, command := range active.values {
		if command.cancelled || command.renewing || command.localExpiry.IsZero() {
			continue
		}
		if !found || command.localExpiry.Before(earliest) {
			earliest, found = command.localExpiry, true
		}
	}
	return active.changed, earliest, found
}

func (active *activeCommands) takeDue(now time.Time) []activeCommand {
	active.mu.Lock()
	defer active.mu.Unlock()
	result := make([]activeCommand, 0, len(active.values))
	for id, command := range active.values {
		if command.cancelled || command.renewing || command.leaseDuration <= 0 || command.nextRenewAt.After(now) {
			continue
		}
		command.renewing = true
		active.values[id] = command
		result = append(result, command)
	}
	if len(result) > 0 {
		active.signalLocked()
	}
	return result
}

func (active *activeCommands) renewalSucceeded(command activeCommand, started time.Time) bool {
	active.mu.Lock()
	defer active.mu.Unlock()
	current, exists := active.values[command.commandID]
	if !exists || current.attemptID != command.attemptID || current.cancelled || !current.renewing {
		return false
	}
	current.localExpiry = started.Add(current.leaseDuration)
	current.nextRenewAt = started.Add(current.leaseDuration / 3)
	current.renewing = false
	current.retryRenewal = false
	active.values[command.commandID] = current
	active.signalLocked()
	return true
}

func (active *activeCommands) renewalRetry(command activeCommand, now time.Time) bool {
	active.mu.Lock()
	defer active.mu.Unlock()
	current, exists := active.values[command.commandID]
	if !exists || current.attemptID != command.attemptID || current.cancelled || !current.renewing {
		return false
	}
	current.renewing = false
	remaining := current.localExpiry.Sub(now)
	if remaining <= 0 {
		// Hand local expiry back to the watchdog so it remains the single
		// cancellation/observation path. Keep renewal sufficiently in the
		// future to avoid racing the watchdog with a hot retry loop.
		current.nextRenewAt = now.Add(max(time.Millisecond, current.leaseDuration))
		active.values[command.commandID] = current
		active.signalLocked()
		return false
	}
	delay := min(max(10*time.Millisecond, current.leaseDuration/12), remaining/4)
	if delay <= 0 {
		delay = min(time.Millisecond, remaining)
	}
	current.nextRenewAt = now.Add(delay)
	current.retryRenewal = true
	active.values[command.commandID] = current
	active.signalLocked()
	return true
}

func (active *activeCommands) renewalLost(command activeCommand) bool {
	return active.cancelLost(command.commandID, command.attemptID)
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
	services.Add(1)
	go func() {
		defer services.Done()
		r.runLeaseManager(serviceCtx)
	}()
	services.Add(1)
	go func() {
		defer services.Done()
		r.runMaintenance(serviceCtx)
	}()
	services.Add(1)
	go func() {
		defer services.Done()
		r.runLeaseWatchdog(serviceCtx)
	}()
	if r.notifications {
		services.Add(1)
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
	r.eventWakes.close()
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
// the application pool. Command hints only reduce scheduler latency; event
// watches use targeted hints plus a broad catch-up after every connection and
// always re-read durable journal truth.
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
		r.eventWakes.signalAll()
		r.observe(ctx, Observation{Kind: ObservationRuntime, Operation: "notify_listener", Outcome: "listening"})
		for ctx.Err() == nil {
			if err := r.faults.Hit(ctx, fault.NotifyBeforeWait); err != nil {
				break
			}
			notification, waitErr := conn.WaitForNotification(ctx)
			if waitErr != nil {
				break
			}
			hint, valid := store.ParseNotificationHint(notification.Payload)
			if valid {
				if hint.Kind == store.NotificationRun {
					r.wake.signal()
				}
				r.eventWakes.signal(hint.RunID)
				r.observe(ctx, Observation{Kind: ObservationRuntime, Operation: "notify_hint", Outcome: "received"})
			} else {
				// Unknown versions and malformed hints are never interpreted as
				// work. A bounded broad wake is forward-compatible and safe.
				r.wake.signal()
				r.eventWakes.signalAll()
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
	if r.eventWakes != nil {
		r.eventWakes.close()
	}
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
	for ctx.Err() == nil {
		changed, dueAt, found := r.active.nextRenewal()
		if !waitActiveDeadline(ctx, changed, dueAt, found) {
			return
		}
		current := r.active.takeDue(time.Now())
		if len(current) == 0 {
			continue
		}
		renewals := make([]store.LeaseRenewal, len(current))
		for index, command := range current {
			renewals[index] = store.LeaseRenewal{
				CommandID: command.commandID, AttemptID: command.attemptID,
				Token: command.token, Duration: command.leaseDuration,
			}
		}
		started := time.Now()
		renewCtx, cancel := context.WithTimeout(ctx, renewalCallTimeout(current, started))
		err := r.faults.Hit(renewCtx, fault.RenewBeforeResult)
		var results []store.LeaseRenewalResult
		if err == nil {
			results, err = r.store.RenewCommandLeases(renewCtx, renewals)
		}
		if err == nil {
			// This test seam sits after the bounded store call and before the
			// result is applied under the active-registry lock. Runtime shutdown
			// still cancels it through the service context.
			err = r.faults.Hit(ctx, fault.RenewAfterStore)
		}
		cancel()
		finished := time.Now()
		if err != nil {
			for _, command := range current {
				r.active.renewalRetry(command, finished)
			}
			r.observe(ctx, Observation{Kind: ObservationLease, Operation: "renew", Outcome: "error",
				Count: int64(len(current)), Duration: finished.Sub(started), Worker: r.replicaName()})
			continue
		}
		counts := map[store.LeaseRenewalOutcome]int64{
			store.LeaseRenewed: 0, store.LeaseLost: 0, store.LeaseUncertain: 0,
		}
		localLost := int64(0)
		for index, result := range results {
			counts[result.Outcome]++
			command := current[index]
			switch result.Outcome {
			case store.LeaseRenewed:
				r.active.renewalSucceeded(command, started)
			case store.LeaseLost:
				if r.active.renewalLost(command) {
					localLost++
				}
			case store.LeaseUncertain:
				r.active.renewalRetry(command, finished)
			}
		}
		outcome := "ok"
		if counts[store.LeaseLost]+counts[store.LeaseUncertain] > 0 {
			outcome = "partial"
		}
		r.observe(ctx, Observation{Kind: ObservationLease, Operation: "renew", Outcome: outcome,
			Count: int64(len(current)), Duration: finished.Sub(started), Worker: r.replicaName()})
		for _, resultOutcome := range []store.LeaseRenewalOutcome{store.LeaseRenewed, store.LeaseLost, store.LeaseUncertain} {
			if counts[resultOutcome] > 0 {
				r.observe(ctx, Observation{Kind: ObservationLease, Operation: "renew_result", Outcome: string(resultOutcome),
					Count: counts[resultOutcome], Worker: r.replicaName()})
			}
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

func renewalCallTimeout(commands []activeCommand, now time.Time) time.Duration {
	shortestLease := commands[0].leaseDuration
	shortestRemaining := commands[0].localExpiry.Sub(now)
	retry := commands[0].retryRenewal
	for _, command := range commands[1:] {
		shortestLease = min(shortestLease, command.leaseDuration)
		shortestRemaining = min(shortestRemaining, command.localExpiry.Sub(now))
		retry = retry || command.retryRenewal
	}
	if !retry {
		return commandRenewalTimeout(shortestLease)
	}
	// A retry may use more than the ordinary cap while there is enough local
	// lease window left, but one nearly expired member must not collapse the
	// shared batch deadline below its normal bounded timeout. The store still
	// classifies each fence independently in one set-oriented call.
	return max(commandRenewalTimeout(shortestLease), min(shortestLease/3, shortestRemaining/2))
}

func (r *Runtime) runLeaseWatchdog(ctx context.Context) {
	for ctx.Err() == nil {
		changed, expiresAt, found := r.active.nextExpiry()
		if !waitActiveDeadline(ctx, changed, expiresAt, found) {
			return
		}
		if cancelled := r.active.cancelExpired(time.Now()); cancelled > 0 {
			r.observe(ctx, Observation{Kind: ObservationLease, Operation: "local_cancel", Outcome: "expired",
				Count: int64(cancelled), Worker: r.replicaName()})
		}
	}
}

func waitActiveDeadline(ctx context.Context, changed <-chan struct{}, deadline time.Time, found bool) bool {
	if !found {
		select {
		case <-ctx.Done():
			return false
		case <-changed:
			return true
		}
	}
	delay := time.Until(deadline)
	if delay <= 0 {
		return true
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-changed:
		return true
	case <-timer.C:
		return true
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
		delay, nextDrainPasses := nextMaintenanceDelay(r.pollInterval, result, consecutiveDrainPasses)
		consecutiveDrainPasses = nextDrainPasses
		timer.Reset(delay)
	}
}

type maintenancePassResult struct {
	progressed bool
	saturated  bool
	drainable  bool
}

func nextMaintenanceDelay(pollInterval time.Duration, result maintenancePassResult, consecutiveDrainPasses int) (time.Duration, int) {
	if !result.drainable {
		return pollInterval, 0
	}
	consecutiveDrainPasses++
	if consecutiveDrainPasses >= maintenanceDrainPasses {
		return min(pollInterval, 25*time.Millisecond), 0
	}
	return min(pollInterval, time.Millisecond), consecutiveDrainPasses
}

func (result *maintenancePassResult) recordCategory(returned, pageSize, changed int) {
	saturated := returned == pageSize
	progressed := changed > 0
	result.saturated = result.saturated || saturated
	result.progressed = result.progressed || progressed
	result.drainable = result.drainable || saturated && progressed
}

func (r *Runtime) runMaintenancePass(ctx context.Context) maintenancePassResult {
	var result maintenancePassResult

	started := time.Now()
	runs, err := r.store.ProbeExpiredRuns(ctx, maintenanceRunPage)
	r.observeMaintenanceProbe(ctx, "deadline_probe", len(runs), err, started)
	if err == nil {
		transitionStarted := time.Now()
		changed, transitionErr := r.runRunDeadlinePage(ctx, runs)
		result.recordCategory(len(runs), maintenanceRunPage, changed)
		if len(runs) > 0 {
			r.observeMaintenanceTransition(ctx, "deadline", len(runs), changed, transitionErr, transitionStarted)
		}
	}

	started = time.Now()
	waits, err := r.store.ProbeExpiredCommandWaits(ctx, maintenanceWaitPage)
	r.observeMaintenanceProbe(ctx, "wait_expiry_probe", len(waits), err, started)
	if err == nil {
		transitionStarted := time.Now()
		changed, transitionErr := r.runWaitExpiryPage(ctx, waits)
		result.recordCategory(len(waits), maintenanceWaitPage, changed)
		if len(waits) > 0 {
			r.observeMaintenanceTransition(ctx, "wait_expiry", len(waits), changed, transitionErr, transitionStarted)
		}
	}

	started = time.Now()
	leases, err := r.store.ProbeExpiredCommandLeases(ctx, maintenanceLeasePage)
	r.observeMaintenanceProbe(ctx, "lease_recovery_probe", len(leases), err, started)
	if err == nil {
		transitionStarted := time.Now()
		changed, transitionErr := r.runLeaseRecoveryPage(ctx, leases)
		result.recordCategory(len(leases), maintenanceLeasePage, changed)
		if len(leases) > 0 {
			r.observeMaintenanceTransition(ctx, "lease_recovery", len(leases), changed, transitionErr, transitionStarted)
		}
	}
	if result.saturated {
		outcome := "blocked"
		if result.drainable {
			outcome = "drain"
		}
		r.observe(ctx, Observation{Kind: ObservationRuntime, Operation: "maintenance_pass", Outcome: outcome,
			Count: 1, Worker: r.replicaName()})
	}

	return result
}

func (r *Runtime) runRunDeadlinePage(ctx context.Context, candidates []store.ExpiredRunCandidate) (int, error) {
	if len(candidates) > 0 {
		if err := r.faults.Hit(ctx, fault.MaintenanceAfterProbe); err != nil {
			return 0, err
		}
	}
	changed := 0
	var firstErr error
	for _, candidate := range candidates {
		progressed, err := r.store.ExpireRun(ctx, candidate.RunID, "run deadline reached")
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if progressed {
			changed++
			r.observe(ctx, Observation{
				Kind: ObservationRun, Operation: ObservationOpTerminal, Outcome: ObservationOutcomeExpired,
				RunID: RunID(candidate.RunID.String()), RunKey: candidate.RunKey, RootCommandName: candidate.Definition,
				Worker: r.replicaName(),
			})
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
		result, err := r.store.ExpireCommandWait(ctx, candidate)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if result.Changed {
			changed++
		}
		if result.Expired {
			r.observe(ctx, Observation{
				Kind: ObservationWait, Operation: ObservationOpExpire, Outcome: ObservationOutcomeExpired,
				RunID: RunID(candidate.RunID.String()), CommandID: CommandID(candidate.CommandID.String()),
				CommandKey: result.CommandKey, RunKey: result.RunKey, RootCommandName: result.Definition,
				Worker: r.replicaName(),
			})
		}
		if result.TerminalRun {
			r.observe(ctx, Observation{
				Kind: ObservationRun, Operation: ObservationOpTerminal, Outcome: ObservationOutcomeFailed,
				RunID: RunID(candidate.RunID.String()), RunKey: result.RunKey, RootCommandName: result.Definition,
				Worker: r.replicaName(),
			})
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
		result, err := r.store.RecoverExpiredCommandLease(ctx, candidate)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if result.Changed {
			changed++
		}
		if result.Recovered {
			r.observe(ctx, Observation{
				Kind: ObservationLease, Operation: ObservationOpRecover, Outcome: ObservationOutcomeRecovered,
				RunID: RunID(candidate.RunID.String()), CommandID: CommandID(candidate.CommandID.String()),
				CommandKey: result.CommandKey, RunKey: result.RunKey, RootCommandName: result.Definition,
				Worker: r.replicaName(),
			})
		}
		if result.ExpiredRun {
			r.observe(ctx, Observation{
				Kind: ObservationRun, Operation: ObservationOpTerminal, Outcome: ObservationOutcomeExpired,
				RunID: RunID(candidate.RunID.String()), RunKey: result.RunKey, RootCommandName: result.Definition,
				Worker: r.replicaName(),
			})
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
	return r.replica
}
