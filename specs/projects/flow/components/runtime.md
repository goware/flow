---
status: complete
completed_at: 2026-08-04
---

# Component: distributed runtime and operations

## Purpose and lifecycle

The runtime connects typed worker decisions to durable PostgreSQL work. `Migrate` is explicit. `New` validates configuration/schema and starts no goroutines. `Register` accepts exact command worker registrations until `Run` freezes the registry.

`Run` starts one bounded command scheduler, command lease renewal, an independent local lease-expiry watchdog, wait-expiry/deadline/recovery maintenance, optional reconnecting notification listening, and graceful shutdown.

Defaults are a 60-second command recovery lease, `GOMAXPROCS` worker concurrency (minimum one), one-second polling, 30-second shutdown grace, notifications enabled, and 1000 commands per run. `WithRecoveryLease` is a durable command-definition override for idempotent/replay-safe work; it is not a runtime or enqueue setting. Public runtime options configure worker/named-queue concurrency, polling, notifications, shutdown, observer, schema, and the command ceiling.

## Claim and invocation

The scheduler probes `flow_command_queue` for registered exact command versions whose readiness time has elapsed. Claims use `SKIP LOCKED`, respect global/queue capacity, and install an attempt/lease fence.

Claim materialization loads typed arguments, command information (including
the root `RunKey` from the run row already read for ownership), and all exact
declared event inputs in bounded queries. It adds no per-command run-key query.
Database resources are released before worker code runs. Invocation adds
attempt timeout, panic recovery, error classification, and private decision
state.

Success settlement atomically accepts commit callback SQL, result, events,
sub-commands, readiness changes, and run progression under the attempt fence.
Failure settlement records retry or terminal failure; any unsuccessful terminal
command fails the run, cancels non-running siblings, and preserves running
fences until they settle.

## Event ingress and readiness

External `Event.Deliver` canonicalizes content, performs target-local
idempotency checks, and remains detached from any source attempt. A regular
runtime client commits independently. `runtime.InTx(tx)` returns one named
transaction client whose Flow-first phase guard makes delivery atomic with
later application writes under caller commit ownership. A new event appends
history, resolves exact wait rows, and updates command readiness in one
transaction. Worker-staged events use the same transition during settlement.

`ReplaceCurrentRun` locks the exact expected live-key predecessor, cancels it,
and inserts a distinct successor in one transaction. An unexpected equivalent
current ID is only rediscovered for retry/ambiguous-commit recovery; a different
declaration conflicts. Cancellation/start notifications and observations keep
their existing post-commit boundaries.

The maintenance scheduler expires unresolved wait budgets independently of initial delay. Events only release predeclared commands; they never invoke application code directly.

## Leases, notifications, and shutdown

Active attempts retain IDs, tokens, their resolved lease duration, conservative local expiry, next renewal, in-flight/retry state, and cancellation function. One earliest-due service renews all due attempts in one mixed-duration, time-bounded statement. Exact fences use `FOR UPDATE SKIP LOCKED` and classify every request as renewed, lost, or uncertain. Errors and uncertain rows receive bounded retries inside their remaining local window; a retry may use more than the ordinary five-second cap but never an unbounded database call. Lost cancels only the matching attempt. The independent earliest-expiry watchdog ignores a matching renewal while its result is in flight, then cancels locally expired contexts. Both services share one lightweight registry-change signal, not a goroutine or timer per attempt. Maintenance recovers expired durable leases unchanged so another replica can retry safely.

Notification hints reduce wake latency. Polling remains sufficient through transaction-pooling proxies, lost messages, reconnects, or disabled notifications.

Cancellation stops claims/listening/maintenance, waits through shutdown grace, then cancels remaining worker contexts. The caller-owned database pool is never closed.

Observers receive bounded command, run, event-ingress, renewal classification/local cancellation, maintenance probe/transition, notification, and shutdown facts. Delivery and shutdown drain remain best-effort and secret-free. Observers must return promptly and should honor context cancellation; a blocked callback cannot block runtime shutdown. Fault hooks cover journal, projection, queue, fence, commit, notification, and ambiguous-commit boundaries.

Deployments may combine all workers, run selected command pools, or use API-only publishers. PostgreSQL remains the sole coordination authority.

Operational callers can batch queue-lane measurements with `GetQueueStats`.
Retention is explicit rather than a runtime service: `PruneTerminalRuns` owns
one short bounded transaction, skips locked candidates, and never runs
automatically in the background.
