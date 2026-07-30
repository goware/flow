---
status: complete
---

# Component: distributed runtime and operations

## 1. Purpose

The runtime connects deterministic engine transitions to concurrent PostgreSQL-backed execution. It owns registration, scheduler capacity, claiming, handler invocation, lease renewal, notification hints, maintenance, shutdown, observers, and fault hooks. It does not reinterpret durable business semantics defined by the engine/store.

## 2. Lifecycle

`New` validates options, applies migrations when requested by the existing schema option, creates store/scheduler state, and returns a client-capable runtime. `Register` is allowed before `Run`; registry mutation freezes when `Run` begins. One runtime runs at most once.

`Run(ctx)` starts:

- command scheduler;
- plan scheduler;
- coordinator scheduler;
- lease renewer;
- dependency/wait/deadline/recovery maintenance;
- optional PostgreSQL notification listener;
- observer adapter.

Cancellation stops new claims, requests handler cancellation, releases or lets leases expire according to shutdown grace, drains short transactions, closes the listener, and returns a joined fatal error if any internal correctness loop failed.

## 3. Configuration

Public options configure worker, plan, coordinator, and queue concurrency; poll interval; notifications; shutdown grace; observers; plan verification; schema/migrations; and maximum commands per execution.

The production command lease is a fixed 60 seconds. Renewal cadence derives from it. No public lease option exists. A private runtime field/config constructor available only to package and in-package tests permits short leases for deterministic fault tests.

Defaults:

| Setting | Default |
|---|---:|
| worker concurrency | 16 |
| plan concurrency | 1 |
| coordinator concurrency | 4 |
| poll interval | 1s |
| command lease | 60s fixed |
| shutdown grace | 30s |
| notifications | enabled |
| max commands/execution | 1,000 |

## 4. Registry and compatibility

Registrations erase typed workers, plans, and coordinators into immutable name/version maps. Duplicate exact keys are rejected even if function pointers appear equal. Worker registrations include codecs, defaults, handler, and optional commit function. Plan registrations include invocation and start codec. Coordinator registrations include state codec and selectors.

Claims match exact command name/version. Dirty-plan claims require exact plan name/version. Coordinator delivery claims require exact coordinator name/version. Unknown work remains durable and unclaimed until a compatible replica appears.

External event emitters require no registry. Command workers in coordinator mode require no coordinator code merely to settle; terminal events remain retained until a coordinator-capable replica processes them.

## 5. Capacity and scheduler structure

Each scheduler owns a semaphore and one coalescing wake channel. The command scheduler also owns per-queue semaphores. It computes free capacity before probing and never claims more work than can run.

No worker goroutine owns a PostgreSQL listener or hot poll loop. One listener fans hints to schedulers. Scheduler polling is bounded and adaptive: immediate repoll after a full claim batch, short yield after partial progress, normal poll interval after empty result.

## 6. Command claim path

The claim probe receives available queue slots and registered command pairs. SQL selects queue rows with eligible time <= database now and non-terminal commands, orders by priority/eligible time/stable ID, limits candidates, and uses `FOR UPDATE SKIP LOCKED` on both queue and command rows.

Claim never waits on a locked execution/command. It sets running ownership, token, lease expiry, invocation ordinal, and attempt start in a short transaction, then commits. Decoding and handler invocation happen after returning the connection.

Unhandled kind/version rows may remain near the queue head. The claim benchmark includes this adversarial distribution; a future denormalized kind index is evidence-driven, not part of M1.

## 7. Worker invocation

Before invoking, the runtime loads immutable arguments, command metadata, and explicitly declared dependency outcomes in bounded batches. It builds `Work[A]` with a private decision buffer.

The handler context is cancelled at the earliest attempt timeout, retry elapsed deadline, execution deadline, runtime shutdown, or detected lease loss. Worker panic becomes a classified attempt conclusion.

On return, the runtime does not reuse any connection held by application code because none was provided. It enters fenced settlement with the attempt token. Scope defects, oversized result, and deterministic child declaration conflicts are permanent failed conclusions.

## 8. Settlement

Successful settlement:

1. lock execution then command and validate running token;
2. capture PostgreSQL time;
3. canonicalize/validate the already returned result and decision;
4. append staged application events in deterministic order;
5. create staged children, append terminal outcome, and close membership;
6. resolve exact event waits, dependencies, fail-fast, dirty plan, and completion;
7. run registered short application commit function;
8. append/send transactional notification hints;
9. commit once.

Flow writes precede application writes but are invisible until shared commit. Commit-function error rolls back all changes. A stale token writes nothing.

Retryable settlement records attempt conclusion, consumed count, and persisted next time. Interruption/lost lease restores the pre-attempt schedule without consuming budget.

## 9. Lease manager

Renewal runs near one third of 60 seconds with deterministic bounded spread. It groups active attempts into one statement per subject table and extends only matching running token rows. Returned rows are authoritative; missing rows cancel their handler contexts as lease lost.

Recovery claims expired running rows with skip-locked bounded batches, records interruption/loss under the execution lock, clears ownership, and restores schedule. Neither renewal nor recovery moves retry budget anchor or execution deadline.

Observers expose interruption count and interruption-to-consumed-attempt ratio so workloads longer than deploy cadence are alertable.

## 10. Plan scheduler

The scheduler probes dirty, non-terminal plan executions whose reconciliation lease is absent/expired and whose exact plan is registered. It claims with `SKIP LOCKED`, loads the snapshot, invokes the pure plan without context, and commits validated reconciliation through the store.

Triggers never execute plan code in caller, worker settlement, event ingress, or maintenance transactions. They only set dirty and hint the scheduler. This isolates plan defects and allows publishers/workers to deploy independently.

Plan verification may double-evaluate the same snapshot. Test/debug configuration may evaluate even when an optimization says skipping is safe, comparing declarations.

Per-step plan progression adds a scheduler hop. Notifications normally reduce it to milliseconds; poll-only adds up to one poll interval. Already declared dependency and wait release happens in the triggering transaction without another plan pass.

## 11. Coordinator scheduler

The scheduler finds coordinators with start pending or a matching journal position above their inbox, respects global capacity, and claims the coordinator row with a fenced lease. One execution has one serialized coordinator decision.

Selection uses event name for `On` and command name/version plus terminal position for `OnOutcome`. It must avoid rescanning unmatched prefixes; indexes and benchmarks cover sparse subscriptions.

The runtime decodes a private state copy and typed `Received[T]`. Handler nil return settles state, staged events, staged commands, inbox, transition history, and optional `Succeed`/`Fail` atomically. Handler error discards the decision and retries the same position using the coordinator's stored accepted policy. Permanent/exhausted error records coordinator failure.

## 12. Notifications and polling

One dedicated connection executes `LISTEN` on schema-derived channels. Transactional `NOTIFY` is emitted only on transitions that may make new work visible. Payloads are bounded hints containing no correctness state.

Listener disconnect uses capped backoff, observes health, and reconnects. Polling continues throughout disconnect and when notifications are disabled. Wake channels are capacity one and coalesce bursts.

## 13. Maintenance

Bounded maintenance tasks include:

- delayed/retry queue eligibility checks;
- exact wait expiry;
- execution deadline terminalization;
- expired command/coordinator/plan lease recovery;
- notification repair hints where needed;
- invariant scans in tests/diagnostics.

Every task uses PostgreSQL time, skip-locked batches where appropriate, semantic execution locking, deterministic journal order, and idempotent repetition.

## 14. Client and transaction composition

`Runtime` directly implements `Client`. Root execution, external `Event.Emit`, cancellation, and inspection use short pool transactions and never invoke application handlers inline.

`Runtime.InTx(tx)` returns a transaction client. Flow operations execute immediately inside the caller transaction without committing or sending local wake signals before commit. The caller performs Flow operations before application row locks. Multiple execution IDs must be ascending; reverse order returns `ErrInvalidState` before waiting.

Application code persists returned `ExecutionID` with its domain object in the same transaction where possible. `ListExecutions` is not used for identity lookup.

## 15. External events

`Event.Emit` resolves its typed descriptor, encodes and bounds the payload, and enters one semantic transaction. Equivalent identity/content succeeds idempotently, including after terminality. Conflict or a genuinely new terminal write returns structured error. It is rejected through a worker/coordinator attempt context because decision output must use staged `flow.Emit`.

New external facts append history, resolve exact waits, dirty plan mode, and wake coordinator selection. Staged facts take the same projection path inside the accepting worker/coordinator settlement. Handler scopes expose no `Client`; calling external ingress from worker/coordinator application code is prohibited because it escapes fencing.

## 16. Inspection

- `GetExecution`: exact current summary by ID.
- `ListExecutions`: bounded operational filters and stable pagination.
- `History`: bounded position scan.
- `Trace`: batched graph/attempt/wait/cause projection.
- `AwaitExecution`: notification-assisted polling until terminal/context cancellation.

Inspection borrows short connections, redacts payloads by default, and never blocks execution locks for long rendering work.

## 17. Observations

The no-op default observer receives immutable values after meaningful transitions. Required categories include:

- execution start/terminal/cancel;
- command claim, attempt start/conclusion, retry, timeout, terminal;
- queue depth/age and unclaimable name/version;
- plan dirty age, claim, evaluation duration, declarations, defect;
- coordinator claim, delivery, retry, failure, terminal decision;
- lease renewal/loss/recovery and interruption ratio;
- notification connect/disconnect/hint/coalescing;
- maintenance batches and failures;
- long-running attempts and connection-pool pressure.

Observer panic is isolated and cannot alter Flow correctness.

## 18. Fault injection

Internal named hooks cover before/after journal append, projection mutation, queue mutation, claim commit, handler return, fence validation, commit function, dependency resolution, plan/coordinator commit, notification, maintenance, and ambiguous commit response.

After successful settlement, each staged application event produces bounded `ObservationEvent` metadata with operation `settle`; payloads never enter observations. The enclosing attempt/coordinator settlement observation reports the staged-event count.

Hooks are unavailable to external packages. Each path's tests prove rollback, idempotent retry, or fence behavior at every hook.

## 19. Error handling

Expected PostgreSQL conflicts map to structured Flow errors. Library-owned safe transactions retry serialization/deadlock/pre-commit connection failures with capped jitter without reinvoking a worker/coordinator handler; they reuse the already canonicalized decision.

Unexpected invariant failure stops the owning scheduler/runtime rather than silently retrying corrupt meaning. Repeated polling errors are rate-limited/aggregated.

## 20. Test plan

Runtime integration tests use real PostgreSQL and cover:

- exact-version registry claims and rolling deployments;
- global/per-queue capacity and no connection held by handlers;
- fixed public lease plus private short-lease test seam;
- renewal, loss, takeover, shutdown, interruption budgets;
- retry/deadline/timeout/cancellation races;
- commit-function atomicity and fence rejection;
- plan dirty coalescing, crash recovery, poll-only latency;
- coordinator start/events/outcomes/retries/terminal decisions;
- notification loss/reconnect and maintenance;
- caller transactions and multi-execution lock order;
- all four examples across multiple replicas.

Benchmarks cover claim probe, same-execution burst, unhandled head kinds, dirty plan probe/snapshot, sparse coordinator outcome matching, notification-vs-poll latency, and connection utilization.

## 21. Acceptance conditions

- No public lease option or legacy API path remains.
- One listener and bounded scheduler loops serve a runtime; workers do not own listeners or hot polls.
- Claims are capacity-bounded, skip-locked, exact-version, and connection-short.
- All attempt/plan/coordinator settlement is fenced and journaled.
- Event publishers and specialized workers need no plan code.
- Poll-only operation is correct.
- Stored command/coordinator policies survive rollout default changes.
- The private lease seam is inaccessible to application packages.
- Multi-replica E2E and fault tests prove takeover and exactly-once accepted progression.
