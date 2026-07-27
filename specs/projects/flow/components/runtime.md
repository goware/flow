---
status: complete
---

# Component: distributed runtime and operations

## 1. Purpose and scope

The runtime is disposable process machinery around the durable store and deterministic engine. It owns registration, process lifecycle, command/coordinator scheduling, dirty-plan reconciliation, handler invocation, lease renewal, polling and optional notifications, maintenance, shutdown, transaction-bound clients, inspection entry points, observations, and fault injection.

Correctness cannot depend on any runtime field surviving a crash. A new compatible replica reconstructs work solely from PostgreSQL.

The `db` handle is application-owned and may be the same `*pgkit.DB` used by every application repository. The runtime borrows it, never closes it, and does not create a second connection pool.

## 2. Runtime state and lifecycle

```go
type Runtime struct {
    db       *pgkit.DB
    store    *store.Store
    cfg      Config
    registry *registry
    observer Observer

    lifecycle atomic.Uint32
    runCtx    context.Context
    stop      context.CancelCauseFunc
    group     runGroup

    workers   *commandScheduler
    plans     *planScheduler
    coords    *coordinatorScheduler
    leases    *leaseManager
    wake      *wakeHub
    notifier  *notifier
    maint     *maintenance
}
```

```text
created -> running -> stopping -> stopped
              \-> failed ------/
```

- `New` validates options and schema compatibility, but starts no goroutine and never migrates.
- `Register` is valid in `created`, including on an API-only runtime that never calls `Run`.
- `Run` freezes the registry, starts components, blocks until cancellation/fatal runtime error, performs shutdown, and may be called once.
- `Stop` is idempotent, triggers graceful shutdown, and waits subject to its context.
- `*Runtime` implements `Client` in every non-closed lifecycle; execution operations do not require `Run`.

A runtime with no registered workers can be an API client. A runtime registering only a subset is a specialized pool. There is no package-global registry.

## 3. Configuration

The functional defaults remain normative:

| Setting | Default |
|---|---|
| command lease | 60 seconds |
| command retry policy | 5 budget-consuming attempts; 1s, 5s, 30s, 2m backoff with jitter |
| elapsed retry bound | none unless declared |
| execution deadline | 30 days |
| commands per execution | 1,000; `0` disables |
| idle poll | 1 second |
| shutdown grace | 30 seconds |
| command args/result | 256 KiB each; automatic command-success events use the result limit |
| application event | 64 KiB for `Emit`/`Publish` |
| coordinator state | 256 KiB |
| dependencies per plan command | 100 |
| plan reconciliation concurrency | 1; configurable independently of command workers |
| PostgreSQL schema | `public`; every Flow table retains its fixed `flow_` prefix |

Command-worker concurrency defaults to `max(1, GOMAXPROCS)` and is configurable globally and per queue. Plan reconciliation uses its separate default above so one runtime holds at most one connection for plan code unless the application opts in to more. A command claim batch is capped by immediately free slots and a small operational maximum; it is not a durable or public queue limit. Invalid lease/renewal relationships, negative concurrency, empty queue names, unsafe schema names, invalid sizes, or negative command ceilings fail `New`.

Environment-variable parsing belongs to the application. Runtime options are typed and immutable after `New`.

## 4. Registry and compatibility

### 4.1 Registration forms

The sealed `Registration` interface is implemented by:

- `Handle(Command, worker, options...)`;
- `PlanDef[A]`;
- `Coordinator[S]`.

This is why the worked plan process registers `ReportExecution` alongside its workers. `Command` alone is not executable without `Handle`; application `Event` definitions need no independent registration.

```go
type registry struct {
    commands     map[nameVersion]workerEntry
    plans        map[nameVersion]planEntry
    coordinators map[nameVersion]coordinatorEntry
    coordEvents  map[nameVersion]selectorSet
}
```

It is built under a mutex while created, deeply copied/frozen by `Run`, and read lock-free afterward. Duplicate and codec/handler conflicts are rejected before any work starts.

### 4.2 Plan reconciliation capability

Application events and terminal command outcomes never execute plan code on the committing replica. They commit their own durable transition, resolve persisted dependencies and `Await` rows, and set `flow_executions.plan_dirty = true`. Consequently, API processes, monitors, and specialized command pools do not need to register the plan whose execution they advance.

A plan scheduler may reconcile a dirty execution only when its registry contains the execution's exact plan name/version. Missing plan code leaves the execution durably dirty and unclaimed; inspection reports `missing_plan_definition`. A compatible replica added later can take over without republishing an event or repeating a worker effect.

Direct execution needs only the command worker. Coordinator command workers likewise do not need the coordinator definition to settle; their terminal events remain in the journal until a compatible coordinator runtime consumes them. Coordinator delivery itself requires the exact coordinator version and matching registered subscription.

### 4.3 Rolling deployment

Claim filters are built from the immutable registry. Old replicas never claim new command versions; a plan scheduler never claims an unknown plan version; and a coordinator replica never claims an unknown coordinator or event/command subscription version. Unknown work remains pending or dirty with no retry consumption.

Rolling deployment assumes that one durable name/version has one orchestration meaning. The registry can diagnose codec and selector conflicts inside one process but cannot compare Go function bodies across replicas. Material plan or coordinator behavior changes therefore deploy under a new version; intentionally mixing divergent code under the same version is unsupported.

Application event keys are reserved across versions while `Await`, plan facts, and coordinator subscriptions match exact versions. A rolling publisher therefore retains the event version required by each in-flight execution until it finishes or is deliberately replaced by a new execution. Publishing a newer version under the same natural key is a conflict, not an upgrade of stored history.

Unclaimable observations distinguish missing command worker, dirty execution missing a plan definition, missing coordinator definition, and no matching coordinator handler. These are deployment gaps, not attempts, and consume no retry budget.

## 5. Command scheduler

### 5.1 Capacity model

There is a global weighted semaphore and logical lane state created on demand for queues returned by the probe. A lane may have a smaller limit and weight; all lanes share the global cap. A registered command pair is capable of every queue. This is important because a changed `WithQueue` default affects new commands only and cannot make old accepted queues unclaimable. The runtime acquires a slot **before** claim and transfers it directly to one handler. There is no leased local backlog.

```go
for runtimeRunning {
    slots := scheduler.tryReserveCapacity()
    if slots == 0 {
        scheduler.waitCapacity(ctx)
        continue
    }

    candidates := store.ProbeCommands(ctx, registry.claimFilter(), slots*probeFactor)
    claimed := scheduler.claimCandidates(ctx, candidates, slots)
    scheduler.releaseUnused(slots - len(claimed))

    if len(claimed) == 0 {
        scheduler.waitAfterEmptyProbe(ctx)
        continue
    }
    for _, c := range claimed {
        scheduler.startHandler(c) // slot ownership moves to goroutine
    }
}
```

The scheduler uses deficit round-robin across nonempty queues so a busy default lane cannot starve a low-rate lane. Fairness is process-local optimization only; database ordering remains `next_run_at, command_id` within a registered kind.

### 5.2 Probe and no-wait claim

Probe uses the denormalized queue index and the exact local command pairs across their stored queues. The probe returns queue names; the scheduler applies global/per-queue capacity and fairness before claim. Command ownership does not depend on plan registration.

Candidates are hints. The scheduler groups by execution, opens a skip-locked semantic transaction, locks the execution first and candidate commands second, and revalidates:

- execution remains running/failing in a state that permits this command;
- the command definition is still locally present;
- command/queue state and schedule agree;
- database time has reached `next_run_at`;
- execution, retry budget, and wait deadlines have not already ended it.

It may claim several commands from the same execution in one short commit, bounded by free slots. Expired budgets/deadlines are settled terminally in that transaction instead of invoking the handler. Successful claim stores the current attempt ID and lease on each queue row and appends its `AttemptStarted` journal entry; it creates no separate attempt row. Handler goroutines start only after commit success.

No claim waits for another execution or work row. A skipped/busy/stale candidate returns its reserved capacity and the loop probes again or sleeps.

### 5.3 Empty-probe waiting

After an empty claim, the lane waits for the earliest of:

- a generation-counted wake hint;
- the next indexed future `next_run_at` among locally compatible work;
- the fallback poll interval;
- shutdown.

The wake generation is sampled before the probe and passed to the wait, closing the hint-between-check-and-sleep race. Timers are local latency aids only; claim revalidates database time.

## 6. Plan reconciliation scheduler

Plan reconciliation has its own small, configurable concurrency limit and never consumes a command-worker slot. The scheduler probes the partial dirty-plan index using the exact plan pairs in its frozen registry, ordered by the first unconsumed trigger's `plan_dirty_since`, then claims execution rows with `FOR UPDATE SKIP LOCKED`. Coalesced triggers do not move that timestamp. It holds no leased local backlog: one claimed execution is evaluated and committed in that same short transaction.

For each claim the runtime loads the durable plan snapshot, evaluates the pure plan to the engine's bounded fixed point, appends the complete reconciliation batch including `PlanReconciled`, advances plan revision and inspection diagnostics, and clears `plan_dirty`. Several facts or command outcomes committed before the claim intentionally coalesce into one evaluation against the complete snapshot. A transaction rollback leaves the dirty bit set, so another compatible replica can retry safely.

Plan code must be CPU-bounded and perform no I/O. The command ceiling bounds monotonic declaration growth; separate plan concurrency protects the connection pool. A missing definition, busy execution lock, or stale probe is skipped without waiting. A deterministic plan defect is recorded and terminalizes the execution in the reconciliation transaction; it cannot roll back application work that committed earlier.

After an empty probe the scheduler waits for a generation-counted plan hint, the fallback poll interval, or shutdown. Hints are optional and carry only that plan work may exist.

## 7. Worker invocation

### 7.1 Preparing work

The claim result contains canonical arguments, accepted policy/timing, command metadata, and active attempt fence. Before user code, the runtime:

1. looks up the frozen worker entry;
2. decodes typed arguments;
3. batch-loads every explicitly declared dependency's definition/state/result or failure;
4. builds `Work[A]` and its staged buffer;
5. constructs a context whose deadline is the earliest accepted per-attempt, retry-budget, and execution deadline;
6. registers the lease and context cancel function with the lease manager.

Decode/type incompatibility for a registered durable version is a permanent safe failure and an operational alert; it is never retried indefinitely as infrastructure noise.

### 7.2 Running

No pool connection or transaction is held while the worker runs. Panic recovery captures a bounded redacted stack fingerprint, not arbitrary arguments or locals. A context uses cancellation causes to distinguish shutdown, lost lease, command/execution cancellation, and timeouts.

The runtime unregisters the active lease only after settlement has either committed, established loss of ownership, or delegated recovery to lease expiry. It does not assume that a locally returned handler still owns the command.

### 7.3 Settling

Success invokes the engine/store settlement from the architecture. The runtime retries only the short database transaction with the already canonical decision buffer. If the registered commit function is present, the store exposes the current `pgx.Tx` through the narrow `flow.Tx` adapter only after all Flow locks/writes are acquired.

On commit-function failure the success transaction rolls back. The runtime classifies the commit-function error and starts a fresh fenced conclusion transaction, which either schedules retry or fails the command. Plan evaluation is not part of either worker transaction. The application function may run again after a deadlock/rollback and must be deterministic/idempotent from its durable inputs.

If commit outcome is ambiguous, the runtime does not blindly run a second success settlement. It queries the stable attempt/command identity: terminal success means done; still-running same fence permits retrying the settlement; changed fence means lost ownership.

External effects performed by the worker before settlement may repeat after crash or lease takeover. Documentation and observations expose `CommandID` as the preferred external idempotency key.

## 8. Lease manager

The lease manager owns only active attempts in this process:

```go
type activeLease struct {
    subject     subjectID
    attemptID   AttemptID
    token       UUID
    expiresAt   time.Time
    cancel      context.CancelCauseFunc
}
```

Renewal is scheduled near one third of lease duration with bounded deterministic per-attempt jitter, grouped into one statement per subject table. It uses PostgreSQL time and extends only rows that remain running with the same token. The returned set is authoritative:

- renewed -> update local accepted expiry;
- missing -> cancel exactly that handler with `ErrLeaseLost` and unregister it;
- transient database error -> retry while conservative local time indicates a safe chance remains; otherwise cancel and let durable expiry recover.

The manager never reacquires, resurrects, or extends an already expired token. Renewals append no journal entry and do not move status, budget, schedule, or attempt-start timestamps.

## 9. Coordinator scheduler

### 9.1 Selecting deliveries

One loop probes active coordinator instances whose exact definition is registered locally. If an instance is idle, the store/engine selects start activation first or the lowest matching retained event above its inbox and persists a stable delivery key. Ordinary event selectors use indexed journal selector columns; `OnOutcome` selectors join a terminal row's `command_id` to the immutable command name/version projection. If ready/retry-wait and due, claim follows the same execution-first skip-locked pattern as commands and appends `AttemptStarted`.

The local selector set includes ordinary `On` event selectors and terminal command selectors for `OnOutcome`. The journal query can jump over unmatched entries. Early events are retained and therefore match after start/instance creation.

### 9.2 Capacity and serialization

Coordinator decisions use a separate configurable semaphore because they should be short and must not be starved by long workers. There is at most one selected/running delivery per instance, while different coordinators and command workers run concurrently.

No database connection is held during the handler. The runtime decodes current state and received payload, invokes the exact frozen handler, and stages changes. A lease manager fences it like a worker.

### 9.3 Settlement and retry

Nil handler return settles state revision, inbox/start acknowledgment, `CoordinatorTransition`, events, commands, and explicit completion atomically. The command ceiling is checked before any output. Handler error discards all mutated state/output and applies coordinator delivery retry policy to the same key; later events cannot overtake it.

Permanent/exhausted error or deterministic output defect appends `CoordinatorFailed`, fails the execution, and cancels work. A process crash after handler code but before commit simply redelivers the same start/event after lease recovery.

## 10. Notifications and polling

### 10.1 Correctness boundary

Every loop can operate with polling only. Notification payloads never carry work, state, payloads, results, errors, positions that act as checkpoints, or ownership. A missed, duplicated, malformed, or reordered notification changes latency only.

### 10.2 Channel and payload

The channel is `flow_` plus a fixed-length lowercase hex digest of normalized schema and database identity. The notifier obtains one session-capable `pgx.Conn`; when the application uses a transaction-pooling proxy it may provide a separate listener connector. Without one, the runtime is intentionally poll-only.

Payloads are small versioned hints:

```json
{"v":1,"kind":"queue","key":"default"}
{"v":1,"kind":"plan","key":"<opaque-plan-pair>"}
{"v":1,"kind":"execution","key":"<opaque-id>"}
```

Queue and execution hints are deduplicated within the semantic transaction, with at most one of each affected key. Unknown payload versions trigger a broad local wake rather than failure.

### 10.3 Listener lifecycle

On initial connect and every reconnect:

1. establish session and `LISTEN`;
2. publish a catch-up generation for every registered queue, plan, and coordinator loop;
3. receive until error/cancellation;
4. close and reconnect with capped jittered backoff.

Catch-up after `LISTEN` closes the connection-gap race. The generation-counted wake hub uses capacity-one channels, coalesces duplicates, and never blocks a transaction observer or listener.

### 10.4 Transactional NOTIFY cost

PostgreSQL serializes portions of transactional notification commit processing. The implementation benchmarks notify-enabled and poll-only throughput before enabling hints by default for production guidance. Because polling is complete, operators can disable notifications without feature loss. A future decoupled best-effort hint flusher is compatible, but is not required in M1.

## 11. Maintenance

Maintenance has no elected leader. Each task performs a bounded stale probe, then enters the ordinary semantic transaction with state/fence revalidation.

| Task | Compatible runtime requirement | Effect |
|---|---|---|
| command lease expiry | none | conclude lost attempt and restore persisted schedule without budget use |
| coordinator lease expiry | exact coordinator version | redeliver same stable delivery |
| `Within` expiry | none | expire command from its persisted deadline, ignoring any fact recorded later; resolve dependencies and mark a plan execution dirty |
| execution deadline | none | expire execution, cancel commands/coordinator, no failure branches |
| unclaimable scan | none | emit safe counts/reasons only |
| consistency audit | none | detect impossible projection/queue/counter shapes; never silently rewrite semantic history |

There is no due-retry or due-delay mover. Time eligibility is a claim predicate. Full maintenance batches self-wake to drain backlog rather than waiting a complete interval.

A consistency audit may repair a purely derived missing wake or delete a provably orphaned nonsemantic queue row only through a named, journal-neutral reconciliation rule. Any repair that changes settled command/execution meaning is an administrative future capability, not automatic maintenance.

## 12. Graceful shutdown

`Stop` and parent context cancellation execute:

1. atomically enter `stopping` and stop new probes/claims;
2. keep notifier and lease renewal alive for in-flight handlers;
3. wait up to the configured grace period;
4. let handlers that return settle normally;
5. cancel remaining handler contexts with shutdown cause;
6. attempt bounded semantic release of their still-owned leases, appending interrupted conclusions without consuming budget;
7. if PostgreSQL is unavailable, stop renewal and rely on lease expiry/takeover;
8. stop maintenance, listener, wake hub, and observers;
9. wait for goroutines and return joined fatal/shutdown errors.

A shutdown release never acknowledges work. `Stop` context expiry may return while database leases remain; fencing and expiry still guarantee recovery. There is no unsafe force-complete API.

## 13. Runtime as Client and transaction composition

### 13.1 Ordinary client

Execution methods, `Issue`, `Publish`, cancellation, and inspection borrow short connections from `pgkit.DB`. They use the same engine/store operations as background runtime paths. They never invoke a worker or coordinator inline.

Bound definitions hold the `Client` interface, not `*Runtime`, so API-only and transaction-scoped use share the same surface.

### 13.2 Transaction-scoped client

`InTx(pgx.Tx)` wraps the supplied transaction:

- begins/commits/rolls back nothing;
- performs no automatic database retry;
- requests Flow execution locks in ascending `ExecutionID` order and rejects a reverse request before SQL;
- requires Flow operations before application locks/writes;
- maps `pgx.ErrTxClosed` to `ErrClosed`;
- emits no commit-dependent observation because pgx exposes no general post-commit hook.

The narrow `flow.Tx` passed to commit functions is a different sealed capability: it permits application SQL but not Flow operations or transaction control.

### 13.3 Plan-independent ingress

Ingress locks the execution, appends its durable mutation, applies generic persisted dependency/`Await` resolution, and marks the execution dirty when the mutation is a plan-visible fact or terminal command outcome. It never resolves or calls Go plan code and therefore never fails because a plan definition is absent. `Issue` creates a command but is not itself a plan input.

The dirty marker and transition commit together. A later plan reconciliation either observes that transition or a still-later snapshot containing it, so monitors need only the event definition and client. This is the same rule for ordinary and transaction-scoped clients.

## 14. Migrations and startup compatibility

Public entry points delegate to the store migration component:

```go
func Migrate(context.Context, *pgkit.DB, ...MigrateOption) error
func CheckSchema(context.Context, *pgkit.DB, ...MigrateOption) (SchemaStatus, error)
func MigrationFS(...MigrateOption) (fs.FS, error)
```

`New` calls `CheckSchema`; missing/incompatible schema is `ErrSchema`. It does not opportunistically migrate, even in development. Migration roles and runtime roles may be different database users; runtime SQL requires only DML/sequence-free permissions on the `flow_` tables in the configured schema and application permissions used by declared commit functions. The default is the application's `public` schema; choosing another schema changes only qualification, never the fixed table prefix.

## 15. Inspection operations

- `GetExecution` and `LookupExecution` return bounded current summaries. `LookupExecution(typ, key)` treats `typ` as the receiver definition name and searches all driver modes; if the same non-empty key/name exists in more than one mode it returns `ErrConflict` rather than choosing one silently. Stable `ExecutionID` remains unambiguous.
- `Trace` uses the store's repeatable-read fixed query set and engine projection types.
- `History(after, limit)` pages immutable journal entries by execution-local position.
- `ListExecutions` uses stable cursor pagination and bounded indexed filters.
- `AwaitExecution` repeatedly reads terminal state, optionally listens for execution hints, and always polls; it consumes no worker slot or durable lease.
- `ResultOf(ExecutionTrace, ...)` performs typed decode against the trace and definition pair without another query.

Trace labels delayed work as derived `scheduled`, exposes accepted/current command count, plan dirty/revision/quiescence and bounded waiting diagnostics, missing registrant reasons, current lease age without token, dependency and wait reasons, child closure, invocation ordinal and consumed-attempt count, attempts reconstructed from journal history plus current delivery state, coordinator inbox, and causation graph. It never invents a `CommandStarted` event.

## 16. Observations

The core interface is intentionally small:

```go
type Observer interface {
    Observe(context.Context, Observation)
}
```

`Observation` is one tagged struct with safe identifiers, timings, counters, and outcome categories. Transaction paths buffer observations and release them only after known commit/rollback. Observer calls run in a bounded asynchronous adapter outside locks; panic is recovered and queue overflow drops observations with one aggregate diagnostic rather than blocking execution.

Required measurements include:

- start/idempotent/conflict and execution outcomes;
- command creation, schedule kind, command-limit use/rejection;
- probes, claims, empty claims, free capacity, handler/commit duration;
- attempt classification, invocation and consumed-attempt counts, persisted retry time, elapsed/attempt budget use;
- lease renewal/loss/recovery and long-running attempts;
- dirty-plan probes, coalesced trigger count, evaluation/fixed-point passes, loaded values, nodes, waiting count, declaration delta, and duration;
- dependency resolution, wait start/expiry, skip cascade, failure closure;
- coordinator delivery lag, retry, state size, inbox position, outcome fan-in;
- notification/poll wake source, reconnect, and latency;
- unclaimable backlog by safe definition/plan/coordinator reason;
- execution-lock wait and journal batch size.

Payloads, results, coordinator state, raw errors, SQL, connection data, and lease tokens are forbidden dimensions.

Attempt-conclusion observations already contain enough information for adapters to count consecutive interruptions and compute interruption-to-consumed-attempt ratios by command name/version. That derived alert belongs in telemetry adapters; the runtime does not emit another event or recommend command-ID metric labels merely because non-consuming interruptions repeat.

## 17. Error and panic handling

Runtime database errors delegate to the schema component's SQLSTATE/constraint mapper. Handler errors delegate to engine classification. Fatal process errors are limited to corrupt registry/internal invariants, incompatible schema discovered after startup, and unrecoverable component startup; ordinary database outages cause backoff and continued polling until context cancellation.

Backoff uses capped full jitter and resets after success. Loops log/observe aggregate repeated errors rather than one record per failed poll. Panics in user worker/coordinator code become attempt outcomes; panics in observer adapters are isolated; panics in internal invariant code stop `Run` after best-effort graceful release because continuing may corrupt meaning.

## 18. `flowtest` package integration

`flowtest` wraps the same definition codecs and engine used in production. It supplies constructors for synthetic `CommandInfo`, PostgreSQL time, facts, outcomes, dependencies, children, command ceilings, and coordinator deliveries. It never imports `internal/store` or requires PostgreSQL.

Worker results expose returned result/error, staged events, child declarations, Optional/StartAfter, scope defects, and dependency reads. Commit tests receive a transaction double. Plan simulation exposes declarations/reads after every synthetic transition. Coordinator simulation exposes state revisions, delayed spawns, mixed outcome deliveries, inbox advance, and terminal decisions. Direct simulation recursively applies successful staged child decisions.

SQL behavior, lock/fence guarantees, actual canonical `bytea` persistence, and application commit SQL remain integration tests against real PostgreSQL.

## 19. Fault injection

Test-only named fault points surround:

```text
probe_return
claim_execution_lock
claim_before_journal
claim_before_commit
handler_start
handler_return
settle_after_fence
settle_after_attempt
settle_after_events
settle_after_children
settle_before_commit_function
settle_after_commit_function
settle_before_commit
settle_commit_ambiguous
coordinator_after_handler
coordinator_before_inbox_advance
plan_after_claim
plan_after_evaluate
plan_before_commit
renew_before_result
maintenance_after_probe
notify_connect
notify_before_wait
migration_each_unit
```

Fault tests assert durable invariants after runtime restart, not exact goroutine schedules. Hooks are absent from release builds unless an internal test build tag is enabled.

## 20. Test plan

### 20.1 Lifecycle and registry

- New starts no goroutines and rejects invalid/incompatible configuration;
- Register/Run/Stop state transitions and concurrent Stop calls;
- duplicate worker/plan/coordinator and subscription conflicts;
- API-only operations without Run;
- plan-independent worker settlement and publication, dirty-plan takeover, and delayed deployment of the exact `PlanDef`;
- rolling replicas with divergent command, plan, coordinator, and event versions;
- event-version rollouts retain exact-version compatibility for in-flight waits and reject a different version under an accepted natural key;

### 20.2 Queue and leases

- no claim beyond immediately free capacity;
- no database connection held across handler execution;
- queue fairness and no unhandled head-of-lane starvation;
- same-execution multi-claim serializes only claim commits;
- renewals batch and one missing lease cancels only its handler;
- lease loss, cancellation, shutdown, and ambiguous settlement races;
- repeated non-consuming interruption remains bounded by elapsed/execution deadlines and is diagnosable from attempt observations and Trace counters;
- handler deadline precedence and database revalidation.

### 20.3 Plan, coordinator, and agents

- concurrent plan triggers coalesce without losing declarations;
- dirty plan claims use skip-locked execution rows, roll back safely, and are recovered by another compatible replica;
- a plan defect cannot roll back an already committed worker result, event, or commit-function write;

- start activation, early event, sparse match scan, strict matching-position order;
- failure redelivery with unchanged state/inbox;
- successful/failed/cancelled/expired/skipped `OnOutcome` delivery exactly once;
- optional tool fan-out, durable wait for user input, delayed next turn, crash during tool and between turns;
- command ceiling fails coordinator decision atomically.

### 20.4 Polling, notification, maintenance, shutdown

- poll-only processes all work;
- notification after commit and none after rollback;
- catch-up closes startup/reconnect gap;
- generation hub closes check/sleep race;
- duplicate maintenance runners and crashes are safe;
- graceful release consumes no retry budget; database-outage shutdown recovers by expiry;
- no goroutine, timer, slot, or connection leak under `go test -race`.

### 20.5 Transactions, errors, and safety

- `InTx` commits/rolls back with application writes, performs Flow first, marks plan work dirty atomically, and inverse order receives a database error without hidden retry;
- transaction-bound definition use after close returns `ErrClosed`;
- commit-dependent observations are suppressed for bare caller transactions;
- every constraint maps by name/SQLSTATE;
- ambiguous commit is re-derived by identity;
- no error/observation contains payload, token, SQL, or secret.

### 20.6 Benchmarks

- end-to-end claim/execute/settle throughput by queue and replica count;
- execution-row contention for wide fan-out completion;
- registry filters with rolling-deploy distributions;
- dirty-plan probe and coalescing throughput with many plans and replicas;
- 500 active lease renewals;
- coordinator high-rate delivery and sparse matching;
- poll latency/load at multiple intervals;
- transactional notification versus poll-only commit throughput;
- shutdown at 10, 100, and 1,000 active handlers.

## 21. Acceptance conditions

- Runtime correctness and recovery work with notifications disabled and every prior process gone;
- registration is frozen for `Run`, and claims cannot drift from executable local definitions;
- worker settlement and event publication never require plan code; dirty plan work remains durable until an exact-plan scheduler reconciles it;
- claimed handlers start only after `AttemptStarted` commits and never hold a database resource while running;
- immediately free capacity bounds claims, and stale leases cannot settle;
- coordinator state/inbox/output is serial, fenced, and redeliverable;
- shutdown never acknowledges unfinished work and interruption consumes no retry budget;
- caller-owned transactions preserve Flow-before-application order and receive no false commit observation;
- poll, notification, maintenance, inspection, migration, error, fault, race, and benchmark suites pass.
