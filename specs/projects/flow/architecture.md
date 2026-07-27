---
status: draft
---

# Architecture: flow

## 1. Purpose and authority

This document turns the approved project overview and functional specification into an implementation architecture for `github.com/goware/flow`. It owns the decisions that cross storage, the deterministic execution engine, and the distributed runtime: durable identity, journal ordering, transactions, locks, command delivery, plan and coordinator activation, failure, and recovery.

The component documents contain the implementation-level detail:

1. [`components/schema.md`](components/schema.md) — PostgreSQL tables, constraints, indexes, statement contracts, migrations, and database tests.
2. [`components/engine.md`](components/engine.md) — typed definitions, staged decisions, plan reconciliation, dependencies, coordinator decisions, retry policy, and completion.
3. [`components/runtime.md`](components/runtime.md) — registration, claiming, handler invocation, leases, polling and notifications, shutdown, inspection, and `flowtest`.

Those three components are intentional. PostgreSQL is the durable mechanism, the engine is the deterministic state-transition core, and the runtime is disposable process machinery. Adding more public concepts or backend abstractions is not part of Milestone 1. No UI architecture is included.

## 2. Architectural thesis

`flow` is a PostgreSQL-backed, event-driven durable execution library:

```text
command -> worker -> event
                 \-> optional child commands

event or terminal command outcome -> optional plan/coordinator -> more commands
```

Every execution has two related representations:

- an immutable, gap-free, execution-local **journal**, which is the complete accepted history of orchestration decisions and lifecycle transitions; and
- indexed mutable **materializations**, which make claiming, dependency checks, retry scheduling, and current-state inspection efficient.

The journal is authoritative for causal history and settled orchestration projections. Materializations are authoritative for current delivery ownership. Application tables remain authoritative for business data. Rebuilding Flow projections never replays user handlers, commit functions, or external effects.

The architecture deliberately does not introduce a global event stream. Each execution owns one total order. Future cross-execution behavior must cross an explicit idempotent export or execution-start boundary and retains the source execution and position; it never merges journals or promises global order.

## 3. Load-bearing invariants

The implementation must preserve these invariants even if physical tables or internal packages later change.

1. **PostgreSQL is the only correctness authority.** Polling alone recovers all progress; notifications and memory only reduce latency.
2. **One orchestration authority per execution.** The driver is exactly one of direct root command, pure plan, or durable coordinator.
3. **One execution row serializes semantic commits.** Every transaction that changes settled execution meaning or appends journal history locks that execution row first.
4. **Journal order is gap-free and commit-ordered within one execution.** Positions are allocated under the execution lock and rolled back with the transaction.
5. **Every accepted command has exactly one `CommandCreated` entry and at most one materialized row.** Every terminal command has exactly one terminal event.
6. **Attempt mechanics are history, not application facts.** Starts and conclusions are journaled; leases and renewals are not `Event[T]` and cannot drive plans.
7. **Handlers execute at least once, progression commits once.** A current lease token fences every settlement; external effects still require application idempotency or reconciliation.
8. **Handler decisions are staged.** An error, panic, cancellation, lost lease, output conflict, or rolled-back transaction exposes none of the staged events or children.
9. **A successful worker closes direct-child membership atomically.** No child may appear later under that parent.
10. **Plans only grow.** A plan may discover new declarations but cannot rewrite or withdraw accepted work.
11. **Coordinator decisions are serialized by journal position.** State, inbox advance, outputs, and the transition record commit together.
12. **The accepted command ceiling belongs to the execution.** Every replica checks the stored value and increments the stored count under the execution lock.
13. **Durable time is PostgreSQL time.** Creation, first eligibility, retries, waits, deadlines, claims, and leases never depend on an application clock.
14. **Flow locks precede application locks.** Library-owned settlement acquires Flow state before invoking a commit function; caller-owned transactions call Flow before touching application rows.
15. **Plan triggers are durable work.** Execution start, application events, and terminal command outcomes atomically set `plan_dirty`; only a successful exact-version reconciliation or terminal plan defect clears ordinary dirty work, and derived success/failure is forbidden while it remains set. Explicit execution cancellation and deadline expiry may instead clear it while terminalizing the execution.

## 4. Technology and dependency decisions

| Area | Decision | Reason |
|---|---|---|
| Database | PostgreSQL 15+ | Required backend; row locks, `SKIP LOCKED`, transactional DDL, and optional `LISTEN`/`NOTIFY` are sufficient. |
| Database access | Application-owned `pgkit/v2` handle and `pgx/v5` transactions | The application and Flow share one database adapter and pool; caller-owned `pgx.Tx` composition remains possible. |
| IDs | UUIDv7 generated in Go, stored as `uuid` | Distributed creation with index locality. Public IDs remain opaque typed strings. |
| Encoding | RFC 8785 canonical JSON bytes plus SHA-256 | Stable durable identity across replicas and Go processes. |
| Isolation | `READ COMMITTED` plus explicit locks, predicates, and constraints | The invariants require targeted serialization, not transaction-wide serializable isolation. |
| Wake-up | Polling for correctness; optional `LISTEN`/`NOTIFY` hints | Works behind transaction-pooling proxies and survives missed hints. |
| Tables | Fixed `flow_` prefix in a configurable schema, default `public`; all SQL qualified | Flow coexists visibly with application tables without requiring a dedicated database or schema and never depends on `search_path`. |
| Migrations | Embedded, explicit, checksummed, advisory-locked | `New` never mutates schema; applications retain deployment control. |
| Observability | Small no-op observer contract in core | OpenTelemetry, metrics, and logging adapters remain optional packages. |

Initial direct dependencies are `github.com/goware/pgkit/v2`, `github.com/jackc/pgx/v5`, and `github.com/google/uuid`; canonical JSON is implemented in `internal/canonical` against RFC 8785 test vectors rather than exposing another library's types. There is no ORM, broker client, distributed-lock service, or mandatory telemetry SDK. New dependencies require a concrete reduction in implementation or verification risk.

`New` and `Migrate` receive the application's existing `*pgkit.DB`; Flow does not create or own a second pool. All ordinary runtime, application, and commit-function work can therefore use one PostgreSQL adapter. An optional dedicated session connection for `LISTEN` may be borrowed from that pool or supplied through a listener connector when required by a proxy, but it is only a wake-up optimization and not another durable backend.

There is one public package plus `flowtest`; internal packages exist for dependency direction, not as user-facing layers.

## 5. System and package structure

```text
API-only process             mixed/worker process             monitor/webhook
Runtime as Client            Runtime.Run                       Runtime.InTx(tx)
       |                           |                                  |
       +---------------------------+----------------------------------+
                                   |
                     github.com/goware/flow
              definitions | client | inspection
                         internal engine
                    internal PostgreSQL store
                                   |
                              PostgreSQL
                 journal + materializations + leases
```

Suggested package layout:

```text
github.com/goware/flow
├── runtime.go          Runtime, Client, New, Run, Stop, InTx
├── definitions.go      Command, Event, PlanDef, Coordinator, With
├── worker.go           Handle, Work, Commit, Emit, Spawn
├── plan.go             Plan, Do, reads, Node builders
├── coordinator.go      handlers, Coordination, On*, completion decisions
├── execute.go          Execute, Issue, Publish, cancellation
├── retry.go            immutable RetryPolicy and error classification
├── inspect.go          Get, Lookup, Trace, History, List, Await
├── errors.go           sentinels and safe structured errors
├── migrate.go          migration entry points
├── flowtest/           database-free application test harness
└── internal/
    ├── canonical/      canonical encode/decode and fingerprints
    ├── definition/     erased descriptors and registration validation
    ├── engine/         snapshots, decisions, reconciliation, completion
    ├── store/          all SQL and row codecs
    ├── runtime/        dispatchers, leases, coordinator loop, shutdown
    ├── notify/         optional listener and coalescing wake hub
    └── observe/        post-commit observation buffering
```

The root package owns generic type safety and delegates through erased internal descriptors containing codecs and immutable definition metadata. `internal/store` is the only package allowed to issue Flow SQL. `internal/engine` has no pool, goroutine, clock, or SQL dependency; it consumes snapshots and returns validated decisions.

## 6. Durable model

The physical schema is specified in `components/schema.md`. The logical records are:

| Record | Purpose |
|---|---|
| execution | Driver identity, immutable start input, lifecycle, deadline, accepted command ceiling/count, open count, and journal allocator. |
| command | One logical request, accepted configuration, topology, state/result projection, immutable budget anchor, and child-membership status. |
| command queue | Narrow hot row for lane, kind, next-run time, and current lease; absent for pending and terminal commands. |
| dependency group/member | Normalized `After*` and `AfterAny` conditions with reverse lookup by predecessor. |
| event wait | Normalized `Await` membership and the satisfying event position. |
| attempt history | `AttemptStarted`/`AttemptConcluded` journal records for one command or coordinator invocation; current ownership remains on the command-queue/coordinator row rather than in another projection. |
| journal entry | Immutable ordered history, including the full logical payload needed by `History` and graph reconstruction. |
| coordinator instance | Current typed state projection, start activation, inbox, current delivery, retry state, and lease. |
| migration metadata | Applied unit, checksum, writer version, and compatibility range. |

Command and execution payloads are stored in both their current projection and the corresponding journal record in the initial implementation. This is intentional simplicity: the journal can stand alone, while hot operations avoid decoding history. A later physical optimization may content-address or normalize immutable bytes as long as `History` and replay observe the same logical records.

The command count is not computed by scanning. `flow_executions.command_count` increments once for each genuinely new root, `Do`, `Spawn`, or `Issue` command and is checked with `max_commands` while the row is locked. Attempts, retries, duplicate reconciliation, events, coordinator activations, and journal rows do not change it.

## 7. Journal model and ordering proof

### 7.1 Entry classes

The journal uses one table and a discriminated body. Milestone 1 entry classes are:

- `ExecutionStarted`;
- `CommandCreated`;
- `AttemptStarted` and `AttemptConcluded` for command and coordinator invocations;
- `PlanReconciled` for each successfully committed dirty-plan decision;
- `EventRecorded` for application events, command terminal events, plan/coordinator failures, and execution terminal events;
- `ExecutionBecameFailing` for the one non-terminal execution lifecycle transition;
- `CoordinatorTransition`.

There is no `CommandStarted` application event and no lease-renewal entry. A command's final transition is represented once by its terminal `EventRecorded`; an execution's final transition is likewise represented by its terminal event, not by a duplicate state-change row. Entering `failing` is not terminal, so its one internal lifecycle entry is required to keep replay complete.

Every entry carries `ExecutionID`, `JournalEntryID`, position, PostgreSQL recorded time, kind, and causation. Typed event entries additionally carry `EventID`, event name/version/key, event class, canonical payload, and origin identifiers. Operational entries remain visible through `History` but cannot be passed to `Fact`, `Await`, or `On`.

### 7.2 Position allocation

Every semantic transaction does this before appending:

```sql
SELECT ... FROM public.flow_executions WHERE execution_id = $1 FOR UPDATE;
```

Once its complete output is known, it increments `next_journal_position` by the exact batch size and assigns consecutive positions. The counter update and inserts are in the same transaction.

The proof is direct:

1. Only one transaction can hold an execution row lock.
2. A later appender cannot allocate until the earlier transaction commits or rolls back.
3. Rollback restores the counter and removes every inserted entry.
4. Therefore committed positions are gap-free and allocation order equals commit order.
5. A reader that has consumed position `n` can never later see a newly committed position below `n`.

No visibility watermark, singleton database allocator, or cross-execution checkpoint protocol is required.

### 7.3 Deterministic order inside one commit

The engine emits a stable batch order so traces do not depend on map iteration:

1. attempt conclusion, coordinator transition, or `PlanReconciled` decision record;
2. application events in handler call order;
3. spawned `CommandCreated` entries in command-key order;
4. the current command's terminal event;
5. plan-created `CommandCreated` entries in command-key order;
6. dependency-derived terminal events in stable graph/key order;
7. `ExecutionBecameFailing` and fail-fast cancellation events when applicable;
8. coordinator or execution terminal events.

Some transactions omit steps. A `PlanReconciled` record is caused by `ExecutionStarted` or the latest journal position included in the coalesced dirty snapshot; its body records the full consumed-through position and plan revision, and plan-created commands are caused by that decision record. Worker outputs are caused by the attempt conclusion; coordinator outputs are caused by the handled activation or event and recorded transition. Exact positions are assigned only after the full batch validates.

## 8. Transaction and lock discipline

### 8.1 Semantic transaction kinds

| Transaction | Main work |
|---|---|
| start | Insert execution and `ExecutionStarted`; add the direct root, mark a plan execution dirty, or create the coordinator activation. |
| ingress | `Issue`, `Publish`, or cancellation against one execution. |
| claim | Create attempt, lease a command/coordinator delivery, append `AttemptStarted`. |
| worker settle | Fence attempt; record result/error and staged events/children; resolve dependencies; run commit function; mark plan dirty; finish only when eligible. |
| plan reconcile | Claim one dirty execution by exact plan version; evaluate/reconcile a bounded fixed point; create declarations; clear dirty or record terminal plan defect. |
| coordinator settle | Fence delivery; commit state/inbox/outputs; finish or retry/fail coordinator. |
| maintenance settle | Recover lease, expire wait/command/execution, and run ordinary dependency/completion logic. |
| renewal | Extend active leases only; no execution lock or journal entry. |

### 8.2 Blocking lock order

Every library-owned semantic transaction concerns one execution and acquires blocking locks in this order:

1. execution row;
2. coordinator instance, when applicable;
3. existing command rows in ascending `CommandID`;
4. queue, dependency, and wait rows belonging to those commands;
5. journal inserts and other new rows in deterministic key order;
6. application rows through the declared commit function;
7. optional `pg_notify` hint;
8. commit.

New command rows are inserted by command key after conflicting existing rows have been locked and validated. The engine never holds an application lock and then returns to Flow-owned rows.

For a blocking semantic transaction, the store captures its single PostgreSQL decision time immediately after acquiring the execution row lock, never before a potentially long lock wait. Claim uses the same rule after its successful no-wait execution lock. This timestamp drives every deadline, schedule, and journal time in that transition.

### 8.3 Claim is execution-first and no-wait

Claims must append `AttemptStarted`, so they also need the execution lock. Candidate discovery is an unlocked, indexed probe. For each candidate execution the claimer opens a short transaction and uses `FOR UPDATE SKIP LOCKED` first on the execution row, then on the revalidated command/queue or coordinator row. If either is unavailable or no longer eligible, the candidate is abandoned immediately.

The claim path acquires only skip-locked rows and never waits. It is therefore exempt from assumptions about blocking lock waits, while still following the execution-first category order. Claim commits for one execution serialize briefly; the claimed handlers then run concurrently without locks or database connections.

### 8.4 Caller-owned transactions

`Runtime.InTx(tx)` never begins, commits, rolls back, or retries the caller's transaction. The required cross-boundary order is:

```text
Flow operation -> application-table locks/writes -> caller commit
```

This matches worker settlement, where Flow locks are acquired before `WithCommit` runs. A transaction must not call Flow after it has acquired application locks, and it must not interleave Flow and application lock phases. The transaction-scoped client tracks execution locks it requests and rejects a later lower `ExecutionID`, so multiple Flow operations use one deterministic order before the application phase. Arbitrary application locks cannot be detected; PostgreSQL may abort a transaction that violates the documented order, and the library returns that error without replaying caller code.

Each public operation targets one execution. An application that composes several existing executions in one caller-owned transaction sorts those operations by lexical `ExecutionID`, invokes every Flow operation first, and then performs its application writes. The existing transaction-scoped client enforces the ordering; M1 adds no batch or ordered-helper API.

`InTx` never invokes plan code. It can stage a plan-visible fact or terminal outcome together with the dirty marker, and both remain subject to the caller's commit or rollback. Any resulting plan defect is discovered later by the library-owned reconciler and cannot retroactively change the caller transaction's outcome. The library never commits behind the caller's transaction or reports a rolled-back trigger as durable.

### 8.5 Isolation and retry

The store uses `READ COMMITTED`. Library-owned transactions may retry SQLSTATE `40001`, `40P01`, and safe pre-commit connection failures with capped full-jitter backoff. A worker or coordinator handler is never re-invoked merely because its short settlement transaction retries; the already canonicalized decision buffer is reused. Caller-owned transactions return the error to their owner.

An ambiguous commit is never reported as a definite rollback. Stable execution, command, event, and inbox identities let the next call or recovery loop discover whether it committed.

## 9. Public definition and registration architecture

`Command[A,R]`, `Event[T]`, `PlanDef[A]`, and `Coordinator[S]` are immutable value descriptors. Their durable identity excludes a private `Client` binding. `.With(client)` copies the descriptor and replaces only that binding, preserving the same static type. `Handle`, `PlanDef`, and `Coordinator` implement the sealed registration capability; `Event` and a bare `Command` do not.

Each typed descriptor contains an internal erased descriptor with:

- stable name and positive version;
- canonical encode/decode functions and a runtime Go type fingerprint used for registration diagnostics;
- immutable definition options;
- for a command, payload/result codecs and its derived success-event descriptor;
- for a plan or coordinator, input/state codec and function/handler metadata.

Definitions do not mutate global state. `Registration` values populate a runtime-local registry, frozen by `Run`. Registration rejects duplicate workers, conflicting codecs for a durable pair, duplicate coordinator handlers, multiple commit functions, and overlapping success-only `On(cmd.Done())` with `OnOutcome(cmd)`.

The registry is also the rolling-deployment capability map. A dispatcher claims only exact command or coordinator definition versions that process can execute. A registered command pair is executable on any stored queue; `WithQueue` is a creation-time scheduling default, not a second handler identity, so changing it cannot strand existing work on its previously accepted queue. Unknown work remains durable and consumes no retry budget.

## 10. Command creation and identity

All creation paths call one store/engine operation. A proposed command has two fingerprints:

- **declaration identity**: name/version, canonical arguments, origin/parent, required classification, normalized dependencies and waits, explicit `Delay`, `Within`, `StartAfter`, and explicit plan-node retry override;
- **accepted operation settings**: resolved queue, per-attempt timeout, and effective declarative retry policy copied at creation.

Plan reconciliation and idempotent rediscovery compare declaration identity. They do not compare the command definition's current operational defaults, so a deployment may tune defaults without rewriting or conflicting with existing work. Duplicate declarations inside one plan evaluation must nevertheless resolve to identical effective settings. Explicit plan-node retry overrides are declaration identity and may not change for an existing key.

Creation while holding the execution lock proceeds as one batch:

1. canonicalize and validate every proposal before writing;
2. coalesce equivalent repeated keys and reject conflicts;
3. resolve references against durable commands plus the complete proposed set;
4. compute genuinely new count and enforce `max_commands` against `flow_executions.command_count`;
5. insert commands, dependencies, waits, and any ready command-queue rows in key order;
6. append one complete `CommandCreated` record per inserted command;
7. increment `command_count` and `open_commands` by the inserted count.

No member of a fan-out or plan evaluation appears if the batch fails. The behavior on a deterministic ceiling violation depends on its source exactly as the functional specification requires: public `Issue` writes nothing and returns `ErrInvalid`; a worker records a permanent command failure; a coordinator records coordinator failure; a plan records `PlanFailed`.

## 11. Command delivery, attempts, and retry

### 11.1 Eligibility

Only commands with satisfied dependencies and waits have a command-queue row. `ready` and `retry_wait` queue rows carry `next_run_at`; no separate scheduled state exists. The claim probe filters by queue lane, exact registered name/version, and PostgreSQL `next_run_at <= now`.

The first eligibility transition sets `BudgetStartedAt` exactly once:

- an immediate command uses the transition's PostgreSQL time;
- `Delay` starts after dependencies and waits resolve, and the anchor is the delayed eligible time;
- `StartAfter` uses the accepting transaction's time plus the staged duration;
- a retry changes only `next_run_at`, never the anchor.

### 11.2 Claim

After the no-wait locks in §8.3, claim revalidates execution state/deadline, command state, definition pair, schedule, and remaining elapsed retry budget using one captured `clock_timestamp()`. It increments the invocation ordinal, stores a fresh random attempt ID, lease token, owner, and start time on the current command-queue/coordinator row, changes the delivery to `running`, and appends `AttemptStarted` atomically. No separate attempt projection is written.

`CommandInfo` is built from these accepted database values. The effective handler context deadline is the earliest of per-attempt timeout, retry elapsed deadline, and execution deadline. Local timers only cancel the goroutine; the settlement transaction rechecks PostgreSQL time.

### 11.3 Conclusion and policy

The runtime first classifies the conclusion as success, retryable error, explicit `RetryAfter`, permanent error, panic, attempt timeout, shutdown interruption, cancellation, or lease loss. The immutable retry policy then decides whether another attempt is allowed. Permanent errors cannot be made retryable. Shutdown interruption and lease loss do not consume retry budget.

The selected retry time is computed once from database time, persisted in the command/queue projection, and written to `AttemptConcluded`. Restarts never recompute jitter. Retry exhaustion records the terminal `CommandFailed` event in the same semantic transition that resolves dependencies.

### 11.4 Fencing

Every worker and coordinator settlement checks all of:

- execution is in a compatible non-terminal state;
- subject is still `running` on the same attempt;
- lease token matches;
- lease has not expired at PostgreSQL time.

Zero affected rows means no staged output may commit. A diagnostic lookup maps it to `ErrLeaseLost`, `ErrTerminal`, or `ErrNotFound`. Renewals only extend the lease row under the same token; they never change journal, command state, or retry time.

## 12. Worker decision and settlement

`Work[A]` is an in-memory scope containing decoded immutable arguments, `CommandInfo`, preloaded dependency outcomes, and a staged decision buffer. `Emit` and `Spawn` validate and canonicalize immediately but perform no SQL. `ResultOf` and `OutcomeOf` read only terminal commands named by durable dependency edges and use a batch loaded before invocation.

On `(result, nil)`, the runtime canonicalizes the result and enters one settlement transaction:

1. acquire the execution and command/queue fence;
2. validate the complete decision buffer, child-key equivalence, payload limits, and command ceiling;
3. append `AttemptConcluded` and materialize the successful parent result;
4. append emitted events in call order;
5. insert and journal the complete child set, closing parent membership;
6. append the parent success event after its additional events;
7. resolve dependencies and awaited facts;
8. set `plan_dirty` when this plan-driven transition appended an application or terminal command event;
9. apply fail-fast and completion logic, with dirty plan work preventing terminal completion;
10. write all Flow materializations and journal records;
11. invoke the statically registered commit function, if any, using only durable `Args`, `Result`, `Info`, and a narrow transaction capability;
12. emit at most one wake hint per affected queue and commit.

The Flow writes in step 10 occur before application writes in step 11, but they remain invisible until the shared transaction commits. If the commit function returns an error, the transaction rolls back. The error is then settled through the ordinary retry/permanent path; no success event, child, emitted event, dirty marker, or application write remains.

A deterministic staged-output defect takes a different path: the proposed success is rejected, its outputs and commit function are discarded, and the command becomes permanently failed without rerunning code that cannot repair the decision.

No plan code runs during worker settlement. A successful worker and commit-function write remain committed even if a later plan reconciliation detects a plan defect; that separate reconciliation transaction records `PlanFailed`, cancels outstanding work, and fails the execution without rerunning the worker.

## 13. Plans

### 13.1 Durable trigger and claim

Plan execution start, every application `EventRecorded`, and every terminal command event set `flow_executions.plan_dirty = true` while holding the execution lock. The clean-to-dirty transition also stores PostgreSQL `plan_dirty_since`; later coalesced triggers do not move it. This keeps a continuously active execution from being postponed forever without producing one delivery row per input. Dependency and persisted-`Await` resolution remains engine-owned and happens in the triggering transaction; only the Go plan is deferred.

Each runtime builds an exact registered plan-pair filter. A bounded plan scheduler probes dirty non-terminal executions by `(definition_name, definition_version)`, reserves local capacity, and acquires the execution row with `FOR UPDATE SKIP LOCKED`. It then captures PostgreSQL time and loads one transaction-consistent snapshot. No lease is needed because the pure plan runs inside that short transaction: a crash releases the row lock and rolls back, leaving `plan_dirty = true` for another replica. Polling alone recovers work; notification hints only reduce latency.

This boundary removes plan-code coupling from publishers and command workers. They commit facts and outcomes plus the dirty marker without possessing the plan. If no compatible plan runtime exists, existing commands may continue, derived success/failure waits, and inspection reports `missing_plan_definition` until a compatible replica appears. Explicit cancellation and the execution deadline remain available terminal overrides.

### 13.2 Snapshot and purity

The snapshot contains root arguments, every command's immutable identity/final state, closed child memberships, normalized dependencies/waits, and a fixed journal high-water position. It initially contains no whole-journal event scan. A provisional pure pass discovers `Fact`/`Facts` selectors, the store batch-loads only those indexed journal slices through the high-water position, and another pass continues against the enlarged in-memory snapshot; large bodies are loaded by locator the same way. Empty selector results are cached for that reconciliation. It contains no persisted consulted-input set. The plan receives no context, database, client, clock, transaction, randomness, or worker scope.

The engine catches panics and records declarations and in-memory read availability. `flowtest` and optional debug mode evaluate identical fully loaded snapshots twice and compare canonical declarations, normalized topology, effective settings, reads, and availability classifications. Persisted history remains the source for simulation; routing correctness no longer depends on tracking what a prior pass happened to read.

### 13.3 Reconciliation and completion state

After the function returns, the engine:

1. validates every read and definition match;
2. validates forward dependency references against durable plus newly declared keys;
3. normalizes clauses and detects cycles in the plan-owned dependency graph;
4. coalesces equivalent duplicate declarations and rejects disagreement;
5. compares existing plan-owned keys by declaration identity;
6. retains previously declared keys absent from this evaluation;
7. classifies new nodes as ready, pending, or immediately skipped;
8. applies the batch command-ceiling check and emits a delta.

Reconciliation runs to a bounded fixed point inside the transaction only when applying the declaration delta creates an immediate terminal transition, such as a newly declared command that is already skipped or expired. The pure plan is then evaluated against that in-transaction outcome. Creating ordinary ready or pending work stops the cycle with `plan_quiescent = false`; its eventual terminal event will dirty the execution again. A no-new-command pass sets `plan_quiescent = true`.

Terminal transitions created by the reconciliation itself are inputs to that fixed point, not new post-commit dirty work. The final clear therefore means the plan consumed both the prior committed prefix and every immediate transition in its own batch. A terminal outcome committed later by a worker or maintenance begins a new dirty cycle normally.

Every additional fixed-point pass follows at least one genuinely new terminal command, so the stored command ceiling bounds that cycle when enabled. A fixed technical guard also bounds total invocations in one transaction, including lazy selector- and value-loading passes. Crossing it is a deterministic plan defect.

On success, the transaction inserts and journals the complete declaration batch, increments `plan_revision`, stores `plan_quiescent`, stores the temporary-read count and at most 32 inspection-only `plan_waiting_on` summaries, and clears `plan_dirty`. The total count remains exact even when the summary is truncated. Because the execution row remains locked, no event or outcome can race between snapshot and clear. Available and permanent reads do not block success; temporary reads do. The full read set is not persisted and never controls scheduling.

A deterministic plan defect records `PlanFailed`, cancels outstanding work, terminalizes the execution as failed, and clears dirty state in the same library-owned transaction. It cannot roll back an earlier worker settlement because plan reconciliation is a separate commit.

## 14. Dependency and wait resolution

Each builder call creates a normalized condition group. Groups combine with logical AND:

| Group | Satisfied | Permanently unsatisfiable |
|---|---|---|
| `After` | all members succeeded | any member terminal unsuccessful |
| `AfterSettled` | all members terminal | never |
| `AfterFailed` | all members terminal unsuccessful | any member succeeded |
| `AfterAny(n)` | at least `n` succeeded | successes plus non-terminal members are fewer than `n` |
| `Await` | every named event exists | never before its deadline/execution expiry |

Terminal command and new event positions seed an in-memory resolution work queue. Reverse indexes locate affected groups; each group transition is guarded so it resolves once. Newly ready commands receive command-queue rows. Permanently impossible nodes become `skipped`, append exactly one terminal event, decrement `open_commands`, and seed further resolution. Stable key ordering makes cascading output deterministic.

Dependency and wait resolution uses stored topology and does not invoke application plan code. In a plan-driven execution the same event or terminal cascade sets `plan_dirty`, regardless of what a previous evaluation read; a later reconciliation sees the complete retained snapshot.

`Within` is stored on a node with `Await` only. When all command-dependency groups settle successfully, the engine checks retained facts first. If any awaited fact is missing, it writes `wait_started_at = db_now` and a once-only deadline capped by the execution deadline. An early fact satisfies the wait immediately.

The persisted deadline, not sweeper timing, decides a race. A matching event whose PostgreSQL `recorded_at` is no later than the deadline satisfies the wait. A later event remains in the journal but leaves that wait unresolved. Bounded maintenance subsequently makes the command `expired`, records its terminal event, and marks a plan-driven execution dirty so failure branches can be declared.

## 15. Fail-fast and completion

When a required command becomes failed, cancelled, or expired, the transaction first records its terminal event and resolves all dependencies. It then changes the execution to `failing` and, when fail-fast is enabled, cancels non-terminal work outside the failure-handling closure. The closure begins with work made viable by `AfterFailed`/`AfterSettled`, includes its descendants, and preserves already running commands. Failure handling therefore cannot be cancelled before it is selected.

Optional unsuccessful commands resolve dependencies but do not make the execution fail. A coordinator that wants to interpret child failure normally uses optional children plus `OnOutcome`.

Completion rules are mode-specific:

- **direct**: `open_commands == 0`; closed child membership makes this a complete tree test;
- **plan**: `open_commands == 0`, `plan_dirty = false`, the latest evaluation has no temporary read, and `plan_quiescent = true`; if already failing, temporary reads are ignored but dirty reconciliation and explicit remaining failure-handling work still block terminal failure;
- **coordinator**: a fenced handler has staged `SucceedExecution` or `FailExecution`; success additionally requires no non-terminal command after the same decision, while failure cancels outstanding work.

Every terminal transition appends one execution terminal event and closes any coordinator. Execution cancellation and completion race on the execution lock; the first committed terminal event wins. Terminal state never reopens.

## 16. Coordinators and durable agents

One coordinator instance belongs to one coordinator-driven execution. It stores current canonical state, a state revision, whether the start activation is pending, the last consumed journal position, and at most one current delivery/lease.

After start activation, the coordinator scheduler finds the lowest matching event position above the inbox for the registered coordinator definition. Ordinary `On` selectors match indexed event metadata. `OnOutcome` matches command-terminal journal rows by joining their `command_id` to the immutable command name/version projection; its typed result or failure comes from the same terminal row. No second failure event or denormalized outcome stream is written. Registration rejects overlap with success-only `On(cmd.Done())`.

The selected event position becomes the durable delivery identity until acknowledged. One handler runs without a database transaction, stages state mutation/events/spawns/completion, and then settles under the execution and coordinator fences. Success appends `CoordinatorTransition`, commits the new state, advances the inbox to the handled position, applies outputs, and clears delivery retry state atomically. Error leaves the inbox unchanged and schedules the same delivery. Exhaustion appends `CoordinatorFailed`, fails the execution, and cancels work.

Unmatched journal entries need not be delivered one by one. The indexed query jumps to the next matching event and safely advances past unmatched positions because the coordinator definition/version and subscriptions are immutable for that execution.

For an adaptive agent, the execution is the episode, coordinator state is bounded orchestration memory, model/tool/sub-agent activities are commands, and terminal outcomes are ordered observations. `StartAfter` creates the next durable turn without sleeping. External user input is an idempotent published event. Large transcripts and artifacts remain application data referenced from state. A recursively adaptive sub-agent will use a future child execution rather than nesting another coordinator authority inside the parent execution.

## 17. Distribution, wake-up, and recovery

Every replica is anonymous. It may register a subset of command, plan, and coordinator definitions, poll the same database, and claim compatible work. Command workers, plan reconcilers, publishers, and coordinator handlers may be separate pools. There is no leader, partition assignment, sticky ownership, or correctness dependency on process identity.

The command queue uses a narrow materialization containing immutable name/version and lane columns, so a rolling-deployment worker does not repeatedly probe an unhandled head-of-lane backlog. Dirty plans use a partial index on execution definition and are claimed directly under the execution row lock. The exact indexes and adversarial benchmarks are specified in the schema component.

Notifications are transactional hints containing only schema-safe queue/execution identifiers. On initial listener connection and reconnect the runtime performs a catch-up poll before sleeping. A generation-counted local wake hub closes the check-then-sleep race. Poll-only operation is fully supported.

Recovery tasks are bounded and leaderless:

- expired command lease -> conclude attempt as interrupted/lease-lost, return command to its persisted retry/ready schedule without consuming budget;
- expired coordinator lease -> same delivery remains selected and retryable;
- expired `Within` or execution deadline -> ordinary fenced terminal transition and dependency/completion processing;
- due delayed/retry work -> no state migration is necessary; the claim index becomes eligible by time;
- dirty plan -> exact-version plan scheduler reconciles it; rollback/crash leaves it dirty;
- unclaimable command, dirty-plan, or coordinator registration gap -> observation and inspection only, never automatic failure.

Optional local affinity is not in M1. A later implementation may store a preferred replica and a short preference window, but another compatible replica must always take over after that window and no handler may depend on local cache state.

## 18. Canonical encoding and durable evolution

Canonical encoding happens at API or staging boundaries, before locks where possible. The library rejects invalid JSON values, non-finite numbers, unsupported map keys, excessive nesting, and configured size limits. Custom `MarshalJSON` output is canonicalized after marshaling; application tests should use `flowtest.AssertCanonicalStable` for custom types.

SHA-256 accelerates equality but is not trusted as the sole comparison on a conflict path: equal digests compare canonical bytes. Stored durable names and versions define schema and orchestration meaning. A material payload, result, coordinator-state/subscription/decision, plan declaration/read, or commit-semantics change requires a new version; behavior-preserving worker implementation changes do not. Go function bodies are not made durable or hashed, so deploying divergent plan or coordinator behavior under one name/version is explicitly invalid.

Errors and observations never include canonical bytes, payloads, connection strings, SQL, lease tokens, or unredacted application failures. Stored failures use bounded structured code/message data and a configurable redaction hook.

## 19. Error model

Public sentinels support `errors.Is`; a structured error adds safe operation, resource, identifier, and reason. Database mapping uses SQLSTATE and stable constraint names, never message text.

| Condition | Result |
|---|---|
| repeated equivalent execution/command/event | existing success/no-op |
| same stable key with different canonical identity | `ErrConflict` |
| invalid option, dependency, state transition, or ceiling on ingress | `ErrInvalid` / `ErrInvalidState` |
| stale lease or delivery | `ErrLeaseLost` |
| operation against non-idempotently terminal target | `ErrTerminal` |
| size limit | `ErrPayloadTooLarge` |
| expired transaction-bound client | `ErrClosed` |
| schema/version/checksum mismatch | `ErrSchema` |
| serialization or deadlock conflict in caller transaction | wrapped transient database error |

Handler errors are separately classified into retryable, explicit retry delay, permanent, panic, timeout, interruption, and lost lease. Deterministic plan/output/coordinator decision defects are permanent and are recorded durably rather than retried forever.

## 20. Inspection and projection

`History` reads the journal alone in position order and supports an after-position cursor. `Trace` combines replayable settled history with current materializations for live lease, next-run time, unresolved dependency, wait deadline, and coordinator-delivery detail.

The causal graph reducer uses:

- `ExecutionStarted` for the run root;
- `CommandCreated` for vertices and dependency/parent edges;
- attempt records for operational activity;
- events and terminal events for facts/outcomes;
- `PlanReconciled` for pure plan decisions and their consumed journal prefixes;
- `CoordinatorTransition` for durable state decisions;
- causation identifiers for decision edges.

A replay conformance test rebuilds settled execution, command, dependency, and coordinator projections from retained history and compares them with live materializations. Live lease-renewal state is deliberately excluded. Replay never invokes application code.

Terminal executions and complete journals are retained indefinitely in M1. Retention/archival is a near-term operational follow-on and may remove bulky payload bodies before causal skeletons, but doing so explicitly narrows historical simulation and rebuild guarantees.

## 21. Observability

The core observer receives post-commit records for execution and command transitions, claim/attempt outcomes, retry schedules and budget use, initial delays, wait starts/expiry, plan dirty/coalesced/claim/reconciliation duration and size, coordinator delivery, command-ceiling use/rejection, lease renewal/loss, unclaimable backlog, notification versus polling wake-up, and execution-lock wait.

Observer calls happen after commit or known rollback and outside locks. Bare caller-owned `InTx` has no commit hook, so commit-dependent observations are suppressed rather than emitted falsely. Adapter packages may translate observations into OpenTelemetry, metrics, or structured logs later.

## 22. Verification strategy

### 22.1 Pure and database-free

- canonical encoding/fingerprints and retry-policy decisions;
- staged worker output, dependency-scoped `ResultOf`/`OutcomeOf`, and commit-function inputs;
- plan declaration, read availability, determinism, reconciliation, dirty-trigger coalescing, and fragment composition;
- dependency condition matrices, cascaded skip, fail-fast closure, and mode-specific completion;
- coordinator event matching, `OnOutcome`, ordered state transitions, delayed turns, mixed successful/unsuccessful fan-in, and command ceiling.

### 22.2 PostgreSQL integration

- gap-free journal positions under concurrent ingress, claims, settlements, and rollbacks;
- exactly one creation record and terminal event per command;
- no stale lease settlement after cancellation, expiry, recovery, or takeover;
- all-or-nothing worker, plan-reconciliation, and coordinator batches and commit-function writes;
- Flow-before-application lock order and deliberate inverse-order failure test;
- immutable retry budget anchor across delay, retry, restart, and lease loss;
- early/late facts, `Within`, dependency resolution, failure handling, and completion;
- coordinator start, historical event delivery, failure redelivery, and typed terminal outcome fan-in;
- command ceiling across every creation path and changed replica defaults;
- rolling deployments with disjoint registered versions;
- poll-only recovery and listener reconnect catch-up;
- dirty-plan takeover, coalesced triggers, and command settlement without plan registration;
- replayed settled projections equal live projections.

### 22.3 Fault injection and properties

Crash points surround claim commit, handler return, every settlement phase, commit-function invocation, notification, ambiguous commit, lease recovery, coordinator inbox advance, and migration units. Properties include monotonic journal position, monotonic plan growth, nonnegative counters, one terminal event per terminal command, immutable closed child membership, and equality between `command_count` and accepted logical commands.

### 22.4 Benchmarks

- claim throughput with 90% locally unregistered head-of-lane kinds;
- same-execution parallel claim and settlement contention;
- plan evaluation and reconciliation at 10, 100, and 1,000 commands;
- immediate-terminal fixed-point chains and the technical pass guard;
- 1,000-child fan-out and 1,000-result concurrent fan-in;
- one coordinator processing a high-rate event stream;
- lease renewal at hundreds of active handlers;
- transactional `NOTIFY` versus poll-only commit throughput;
- WAL, dead tuples, autovacuum, and journal growth at realistic payload sizes.

## 23. Known trade-offs and extension boundaries

- The execution row lock serializes short semantic commits and coordinator decisions within one execution. This buys simple ordering and atomic progression; very large independent adaptive branches should become child executions later.
- Plan evaluation occurs inside its own short execution-lock transaction. The 1,000-command default, bounded plan concurrency, trigger coalescing, and plan metrics make the cost explicit; direct and coordinator modes avoid repeated whole-graph evaluation.
- Removing persistent plan-read routing deliberately permits over-evaluation: an application event may dirty a plan that does not use it. The trigger adds no row and updates an execution row already locked for journal append; repeated triggers coalesce behind one bit. This favors a simple no-missed-input proof over subscription bookkeeping. Benchmarks must include irrelevant-event floods, and any future skip index may be only a disposable hint—never a correctness dependency.
- The initial journal duplicates some immutable bytes also present in projections. Simplicity and standalone history win in M1; content addressing is a compatible storage optimization.
- Coordinator processing is deliberately single-threaded per execution. Parallel work belongs in spawned commands; the coordinator remains the short durable decision point.
- Transactional notifications can impose a database-wide commit cost. They are optional hints, measured before default enablement, and can be disabled without changing correctness.
- Child executions, administrative fork/retry, recurring schedules, local affinity, cross-execution export, UI, and telemetry adapters are additive boundaries. Retention is the first operational follow-on but does not change M1 semantics. None may weaken per-execution ordering or introduce a second orchestration authority into one execution.

## 24. Component responsibility matrix

| Concern | Schema | Engine | Runtime |
|---|---:|---:|---:|
| DDL, indexes, constraints, SQL contracts | owner | consumer | consumer |
| journal batch shape and replay reducer | physical owner | semantic owner | driver |
| canonical typed definitions | storage codec | owner | registry |
| command/dependency/plan/coordinator state machines | persistence | owner | invokes |
| worker/coordinator invocation and leases | statements | decision contracts | owner |
| polling, notification, shutdown, recovery | statements | transition contracts | owner |
| migration and SQL error mapping | owner | — | entry points |
| `Trace`/`History` | queries | projection semantics | public API |
| `flowtest` | — | pure harness core | public package |

The implementation plan must treat the functional specification's §22 acceptance criteria as milestone exit criteria, not as optional follow-up tests.
