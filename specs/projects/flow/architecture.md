---
status: draft
---

# Architecture: flow

## 1. Purpose and scope

This document translates the overview and functional specification into technical structure: data model, transaction rules, concurrency rules, package layout, and the algorithms behind command dispatch, bounded child spawning, terminal outcome events, event ordering, plan evaluation, and completion.

It is the authority on cross-cutting decisions and invariants. Three component documents own exact DDL, SQL, and per-component test matrices:

1. `components/schema.md` — complete DDL, indexes, constraints, and every statement.
2. `components/engine.md` — plan evaluation, dependency resolution, coordinator inbox, completion.
3. `components/runtime.md` — claim loop, leases, notifier, maintenance, migrations, error mapping.

No UI architecture is required.

## 2. Architecture decisions

| Area | Decision | Rationale |
|---|---|---|
| Backend | PostgreSQL 15+ via `pgkit/v2` over `pgx/v5` | Product constraint; enables atomic application/flow transactions. |
| Package shape | One public package `flow` plus `internal/` | No backend abstraction exists to justify a contract layer (FS §2.1). |
| IDs | UUIDv7 generated in Go, stored as `uuid`, exposed as opaque typed strings | Distributed generation, index locality, no public dependency on a UUID package. |
| Serialization point | **The execution row lock** | One mechanism provides commit serialization, gap-free event positions, and consistent plan evaluation. |
| Event positions | Counter on the execution row, allocated under that lock | Gap-free and commit-ordered by construction (§7). |
| Command claiming | `FOR UPDATE SKIP LOCKED` on command rows only, never the execution lock | Claiming appends no event and changes no execution state, so it must not serialize per execution. |
| Pending work | Declared-but-unrunnable nodes are command rows in state `pending` | One table, one lifecycle, uniform trace; no parallel node abstraction. |
| Command creation | Plans use repeatable `Do`; handlers stage asynchronous `Spawn`; clients use `Issue` | The verbs expose different idempotency and causation semantics while sharing one command lifecycle. |
| Worker fan-out | Spawned work is stored as direct children on `commands`; successful parent settlement closes membership | Bounded dynamic fan-out needs no coordinator or fan-out table. |
| Dependencies | Clause rows plus a denormalized unsatisfied-clause counter | Supports all five builders with an O(1) readiness check. |
| Terminal outcomes | Exactly one terminal event per command, enforced by a partial unique index | Success, failure, cancellation, expiry, and skip are replayable facts; attempts remain operational history. |
| Identity | Canonical JSON (RFC 8785) hashed with SHA-256 | Deterministic identity independent of Go memory layout or database formatting. |
| Time | `clock_timestamp()` for all durable scheduling and lease decisions | Avoids worker clock skew. |
| Notifications | `LISTEN`/`NOTIFY` advisory only, always with poll fallback | Polling is the correctness path. |
| Schema | Configurable, default `flow`; all SQL fully qualified | Avoids `search_path` and PgBouncer session-state assumptions. |
| Migrations | Embedded, explicit, checksummed, advisory-locked per unit | Supports library-driven or application-driven schema management. |

Decisions I am making on your behalf and would revisit on request: PostgreSQL 15 as the floor (nothing here needs 16+; UUIDv7 is generated in Go), RFC 8785 rather than a hand-rolled canonicalizer, and three component documents rather than one large architecture file.

## 3. System context

```text
      application processes                     worker processes
   (Client only — Start/Publish)            (Runtime — claim and execute)
                  │                                      │
                  └──────────────────┬───────────────────┘
                                     ▼
              ┌──────────────────────────────────────────┐
              │  github.com/goware/flow                  │
              │                                          │
              │  definitions · plans · client · runtime  │
              │  engine · worker pool · notifier         │
              └──────────────────┬───────────────────────┘
                                 │  pgkit/v2 · pgx/v5
                                 ▼
              ┌──────────────────────────────────────────┐
              │  PostgreSQL                              │
              │                                          │
              │  executions · commands · command_deps    │
              │  attempts · events · coordinators        │
              │  schema_migrations                       │
              └──────────────────────────────────────────┘
```

PostgreSQL is the only durable authority. Goroutines, channels, timers, notification payloads, and caches are accelerators.

## 4. Package structure

```text
github.com/goware/flow
├── flow.go            // Runtime, Client, New, Run, Stop, InTx
├── define.go          // DefineCommand/Event/Plan/Coordinator, Register, RegisterAll
├── command.go         // Command[A,R], Work[A], Handle, Spawn, CommandOutcome[R]
├── event.go           // Event[T], EventName, Received[T]
├── plan.go            // Plan, Node, Do, Fact, Facts, Result, Outcome
├── coordinator.go     // Coordinator[S], Coordination[S], On
├── execute.go         // Start, StartWith, Issue, Publish, Cancel*
├── inspect.go         // Get, Lookup, Trace, History, List, Await, ResultOf
├── errors.go          // sentinels and typed Error
├── options.go         // Option, CommandOption, StartOption, ...
├── migrate.go         // Migrate, CheckSchema, MigrationFS
├── migrations/        // embedded .sql units
├── flowtest/          // database-free harness for workers and plans
└── internal/
    ├── store/         // every SQL statement, row types, scanning
    ├── engine/        // plan evaluation, reconciliation, dependency resolution, completion
    ├── worker/        // claim loop, dispatch, lease manager, shutdown
    ├── notify/        // listener, wake hub, poll fallback
    ├── canonical/     // RFC 8785 encoding and SHA-256 identity
    └── maint/         // reconciler, deadline expiry, unclaimable reporting
```

Rules: the public package holds types and thin entry points only; all SQL lives in `internal/store`; `flowtest` is public so applications can test their own handlers; no package exists solely to hold one type.

## 5. Dependencies

- `github.com/goware/pgkit/v2` — querying and transaction-bound `DB.InTx`
- `github.com/jackc/pgx/v5` — transactions, batches, pool, PostgreSQL errors, dedicated `LISTEN` sessions
- `github.com/google/uuid` v1.6.0 — UUIDv7 generation and parsing
- Go standard library — JSON, contexts, sync, `embed`, `log/slog`

No ORM, no distributed-lock service, no broker, no mandatory observability SDK. New dependencies require a demonstrated reduction in complexity.

## 6. Data model

### 6.1 Tables

| Table | Holds | Lifecycle owner |
|---|---|---|
| `executions` | identity, type, key, status, deadline, counters, **event position allocator**, plan/coordinator binding | execution lifecycle |
| `commands` | one row per logical command including declared-but-pending nodes and worker-spawned children | command lifecycle |
| `command_deps` | dependency clauses and their members | plan/coordinator declaration |
| `attempts` | one row per claimed execution of a command | worker runtime |
| `events` | the append-only per-execution log | append only |
| `plan_reads` | inputs the latest plan evaluation consulted | plan engine |
| `coordinators` | instance state and inbox position (hand-written coordinators only) | coordinator lifecycle |
| `schema_migrations` | migration metadata | migration engine |

### 6.2 Key columns

`executions` carries the counters that make hot-path decisions O(1):

- `next_event_position bigint` — the allocator (§7);
- `open_commands int` — non-terminal command count;
- `absent_reads int` — consulted-but-absent inputs from the latest evaluation;
- `failing bool` and `fail_fast bool` — outcome state machine;
- `plan_name`, `plan_version` or `coordinator_name`, `coordinator_version`.

`commands` carries `unsatisfied_clauses int`, decremented as clauses resolve; zero means runnable. It also carries:

- `source_kind` (`plan`, `worker`, `coordinator`, or `external`) and `parent_command_id`, which is non-null only for worker-spawned direct children;
- `required bool`, set false by `Node.Optional()` or `flow.Optional()`;
- canonical payload identity and the terminal result/failure projection used by `Result` and `Outcome`;
- the lease triple (`lease_id`, `leased_at`, `lease_expires_at`), `eligible_at`, and the timestamp taxonomy from FS §17 as four distinct columns.

`events` carries `(execution_id, position)` as a unique key, `(execution_id, name, event_key)` as the idempotency key for published facts, `event_kind`, and nullable `command_id` / `attempt_id` origins. A partial unique index on `command_id` for terminal command event kinds enforces exactly one of completion, failure, cancellation, expiry, or skip per command.

### 6.3 Why pending nodes are commands

A plan node that cannot yet run is a `commands` row in state `pending`, not a row in a separate node table. Its payload is known at declaration time, so nothing is deferred. Worker-spawned children are ordinary `ready` command rows with a parent link. This gives one lifecycle, one execution-wide key namespace, one set of indexes, and a trace where declared, spawned, waiting, and running work are the same shape. The claim query simply never sees `pending`.

## 7. Event ordering: the central proof

FS §9.4 requires that a checkpoint never permanently skip an event whose transaction becomes visible later. The design satisfies this by construction rather than by a watermark protocol.

**Rule: every transaction that appends an event to an execution first takes `SELECT … FROM executions WHERE id = $1 FOR UPDATE`.** Positions are allocated by incrementing `next_event_position` under that lock.

It follows that:

1. Only one transaction at a time can allocate positions for an execution.
2. A later allocator blocks until the current holder commits or rolls back, so it observes the committed counter.
3. A rolled-back append returns its positions; no gap is created.
4. Position order therefore equals commit order, and there is no window in which position *n* is visible while *n−1* is still uncommitted.
5. A reader that has consumed through position *n* can never later discover an unseen event at a position below *n*.

This is the jobqueue EventStore's singleton-allocator argument scoped to one execution — but here the lock is already required for commit serialization (FS §12.5), so ordering costs nothing extra. The database-wide allocator that dominated the jobqueue design does not exist because no ordering spans executions.

Consequence: per-execution append throughput is bounded by that row lock, which is the documented per-execution ceiling (FS §16.6).

The ordered event log is replayable orchestration history, not the sole persistence model. Command rows durably record issuance, source, parentage, and current projection; attempts preserve transient mechanics; terminal events and domain events preserve immutable facts. A trace or replacement read model rebuilds from all four. Runtime recovery resumes non-terminal materialized commands and never re-invokes historical successful handlers merely to reconstruct state.

## 8. Transaction model

### 8.1 Transaction kinds

| Kind | Takes execution lock | Purpose |
|---|---|---|
| **Claim** | no | `ready` → `running`, create attempt |
| **Settle** | yes | worker result → spawned children, completion event, plan evaluation, reconciliation, outcome |
| **Ingress** | yes | `Start`, `Issue`, `Publish`, `Cancel*` |
| **Maintenance** | yes, one execution at a time | deadline expiry, dispatch reconciliation |
| **Renew** | no | batched lease extension |

### 8.2 Lock order

Within any transaction that takes the execution lock:

1. `executions` row;
2. existing `commands` rows in ascending `command_id` order, then new command inserts in ascending command key order;
3. `command_deps`, `attempts`, `coordinators` for those commands;
4. `events` insert;
5. application writes registered through `OnCommit`;
6. `pg_notify`;
7. commit.

`OnCommit` callbacks run at step 5 — after all flow-owned rows are locked and before notification. Application tables therefore sit outside this order; a caller-owned transaction that locks application rows first and then calls into `flow` can deadlock. The rule for callers (FS §12.7) is: perform application writes first, call `flow` operations last, and let `InTx` participate rather than interleaving.

### 8.3 Claim is the no-wait exception

The claim transaction never takes the execution lock and never waits for a row: it uses `FOR UPDATE SKIP LOCKED` on candidate command rows and abandons any candidate it cannot lock immediately. A path that never waits cannot deadlock, so claiming is exempt from the order above.

This is why claiming does not serialize per execution: many commands of one execution can be claimed concurrently across replicas, and they queue only when they settle.

Races are resolved by state predicates rather than ordering. A claim that wins the row transitions `ready → running` under `WHERE state = 'ready'`; a concurrent cancel holding the execution lock blocks on the same row, then observes `running` and cancels with fencing. Reversed, the claim skips the locked row entirely.

### 8.4 Isolation and retry

`READ COMMITTED` throughout. Correctness comes from row locks, state predicates, lease fences, and unique constraints. Library-owned transactions retry SQLSTATE `40001` and `40P01` up to three times with full-jitter backoff of 5–100 ms; caller-owned transactions return the error. Handler code never re-runs on a database retry — only the settle transaction replays, from the already-buffered outputs.

## 9. Command dispatch

### 9.1 Claim query

Per lane, a worker claims with one statement:

```sql
WITH t AS MATERIALIZED (SELECT clock_timestamp() AS now),
candidates AS (
    SELECT c.command_id
    FROM flow.commands c, t
    WHERE c.lane = $1
      AND c.state = 'ready'
      AND c.eligible_at <= t.now
      AND (c.name, c.version) = ANY($2::flow.name_version[])
    ORDER BY c.eligible_at, c.command_id
    FOR UPDATE OF c SKIP LOCKED
    LIMIT $3
)
UPDATE flow.commands …            -- state, lease triple, from t.now
```

`$2` is the process's registered `(name, version)` set, which is what makes rolling deployments safe (FS §7.6). `$3` is bounded by immediately free local capacity, never by a configured batch alone, so leases never expire in a local queue.

The supporting index is `(lane, state, eligible_at, command_id)` partial on `state = 'ready'`, keeping pending, running, and terminal rows out of the hot index entirely. Whether `(name, version)` belongs in that index is a benchmark question deferred to `components/schema.md`; the adversarial case is a lane whose head is dominated by kinds the process does not register.

### 9.2 Leases and fencing

Claiming writes a fresh `lease_id`. Every subsequent write for that attempt — settle, fail, renew — carries `WHERE lease_id = $x AND lease_expires_at > clock_timestamp()`. Zero affected rows maps to `ErrLeaseLost`.

Renewal is batched per process: one statement extends many leases from database time using `unnest` pairs, and any receipt not returned has lost ownership, cancelling only that handler's context.

## 10. The settle transaction

This is the system's central algorithm. On a successful worker return:

1. lock the execution row; reject if terminal;
2. verify the attempt's fence: command `running`, matching `lease_id`, unexpired;
3. prevalidate the complete buffered output: canonical sizes, the applicable total-command ceiling, duplicate child keys, and conflicts against every existing execution key; deterministic output conflicts become a permanent command failure rather than a futile retry;
4. insert all worker-spawned children in command-key order, with `source_kind = worker`, `parent_command_id` set to the current command, inherited execution identity and causation, and `required` from the spawn option; increment `open_commands` for the batch;
5. allocate positions and append the worker's emitted domain events, then its **completion event** carrying the typed result;
6. mark the parent command `succeeded`, store its result projection, clear its lease, and decrement `open_commands`;
7. **resolve dependencies** naming the parent (§11);
8. **evaluate the plan** if required (§12), against a snapshot that already includes the parent result and complete child set, reconciling plan declarations and creating newly runnable commands;
9. run `OnCommit` callbacks;
10. evaluate completion or fail-fast (§13–14);
11. `pg_notify` every affected lane;
12. commit.

Children are buffered in Go while the handler runs; `Spawn` performs no SQL. Step 3 validates the entire set before step 4 writes any row. Equivalent duplicate keys inside one buffer coalesce. Different buffered content, collision with a key owned by another creation source, or a command-count overflow is a structured permanent output failure: no staged child, domain event, completion result, or `OnCommit` write commits; instead the parent records `CommandFailed` and follows ordinary dependency and execution-failure rules.

A retryable handler error records only attempt history and `retry_wait`; it commits no terminal event or staged output. Exhaustion, `Permanent`, cancellation, expiry, and dependency skipping use the same execution lock and each append exactly one `CommandFailed`, `CommandCancelled`, `CommandExpired`, or `CommandSkipped` event before resolving dependents. Skip cascades allocate one ordered event per skipped command.

A plan defect found at step 8 is handled specially because repeating accepted worker work cannot repair plan code. The engine discards the plan declaration buffer, preserves the parent success and its staged outputs, appends `PlanFailed`, cancels every non-terminal command including newly inserted children with terminal outcome events, appends `ExecutionFailed`, runs the parent's `OnCommit` callbacks, and commits. It does not select plan failure branches and consumes no worker retry budget.

Database-level retries replay only the already-buffered settle algorithm, never the handler. An ordinary transactional failure, including an `OnCommit` error, rolls the whole settlement back and leaves the command eligible for normal retry; a recovered plan defect is a deliberate committed terminal outcome, not such a failure.

## 11. Dependency resolution

### 11.1 Clause model

A command's readiness is a conjunction of clauses:

| Builder | Clause kind | Satisfied when |
|---|---|---|
| `After(k…)` | `all_succeeded` | every member succeeded |
| `AfterSettled(k…)` | `all_settled` | every member terminal |
| `AfterFailed(k…)` | `all_unsuccessful` | every member failed, expired, cancelled, or skipped |
| `AfterAny(n, k…)` | `at_least` | ≥ n members succeeded |
| `Await(e…)` | `await_event` | an event of each named `(name, version)` exists |

Each clause stores its kind, threshold, and members in `command_deps`. A member is resolved by the execution-wide command key and may have been created by `Do`, `Spawn`, or `Issue`. `commands.unsatisfied_clauses` counts clauses not yet satisfied; a command is runnable at zero.

### 11.2 Resolution algorithm

When a command reaches a terminal state, in the same transaction:

1. select clauses naming it, joined to their dependents, locking dependents in `command_id` order;
2. for each clause, recompute satisfied and unsatisfiable status from member states;
3. a newly satisfied clause decrements `unsatisfied_clauses`; reaching zero transitions the dependent `pending → ready` with `eligible_at = max(now, declared delay)`;
4. a clause that has become **permanently unsatisfiable** transitions the dependent to `skipped`, appends its unique `CommandSkipped` outcome event, decrements `open_commands`, and recurses to step 1.

Reconciliation validates that every new dependency targets an existing command or another declaration in the same plan evaluation and that the resulting graph is acyclic. Spawned and externally issued commands have no dependency clauses of their own. Resolution is bounded by the command limit and processed as an explicit work queue, not actual recursion.

Every guarded update carries its expected prior state, so a redelivered or concurrent resolution is a no-op rather than a double decrement.

### 11.3 Await clauses

An `await_event` clause is satisfied by existence, and events are append-only, so satisfaction is monotonic. Rather than scanning the log, the engine checks satisfaction when an event of a matching `(name, version)` is appended — one indexed lookup of pending clauses awaiting that name.

## 12. Plan evaluation

### 12.1 When

Per FS §10.3, evaluation is required at execution start, when a terminal outcome is appended for a command in `plan_reads` or `command_deps`, or when another event arrives whose name appears in `plan_reads`. Claim, lease, `running`, and `retry_wait` changes are invisible to plans and do not trigger evaluation. The engine reads these narrow indexes in the settle transaction and skips evaluation otherwise. Because plans are pure, skipping is sound; the implementation may over-evaluate freely.

### 12.2 How

Inside any transaction evaluating a plan while holding the execution lock:

1. load the complete command set for this execution: `(key, name, version, source_kind, parent_command_id, required, state, payload_hash, result/failure locator)` — one narrow indexed query;
2. load the event index: `(name, version)` present, and fetch payloads or command outcomes lazily for `Fact`, `Facts`, `Result`, and `Outcome`;
3. construct a `*Plan` bound to that snapshot and invoke the user's plan function in Go;
4. collect the declared node set and the consulted-input set;
5. validate every declaration, dependency, and read before writing; recover a panic as a typed plan defect;
6. reconcile (§12.3), or apply §10's terminal plan-defect path;
7. replace `plan_reads` with the new consulted set and update `executions.absent_reads`.

The plan API exposes no I/O capability: no context, database, client, transaction, clock, randomness, or handler scope. Go cannot prevent application code from reaching a package global, so this is a contract rather than a sandbox. `flowtest.AssertDeterministic` evaluates an identical snapshot at least twice and compares canonical declarations and consulted reads; an optional debug runtime mode does likewise. Reconciliation conflicts, invalid reads, panics, and debug-mode divergence are plan defects, never worker failures.

`Result` and `Outcome` resolve the key in either the declarations already buffered by the current evaluation or the complete durable command snapshot, then verify the supplied definition's name and version. A missing key or definition mismatch is invalid. A newly declared command has no outcome yet. `Result` records an absent read until the command succeeds; `Outcome` records an absent read until it is terminal. A durable command need not be owned by the plan, which is how a plan joins worker-spawned children.

The §10 terminal plan-defect path is shared by every transaction that evaluates a plan. During `Start`, the new execution is retained as failed with `PlanFailed` and `ExecutionFailed`; during ingress, the accepted triggering command or fact is retained before the same terminal transition. The public operation returns a typed error containing the durable `ExecutionID`, so inspection is possible and an idempotent retry resolves to the same execution rather than evaluating again.

### 12.3 Reconciliation

| Declared key | Action |
|---|---|
| absent, all clauses satisfied | insert command as `ready`, `open_commands++` |
| absent, clauses unsatisfied | insert command as `pending` with clause rows, `open_commands++` |
| absent, any clause permanently unsatisfiable | insert command as `skipped`, append `CommandSkipped`, do not increment `open_commands` |
| present and `source_kind = plan` | compare stored payload, definition, required flag, and normalized clauses; any mismatch → plan defect |
| present from worker, coordinator, or external ingress | `Do` ownership conflict → plan defect; reads and dependency references remain valid |
| previously declared, now absent | retained untouched |

For a plan-driven execution, the engine validates all references, the 1,000-total-command limit including children and ingress, dependency counts, and acyclicity before inserting anything. All inserts happen in the one transaction, satisfying FS §12.4's atomicity requirement. Insert order is by command key so concurrent executions cannot deadlock on shared unique indexes.

### 12.4 Snapshot consistency

Because the execution row lock is held, the snapshot cannot change under the evaluation, and two evaluations for one execution can never interleave. This is what lets the plan be a plain pure function with no concurrency contract.

## 13. Outcome and fail-fast

`executions.failing` and `open_commands` drive the outcome machine.

On a `failed`, `expired`, or `cancelled` terminal transition of a required command, regardless of whether its source is plan, worker, coordinator, or external ingress, within the same transaction and in FS §6.3's mandated order:

1. record the terminal state;
2. **resolve all dependency clauses naming it first**, so `AfterFailed` and `AfterSettled` dependents become runnable and success-only dependents become `skipped`;
3. set `failing = true` and cancel remaining non-terminal commands **except** the failure-handling closure — the dependents just made runnable, their transitive dependents, and anything already `running`;
4. leave the execution non-terminal until that closure resolves.

The closure is computed by walking outward from the newly runnable set over `command_deps`, bounded by the command limit. Ordering matters: step 2 strictly precedes step 3, which is what guarantees a refund branch its chance to run. With fail-fast disabled, siblings continue and outcome is evaluated only after all commands settle.

Optional commands still append their terminal outcome events and resolve dependencies, but an unsuccessful optional outcome does not set `executions.failing`. A plan may use `Outcome` plus `AfterSettled` to build a partial-result command from those facts.

## 14. Completion

A plan-driven execution succeeds when, at the end of a settle transaction:

- `failing = false` and no required command ended failed, expired, or cancelled;
- `open_commands = 0`;
- `absent_reads = 0`;
- the final evaluation declared no new node.

These are cheap reads on the already-locked execution row. If `failing` is true, reaching zero open commands completes as failed after the failure-handling closure settles. `open_commands` counts plan-declared, spawned, and externally issued commands alike, so unfinished child work cannot disappear from completion. `absent_reads` prevents a plan that branches on a never-arriving fact or non-terminal child outcome from reporting false success (FS §10.1).

A coordinator-driven execution succeeds only on an explicit `SucceedExecution`, validated against `open_commands = 0`.

Either way the transition writes the terminal state, cancels anything outstanding, and appends `ExecutionSucceeded` or `ExecutionFailed` at the next position.

## 15. Coordinator inbox

A hand-written coordinator holds `inbox_position` on its row. Delivery selects the lowest-positioned event above it whose `(name, version)` the definition subscribes to, takes the execution lock, invokes the handler, and in one transaction persists new state, staged outputs, and the advanced position.

Head-of-line blocking is intended (FS §9.4): a failed delivery does not advance the position, so ordering is preserved and the coordinator retries under the standard policy. A new instance starts at position 0 and scans the retained log in order; no separate broker replay protocol or in-memory backlog exists.

## 16. Notifications and wake-up

One optional session-persistent `pgx.Conn` per process runs `LISTEN` on a channel derived from a SHA-256 hash of the configured schema (`flow_<hash>`), isolating schemas that share a database. Payload is compact versioned JSON naming the lane or execution — never payloads, arguments, or errors.

On connect the notifier issues `LISTEN`, then performs a catch-up wake for every bound lane before relying on notifications, closing the start-up race. A local wake hub coalesces duplicate hints and uses a per-key generation counter so a wake arriving between an empty claim and a sleep is not lost.

Transactional `NOTIFY` takes a global lock through commit, serializing notifying commits database-wide — the DBOS finding recorded in the jobqueue architecture. Exposure here is bounded because settle transactions emit at most one hint per affected lane. The sanctioned evolution, if benchmarks demand it, is a decoupled hint flusher batching hints into separate small transactions; fallback polling already covers hint loss. This must be benchmarked early.

## 17. Canonical identity

`internal/canonical` implements RFC 8785 (JSON Canonicalization Scheme) and hashes with SHA-256 into a 32-byte column. Canonical bytes back every identity comparison: execution start idempotency, command payload equality on re-declaration, and event idempotency.

Types with custom `MarshalJSON` are canonicalized from their emitted JSON, so a marshaler that is itself nondeterministic produces unstable identity. This constraint is documented rather than enforced; `flowtest` provides a round-trip stability assertion applications can run over their own payload types.

## 18. Errors

Sentinels support `errors.Is`; a typed `Error` carries operation, resource, identifier, and reason. The PostgreSQL adapter maps by SQLSTATE and constraint name — **never message text** — and retains the underlying `*pgconn.PgError` through wrapping.

| Condition | Mapping |
|---|---|
| `23505` on an idempotency constraint | compare stored hash, then success or `ErrConflict` |
| staged child key conflicts with another creation source | permanent parent `CommandFailed`; no staged output commits |
| staged batch exceeds the execution command limit | permanent parent `CommandFailed`; no staged output commits |
| `23503` ownership reference | `ErrNotFound` or `ErrConflict` by operation |
| `23514` check violation | `ErrInvalid` with the safe field name |
| zero fenced update rows | diagnostic lookup → `ErrLeaseLost`, `ErrTerminal`, or `ErrNotFound` |
| `40001` / `40P01` | internal retry where authorized, else typed transient |
| connection loss at commit | uncertain-commit detail; stable keys let the caller re-derive outcome |
| checksum or version mismatch | `ErrSchema`, startup-fatal |

Constraint names are a stable part of the migration contract and cannot be renamed casually.

## 19. Migrations

SQL is embedded with `go:embed` and also exposed as an `fs.FS` with the configured schema rendered in, so applications may run it through their own tooling.

`Migrate` applies one unit per transaction, each guarded by `pg_advisory_xact_lock` keyed on database identity and schema hash: acquire, re-check the next required version, verify checksums of applied units, apply, record version/name/checksum/library version, commit. Concurrent migrators interleave only at unit boundaries and never apply a unit twice.

Startup verifies compatibility without mutating. Schema evolution follows expand/migrate/contract, and durable-schema evolution of payloads is handled by `(name, version)` pairs rather than by migrations.

## 20. Observability

The root package defines one `Observation` struct and an `Observer` interface with a no-op default. Transactional code buffers observations and emits them only after the commit or rollback result is known; a bare `InTx` suppresses commit-dependent observations because a `pgx.Tx` exposes no commit hook, and `Transact`/`BindTx` provide the hooked form.

Required measurements: claim rate and empty claims, handler duration, attempt outcomes split by operational interruption versus application failure, retry schedule depth, lease renewal and loss, spawned-child count and settle conflicts, **plan evaluation count, total command count, and duration**, plan defects and debug determinism failures, evaluations skipped, dependency resolutions per settle, `absent_reads` per execution, unclaimable backlog by `(name, version)`, notification latency versus poll, and per-execution lock wait.

Observer callbacks run outside transactions and locks; panics are recovered and cannot affect durable results.

## 21. Testing strategy

**Unit** — validation, state machines, retry decisions, clause satisfaction, canonical encoding stability, error classification, wake-hub generation logic.

**`flowtest`** — the public harness. A worker is exercised as a function with inspection of staged children, events, and `OnCommit` callbacks. A plan is exercised as a pure function over `(args, facts, command states)` asserting declarations, clauses, `Result` / `Outcome` reads, and consulted inputs. `AssertDeterministic` compares repeated evaluation of the identical snapshot. No database.

**Integration against real PostgreSQL** — hundreds of concurrent claimers; lease expiry and stale fencing; claim/cancel races; settle/cancel races; crash injection at every step of §10; all-or-nothing child spawning; parent retry and duplicate child keys; spawned-child `Result` / `Outcome` joins; exactly one terminal event for success, failure, cancellation, expiry, and skip; publish-before-declare and declare-before-publish; repeated evaluation creating no duplicate commands; plan-defect terminal handling without worker retry; `absent_reads` blocking false success; fail-fast preserving `AfterFailed` branches; `Await` expiry; gap-free positions under concurrent ingress; coordinator head-of-line ordering; migration concurrency and checksums; rolling deployments with divergent registered versions.

**Property tests** — `unsatisfied_clauses` never negative; a command becomes `ready` at most once; a command has exactly one terminal event iff it is terminal; a successful parent has an immutable complete direct-child set; positions are gap-free and strictly increasing per execution; `open_commands` equals the true non-terminal count after any operation sequence.

**Benchmarks** — claim throughput and plan cost at the 1,000-command ceiling; settle latency with worker-spawned and plan-declared fan-out; per-execution lock contention; transactional `NOTIFY` commit throughput versus a batched flusher versus poll-only; autovacuum and WAL behavior on `commands` and `events`.

## 22. Performance strategy

Correctness precedes throughput claims. Every hot query is checked with `EXPLAIN (ANALYZE, BUFFERS)` against representative distributions, including the adversarial claim case in §9.1. Index changes require write-amplification measurement, not only read plans.

`commands` and `events` are the churn-heavy tables and get explicit autovacuum settings in the initial migration, with the fillfactor and thresholds recorded in `components/schema.md`. Partitioning is deferred pending evidence; `events` and terminal executions are the first candidates.

No connection is held for handler duration. One optional listener connection plus short pool borrows support many worker goroutines.

## 23. Component responsibilities

**`components/schema.md`** — every table's DDL, constraints, and indexes; the claim, settle, resolution, and maintenance statements verbatim; autovacuum settings; index benchmark results including the `(name, version)` question from §9.1.

**`components/engine.md`** — staged-child validation and insertion; plan snapshot construction and the `Plan` binding; `Result` / `Outcome`; reconciliation and resolution as executable pseudocode; terminal outcome uniqueness; the failure closure computation; plan-defect settlement; completion evaluation; coordinator inbox delivery; the plan-purity test matrix.

**`components/runtime.md`** — worker pool, capacity-aware dispatch, lease manager, notifier and wake hub, maintenance tasks, migration engine, error mapping tables, and the `flowtest` harness API.

## 24. References

- [`pgkit/v2`](https://github.com/goware/pgkit) · [`pgx/v5`](https://github.com/jackc/pgx) · [`google/uuid`](https://github.com/google/uuid)
- [PostgreSQL `SELECT`, row locks, `SKIP LOCKED`](https://www.postgresql.org/docs/15/sql-select.html) · [explicit locking](https://www.postgresql.org/docs/15/explicit-locking.html) · [`LISTEN`](https://www.postgresql.org/docs/15/sql-listen.html) · [`NOTIFY`](https://www.postgresql.org/docs/15/sql-notify.html) · [routine vacuuming](https://www.postgresql.org/docs/15/routine-vacuuming.html)
- [RFC 8785 — JSON Canonicalization Scheme](https://www.rfc-editor.org/rfc/rfc8785)
- [DBOS: Postgres LISTEN/NOTIFY scalability](https://www.dbos.dev/blog/postgres-listen-notify-scalability) · [HN discussion](https://news.ycombinator.com/item?id=49040296)
