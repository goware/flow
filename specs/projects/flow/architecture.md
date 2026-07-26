---
status: draft
---

# Architecture: flow

## 1. Purpose and scope

This document translates the completed overview and functional specification into technical structure: data model, transaction rules, concurrency rules, package layout, and the algorithms behind command dispatch, event ordering, plan evaluation, and completion.

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
| Dependencies | Clause rows plus a denormalized unsatisfied-clause counter | Supports all five builders with an O(1) readiness check. |
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
├── command.go         // Command[A,R], Work[A], Handle
├── event.go           // Event[T], EventName, Received[T]
├── plan.go            // Plan, Node, Do, Fact, Facts, Result
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
| `commands` | one row per logical command including declared-but-pending nodes | command lifecycle |
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

`commands` carries `unsatisfied_clauses int`, decremented as clauses resolve; zero means runnable. It also carries the lease triple (`lease_id`, `leased_at`, `lease_expires_at`), `eligible_at`, and the timestamp taxonomy from FS §17 as four distinct columns.

`events` carries `(execution_id, position)` as a unique key and `(execution_id, name, event_key)` as the idempotency key.

### 6.3 Why pending nodes are commands

A plan node that cannot yet run is a `commands` row in state `pending`, not a row in a separate node table. Its payload is known at declaration time, so nothing is deferred. This gives one lifecycle, one identity, one set of indexes, and a trace where declared-but-waiting work and running work are the same shape. The claim query simply never sees `pending`.

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

## 8. Transaction model

### 8.1 Transaction kinds

| Kind | Takes execution lock | Purpose |
|---|---|---|
| **Claim** | no | `ready` → `running`, create attempt |
| **Settle** | yes | worker result → completion event, plan evaluation, reconciliation, outcome |
| **Ingress** | yes | `Start`, `Issue`, `Publish`, `Cancel*` |
| **Maintenance** | yes, one execution at a time | deadline expiry, dispatch reconciliation |
| **Renew** | no | batched lease extension |

### 8.2 Lock order

Within any transaction that takes the execution lock:

1. `executions` row;
2. `commands` rows in ascending `command_id` order;
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

`$2` is the process's registered `(name, version)` set, which is what makes rolling deployments safe (FS §7.5). `$3` is bounded by immediately free local capacity, never by a configured batch alone, so leases never expire in a local queue.

The supporting index is `(lane, state, eligible_at, command_id)` partial on `state = 'ready'`, keeping pending, running, and terminal rows out of the hot index entirely. Whether `(name, version)` belongs in that index is a benchmark question deferred to `components/schema.md`; the adversarial case is a lane whose head is dominated by kinds the process does not register.

### 9.2 Leases and fencing

Claiming writes a fresh `lease_id`. Every subsequent write for that attempt — settle, fail, renew — carries `WHERE lease_id = $x AND lease_expires_at > clock_timestamp()`. Zero affected rows maps to `ErrLeaseLost`.

Renewal is batched per process: one statement extends many leases from database time using `unnest` pairs, and any receipt not returned has lost ownership, cancelling only that handler's context.

## 10. The settle transaction

This is the system's central algorithm. On a successful worker return:

1. lock the execution row; reject if terminal;
2. verify the attempt's fence: command `running`, matching `lease_id`, unexpired;
3. allocate positions and append the worker's emitted domain events, then its **completion event** carrying the typed result;
4. mark the command `succeeded`, clear its lease, decrement `open_commands`;
5. **resolve dependencies** naming this command (§11);
6. **evaluate the plan** if required (§12), reconciling declared nodes and creating newly runnable commands;
7. apply fail-fast if this transition was an unsuccessful terminal one (§13);
8. run `OnCommit` callbacks;
9. evaluate completion (§14);
10. `pg_notify` affected lanes;
11. commit.

Failure and cancellation follow the same skeleton with different step 3–4 outcomes. If any step fails, nothing commits and the command is retried per policy; only steps 3–10 replay on a database-level retry, never the handler.

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

Each clause stores its kind, threshold, and members in `command_deps`. `commands.unsatisfied_clauses` counts clauses not yet satisfied; a command is runnable at zero.

### 11.2 Resolution algorithm

When a command reaches a terminal state, in the same transaction:

1. select clauses naming it, joined to their dependents, locking dependents in `command_id` order;
2. for each clause, recompute satisfied and unsatisfiable status from member states;
3. a newly satisfied clause decrements `unsatisfied_clauses`; reaching zero transitions the dependent `pending → ready` with `eligible_at = max(now, declared delay)`;
4. a clause that has become **permanently unsatisfiable** transitions the dependent to `skipped`, which is itself a terminal transition and recurses to step 1.

Recursion is bounded because edges only ever point at nodes created in a later declaration, so the graph is acyclic by construction (FS §10.2's growth rule). The engine processes it as an explicit work queue, not actual recursion.

Every guarded update carries its expected prior state, so a redelivered or concurrent resolution is a no-op rather than a double decrement.

### 11.3 Await clauses

An `await_event` clause is satisfied by existence, and events are append-only, so satisfaction is monotonic. Rather than scanning the log, the engine checks satisfaction when an event of a matching `(name, version)` is appended — one indexed lookup of pending clauses awaiting that name.

## 12. Plan evaluation

### 12.1 When

Per FS §10.3, evaluation is required at execution start, when a declared node changes state, or when an event arrives whose name appears in `plan_reads`. The engine reads `plan_reads` in the settle transaction — it is a small set — and skips evaluation otherwise. Because plans are pure, skipping is sound; the implementation may over-evaluate freely.

### 12.2 How

Inside the settle transaction:

1. load the declared command set for this execution: `(key, name, version, state, payload_hash)` — one narrow indexed query;
2. load the event index: `(name, version)` present, and for `Fact`/`Facts` the payloads actually consulted, fetched lazily;
3. construct a `*Plan` bound to that snapshot and invoke the user's plan function in Go;
4. collect the declared node set and the consulted-input set;
5. reconcile (§12.3);
6. replace `plan_reads` with the new consulted set and update `executions.absent_reads`.

The plan function is pure and runs in memory; it performs no I/O and cannot see anything outside the snapshot. A panic in a plan function aborts the transaction and is reported as a plan defect, never as a command failure.

### 12.3 Reconciliation

| Declared key | Action |
|---|---|
| absent, all clauses satisfied | insert command as `ready`, `open_commands++` |
| absent, clauses unsatisfied | insert command as `pending` with clause rows, `open_commands++` |
| present | compare stored `payload_hash` and `(name, version)`; mismatch → `ErrConflict` |
| previously declared, now absent | retained untouched |

All inserts happen in the one transaction, satisfying FS §12.4's atomicity requirement. Insert order is by command key so concurrent executions cannot deadlock on shared unique indexes.

### 12.4 Snapshot consistency

Because the execution row lock is held, the snapshot cannot change under the evaluation, and two evaluations for one execution can never interleave. This is what lets the plan be a plain pure function with no concurrency contract.

## 13. Outcome and fail-fast

`executions.failing` and `open_commands` drive the outcome machine.

On an unsuccessful terminal transition of a non-`Optional` command, within the same transaction and in FS §6.3's mandated order:

1. record the terminal state;
2. **resolve all dependency clauses naming it first**, so `AfterFailed` and `AfterSettled` dependents become runnable and success-only dependents become `skipped`;
3. set `failing = true` and cancel remaining non-terminal commands **except** the failure-handling closure — the dependents just made runnable, their transitive dependents, and anything already `running`;
4. leave the execution non-terminal until that closure resolves.

The closure is computed by walking outward from the newly runnable set over `command_deps`, bounded by the node limit. Ordering matters: step 2 strictly precedes step 3, which is what guarantees a refund branch its chance to run.

## 14. Completion

A plan-driven execution succeeds when, at the end of a settle transaction:

- `open_commands = 0`;
- `absent_reads = 0`;
- the final evaluation declared no new node.

All three are cheap counter reads on the already-locked execution row. `absent_reads` is what prevents a plan that branches on a never-arriving fact from reporting false success (FS §10.1).

A coordinator-driven execution succeeds only on an explicit `SucceedExecution`, validated against `open_commands = 0`.

Either way the transition writes the terminal state, cancels anything outstanding, and appends `ExecutionSucceeded` or `ExecutionFailed` at the next position.

## 15. Coordinator inbox

A hand-written coordinator holds `inbox_position` on its row. Delivery selects the lowest-positioned event above it whose `(name, version)` the definition subscribes to, takes the execution lock, invokes the handler, and in one transaction persists new state, staged outputs, and the advanced position.

Head-of-line blocking is intended (FS §9.4): a failed delivery does not advance the position, so ordering is preserved and the coordinator retries under the standard policy. Because a new instance starts at position 0, historical facts are delivered in order without a replay mechanism.

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

Required measurements: claim rate and empty claims, handler duration, attempt outcomes split by operational interruption versus application failure, retry schedule depth, lease renewal and loss, **plan evaluation count, node count, and duration**, evaluations skipped, dependency resolutions per settle, `absent_reads` per execution, unclaimable backlog by `(name, version)`, notification latency versus poll, and per-execution lock wait.

Observer callbacks run outside transactions and locks; panics are recovered and cannot affect durable results.

## 21. Testing strategy

**Unit** — validation, state machines, retry decisions, clause satisfaction, canonical encoding stability, error classification, wake-hub generation logic.

**`flowtest`** — the public harness. A worker is exercised as a function; a plan is exercised as a pure function over `(args, facts, node states)` asserting declared nodes, clauses, and consulted reads. No database.

**Integration against real PostgreSQL** — hundreds of concurrent claimers; lease expiry and stale fencing; claim/cancel races; settle/cancel races; crash injection at every step of §10; publish-before-declare and declare-before-publish; repeated evaluation creating no duplicate commands; `absent_reads` blocking false success; fail-fast preserving `AfterFailed` branches; `Await` expiry; gap-free positions under concurrent ingress; coordinator head-of-line ordering; migration concurrency and checksums; rolling deployments with divergent registered versions.

**Property tests** — `unsatisfied_clauses` never negative; a command becomes `ready` at most once; positions are gap-free and strictly increasing per execution; `open_commands` equals the true non-terminal count after any operation sequence.

**Benchmarks** — claim throughput and plan cost at the 1,000-node ceiling; settle latency with fan-out; per-execution lock contention; transactional `NOTIFY` commit throughput versus a batched flusher versus poll-only; autovacuum and WAL behavior on `commands` and `events`.

## 22. Performance strategy

Correctness precedes throughput claims. Every hot query is checked with `EXPLAIN (ANALYZE, BUFFERS)` against representative distributions, including the adversarial claim case in §9.1. Index changes require write-amplification measurement, not only read plans.

`commands` and `events` are the churn-heavy tables and get explicit autovacuum settings in the initial migration, with the fillfactor and thresholds recorded in `components/schema.md`. Partitioning is deferred pending evidence; `events` and terminal executions are the first candidates.

No connection is held for handler duration. One optional listener connection plus short pool borrows support many worker goroutines.

## 23. Component responsibilities

**`components/schema.md`** — every table's DDL, constraints, and indexes; the claim, settle, resolution, and maintenance statements verbatim; autovacuum settings; index benchmark results including the `(name, version)` question from §9.1.

**`components/engine.md`** — plan snapshot construction and the `Plan` binding; the reconciliation and resolution algorithms as executable pseudocode; the failure closure computation; completion evaluation; coordinator inbox delivery; the plan-purity test matrix.

**`components/runtime.md`** — worker pool, capacity-aware dispatch, lease manager, notifier and wake hub, maintenance tasks, migration engine, error mapping tables, and the `flowtest` harness API.

## 24. References

- [`pgkit/v2`](https://github.com/goware/pgkit) · [`pgx/v5`](https://github.com/jackc/pgx) · [`google/uuid`](https://github.com/google/uuid)
- [PostgreSQL `SELECT`, row locks, `SKIP LOCKED`](https://www.postgresql.org/docs/15/sql-select.html) · [explicit locking](https://www.postgresql.org/docs/15/explicit-locking.html) · [`LISTEN`](https://www.postgresql.org/docs/15/sql-listen.html) · [`NOTIFY`](https://www.postgresql.org/docs/15/sql-notify.html) · [routine vacuuming](https://www.postgresql.org/docs/15/routine-vacuuming.html)
- [RFC 8785 — JSON Canonicalization Scheme](https://www.rfc-editor.org/rfc/rfc8785)
- [DBOS: Postgres LISTEN/NOTIFY scalability](https://www.dbos.dev/blog/postgres-listen-notify-scalability) · [HN discussion](https://news.ycombinator.com/item?id=49040296)
