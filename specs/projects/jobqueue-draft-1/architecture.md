---
status: complete
---

# Architecture: jobqueue

> **Superseded.** This is an earlier design for the same problem, structured as five layers — `MessageQueue`, `JobQueue`, Workflow DAG, `EventBus`, and `EventStore`. The active design lives in `specs/projects/flow` and uses a command / worker / event model with declarative plans instead. These documents are retained for their reasoning and their PostgreSQL mechanics, much of which carried forward; the APIs they describe are not current.

## 1. Purpose and Scope

This document defines the system-wide architecture for `github.com/goware/jobqueue`. It translates the completed project overview and functional specification into technical boundaries, data ownership, transaction rules, concurrency rules, package structure, and component interactions.

The project uses a two-level design:

- this document owns cross-component decisions and invariants;
- detailed component documents own exact public signatures, DDL, SQL, algorithms, and component test matrices.

Planned component documents:

1. `components/message_queue.md`;
2. `components/job_runtime.md`;
3. `components/workflow_engine.md`;
4. `components/event_bus.md`;
5. `components/event_store.md`;
6. `components/postgres_operations.md`.

No UI architecture is required.

## 2. Architecture Decisions

| Area | Decision | Rationale |
|---|---|---|
| Backend | PostgreSQL 15+ through `pgkit/v2` and `pgx/v5` | Matches the product constraint and enables atomic application/job transactions. |
| Delivery storage | Separate raw-message, job-dispatch, and subscription-delivery tables | Each layer enforces its own lifecycle without conditional cross-layer cleanup. |
| Public packages | Flat backend-neutral root package plus `postgres` implementation | Keeps application imports small and prevents backend details leaking into contracts. |
| IDs | UUIDv7 generated in Go, stored as PostgreSQL `uuid`, exposed as opaque typed strings | Provides distributed generation and index locality without coupling public contracts to a UUID package. |
| Queue coordination | Short `READ COMMITTED` transactions with `FOR UPDATE SKIP LOCKED` | Supports competing consumers without holding transactions during work. |
| Lease ownership | Fresh UUID lease token on every claim | Fences stale acknowledgements, renewals, failures, and completion. |
| Time | PostgreSQL `clock_timestamp()` for durable scheduling and lease decisions | Avoids worker clock skew. |
| Notifications | `LISTEN`/`NOTIFY` as advisory wake-up only | Polling remains the correctness path. |
| Job routing | Claim query filters by kinds registered in the current process | Makes heterogeneous rolling deployments safe. |
| Job dispatch | Reconstructable internal dispatch rows owned by `JobQueue` | Raw retention and dead-letter policy cannot strand or terminate jobs. |
| Finalization | Per-kind PostgreSQL finalizer invoked inside fenced completion | Keeps application-specific state updates typed and atomic without a giant switch. |
| Workflows | Materialized current state plus immutable operational history | Gives efficient inspection and durable audit without replaying arbitrary Go code. |
| Event positions | Transactional singleton allocator acquired late in append transactions | Provides a simple safe checkpoint order for the first event-store implementation. |
| Schema | Configurable schema, default `jobqueue`; all SQL fully qualified | Avoids `search_path` and PgBouncer session-state assumptions. |
| Migrations | Embedded, explicit, checksummed migrations | Supports library-driven or application-driven schema management. |

## 3. System Context

```text
Application producers/readers
          │
          ▼
┌──────────────────────────────────────────────────────────────┐
│ github.com/goware/jobqueue                                  │
│                                                             │
│ MessageQueue  JobQueue  Worker APIs  Workflow  Events       │
│ backend-neutral types, contracts, errors, helpers           │
└──────────────────────────────┬───────────────────────────────┘
                               │ implemented by
                               ▼
┌──────────────────────────────────────────────────────────────┐
│ github.com/goware/jobqueue/postgres                         │
│                                                             │
│ Backend · transactions · worker pool · migrations           │
│ notifier · dispatchers · lease managers · reconcilers       │
└──────────────────────────────┬───────────────────────────────┘
                               │
               pgkit/v2 queries and pgx/v5 sessions
                               │
                               ▼
┌──────────────────────────────────────────────────────────────┐
│ PostgreSQL                                                   │
│                                                             │
│ raw queue tables       job/workflow tables                   │
│ EventBus tables        EventStore tables                     │
│ migration metadata    worker capability registrations       │
└──────────────────────────────────────────────────────────────┘
```

PostgreSQL is the only durable authority. Local goroutines, channels, timers, notification payloads, caches, and worker registrations are accelerators or ephemeral coordination state.

## 4. Go Package Structure

```text
github.com/goware/jobqueue
├── errors.go
├── ids.go
├── message.go
├── queue.go
├── queue_admin.go
├── job.go
├── jobqueue.go
├── handler.go
├── outcome.go
├── retry.go
├── observer.go
├── workflow.go
├── graph.go
├── event.go
├── eventbus.go
├── eventstore.go
├── typed.go
├── jobqueuetest/
│   ├── messagequeue.go
│   └── jobqueue.go
├── internal/
│   ├── backoff/
│   ├── id/
│   └── lifecycle/
└── postgres/
    ├── backend.go
    ├── options.go
    ├── tx.go
    ├── migrate.go
    ├── worker_pool.go
    ├── finalizer.go
    ├── observer.go
    ├── migrations/
    └── internal/
        ├── rawqueue/
        ├── jobs/
        ├── workflow/
        ├── eventbus/
        ├── eventstore/
        ├── runtime/
        ├── maintenance/
        └── sqlutil/
```

Rules:

- The root package imports neither `pgkit` nor `pgx`.
- Only `postgres` is a public backend package initially.
- Internal PostgreSQL subpackages may import the root package but do not expose public contracts.
- `jobqueuetest` is public test-only support so future external backends can run the same conformance suites.
- Component-specific SQL lives with its PostgreSQL internal component, not in the root package.
- Common SQL helpers cover identifier qualification, error mapping, transaction retry, and row scanning; they do not create a generic repository abstraction.
- No package is created solely to hold one interface or one data type.

## 5. Dependencies

Initial direct dependencies:

- `github.com/goware/pgkit/v2` at the initial v2.9.0 baseline for application-aligned PostgreSQL querying and transaction-bound `DB.InTx` integration;
- `github.com/jackc/pgx/v5` at the v5.9.0 baseline used by pgkit v2.9.0 for transactions, batches, pool access, PostgreSQL errors, and dedicated `LISTEN` sessions;
- `github.com/google/uuid` v1.6.0 for UUIDv7 generation and parsing;
- the Go standard library for JSON, contexts, synchronization, embedded migrations, and structured logging adapters.

The repository currently targets Go 1.26.4. The code pins concrete module versions in `go.mod`; the architecture does not float dependencies at runtime. Dependency upgrades are separate reviewed changes with the full PostgreSQL test suite. New dependencies require a demonstrated reduction in complexity and a compatible license.

The project does not require an ORM, distributed-lock service, message broker, workflow framework, or mandatory observability SDK.

## 6. Identifier Design

Root types are opaque strings:

```go
type MessageID string
type LeaseID string
type DeadMessageID string
type JobID string
type AttemptID string
type WorkflowRunID string
type EventID string
type SubscriptionID string
type DeliveryID string
type ReplayID string
type NodeKey string

type StreamVersion int64
type GlobalPosition int64
```

PostgreSQL-backed entity IDs, including `DeadMessageID`, must parse as UUIDs and are stored in `uuid` columns. The backend generates UUIDv7 in Go before entering SQL so IDs are available for batch construction, logging, deterministic ordering of writes, and result mapping. `StreamVersion` and `GlobalPosition` are positive typed numeric cursors, not entity identities.

UUIDv7 is used for locality, not semantic ordering. APIs never promise that sorting IDs reproduces exact creation or commit order.

The following remain text because they are application-defined names rather than entity identities:

- queue and job-lane names (`QueueName`);
- job kinds;
- topic names (`TopicName`) and subscription names;
- stream IDs (`StreamID`);
- node keys (`NodeKey`), spawn keys, uniqueness keys, and correlation IDs.

Deterministic orchestration uses database uniqueness constraints such as `(parent_job_id, spawn_key)` and `(workflow_run_id, node_key)`. It does not derive deterministic UUIDs. On conflict, the transaction reads and returns the existing entity after verifying immutable fields.

## 7. PostgreSQL Namespace and Storage Boundaries

### 7.1 Schema

The default schema is `jobqueue`. A validated schema option is resolved when constructing the backend and when rendering migrations.

Every generated statement uses a safely quoted, fully qualified identifier. Runtime behavior never depends on `search_path`.

### 7.2 Table groups

The schema is separated by lifecycle ownership:

| Owner | Primary tables | Lifecycle authority |
|---|---|---|
| Raw MessageQueue | `raw_queues`, `raw_messages`, `raw_dead_messages` | Raw retention and maximum deliveries |
| JobQueue | `job_lanes`, `jobs`, `job_dispatches`, `job_attempts`, `job_admin_events` | Job state and retry policy |
| Worker runtime | `worker_registrations`, `worker_capabilities` | Expiring process capability leases |
| Workflow | `workflow_runs`, `workflow_nodes`, `workflow_dependencies`, `workflow_events` | Workflow and node policy |
| EventBus | `bus_topics`, `bus_subscriptions`, `bus_events`, `subscription_deliveries`, `subscription_dead_deliveries`, `subscription_replays` | Subscription policy and bus retention |
| EventStore | `event_streams`, `event_store_events`, `event_global_allocator`, `event_projection_checkpoints` | Stream append and event retention |
| Operations | `schema_migrations` | Migration engine |

Separate tables are intentional even where lease columns look similar. Shared Go helpers may implement equivalent encoding and validation, but each component owns its SQL and constraints.

### 7.3 Cross-group references

Permitted references:

- `job_dispatches.job_id → jobs.id` with one dispatch row per dispatchable job;
- `job_attempts.job_id → jobs.id`;
- workflow nodes reference their job and run;
- workflow dependencies reference nodes in one run;
- subscription deliveries reference one immutable bus event and subscription;
- bus events optionally reference an originating stored stream event;
- operational history references its owning aggregate.

Raw messages never reference or contain managed job or subscription delivery rows. Public raw queue operations therefore cannot address managed work even if names happen to match.

### 7.4 Hot and historical data

Active raw messages, job dispatches, and subscription deliveries remain narrow hot tables. Job records, attempts, dead deliveries, workflow events, and stream events retain history separately.

Large payloads are stored once and dispatch rows contain compact references. Initial payload limits remain those in the functional specification.

Partitioning is deferred. Each table begins unpartitioned with explicit autovacuum settings and benchmark-driven indexes. Historical tables are the first partitioning candidates if measurements justify it.

## 8. PostgreSQL and pgkit Integration

### 8.1 Backend

```go
type Backend struct {
    db     *pgkit.DB
    config Config
}

func New(db *pgkit.DB, opts ...Option) (*Backend, error)
```

`New` validates configuration and schema compatibility but does not run migrations implicitly.

### 8.2 Transaction binding

The PostgreSQL package exposes transaction binding:

```go
func (b *Backend) InTx(tx pgx.Tx) *Backend
```

The clone calls `b.db.InTx(tx)`, retaining the pool and SQL builder while routing `Query` through the supplied `pgx.Tx`. This matches the current `pgkit/v2` transaction model.

Application-owned composition uses:

```go
err := pgx.BeginFunc(ctx, db.Conn, func(tx pgx.Tx) error {
    txdb := db.InTx(tx)
    txbackend := backend.InTx(tx)

    if err := updateApplicationState(ctx, txdb); err != nil {
        return err
    }
    _, err := txbackend.Publish(ctx, request)
    return err
})
```

The root package has no transaction-binding API because a portable transaction type does not exist.

When one caller transaction mixes raw publication, job enqueue, EventBus publication, and EventStore append, `postgres.Backend.ExecuteAtomic` buffers those root requests and executes them in the global lock order. Standalone component methods use the same ordered executor for their subset. This is the preferred composition surface after application-owned table updates.

### 8.3 Transaction ownership

There are three transaction modes:

1. **Backend-owned:** one public call opens, commits, and conditionally retries its short transaction.
2. **Worker completion-owned:** the runtime controls the entire fenced completion transaction and may safely retry database-only work.
3. **Caller-owned:** `InTx` participates in the caller's transaction and never commits or automatically retries it.

Methods bound to a caller transaction must not start nested transactions.

## 9. Transaction and Locking Rules

### 9.1 Isolation

Use `READ COMMITTED` initially. Correctness comes from row locks, lease fences, state predicates, and unique constraints rather than stronger global isolation.

Serialization failures and deadlocks remain possible. Backend-owned idempotent transactions may retry SQLSTATE `40001` and `40P01` with a small bounded jitter. Caller-owned transactions return the error to the caller.

### 9.2 Database time

Durable timestamps use PostgreSQL time:

- `clock_timestamp()` for actual lease, availability, expiry, and retry decisions;
- transaction timestamps only where one stable time for the whole transaction is intended.

Go time controls local waits and context deadlines but never decides durable lease ownership.

### 9.3 Global lock order

Transactions acquire locks in this order when applicable:

1. workflow-run rows for operations participating in a workflow;
2. job rows in stable ID order;
3. related dispatch, attempt, node, and dependency rows in stable ID/key order;
4. EventBus topic and subscription routing rows in topic/UUID order;
5. event stream rows in lexical stream-ID order;
6. singleton global event allocator row;
7. inserts into immutable event/history/delivery tables;
8. notifications, then commit.

The application finalizer runs before EventBus routing rows, event stream locks, and the global allocator. No user callback runs after routing/stream lock preparation begins or after the allocator is acquired.

Batch operations sort database writes by generated/stable ID or uniqueness key to reduce unique-index deadlocks, while returning results in original request order.

The claim path is the deliberate exception to the blocking lock order: it requests both job and dispatch locks with `SKIP LOCKED`, never waits for a candidate row, and skips the whole candidate if either lock is unavailable.

### 9.4 Transaction duration

Claims, settlement, heartbeat, finalization, reconciliation, and maintenance transactions perform bounded database work and then commit. Handlers never run inside them.

Large fan-out and append batches have configurable limits so a single completion cannot hold locks or generate WAL without bound.

## 10. Raw MessageQueue Architecture

### 10.1 Eligibility representation

`raw_messages` uses one indexed `visible_at` column:

- initial schedule time before first claim;
- lease expiry while leased;
- release/retry time after a negative settlement.

Retention expiration is stored separately and is based on first availability plus configured retention.

### 10.2 Claim

One data-modifying statement:

1. selects eligible IDs from one raw queue ordered by priority, visibility, and ID;
2. locks candidates with `FOR UPDATE SKIP LOCKED` and a bounded limit;
3. updates each with a new UUIDv7 lease ID, database timestamps, and incremented receive count;
4. returns complete deliveries;
5. commits immediately.

The component design specifies exact SQL and indexes.

### 10.3 Settlement

Acknowledgement deletes by `(message_id, lease_id)`. Release and extension update by the same pair and require a still-current lease. Zero affected rows maps to `ErrLeaseLost`.

Raw dead-letter movement and retention cleanup operate only on raw tables.

## 11. JobQueue Architecture

### 11.1 Enqueue

Single enqueue and atomic batch enqueue:

1. validate and normalize requests;
2. generate UUIDv7 IDs for omitted identities;
3. order writes deterministically;
4. insert jobs under stable ID and active uniqueness constraints;
5. verify immutable fields when a conflict represents an idempotent replay;
6. insert one `job_dispatches` row for every non-blocked, non-terminal job;
7. notify affected lanes after rows are durable in the transaction;
8. commit and restore result order.

`job_dispatches.visible_at` represents initial availability, retry time, or current lease expiry. It has no retention deadline or maximum-delivery terminal policy.

### 11.2 Registered-kind claim

Each worker dispatcher maintains the immutable registered-kind set captured when the pool starts. A claim request includes:

- lane;
- registered kinds;
- immediately available local capacity;
- visibility duration;
- process and worker identity.

The claim query joins `job_dispatches` to `jobs`, filters `jobs.kind = ANY($registeredKinds)`, and requests `FOR UPDATE OF j, d SKIP LOCKED`. It acquires both job and dispatch locks without waiting or skips the whole candidate, then in one transaction:

- verifies the job is dispatchable and not past `ExpiresAt`;
- reconciles any previous expired lease/attempt;
- assigns a fresh lease and attempt ID;
- creates the attempt;
- marks the job running under that fence;
- returns the job delivery.

An empty registered-kind set is rejected before SQL. Unknown-kind handling after claim remains a defensive fallback for registry mutation, corrupted envelopes, or implementation defects; it releases without consuming retry budget.

### 11.3 Worker capability registration

Each process writes expiring capability registrations for its `(process, lane, kind)` combinations. A heartbeat renews registration expiry using database time. Shutdown removes registrations best-effort; expiry handles crashes.

Capability rows are observational, not required for claim correctness. They enable queries and metrics for job kinds with backlog but no live capable process.

### 11.4 Managed dispatch reconciliation

The invariant is:

> Every non-terminal dispatchable job has recoverable dispatch state; no terminal or blocked job has claimable dispatch state.

A bounded, idempotent reconciler:

- inserts missing dispatch rows for eligible `available` or `retrying` jobs with `ON CONFLICT DO NOTHING`;
- deletes dispatch rows for terminal or blocked jobs;
- expires jobs whose start deadline has passed;
- repairs job/dispatch fence disagreement only when database predicates prove no current valid owner;
- records repair counts and anomalies.

Normal state transitions maintain the invariant synchronously. Reconciliation repairs crashes, manual database damage, or bugs; it is not the ordinary scheduling path.

### 11.5 Handler execution and outcome

Handlers run outside database transactions with a context carrying job identity, attempt identity, metadata, and a mutable in-memory outcome builder.

The builder accepts serializable operations only:

- JSON result;
- deterministic child job requests with spawn keys;
- future workflow mutations;
- future event append/publication operations.

No database write occurs when adding an outcome operation.

### 11.6 Per-kind finalizers

The PostgreSQL worker pool registers handlers as:

```go
workers.Handle(
    kind,
    handler,
    postgres.WithFinalizer(finalizer),
    postgres.WithRetryPolicy(policy),
    postgres.WithHandlerTimeout(timeout),
)
```

The exact option types are specified in the job runtime component design. A finalizer has access to a transaction-bound `*pgkit.DB`, the immutable job/attempt snapshot, and the validated buffered outcome.

Finalizers:

- are optional per kind;
- execute only after the handler returns success;
- run inside the completion transaction after fence validation;
- may update application tables in the same PostgreSQL database;
- must be short and free of external network calls;
- may be retried if the entire database transaction rolls back with a retryable database error.

Optional pool-wide before/after database hooks may add shared metadata or observations but cannot replace the per-kind finalizer.

### 11.7 Completion

The runtime opens a short transaction and:

1. locks the workflow run when applicable, then the job and current attempt;
2. verifies running state, attempt ID, lease ID, cancellation state, and unexpired current ownership;
3. validates the outcome and deterministic keys;
4. invokes the per-kind finalizer;
5. materializes child jobs and future workflow/event operations;
6. writes result and immutable operational history;
7. marks attempt and job succeeded;
8. deletes managed dispatch;
9. notifies newly eligible lanes;
10. commits.

If the fence check fails, no finalizer or outcome operation runs. If commit succeeds but the response is lost, the terminal job row and uniqueness constraints make the persisted result authoritative.

### 11.8 Failure, interruption, and retry budget

Application failure and panic consume retry budget. Shutdown interruption, unknown-handler deferral, and lease loss do not.

Every invoked execution may have an attempt record with `consumes_retry_budget`. Failure processing locks and fences the attempt, then either reschedules `job_dispatches.visible_at` or makes the job terminal.

No raw dead-letter operation applies to job dispatch. Terminal failure diagnostics remain in job and attempt history.

### 11.9 Cancellation and expiration

Cancellation and success race on the locked job row. The first terminal transition wins. Cancellation invalidates the current completion fence before signalling the local handler context.

`ExpiresAt` is checked with database time before attempt creation. Scheduled state remains derived from `state` plus `available_at`; no scheduler writes a state transition merely because time passed.

## 12. Worker Runtime Topology

```text
PostgreSQL
    │
    ├── dedicated LISTEN session (optional)
    │         │
    │      Notifier ──────┐
    │                     ▼
    │                local WakeHub
    │                     │
    │            capacity-aware dispatchers
    │                     │
    │                worker slots
    │                     │
    │             handler execution
    │                     │
    ├──────────── completion/failure transactions
    │
    ├──────────── batched LeaseManager
    │
    └──────────── capability heartbeat + reconcilers
```

### 12.1 Notifier

The notifier owns one session-persistent `pgx.Conn` per process, executes `LISTEN`, reconnects with backoff, and passes compact namespace/name hints to the local wake hub.

The notification channel is a fixed-length identifier derived from a hash of the configured schema, for example `jq_<schema-hash>_wake`. It is safely quoted for `LISTEN`; publishers pass the same channel as a parameter to `pg_notify`. This isolates multiple jobqueue schemas in one database without relying on `search_path` or embedding untrusted identifiers.

After connecting, it commits `LISTEN`, performs a catch-up wake for every bound lane/queue/subscription, and only then relies on future notifications. This closes the documented listener-start race.

A notification connector option supports a direct PostgreSQL endpoint when ordinary queries use PgBouncer transaction pooling. Without a session-capable connector, the runtime uses poll-only mode.

Transactional `NOTIFY` has a known write-scalability property: to preserve commit-order delivery, PostgreSQL takes a global lock over its notification queue from the start of commit through `fsync` for every notifying transaction, serializing those commits and defeating group commit (see the DBOS analysis and its discussion in §24). Because nearly every publish, enqueue, and completion transaction in this design notifies, this lock is a potential system-wide commit ceiling and must be benchmarked early. Exposure is already bounded — hints are coalesced to at most one per affected owner per transaction, notifications are advisory, and poll-only mode is fully correct. If notify-at-commit serialization limits hot-path throughput, the sanctioned evolution is an optional decoupled hint flusher that buffers wake hints in memory and emits them in small separate transactions, accepting slightly higher wake latency; fallback polling already covers hint loss, and PostgreSQL row state remains the only source of truth.

### 12.2 WakeHub and dispatchers

WakeHub coalesces duplicate hints and never stores durable work. Dispatchers wake because of notification, next-visible timer, fallback poll, capacity change, or shutdown.

Each dispatcher claims at most immediately free slots. Local handoff channels are unbuffered or minimally buffered.

### 12.3 LeaseManager

One lease manager per process tracks active raw, job, and subscription receipts in memory, grouped by owner and renewal duration. It uses owner-specific batch extension queries.

Loss of one receipt cancels only its handler/subscriber context. The manager does not attempt to recover an expired lease.

### 12.4 Maintenance

Maintenance roles include raw retention/DLQ movement, EventBus retention, history cleanup, capability expiry, and job-dispatch reconciliation.

Each task claims bounded rows with `SKIP LOCKED`, making duplicate maintenance runners safe. A transaction-scoped advisory lock may be used only for singleton operations such as migrations; ordinary cleanup does not depend on global leadership.

## 13. Workflow Architecture

Workflow runs, nodes, dependencies, and operational events are stored separately from standalone jobs while nodes reference their corresponding jobs.

### 13.1 Static creation

The Go graph builder validates unique node keys, dependency references, conditions, and cycles. The backend repeats critical uniqueness and self-edge checks with constraints.

One transaction creates the run, nodes/jobs, dependency edges, operational start events, and dispatch rows only for zero-dependency nodes.

### 13.2 Completion and dependency resolution

Workflow-aware job completion follows the global order by locking the workflow run before the job, resolves matching dependency edges, decrements readiness counters with guarded predicates, and creates dispatch for nodes whose count reaches zero.

Unique `(workflow_run_id, node_key)` and dependency primary keys make replayed mutations idempotent.

### 13.3 Dynamic mutation

Dynamic operations are buffered in the job outcome. Initial APIs can only add new nodes and edges pointing toward newly created nodes, preventing cycles structurally.

Fan-out and joins are syntax over node and dependency operations. Large mutations are rejected above configured limits before the completion transaction begins.

### 13.4 Workflow history

Workflow operational events use a per-run sequence allocated while the workflow run row is locked. This does not use the global domain event allocator.

Current run/node tables are query projections; operational events provide audit history. The system never reconstructs execution by replaying arbitrary handler code.

## 14. EventBus Architecture

`bus_events` stores one immutable payload. Publication selects matching active subscriptions and inserts one `subscription_deliveries` reference per subscription in the same transaction.

Uniqueness on event ID provides idempotent publication. Uniqueness on `(subscription_id, event_id)` prevents duplicate fan-out after retry or commit uncertainty.

Subscription deliveries have their own lease, retry, retention, and dead-letter columns. They never share raw-message or job-dispatch lifecycle tables.

Cleanup can delete a bus event only after its minimum retention and when no active or retained dead delivery references it. Foreign keys use restrictive deletion so a cleanup bug cannot orphan a delivery payload.

When a stored stream event is published, `bus_events` can reference the stream event and use the same canonical payload rather than copy JSON. Standalone bus events retain payload in `bus_events`. The event component designs define the exact envelope and constraints.

## 15. EventStore Architecture

### 15.1 Stream concurrency

`event_streams` holds the current version for each text stream ID. Append locks or atomically creates the stream row, verifies `expectedVersion`, and reserves a contiguous per-stream version range.

Multiple streams appended in one transaction are locked in lexical stream-ID order.

### 15.2 Initial global allocator

`event_global_allocator` contains exactly one row with `next_position`.

After all application finalizers, aggregate locks, stream locks, validation, and fallible preparation are complete, append allocates a block:

```sql
UPDATE jobqueue.event_global_allocator
SET next_position = next_position + $1
WHERE singleton = true
RETURNING next_position - $1 AS first_position;
```

The row update is transactional:

- rollback restores the allocator value, so rolled-back appends do not permanently consume positions;
- a concurrent allocator waits until the current holder commits or rolls back;
- a later allocation cannot commit before the allocation it waited on;
- events in one append receive consecutive global positions;
- the safe checkpoint is the greatest committed allocated position returned to readers.

The allocator is deliberately acquired late and no user callback runs afterward. This briefly serializes global event append commits but gives simple checkpoint correctness.

Application-owned transactions that call append should do so near commit; holding the transaction open after allocation preserves correctness but reduces global append throughput. Metrics report allocator wait and lock-hold duration.

### 15.3 Projection reads

Global reads order by position and return a safe checkpoint with the page. A projection persists its consumer name and last applied checkpoint atomically with its own projection updates when both share PostgreSQL.

Per-stream reads order by stream version. Global position is an ordering cursor, not a wall-clock timestamp.

### 15.4 Possible future concurrent allocator

If benchmarks show the singleton row is an unacceptable bottleneck, a future design may use concurrent position reservations plus a safe-watermark protocol. That design must preserve these proofs before adoption:

1. every reserved range is durably classified as committed or abandoned;
2. a reader never advances beyond the earliest unresolved reservation;
3. crashed or rolled-back reservations become provably skippable without relying only on elapsed wall time;
4. event rows and committed reservation state cannot disagree;
5. cleanup cannot turn a temporarily unresolved range into a permanently skipped committed event;
6. projection restart produces the same safe sequence.

Candidate mechanisms to investigate:

- a durable reservation table with explicit states and PostgreSQL transaction-status evidence;
- sequence-allocated ranges plus an independently committed reservation registry and conservative watermark;
- a commit-ordered feed derived from logical decoding/WAL LSN for deployments willing to operate replication slots;
- grouped allocation that amortizes the singleton lock while preserving one total order.

Sharded allocators alone are not equivalent because they do not provide one total global order. This future work requires a benchmark, failure model, design review, and migration plan; it is not an implementation escape hatch for milestone 3.

## 16. Public Interface Boundaries

The root package exposes narrow capability interfaces:

```go
type QueuePublisher interface { Publish(context.Context, PublishRequest) (PublishResult, error) }
type QueueReceiver interface { Receive(context.Context, ReceiveRequest) ([]Delivery, error) }
type QueueSettler interface {
    Ack(context.Context, Receipt) error
    Nack(context.Context, Receipt, NackOptions) error
    Extend(context.Context, Receipt, time.Duration) (Lease, error)
}

type JobEnqueuer interface {
    Enqueue(context.Context, EnqueueRequest) (EnqueueResult, error)
    EnqueueBatch(context.Context, []EnqueueRequest) ([]EnqueueResult, error)
}
type JobReader interface { /* GetJob and bounded ListJobs */ }
type JobController interface { /* CancelJob and RetryJob */ }

type EventPublisher interface { /* PublishEvent */ }
type EventAppender interface { /* Append with expected version */ }
```

Detailed signatures, options, filter structures, and result types belong to component designs. Cross-cutting interface rules:

- every blocking call accepts `context.Context` first;
- request structs are used where parameters are expected to grow;
- zero values either have a documented default or fail validation;
- list APIs require limits and stable cursor pagination;
- batch results preserve input order;
- typed helpers wrap rather than replace JSON contracts;
- implementation structs can expose optional performance/admin capabilities without expanding base interfaces.

## 17. Errors and Retry Strategy

Root sentinel categories support `errors.Is`; typed errors carry safe structured context.

```go
type ErrorCode string

const (
    CodeNotFound       ErrorCode = "not_found"
    CodeConflict       ErrorCode = "conflict"
    CodeInvalid        ErrorCode = "invalid"
    CodeInvalidState   ErrorCode = "invalid_state"
    CodeLeaseLost      ErrorCode = "lease_lost"
    CodeRemoved        ErrorCode = "removed"
    CodeTerminal       ErrorCode = "terminal"
    CodeClosed         ErrorCode = "closed"
    CodeUnavailable    ErrorCode = "unavailable"
    CodePayloadTooLarge ErrorCode = "payload_too_large"
    CodeTransient      ErrorCode = "transient"
    CodeUncertainCommit ErrorCode = "uncertain_commit"
    CodeSchema         ErrorCode = "schema"
)

var (
    ErrNotFound       error
    ErrConflict       error
    ErrInvalid        error
    ErrInvalidState   error
    ErrLeaseLost      error
    ErrRemoved        error
    ErrTerminal       error
    ErrClosed         error
    ErrUnavailable    error
    ErrPayloadTooLarge error
    ErrTransient      error
    ErrUncertainCommit error
    ErrSchema         error
)

type Error struct {
    Code      ErrorCode
    Operation string
    Resource  string
    ID        string
    Field     string
    Reason    string
    Expected  string
    Actual    string
    Cause     error
}

func (e *Error) Error() string
func (e *Error) Unwrap() error
func (e *Error) Is(error) bool
```

Every sentinel is a stable category, not a separate resource-specific error. For example, a missing queue and a missing job both match `ErrNotFound`; callers use `errors.As` to inspect `Error.Resource`. `Expected` and `Actual` are populated only with safe values such as numeric versions or normalized configuration—not payload data.

The PostgreSQL adapter maps by SQLSTATE and constraint name, never by message text. It retains the underlying `*pgconn.PgError` through wrapping.

Error classes:

- validation and incompatible configuration: caller-fatal;
- not found and terminal conflict: operation-fatal;
- duplicate matching identity: successful idempotent result;
- duplicate conflicting identity: `ErrConflict`;
- lease loss: ownership-fatal for the current attempt;
- handler unregistered locally: infrastructure deferral;
- connection, serialization, deadlock, and transient resource errors: bounded retry where transaction ownership permits;
- migration checksum/version mismatch: startup-fatal;
- handler error classifications: job-policy input, not database errors.

Errors and observations include entity IDs but never arbitrary payload bodies or unredacted database credentials.

## 18. Migrations and Compatibility

SQL migrations are embedded with `go:embed` and exposed as an `fs.FS` for external migration tools.

`postgres.Migrate(ctx, db, opts...)`:

1. validates and quotes the schema;
2. obtains a transaction-scoped advisory migration lock;
3. creates migration metadata if absent;
4. verifies applied checksums;
5. applies pending migrations in order;
6. records version, checksum, library version, and database time;
7. commits each documented migration unit.

Backend startup checks schema compatibility without mutating it. Rolling upgrades require envelope readers to accept the current and previous supported versions. Destructive column removal uses expand/migrate/contract across releases.

Migration component tests cover clean install, repeated install, checksum mismatch, concurrent migrators, upgrade from every supported released schema, and caller-provided schema names.

## 19. Observability

The root package defines one optional observer contract. The default observer is a no-op.

```go
type Observation struct {
    Component string
    Operation string
    Outcome   string
    ErrorCode ErrorCode

    Namespace      string
    Queue          QueueName
    Kind           string
    MessageID      MessageID
    LeaseID        LeaseID
    JobID          JobID
    AttemptID      AttemptID
    WorkflowID     WorkflowRunID
    NodeKey        NodeKey
    EventID        EventID
    SubscriptionID SubscriptionID
    StreamID       StreamID

    Count      int64
    Duration   time.Duration
    OccurredAt time.Time
    Attributes map[string]string
}

type Observer interface {
    Observe(context.Context, Observation)
}

type ObserverFunc func(context.Context, Observation)
func (f ObserverFunc) Observe(ctx context.Context, o Observation) { f(ctx, o) }
```

The library constructs observations and defensively copies `Attributes`; adapters treat values as immutable. `Component`, `Operation`, and `Outcome` use documented low-cardinality constants in code even though their public representation is string-based for adapter compatibility.

Required dimensions where applicable:

- namespace, queue/lane/subscription;
- message, lease, job, attempt, workflow, node, event, stream;
- handler kind and build version;
- worker and process identity;
- correlation and causation IDs;
- outcome/error category without payload data.

Required operational measurements include:

- publish/claim/settle/finalize counts and latency;
- available, scheduled, running, leased, retrying, dead, and expired counts;
- lease renewal and loss;
- long-running duration and last heartbeat;
- unhandled backlog by lane and kind;
- listener reconnects, fallback polls, and empty claims;
- reconciliation repairs and anomalies;
- transaction retries and deadlocks;
- global event allocator wait and hold time;
- projection lag by safe checkpoint.

Backend-owned and worker-owned transactional code buffers observations and emits them only after commit or rollback is known. Caller-owned composition gets the same guarantee through PostgreSQL `TxBinding`/`Transact`; bare `InTx` suppresses commit-dependent observations because a `pgx.Tx` exposes no later commit hook. Observer callbacks run outside database transactions and critical local locks and must not be allowed to break queue correctness. Logging, metrics, and tracing adapters consume observations asynchronously or with bounded cost.

## 20. Security and Resource Safety

- Validate identifiers, payload sizes, headers, batch sizes, graph sizes, and durations before SQL.
- Quote configurable schema identifiers; parameterize all data.
- Fully qualify tables and derive/quote fixed notification channels; do not rely on mutable session state.
- Bound stored errors and redact through configured hooks.
- Do not log payloads by default.
- Keep handlers and external calls outside transactions.
- Reject unbounded list operations.
- Use context deadlines for queries, listener reconnect, shutdown, and migrations.
- Keep notification payloads to compact routing hints; never include job/event bodies.
- Treat finalizers as trusted application code but measure and document their lock duration.
- Do not use session advisory locks to represent work leases.

Hostile multi-tenant isolation and row-level security remain future designs.

## 21. Testing Architecture

### 21.1 Unit tests

Use Go's `testing` package for validation, state-machine helpers, retry decisions, typed encoding, graph validation, backoff, observer behavior, and error classification.

Tests use deterministic injected local clocks only for local runtime behavior. Database-time behavior is tested against PostgreSQL.

### 21.2 Conformance tests

The root package provides reusable MessageQueue and JobQueue behavior suites. Backend factories create isolated schemas and return cleanup functions.

### 21.3 PostgreSQL integration tests

Integration tests use real supported PostgreSQL versions and exercise:

- hundreds of concurrent claimers;
- lease expiry, renewal, and stale fences;
- registered-kind filtering during rolling deployment;
- missing dispatch reconciliation;
- cancellation/completion races;
- batch enqueue conflict rollback and result order;
- same-database application finalization;
- listener loss and poll recovery;
- workflow fan-out/join crash recovery;
- EventBus referential retention;
- stream version conflicts and global allocator ordering;
- transaction deadlock/serialization retry;
- migration concurrency and checksums.

Mocks do not substitute for PostgreSQL concurrency tests.

### 21.4 Fault injection

Named fault points exist before and after handler return, finalizer, outcome materialization, terminal update, notification, and commit response. Tests restart runtimes and assert durable invariants rather than specific goroutine timing.

### 21.5 Race and fuzz testing

Run `go test -race` for local runtime packages. Fuzz request validation, envelope decoding, graph operations, error redaction, and retry classification. Property tests assert legal state transitions, nonnegative dependency counters, and idempotent deterministic operations.

## 22. Performance Strategy

Initial correctness targets precede throughput claims.

Benchmarks separately measure:

- raw claim/settlement throughput;
- job claim plus attempt creation;
- lease renewal batches;
- notification latency versus poll-only mode;
- commit-throughput impact of transactional `pg_notify` at high publish/enqueue/completion rates, compared against a decoupled batched hint flusher and poll-only mode;
- completion with child fan-out;
- workflow dependency resolution;
- EventBus fan-out;
- EventStore append under allocator contention;
- reconciliation at different orphan rates;
- autovacuum/WAL behavior at realistic table sizes.

Every hot query is checked with `EXPLAIN (ANALYZE, BUFFERS)` against representative available/leased/scheduled distributions. Index changes require write-amplification measurements as well as read plans.

No connection is held for handler duration. One optional persistent listener connection and short pool borrows support many worker goroutines.

## 23. Component Design Responsibilities

The component documents must resolve the following without changing cross-cutting decisions here:

### MessageQueue

- exact types and public APIs;
- raw table DDL, indexes, SQL, retention, DLQ, redrive, and conformance cases.

### Job runtime

- exact job/attempt/dispatch/capability DDL;
- enqueue, claim, heartbeat, completion, failure, cancellation, reconciliation SQL;
- handler, outcome, per-kind finalizer, worker pool, and typed helper APIs;
- state-transition matrix and retry-budget accounting.

### Workflow engine

- exact run/node/dependency/history DDL;
- graph builder and mutation API;
- dependency resolution, joins, cancellation, and versioning algorithms.

### EventBus

- topic/subscription/event/delivery DDL;
- filters, publication, fan-out, settlement, dead-letter, redrive, and retention.

### EventStore

- stream/event/allocator/checkpoint DDL;
- append and global-read SQL;
- allocator correctness proof, transaction ordering, and projection APIs;
- recorded future concurrent-watermark research notes.

### PostgreSQL operations

- backend/options/transaction APIs;
- listener connector and poll-only behavior;
- WakeHub, dispatcher, LeaseManager, maintenance, migrations, error mapping, and operational testing.

## 24. Primary Technical References

- [`pgkit/v2` source and transaction-bound `DB.InTx`](https://github.com/goware/pgkit)
- [`pgx/v5` PostgreSQL driver and toolkit](https://github.com/jackc/pgx)
- [PostgreSQL 15 `SELECT`, row locks, and `SKIP LOCKED`](https://www.postgresql.org/docs/15/sql-select.html)
- [PostgreSQL 15 explicit and advisory locking](https://www.postgresql.org/docs/15/explicit-locking.html)
- [PostgreSQL `LISTEN`](https://www.postgresql.org/docs/15/sql-listen.html)
- [PostgreSQL `NOTIFY`](https://www.postgresql.org/docs/15/sql-notify.html)
- [DBOS: Postgres LISTEN/NOTIFY scalability — the global notification-queue lock serializes notifying commits; batched notification flushing plus fallback polling raised throughput ~20x](https://www.dbos.dev/blog/postgres-listen-notify-scalability)
- [Hacker News discussion of the DBOS analysis — hardware caveats, burst-load behavior, the 8 KB payload limit, and alternatives such as logical replication and plain polling](https://news.ycombinator.com/item?id=49040296)
- [PostgreSQL routine vacuuming](https://www.postgresql.org/docs/15/routine-vacuuming.html)
- [`google/uuid` UUID implementation](https://github.com/google/uuid)
