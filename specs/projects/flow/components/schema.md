---
status: complete
---

# Component: PostgreSQL storage and journal

## 1. Purpose

The store owns Flow's durable authority: migrations, constraints, indexes, ordered semantic transactions, queue materialization, graph/wait rows, fencing, idempotency, journal append, projections, and inspection queries.

All tables use the `flow_` prefix and live in the application's PostgreSQL database/schema.

## 2. Storage rules

1. Every semantic mutation belongs to exactly one execution and locks its execution row first.
2. Journal positions are allocated under that lock and are gap-free/commit-ordered per execution.
3. Mutable projections and immutable journal rows commit together.
4. Queue/lease churn is isolated from immutable command identity and results.
5. Claims use only no-wait `SKIP LOCKED` locks.
6. All durable times come from PostgreSQL after required locks.
7. Canonical bodies and hashes make retries comparable without application callbacks.
8. Constraints reject impossible local row shapes; store code validates cross-row invariants.

## 3. Table inventory

| Table | Responsibility |
|---|---|
| `flow_executions` | execution identity, mode, counters, deadline, plan dirty/lease fields, terminal state |
| `flow_commands` | immutable command identity/config plus current lifecycle/result/fence state |
| `flow_command_queue` | hot claim projection: eligible time, priority, queue, lease locality fields |
| `flow_command_dependency_groups` | normalized all-succeeded/all-settled/all-failed groups |
| `flow_command_dependency_members` | predecessor membership and reverse lookup |
| `flow_command_event_waits` | exact application-event waits and durable wait deadlines |
| `flow_coordinators` | durable state, inbox, lease, persisted delivery retry policy/state |
| `flow_journal` | immutable ordered execution history |
| `flow_schema_migrations` | embedded migration ledger |

No table is merged merely to reduce count: their access patterns, mutation rates, and indexes differ materially.

## 4. Execution rows

Important fields:

- UUID primary key;
- driver mode `direct|plan|coordinator`;
- definition name/version and canonical start input;
- stable/live key scope and start fingerprint;
- status `running|failing|succeeded|failed|cancelled|expired`;
- fail-fast, deadline, metadata, max/accepted command count;
- open/required-failed/waiting counters;
- next journal position;
- terminal journal position and timestamps;
- plan dirty, dirty-since, quiescent, reconciliation revision/lease fields;
- created/updated PostgreSQL times.

There is no `outcome_ref`. Typed final command results and application tables own result references.

Stable start uniqueness is `(driver,name,key)` for non-empty stable keys plus start-fingerprint equivalence. Live keys use a partial live-row ownership relation released on terminality. Empty keys create distinct executions.

Terminal rows satisfy terminal-position/time and clean-plan constraints. Counters are non-negative. Plan fields are null/default for other modes.

## 5. Command rows

Immutable fields include execution ID, command ID/key, name/version, origin, parent, required flag, canonical arguments/hash, accepted retry policy/hash, timeout, queue, creation position/time, and initial schedule/budget anchor.

Mutable fields include lifecycle state, result/failure, invocation and consumed-attempt counts, next-attempt time, running attempt ID/token/start/lease expiry, child-membership closure, terminal position/time, and update time.

Origins are:

- `direct_root`;
- `plan`;
- `worker_child`;
- `coordinator_command`.

No `external_issue` origin exists. One `(execution_id,command_key)` is unique. One terminal position per command is unique and references its terminal journal subject logically.

The accepted policy includes fixed effective jitter and never changes because a replica redeploys. `BudgetStartedAt` is immutable once first eligible. `NextAttemptAt` may move.

## 6. Command queue

One queue row exists only while a command is claimable now or later. It stores command/execution IDs, queue name, priority, eligible time, and optional affinity hint. It does not duplicate results, dependencies, or event selectors.

The claim index begins with queue and eligible time and includes a stable tie-breaker. Partial predicates exclude terminal/running commands through maintained row existence/state rather than volatile expressions.

Queue creation/deletion always occurs in the same semantic transaction as the lifecycle change that makes it necessary/unnecessary.

## 7. Dependency groups and members

Group rows contain command ID, stable group ordinal, and kind restricted to:

- `all_succeeded`;
- `all_settled`;
- `all_failed`.

There is no threshold column or quorum kind.

Member rows map each group to predecessor command ID/key. Unique membership prevents duplicate edges. Reverse index `(predecessor_command_id,group_id)` supports terminal resolution; `(command_id,group_id)` supports trace/snapshot loads.

Unknown forward references are resolved within one accepted plan batch before insertion. Missing references reject the entire reconciliation.

## 8. Exact event waits

Each row contains execution ID, command ID, application event name, non-empty event key, optional satisfied journal position, optional wait-start time, and optional persisted deadline.

Primary/unique identity is `(command_id,event_name,event_key)`. Reverse lookup index begins `(execution_id,event_name,event_key)` and includes unresolved command ID. No event namespace or version is needed because only application `Event[T]` can be a wait operand.

Wait state constraints enforce:

- unsatisfied rows have null satisfied position;
- deadline is null until wait starts;
- `Within` creates one deadline only after command dependencies satisfy;
- terminal commands leave no active unresolved wait.

## 9. Coordinator rows

One row per coordinator execution stores coordinator ID, name/version, canonical state/hash, inbox position, start-pending flag, state revision, running delivery key/position, attempt/token/lease fields, retry state, accepted canonical `retry_policy` and `retry_policy_hash`, terminal/failure fields, and timestamps.

The stored policy remains even though M1 exposes no public coordinator retry option. Without it, restart or rolling deployment could reinterpret an active delivery.

Inbox advances only with the matching committed state transition. One lease token fences all coordinator writes.

## 10. Journal

Columns include:

- execution ID and positive position primary key;
- globally unique journal entry ID;
- PostgreSQL recorded time;
- kind and canonical body/hash;
- causation entry/command/event IDs where applicable;
- command, attempt, coordinator subject IDs;
- application event ID/name/key for staged or externally ingressed facts;
- event/terminal classification fields needed by indexed matching;
- definition name/version where useful for projections.

Application event rows have name/key and no event version. Runtime terminal rows use command/attempt subjects and kinds, not a public command-success namespace.

Partial unique indexes enforce:

- one `CommandCreated` per command;
- one terminal event per command;
- one application event identity `(execution_id,event_name,event_key)`;
- one execution terminal event;
- one plan reconciliation revision;
- stable coordinator delivery/transition identity.

Journal bodies are canonical bounded JSON. Indexed columns repeat only fields required for idempotency, selection, and inspection.

## 11. Journal body contracts

- `ExecutionStarted`: mode, definition/version, key scope, canonical input/initial state, deadline, fail-fast, command ceiling, metadata, accepted coordinator policy when applicable.
- `CommandCreated`: key, definition/version, arguments, origin, parent, required flag, normalized dependencies/waits, accepted absolute initial schedule, retry/timeout/queue, causation.
- attempt entries: attempt ID/ordinal, database times, conclusion classification, consumed count, next schedule where applicable.
- application event: event name/key and canonical payload.
- command terminal: status, typed result or structured failure.
- `PlanReconciled`: revision, snapshot position, quiescence, accepted command keys/fingerprints.
- coordinator transition: prior/new revision, delivered position, state hash/body, and terminal intent; staged events and commands are separate causally linked entries in the same batch.
- execution terminal: final status and structured reason.

`PlanReconciled` never duplicates full command arguments. `CommandCreated` is their authoritative historical record.

## 12. Ordered semantic transaction

`SemanticTx` owns:

- caller/library transaction handle;
- locked execution row and captured database now;
- next journal position cursor;
- deterministic journal batch;
- greatest execution ID locked in caller-owned composition;
- projection/queue/graph mutation helpers.

Opening a semantic transaction locks the execution row and checks ascending ID order. Appending N entries reserves the next N positions in memory and updates the execution counter once. Bodies and projections are written before commit; notification hints are sent transactionally last.

The store exposes narrow operations for start, event emit, cancellation, claim, settle, plan snapshot/reconcile, coordinator claim/settle, maintenance, and inspection. No caller assembles SQL categories ad hoc.

## 13. Idempotent start

Start first searches the appropriate stable/live-key identity. If found, it verifies start fingerprint for stable keys and returns the existing handle. Live-key repetition returns the current live handle according to live-dedupe semantics. A new start locks/creates identity, appends `ExecutionStarted`, creates root/coordinator state where applicable, and marks plan dirty where applicable.

An ambiguous client response is recovered by repeating identical `Execute`; no type/key lookup API is required.

## 14. Event emission

`Event.Emit` first checks existing `(execution,name,key)` before terminal rejection. Equivalent canonical body returns success; disagreement conflicts. A new fact then locks the running execution, rechecks identity, appends the journal row, resolves exact wait rows, sets plan dirty when applicable, and updates coordinator wake eligibility.

Two concurrent first emissions serialize on the execution lock and unique index. Unique violations are mapped back through the semantic equality check, never exposed as raw SQL ambiguity.

`flow.Emit` uses the same application-event row shape but performs no ingress SQL. Worker/coordinator settlement sorts staged identities, coalesces equivalent durable rows, rejects conflicts, assigns positions in the shared semantic batch, resolves matching waits, and commits their projections atomically with the enclosing decision.

## 15. Command creation batch

All command-producing paths call one batch primitive with distinct internal origins. Under the execution lock it:

1. resolves keys against existing commands and within-batch declarations;
2. validates equivalence and ownership;
3. checks command ceiling before writing any member;
4. assigns command IDs and `CommandCreated` positions deterministically;
5. inserts immutable command rows;
6. inserts dependencies and exact waits;
7. derives pending/ready/delayed/skipped state;
8. inserts queue rows where needed;
9. updates execution counters;
10. returns created/existing IDs.

Worker batches additionally set one parent and close its membership atomically. Plan batches carry normalized dependency records. Coordinator batches share the transition/inbox transaction.

## 16. Claim and fencing SQL

### 16.1 Command claim

The probe selects a bounded eligible set by queue and registered name/version, then locks queue and command rows with `FOR UPDATE SKIP LOCKED`. It updates state/token/lease and appends attempt history in the same short transaction.

The claim path never blocks on an execution lock and is exempt from semantic blocking order. If later semantic attempt-start journaling requires the execution lock, candidates whose execution is locked are skipped/no-wait rather than held while waiting.

### 16.2 Renewal

Renewal is one fenced update matching command ID, running state, attempt ID, and token. It returns renewed IDs. No journal row is written for heartbeat.

### 16.3 Settlement

Settlement locks execution then command, verifies fence, and applies the engine change set. Token mismatch returns lease lost without writes. The registered application commit function executes only after fence validation and Flow-owned locks/writes.

## 17. Plan storage operations

Dirty probe uses a partial index over non-terminal plan rows with `plan_dirty=true` ordered by dirty-since/ID and `SKIP LOCKED`. Claim stores plan lease token/expiry.

Snapshot loads are bounded batched queries for execution, commands, dependencies, waits, terminal values, child membership, and lazily selected application events. There is no plan-read table.

Reconciliation re-locks/validates lease, inserts command delta through the batch primitive, appends `PlanReconciled`, increments revision, sets quiescence, and clears dirty only if no later trigger exists under the same lock.

## 18. Coordinator selection

Selection finds the earliest matching position above inbox from:

- application event rows matching registered event names;
- command terminal rows joined to command name/version for `OnOutcome`.

Indexes support `(execution,event_name,position)` and `(execution,command_name,command_version,terminal_position)` shapes so sparse subscriptions do not re-filter an unmatched prefix indefinitely.

Claim writes delivery identity/token/lease. Settlement verifies token and inbox, writes state/commands/terminality, appends transition, and advances inbox once.

## 19. Maintenance

Bounded maintenance uses partial indexes and skip-locked claims for:

- expired command leases;
- expired coordinator/plan leases;
- due retry/delay queue rows;
- exact event wait deadlines;
- execution deadlines.

Each winning transition locks execution, rechecks predicates against PostgreSQL now, journals once, and updates projections. Repetition is harmless.

## 20. Inspection

`GetExecution` uses primary key. `ListExecutions` requires bounded filters/order/page size and supporting indexes; it is not a point-identity API. `History` scans `(execution_id,position)` only. `Trace` batches by execution and uses parent/dependency/wait/attempt indexes rather than N+1 queries.

Payload bodies are optional in inspection query shapes and redacted by default where APIs permit.

## 21. Migrations

The implementation is pre-release. The reduced design edits the initial migration in place and recreates development/test schemas unless a preserved external database is declared before work. Migration checksums and schema-version rows detect incompatible binaries.

Required reduction:

- remove event-version columns, constraints, indexes, and codecs;
- remove `outcome_ref`;
- remove dependency threshold;
- remove external-issue origin;
- replace spawn origin names with worker/coordinator command origins;
- remove command-success application-event selector namespace;
- retain coordinator retry policy/hash;
- retain exactly nine prefixed tables.

## 22. Error mapping

Constraint/SQL errors map to stable Flow categories. Unique violations trigger semantic reread where equivalence matters. Lock-not-available/no-row fence cases map to lease/invalid-state errors. Caller transactions receive retryable PostgreSQL errors rather than hidden retries that could repeat caller application work.

## 23. Growth and retention

Hot tables (`flow_command_queue`, running fields, coordinator lease fields) receive benchmark-driven fillfactor/autovacuum tuning. Journal is append-only and expected to dominate size. M1 retains indefinitely; growth evidence records bytes per command/attempt/fact and vacuum behavior.

Future retention removes complete terminal executions only under an explicit coordinated policy. Payload-body thinning may precede causal-skeleton deletion but must record the resulting loss of historical decode/simulation guarantees.

## 24. Test and benchmark plan

DDL tests verify every check, unique/foreign key, partial index, and row shape. Concurrency tests cover start/event idempotency, command batches, gap-free ordering, claim no-wait behavior, fence races, wait/deadline races, plan dirty clear, coordinator inbox, cancellation, and caller lock order.

Replay tests compare every semantic path to projections. Query-plan evidence covers 10K/1M/10M representative rows for claim, dirty plan, exact waits, coordinator outcome matching, history, and trace.

## 25. Acceptance conditions

- Fresh migration creates exactly the nine documented `flow_` tables.
- No event-version, threshold, `outcome_ref`, external-issue, or command-success-selector storage remains; staged events reuse ordinary application-event journal rows.
- Coordinator retry policy/hash remain durable.
- Journal positions are gap-free and commit-ordered per execution.
- Every projection change shares a transaction with its journal cause.
- Claims and maintenance use bounded indexed/no-wait shapes.
- Exact event idempotency and wait resolution are race-safe.
- Replay reconstructs stored settled state after every write path.
