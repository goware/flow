---
status: complete
completed_at: 2026-08-04
---

# Component: PostgreSQL storage and journal

## Purpose and invariants

The store owns migrations, constraints, indexes, semantic transactions, queues, exact waits, fencing, idempotency, the immutable journal, and inspection projections. All objects use the configured schema and `flow_` prefix.

Semantic mutations lock their run first. Journal positions are positive, gap-free, and commit-ordered per run. Immutable entries and projections commit together. Claims use no-wait `SKIP LOCKED`; durable timestamps come from PostgreSQL; canonical bodies and retained identity hashes make retries comparable.

## Six-table inventory

| Table | Responsibility |
|---|---|
| `flow_runs` | run identity, counters, deadline, status, and idempotency |
| `flow_commands` | immutable declaration, provenance, and semantic result/state |
| `flow_command_queue` | readiness, active attempt, lease, and retry timing |
| `flow_command_event_waits` | exact selectors and satisfying journal positions |
| `flow_journal` | immutable ordered semantic history and retained events |
| `flow_schema_migrations` | checksummed migration records |

There is one run kind. Permanent and live-key uniqueness use `(definition_name, run_key)`. Every run has a non-null root command owned by that run. A null parent identifies the root command; a non-null parent identifies a worker-staged sub-command in the same run. Composite foreign keys also prevent queue, event-wait, and journal command references from crossing run ownership.

## Durable types and vocabularies

Go versions and counters remain `int`; public construction and store persistence validate them against PostgreSQL's signed `integer` range before binding or applying a transition. Positive public duration-bearing configuration is rounded upward once to whole-millisecond precision; store and decode boundaries remain exact. Millisecond columns remain `bigint`, and reads plus database-time additions use checked conversions.

Retry policy is opaque canonical `bytea`; no SQL path parses, filters, or reserializes it. Run status, command status, delivery state, key scope, and terminal status remain PostgreSQL `text` with named `CHECK` constraints synchronized with typed public constants and exhaustive decoders.

Every Flow-generated identifier — run, command, attempt, lease token,
journal entry, and event — is a UUIDv7 produced by `internal/uuid`.
Time-ordered identifiers keep hot primary-key indexes append-mostly and make
identifier byte order correlate with creation order; generation is strictly
monotonic within one process. Identifiers therefore encode their creation
time and are not secrets. The storage type is the ordinary 16-byte `uuid`, so
rows created before this policy coexist unchanged.

## Commands, waits, and journal

Command states are `pending`, `ready`, `running`, `retry_wait`, `succeeded`, `failed`, `cancelled`, and `expired`. Queue rows exist only for claimable/retrying/running commands and contain the minimum claim and fence dimensions.

Wait rows are keyed by `(command_id,event_name,event_key)` and optionally reference `satisfied_position`. On creation the store checks retained events. Later ingress uses the partial reverse index to update only matching unresolved rows, groups newly satisfied rows by command, decrements `unsatisfied_waits`, and queues only commands whose counter reaches zero. Claim materialization joins all waits for a same-run claim batch to their exact application-event journal bodies in one query and validates a maximum of 256 per command.

Journal kinds are `execution_started`, `execution_failing`, `command_created`,
`attempt_started`, `attempt_concluded`, and `event_recorded`. The first two are
retained historical wire values; migration 004 does not rewrite journal bytes.
Event classes are application, command terminal, and run terminal. Application
events are immutable journal facts; no separate event-payload table exists. All
stored position references are positive, and causation must precede the current
journal position.

Integrity responsibilities are split deliberately. Journal append
re-canonicalizes and hash-checks every accepted body. Event-input claim
materialization verifies the retained hash, decodes the typed versioned body
once, and validates the nested canonical payload without creating another
canonical copy. Full replay verifies the hash and reconstructs canonical bodies
again for stronger diagnostics.

Indexes are kept deliberately narrow. Primary and unique indexes enforce public run, command, application-event, lifecycle, and same-run ownership invariants; journal entry/event UUIDs are retained data but are not separately indexed because no lookup, foreign key, or idempotency contract consumes them. One partial `(attempt_id, entry_kind)` guard permits one start and one conclusion per attempt. Exact application-event reads state the identity-index predicate, and maintenance, queue, and wait indexes cover only their bounded hot probes. The metadata GIN index remains because containment filtering is a documented indexed inspection feature.

## Retained semantic projections

The baseline deliberately retains the following columns after pruning redundant write-only hashes:

| Column | Writer and reader | Invariant and replacement cost |
|---|---|---|
| `flow_runs.input` | start writes it; permanent-key equivalence reads it | exact accepted root input; replacement requires loading the root declaration or journal and changes start-idempotency coupling |
| `flow_runs.metadata_canonical` | start writes it; permanent-key equivalence reads it while inspection queries `metadata` | exact bytes correspond to indexed metadata JSON; replacement requires recanonicalizing reads or changing identity semantics |
| `flow_commands.declaration_fingerprint` | command creation writes the accepted declaration digest; identity, migration, and diagnostic paths retain it while replay validates the journal copy | covers the complete immutable declaration; replacement requires full field/wait comparison and benchmark evidence |
| `flow_commands.result` | successful settlement writes the point-read result projection | non-null only for success and equal to the terminal journal result; removal makes result reads replay-dependent |
| `flow_commands.last_error` | retry/recovery transitions write it; live trace reads it | latest operational failure and cleared by success; removal requires folding attempt history for live inspection |
| `flow_commands.terminal_failure` | unsuccessful terminal transitions write it; terminal comparison/inspection reads it | stable unsuccessful terminal reason; removal couples idempotency and inspection to replay |

The shared failure encoding does not merge `last_error`, `terminal_failure`, or run `failure`: each describes a different lifecycle fact. The journal keeps the corresponding semantic history independently of these point-read projections.

## Transactions and maintenance

Start creates one run and root command. Success settlement accepts the application commit callback, result, staged events/sub-commands, attempt conclusion, wait resolution, and completion progression atomically. Normalized event identity reads, retained-event reads, command/wait/queue insertion, and ordinary same-run claims use bounded operation-specific sets rather than one SQL round trip per item. Failure settlement applies retry classification or terminal/fail-fast transitions.

External event ingress through `Event.Deliver` enforces exact target-local identity, appends the event, records satisfying positions, and applies delta readiness. Equivalent repeats are idempotent; conflicting repeats fail; terminal runs cannot be reopened. Delivery adds no source identity or storage shape and uses a caller transaction unchanged when supplied.

The generic journal append emits no scheduler notification. A semantic
operation may emit at most one transactional hint after it creates work that is
immediately runnable at database time; polling remains the correctness path.

Bounded indexed maintenance recovers command leases, expires unresolved waits, and enforces run deadlines. Inspection uses indexed lookup/keyset pagination. Trace folds the journal under repeatable read and overlays bounded operational command data.

## Migration policy

Migrations 001–003 are the immutable historical baseline. Migration 004
renames the live catalog from execution to run vocabulary while preserving
rows, relationships, index/constraint definitions, journal wire bytes, and all
historical checksums. Every later schema change must add another forward
migration. Applications run migrations before a newer runtime and should back
up durable data before upgrading. `CheckSchema` verifies migration checksums,
their contiguous order, compatibility, and the exact six-table inventory.
