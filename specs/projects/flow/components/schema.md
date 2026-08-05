---
status: complete
completed_at: 2026-08-04
---

# Component: PostgreSQL storage and journal

## Purpose and invariants

The store owns migrations, constraints, indexes, semantic transactions, queues, exact waits, fencing, idempotency, the immutable journal, and inspection projections. All objects use the configured schema and `flow_` prefix.

Semantic mutations lock their execution first. Journal positions are gap-free and commit-ordered per execution. Immutable entries and projections commit together. Claims use no-wait `SKIP LOCKED`; durable timestamps come from PostgreSQL; canonical bodies and hashes make retries comparable.

## Six-table inventory

| Table | Responsibility |
|---|---|
| `flow_executions` | execution identity, counters, deadline, status, and idempotency |
| `flow_commands` | immutable declaration, provenance, and semantic result/state |
| `flow_command_queue` | readiness, active attempt, lease, and retry timing |
| `flow_command_event_waits` | exact selectors and satisfying journal positions |
| `flow_journal` | immutable ordered semantic history and retained events |
| `flow_schema_migrations` | checksummed migration records |

There is one execution kind. Permanent and live-key uniqueness use `(definition_name, execution_key)`. A null parent identifies the root command; a non-null parent identifies a worker-staged sub-command.

## Commands, waits, and journal

Command states are `pending`, `ready`, `running`, `retry_wait`, `succeeded`, `failed`, `cancelled`, and `expired`. Queue rows exist only for claimable/retrying/running commands and contain the minimum claim and fence dimensions.

Wait rows are keyed by `(command_id,event_name,event_key)` and optionally reference `satisfied_position`. On creation the store checks retained events; on ingress it resolves matching unresolved rows. Claim materialization joins all wait rows to their exact application-event journal bodies in one query and validates a maximum of 256.

Journal kinds are `execution_started`, `execution_failing`, `command_created`, `attempt_started`, `attempt_concluded`, and `event_recorded`. Event classes are application, command terminal, and execution terminal. Application events are immutable journal facts; no separate event-payload table exists.

## Transactions and maintenance

Start creates one execution and root command. Success settlement accepts the application commit callback, result, staged events/sub-commands, attempt conclusion, wait resolution, and completion progression atomically. Failure settlement applies retry classification or terminal/fail-fast transitions.

External event ingress enforces exact identity, appends the event, records satisfying positions, and recalculates readiness. Equivalent repeats are idempotent; conflicting repeats fail; terminal executions cannot be reopened.

Bounded indexed maintenance recovers command leases, expires unresolved waits, and enforces execution deadlines. Inspection uses indexed lookup/keyset pagination. Trace folds the journal under repeatable read and overlays bounded operational command data.

## Migration policy

This pre-release refactor rewrites baseline migrations. Existing development schemas from the removed architecture must be recreated. `CheckSchema` verifies migration checksums and the exact six-table inventory.
