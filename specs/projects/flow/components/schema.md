---
status: complete
---

# Component: PostgreSQL storage and journal

## Purpose

The store is Flow's durable authority. It owns migrations, constraints, indexes, semantic transactions, queues, exact wait rows, fencing, idempotency, the immutable journal, and inspection projections.

All objects use the `flow_` prefix in the selected application schema.

## Storage invariants

1. A semantic mutation locks its execution row first.
2. Journal positions are gap-free and commit-ordered per execution.
3. Immutable journal entries and mutable projections commit together.
4. Queue/lease churn is separate from immutable command identity and result.
5. Claim probes use no-wait `SKIP LOCKED` behavior.
6. Durable timestamps come from PostgreSQL after required locks.
7. Canonical bodies and hashes make retries comparable.
8. Constraints reject impossible local row shapes; store code validates cross-row transitions.

## Seven-table inventory

| Table | Responsibility |
|---|---|
| `flow_executions` | execution identity, mode, counters, deadline, status, idempotency |
| `flow_commands` | immutable declaration plus current semantic command state/result |
| `flow_command_queue` | claim eligibility, active attempt, lease, and retry timing |
| `flow_command_event_waits` | exact event selectors and satisfaction positions |
| `flow_coordinators` | typed state, inbox cursor, delivery, retry, and lease state |
| `flow_journal` | immutable ordered semantic history and retained events |
| `flow_schema_migrations` | checksummed migration and compatibility records |

The only execution driver modes are `direct` and `coordinator`.

## Executions

Execution rows store definition identity, permanent/live key scope, start fingerprint/input, metadata, fail-fast flag, deadline, command ceiling/counters, journal allocator, optional root command, failure, and timestamps.

Statuses are `running`, `failing`, `succeeded`, `failed`, `cancelled`, and `expired`. Permanent keys have a partial unique index across all executions; live keys are unique only across running/failing executions.

## Commands

Command rows store execution/key identity, definition, origin, optional parent, required flag, canonical arguments/fingerprint, semantic state, queue/retry/timeout settings, initial delay, wait timing, attempt counters, result/failure, journal positions, and timestamps.

Origins are:

- `direct_root`;
- `worker_child`;
- `coordinator_command`.

States are `pending`, `ready`, `running`, `retry_wait`, `succeeded`, `failed`, `cancelled`, and `expired`. Pending requires unresolved waits. Terminal states require terminal position/time, and only success stores result bytes/hash.

Each `(execution_id, command_key)` is unique. Parent identity is present only for worker-created children.

## Command queue

Queue rows exist only for ready, retry-wait, or running commands. They duplicate the minimum claim dimensions: command/execution IDs, definition version, queue, next run time, and active lease fence.

Ready/retry claims use an index over definition, time, queue, and command ID. Lease recovery uses a separate partial expiry index. Deleting a queue row cannot delete immutable command/history state.

## Exact event waits

Each row is keyed by `(command_id,event_name,event_key)` and records its execution plus optional satisfying journal position. A reverse partial index finds unresolved matches when an application event arrives.

At command creation, the store checks retained application events before calculating `unsatisfied_waits`. The command row owns `wait_started_at`, optional `wait_deadline_at`, and `wait_timeout_ms`. Delay and wait timing remain independent.

## Coordinators

One coordinator row belongs to one coordinator execution. It stores name/version, canonical state/revision/position, start-pending flag, monotonic inbox and scan positions, current delivery identity, retry counters, lease fence, status, and timestamps.

Partial indexes support ready delivery claims, idle retained-history scanning, and expired-lease recovery. Coordinator status is `active`, `completed`, `failed`, or `cancelled`.

## Journal

The primary key is `(execution_id,position)`. Each entry has a globally unique entry ID, kind, PostgreSQL time, optional causation position, typed subject columns, canonical body, and hash.

Kinds are:

- `execution_started`;
- `execution_failing`;
- `command_created`;
- `attempt_started`;
- `attempt_concluded`;
- `event_recorded`;
- `coordinator_transition`.

Event classes are application, command terminal, execution terminal, and coordinator terminal. Unique partial indexes enforce one command-created event, terminal events, attempt boundaries, and exact application-event identity.

The journal is also retained event storage. There is no separate application-event table.

## Start transactions

A start checks permanent/live identity, compares fingerprints where required, locks or creates the execution, appends `ExecutionStarted`, and creates either a root command or coordinator row.

Root command creation records exact waits, resolves any retained matching events, derives pending/ready/delayed state, inserts queue state if eligible, increments counters, and appends `CommandCreated` in the same transaction.

## Command settlement

Success settlement:

1. locks execution, command, and fenced queue state;
2. validates the active attempt;
3. runs the optional application commit callback;
4. canonicalizes and appends staged application events;
5. creates staged children in stable key order;
6. records attempt conclusion and command terminal event;
7. resolves waits/readiness and execution failure/completion;
8. mutates projections and removes the active queue row.

If the execution is already failing, new child declarations from a surviving attempt are inserted and immediately cancelled, while its accepted result/events/commit remain durable.

Failure settlement chooses retry or terminal failure from policy/classification. Terminal required failure can record `ExecutionFailing` and cancel non-running work in the same transaction.

## Coordinator settlement

Coordinator claim and settlement use a delivery/lease fence separate from command attempts. Accepted settlement updates state, stages event/command batches, advances inbox, records the transition, and optionally terminalizes coordinator/execution atomically.

Unmatched retained entries advance the scan cursor without handler invocation. Matched but unaccepted inputs remain deliverable across crashes.

## Event ingress

External event ingress first checks exact identity for idempotency, then locks the execution and rechecks. A new canonical application event is appended, matching unresolved wait rows receive its position, affected command readiness is recalculated, and coordinator discovery is hinted.

Equivalent repeats succeed even if the execution later terminalized. Conflicting repeats fail. New events cannot be added to a terminal execution.

## Maintenance

Bounded indexed maintenance handles:

- expired command and coordinator leases;
- unresolved event-wait deadlines;
- execution deadlines;
- coordinator retained-input discovery;
- completion after derived transitions.

Every semantic maintenance change uses the same execution lock/journal rules.

## Inspection and replay

Point lookup uses execution primary keys. Listing uses bounded indexed filters and keyset cursors. History scans `(execution_id,position)`. Trace folds the journal under repeatable read and loads operational command/coordinator fields in bounded queries.

Replay rejects invalid kind/body/transition shapes and is tested against live projections. Operational queue and lease changes do not alter semantic replay.

## Migration policy

This pre-release refactor edits the baseline schema and recreates development/test schemas. Removed workflow projection and command-relationship formats receive no compatibility columns or data conversion. `CheckSchema` verifies the exact two checksummed migrations and the seven expected tables.

## Verification

DDL tests exercise constraints, indexes, idempotency, lock order, claim behavior, fences, wait/event races, deadlines, reduced fail-fast, coordinator inboxes, replay conformance, and caller transactions. Query/workload tests cover command claims, exact wait lookup, coordinator scanning, history/trace, journal growth, and multi-replica takeover.
