# Flow retained data and archival design

Status: Design complete; implementation deferred

Designed from: `hardening-2` / Plan 6

## 1. Decision boundary

Flow currently retains every execution, command, wait, journal entry, argument,
result, failure, and application event indefinitely. This document defines what
a future retention implementation would have to preserve. It does **not**
authorize deletion, add a purge API, create an archive table, or change current
History, Trace, replay, or execution-key behavior.

The recommended first implementation scope is deliberately narrow:

- terminal unkeyed executions may become purge candidates after an explicit
  operator-configured minimum age;
- terminal live-key executions may become purge candidates because their live
  uniqueness reservation is already released at terminality;
- permanent-key executions remain in PostgreSQL unless a later design preserves
  exact permanent rediscovery and equivalent-start conflict behavior; and
- running or failing executions are never candidates.

Until the decisions in Section 8 are approved, retention remains deferred.

## 2. Capacity and operational motivation

Plan 5 measured approximately 1,996 bytes of journal tuple storage per completed
small command before indexes, commands, execution projections, and WAL. Pure
arithmetic at a sustained rate is:

| Sustained commands | Journal tuples/hour | Journal tuples/day |
|---:|---:|---:|
| 10/s | about 71.9 MB | about 1.72 GB |
| 100/s | about 718.6 MB | about 17.2 GB |
| 400/s | about 2.87 GB | about 69.0 GB |

These are capacity examples, not promised throughput or observed production
growth. Actual storage includes indexes, command/execution rows, TOAST, dead
tuples, WAL, backups, and payload-dependent journal bodies.

Large or sensitive values should normally be stored in application-owned object
storage and passed through Flow as stable references. Flow's durable arguments,
results, metadata, failures, and event payloads are operational history, not a
general-purpose blob store.

## 3. Retained object inventory

### 3.1 `flow_executions`

Writers are execution ingress plus semantic command/event/maintenance
transactions. Readers include Execute rediscovery, GetExecution, History/Trace
headers, listing, live-key lookup, scheduler/maintenance fences, and replay/live
checks.

Important retained values are:

- permanent/live identity: definition name/version, execution key, key scope,
  start fingerprint, exact input, immutable metadata and canonical metadata;
- current projection: status, failure, deadline, fail-fast mode, command ceiling,
  command/open counters, root command, and journal allocator; and
- lifecycle timestamps.

Deleting this row cascades queue and wait ownership but is restricted by retained
commands and journal. Permanent identity cannot be represented by a hash alone:
current rediscovery compares exact canonical start identity and must distinguish
equivalent starts from conflicts.

### 3.2 `flow_commands`

Writers are root/child creation, claims, settlement, retries, cancellation,
deadline expiry, and lease recovery. Readers include claims, event readiness,
ResultOf, inspection, Trace, maintenance, fail-fast, and replay comparison.

Retained values include immutable declaration identity and arguments; parent/root
graph ownership; retry, timeout, wait, and scheduling policy; attempt counters;
result and failures; journal positions; and lifecycle timestamps. Arguments,
results, retry policy, and failures may be TOASTed and may contain application
data. Commands cannot be deleted while journal command references remain.

### 3.3 `flow_command_queue`

This is the mutable delivery projection. Claims, renewals, retry release,
settlement, cancellation, and lease recovery write it; schedulers, maintenance,
and QueueDepth read it. It contains no terminal history and should normally have
no row for terminal commands. Its frequent updates/deletes create dead tuples,
so queue bloat and autovacuum are important even before retention exists.

Queue rows cascade with their execution/command. They are not independently
archivable and must not be used as evidence of completed work.

### 3.4 `flow_command_event_waits`

These rows hold exact event selectors and their satisfying journal position.
Decision settlement inserts them; accepted event deltas, retained-event matching,
and wait expiry update/read them. Trace and readiness validation depend on them.
Selectors can reveal application identifiers. Waits cascade with their command
or execution and must not outlive either.

### 3.5 `flow_journal`

The journal is immutable accepted history and the input to History, Trace, replay,
causation, retained-event matching, attempt resolution, and corruption checks.
Bodies contain canonical command/event/attempt data and may be TOASTed. Hashes,
event identities, attempt identities, causation, terminal uniqueness, and
gap-free positions are part of the durable contract.

Journal rows use restrictive execution and command ownership. Individual rows or
bodies cannot be compacted away while claiming that retained Flow history and
replay are unchanged.

### 3.6 `flow_schema_migrations`

The migration ledger records durable schema identity and compatibility. It is
never scoped to an execution and is never a retention candidate.

## 4. Eligibility contract for a future implementation

A candidate must satisfy all of the following in one current committed snapshot:

1. execution status is terminal (`succeeded`, `failed`, `cancelled`, or
   `expired`);
2. `finished_at` is older than an explicitly configured minimum age;
3. key scope is approved: initially unkeyed permanent (`execution_key=''`) or
   terminal live;
4. no legal hold, application hold, export-in-progress, or operator exclusion
   applies under the future control model;
5. the complete execution can be locked with `FOR UPDATE SKIP LOCKED` and remains
   eligible after locking; and
6. any required archive has a verified completion record before deletion begins.

Permanent non-empty keys are excluded. Running/failing rows are excluded even if
their deadlines or leases appear abandoned; maintenance owns recovery.

## 5. Archive protocol requirements

If archival is selected, use an execution-granular protocol rather than row-level
best effort:

1. assign an immutable export identity containing archive format version,
   Flow schema version, execution ID, and destination;
2. read the execution, commands, waits, and journal in deterministic order from
   a consistent snapshot;
3. write one complete archive object or manifest plus content-addressed parts;
4. compute and store a cryptographic checksum over the canonical manifest and
   every exported byte;
5. verify the destination independently;
6. record a durable completed marker in a separately approved operator ledger;
7. on retry, compare the exact identity/checksum and resume or reuse the same
   completed export; and
8. only then make the execution eligible for a deletion transaction.

Remote object writes and PostgreSQL commits cannot be made exactly once by
assertion. A crash after export but before marking complete must be retryable. A
crash after marking complete but before deletion must safely reuse the archive.
Conflicting content for the same export identity is a hard stop.

The archive format must preserve raw canonical bodies and hashes, journal order,
all projection fields needed by inspection, schema/codec versions, and enough
metadata to validate causation and replay without the live database.

## 6. Deletion transaction requirements

A later operator implementation should work in small bounded batches and use one
short transaction per execution:

1. select old terminal candidate IDs in deterministic order;
2. lock one execution `FOR UPDATE SKIP LOCKED`;
3. revalidate status, age, key scope, holds, and archive completion;
4. optionally compare retained row counts/checksums with the archive manifest;
5. delete `flow_journal` rows first because their execution/command references
   are restrictive;
6. delete `flow_commands`; queue and wait rows cascade, while the deferred root
   foreign key is satisfied by deleting the execution in the same transaction;
7. delete `flow_executions`; and
8. commit and record a bounded safe operational result.

Every delete must check affected counts against the locked manifest. An
unexpected row shape, reference, archive mismatch, or concurrent state change
rolls back the whole execution. Do not drop or weaken execution/command/root,
parent, journal, or wait ownership constraints to make cleanup easier.

The future tool needs dry-run/list mode, explicit schema and age arguments,
bounded batch/transaction sizes, cancellation, audit logging, metrics, and an
operator confirmation boundary. It must never accept a workspace-wide wildcard
as a destructive target.

## 7. Post-archive read semantics and compatibility options

The current API assumes retained PostgreSQL data. A future implementation must
choose one coherent behavior:

- **not found after purge:** simplest and recommended initially for unkeyed/live
  data when no archive-backed read API is promised;
- **archived/tombstone response:** requires a new durable identity relation and
  additive public status/read semantics;
- **transparent archive reads:** largest scope, requiring versioned archive
  clients, latency/error contracts, authorization, and replay conformance; or
- **operator-only archive:** live APIs return not found, while a separate tool
  inspects verified archived history.

Mixed library versions must remain safe. A release that introduces a retention
ledger or tombstones needs explicit minimum reader/writer compatibility and
forward migrations. Released migrations are never rewritten.

Permanent-key options remain:

1. retain the complete execution indefinitely (recommended now);
2. archive history but retain exact relational identity and enough inspection
   state;
3. add an exact, versioned permanent tombstone representation; or
4. introduce a separately named, operator-authorized identity-release operation
   with intentionally different public semantics.

Option 4 is deletion of an idempotency guarantee and cannot be implicit.

## 8. Maintainer decision record

The following recommendations are recorded; implementation remains deferred
until explicitly approved:

| Decision | Recommendation | Current approval |
|---|---|---|
| eligible key scopes | terminal unkeyed and terminal live only | deferred |
| minimum age | deployment/operator configured; no library default yet | deferred |
| permanent keys | retain complete execution indefinitely | accepted design default |
| archive destination | optional operator-owned versioned object/manifest | deferred |
| deletion surface | separate operator tool, not the runtime hot path | accepted design default |
| post-purge reads | not found for initial unkeyed/live purge scope | deferred |
| legal/application holds | required before production deletion | deferred |

No production purge, archive, tombstone, or deletion code is authorized by this
record.

## 9. Monitoring guidance

Useful PostgreSQL observations include:

```sql
SELECT relname,
       pg_size_pretty(pg_total_relation_size(relid)) AS total,
       n_live_tup, n_dead_tup, last_autovacuum, autovacuum_count
FROM pg_stat_user_tables
WHERE schemaname = '<flow-schema>'
ORDER BY pg_total_relation_size(relid) DESC;

SELECT schemaname, relname, indexrelname,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       idx_scan
FROM pg_stat_user_indexes
WHERE schemaname = '<flow-schema>'
ORDER BY pg_relation_size(indexrelid) DESC;

SELECT pid, application_name, xact_start,
       clock_timestamp() - xact_start AS transaction_age
FROM pg_stat_activity
WHERE xact_start IS NOT NULL
ORDER BY xact_start;
```

Also monitor journal growth rate, queue dead tuples, lease-recovery lag, renewal
errors/timeouts, database and filesystem free space, WAL/archive growth, backup
duration, and oldest transaction age. Long caller-owned transactions and long
`WithCommit` callbacks delay locks and vacuum. Do not address growth by disabling
`fsync`, synchronous commit, full-page writes, or autovacuum.
