# Plan 5: Reduce hot-path work while preserving Flow's command/event model

Status: Planned

Planned at: `d2713d8` on 2026-08-06

> **Executor instructions:** Read this complete plan before changing code. Follow
> the phases in order, run every phase-specific verification gate, and keep each
> phase independently reviewable. If a STOP condition occurs, stop and report it
> rather than weakening a durable invariant or improvising a broader design.
>
> **Drift check:** Before implementation, run:
>
> ```text
> git diff --stat d2713d8..HEAD -- \
>   command_runtime.go hardening_benchmark_test.go \
>   internal/store migrations notification_test.go \
>   command_runtime_test.go event_gate_test.go staged_event_runtime_test.go
> ```
>
> If an in-scope hot path has changed, reconcile the current implementation with
> the source excerpts and invariants in this plan before proceeding. Do not apply
> the described SQL mechanically to a changed transition.

## 1. Summary

Flow's public architecture is already appropriately small:

```text
commands + workers + execution-scoped events + exact event gates
```

The execution row is the intentional aggregate lock, PostgreSQL is the
coordination authority, commands are the only durable execution unit, and the
journal plus mutable projections commit atomically. This plan does not replace
those decisions. It makes the existing implementation perform less work while
holding that lock.

The current hot paths pay for several avoidable operations:

- every success or event ingress can load every command in an execution to
  recalculate readiness;
- event, child, and wait batches are persisted using per-item SQL loops;
- the command-key uniqueness index includes frequently changing projection
  columns, causing avoidable index rewrites;
- one runtime claims independent executions serially even when the database pool
  and worker capacity can support concurrent transactions;
- journal position allocation uses redundant reads, and every journal append
  emits a notification whether or not runnable work was created; and
- event-input claims repeatedly canonicalize and decode bytes that Flow itself
  already accepted as canonical.

The target is a set-oriented implementation whose cost is proportional to the
commands and waits affected by a transition, not to the complete retained
execution. The expected result is materially higher small-command and event
throughput, shorter execution-lock hold time, lower index churn, and much lower
memory usage for large event-input snapshots.

This plan deliberately starts by adding saturation benchmarks. Timing claims in
this document are development-machine goals, not service-level objectives and
must not become flaky test thresholds.

## 2. Controlling decisions

### 2.1 Preserve the aggregate and durability model

Every semantic mutation continues to:

1. operate on exactly one execution;
2. acquire the execution row before dependent durable rows;
3. use PostgreSQL database time for accepted transitions;
4. allocate gap-free, commit-ordered journal positions within the execution;
5. append semantic history and update projections in one transaction;
6. preserve the current command attempt fence and lease checks;
7. run `WithCommit` inside the fenced success transaction; and
8. commit or roll back the complete mutation atomically.

Do not relax the execution lock, gap-free journal, or fenced settlement to reach
a benchmark target. Same-execution mutations are intentionally serialized. This
plan improves how much work occurs inside that serialized region.

### 2.2 Retain the six-table model

Keep the current tables and responsibilities:

| Table | Responsibility retained by this plan |
|---|---|
| `flow_executions` | aggregate identity, counters, deadline, status, lock, journal allocator |
| `flow_commands` | durable declarations and semantic command projection |
| `flow_command_queue` | narrow hot readiness, retry, attempt, and lease projection |
| `flow_command_event_waits` | exact reverse wait selectors and satisfying positions |
| `flow_journal` | immutable semantic history and application-event bodies |
| `flow_schema_migrations` | checksummed compatibility ledger |

Do not merge the queue into the wider command row. Do not remove the reverse
wait table or its `unsatisfied_waits` command counter. Those two projections are
what make incremental readiness possible.

### 2.3 Preserve public behavior

The following contracts remain unchanged:

- permanent and live execution-key semantics;
- command-key and application-event idempotency;
- exact AND event gates and the 256-wait bound;
- events committed on or before a wait deadline winning over delayed expiry;
- deterministic command/event normalization;
- command ceilings, delays, retries, attempt timeouts, execution deadlines,
  fail-fast, cancellation, and lease recovery;
- atomic result, staged event, staged child, `WithCommit`, journal, and projection
  settlement;
- caller-owned transaction behavior and lock ordering;
- history, trace, replay, observer, and notification correctness; and
- the current public API and six-table inventory.

An optimization that needs a new public orchestration primitive is out of scope.
Internal helpers may change freely when they make the existing semantics clearer.

### 2.4 Prefer set-oriented, operation-specific SQL

Use `unnest`, `UPDATE ... FROM`, data-modifying CTEs, `RETURNING`, and
`CopyFrom` where they replace per-row network round trips. Keep the SQL grouped
around real store operations such as:

- append a semantic journal batch;
- satisfy exact waits and release commands;
- insert a normalized child-command batch; and
- claim a bounded set of commands from one execution.

Do not introduce a generic mutation DSL, ORM abstraction, database function
framework, stored procedure layer, or second projection engine. Focused helpers
are easier to verify against Flow's existing fault and replay tests.

### 2.5 Treat measurements as evidence, not contracts

Performance work must be justified by repeatable benchmarks and structural
query-count reductions. Do not put wall-clock throughput assertions in ordinary
CI tests. Record machine, PostgreSQL version/settings, concurrency, payload
shape, benchmark command, and repeated samples in benchmark evidence.

## 3. Baseline and evidence

### 3.1 Development-machine baseline

The measurements that motivated this plan were taken on:

- Linux/amd64;
- Intel Core Ultra 7 255H, 16 logical CPUs;
- 64 GiB RAM and local NVMe storage;
- PostgreSQL 18.1;
- `fsync=on`, `synchronous_commit=on`, and `full_page_writes=on`; and
- the current default 16-worker runtime shape with a 12-connection test pool.

Observed at commit `d2713d8`:

| Workload | Observed result |
|---|---:|
| sequential execution ingress, poll-only | 4.92-5.21 ms/op |
| sequential execution ingress, notifications | 4.93-5.08 ms/op |
| 100 no-op commands in one fan-out execution | 0.93-1.03 s total |
| journal for those 100 commands | 402 rows |
| journal tuple bytes per small command | approximately 1,996 bytes |
| claim snapshot, 256 x 64 KiB event inputs | approximately 386.8 ms and 390.3 MiB allocated |

The ingress benchmark is sequential and the 100-command measurement includes
test setup. They are useful regression anchors, not saturation ceilings.

### 3.2 Current structural costs

#### Readiness scans the complete execution

`internal/store/graph.go` currently loads all command readiness projections:

```go
// internal/store/graph.go:371-412 at d2713d8
func (s *Store) resolveReadinessLocked(...) (readinessResolution, error) {
	commands, err := s.loadReadinessCommands(ctx, semantic)
	// ...
}

func (s *Store) loadReadinessCommands(...) ([]readinessCommand, error) {
	rows, err := semantic.PGX().Query(ctx, `SELECT ...
		FROM `+pgschema.Table(s.schema, "flow_commands")+
		` WHERE execution_id=$1 ORDER BY command_key`, semantic.ExecutionID())
```

The same file then executes individual wait, command, queue, and wait-start
updates in loops. A successful command with no staged events still enters this
resolution path.

#### Decisions are journal-batched but projection-row-oriented

`internal/store/commands.go` performs one wait-match query per staged event and
one complete `insertCommand` call per staged child:

```go
// internal/store/commands.go:738-746, 871-878 at d2713d8
for index, event := range request.Events {
	waits, matchErr := s.matchingWaitsLocked(...)
	// ...
}

for index, child := range request.Children {
	if err := s.insertCommand(...); err != nil {
		return SettleResult{}, err
	}
}
```

`internal/store/ingress.go:355-440` performs one retained-event lookup and one
wait-row insertion per wait. `coalesceApplicationEvents` performs one existing
event lookup per staged event.

#### The command-key index contains mutable fields

`migrations/001_initial.sql` currently declares:

```sql
CONSTRAINT flow_commands_execution_key_uq UNIQUE (execution_id, command_key)
    INCLUDE (command_id, name, version, parent_command_id, required,
             state, unsatisfied_waits, terminal_position)
```

`state`, `unsatisfied_waits`, and `terminal_position` change on ordinary command
lifecycle paths. Including them forces updates to this wide index and prevents
HOT updates that would otherwise be possible. The readiness and trace queries
read additional non-included columns and therefore do not receive a complete
index-only plan from this `INCLUDE` list.

#### Independent execution claims are serialized by one scheduler loop

`command_runtime.go:78-80` iterates execution groups synchronously:

```go
for _, group := range groupCandidatesByExecution(selected) {
	result, claimErr := r.store.ClaimCommands(ctx, group, ...)
	// ...
}
```

This is correct but means one runtime issues one durable claim transaction at a
time when selected commands belong to independent executions.

#### Journal append does redundant work

`internal/store/store.go:242-275` reads the next journal position, updates the
allocator, uses `CopyFrom`, and calls `pg_notify` for every semantic batch.
Successful settlement also reads the next position before calling `Apply`,
which reads it again.

An `attempt_started` journal append and a terminal settlement currently notify
the scheduler even though neither necessarily creates runnable work.

#### Event snapshots reprocess accepted canonical bytes

`internal/store/commands.go:368-408` canonicalizes the full stored event body,
compares its digest, decodes the versioned wrapper, and canonicalizes the
payload again before copying it into the worker snapshot. This compounds memory
for the documented maximum of 256 x 64 KiB inputs.

## 4. Scope

### 4.1 Expected implementation files

The executor may modify these production files as required by the phases:

- `command_runtime.go`
- `internal/store/store.go`
- `internal/store/ingress.go`
- `internal/store/graph.go`
- `internal/store/commands.go`
- `internal/store/journalcodec/journalcodec.go`
- `migrations/001_initial.sql`

The executor may modify or add focused coverage in:

- `hardening_benchmark_test.go`
- `notification_test.go`
- `command_runtime_test.go`
- `event_gate_test.go`
- `staged_event_runtime_test.go`
- `execute_test.go`
- `claim_test.go`
- `migrations_test.go`
- `internal/store/store_test.go`
- other existing `*_test.go` files only when a named invariant in this plan is
  already primarily covered there.

The final documentation phase may update:

- `README.md`
- `doc.go`
- `specs/projects/flow/project_overview.md`
- `specs/projects/flow/architecture.md`
- `specs/projects/flow/functional_spec.md`
- `specs/projects/flow/components/schema.md`
- `specs/projects/flow/components/engine.md`
- `specs/projects/flow/benchmark_evidence/`
- `specs/projects/flow/implementation_plan.md`
- this plan's status and punchlist.

### 4.2 Out of scope

Do not add or change:

- exported command, event, worker, execution, history, or trace APIs;
- a seventh runtime table;
- a cache, broker, outbox, sidecar, or external coordination service;
- an execution-independent journal sequence;
- journal gaps or best-effort projection updates;
- parallel settlement inside one execution;
- a coordinator, graph evaluator, state-machine API, outcome subscription, or
  event callback;
- OR, quorum, race, or first-of-N gates;
- automatic retention, partitioning, pruning, or archival;
- an application-event payload projection that duplicates journal payloads;
- changes to retry, fail-fast, cancellation, deadline, or idempotency semantics;
- removal of parent ownership, composite ownership foreign keys, unique
  lifecycle guards, or the reverse-wait index; or
- database tuning that disables durability.

Whole-execution archival remains important operational follow-up, but it is a
separate product and retention design. This plan only prevents avoidable work
and bloat in retained data.

## 5. Repository commands and conventions

### 5.1 Verification commands

Run PostgreSQL-backed tests in non-skip mode. The repository's preferred gates
are:

| Purpose | Command | Expected result |
|---|---|---|
| format | `gofmt -w <changed Go files>` | changed Go files are formatted |
| build | `make build` | exit 0 |
| complete race suite | `make test` | exit 0; PostgreSQL tests run rather than skip |
| static analysis | `go vet ./...` | exit 0 with no findings |
| ordinary suite | `go test -count=1 ./...` | exit 0 |
| focused benchmark | `go test -run '^$' -bench '<benchmark regexp>' -benchmem -count=5` | all requested benchmarks run and pass |
| diff validation | `git diff --check` | no output, exit 0 |

Use `FLOW_TEST_DATABASE_URL` and `FLOW_TEST_DATABASE_PASSWORD` through the
existing test configuration. Never print credential values in logs or evidence.

### 5.2 Code conventions

- Keep SQL in `internal/store` and render table names through
  `pgschema.Table`; never concatenate caller-controlled identifiers.
- Map database errors through `MapError` with a safe, operation-specific label.
- Validate public/store values with existing `definition`, `durable`,
  `canonical`, and `flowerr` helpers rather than adding parallel validation.
- Continue using PostgreSQL time from `SemanticTx.DBNow()` for durable decisions.
- Preserve stable command-key and event name/key ordering whenever journal or
  cancellation order is observable.
- Keep fault hooks at equivalent semantic boundaries. Moving several SQL
  statements into one set-oriented operation must not silently remove a tested
  ambiguous-commit or rollback seam.
- Use table-driven tests and the existing real-PostgreSQL `testpg` helpers.
- Keep public errors free of raw SQL, driver detail, payloads, and lease tokens.
- Match the repository's imperative commit-message style, for example:
  `Optimize incremental event wait resolution`.

### 5.3 Git workflow

- Use a focused branch such as `perf/flow-hot-path` if the operator requests a
  branch.
- Prefer one commit per completed phase so benchmark and semantic regressions
  can be bisected.
- Do not push or open a PR unless the operator explicitly requests it.
- Do not rewrite or discard unrelated working-tree changes.

## 6. Phase 0: Establish saturation and structural baselines

### 6.1 Add end-to-end command benchmarks

Extend `hardening_benchmark_test.go` with reusable setup that excludes migration,
runtime startup, and fixture creation from the timed region where possible.
Keep handlers as no-op typed workers with small canonical arguments/results.

Add these workloads:

1. `BenchmarkIndependentCommandLifecycle`
   - sub-benchmarks for producer concurrency 1, 4, and 16;
   - start distinct permanent-key executions;
   - run the registered no-op worker;
   - wait until every execution is terminal;
   - report completed commands per second as a custom metric;
   - use enough commands per iteration/batch that timer and polling granularity
     do not dominate.
2. `BenchmarkSameExecutionFanout`
   - fan-out sizes 10 and 100 in ordinary repeated runs;
   - an explicit one-shot 1,000-command stress workload, not part of short CI;
   - no-op child workers and notifications disabled for deterministic polling;
   - report total completion time and commands per second.
3. `BenchmarkStagedDecisionBatch`
   - child counts 1, 10, and 100;
   - event counts 0, 10, and 100;
   - a representative mix of children with no waits, one wait, and several
     waits;
   - measure the settlement that persists the decision separately from later
     child execution where feasible.

Do not reuse execution keys between benchmark iterations unless the benchmark
is explicitly measuring idempotent rediscovery.

### 6.2 Add event-ingress benchmarks

Add `BenchmarkExternalEventIngress` with these target shapes:

- distinct small live executions, one event each;
- one small hot execution receiving distinct event keys sequentially;
- one execution with 100 retained commands and indexed waits;
- events matching no waits;
- events matching one wait; and
- one event matching several waiting commands.

Keep the target execution non-terminal with a deliberate unsatisfied gate.
Never use an unbounded execution or a sleeping worker merely to keep it alive.
Use distinct event identities so the normal path is measured separately from
idempotent rediscovery.

### 6.3 Retain the adversarial event-input benchmark

Keep `BenchmarkEventSnapshotMaterialization256` as the maximum-payload guard.
Add smaller shapes such as 1 x 1 KiB and 32 x 1 KiB so optimizations can be
evaluated for ordinary input as well as the 16 MiB adversarial case.

### 6.4 Record baseline evidence

Create `specs/projects/flow/benchmark_evidence/plan_5_hot_path.md` containing:

- commit SHA;
- machine and PostgreSQL version;
- durability settings;
- connection and worker concurrency;
- exact commands;
- payload and execution shapes;
- five repeated samples or medians/ranges; and
- a warning that results are regression evidence rather than SLOs.

Do not overwrite historical superseded measurements. Link them where useful.

### 6.5 Phase 0 verification

Run:

```text
go test -count=1 -run '^TestJournalGrowthMeasurement100Commands$' -v .
go test -run '^$' -bench 'Benchmark(IndependentCommandLifecycle|SameExecutionFanout|StagedDecisionBatch|ExternalEventIngress|ExecutionIngressNotification|EventSnapshotMaterialization)' -benchmem -benchtime=3s -count=5 .
```

Expected:

- every named benchmark actually runs against PostgreSQL;
- no integration benchmark is silently skipped;
- repeated samples are recorded in the evidence file;
- setup outside the timed region is documented; and
- no timing assertion is added to an ordinary test.

If benchmark design cannot distinguish setup from the operation being measured,
record that limitation rather than presenting the result as operation latency.

## 7. Phase 1: Narrow the command-key unique index

### 7.1 Remove the `INCLUDE` payload

Change the baseline constraint to:

```sql
CONSTRAINT flow_commands_execution_key_uq
    UNIQUE (execution_id, command_key)
```

Remove every included column. Do not add a replacement covering index unless an
`EXPLAIN (ANALYZE, BUFFERS)` workload demonstrates a current query that becomes
materially worse and cannot use the narrow unique index plus heap access.

This is a correction to Plan 5's planned-at schema. The uniqueness key and
same-execution ownership constraint remain unchanged.

### 7.2 Update migration tests

Extend `migrations_test.go` to prove:

- the index is unique on exactly `(execution_id, command_key)`;
- `pg_get_indexdef` contains no `INCLUDE` clause;
- no duplicate command-execution index is introduced;
- the composite `(execution_id, command_id)` ownership key still exists; and
- parent, queue, wait, journal, and root ownership constraints remain valid.

Retain the existing expected inventory count unless this phase exposes a real
inventory mismatch. This phase should change index shape, not index count.

### 7.3 Compare query plans and update cost

Capture plans for:

- `WHERE execution_id=$1 ORDER BY command_key` over a 100-command execution;
- child-key conflict lookup with `command_key=ANY($2)`; and
- trace's command scan and queue join.

The narrow unique index should still provide execution/key lookup and ordering.
Heap reads are expected because the queries select non-indexed projection data.

Where practical, measure command state-transition updates before and after the
change and record whether HOT updates become possible. Do not add a fragile CI
assertion around PostgreSQL's cumulative statistics.

### 7.4 Phase 1 verification

Run:

```text
go test -count=1 -run 'TestMigration|TestSchema' -v .
make test
go vet ./...
```

Expected:

- migration/schema tests pass against a clean schema;
- the unique index definition has no included columns;
- all ownership/idempotency tests continue to pass; and
- no additional index appears.

## 8. Phase 2: Simplify journal allocation and make notifications meaningful

### 8.1 Reserve journal positions in one statement

Refactor `SemanticTx.Apply` so allocation uses one `UPDATE ... RETURNING`:

```sql
UPDATE <schema>.flow_executions
SET next_journal_position = next_journal_position + $2,
    updated_at = $3
WHERE execution_id = $1
RETURNING next_journal_position - $2;
```

Required behavior:

- validate the complete journal batch before reserving;
- reserve exactly `len(entries)` positions;
- construct rows from the returned first position;
- retain causation validation and deterministic order;
- append all rows with the existing atomic `CopyFrom` behavior;
- treat a missing execution row or invalid returned position as
  `ErrInvalidState`; and
- rely on the already-held execution lock rather than a redundant expected-value
  predicate.

Delete `nextJournalPosition` if no remaining caller needs it.

### 8.2 Remove success settlement's position pre-read

`SettleCommandSuccess` currently needs future event positions before `Apply` so
it can find matching waits. Reorder the transaction:

1. normalize and coalesce staged events;
2. validate the decision and construct the deterministic journal entries;
3. append the journal batch;
4. read each staged event's accepted position from the returned journal rows;
5. resolve waits using those positions;
6. update command, child, queue, wait, and execution projections; and
7. invoke `WithCommit` at its existing semantic point before final commit.

The journal insert remains invisible until commit. Moving wait resolution after
the insert does not expose partial state and lets new child insertion find a
staged event in the same transaction, preserving same-decision event/child gate
behavior.

Add an explicit assertion or helper that maps the stable journal layout:

```text
attempt conclusion
staged application events in normalized order
staged command-created entries in normalized order
parent command terminal event
optional child-cancellation and execution-terminal entries
```

Do not scatter arithmetic indexes without a named offset/helper and tests.

### 8.3 Remove notification from generic journal append

`SemanticTx.Apply` should append journal data only. Remove unconditional
`pg_notify` from this generic function.

Add a narrowly named semantic-transaction helper for a transactional command
wake, for example `NotifyRunnableCommands`. It must:

- issue at most one notification for one store operation;
- use the existing schema/database channel and versioned safe payload;
- execute inside caller-owned transactions without committing them;
- remain a latency hint only; and
- be called only after the operation has created immediately claimable work.

Immediately claimable means a queue row is in `ready` or `retry_wait` and its
`next_run_at` is not after the transaction's `DBNow`. A delayed child or future
retry does not benefit from an immediate notification; correctness polling
already observes it later.

Audit every `SemanticTx.Apply` call site and make notification intent explicit:

| Transition | Notify? |
|---|---|
| start with immediately runnable root | yes |
| start with unresolved waits or future delay | no |
| claim / `attempt_started` | no |
| success with no immediately ready child/wait release | no |
| success creating immediate ready children | once |
| success event releasing immediate ready commands | once |
| external event matching no wait | no |
| external event releasing immediate ready commands | once |
| terminal failure/cancellation/expiry with no new work | no |
| immediate retry/recovery made claimable | once |
| future retry | no |

When the transition performs several sources of readiness, coalesce them into
one notification.

### 8.4 Notification tests

Extend `notification_test.go` to prove:

- an immediately runnable root still wakes a remote runtime whose correctness
  poll is several seconds;
- an event releasing a gated command wakes the remote runtime;
- transaction rollback sends no visible wake;
- commit sends the wake only after commit;
- claim alone does not publish a command-ready hint;
- terminal settlement with no follow-up work does not publish a hint;
- an unrelated event matching no wait does not publish a hint; and
- reconnect catch-up plus correctness polling still recover missed hints.

Use a real listener or the existing notification fault seams. Do not make tests
depend on receiving an exact count when PostgreSQL is allowed to coalesce
duplicate identical transaction notifications; assert the semantic distinction
between no hint and at least one hint.

### 8.5 Phase 2 verification

Run:

```text
go test -count=1 -run 'Test.*Notification|TestDistributedNotificationAndReconnectCatchUp|TestSemantic' -v .
go test -count=1 ./internal/store ./...
make test
```

Expected:

- the next-position pre-read helper has no production references;
- journal tests retain gap-free positions and rollback creates no gaps;
- immediate runnable work retains notification latency behavior;
- journal-only transitions no longer notify; and
- caller-owned transaction notification behavior remains correct.

Record before/after ingress and simple lifecycle results in the Plan 5 benchmark
evidence.

## 9. Phase 3: Replace global readiness recomputation with delta resolution

### 9.1 Define the transition around affected waits

Replace the `resolveReadinessLocked` / `loadReadinessCommands` design with an
operation whose input is only accepted event tuples:

```text
(event_name, event_key, satisfying_journal_position)
```

The implementation must:

1. find and lock only unresolved wait rows matching those tuples;
2. enforce the current event-time-versus-wait-deadline rule using `DBNow`;
3. set each matched wait's `satisfied_position` once;
4. count newly satisfied waits per command;
5. decrement `flow_commands.unsatisfied_waits` by exactly that count;
6. transition only commands whose counter reaches zero from `pending` to
   `ready`;
7. set their retry budget start and next attempt consistently;
8. insert queue rows only for commands newly released; and
9. return whether any command is immediately runnable for notification gating.

Use the existing partial reverse index:

```sql
(execution_id, event_name, event_key, command_id)
WHERE satisfied_position IS NULL
```

Do not add an execution-wide command-state index merely to retain the old scan.

### 9.2 Use a set-oriented wait update

Pass incoming tuples with typed arrays/`unnest` or an equally bounded relation.
The preferred shape is a small number of statements:

1. one `UPDATE ... FROM incoming ... RETURNING command_id` for wait rows;
2. one grouped command update returning commands that reached zero; and
3. one bulk queue insertion for released commands.

A single data-modifying CTE is acceptable if it remains readable, reports row
counts, maps errors clearly, and preserves the ability to test critical
boundaries. Avoid a monolithic statement that hides every semantic transition.

Compute delayed `next_run_at` with existing checked Go duration helpers if doing
the timestamp arithmetic entirely in SQL would bypass current overflow and
exact-duration validation. One additional bulk statement is preferable to an
unchecked timestamp expression.

### 9.3 Remove impossible full-scan readiness states

Command creation already determines whether retained events satisfy each wait
and initializes pending wait timing. The schema requires a pending command to
have `unsatisfied_waits > 0`. Therefore an unrelated success does not need to
search for a pending command with zero waits.

After all creation paths initialize `wait_started_at` and `wait_deadline_at` in
the command insert itself, remove the global `resolution.waiting` pass. Consider
strengthening the pending-shape constraint to require `wait_started_at IS NOT
NULL`, but only after command insertion becomes a single valid statement. Do
not temporarily violate a new immediate constraint and repair it in a later
statement.

### 9.4 Separate ordinary readiness from fail-fast cancellation

Failure resolution is not event readiness.

- Optional failure and required failure with fail-fast disabled should not scan
  every command merely to recompute exact waits.
- Required failure with fail-fast enabled legitimately affects every open
  command. Load open commands once in stable command-key order, partition
  running survivors from cancellable work, and preserve deterministic journal
  cancellation order.
- Bulk-update cancelled commands and bulk-delete their queue rows using the
  journal positions returned for their cancellation entries.
- Do not call the ordinary readiness resolver a second time from failure
  resolution.

### 9.5 Convert every current caller

Inventory with:

```text
rg -n 'resolveReadinessLocked|applyReadinessResolution|matchingWaitsLocked|loadReadinessCommands' internal/store
```

Convert at least:

- successful settlement with staged events;
- external `Emit`/`Deliver` ingress;
- timely-event reconciliation during wait-expiry maintenance;
- optional command cancellation/expiry;
- required failure with fail-fast on and off;
- execution cancellation; and
- any lease/deadline transition that currently enters readiness indirectly.

At the end, no ordinary success or unrelated event should load all commands in
the execution.

### 9.6 Delta-readiness tests

Add or strengthen PostgreSQL tests for:

- one event satisfying the final wait of one command;
- one event satisfying only one of several waits;
- several events in one settlement satisfying several waits on one command;
- one event satisfying several commands;
- duplicate/equivalent event ingress decrementing no counter twice;
- a conflicting event rolling back all wait updates;
- an event at the exact persisted deadline winning;
- an event after the deadline not resurrecting the command;
- an event recorded before child declaration satisfying that child at creation;
- a staged event and new gated child in the same decision;
- an event matching no waits changing no command or queue row;
- optional terminal work not causing an execution-wide readiness scan;
- fail-fast preserving running survivors and cancelling only non-running open
  commands;
- fail-fast-disabled work continuing unchanged; and
- replay and trace reporting the same satisfying positions as projections.

Add a scale/query-plan test with at least 10,000 unrelated unresolved wait rows
and a small matching set. Require the reverse-wait index to appear in
`EXPLAIN (ANALYZE, BUFFERS)` and require returned/updated rows to be bounded by
the matches. Keep the large fixture opt-in if ordinary test time would become
material.

### 9.7 Phase 3 verification

Run:

```text
go test -count=1 -run 'Test.*Event|Test.*Wait|Test.*FailFast|Test.*Cancel|Test.*Replay' -v .
go test -count=1 ./internal/store ./...
make test
go vet ./...
rg -n 'resolveReadinessLocked|loadReadinessCommands' internal/store
```

Expected:

- all semantic tests pass;
- the final `rg` has no production matches;
- the exact reverse-wait index serves the scale plan;
- a no-event successful settlement executes no readiness query; and
- event cost grows with matched waits rather than total execution commands.

Repeat same-execution and event-ingress benchmarks and append results to the
evidence file before beginning Phase 4.

## 10. Phase 4: Batch staged events, commands, waits, and queues

### 10.1 Batch staged-event identity lookup

Keep in-memory normalization and duplicate/conflict detection. Replace one
`LookupApplicationEvent` call per staged event with one query over all normalized
event identities for the execution.

The query must return existing `(name, key, body)` values. Compare canonical
bodies exactly:

- equivalent retained events are omitted from the new journal batch;
- conflicting retained bodies reject the complete decision; and
- new events retain deterministic normalized order.

External single-event ingress may continue using its focused lookup. Do not make
the common one-event path marshal an artificial unbounded batch.

### 10.2 Resolve retained events for all new waits in one query

Before inserting a child batch:

1. flatten every normalized child's exact waits;
2. deduplicate lookup identities only for the database query;
3. query retained application-event positions once using the existing unique
   application-event identity index;
4. map positions back to every `(child command, event name, event key)` wait;
5. calculate each child's exact `unsatisfied_waits`, initial state, budget start,
   next attempt, wait start, and capped wait deadline in Go; and
6. validate every child completely before the first projection write.

One retained event may satisfy waits on several children. Store the same
immutable journal position in each corresponding wait row; do not duplicate the
event payload.

### 10.3 Bulk-insert commands

Replace repeated `insertCommand` calls with a batch helper that accepts one or
more already validated `CommandCreate` values plus their accepted journal
positions and computed timing/state.

Use `CopyFrom` or typed `unnest` to insert all `flow_commands` rows. Requirements:

- row order follows normalized command-key order;
- `created_position` maps exactly to the matching `command_created` journal row;
- root and parent same-execution foreign keys remain satisfied;
- all integer/duration bounds are validated before binding;
- affected row count equals the requested command count; and
- any database error rolls back journal, commands, waits, queues, counters, and
  application `WithCommit` changes.

Use the same helper for a one-command root only if doing so keeps start logic
clear and does not regress ordinary ingress. A focused one-row fast path is
acceptable when benchmarks justify it.

### 10.4 Bulk-insert waits and ready queue rows

Insert all `flow_command_event_waits` rows in one batch and all initially
ready `flow_command_queue` rows in one batch. No SQL statement should appear
inside a loop over waits or children.

Pending commands must be inserted with their final valid:

- `unsatisfied_waits`;
- `wait_started_at`;
- `wait_deadline_at`; and
- null budget/next-attempt fields.

Ready commands must be inserted with:

- zero unsatisfied waits;
- accepted budget start and next attempt;
- null wait timing unless the existing public trace contract requires retaining
  a completed wait start; and
- exactly one queue row.

### 10.5 Batch fail-fast cancellation of staged children

When a running survivor settles after an execution has entered fail-fast:

- retain the current command-created and command-cancelled history;
- insert staged child projections in a batch;
- bulk-update those child commands to `cancelled` with their individual terminal
  journal positions; and
- bulk-delete any queue rows that were provisionally created.

Preserve stable journal order and the rule that these children never become
claimable.

### 10.6 Preserve fault boundaries and `WithCommit`

Review every settlement fault hook before and after batching. The implementation
must retain coverage for failures:

- after the attempt fence;
- before/after journal application where currently promised;
- after child projection persistence;
- after event/wait projection persistence;
- before/after the application commit callback;
- before commit; and
- after an ambiguous commit.

Do not split `WithCommit` into another transaction or run it before the Flow
transition has been prepared enough to preserve existing atomicity and error
classification.

### 10.7 Batch-persistence tests

Add tests for:

- 100 children with no waits;
- children with distinct and shared waits;
- retained events satisfying all, some, or none of a child batch;
- staged events satisfying existing commands and same-decision new children;
- one invalid/conflicting child rolling back every child/event/result change;
- one conflicting staged event rolling back every child/result change;
- command-ceiling rejection before projection writes;
- exact command/open counters for large batches;
- journal-to-command created-position mapping;
- fail-fast survivor child cancellation;
- deferred root and parent foreign keys under batched insertion;
- caller-owned transaction rollback; and
- replay/trace equivalence after a mixed event/child decision.

### 10.8 Phase 4 verification

Run:

```text
go test -count=1 -run 'TestRuntimeStages|TestWorkerEvent|Test.*CommandCeiling|Test.*Fault|Test.*Replay' -v .
go test -count=1 ./internal/store ./...
make test
go vet ./...
rg -n 'for .*range.*' internal/store/ingress.go internal/store/commands.go internal/store/graph.go
```

Inspect every remaining loop reported by `rg`. Loops that only validate, encode,
sort, or prepare batch arguments are expected. No retained-event lookup, wait
insert, child insert, wait satisfaction, command cancellation update, or queue
delete should execute once per item.

Repeat fan-out and staged-decision benchmarks. Record query-shape and throughput
changes before Phase 5.

## 11. Phase 5: Increase claim throughput safely

### 11.1 Claim independent execution groups concurrently

Keep candidate probing, fair queue ordering, and process/queue slot reservation
centralized in `runCommandScheduler`. After candidates are selected and grouped
by execution, issue a bounded number of independent `ClaimCommands`
transactions concurrently.

Requirements:

- never put more than one execution in one claim transaction;
- preserve execution-first locking inside each transaction;
- reserve slots before issuing claims and release every unclaimed slot exactly
  once;
- keep queue fairness in candidate selection even though independent claims may
  finish in a different order;
- gather claim results before the next probe iteration, or otherwise prove that
  overlapping iterations cannot double-reserve capacity;
- start workers from the scheduler-owned lifecycle path so shutdown cannot call
  `Wait` concurrently with an unsafe `WaitGroup.Add` pattern;
- retain ambiguous-commit ownership resolution per execution group; and
- bound database claim concurrency so lease renewal and maintenance retain pool
  access.

Derive the initial internal bound from benchmark evidence and pool capacity. A
reasonable starting rule is capped at a small number such as 4-8 and must leave
at least two application-pool connections available when the configured pool is
large enough. Do not expose a public claim-concurrency option unless measurements
demonstrate that one internal policy cannot serve the supported deployment
shapes.

### 11.2 Test scheduler concurrency and shutdown

Add deterministic tests proving:

- claim transactions for different execution IDs may overlap;
- two candidates from one execution still enter one execution group;
- global and named-queue handler limits are never exceeded;
- failed/locked claims release their reserved slots;
- queue fairness remains bounded when one execution lock is busy;
- shutdown stops new claim work, drains in-flight claim operations, and does not
  race worker-group accounting;
- a pool with one or two available connections makes progress without
  maintenance starvation; and
- multiple runtime replicas retain correct fencing and no duplicate accepted
  settlement.

Use barriers/fault hooks rather than sleeps for overlap assertions where
possible.

### 11.3 Truly batch same-execution claim persistence

After independent claim concurrency is stable, refactor `Store.ClaimCommands`
so an ordinary eligible group performs work in sets rather than calling the
single-command transition repeatedly.

Target shape:

1. lock/load selected queue and command rows in one query with stable order and
   `SKIP LOCKED` behavior;
2. load all event-input rows for all locked command IDs in one query and group
   them in Go;
3. decode/validate retry policies and eligibility before mutation;
4. generate attempt IDs, lease tokens, expiry values, and attempt-started journal
   entries in stable order;
5. append all attempt-started entries in one journal batch;
6. bulk-update queue attempt/lease fields;
7. bulk-update command state and attempt ordinals; and
8. commit once and return claimed commands with the correct individual journal
   positions.

Preserve slow-path correctness:

- stale or locked candidates are skipped and their slots released;
- retry elapsed-budget expiry follows the existing terminal/fail-fast path;
- one malformed durable policy/input fails safely without accepting a partial
  claim batch;
- every returned attempt has the exact args, event snapshot, database time,
  deadline, timeout, lease expiry, and causation position currently promised;
- no attempt-started notification is sent; and
- ambiguous commit resolution remains possible for every returned fence.

If batching elapsed-budget terminalization with ordinary claims would make the
transaction difficult to audit, partition candidates after the locked read:
batch ordinary claims, then process the rare terminalization path with a focused
transition. Do not silently leave an expired candidate hot-looping in the queue.

### 11.4 Claim-batch tests

Add tests for:

- 16 immediately ready siblings claimed in one transaction;
- fewer available slots than siblings;
- one locked command row among otherwise claimable siblings;
- mixed registered command versions and queues;
- event inputs grouped to their correct commands;
- no-wait and 256-wait commands in the same group;
- retry elapsed expiry mixed with eligible siblings;
- exact attempt journal ordering and causation;
- one queue and command projection update per claimed item;
- claim rollback leaving no attempt rows or fences;
- ambiguous commit ownership resolution for multiple claims; and
- multiple replicas competing for the same execution without duplicate active
  fences.

### 11.5 Phase 5 verification

Run:

```text
go test -count=1 -run 'Test.*Claim|TestRuntimeQueueConcurrencyAndFairSelection|TestRuntimeCapacity|TestRuntimeCooperativeShutdown|TestDistributed' -v .
go test -race -count=1 -run 'Test.*Claim|TestRuntimeQueueConcurrencyAndFairSelection|TestRuntimeCooperativeShutdown' .
make test
go vet ./...
```

Repeat independent lifecycle and same-execution fan-out benchmarks. Expected
directional results:

- independent no-op executions improve materially when producer and worker
  concurrency exceed one;
- same-execution claim cost grows by batches rather than per-command round trips;
- worker, queue, and connection bounds remain effective; and
- no regression appears for long-running handlers where claim throughput is not
  the bottleneck.

Do not claim a multiplier until five repeated post-change samples are recorded.

## 12. Phase 6: Reduce event-input snapshot CPU and memory

### 12.1 Keep corruption detection without recanonicalizing twice

`flow_journal.body` is canonicalized and hashed at the accepted write boundary.
On claim:

1. calculate SHA-256 directly over the stored body bytes;
2. compare it to the stored `body_hash`;
3. decode the versioned `ApplicationEventBody` once;
4. require the supported positive body version;
5. validate that the payload is present, valid JSON, and within 64 KiB;
6. copy the already-canonical payload exactly once into the immutable attempt
   snapshot; and
7. let typed `GetEventValue` retain its existing type-decode behavior.

Do not run `canonical.Canonicalize` over the complete body and then again over
the nested payload on every claim/retry. Full replay/integrity paths may continue
to perform stronger canonical reconstruction where diagnostic certainty matters
more than hot-path allocation.

This trust boundary is limited to bytes written into Flow-owned tables through
Flow's validated journal path. If tests or external migration APIs can insert a
body and matching hash without canonical validation, fix that write boundary or
retain an explicit integrity mode; do not silently accept a weaker invariant.

### 12.2 Add a single-pass journal codec helper

If the generic `journalcodec.Decode` currently parses the version header and
then parses the body again, add a focused decoder that performs one typed decode
and validates `V` afterward. Keep unknown/zero versions rejected as
`ErrInvalidState` at the store boundary.

Do not loosen duplicate-key, malformed JSON, trailing-data, payload-size, or
typed worker-decode handling.

### 12.3 Snapshot tests and benchmarks

Test:

- one ordinary event input;
- 256 distinct inputs;
- malformed body bytes;
- body/hash mismatch;
- missing/zero/unknown body version;
- missing payload;
- payload above 64 KiB;
- selector metadata not matching the satisfying journal row;
- duplicate wait snapshots;
- retry and lease takeover receiving identical immutable payload bytes; and
- replay still detecting noncanonical or corrupt history as currently promised.

Benchmark at least:

- 1 x 1 KiB;
- 32 x 1 KiB;
- 256 x 1 KiB; and
- 256 x 64 KiB.

The adversarial 16 MiB case should show a substantial allocation reduction from
the approximately 390 MiB baseline. Use the measured result to decide whether a
future journal-body representation redesign is justified. Do not add a duplicate
event-payload column in this plan.

### 12.4 Phase 6 verification

Run:

```text
go test -count=1 -run 'Test.*EventInput|Test.*Snapshot|Test.*Replay|Test.*Journal' -v .
go test -run '^$' -bench 'Benchmark(EventSnapshotMaterialization|GetEventValueLookup)' -benchmem -benchtime=3s -count=5 .
make test
go vet ./...
```

Expected:

- malformed/corrupt durable data still fails safely;
- ordinary worker event-input behavior is unchanged;
- the maximum snapshot allocates materially less than baseline; and
- the benchmark evidence reports both bytes/op and allocations/op.

## 13. Phase 7: Document efficient primitive composition

Update active caller documentation with guidance that follows from the aggregate
architecture rather than exposing implementation details.

### 13.1 Command granularity

Document:

> A command should represent a retry, side-effect, isolation, or parallelism
> boundary, not every deterministic line of business logic.

Examples and guidance should show:

- keeping small deterministic transformations inside one worker;
- grouping several small same-database writes in one `WithCommit` callback;
- avoiding separate durable commands for microsteps whose lifecycle overhead is
  larger than their work;
- using child commands when independent retry, timeout, queueing, ownership, or
  concurrency is actually required; and
- keeping `WithCommit` callbacks free of remote calls and short enough not to
  hold the execution lock unnecessarily.

### 13.2 Execution granularity

Document that one execution is one serialized semantic aggregate:

- put causally related work in one execution;
- do not use one execution as a tenant-wide or global work container;
- use separate executions for independent bulk items/shards;
- treat the default 1,000-command ceiling as a safety limit, not a normal target;
- prefer ordinary executions in the tens or low hundreds of commands; and
- chunk very large fan-out into bounded batch commands when the work does not
  require one enormous atomic child declaration.

Do not turn these observations into a new hard public limit without separate
product evidence.

### 13.3 Event and payload granularity

Document:

- pass parent-produced data directly in child arguments;
- use exact events for sibling/cross-branch/external facts;
- stage related events and children in one worker decision;
- use hierarchical joins for large input sets;
- store large/sensitive documents in application storage and pass stable
  references; and
- keep caller-owned transactions short because execution locks are retained
  until the caller commits.

### 13.4 Synchronize design and evidence docs

Update architecture/schema descriptions to state that:

- readiness is delta-based through reverse waits and `unsatisfied_waits`;
- notifications are emitted only for newly immediate runnable work;
- decisions and claims use bounded set-oriented persistence;
- independent execution groups may be claimed concurrently while one execution
  remains serialized; and
- journal/input integrity validation is split deliberately between the accepted
  write boundary, hot claim path, and full replay diagnostics.

Update `specs/projects/flow/implementation_plan.md` to link Plan 5 and list its
completed outcomes only after implementation and evidence are complete.

## 14. Final verification and release evidence

### 14.1 Full functional gates

Run against a real PostgreSQL instance with integration tests configured to fail
rather than skip:

```text
gofmt -w <all changed Go files>
git diff --check
make build
go vet ./...
go test -count=1 ./...
make test
```

Expected: every command exits zero, `make test` includes the race detector, and
PostgreSQL-backed tests run.

Where the release matrix supports it, also run the full suite against the oldest
and newest supported PostgreSQL majors. Record versions and commands without
recording credentials.

### 14.2 Architecture-specific gates

Confirm with source/schema scans:

```text
rg -n 'resolveReadinessLocked|loadReadinessCommands' internal/store
rg -n 'nextJournalPosition' internal/store
rg -n 'INCLUDE .*state|unsatisfied_waits.*terminal_position' migrations
rg -n 'pg_notify' internal/store
```

Expected:

- no global readiness resolver remains;
- no redundant journal-position pre-read remains;
- the command-key unique constraint contains no mutable included projection;
- every remaining `pg_notify` call is behind an explicit runnable-work helper;
  and
- manual inspection finds no SQL round trip inside event/child/wait persistence
  loops.

Also confirm:

- exact six-table schema inventory;
- all foreign keys and unique lifecycle guards remain;
- no public API was added or removed;
- no coordinator/plan/state-machine symbols returned; and
- the production diff contains no unsafe durability tuning.

### 14.3 Before/after benchmark report

Run the exact same baseline commands, machine, PostgreSQL settings, pool size,
worker count, payloads, and sample counts used in Phase 0. Append a before/after
table to `plan_5_hot_path.md`.

Report at minimum:

- sequential execution ingress latency;
- independent complete commands/second at concurrency 1, 4, and 16;
- same-execution 10/100-command completion rate;
- external event ingress for independent and one-hot-execution targets;
- staged 10/100 child and event decision settlement;
- same-execution claim batch latency;
- maximum event-input snapshot time, bytes/op, and allocations/op; and
- 100-command journal rows and tuple bytes.

The intended directional goals are:

- simple successful settlement no longer performs execution-wide readiness
  work;
- staged decision query count becomes bounded rather than linear in items;
- independent claim throughput uses multiple database connections safely;
- same-execution claim persistence occurs per batch rather than per command;
- event ingress scales with matching waits rather than retained commands;
- command state transitions avoid rewriting a wide command-key index; and
- maximum event-input allocation falls substantially.

If an optimized workload regresses by more than normal repeated-sample variance,
investigate and document the cause before marking the phase complete. Do not
hide a regression by changing benchmark setup or payload shape.

## 15. Acceptance criteria

This plan is complete when all of the following are true:

1. Reproducible saturation benchmarks cover independent command lifecycles,
   same-execution fan-out, staged decisions, external event ingress, claims, and
   event-input snapshots.
2. Benchmark evidence includes exact environment, commands, repeated samples,
   limitations, and before/after results.
3. `flow_commands_execution_key_uq` is narrow and includes no mutable columns.
4. Journal position reservation uses one `UPDATE ... RETURNING` and success
   settlement performs no duplicate next-position read.
5. Generic journal append emits no notification.
6. At most one transactional notification is emitted when an operation creates
   immediate runnable work; journal-only transitions do not notify.
7. Ordinary success and event ingress perform no execution-wide command
   readiness scan.
8. Wait satisfaction updates only matching unresolved waits and decrements each
   affected command exactly once.
9. Commands reaching zero unsatisfied waits are transitioned and queued in
   bounded set-oriented operations.
10. Fail-fast scans open work once only when it intentionally affects all open
    commands; cancellation projection updates are batched.
11. Staged-event identity lookup, retained-event lookup for child waits, command
    insertion, wait insertion, and queue insertion are batched.
12. Large mixed decisions remain completely atomic with result, journal,
    `WithCommit`, counters, and replay.
13. Independent execution groups can be claimed concurrently within a bounded
    pool-safe limit.
14. Same-execution command claims append attempt history and update projections
    per batch rather than through a SQL loop per command.
15. Event-input claims verify integrity and decode accepted canonical payloads
    without recanonicalizing the full body and payload repeatedly.
16. All permanent/live key, event identity, deadline, retry, fencing, fail-fast,
    cancellation, caller-transaction, history, trace, replay, notification, and
    migration tests pass.
17. Public docs explain efficient command, execution, event, payload, and
    transaction granularity.
18. The schema remains exactly six tables, public API remains compatible, and
    the full PostgreSQL-backed/race/vet/build gates pass.

## 16. Punchlist

### Measurement

- [x] Add independent end-to-end command lifecycle benchmarks.
- [x] Add same-execution fan-out benchmarks for 10, 100, and opt-in 1,000 commands.
- [x] Add staged decision benchmarks for child/event/wait batch shapes.
- [x] Add external event-ingress benchmarks for independent and hot targets.
- [x] Add ordinary and maximum event-input snapshot shapes.
- [x] Record complete `d2713d8` baseline evidence.

### Command index

- [x] Remove the `INCLUDE` list from `flow_commands_execution_key_uq`.
- [x] Prove the unique key and same-execution ownership constraints remain.
- [x] Capture lookup/order/trace query plans with the narrow index.
- [x] Confirm no replacement duplicate index is added.

### Journal and notifications

- [x] Replace select-then-reserve with one allocator `UPDATE ... RETURNING`.
- [x] Remove success settlement's pre-read of the next journal position.
- [x] Make accepted journal position mapping explicit and tested.
- [x] Remove `pg_notify` from generic journal append.
- [x] Add one explicit transactional runnable-work notification helper.
- [x] Audit and classify every journal-apply call site for notification intent.
- [x] Add commit, rollback, remote wake, no-op event, claim, and terminal notification tests.

### Incremental readiness

- [x] Implement set-oriented matching wait satisfaction.
- [x] Group new wait satisfactions by command.
- [x] Atomically decrement `unsatisfied_waits` only for newly satisfied rows.
- [x] Transition and queue only commands reaching zero.
- [x] Remove execution-wide ordinary readiness scans.
- [x] Remove the redundant pending/wait-start sweep after creation paths are final-shape inserts.
- [x] Separate fail-fast open-command resolution from event readiness.
- [x] Batch fail-fast command updates and queue deletes.
- [x] Add exact-deadline, duplicate, multi-wait, multi-command, same-decision, and replay tests.
- [x] Add an indexed scale/query-plan gate for sparse matching waits.

### Batched decisions

- [x] Load all existing staged-event identities in one query.
- [x] Load retained events for all child waits in one query.
- [x] Compute final child/wait/queue shapes before persistence.
- [x] Bulk-insert command rows.
- [x] Bulk-insert wait rows.
- [x] Bulk-insert ready queue rows.
- [x] Batch cancellation of children staged by a fail-fast survivor.
- [x] Preserve fault hooks and `WithCommit` ordering.
- [x] Add 100-child, shared-wait, mixed event/child, conflict rollback, ceiling, and replay tests.

### Claims

- [ ] Add bounded concurrent claiming for independent execution groups.
- [ ] Preserve selection fairness and exact slot release.
- [ ] Leave pool capacity for lease/deadline maintenance.
- [ ] Add deterministic overlap, locked-group, small-pool, and shutdown tests.
- [ ] Lock/load one same-execution claim group in a batch.
- [ ] Load all claimed event inputs in one query.
- [ ] Append all attempt-started entries in one journal batch.
- [ ] Bulk-update command and queue claim projections.
- [ ] Preserve retry-expiry and ambiguous-commit slow paths.
- [ ] Add batch fencing, rollback, event-input grouping, and multi-replica tests.

### Event-input snapshots

- [ ] Replace full body recanonicalization with direct SHA-256 verification.
- [ ] Decode the versioned application-event body once.
- [ ] Validate and copy the already-canonical payload once.
- [ ] Retain full replay/integrity diagnostics.
- [ ] Add malformed/hash/version/size/selector/retry/takeover tests.
- [ ] Record before/after time and allocation measurements for all snapshot sizes.

### Documentation and release

- [ ] Document command granularity around retry/effect/isolation/parallelism boundaries.
- [ ] Document execution granularity as a bounded serialized aggregate.
- [ ] Document fan-out chunking, hierarchical joins, and stable external payload references.
- [ ] Document short `WithCommit` and caller-owned transaction guidance.
- [ ] Synchronize architecture, functional, schema, engine, overview, README, and package docs.
- [ ] Link completed Plan 5 outcomes from the active implementation plan.
- [ ] Run formatting, diff, build, vet, ordinary, PostgreSQL, and race gates.
- [ ] Run supported PostgreSQL-version coverage.
- [ ] Record final before/after benchmark evidence.
- [ ] Mark this plan complete only after every acceptance criterion is verified.

## 17. STOP conditions

Stop and report rather than improvising if:

- any in-scope transition has drifted materially from the excerpts and the new
  behavior is not represented in this plan;
- the baseline migrations have become released/immutable for real durable
  installations; in that case design a forward migration and compatibility
  plan before changing `001_initial.sql`;
- an optimization appears to require relaxing the execution-first lock, gap-free
  journal, transaction atomicity, attempt fence, or database-time semantics;
- delta readiness cannot preserve the exact wait-deadline rule or
  same-decision staged-event/new-child behavior;
- a batched update cannot map a distinct journal position to every affected
  command deterministically;
- batching removes a fault boundary required to prove rollback or ambiguous
  commit behavior and no equivalent seam is available;
- concurrent claims can starve lease/deadline maintenance or require unbounded
  goroutines/connections;
- event-input optimization would accept bytes from an unvalidated public or
  migration write path without equivalent integrity checking;
- implementation needs a new public API, new runtime table, duplicate payload
  projection, broker/cache, or coordinator/state-machine model;
- an expected performance improvement is absent and profiling contradicts the
  assumed bottleneck;
- a phase's focused or full verification fails twice after a reasonable scoped
  correction; or
- unrelated user changes overlap an in-scope file and cannot be preserved.

## 18. Alternatives considered and rejected

### 18.1 Remove the execution lock

Rejected. The lock is the simple authority for journal order, counters,
terminality, fail-fast, deadlines, and application `WithCommit` atomicity.
Fine-grained optimistic transitions would greatly expand the concurrency proof
and still serialize on a gap-free journal allocator held until commit.

### 18.2 Merge queue fields into `flow_commands`

Rejected. It would reduce table count but put frequent probe, claim, renewal, and
recovery churn on the wide semantic command row and its indexes. The narrow hot
queue projection remains the better physical design.

### 18.3 Remove the reverse wait table or `unsatisfied_waits`

Rejected. Deriving waits from journal/command scans would make event resolution
more expensive. The correct improvement is to use these projections
incrementally and atomically.

### 18.4 Drop unique lifecycle and ownership guards

Rejected. These constraints are inexpensive relative to the corruption and race
conditions they prevent. The command-key `INCLUDE` payload is removable because
it does not implement uniqueness; the narrow unique key itself remains.

### 18.5 Make the journal optional or collapse semantic entries immediately

Rejected for this plan. The journal supports history, trace, replay conformance,
causation, idempotency diagnostics, and attempt fencing evidence. Future
retention/compaction needs an explicit product contract rather than a hidden
performance mode.

### 18.6 Add a separate application-event payload table

Rejected for now. It adds a table and duplicate journal/projection writes. First
remove avoidable canonicalization and measure. Revisit the journal body format
only if the optimized claim path remains materially allocation-bound.

### 18.7 Add Redis, Kafka, or another scheduler

Rejected. Current bottlenecks are repeated scans, per-item round trips, serial
independent claims, index churn, and redundant decoding inside the PostgreSQL
transaction model. Another authority would complicate recovery without fixing
those costs.

### 18.8 Parallelize settlements within one execution

Rejected. Same-execution serialization is a deliberate aggregate boundary.
Scale independent work through separate executions and make each serialized
settlement shorter.

## 19. Maintenance notes

After implementation:

- any new event ingress path must use the incremental wait resolver rather than
  resurrecting an execution-wide command scan;
- any new queue insertion path must explicitly decide whether immediate runnable
  work warrants a transactional notification;
- any new batch field must preserve deterministic command/event order and exact
  journal-position mapping;
- any new indexed `INCLUDE` column should be reviewed for mutability and a real
  index-only query before adoption;
- any new scheduler concurrency must remain bounded by worker slots and database
  capacity while leaving maintenance headroom;
- event body format changes must benchmark claim memory as well as journal
  storage and replay compatibility; and
- throughput reviews must report retention cost. Even after hot-path
  optimization, the journal remains retained and storage capacity can dominate
  sustained high-volume operation.

Reviewers should scrutinize semantic equivalence and query shape before headline
benchmark gains. A faster transition that weakens an exact deadline, rollback,
fence, idempotency, or replay guarantee does not satisfy this plan.
