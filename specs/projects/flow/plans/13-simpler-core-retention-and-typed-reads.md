# Plan 13: Simplify the shipped core, make reads definition-safe, and bound retention

Status: In progress

Planned at: `7abfb8c` on 2026-08-12

- **Target release:** v0.4.0 candidate; this is an intentional breaking v0.x
  API and schema release, but implementation must not tag or publish a release
  without a separate operator decision
- **Priority:** P1 — simplify Flow around its primary Trails API consumer before
  adding another durable execution mode
- **Effort:** L, phased
- **Risk:** HIGH — this removes public behavior and projection columns, changes
  start result types, adds destructive retention, and renames inspection types
- **Depends on:** Plans 9 and 10 released in v0.3.0; Plan 7 lease and
  maintenance fixes retained; Plan 8 runtime/release verification guarantees
  retained except its migration-immutability policy, which this development-only
  reset deliberately supersedes
- **Precedes:** Plan 12. Plan 12 must be amended against this plan's reviewed
  final commit before implementation
- **Defers:** Plan 11 durable inline calls. Plan 11 is not implemented; its old
  migration-number proposal is discarded with the old schema history
- **Primary consumer target:**
  `/home/peter/Dev/0xsequence/trails-api`, currently consuming
  `github.com/goware/flow v0.3.3`
- **Database assumption:** development reset only. Existing Flow schemas and
  journal rows are disposable and must be dropped/recreated; there is no
  in-place upgrade or old-data compatibility path
- **Schema impact:** replace migrations 001-004 with one clean
  `001_initial.sql`; remove five current projection columns and the metadata GIN
  index, add one bounded-pruning index, and retain exactly six Flow tables
- **Public API impact:** intentionally breaking; keep both command-method and
  top-level forms of current-run/result reads
- **Durable history impact:** intentionally breaking and current-only. Remove
  retired fields and execution-era wire names from journal bodies, replay, and
  fingerprints; old retained history is unsupported

> **Executor instructions:** Read this plan completely before editing. Execute
> one phase at a time, run that phase's focused gates, and review its diff before
> continuing. Favor deletion and direct code over compatibility wrappers. This
> is a deliberate v0.x development reset: do not retain aliases, migration
> shims, old journal decoders, or dead fields merely to keep old callers or old
> databases working.
>
> The controlling product model remains:
>
> ```text
> Command definition --Enqueue--> Run
> Runtime.Run -----------claim----> attempt-local Work
> Work ----------------Enqueue----> staged child Command
> Work -----------------Emit------> staged Event
> Event + WaitFor ----------------> runnable Command
> ```
>
> Do not add `Task`, `Action`, `Step`, `Checkpoint`, hidden worker state in
> `context.Context`, a seventh table, a broker, or another delivery mode. If a
> change appears to require weakening attempt fencing, run-first locking,
> canonical validation of the new history, or same-database atomic settlement,
> STOP and report.
>
> **Initial drift check:**
>
> ```bash
> git status --short --branch
> git diff --stat 7abfb8c..HEAD -- \
>   definitions.go enqueue.go inspection.go readapi.go trace.go types.go node.go \
>   worker.go runtime.go command_runtime.go runtime_run.go migrations.go migrations \
>   internal/store internal/replay README.md flow.go doc.go examples \
>   specs/projects/flow
> git -C /home/peter/Dev/0xsequence/trails-api status --short --branch
> git -C /home/peter/Dev/0xsequence/trails-api rev-parse HEAD
> ```
>
> Reconcile every in-scope Flow change after `7abfb8c`. Compare the separately
> recorded Trails commit and status with Section 2.2 before using a disposable
> Trails worktree. A changed public contract, journal codec, settlement fence,
> or Trails ownership model outside the reset described here is a STOP condition
> until this plan is amended.

## 1. Purpose

Flow's durable engine is robust and is a better fit for Trails than a linear
task/checkpoint engine: Trails needs typed commands, several queue lanes,
run-scoped exact events, fan-out and joins, independently rooted provider and
edge lifecycles, same-database commit callbacks, and durable attempt fencing.
Those capabilities remain.

The shipped surface nevertheless retains features and names that Trails does
not need:

- current-run discovery accepts a raw command-name string, and Trails has
  several call sites that accidentally pass `Command.Queue()` because the
  current command name and queue happen to be equal;
- optional commands and configurable fail-fast behavior add projection
  columns, journal fields, replay state, and branches across every terminal
  transition, but Trails and the production examples never use them;
- run metadata duplicates the application's searchable domain data and is
  unused by Trails, while requiring two columns, canonicalization, filtering,
  and a GIN index;
- `flow_runs.input` duplicates the root command's canonical arguments;
- `Enqueue` returns a complete `Run` snapshot and stores the call-specific
  `Created` outcome on that durable snapshot type;
- inspection names such as `LiveWork`, `QueueDepth`, `Run.Type`, and
  `TraceCommand.State` do not match the current Run/Command vocabulary;
- Trails issues one queue-statistics query per configured lane; and
- Flow retains terminal runs, payloads, and journal rows indefinitely.

This plan removes those costs without changing the core command model. It also
caps the number of events one worker may stage in a decision, closing the last
unbounded durable settlement collection documented by Plan 8.

### 1.1 Coverage of the eight approved findings

The eight findings approved for Plan 13 are intentionally delivered as one
coherent breaking release. This table is the traceability map an implementer
and reviewer must use; none is an optional stretch goal.

| # | Approved finding | Controlling design | Delivery and proof |
|---:|---|---|---|
| 1 | Make command-specific reads definition-safe without removing the useful dynamic form | Sections 3.1, 3.2, and 4.1 | Phases 1 and 7; acceptance 1-3 |
| 2 | Remove optional commands and configurable fail-fast in favor of one failure rule | Section 3.3 | Phases 2-3; acceptance 5-6 and 12 |
| 3 | Bound retained durable data with an explicit safe pruning API | Section 3.7 | Phases 3 and 6; acceptance 11-12 |
| 4 | Remove unused run metadata and its canonical/indexing costs | Sections 3.4 and 5 | Phases 2-3; acceptance 5-6 and 12 |
| 5 | Stop storing root input twice while preserving permanent-key collision checks | Sections 3.5 and 5.3 | Phase 3; acceptance 7 and 12 |
| 6 | Return compact operation outcomes instead of pretending an enqueue result is a current Run snapshot | Section 3.6 | Phase 1; acceptance 4 |
| 7 | Make public vocabulary coherent and delete genuinely dead exports | Sections 4.2 and 4.4 | Phases 4 and 7; acceptance 8 |
| 8 | Remove the queue-stat N+1 and bound the remaining unbounded decision collection | Section 4.3 and Phase 5 | Phase 5; acceptance 9-10 |

Findings 2, 4, and 5 share one rewritten baseline schema because there is no
old-data rollout to preserve. Do not manufacture an upgrade migration or keep
private legacy fields when the accepted deployment procedure is to recreate the
Flow schema. Finding 8 groups two small hot-path bounds: one batched read for
all requested queues and one maximum for distinct staged application events.

## 2. Baseline evidence and primary consumer constraints

### 2.1 Flow baseline

At the planning commit:

- Flow owns exactly six tables: `flow_runs`, `flow_commands`,
  `flow_command_queue`, `flow_command_event_waits`, `flow_journal`, and
  `flow_schema_migrations`;
- the current development catalog was assembled by migrations 001 through 004;
- Plan 13 deliberately replaces that chain with one clean version-1 baseline;
- a run is the semantic lock and journal-allocation boundary;
- claims are grouped by run, use `SKIP LOCKED`, and install an attempt ID plus
  lease-token fence;
- worker invocation is at-least-once, while durable settlement is fenced;
- `WithCommit` joins short application writes to fenced settlement; and
- Flow has no supported archival or pruning API.

The retained Plans 9-10 benchmark record on durability-enabled PostgreSQL 18.1
reports candidate medians of approximately:

| Shape | Retained median |
|---|---:|
| ingress, polling | 5.025 ms |
| ingress, notification | 5.037 ms |
| independent lifecycle, 1 producer | 151.5 commands/s |
| independent lifecycle, 4 producers | 404.4 commands/s |
| independent lifecycle, 16 producers | 360.8 commands/s |
| one run, 10 commands | 87.14 ms |
| one run, 100 commands | 678.4 ms |
| 16-command same-run claim | about 2,900 commands/s |

These are regression baselines, not service-level promises.

### 2.2 Trails capabilities that must remain

The executor must inspect the live Trails repository before changing Flow and
must preserve all of these current production uses:

1. `DefineCommand` and `Handle` remain separate. Trails exports command
   definitions from `lib/intentmachine` and registers handlers in worker
   processes.
2. `Command.Queue()` and `WithQueue` remain. Trails uses multiple lanes with
   distinct process-local concurrency limits.
3. Root `Command.Enqueue`, worker `flow.Enqueue`, `WaitFor`, `Within`, `Delay`,
   `WithStartDelay`, and run deadlines remain.
4. Typed `Event`, staged `flow.Emit`, exact `GetEventValue`, and immediate
   targeted `Event.Deliver` remain. Independently rooted provider, OIF, and edge
   owners use `Deliver` to signal the current `intent.run`.
5. `WithLiveKey`, `GetCurrentRun`, and `ReplaceCurrentRun` remain. Trails uses
   them for one-current-owner generation semantics.
6. `WithCommit`, `Runtime.InTx`, `TransactionClient`, and
   `BeginApplicationWrites` remain. They are the atomic boundary for money and
   provider state.
7. `Trace`, retained history, active-command inspection, cancellation, retries,
   leases, notifications, observers, and migration/schema checks remain.
8. Independent provider/edge/OIF runs remain independent. Do not collapse all
   work for an intent into one hot run; one run is intentionally serialized.
9. The handler argument remains `*flow.Work[A]`. It names one attempt-local
   command invocation and is neither the whole Run nor the immutable Command
   definition.

### 2.3 Absurd lessons adopted and rejected

Adopt only these lessons from `/home/peter/Dev/other/absurd`:

- return a compact result from starting durable work; and
- provide explicit bounded cleanup for terminal durable state.

Do not adopt:

- a task/checkpoint vocabulary;
- handlers stored inside task definitions;
- hidden task state in `context.Context`;
- invocation-order checkpoint suffixes;
- queue-global events;
- same-queue blocking task waits; or
- per-queue physical tables.

Flow's fixed six-table schema and run-scoped event graph are simpler for the 16
current Trails lanes than creating five or six tables per queue.

### 2.4 Current source map for the executor

Use this map before editing; it identifies the present ownership boundaries so
the work does not drift into a new layer or duplicate SQL:

| Area | Current files and symbols |
|---|---|
| Command definitions and root options | `definitions.go`; `enqueue.go` (`RunOption`, `WithMetadata`, `WithFailFast`, start fingerprints, `Command.Enqueue`, `Command.ReplaceCurrentRun`) |
| Worker decision construction | `worker.go` (`Work`, `Emit`, `Enqueue`, `decisionState`); `node.go` (`Node`, `Optional`, `WaitFor`, `Within`, `Delay`) |
| Public run/result/queue reads | `inspection.go` (`RunFilter`, `GetRun`, `GetCurrentRun`, `GetResult`, `GetQueueDepth`); `trace.go` (`Run`, `TraceCommand`, `Trace`) |
| Bounded operator reads | `readapi.go` (`LiveWork*`, `ListLiveWork`, `ListHistoryByKeys`) |
| Start, replacement, identity, and batch inserts | `internal/store/ingress.go` (`StartRequest`, `StartResult`, `StartInTx`, `ReplaceCurrentRunInTx`, prepared command/event batches) |
| Failure and terminal transitions | `internal/store/commands.go`, `internal/store/ingress.go`, and `internal/store/graph.go`; preserve their semantic-transaction run-first locking and attempt fences |
| Current projections and queue statistics | `internal/store/inspection.go`; bounded operator queries are in `internal/store/readapi.go`; trace overlays are in `internal/store/trace.go` |
| Current journal and replay shape | `internal/store/journalcodec/journalcodec.go` and `internal/replay/replay.go`; delete retired fields/names and keep canonical/hash/state-machine tests beside both packages |
| Clean baseline schema | `migrations.go`, `migrations/*.sql`, and `migrations_test.go`; consolidate the final catalog into 001 and delete 002-004 |
| Observer default | `observer.go` and runtime construction/use sites; only the exported no-op implementation is removed |
| Primary Trails integration | `lib/intentmachine` owns definitions/current-run helpers; `rpc/info_workers.go` performs the production queue-stat loop; `workers` and Flow-focused tests cover lane behavior |

Follow the existing layering: public functions validate/convert types, one
private/store operation owns each SQL shape, and journal codecs remain private.
Do not move durable reads into command definitions or put database handles in
`Command` values merely to support method syntax.

## 3. Controlling decisions

### 3.1 Keep both forms of command-specific reads

Commands are already typed. Run keys and command keys remain application-owned
strings. This plan does **not** introduce typed key wrappers.

Add command methods as the preferred application form:

```go
run, found, err := IntentRunCommand.GetCurrentRun(ctx, client, runKey)

result, found, err := FinalizeOrder.GetResult(
	ctx, client, runID, "finalize",
)
```

Retain top-level forms for generic operator code that knows a definition name
only at runtime or prefers the existing function style:

```go
run, found, err := flow.GetCurrentRun(
	ctx, client, rootCommandName, runKey,
)

result, found, err := flow.GetResult(
	ctx, client, runID, "finalize", FinalizeOrder,
)
```

The top-level `GetCurrentRun` parameter must be named and documented as
`rootCommandName`, never `typ`. Passing a queue remains syntactically possible
for the dynamic escape hatch, but application docs and examples must prefer the
method form.

Both forms must delegate to one implementation and have identical transaction,
not-found, corruption, and conflict behavior. Do not duplicate SQL.

`Command.GetCurrentRun` is definition-name safe, not version-filtered. It must
preserve the current meaning of “the live run holding this key for this root
command family,” including finding an older command version still in progress.
It validates the supplied `Command` and derives `Command.Name()`, but it must
not reject or hide a live run merely because `Run.RootCommandVersion` differs.
By contrast, `Command.GetResult` must retain the existing exact stored command
name-and-version check before decoding `R`.

### 3.2 Keys stay strings

A run key identifies one root run inside a command definition. A command key
identifies one command inside a run. They are domain identities such as
`intent:<id>`, `transaction:<id>`, or `finalize`; Flow must not prescribe their
shape. Application helpers such as Trails's `IntentEntityKey` remain the right
way to construct them.

### 3.3 One failure rule replaces optional/fail-fast modes

All runnable state uses one rule:

> A command that becomes terminal unsuccessfully fails its run and
> cancels every open command that has no active attempt. Already-running
> attempts remain fenced survivors and may settle. The run becomes terminal
> only after those survivors conclude.

“Terminal unsuccessfully” includes permanent/exhausted command failure,
command cancellation, command expiry, wait expiry, and fail-before-claim
terminalization. It does not include a retryable conclusion that schedules a
future attempt. Whole-run cancellation and run-deadline expiry keep their
existing aggregate behavior. Retain `RunStatusFailing` and a `run_failing`
journal entry: this plan removes a configuration choice, not the durable
intermediate state needed to drain fenced survivors.

After this plan every command is required, so public and projection vocabulary
should simply say “command”; do not replace `required` with another boolean or
enum. A best-effort operation should return a typed outcome rather than fail its
command. A failure that must not affect another run belongs in an independent
run.

Delete `fail_fast` and `required` from journal bodies, replay state,
fingerprints, store request/row structs, test-engine shapes, and all current
tests. Rebuild the clean v1 start/declaration fingerprint records from only the
remaining declaration fields. There is no decoder or compatibility fixture for
old optional/non-fail-fast history.

### 3.4 Remove run metadata completely

Remove public run metadata and metadata filtering. Trails stores searchable
domain attributes in its application tables, and no production Trails caller
uses `WithMetadata` or `RunFilter.Metadata`.

Delete metadata from the current journal body, replay state, start fingerprint,
store requests/rows, validation constants, public filters, and tests. Do not add
a replacement tags/labels API or a decoder for the old metadata-bearing body.
Existing schemas are dropped, not inspected or upgraded.

### 3.5 Remove the duplicate run input projection

`flow_runs.input` and the start journal body's input both duplicate the root
command's `flow_commands.args`. Remove both copies. Permanent-key rediscovery
must preserve the current collision guard by loading canonical root arguments
through `root_command_id` and comparing them with the requested input in
addition to the start fingerprint. Replay obtains the root arguments from the
root `command_created` record. Do not remove root command arguments or weaken
permanent-key equivalence.

### 3.6 Compact operation results are not state snapshots

Replace the current `Run.Created` mixed model with:

```go
type EnqueueResult struct {
	RunID   RunID
	Created bool
}

type ReplaceRunResult struct {
	RunID    RunID
	Replaced bool
}
```

`Command.Enqueue` returns `EnqueueResult`. `Command.ReplaceCurrentRun` returns
`ReplaceRunResult`. `GetRun`, `GetCurrentRun`, `ListRuns`, `AwaitRun`, and
`Trace` return the full durable `Run` snapshot, which no longer has `Created`.

Preserve Plan 10's replacement rule while compacting the result: when the
current live run ID equals `expected`, replace it even if the new declaration
is equivalent. Declaration equivalence is used only when the current run ID is
different from `expected`: return that equivalent current run with
`Replaced=false`, or return `ErrConflict` if its declaration differs. The
compact result's `RunID` is the accepted current/new run in every successful
case.

The store start path should return only what the public result needs. A
rediscovered start must not issue a full `GetRun` projection query merely to
return an ID and `Created=false`. Preserve all conflict comparison and locking.

### 3.7 Retention is explicit, bounded, and aggregate-scoped

Add exactly one initial destructive API:

```go
type PruneResult struct {
	Runs           int64
	Commands       int64
	JournalEntries int64
}

func PruneTerminalRuns(
	ctx context.Context,
	runtime *Runtime,
	finishedBefore time.Time,
	limit int,
) (PruneResult, error)
```

Semantics:

- `finishedBefore` is exclusive and must be non-zero;
- `limit` must be 1 through 1,000; there is no unbounded/default delete;
- eligible runs are terminal and either live-keyed or unkeyed;
- permanent non-empty keys are never eligible because deletion would erase
  permanent idempotency ownership;
- candidates are ordered by `(finished_at, run_id)`, locked with
  `FOR UPDATE SKIP LOCKED`, and limited before deletion;
- the entire run aggregate is deleted in one Flow-owned transaction;
- the API accepts `*Runtime`, not `Client`, and cannot join a caller-owned
  transaction;
- only Flow's six tables are in scope; pruning never deletes, archives, or
  infers ownership of application rows written through `WithCommit`; and
- no automatic retention goroutine, TTL option, partitioning, tombstone table,
  observer event, or application-archive protocol is added.

Delete in an FK-safe, set-oriented order: selected journal rows, selected
commands (letting command queue/waits cascade), and then selected runs. The
deferred root ownership constraint must be satisfied when the transaction
commits. Do not weaken or remove ownership foreign keys to make pruning easier.
`Runs`, `Commands`, and `JournalEntries` report actual SQL rows deleted. The
run delete count must equal the number selected; otherwise return
`ErrInvalidState` and roll back rather than reporting a partial prune.

### 3.8 Plan ordering

Implement Plan 13 before Plan 12. This plan removes state and rewrites the
command/start/claim-adjacent public shapes on which Plan 12 is based. After Plan
13 is reviewed, amend Plan 12 at the exact final commit. Its amendment must
design mixed-lease renewal explicitly because the current runtime uses one
global renewal ticker and one lease duration per batch.

Plan 11 remains deferred. If a concrete consumer later needs durable inline
subroutines, write a new plan against the post-Plan-13 schema. Its old migration
005 proposal has no standing after the migration chain is consolidated.

## 4. Target public surface

### 4.1 Definition-bound reads

Add:

```go
func (cmd Command[A, R]) GetCurrentRun(
	ctx context.Context,
	c Client,
	key string,
) (Run, bool, error)

func (cmd Command[A, R]) GetResult(
	ctx context.Context,
	c Client,
	id RunID,
	key string,
) (R, bool, error)
```

Retain:

```go
func GetCurrentRun(
	ctx context.Context,
	c Client,
	rootCommandName string,
	key string,
) (Run, bool, error)

func GetResult[A, R any](
	ctx context.Context,
	c Client,
	id RunID,
	key string,
	cmd Command[A, R],
) (R, bool, error)
```

The command method validates the command definition and derives its own name.
`Command.GetResult` also derives the exact result type, name, and version.

Preserve existing absence semantics: `GetCurrentRun` returns `found=false` only
when no live-key holder exists; `GetResult` returns `found=false` when the run
exists but the command is absent or has no successful result. A missing run is
still `ErrNotFound`, and a stored command with a different name or version is
still `ErrConflict`. Both method and top-level forms must work through a
`TransactionClient` and observe the caller transaction exactly as today.

### 4.2 Renamed current-state types

Perform these breaking renames without aliases:

| Current public name | Target public name |
|---|---|
| `Run.Type` | `Run.RootCommandName` |
| `Run.Version` | `Run.RootCommandVersion` |
| `Run.Key` | `Run.RunKey` |
| `TraceCommand.State` | `TraceCommand.Status` |
| `Node` | `StagedCommand` |
| `LiveWork` | `ActiveCommand` |
| `LiveWorkFilter` | `ActiveCommandFilter` |
| `LiveWorkPage` | `ActiveCommandPage` |
| `ListLiveWork` | `ListActiveCommands` |
| `ListHistoryByKeys` | `ListHistoryByRunKeys` |
| `QueueDepth` | `QueueStats` |
| `RunFilter.Type` | `RunFilter.RootCommandName` |

`flow.Enqueue` returns `*StagedCommand`. Keep the fluent `WaitFor`, `Within`,
and `Delay` methods. `Optional` is deleted.

Renaming the active-command read cursor kind is allowed to invalidate opaque
v0.3 cursor strings. Document that cursors are release/query specific and must
not be persisted as durable application identity.

### 4.3 Batched queue statistics

Replace single-lane `GetQueueDepth` with one batch-capable operation:

```go
func GetQueueStats(
	ctx context.Context,
	c Client,
	queues ...string,
) (map[string]QueueStats, error)
```

Rules:

- validate and resolve `Client` first; with a valid client, zero queues returns
  a non-nil empty map without SQL;
- accept at most `MaxReadKeys` inputs before duplicate removal;
- validate every queue using the same definition-name validation used by
  `WithQueue`;
- deduplicate repeated queue names;
- return one map entry for every requested distinct queue, including all-zero
  statistics for a lane with no rows;
- capture one statement-stable PostgreSQL timestamp for every lane; and
- issue one grouped query, not one query per lane.

Use the existing semantics explicitly: capture `clock_timestamp()` exactly
once in a materialized one-row CTE (or an equally provable single-evaluation
shape), then classify every requested lane against that same value. Do not call
`clock_timestamp()` independently in per-lane aggregates.

Keep `QueueStats.Queue`, `Ready`, `Delayed`, `Running`, and `OldestReadyFor`.
Do not add mutable queue counters or another table.

### 4.4 Remove dead/redundant exports

Delete without aliases:

- `WithFailFast`;
- `WithMetadata`;
- `Node.Optional` as part of the `StagedCommand` rename;
- `Run.FailFast`;
- `Run.Metadata`;
- `Run.Created`;
- `TraceCommand.Required`;
- `TraceOption` and the variadic `Trace` parameter, because there are no trace
  option constructors;
- `CommandFailure`, retaining `Failure`;
- `StatusSucceeded`, `StatusFailed`, `StatusCancelled`, and `StatusExpired`,
  retaining the explicit `CommandStatus*` constants; and
- public `NopObserver`; use a private no-op observer or nil-safe adapter as the
  runtime default.

This is a final-state inventory, not a second implementation pass. Phase 2
removes the semantic options/fields (`WithFailFast`, `WithMetadata`,
`Optional`, and their projections); Phase 4 performs the remaining pure
vocabulary/dead-export cleanup and verifies the whole list is absent.

Do not remove `ResultOf`, `History` options, `Command.Queue`, `EventRef`,
`Registration`, or sealed option interfaces merely because Trails uses them
infrequently. They express real supported behavior.

## 5. Target clean schema and current-only durable format

### 5.1 Consolidate the migration chain

This plan has explicit operator authorization to discard all existing Flow
database state. Rewrite `migrations/001_initial.sql` so it directly creates the
final Run-named catalog, fold in the still-required live-key, release-read, and
ownership changes from migrations 002-004, then delete:

```text
migrations/002_live_keys.sql
migrations/003_release_read_paths.sql
migrations/004_run_vocabulary.sql
```

Do not add migration 005. Register only the consolidated `initial` migration at
schema version 1 with reader/writer versions 1/1. Keep the migration ledger,
checksums, `Migrate`, `MigrationFS`, `CheckSchema`, and exact six-table catalog;
they are useful infrastructure for the clean baseline and future releases.

An existing schema with the old migration checksum/ledger is unsupported and
must continue to fail normal checksum/unknown-migration validation with
`ErrSchema`; do not add special old-schema detection code. It must not be
upgraded, partially modified, or silently accepted. Update README and package
migration docs with the one operator action: drop and recreate the Flow schema.

### 5.2 Final projection catalog

The consolidated baseline omits these current columns/index from creation:

```text
flow_runs.input
flow_runs.metadata
flow_runs.metadata_canonical
flow_runs.fail_fast
flow_commands.required
flow_runs_metadata_idx
```

It adds one partial pruning index equivalent to:

```sql
CREATE INDEX flow_runs_prune_idx
    ON {{schema}}.flow_runs (finished_at, run_id)
    WHERE finished_at IS NOT NULL
      AND (run_key = '' OR key_scope = 'live');
```

Use Run vocabulary in table, column, constraint, and index names from the first
statement; the baseline must never create a `flow_executions` table or
`execution_id` column and then rename it. The exact pruning predicate must match
the candidate query and exclude permanent non-empty keys.

### 5.3 Remove legacy fields from journal, replay, and fingerprints

Define one clean current format rather than a compatibility decoder:

- restart the reset-only `RunStartedBody`, `CommandCreatedBody`, start
  fingerprint, and declaration-fingerprint shapes at internal version 1; do not
  label the clean shape version 2 merely to describe data that will not exist;
- `RunStartedBody` uses JSON keys `run_id` and `run_key` and has no `input`,
  `metadata`, or `fail_fast` fields;
- `CommandCreatedBody` has no `required` field;
- replay `Run`/`Command` state has no `Input`, `Metadata`, `FailFast`, or
  `Required` fields;
- the start fingerprint still includes the canonical root input because input
  is durable identity, but removes metadata and fail-fast; and
- the command declaration fingerprint removes required while retaining every
  actual declaration property (definition/version, canonical args, queue,
  retry/timeout/delay/waits/within, and parent identity where currently used).

The root `command_created` body is the one retained journal copy of root args.
Replay discovers root args when that record follows `run_started`; it must still
validate exactly one root, command counts, canonical bodies/hashes, contiguous
positions, causation, and terminal state transitions.

### 5.4 Finish the Run vocabulary in durable strings

Because old rows are unsupported, replace the remaining execution-era durable
names instead of carrying exceptions:

| Current string | Clean string |
|---|---|
| `execution_started` | `run_started` |
| `execution_failing` | `run_failing` |
| `execution_terminal` | `run_terminal` |
| `flow.execution_succeeded` | `flow.run_succeeded` |
| `flow.execution_failed` | `flow.run_failed` |
| `flow.execution_cancelled` | `flow.run_cancelled` |
| `flow.execution_expired` | `flow.run_expired` |
| failure code `execution_expired` | failure code `run_expired` |

Apply these strings consistently to journal constraints/indexes, store entry
kinds, history constants, replay switches, runtime event construction, failure
projections, tests, examples, and active docs. Keep the semantic distinction
between command-terminal and run-terminal events. Do not rename ordinary Go
verbs such as `executeClaim`, or rewrite historical plan/evidence prose.

The clean replay suite must build current bodies through production codec
helpers and cover success, failure with running survivors, cancellation,
expiry, waits, retries, application events, corrupt hashes/noncanonical bodies,
and unknown body versions. There are no old-body fixtures.

## 6. Implementation phases

Each phase must finish with a focused test run, `go test` compile coverage for
all packages, `go vet ./...`, `gofmt`, and `git diff --check`. Obtain a cold diff
review after Phases 2, 3, 5, and 7 before proceeding.

### Phase 0: Freeze the baseline and inventory consumers

1. Record:

   ```bash
   git status --short --branch
   git rev-parse HEAD
   git describe --tags --always
   sha256sum migrations/001_initial.sql migrations/002_live_keys.sql \
     migrations/003_release_read_paths.sql migrations/004_run_vocabulary.sql
   go version
   go env GOOS GOARCH
   ```

2. Run the current Flow build, vet, ordinary PostgreSQL, and race suite against
   a real configured PostgreSQL 17 or 18 server. Named database tests must not
   skip.
3. Inventory all Flow and Trails references to every removed/renamed symbol and
   save the counts in implementation evidence.
4. Record current schema table, column, index, constraint, and migration-ledger
   inventories.
5. Run the retained benchmark shapes listed in Section 10 with at least three
   samples on one durability-enabled PostgreSQL server. Record environment and
   exact commands.
6. Confirm the live Trails working tree state. Because it may contain unrelated
   owner work, do not edit it in place; later consumer-proof work must use a
   disposable worktree or be performed by the Trails owner.

**Gate:** no existing named test failure, named skip, schema mismatch, or
unexplained benchmark failure. Existing unrelated dirty work in either repo is
a STOP condition unless the operator explicitly scopes around it.

### Phase 1: Add definition-bound reads and compact start results

1. Add failing tests for both `Command.GetCurrentRun` and the retained
   top-level function. Use a command whose name differs from its queue. Prove:
   - the method derives the command name and finds the live run;
   - the top-level call with `cmd.Name()` finds it;
   - the top-level call with `cmd.Queue()` does not accidentally find it; and
   - transaction visibility and `found=false` remain unchanged.
2. Add `Command.GetResult` tests beside existing `GetResult` coverage. Both forms
   must return the same typed value and the same conflict/corruption errors.
3. Introduce one private implementation per operation; both public forms call
   it.
4. Add `EnqueueResult`, change root `Enqueue`, and remove `Run.Created`.
5. Change `ReplaceRunResult` to `{RunID, Replaced}`.
6. Narrow `store.StartResult` and replace-start plumbing so ordinary start and
   rediscovery return only the accepted run ID and operation outcome. Preserve
   complete permanent-key identity checks, live-key locking, ambiguous commit
   recovery, transaction visibility, notification behavior, and observations.
7. Update every Flow example and test to call `GetRun`/`AwaitRun` only where a
   state snapshot is actually needed.

**Focused verification:** enqueue/idempotency, replacement concurrency,
transaction-client, notification, current-run, typed-result, ambiguous commit,
and examples under PostgreSQL and `-race`.

### Phase 2: Remove optional/fail-fast and metadata semantics

1. Add characterization tests for the one current default behavior before
   deleting branches: every command is required, run failure is fail-fast, and
   running survivors retain their fences.
2. Make current commands unconditionally required and current runs use the one
   reduced fail-fast rule in Section 3.3.
3. Delete public `Optional`, `WithFailFast`, and their public projection fields.
4. Delete current transition branches whose only purpose was optional or
   fail-fast-disabled behavior. Keep the running-survivor fence rule.
5. Delete `WithMetadata`, metadata filtering, and public metadata projections.
6. To keep this phase testable before the schema reset in Phase 3, old
   NOT-NULL insert/body fields may remain temporarily as private constants only.
   Do not add a decoder branch or compatibility abstraction around them. Phase 3
   must delete those temporary fields, codecs, and writes completely.
7. Replace optional/fail-fast behavior tests with tests for the one rule:
   - queued/delayed/pending siblings cancel after a failed command;
   - running siblings remain fenced and may settle;
   - staged children accepted after the run enters failing are recorded
     cancelled according to the existing survivor rule;
   - run counters and journal order are exact; and
   - retryable failures do not enter terminal failure early.

**Focused verification:** all failure, cancellation, expiry, wait-deadline,
retry, fail-before-claim, replay, trace, and same-decision event/child tests
against PostgreSQL and under `-race`.

### Phase 3: Consolidate the clean schema and durable format

1. Rewrite `migrations/001_initial.sql` as the direct final Run-named schema,
   fold in the retained behavior/indexes from 002-004 plus the prune index, and
   delete migration files 002-004. Update the registry to one version-1,
   reader/writer-1 migration. Add no migration 005.
2. Replace upgrade/refusal/checksum-preservation tests with clean-baseline
   tests: empty install, idempotent rerun, external `MigrationFS` install,
   exact columns/indexes/constraints, exactly six tables, and the existing
   generic checksum/unknown-ledger rejection with no mutation.
3. Remove `Input`, `Metadata`, `FailFast`, and `Required` from current store
   request/row/projection structs and all SQL where they are no longer
   meaningful.
4. Change permanent-key rediscovery to compare requested canonical input to the
   root command's canonical `args`, loaded using the run's non-null
   `root_command_id`. Retain fingerprint, definition version, key scope, and
   root-definition comparison.
5. Replace start/command journal bodies, fingerprints, replay fields, entry
   kinds, event classes/names, and failure codes with the current-only shapes
   and Run vocabulary in Sections 5.3-5.4. Delete old fixtures and decoders.
6. Prove a freshly created permanent run is rediscovered successfully from the
   root command args and compact start data.
7. Prove a different root input still returns `ErrConflict` after the duplicate
   run input column is gone.
8. Add and EXPLAIN the pruning candidate index path using enough terminal rows
   to prevent a cost-equivalent sequential scan from hiding a missing index.

**STOP:** do not add an upgrade shim, compatibility migration/body decoder, or
old-data conversion. Do not weaken the root/parent/queue/wait/journal ownership
constraints or add a seventh table.

### Phase 4: Perform the vocabulary and public-surface cleanup

1. Apply the exact rename table in Section 4.2 across implementation, tests,
   examples, README, package docs, functional spec, architecture, components,
   and benchmark labels where they describe public vocabulary.
2. Remove the dead exports in Section 4.4 without compatibility aliases.
3. Change `Trace` to exactly `Trace(ctx, client, runID)` and delete empty option
   application logic.
4. Make the runtime's default observer private while preserving `WithObserver`
   validation and bounded observer delivery.
5. Update `go doc` examples so the first visible path uses command methods for
   definition-bound reads.
6. Verify the durable Run-string cleanup in Section 5.4. Ordinary Go verbs such
   as `executeClaim` and explicit negative compile-contract tests are not old
   domain aliases and need not be renamed.

**Static gate:** outside historical plan/evidence prose and explicit negative
compile-contract tests, production code, active tests, examples, and active docs
have no removed exports or execution-era domain/wire strings.

### Phase 5: Batch queue statistics and cap staged events

1. Add `QueueStats` and batch `GetQueueStats` with the exact behavior in Section
   4.3.
2. Implement one set-oriented query using requested queue names and one
   statement-stable timestamp. Return zero entries for empty lanes in Go or via
   a left join.
3. Replace the internal single-queue query and query-plan test. Prove the
   existing `(queue, state, next_run_at)` release-read index supports the new
   query. Add no new queue index without evidence.
4. Add tests for zero inputs, one lane, 16 mixed lanes, duplicate names, invalid
   names, too many inputs, nil client, caller-transaction visibility,
   delayed/ready/running classification, and one shared observation time.
5. Add a private maximum of 256 distinct staged application events per worker
   decision. Re-emitting the exact same `(event name, key, payload)` is
   idempotent and does not consume another slot; a 257th distinct event poisons
   the decision with `ErrInvalid` before successful settlement. Keep the
   existing decision-error classification path; this plan does not invent a
   special retry or terminal-failure class for event overflow. “Distinct” means
   a new accepted `(event name, event key)` identity. At the 256-event boundary,
   preserve error precedence: an exact duplicate remains a no-op, a duplicate
   identity with different payload remains `ErrConflict`, and only a valid new
   identity returns the capacity `ErrInvalid`.
6. Add decision and PostgreSQL rollback tests proving 256 succeeds, 257 fails
   with no successful result, application-event rows, child commands,
   application commit callback, or related queue/counter changes, and
   duplicates do not count twice. The claimed attempt still concludes through
   the ordinary retry/failure path, so its expected attempt/failure journal and
   projection writes must be asserted rather than incorrectly forbidden.

**Performance gate:** a 16-lane statistics request executes one SQL statement,
and staged decision benchmarks show no material regression for existing 0/10/100
event shapes.

### Phase 6: Add bounded terminal-run pruning

1. Add store-level tests for the candidate query and aggregate deletion before
   the public API.
2. Use the `flow_runs_prune_idx` created by the consolidated baseline and ensure
   the production candidate query predicate matches it exactly.
3. Implement `PruneTerminalRuns` and `PruneResult` exactly as Section 3.7.
4. Select and lock at most `limit` eligible run IDs. Delete journal rows,
   commands, and runs set-wise in the same transaction. Count actual deleted
   rows; do not derive counts from stale projections.
5. Tests must cover:
   - unkeyed terminal success/failure/cancellation/expiry;
   - terminal live-key generations;
   - permanent non-empty keyed terminal runs never deleted;
   - non-terminal runs never deleted;
   - exclusive cutoff and deterministic limited batches;
   - complete removal of queue, waits, commands, journal, and run rows;
   - an empty batch;
   - invalid cutoff/limits and nil runtime;
   - concurrent pruners sharing work through `SKIP LOCKED` without duplicate
     counts or blocking indefinitely;
   - a run locked by another semantic transaction being skipped while a later
     candidate is pruned;
   - concurrent `Trace`/`GetRun` observing either a complete pre-delete snapshot
     or `ErrNotFound`, never a partial aggregate; and
   - runtime scheduler/maintenance activity continuing while unrelated terminal
     runs are pruned.
6. Run an `EXPLAIN (ANALYZE, BUFFERS)` test with at least 10,000 ineligible or
   newer rows and a small eligible batch; assert the partial index participates
   and the changed/deleted row count is bounded by `limit`.

**STOP:** if FK-safe deletion requires dropping, weakening, or converting an
ownership `RESTRICT` constraint to broad cascade, stop and report. The intended
solution is ordered aggregate deletion inside the existing deferred-root
transaction.

### Phase 7: Synchronize documentation and prove Trails remains served

1. Update `flow.go`, `README.md`, functional spec, architecture, schema and
   runtime/engine component docs, examples, and API comments.
2. Document:
   - both read forms and why command methods are preferred;
   - run and command keys remaining strings;
   - the single failure rule;
   - compact enqueue results versus durable snapshots;
   - the development-only drop/recreate requirement and the absence of an
     upgrade path from the old four-migration catalog;
   - the clean current-only journal/fingerprint format and Run wire names;
   - explicit retention eligibility and permanent-key exclusion;
   - the staged-event cap;
   - queue statistics batching; and
   - Plan 11 deferral and Plan 12 sequencing.
3. Update the schema inventory to five fewer columns, one removed index, one new
   pruning index, and still exactly six tables.
4. In a disposable Trails worktree at the recorded baseline, replace its Flow
   module with the Plan 13 working tree and perform the mechanical adaptation:
   - prefer `Command.GetCurrentRun` where the definition is statically known;
   - retain top-level `GetCurrentRun` for dynamic operator loops, passing command
     names rather than queues;
   - use compact enqueue/replacement result fields;
   - apply renamed Run/Trace/active-command/history/queue-stat types; and
   - issue one `GetQueueStats` call for the full lane list.
5. Run Trails's Flow-focused tests and full compile/test gates required by that
   repository. Do not commit or push the disposable adaptation unless the
   operator separately authorizes a Trails change.
6. Record every required Trails edit in Plan 13 implementation evidence so its
   owner can apply a small, deterministic adaptation.

**Consumer gate:** Trails retains command graphs, event delivery,
transactional settlement, owner replacement, retries, queue concurrency, and
operator visibility. No deleted feature is reimplemented locally in Trails.

### Phase 8: Final performance, concurrency, and release audit

1. Repeat the baseline benchmarks back-to-back on the same durability-enabled
   PostgreSQL server.
2. Add focused measurements for:
   - permanent-key enqueue rediscovery after the full Run projection is removed;
   - 16-lane `GetQueueStats` versus 16 old single-lane calls, using a retained
     evidence-only baseline if needed; and
   - bounded pruning batches of 100 and 1,000 small terminal runs.
3. Inspect locks and query plans for start rediscovery, current-run lookup,
   queue stats, prune candidates, and aggregate deletion.
4. Run multi-replica race/stress coverage proving:
   - exactly one active durable fence per command;
   - stale attempts cannot settle after takeover;
   - replacement versus settlement remains atomic;
   - pruning never touches live work; and
   - queue-stat reads do not lock claimable rows.
5. Run the complete final gates in Section 10 on PostgreSQL 17 and 18 with
   durability settings enabled. Run a named-test audit and require zero named
   skips.
6. Review every diff hunk against this plan. Confirm there is no `flow.Call`,
   no seventh table, no migration beyond the consolidated 001 baseline, no old
   journal decoder, and no public compatibility alias for a deliberately
   removed name.

## 7. Detailed test matrix

### 7.1 API and start identity

- command method and top-level current-run success/not-found/error parity;
- definition name different from queue;
- command method and top-level typed-result parity;
- result absent, wrong name/version, malformed bytes, and caller-transaction
  visibility;
- unkeyed, permanent-key, and live-key compact enqueue results;
- idempotent rediscovery has the same `RunID` and `Created=false`;
- replacement exact expected, stale rediscovery, different declaration,
  concurrent replacement, rollback, and ambiguous commit;
- full snapshots remain available only through read APIs.

### 7.2 Failure/replay

- queued, delayed, pending, and running sibling behavior under the single rule;
- retry versus terminal failure;
- command cancellation, wait expiry, run deadline, and lease recovery;
- current journal/start/declaration bodies and fingerprints contain no retired
  input copy, metadata, fail-fast, or required fields;
- `run_started`, `run_failing`, and `run_terminal` replay through the clean
  current-only body shapes;
- canonical/hash corruption and unknown current body versions are rejected;
- Trace exposes no retired public flags while preserving statuses and failures.

### 7.3 Migration/schema

- one clean version-1 install and idempotent rerun;
- exactly one registered/embedded migration (`001_initial.sql`), with 002-004
  absent and no 005;
- direct Run-named tables, columns, constraints, indexes, and wire checks—no
  create-then-rename path;
- exact five omitted projection columns, omitted GIN index, and included prune
  index;
- six-table inventory and retained ownership constraints;
- external `MigrationFS` and embedded migration produce the same catalog; and
- wrong checksums and unknown ledger versions are rejected without mutation;
  active documentation gives drop/recreate as the only transition from the old
  development catalog.

### 7.4 Queue/event bounds

- batched queue statistics correctness and exact one-query execution;
- statement-stable time across lanes;
- transaction visibility and no row locks;
- 256 distinct staged events accepted;
- 257 rejected without partial successful-decision writes, while its attempt
  conclusion follows the ordinary retry/failure contract atomically;
- exact duplicate event does not consume another slot;
- conflicting duplicate still returns `ErrConflict`.

### 7.5 Pruning and concurrency

- eligibility matrix by status/key scope/key emptiness/cutoff;
- deterministic bounded batches and result counts;
- complete aggregate deletion;
- two pruners, locked candidate, concurrent runtime, concurrent read snapshot;
- permanent idempotency retained;
- index plan and bounded buffers/rows;
- no blocked claim, maintenance, or replacement of unrelated runs.

## 8. Documentation targets

Update at least:

- `README.md`;
- `flow.go` and package documentation;
- `specs/projects/flow/overview.md`;
- `specs/projects/flow/functional_spec.md`;
- `specs/projects/flow/architecture.md`;
- `specs/projects/flow/components/schema.md`;
- `specs/projects/flow/components/engine.md`;
- `specs/projects/flow/components/runtime.md`;
- every maintained example;
- Plan 11's deferred status;
- Plan 12's post-Plan-13 amendment requirement; and
- a new implementation evidence record under
  `specs/projects/flow/benchmark_evidence/`.

Active schema and deployment docs must state plainly that Plan 13 is a reset:
drop the old Flow schema, run `Migrate`, and recreate work. Do not publish SQL
that copies, transforms, or preserves old Flow rows.

Do not rewrite historical plan/evidence prose merely to replace old public
names. Historical records may describe the API and schema that existed when
they were executed. Add an explicit historical exception to static scans.

## 9. Performance acceptance

This plan primarily removes features and round trips; it must not trade away
the Plan 5/7 correctness work for benchmark numbers.

Acceptance rules:

1. No retained ingress, independent lifecycle, same-run fan-out, claim, staged
   decision, or ordinary snapshot median regresses by more than 10% in a
   contemporaneous repeated comparison without a documented environmental
   explanation and reviewer approval.
2. Permanent-key rediscovery performs no full Run projection read and is no
   slower materially than the current path.
3. A 16-lane queue-stat request uses one database round trip and improves
   elapsed time materially over 16 current calls.
4. Pruning work and locks are bounded by the requested limit. A locked old run
   cannot prevent pruning later eligible runs in the same call.
5. The prune candidate query uses `flow_runs_prune_idx` for a selective
   representative shape.
6. Removing the metadata GIN index does not introduce another metadata index or
   application-facing metadata scan.
7. Large event snapshot behavior is unchanged; the existing advice to keep
   large/sensitive payloads behind stable external references remains.

If a retained shape repeatedly regresses beyond the gate, stop and diagnose the
query/lock/allocation change. Do not widen the threshold or omit the result.

## 10. Commands and verification gates

Use the repository Makefile and a real PostgreSQL test database. The standard
commands are:

```bash
gofmt -d $(find . -name '*.go' -not -path './.git/*')
git diff --check
make build
go vet ./...
go test -count=1 -p 1 -parallel 4 ./...
make test
go mod verify
go mod tidy -diff
```

The reset-specific source/inventory gates are:

```bash
test "$(find migrations -maxdepth 1 -type f -name '[0-9][0-9][0-9]_*.sql' -printf '%f\n')" = '001_initial.sql'
! rg -n 'flow_executions|execution_id|execution_key|flow_runs_metadata_idx' \
  migrations/001_initial.sql
! rg -n 'execution_started|execution_failing|execution_terminal|flow\.execution_|execution_expired' \
  --glob '*.go' --glob '*.sql' .
! rg -n 'json:"(input|metadata|fail_fast|required)"' \
  internal/store/journalcodec internal/replay
```

Every command must exit 0. The first command proves there is exactly one SQL
migration file; the negated scans must print no matches. Historical Markdown
plans/evidence are intentionally outside these Go/SQL scans.

On the primary final PostgreSQL environment, preserve a machine-readable named
test audit rather than relying on verbose console inspection:

```bash
go test -count=1 -p 1 -parallel 4 -json ./... > /tmp/flow-plan13-tests.json
jq -se '[.[] | select(.Action == "skip" and (.Test // "") != "")] | length == 0' \
  /tmp/flow-plan13-tests.json
jq -se '[.[] | select(.Action == "fail")] | length == 0' \
  /tmp/flow-plan13-tests.json
```

Both `jq` commands must print `true` and exit 0. Package-level `skip` events
without a `Test` name mean a package has no test files and are not named-test
skips. Record the number of `run`, `pass`, `skip`, and `fail` events in the
implementation evidence, then remove the temporary JSON file.

The repeated benchmark gate is:

```bash
go test -run '^$' \
  -bench '^(BenchmarkRunIngressNotification|BenchmarkIndependentCommandLifecycle|BenchmarkSameRunFanout|BenchmarkSameRunClaimBatch|BenchmarkStagedDecisionBatch|BenchmarkEventSnapshotMaterialization)$' \
  -benchmem -benchtime=3s -count=5 .
```

Run PostgreSQL-backed tests with `FLOW_TEST_DATABASE_URL` and credentials set so
named tests execute rather than skip. `make test` is the complete race suite and
must pass. Before final acceptance, run both the ordinary and race suites on
supported PostgreSQL 17 and 18 with `fsync=on`, `synchronous_commit=on`, and
`full_page_writes=on`.

Run the repository's named-test JSON audit and record total named tests, skips,
and failures. Expected: zero named skips and zero failures on the primary final
PostgreSQL environment.

## 11. Scope

### 11.1 In scope

- public definitions, enqueue/results, inspection/read APIs, trace types, and
  worker decision builder;
- current store ingress, inspection, failure, cancellation, expiry, replay, and
  pruning paths;
- consolidated `migrations/001_initial.sql`, deletion of migrations 002-004,
  migration registry/schema catalog tests, and current-only journal wire names;
- tests, examples, public docs, architecture/component specs, and performance
  evidence needed for these changes;
- a disposable Trails consumer adaptation and test proof; and
- Plan 11/12 status and sequencing notes.

### 11.2 Out of scope

- implementing Plan 11 or adding `Call`/inline delivery;
- implementing Plan 12 per-command leases;
- changing the 60-second production lease, renewal timeout, watchdog, or
  maintenance pacing;
- changing attempt IDs, lease tokens, settlement fences, or at-least-once
  handler invocation;
- removing queues, events, waits, child commands, `Deliver`, `WithCommit`,
  transaction clients, cancellation, history, or observers;
- automatic TTL cleanup, permanent-key tombstones, archival sinks,
  partitioning, vacuum tuning, or background retention goroutines;
- changing Trails domain graphs, queue concurrency, owner separation, or
  application schema; and
- any in-place migration, retained-data conversion, old-body decoder, or
  compatibility view/alias for the discarded schema/history.

## 12. STOP conditions

Stop and report rather than improvising if:

1. the live source/public API/schema differs materially from the planning
   snapshot and cannot be reconciled mechanically;
2. any real environment is discovered that must preserve or upgrade its
   existing Flow schema/history; this plan is authorized only for drop/recreate;
3. consolidating 002-004 into 001 loses a current live-key, read-index,
   ownership, check-constraint, or durability invariant;
4. the new current-only journal cannot retain canonical/hash/version validation
   and exact replay semantics after the dead fields are removed;
5. removing both duplicate input copies would weaken the exact permanent-key
   input comparison;
6. pruning requires weakening ownership foreign keys, deleting permanent
   non-empty key ownership, or scanning/deleting an unbounded set;
7. a Trails production path genuinely uses optional commands, fail-fast false,
   or Flow run metadata;
8. the Trails consumer proof requires restoring a deleted queue/job façade or
   collapsing independent owners into one run;
9. any fence, lock-order, same-database atomicity, or multi-replica invariant
   fails twice after a reasonable correction;
10. a retained performance shape repeatedly exceeds the 10% regression gate;
11. database-backed tests skip on the claimed final environment; or
12. implementation begins adding Plan 11 or Plan 12 machinery to “prepare” for
    a later phase.

## 13. Acceptance criteria

Plan 13 is complete only when all of the following are true:

1. Both `Command.GetCurrentRun` and top-level `GetCurrentRun` exist, delegate to
   one implementation, and command methods are the documented default.
2. Both `Command.GetResult` and top-level `GetResult` exist with identical typed
   semantics.
3. Run and command keys remain strings; no key-wrapper concept is added.
4. `Enqueue` returns `EnqueueResult`; replacement returns compact
   `ReplaceRunResult`; `Run` has no operation-specific `Created` field.
5. Optional commands, configurable fail-fast, and run metadata are absent from
   the current public API, projections, schema, and transition branches.
6. Journal bodies, replay, fingerprints, tests, and current docs have no
   optional/fail-fast/metadata compatibility fields or decoder paths and use
   the exact Run durable strings in Section 5.4.
7. `flow_runs.input` and the start-body input copy are removed while
   permanent-key rediscovery still compares exact canonical root arguments and
   conflicts on different input.
8. The exact public renames and dead-export removals in Section 4 are complete
   without aliases.
9. `GetQueueStats` returns all requested lanes through one set-oriented query.
10. One worker decision accepts at most 256 distinct staged application events
    and rejects overflow atomically.
11. `PruneTerminalRuns` deletes only bounded terminal live-key/unkeyed
    aggregates, never permanent non-empty keyed or non-terminal runs.
12. The migration chain is one clean Run-named `001_initial.sql`, contains no
    002-005 files, retains exactly six tables, and does not contain an old-schema
    upgrade path; active docs require a reset.
13. Plan 11 is deferred and not implemented; Plan 12 remains deferred until it
    is amended against the final Plan 13 commit.
14. The disposable Trails adaptation compiles and its Flow-focused tests pass
    without locally recreating removed Flow features.
15. PostgreSQL 17/18 ordinary and race suites, build, vet, formatting, module,
    migration, replay, concurrency, and named-test gates pass.
16. Retained performance shapes satisfy Section 9 and evidence records exact
    environment, commands, medians, ranges, and caveats.
17. A final hunk-by-hunk review finds no unplanned API, schema, journal, lock,
    or consumer change.

## 14. Implementation sequence

The required sequence is:

1. baseline and consumer inventory;
2. definition-bound reads and compact operation results;
3. one failure rule and metadata removal in current code;
4. consolidated baseline, current-only journal, and duplicate projection/body
   removal;
5. public vocabulary/dead export cleanup;
6. batched queue statistics and staged-event bound;
7. bounded pruning;
8. documentation and disposable Trails proof;
9. performance/concurrency/final verification; and
10. independent final review.

Do not begin Plan 12 until step 10 is accepted and Plan 12 is amended at the
exact accepted commit.

## 15. Punchlist

### Baseline and decisions

- [ ] Record Flow/Trails/Absurd commits, dirty state, Go/PostgreSQL versions,
  migration checksums, schema catalog, supported durability settings, and exact
  baseline test/benchmark commands.
- [ ] Confirm Trails has no production `Optional`, `WithFailFast(false)`, or
  Flow run metadata use.
- [ ] Confirm Plan 13 executes before Plan 12 and Plan 11 remains deferred.
- [ ] Confirm the target release is a breaking v0.4.0 candidate and no release
  or tag is created by implementation alone.
- [ ] Confirm with the operator that every existing Flow database may be dropped
  and recreated and record that authorization in implementation evidence.

### Definition-bound reads and compact results

- [x] Add `Command.GetCurrentRun` while retaining top-level `GetCurrentRun`.
- [x] Rename the top-level parameter/documentation to `rootCommandName` and add
  the name-different-from-queue regression test.
- [x] Add `Command.GetResult` while retaining top-level `GetResult`.
- [x] Route each pair through one private implementation.
- [x] Add `EnqueueResult` and remove `Run.Created`.
- [x] Compact `ReplaceRunResult` to `RunID` plus `Replaced`.
- [x] Narrow store start results and remove unnecessary full Run rediscovery
  reads without changing identity or lock behavior.
- [x] Pass enqueue/replacement/current-run/result/transaction/ambiguity focused
  PostgreSQL and race gates.

### One failure rule and metadata removal

- [x] Add current-default characterization tests before deleting branches.
- [x] Remove `Optional`, `WithFailFast`, `Run.FailFast`, and
  `TraceCommand.Required` from current/public behavior.
- [x] Collapse failure/cancellation/expiry settlement to one reduced fail-fast
  rule while retaining running survivors.
- [x] Remove `WithMetadata`, `Run.Metadata`, and `RunFilter.Metadata`.
- [x] Delete retired fields from journal codecs, replay, fingerprints,
  test-engine shapes, and fixtures; add no legacy decoder.
- [x] Replace retired-mode tests with complete one-rule transition coverage.
- [x] Pass failure/retry/cancel/expiry/wait/replay/trace focused PostgreSQL and
  race gates.

### Clean baseline and projection pruning

- [x] Rewrite `001_initial.sql` as the direct final Run-named schema, fold in
  retained 002-004 behavior, delete 002-004, and add no 005.
- [ ] Reset the registry to one schema-version-1, reader/writer-1 migration and
  document drop/recreate as the only transition from the old catalog.
- [x] Omit five projection columns and the metadata GIN index from the clean
  baseline.
- [x] Include `flow_runs_prune_idx` with the exact eligibility predicate.
- [x] Load canonical input from the root command for permanent-key equivalence
  after removing `flow_runs.input`.
- [x] Remove the start-body input copy; prove fresh permanent-key rediscovery
  succeeds and different input still conflicts.
- [x] Rename execution-era journal kinds/classes/event names/failure codes to
  the exact Run strings in Section 5.4.
- [x] Prove clean/idempotent embedded and external installs, exact six-table
  catalog and ownership constraints, one-file migration inventory, and generic
  checksum/unknown-ledger rejection.

### Public vocabulary and surface

- [x] Apply every rename in Section 4.2 without aliases.
- [x] Remove `TraceOption`, `CommandFailure`, short status aliases, and public
  `NopObserver`.
- [x] Preserve `ResultOf`, history options, queue/event/transaction/cancellation
  capabilities, and `Work`.
- [ ] Update examples and `go doc`; exclude only historical plan/evidence prose,
  ordinary execute verbs, and explicit negative compile tests from old-name
  scans.
- [x] Pass build, vet, formatting, diff, API-surface, example, and focused read
  tests.

### Queue statistics and decision bounds

- [ ] Replace `GetQueueDepth` with batched `GetQueueStats` and `QueueStats`.
- [ ] Return every requested distinct lane, including empty lanes, from one SQL
  statement and one statement-stable timestamp.
- [ ] Add zero/one/16/duplicate/invalid/too-many/transaction query tests and an
  exact query-count assertion.
- [ ] Prove the existing queue read index supports the grouped query.
- [ ] Enforce a 256-distinct-event decision limit with duplicate-idempotency
  semantics unchanged.
- [ ] Prove 257-event overflow writes no result, application event, child,
  application commit, or partial successful-decision projection; assert the
  exact ordinary attempt conclusion, allocator positions, journal, retry or
  terminal projection, and run counters instead of requiring no writes at all.

### Bounded retention

- [ ] Add `PruneResult` and `PruneTerminalRuns` with explicit cutoff and 1-1,000
  limit.
- [ ] Select eligible terminal live-key/unkeyed runs in deterministic order with
  `FOR UPDATE SKIP LOCKED`.
- [ ] Delete selected journal/command/run aggregates set-wise in one transaction
  without weakening FKs.
- [ ] Prove permanent non-empty keys and all non-terminal runs remain.
- [ ] Prove cutoff, batch limit, exact counts, complete aggregate deletion,
  concurrent pruners, locked-candidate progress, coherent reads, and unrelated
  runtime progress.
- [ ] Prove the partial prune index on a selective 10,000-row plan shape.

### Consumer, performance, and final closure

- [ ] Update package docs, README, functional spec, architecture, schema,
  engine/runtime components, examples, and implementation evidence.
- [ ] Perform the disposable Trails adaptation without touching its live dirty
  worktree and record every required consumer edit.
- [ ] Pass Trails Flow-focused compile/tests with all core graphs, delivery, and
  transaction ownership intact.
- [ ] Repeat retained benchmarks and add compact rediscovery, 16-lane stats,
  and 100/1,000-run pruning measurements.
- [ ] Pass multi-replica fence/replacement/pruning/queue-read concurrency gates.
- [ ] Pass PostgreSQL 17 and 18 ordinary plus full race suites with durability
  enabled and zero named-test skips/failures.
- [ ] Pass build, vet, gofmt, diff, module verify/tidy, consolidated migration
  checksum/inventory, public surface, current journal/replay, and source scans.
- [ ] Review every hunk against Plan 13 and obtain an independent final review.
- [ ] Record the final accepted commit; only then amend Plan 12 against it.
