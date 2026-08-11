# Plan 9: Simplify Flow's developer experience without weakening its model

Status: Implemented (combined v0.3 candidate; awaiting human approval)

Planned at: `788c9b5` on 2026-08-11; implementation baseline remains the
tagged `v0.2.0` source at `3d2b29b`

- **Target release:** intermediate milestone in the combined v0.3.0 candidate;
  Plan 9 is implemented and reviewed before Plan 10, but is not tagged alone
- **Priority:** P1 for the public API cleanup and Trails-facing ergonomics
- **Effort:** L
- **Risk:** MEDIUM-HIGH; the durable model remains unchanged, but this plan
  intentionally performs one coordinated pre-v1 public, internal, and
  catalog-vocabulary migration, removes one public entry point, and changes
  the event-read signature
- **Schema impact:** breaking forward migration 004 renames
  `flow_executions` to `flow_runs`, every run-ownership column, and all
  corresponding constraints and indexes; the table count remains six
- **Durable format impact:** none; migration 004 renames catalog identifiers
  but does not rewrite append-only journal bytes or their versioned wire
  strings
- **Public API impact:** breaking `Execution`→`Run` family and
  `Execute`→`Enqueue` vocabulary cleanup; one direct root-enqueue form,
  `Command.Enqueue(ctx, client, ...)`, with removal of `Command.With` and its
  hidden client binding; removal of redundant method `Event.Emit` while
  retaining `Event.Deliver` for immediate targeted ingress; breaking addition
  of a `found` result to `GetEventValue`; compatible expansion of accepted
  positive durable durations

> **Planning snapshot:** this plan was written against the clean v0.2.0
> `master` source at `3d2b29b`. A separate decision/change-set naming refactor
> was observed while planning but is not part of this plan. If that work lands
> first, reconcile its intentional naming changes rather than overwriting them.
> Implement Plan 9 only from a clean, reviewed base.
>
> **Executor instructions:** Read this document completely before editing. Work
> in phase order, keep each phase independently reviewable, and run its focused
> tests before continuing. Prefer wrappers and deletion over new abstractions.
> The run-vocabulary migration described here is intentional. If an ergonomic
> helper requires a new durable concept, seventh table, scheduler branch, or
> weaker fence, stop: that is evidence that the helper is outside this plan.
>
> **Release sequencing:** Plan 9 owns the breaking v0.2-to-v0.3 vocabulary and
> schema migration, but it is an intermediate reviewed checkpoint rather than
> a tag. Plan 10 is implemented on that same branch and the combined candidate
> is tagged only after both plans' migration, consumer, documentation, and
> verification gates pass. Plan 11 is a separately approved, optional inline-
> call proposal and is not part of this release.
>
> **Initial drift check:**
>
> ```text
> git status --short --branch
> git log -1 --decorate --oneline
> git diff --stat 3d2b29b..HEAD -- \
>   definitions.go execute.go worker.go node.go retry.go errors.go \
>   runtime.go command_runtime.go testing_bridge.go flowtest internal/testengine \
>   internal/durable internal/retry internal/failure \
>   migrations.go migrations migrations_test.go internal/store internal/replay \
>   compile_contract_test.go durable_contract_test.go examples \
>   README.md flow.go specs/projects/flow
> ```
>
> Reconcile any accepted change-set rename and later work before implementation.
> New public API, durable-duration, event-ingress, schema, durable-format, or
> transaction-order changes not described here are a STOP condition until this
> plan is amended.

## 1. Purpose and recommendation

Flow is already conceptually small:

```text
run
  -> commands
       -> workers
       -> staged child commands
       -> run-scoped events
       -> exact event gates
```

That model is not the complexity to remove. It is the reason Flow fits
`trails-api`: one business operation owns a durable graph, each side-effect or
isolation boundary is a command, each command selects its own queue, and exact
events join asynchronous domain facts back into the graph. PostgreSQL keeps
the graph, journal, queue projections, attempts, and application writes
consistent.

The comparison with Absurd does identify useful lessons, but they are mostly
about developer experience:

1. common cases should read like ordinary Go;
2. callers should not need small duration-normalization helpers; and
3. optional event input should not be inferred from a command-key suffix; and
4. the public vocabulary should distinguish a durable `Run` from an
   asynchronously enqueued command without pre-committing to another
   composition primitive.

Plan 9 applies those lessons while explicitly rejecting an Absurd-style
procedural checkpoint/replay layer. It keeps one explicit detached targeted
event-ingress verb, keeps root starts dependency-explicit, changes the existing
typed event-input reader to report presence, and supplies one realistic
multi-queue example.

The target v0.3.0 API is:

```text
start once:        command.Enqueue(ctx, client, key, args, ...)
stage a child:     flow.Enqueue(work, key, command, args)
stage a fact:      flow.Emit(work, event, key, payload)
deliver a targeted fact: event.Deliver(ctx, client, runID, key, payload)
read a fact input: value, found, err := flow.GetEventValue(work, event, key)
```

The terminology cleanup does justify one schema/durable-vocabulary migration;
it must preserve the six-table model and semantic data rather than becoming a
storage redesign or performance project.

## 2. What Flow should and should not learn from Absurd

### 2.1 The two engines optimize for different workflow shapes

Absurd presents one task handler as a procedural program. Named steps persist
checkpoint values so a retry can skip completed code, and a task can await a
child task's result. This is concise for a mostly linear workflow whose steps
share one queue and worker lifecycle.

Flow presents the durable structure directly. A command is an independently
queued and retried unit; a successful command may atomically create multiple
children and events; exact event gates wait without consuming a worker; and
all commands remain owned by one run. That is a better representation
for fan-out, joins, external signals, different worker pools, version drains,
and application/Flow transaction boundaries.

| Concern | Absurd tendency | Flow decision |
|---|---|---|
| Workflow shape | procedural handler with persisted steps | explicit command/event graph |
| Durable unit | task/run/checkpoint | run/command/attempt/journal |
| Queue | owns tasks and often the whole handler | delivery property of each command |
| Waiting | checkpointed wait or child-task wait | durable event gate with no worker slot |
| Composition | replay code from the top and skip checkpoints | atomically stage children/events at success |
| Result flow | step values and awaited task result | typed command result and exact event payload |
| Best fit | concise linear durable functions | multi-queue graphs, joins, external facts, inspection |

Neither model is universally simpler. The mistake would be to place both in
Flow. A new `Task`, `Step`, checkpoint, replay cursor, or workflow function
would create two composition models, two sets of retry expectations, and an
unclear boundary between a command result and a step result.

Plan 11 separately evaluates one possible way to narrow the linear-workflow
syntax gap with a durable synchronous command call. That proposal is optional,
has its own release and storage review, and is neither a prerequisite nor a
partially implemented seam in Plan 9.

### 2.2 Trails demonstrates why Flow's model stays

The relevant Trails shape is approximately:

```text
intent.run Run (live key = intent)
  -> stage command
       -> intent.txn.send       queue intent.txn.send
       -> intent.txn.mine       queue intent.txn.mine
       -> intent.txn.confirm    queue intent.txn.confirm
       -> intent.txn.join       exact terminal-event join
  -> external bridge/OIF/edge gate
  -> next stage or settlement
```

Those queues intentionally have different local concurrency. The run is
the domain operation; the queue is only where a particular command can run.
Collapsing this to one Absurd task would lose queue-level isolation. Splitting
it into unrelated tasks would lose the run aggregate, atomic child/event
settlement, built-in cancellation, exact version ownership, and one trace.
Waiting for a child task inside a handler would also risk consuming a worker
slot, whereas Flow event gates consume none.

Trails also relies on behavior that remains first-class:

- `Runtime.InTx(tx)` couples application row changes to run enqueues,
  cancellation, and event delivery;
- live keys admit one current intent run while allowing later
  generations;
- exact command versions let v1 and v2 workers drain safely;
- retry and lease attempts are fenced even when handler bodies overlap;
- external facts include domain generation identifiers and satisfy exact
  gates; and
- dynamic successor commands are derived from accepted state, not a static
  workflow script.

### 2.3 Concrete friction and constraints observed in Trails

This plan addresses demonstrated usage rather than hypothetical shorthand:

1. `workers/intent_run.go` currently checks whether the command key ends in an
   `/await/<gate>` presentation suffix before calling `GetEventValue`. That
   couples handler control flow to the command-key naming convention.
2. Both `workers/intent_run.go` and `lib/jobqueue/queue.go` define an identical
   `exactFlowDuration` helper that rounds positive durations upward to the next
   millisecond.
3. Every root start currently requires `command.With(client).Execute(...)`.
   The bound value is usually temporary, hides transaction lifetime inside a
   copied definition, and creates a second root-start shape. A direct
   `command.Enqueue(ctx, client, ...)` form is simpler and sufficient.
4. Trails requires immediate targeted delivery from independently rooted
   bridge, edge, OIF, and provider monitors. Their observation lifecycle is
   separate from the exact current `intent.run`, and delivery often joins a
   caller-owned application transaction. Top-level `flow.Emit(work, ...)`
   cannot replace this cross-run operation. Completed Trails Plan 004 removed
   the transitional V1 compatibility roots; the enduring monitor paths are the
   only consumer proof Plan 9 should use.

These are good candidates for library improvements because the replacements
remain typed wrappers over behavior Flow already has.

### 2.4 Simplifications considered and rejected

Three ideas from the comparison are deliberately **not** part of Plan 9:

- **No `Action`, `DefineAction`, or `HandleAction`.** Seven of Trails's eight
  production definitions return `flow.None`, but an action would still be a
  command durably. Naming it differently introduces another public concept and
  another way to register workers merely to avoid returning `None{}`. Keeping
  every executable unit visibly a `Command[A, R]` is simpler.
- **No `DeliverToLive` helper.** Trails has one small domain helper that calls
  `GetCurrentRun` and then `Event.Deliver`. The two calls have a meaningful
  race: the observed current run may settle before delivery, and Flow must
  not silently retarget a generation-specific payload. Keeping this composition
  explicit in Trails makes that race and its domain error handling visible.
- **No `flowctl` or dashboard.** Flow's bounded inspection APIs already let an
  application build the operator surface it needs. A CLI adds connection,
  output, payload-redaction, and compatibility surface unrelated to simplifying
  the library. Reconsider it only when an operator requirement exists.

### 2.5 Lessons already adopted

Flow v0.2.0 already adopted time-ordered UUIDv7 durable identifiers. Plan 5
already made readiness, decisions, and claims set-oriented and bounded. There
is no reason to reopen either project here. Index fill factors, partitions, or
other storage tuning require measurements from a representative retained data
set; they are not developer-experience changes.

## 3. Controlling architecture decisions

### 3.1 Preserve the durable core

Plan 9 must preserve all of these invariants:

1. The schema remains exactly six Flow tables: runs,
   commands, command queue, command event waits, journal, and migration ledger.
2. One public `Run`—persisted by the renamed run row—remains the
   aggregate and first semantic lock.
3. Commands remain the only durable executable unit.
4. Queue remains an immutable command-definition delivery property, not the
   owner or identity of a run.
5. Worker success continues to journal the attempt, result, events, children,
   waits, queue projections, counters, and `WithCommit` effects atomically.
6. Event identity remains `(run ID, event name, event key)` with the
   existing idempotency and conflict rules.
7. Waits remain exact AND gates and consume no worker or connection while
   waiting.
8. Attempt ID, lease token, queue state, run lock, and lease expiry keep
   fencing stale settlement. At-least-once handler invocation remains explicit;
   exactly-once side effects are not promised.
9. PostgreSQL database time remains authoritative for durable transitions.
10. `Runtime.InTx(tx)` retains transaction ownership and run-first lock
    ordering; Flow never commits or rolls back the caller transaction.
11. Command/event payloads, results, failures, retry policies, declaration
    fingerprints, replay semantics, and trace semantics remain unchanged.
    Current symbols and catalog identifiers move to Run vocabulary while
    versioned historical journal strings remain byte-compatible.
12. Existing permanent-key and live-key semantics remain unchanged.

### 3.2 Keep one composition model

Do not add any of the following:

- `Task`, `Step`, `Workflow`, `Plan`, `Coordinator`, or state-machine APIs;
- replay-from-the-top handler execution;
- mutable checkpoint state or a seventh checkpoint table;
- automatic command-result dataflow;
- a worker-blocking child-result await primitive;
- global event routing, pub/sub discovery, or first-listener-wins semantics;
- queue-owned run identity or physical tables per queue; or
- a second retry, cancellation, or leasing subsystem.

A useful boundary rule belongs in the Plan 9 documentation: keep
deterministic, inexpensive microsteps in one worker function, and use
`flow.Enqueue` when work needs its own retry boundary, queue/concurrency lane,
side-effect fence, timeout, external wait, observability identity, or
fan-out/join lifecycle. Plan 11 separately evaluates whether a synchronous
durable subroutine is valuable enough to add; Plan 9 neither implements nor
depends on it.

### 3.3 Use composed rendezvous keys with stable typed events

Borrow Absurd's strongest event convention: publisher and waiter should derive
the same deterministic identity from domain data. Flow should keep that
identity structured instead of flattening its type and correlation into one
untyped event name.

Absurd commonly writes:

```go
name := "terminal:" + txnID + ":" + strconv.FormatUint(generation, 10)
absurd.EmitEvent(ctx, queue, name, payload)
payload, err := absurd.AwaitEvent[Terminal](ctx, name, opts)
```

The Flow equivalent is already concise:

```go
var TxnTerminal = flow.DefineEvent[Terminal]("txn.terminal")

func txnTerminalKey(txnID string, generation uint64) string {
    return txnID + ":" + strconv.FormatUint(generation, 10)
}

key := txnTerminalKey(txnID, generation)

// Publisher: immediate targeted ingress.
err := TxnTerminal.Deliver(ctx, client, runID, key, payload)

// Parent worker: declare the exact rendezvous in the same run.
flow.Enqueue(parentWork, "join/"+key, JoinTxn, args).WaitFor(TxnTerminal, key)

// JoinTxn worker: derive the same key from its durable arguments.
key = txnTerminalKey(joinWork.Args.TxnID, joinWork.Args.Generation)
payload, found, err := flow.GetEventValue(joinWork, TxnTerminal, key)
// found must be true because this command declared WaitFor(TxnTerminal, key).
```

Logically, Absurd uses one flattened identity while Flow uses the tuple
`(run ID, event definition name, event key)`. The stable event definition
preserves compile-time payload typing and a useful trace/filter category. The
dynamic key carries transaction ID, generation, or any other correlation fence.
Run scope prevents an identical key in another run from colliding.

Document and demonstrate this convention:

- define one typed event per semantic fact kind, not one definition per entity;
- derive the event key in one small domain helper;
- use the exact same key at delivery, `WaitFor`, and value lookup;
- include generation/attempt identity when stale external facts must not satisfy
  later work; and
- do not add an untyped string-event API or concatenate dynamic identity into
  `DefineEvent` names.

### 3.4 Ship one coherent v0.3 compatibility window

`Command.With`, method `Event.Emit`, and `Event.Deliver` are public in v0.2.0.
The smallest coherent v0.3 surface removes client binding from command
definitions, keeps one direct root-enqueue spelling, keeps the semantically
clear `Deliver` verb for detached targeted ingress, and removes the redundant
method `Emit`. Renaming run/queued-delivery vocabulary, changing
`GetEventValue`, and applying migration 004 are already real breaking changes,
so these deletions belong in the same pre-v1 upgrade. Therefore:

- complete and review Plan 9 as the intermediate v0.3.0 API checkpoint;
- do not move or rewrite the v0.2.0 tag;
- publish one complete v0.2-to-v0.3 migration guide with mechanical before/
  after examples;
- do not retain `Command.With`, `BoundCommand`, or method `Event.Emit` as
  deprecated wrappers; and
- do not begin Plan 10 until the Plan 9 API, migration, and Trails consumer
  proof are accepted; do not tag the intermediate checkpoint.

Trails Plan 004 is complete, and this feature work has not been deployed. Its
V1 compatibility roots therefore do not constrain the Flow upgrade. The
consumer proof must exercise an enduring bridge, edge, OIF, or provider
monitor delivering an exact generation-fenced fact into the current
`intent.run`; it must not restore deleted compatibility roots merely to test an
obsolete path.

### 3.5 Name the public aggregate `Run` and queued delivery `Enqueue`

The top-level public object is one concrete durable generation containing its
root command, every enqueued command, exact events, queued-command
attempts, counters, and ordered history. Call it a `Run`:

```text
Run
├── root Command
├── enqueued Commands
├── Events
├── Attempts for queued Commands
└── ordered Journal
```

Do not call this object a `Task`: a task ordinarily means one queue item or
unit of work, which is already a `Command` in Flow. Do not call it a
`Workflow`: Flow has no separately registered workflow definition, static
graph, or second composition model. `Execution` is technically defensible but
is easily confused with one handler invocation; Flow already names that
smaller lifecycle an `Attempt`. `Run` is the shortest accurate name for one
generation of the dynamic command graph.

Rename the complete public family coherently rather than leaving mixed
vocabulary:

| v0.2 public name | Plan 9 public name |
|---|---|
| `Execution`, `ExecutionID`, `ExecutionStatus` | `Run`, `RunID`, `RunStatus` |
| `ExecutionOption`, `ExecutionFilter`, `ExecutionPage` | `RunOption`, `RunFilter`, `RunPage` |
| `ExecutionTrace.Execution` | `RunTrace.Run` |
| `GetExecution`, `AwaitExecution`, `ListExecutions`, `CancelExecution` | `GetRun`, `AwaitRun`, `ListRuns`, `CancelRun` |
| `LookupLiveExecution` | `GetCurrentRun` |
| `WithExecutionDeadline`, `WithoutExecutionDeadline` | `WithRunDeadline`, `WithoutRunDeadline` |
| `WithMaxCommandsPerExecution` | `WithMaxCommandsPerRun` |
| public `ExecutionID`, `ExecutionKey`, and `ExecutionStatus` fields | corresponding `RunID`, `RunKey`, and `RunStatus` fields |
| `ObservationExecution` | `ObservationRun` |
| `HistoryExecutionStarted`, `HistoryExecutionFailing` | `HistoryRunStarted`, `HistoryRunFailing` |

The first column must be verified against the actual v0.2 exported API during
Phase 0; it is descriptive, not permission to miss embedded occurrences.
Inventory `CommandInfo`, `Observation`, `HistoryEntry`, `LiveWork`,
`KeyedHistoryEntry`, trace/read filters and pages, testing bridges, examples,
and consumer structs so no exported execution-prefixed identifier or field is
left accidentally. Rename public status constants consistently to
`RunStatusRunning`, `RunStatusFailing`, and the four terminal variants.

`GetCurrentRun(ctx, client, commandName, key)` returns `(Run, found, error)`.
It is the public read for the current non-terminal holder of a live-scoped key;
it does not return the most recent terminal generation. The `Get` prefix is
deliberate: `found` already represents ordinary absence, and callers should not
have to learn a separate `Lookup` naming convention.

The exported-API audit found no other public `LookupX` function in v0.2. The
same naming rule should clean up the three production internal methods that
currently use the prefix:

| v0.2 internal name | Plan 9 name | Exact behavior |
|---|---|---|
| `Store.LookupApplicationEvent` | `Store.GetEvent` | point-read one user-defined Flow Event by `(run ID, event name, event key)`; return `(EventRecord, found, error)` |
| `Store.LookupCommandExecution` | `Store.GetCommandRunID` | return only the owning Run ID for one command ID; absence remains an error |
| `Store.LookupLiveExecutionInTx` | `Store.GetCurrentRun` | return the one non-terminal live-key Run for `(definition name, key)` as `(RunRow, found, error)` |

“Application” exists today because the journal also records command-terminal
and run-terminal event classes. That discriminator remains in storage SQL, but
callers of this method already ask for a public Flow `Event`, so `GetEvent` is
the clearer operation name. `GetCommandRunID` includes `ID` because it returns
a UUID, not a loaded Run. Drop `InTx` from `GetCurrentRun`: its `pgx.Tx`
argument is optional, so the suffix currently overstates its contract. Rename
associated internal result/types and error text where that improves the same
vocabulary, and leave no production `LookupX` function behind. This is a
symbol naming rule, not a ban on using “lookup” as an ordinary English noun or
verb in implementation prose.

Rename queued/asynchronous command delivery from `Execute` to `Enqueue`:

- root `Command.Enqueue` creates or idempotently rediscovers a `Run`;
- worker `flow.Enqueue` stages an independently delivered child command in the
  current `Run`.

`Enqueue` names the asynchronous delivery intent, not an immediate runnable
queue-row insertion: waits, gates, and delays may defer runnable projection,
and stable command/run identity may rediscover an equivalent existing
declaration. Document both points explicitly. Keep
`Runtime.Run(ctx)` unchanged; there, `Run` is the conventional verb for the
long-lived processor loop and is unambiguous beside the `Run` noun.

The resulting front-door vocabulary is `Run`, `Command`, `Queue`, `Worker`,
`Event`, and `Attempt`, with `Enqueue` for asynchronous delivery. A `Queue`
remains a routing and
concurrency lane: one Run may contain Commands assigned to several Queues, and
one Queue serves Commands from many Runs. It therefore cannot replace `Run`.
Do not add a public `Task`, `Job`, `Ack`, or `Nack` layer. A worker's returned
decision already settles its Attempt, result, Events, and child Commands
atomically; manual acknowledgement would expose broker mechanics and permit
partial settlement mistakes.

### 3.6 Rename the live schema and implementation vocabulary coherently

Plan 9 owns forward migration `004_run_vocabulary.sql`. Because v0.2.0 is
already tagged, migrations 001–003 and their checksums remain byte-for-byte
unchanged. Migration 004 must transactionally:

- rename `flow_executions` to `flow_runs`;
- rename `execution_id` to `run_id` on `flow_runs`, `flow_commands`,
  `flow_command_queue`, `flow_command_event_waits`, and `flow_journal`;
- rename `execution_key` to `run_key` on `flow_runs`;
- rename every current constraint and index whose name contains `execution`
  so the final catalog uses `run` consistently;
- preserve every primary key, foreign key, delete action, check predicate,
  uniqueness rule, index key/include/order/collation/predicate, and ownership
  relationship exactly; and
- leave the table count at six, with no compatibility view, duplicate column,
  trigger, or alias.

Update current production SQL, store/replay types, local variables, error text,
observations, test helpers, examples, docs, and current specifications from
execution vocabulary to run vocabulary. Opaque public cursor encodings are
pre-v1 API data and may receive a new run-named version; old cursors must fail
cleanly rather than decode incorrectly.

Do not rewrite migrations 001–003 or historical plans/evidence. Also do not
rewrite append-only journal history merely for terminology: values such as
`execution_started`, `execution_failing`, `execution_terminal`, and the v1
run-start body's `execution_id`/`execution_key` JSON fields are versioned wire
protocol, not catalog identifiers. Current Go symbols should be renamed to
`RunStarted`, `RunFailing`, `RunTerminal`, and `RunStartedBody` while retaining
those exact stored strings/tags for v0.2 history and new writes. This narrow
wire exception avoids a risky full-journal rewrite and keeps replay/trace
compatible; document it instead of pretending released bytes never existed.

Migration 004 is intentionally incompatible with a running v0.2 binary. It
must record `min_reader_version=2` and `min_writer_version=2`, with the Plan 9
library's current reader/writer versions raised to 2 for the coordinated v0.3
deployment. It runs under the existing migration advisory lock, preserves all
rows and journal hashes, and supports both clean install (001→004) and
populated v0.2 upgrade (003→004). Plan 11 owns any later migration 005.

## 4. Target public API

### 4.1 Use one direct root-enqueue form

Root enqueue always names the client at the operation boundary:

```go
run, err := SendEmail.Enqueue(
    ctx,
    client,
    "email/order-42",
    SendEmailArgs{OrderID: "42"},
    flow.WithLiveKey(),
)
```

The target shape is:

```go
func (cmd Command[A, R]) Enqueue(
    ctx context.Context,
    client Client,
    key string,
    args A,
    opts ...RunOption,
) (Run, error)
```

`Command` is only an immutable typed definition. Remove its hidden client field,
`Command.With`, and any `BoundCommand` adapter. Services already retain a
`Runtime` or another `Client`; binding that dependency into every copied command
adds a second root-start spelling and makes transaction lifetime less visible.
Passing `runtime.InTx(tx)` directly keeps caller-owned transaction scope explicit
at the exact operation that joins it.

Why this is worthwhile:

- every root start has one searchable spelling;
- transaction ownership and client lifetime remain explicit;
- command definitions remain safe package-level values with no operational
  dependency hidden inside them; and
- docs, examples, compile contracts, and consumer migrations become smaller.

This changes no start identity, store request, lock, transaction, notification,
or error behavior.

### 4.2 Reduce event ingress from three forms to two

Retain only these concepts:

| API | Meaning |
|---|---|
| `flow.Emit(work, event, key, value)` | stage an event in the active worker change set; commit or discard it with fenced settlement |
| `event.Deliver(ctx, client, runID, key, value)` | immediate targeted ingress in its own or a caller-owned transaction; detached from any worker result |

Delete method `Event.Emit(ctx, client, runID, key, value)` and retain
`Event.Deliver` unchanged.

Today method `Event.Emit` and `Event.Deliver` reach the same target-side
implementation, but `Deliver` names the important semantic distinction:
immediate ingress to a named run is detached from the current worker's staged
decision. `Emit` belongs to the top-level `flow.Emit(work, ...)` operation that
is committed or discarded with worker settlement. Keeping those verbs distinct
prevents code review from mistaking a detached write for an atomic staged fact.

The targeted capability itself is required. The enduring proof is Trails's
independently rooted bridge, edge, OIF, and provider monitors, which deliver an
  exact generation-fenced fact into a different current `intent.run`
  generation.
They often use `Runtime.InTx(tx)` so the event and application-row changes
commit or roll back together. Trails Plan 004 removes the legacy separately
rooted receipt-transaction fan-in, but explicitly retains these monitor roots
and their targeted exact-event delivery. Removing the Flow capability would
therefore require a larger Trails redesign, such as a separate transactional
outbox publisher, and would add a durable concept rather than simplify Flow.
Plan 9 keeps `Deliver` and removes only the redundant method `Emit`.

After deletion:

- external/application publishers use `Event.Deliver`;
- worker facts that must be atomic with success use top-level `flow.Emit`;
- deliberately detached cross-run ingress from a worker uses method
  `Event.Deliver` with an explicit client and target; and
- docs must warn that delivery to the current run is legal but
  detached and is almost never the right replacement for `flow.Emit`.

The distinction is therefore structural, not a second vocabulary word:

```go
flow.Emit(work, TxnTerminal, key, payload) // current decision, atomic

TxnTerminal.Deliver(ctx, client, targetRunID, key, payload) // named target, immediate
```

Retain `Event.Deliver`'s existing ability to run from an active attempt and its
documented detached semantics. Delete method `Event.Emit` and its attempt guard
together; do not preserve private context plumbing solely for that removed
guard or for speculative future inline execution. Compile-contract tests must
continue proving typed event payload safety through `Event.Deliver`.

### 4.3 Make event-input presence explicit through one API

Change the existing function to:

```go
func GetEventValue[W, T any](
    work *Work[W],
    event Event[T],
    key string,
) (value T, found bool, err error)
```

Required behavior:

1. It is an O(1), attempt-local memory lookup and performs no SQL, polling, or
   waiting.
2. It validates the work, event definition, and stable event key exactly as
   the current `GetEventValue` does.
3. A valid selector absent from the current command's input snapshot returns
   `(zero, false, nil)` and does **not** poison settlement.
4. A present selector returns the typed decoded payload and `found=true`.
5. Corrupt bytes, a codec/type mismatch, invalid work, invalid definition, or
   invalid key return an error and poison the complete decision exactly as the
   strict API does.
6. Repeated calls are deterministic and do not mutate the snapshot.

The claim path already validates and materializes the retained wait rows before
the handler starts. `GetEventValue` should treat a selector missing from that
validated snapshot as absent; it should not add another declaration map, query,
or durable field solely to diagnose unsupported direct SQL deletion. Corruption
detected at the accepted Flow write/claim boundary must still fail before or
during typed decode.

Do not add `LookupEventValue`, a strict companion, a `Must` variant, or another
event-input reader. Callers decide whether absence is valid by checking
`found`. A command that declared an exact `WaitFor` should treat `found=false`
as an invariant failure and return an application error. The ordinary gated
path cannot be absent through supported Flow writes, and the explicit boolean
keeps optional and required reads within one conventional Go API.

This lets Trails replace command-key suffix inspection with data-oriented
control flow:

```go
fact, found, err := flow.GetEventValue(work, StageTerminal, gateKey)
if err != nil {
    return flow.None{}, err
}
if found {
    return route(fact)
}
// Start external work and stage the exact gated successor.
```

This helper is not an event query. It cannot inspect arbitrary history or
observe an event that was not materialized into the current attempt snapshot.

### 4.4 Accept positive durable durations and round upward once

Flow persists scheduling configuration as integer milliseconds. Keep that
durable representation, but stop requiring every caller to pre-normalize a Go
duration.

At the **public durable scheduling boundary**, accept every positive
`time.Duration` that can be represented after ceiling and canonicalize it
upward to the next whole millisecond:

```text
1ns                 -> 1ms
999.999µs           -> 1ms
1ms                 -> 1ms
1ms + 1ns           -> 2ms
1.5s                -> 1500ms
```

Rounding up is deliberate: a timeout, wait budget, retry, or delay must never
occur earlier than requested. Zero and negative values retain each feature's
existing invalid/disabled semantics. A value whose ceiling would overflow
`time.Duration`, durable integer milliseconds, or timestamp arithmetic is
invalid.

Apply normalization consistently to public values that become durable:

- `WithRunDeadline`;
- `WithTimeout`;
- `WithStartDelay`;
- root `Within`;
- `Node.Delay` and `Node.Within`;
- `RetryFor` elapsed bounds;
- `RetryPolicy.Backoff` entries; and
- `RetryAfter` explicit delays.

Do not silently change runtime-only operational tuning such as
`WithPollInterval` or `WithShutdownGrace`; those values are not part of an
run's durable identity. Keep store-layer `ExactMilliseconds` validation
strict so internal callers and corrupted inputs cannot bypass the normalized
public boundary.

Introduce one internal public-boundary helper, for example
`durable.CeilMilliseconds`, that returns both the normalized duration and
integer milliseconds with overflow checks. Do not scatter ceiling arithmetic.
Normalize before storing options, comparing duplicate options, constructing
retry policies, or computing fingerprints. Consequently, semantically equal
inputs such as `1ns` and `1ms` must produce the same durable declaration and
start identity. Permanent-start rediscovery and repeated child declarations
with those equivalent inputs must not conflict.

This is a compatible expansion for exact-millisecond callers but a behavioral
change for callers that previously expected `ErrInvalid` from fractional
durations. Plan 9 documents it in the final v0.2-to-v0.3 migration guide.

## 5. Example and documentation improvements

### 5.1 Add one realistic multi-queue example

Add `examples/pipeline` (or another neutral name) demonstrating the shape that
matters to Trails without copying Trails domain code:

1. enqueue one live-key order run;
2. stage independent payment and inventory commands on distinct queues;
3. emit exact completion events atomically from those workers;
4. join both events in a command that consumes no worker while gated;
5. deliver one external approval through `Runtime.InTx(tx)` alongside an
   application row update;
6. derive a dynamic successor from accepted results; and
7. inspect or trace the final run.

The example must keep every executable unit a `Command`, use the `found` result
from `GetEventValue` for optional first-pass versus resumed behavior, compose
`GetCurrentRun` and `Event.Deliver` explicitly where current-run delivery is
useful, and demonstrate direct `Command.Enqueue`. It should
explain the possible settle-between-lookup-and-emission race and why each
command boundary exists: queue, retry, side effect, external wait, or join.
Keep the example small enough to read in one sitting and test it like the
existing examples.

The external publisher and gated command must share one deterministic
transaction/generation key helper as described in Section 3.3. Add a regression
showing that an earlier generation's event does not release the later
generation's wait.

Do not create a workflow DSL, builder, static graph registration, or generated
code to make the example look shorter.

### 5.2 Synchronize the public documentation

Update at least:

- `README.md` quick start and API vocabulary;
- `flow.go` package overview and source-level entrypoint;
- `specs/projects/flow/project_overview.md`;
- `specs/projects/flow/functional_spec.md`;
- `specs/projects/flow/architecture.md`;
- `specs/projects/flow/components/engine.md`;
- relevant runtime/inspection component docs;
- examples and their tests; and
- one complete v0.2-to-v0.3 migration guide owned by Plan 9.

The documentation must say plainly:

- Flow can express a linear workflow without one command per source-code line;
- split commands only at durable operational boundaries;
- queues are internal delivery lanes, not workflows or business identities;
- every durable executable unit is a command, including commands whose result
  type is `None`;
- typed event definitions are stable semantic fact kinds, while deterministic
  event keys carry entity and generation identity;
- `GetEventValue` looks only at the current immutable attempt snapshot and its
  `found` result distinguishes absent input from a present typed value;
- method `Event.Deliver` is targeted and detached while top-level `flow.Emit`
  is attempt-atomic;
- fractional positive durable durations are rounded upward;
- Flow provides at-least-once handler invocation with exactly-once fenced
  durable settlement, not exactly-once remote side effects.

## 6. Deletions and migration map

| v0.2 form | v0.3 form | Nature | Durable effect |
|---|---|---|---|
| public `Execution*` types/functions/fields | coherent `Run*` family from §3.5 | breaking, mechanical Go vocabulary change | migration 004 renames the live schema; versioned journal wire strings remain compatible |
| `LookupLiveExecution(...)` | `GetCurrentRun(...)` | breaking rename; same indexed non-terminal-key query | reads `flow_runs.run_key` after migration 004 |
| internal `LookupApplicationEvent` | internal `GetEvent` | exact rename plus conventional `(record, found, error)` result | same journal point read by run/name/key |
| internal `LookupCommandExecution` | internal `GetCommandRunID` | exact rename clarifying that only the UUID is returned | reads `flow_commands.run_id` |
| internal `LookupLiveExecutionInTx` | internal `GetCurrentRun` | exact rename; remove misleading optional-transaction suffix | same live-key query on `flow_runs` |
| `flow_executions`, `execution_id`, `execution_key`, execution-named constraints/indexes | `flow_runs`, `run_id`, `run_key`, run-named constraints/indexes | breaking transactional catalog rename in migration 004 | all rows, keys, predicates, and relationships preserved |
| `cmd.With(client).Execute(ctx, key, args, opts...)` | `cmd.Enqueue(ctx, client, key, args, opts...)` | breaking verb rename and removal of hidden client binding | none |
| explicitly typed or rebound client-bearing `Command` values | immutable client-free `Command[A, R]`; keep the `Client` in the service/component | breaking removal of `Command.With` and copied client-bearing definitions | none |
| `event.Emit(ctx, client, id, key, value)` | `event.Deliver(ctx, client, id, key, value)` | breaking removal of the redundant immediate-ingress spelling; detached semantics remain under the clearer verb | none |
| `value, err := GetEventValue(...)` and command-key suffix tests | `value, found, err := GetEventValue(...)` | breaking, mechanical; `found` replaces presentation-key inference | none |
| app-local millisecond ceiling helper | pass positive duration directly | compatible input expansion | same integer-ms form |

The repository-wide migration is mechanical but broad. Update production,
tests, benchmarks, examples, compile fixtures, comments, specs, and evidence
commands. Historical completed plans and historical benchmark evidence must
not be rewritten to pretend the old API never existed; add a short historical
note only where a command would otherwise be impossible to reproduce.

Trails Plan 004 is complete and the feature branch has not been deployed, so
the migration checklist is specifically:

1. do not restore V1 compatibility call sites deleted by Plan 004;
2. migrate the complete public `Execution*` family and embedded fields to
   `Run*`, including `GetCurrentRun`, and update any application SQL that
   deliberately reads Flow's now-renamed `flow_runs`/`run_id` catalog;
3. rename every queued root/child `Execute` call to `Enqueue` and replace every
   bound root start with direct `Command.Enqueue(ctx, client, ...)`;
4. retain `Event.Deliver` for all enduring independent-monitor ingress and
   remove any method-`Event.Emit` usage;
5. replace the two `exactFlowDuration` helpers with direct duration values;
6. replace `/await/<gate>` suffix branching with the `found` result from
   `GetEventValue`; and
7. retain the domain-local current-run delivery helper, including its existing
   error context and explicit generation behavior, for independently rooted
   bridge, edge, OIF, and provider monitors.

The disposable migration must prove an enduring monitor-to-`intent.run` path.
No compatibility root or legacy-only test is valid evidence for the current
architecture.

## 7. Phased implementation

### Phase 0 — Baseline, API inventory, and proof examples

Before editing production code:

1. reconcile any accepted change-set rename and obtain a clean implementation
   base;
2. record `go list -m all`, Go/PostgreSQL versions, schema version, and the
   current exported API;
3. inventory every exported `Execution*` identifier and embedded public field,
   every production `Lookup*` function, every schema/index/constraint/SQL
   execution identifier, every `Command.With`, directly started command,
   `Event.Deliver`, method `Event.Emit`,
   durable-duration validation,
   and attempt-scope use in production, tests, examples, and docs;
4. inventory the corresponding Trails call sites without editing that
   repository and identify the enduring independent-monitor paths retained
   after completed Plan 004;
5. write small compile-only target examples for the `Run` family,
   `GetCurrentRun`, direct enqueues, staged/detached event
   ingress, and presence-aware event-input reads; and
6. run the full baseline suite before changing behavior.

Focused gate:

```text
gofmt -w <temporary Go fixtures>
git diff --check
make build
go vet ./...
go test -count=1 -p 1 -parallel 4 ./...
make test
```

Remove temporary fixtures after they have clarified the final signatures.

### Phase 1 — Establish Run/Enqueue vocabulary across API, storage, and ingress

Implement Sections 3.5, 3.6, 4.1, and 4.2 together so the vocabulary changes
once:

- rename the complete exported `Execution*` family, fields, status constants,
  observation kind, and history constants to their `Run*` forms; add no public
  compatibility aliases;
- rename public `LookupLiveExecution` and internal
  `LookupLiveExecutionInTx` to their respective `GetCurrentRun` forms, rename
  `LookupApplicationEvent` to `GetEvent`, rename `LookupCommandExecution` to
  `GetCommandRunID`, and leave no production `LookupX` function;
- add migration 004, bump schema and reader/writer compatibility versions,
  rename the live table/columns/constraints/indexes from execution to run,
  and update every current SQL/store/replay identifier while preserving the
  versioned journal wire strings described in §3.6;
- remove the hidden client field from `Command`, `Command.With`, and any
  `BoundCommand` adapter;
- add the direct-client `Command.Enqueue` signature as the only root-start
  form;
- retain `Event.Deliver`, convert any method-`Event.Emit` call sites to
  `Deliver`, and delete method `Event.Emit`;
- retain top-level worker `flow.Emit` unchanged;
- update generic compile-misuse tests and the removed-API AST guard.

Required tests:

- compile-contract and API-inventory tests prove the coherent `Run` surface,
  absence of old exported `Execution*`, `Execute`, and
  `LookupLiveExecution` names, absence of all production `LookupX` symbols,
  and retention of the versioned durable encodings;
- migration tests prove migrations 001–003 have unchanged checksums, clean
  install through 004, populated 003→004 upgrade, exact six-table final
  inventory, no old live catalog identifiers, renamed constraints/indexes with
  unchanged definitions, preserved row counts/FKs/index paths, and rejection
  by an incompatible v0.2 reader/writer;
- `GetEvent` returns the matching Event record and ordinary `found=false`,
  `GetCommandRunID` returns exactly the owner ID and errors for a missing
  command, and internal `GetCurrentRun` works with and without a caller tx;
- `GetCurrentRun` returns only the current non-terminal live-key generation,
  returns `found=false` after terminal settlement, and never returns an older
  terminal run;
- direct Runtime and `Runtime.InTx(tx)` starts retain current creation,
  rediscovery, conflict, observation, error, commit, and rollback behavior;
- zero/nil/mismatched direct clients retain current error classification;
- typed command arguments still fail at compile time;
- typed event payloads still fail at compile time through method `Deliver`;
- targeted delivery from an independent monitor can
  release an exact generation-fenced gate in a different `intent.run`
  run and remains independent of the source worker settlement;
- targeted delivery through `Runtime.InTx(tx)` follows caller-owned
  transaction commit and rollback visibility;
- staged `flow.Emit` still commits and rolls back with settlement; and
- compile-contract coverage prevents removed `With`, method `Event.Emit`,
  `Execute`, and old public/internal execution/lookup vocabulary from being
  accidentally reintroduced.

Do not add deprecated forwarding methods. One clean v0.3.0 break is simpler
than permanent duplicate spellings.

### Phase 2 — Normalize durable durations at public boundaries

Implement Section 4.4:

1. add one overflow-safe upward-normalization helper;
2. normalize each public durable duration before it enters option/policy state;
3. retain strict exact-millisecond validation in store and durable decoding;
4. ensure retry policy canonicalization and fingerprints use the normalized
   values; and
5. replace tests that expect fractional rejection with a boundary matrix.

Required tests include:

- 1ns, sub-millisecond, exact millisecond, fractional millisecond, maximum
  safe, overflow, zero, and negative cases for every public durable feature;
- equivalent permanent root rediscovery for `1ns` versus `1ms`;
- equivalent repeated child declarations after normalization;
- conflicting values that normalize to different milliseconds still reject;
- retry policy encode/decode and fingerprint stability;
- `RetryAfter` never schedules earlier than requested;
- timestamp overflow remains rejected; and
- direct internal/store fractional values remain invalid.

No schema or stored JSON shape changes: canonical values remain integer
milliseconds.

### Phase 3 — Unify required and optional event-input reads

Implement the Section 4.3 signature change in the existing event-input
lookup/decode path. Update every caller to handle the additional `found`
result explicitly.

Required tests:

- `GetEventValue` absent/present/invalid/corrupt/repeated behavior;
- required-input callers reject an unexpected `found=false` result;
- no SQL is performed by `GetEventValue`;
- retry and lease takeover return identical snapshots; and
- the change adds no durable selector state or claim query.

### Phase 4 — Add the multi-queue example

Implement Section 5.1 without touching engine/storage internals.

Example tests should run the complete multi-queue graph, including the external
transactional event, exact join, dynamic successor, `None`-result commands,
optional input branch, and final trace. Keep test time deterministic and
short.

### Phase 5 — Documentation, consumer proof, and Plan 9 checkpoint

1. synchronize the documentation listed in Section 5.2;
2. finish the complete v0.2-to-v0.3 migration guide;
3. ensure examples and doc snippets compile;
4. in a disposable Trails worktree or branch, perform the mechanical API
   migration against the repository's actual Plan 004 state and run its
   Flow-focused unit/integration tests; do not restore deleted V1 call sites,
   do not use a legacy-only delivery test as the compatibility proof, and do
   not commit cross-repository changes without separate authorization;
5. run source scans for deleted and forbidden concepts;
6. run the complete PostgreSQL and race gates; and
7. report results for review, record the exact checkpoint SHA for Plan 10's
   drift audit, and continue without creating a release tag.

The Trails proof is a Plan 9 checkpoint gate because Trails is the concrete
compatibility target. It must include a retained bridge, edge, OIF, or provider
monitor delivering into the exact current `intent.run` generation; a deleted
V1 compatibility path is not evidence.
If the migration requires a lost engine capability rather than a mechanical/
API-helper adjustment, stop and amend this plan.

These complete gates certify the Plan 9 portion of the combined v0.3.0
candidate. Plan 10 performs its own drift, consumer, and release review from
the recorded checkpoint.

Final commands, adjusted only for the repository's then-current supported
matrix:

```text
gofmt -w <all changed Go files>
git diff --check
go mod verify
go mod tidy -diff
make build
go vet ./...
go test -count=1 -p 1 -parallel 4 ./...
make test
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
```

Run ordinary and race suites against every PostgreSQL major promised by the
README, with `fsync`, `synchronous_commit`, and `full_page_writes` enabled.
Audit named test output for skips rather than treating a credential skip as a
pass. Re-run the retained hot-path benchmark shapes as a non-regression sanity
check; do not add timing thresholds to tests or claim a throughput improvement
from a terminology/ergonomics project.

## 8. Explicit non-goals and deferred work

### 8.1 Retention needs its own design

Absurd provides task cleanup. Flow will eventually need a deliberate retention
story, but it is unsafe to bolt one onto this plan. Permanent keys currently
promise identity over retained Flow data, journal history powers replay/trace,
and runs own commands, waits, queues, and journal entries. Naive deletion
can permit an old permanent operation to run again or erase the evidence needed
for diagnosis.

Do not add purge SQL, partitions, tombstones, archive tables, or retention
options here. A later plan must first choose among retaining permanent-key
tombstones, compacting terminal runs, limiting cleanup to unkeyed/live-
key generations, or explicitly changing the key contract. It must include
bounded deletion, concurrent inspection/start behavior, foreign-key order,
vacuum impact, and operator recovery.

### 8.2 No speculative engine or schema tuning

Apart from the exact migration 004 renames in §3.6, this plan does not
authorize:

- table collapse or a seventh table;
- new data-bearing columns, index key/predicate changes, constraints, triggers,
  or another migration;
- queue partitions or queue-specific tables;
- journal compaction or format changes;
- scheduler, lease, maintenance, notification, or claim changes;
- a public lease-duration or renewal-timeout option;
- exactly-once handler or remote-effect claims;
- a Flow CLI, dashboard, or other operator application;
- dependency upgrades unrelated to a real build/security need;
- a new serialization library or framework; or
- broad renaming beyond the explicit `Run`/`Enqueue` and schema vocabulary in
  §§3.5–3.6 and separately reviewed change-set work.

## 9. Acceptance criteria

Plan 9 is complete only when all of the following are true:

1. Flow still exposes one run/command/event composition model and no
   task/step/checkpoint/workflow abstraction. The Plan 9 completion head does
   not pre-implement `flow.Call`, inline delivery, or migration 005.
2. The schema remains exactly six tables with `flow_runs`, `run_id`, `run_key`,
   and consistently run-named live constraints/indexes. Migration 004 supports
   clean install and populated v0.2 upgrade, preserves all rows and catalog
   semantics, raises compatibility deliberately, and leaves migrations 001–003
   and their checksums unchanged.
3. Versioned journal encodings and hashes, canonical command/event/result
   data, replay, and trace remain compatible with v0.2.0 history; current Go
   symbols use Run vocabulary while documented historical wire strings retain
   their exact bytes.
4. The public aggregate family is coherently `Run`, `RunID`, `RunStatus`,
   `RunOption`, `RunFilter`, `RunPage`, and `RunTrace`; all embedded public
   fields, constants, read/cancel APIs, observations, history names, examples,
   and testing surfaces agree. No old exported `Execution*` identifier or
   current execution-named schema/SQL/store/replay identifier remains outside
   migration 004's old-name operands and documented historical wire strings.
5. Queued root and child delivery is named `Enqueue`; no public command
   `Execute` remains. Root enqueue returns a `Run`, worker enqueue stages a
   command in the current run, and identity may rediscover equivalent existing
   work. A wait, gate, or delay may defer its runnable queue projection.
6. `GetCurrentRun` is the sole current live-key-holder read, returns
   `(Run, found, error)`, never returns terminal history, and replaces the only
   public `Lookup*` function. Internal store operations are `GetEvent`,
   `GetCommandRunID`, and `GetCurrentRun`; their exact value/found/error
   contracts are tested and no production `LookupX` symbol remains.
7. `Command` no longer stores a hidden client; `Command.With` and
   `BoundCommand` do not exist.
8. `Command.Enqueue(ctx, client, key, args, options...)` is the only root-start
   spelling and retains existing transactional, idempotency, validation,
   observation, and error behavior.
9. `Event` has one targeted `Deliver` method and no method `Emit`.
10. Top-level `flow.Emit(work, ...)` retains fenced atomic settlement behavior.
   `Event.Deliver` remains explicitly detached, targeted ingress.
11. Removed API names are guarded against accidental reintroduction.
12. `GetEventValue` is the only event-input reader. It is O(1), in-memory,
   returns `(zero, false, nil)` for absence, returns `found=true` for a present
   typed value, and remains strict and decision-poisoning for malformed input.
13. No `LookupEventValue`, strict companion, or `Must` variant is added; normal
    gated callers explicitly reject an unexpected `found=false` result.
14. No `Action`, `DefineAction`, `HandleAction`, or `DeliverToLive` API is added;
    `None`-result work remains visibly a command and live delivery remains an
    explicit application composition.
15. Runtime and caller-owned transaction clients both work with every relevant
    new API, preserving lock-order guards and rollback visibility.
16. Every positive public durable scheduling duration is rounded upward once,
    and zero/negative/overflow behavior is tested.
17. Equivalent rounded durations produce identical durable fingerprints and
    rediscovery behavior.
18. Store-layer exact-millisecond validation remains strict.
19. The multi-queue example demonstrates current-run ownership, per-command queues,
    exact gates, a non-worker wait, transactional delivery, dynamic composition,
    a shared composed event key, stale-generation isolation, and final
    inspection without a DSL.
20. README, package docs, normative specs, component docs, examples, and the
    complete v0.2-to-v0.3 migration guide agree on the Plan 9 API.
21. Historical plans/evidence remain historically accurate, and a disposable
    Trails migration compiles with its Flow-focused tests passing against the
    repository's actual Plan 004 state. The proof includes an enduring
    independent-monitor-to-`intent.run` targeted event, not only a V1
    compatibility path.
22. All named Flow tests run with no unintended skips against supported
    PostgreSQL majors; ordinary and race suites pass.
23. Build, vet, formatting, module verification/tidiness, vulnerability, and
    retained performance non-regression gates pass before the Plan 9
    checkpoint is handed to Plan 10.

## 10. STOP conditions

Stop implementation and report the evidence if any of these occurs:

1. the working tree contains overlapping unreviewed changes that cannot be
   separated safely;
2. migration 004 cannot preserve every row, constraint, foreign key, index
   definition/path, and journal hash, requires an append-only journal rewrite,
   or cannot produce the exact six-table run-named catalog without compatibility
   views/duplicate columns;
3. direct `Enqueue` cannot retain current start identity, transaction
   ownership, validation, observation, error classification, or lock order;
4. deleting method `Event.Emit` or retaining `Event.Deliver` changes
   target-side identity, transaction, lock, idempotency, conflict, or cross-run
   behavior instead of only consolidating the public verb;
5. `GetEventValue` cannot report ordinary absence without poisoning the
   decision, or cannot distinguish absence from present corruption;
6. duration normalization changes already exact values, stored integer-ms
   shapes, retry determinism, or permanent-start identity;
7. a public duration can round down, wrap, or overflow timestamp arithmetic;
8. the realistic example needs a task/step/checkpoint abstraction to remain
   understandable;
9. the Trails migration reveals loss of per-command queues, exact event gates,
   transactional ingress, independently rooted monitor delivery, version
   drains, cancellation, or generation fencing;
10. a schema/durable change beyond migration 004 or any Plan 11
    `flow.Call`/inline-delivery/migration-005 work appears necessary; or
11. any full PostgreSQL/race gate fails repeatedly for an in-scope reason.

Do not weaken an invariant, preserve a redundant API indefinitely, or expand
the engine to work around a STOP condition. Amend the plan with the newly
discovered constraint.

## 11. Punchlist

### Baseline and decisions

- [x] Reconcile any accepted change-set rename and start from a clean reviewed base.
- [x] Record the final implementation commit; Go/PostgreSQL versions, schema
  version, and the exported API inventory are recorded in candidate evidence.
- [x] Inventory all exported/internal `Execution*`/`Lookup*` names and fields, every execution-named live catalog/SQL identifier, plus all Flow and Trails uses of bound/direct command starts, `Event.Deliver`, method `Event.Emit`, duration normalization, and command-key suffix input detection; identify the enduring independent-monitor paths retained after Trails Plan 004.
- [x] Confirm the Plan 9 v0.3 signatures with compile-only examples before
  production edits.
- [x] Run and record the complete pre-change ordinary and race baseline.

### Run and Enqueue vocabulary

- [x] Rename the complete exported `Execution*` type/function/field/constant family to `Run*`, including trace, read APIs, observations, history, testing surfaces, and examples.
- [x] Rename public `LookupLiveExecution` to `GetCurrentRun`; prove it returns only the current non-terminal live-key generation and that no other public `Lookup*` API exists.
- [x] Rename internal `LookupApplicationEvent`→`GetEvent`, `LookupCommandExecution`→`GetCommandRunID`, and `LookupLiveExecutionInTx`→`GetCurrentRun`; adopt explicit found/error contracts and leave no production `LookupX` symbol.
- [x] Rename queued root/child `Execute` to `Enqueue`; document deferred runnable projection and idempotent rediscovery rather than guaranteed immediate insertion.
- [x] Add migration 004: rename `flow_executions`→`flow_runs`, every ownership `execution_id`→`run_id`, `execution_key`→`run_key`, and every execution-named live constraint/index without changing definitions.
- [x] Update current production SQL/store/replay/error/observation/test vocabulary and public cursor encoding to Run; retain only migrations 001–003, migration 004 old-name operands, historical plans/evidence, and versioned journal wire strings as documented exceptions.
- [x] Prove clean 001→004 install and populated 003→004 upgrade, exact row/hash/FK/index preservation, six-table inventory, compatibility-version rejection, and byte-identical 001–003 checksums.
- [x] Add compile/API guards against old exported `Execution*`, `Execute`, and all production `LookupX` symbols without banning ordinary implementation prose that uses the English word “lookup.”

### Public API ergonomics and deletion

- [x] Remove the hidden client field from `Command`, `Command.With`, and `BoundCommand`.
- [x] Make `Command.Enqueue(ctx, client, key, args, options...)` the sole root-start form.
- [x] Prove Runtime and `Runtime.InTx(tx)` direct starts preserve semantics, errors, observations, and lock order.
- [x] Retain `Event.Deliver(ctx, client, runID, key, payload)`.
- [x] Remove method `Event.Emit` and retain top-level worker `flow.Emit` unchanged.
- [x] Add a cross-run worker test matching an enduring Trails bridge/edge/OIF/provider-monitor-to-`intent.run` delivery shape, including exact generation fencing and caller-owned transaction commit and rollback.
- [x] Add compile/AST guards for removed APIs and compile-contract coverage for the direct command-start form while retaining generic type-safety checks.
- [x] Verify Runtime and `InTx` start/delivery commit, rollback, lock-order, and notification behavior.

### Durable duration ergonomics

- [x] Add one overflow-safe upward millisecond-normalization helper for public durable inputs.
- [x] Normalize run deadline, attempt timeout, initial delay, and root/child wait budgets.
- [x] Normalize retry elapsed bounds, backoff entries, and explicit retry-after delays.
- [x] Preserve strict exact-millisecond store and decode validation.
- [x] Prove equivalent normalized values have identical fingerprints and rediscovery behavior.
- [x] Add the full positive/fractional/zero/negative/maximum/overflow test matrix.

### Optional event input

- [x] Change `GetEventValue` to return `(value, found, error)` without adding a second reader.
- [x] Update required-input callers to reject unexpected absence explicitly.
- [x] Prove the single helper remains O(1), in-memory, deterministic, and strict for corruption.
- [x] Guard against adding action aliases or a live-delivery routing abstraction.

### Example

- [x] Add and test the realistic multi-queue pipeline example.
- [x] Keep every executable unit a command, including `None`-result work.
- [x] Use one deterministic entity/generation event-key helper at delivery, wait declaration, and lookup.
- [x] Prove an event for an earlier generation cannot release a later generation's wait.
- [x] Demonstrate explicit `GetCurrentRun` plus `Event.Deliver` composition and its terminal race.
- [x] Explain every command boundary and keep the example free of a DSL.

### Documentation and consumer proof

- [x] Update README and `flow.go` to make Run/Command/Queue/Worker/Event/Attempt plus Enqueue/Deliver/Runtime.Run the front-door vocabulary.
- [x] Update project overview, functional spec, architecture, and component docs.
- [x] Document presence-aware snapshot reads, explicit current-run get/emission races, and upward duration rounding.
- [x] Document stable typed event definitions plus deterministic composed rendezvous keys.
- [x] Finish the combined v0.2-to-v0.3 migration guide and explicit Trails
  migration checklist.
- [x] Document the linear-workflow boundary rule and retained at-least-once/fencing semantics.
- [x] Preserve historical plans and benchmark evidence as historical records.
- [x] Perform the disposable Trails API migration against its actual Plan 004 state, without restoring deleted V1 call sites, and run its Flow-focused tests including an enduring independent-monitor delivery path.

### Final verification and Plan 9 checkpoint

- [x] Confirm exactly six Flow tables with `flow_runs` and no old live catalog identifiers; confirm only migration 004 was added and migrations 001–003 retain their checksums.
- [x] Scan for removed APIs and forbidden task/step/checkpoint/workflow concepts.
- [x] Confirm Plan 9 did not add `flow.Call`, inline delivery fields, journal
  rewrites, or migration 005; those remain deferred to Plan 11.
- [x] Run gofmt, diff check, module verify/tidy, build, vet, and vulnerability gates.
- [x] Run every named ordinary and race test with zero unintended PostgreSQL skips.
- [x] Run the supported PostgreSQL-major matrix with durability settings enabled.
- [x] Run retained performance shapes and record a non-regression conclusion.
- [x] Review every changed hunk against this plan and all 23 acceptance criteria.
- [x] Review the exact combined implementation SHA as Plan 9's logical
  checkpoint and record it for Plan 10 without creating an intermediate
  release tag.
