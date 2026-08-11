# Plan 9: Simplify Flow's developer experience without weakening its model

Status: Planned

Planned at: `3d2b29b` (`v0.2.0`) on 2026-08-10

- **Release boundary:** none — this is an untagged intermediate milestone;
  Plans 9 and 10 ship together under one v0.3.0 tag after Plan 10 passes
- **Priority:** P1 for the public API cleanup and Trails-facing ergonomics
- **Effort:** M
- **Risk:** MEDIUM; the durable model and schema do not change, but this plan
  intentionally removes one pre-v1 public entry point and changes two public
  signatures
- **Schema impact:** none
- **Durable format impact:** none
- **Public API impact:** breaking removal of `Event.Deliver`; additive direct
  `Command.Execute(ctx, client, ...)` while retaining
  `Command.With(client).Execute(...)` through an explicit `BoundCommand`;
  breaking addition of a `found` result to `GetEventValue`; compatible
  expansion of accepted positive durable durations

> **Planning snapshot:** this plan was written against the clean v0.2.0
> `master` source at `3d2b29b`. A separate decision/change-set naming refactor
> was observed while planning but is not part of this plan. If that work lands
> first, reconcile its intentional naming changes rather than overwriting them.
> Implement Plan 9 only from a clean, reviewed base.
>
> **Executor instructions:** Read this document completely before editing. Work
> in phase order, keep each phase independently reviewable, and run its focused
> tests before continuing. Prefer wrappers and deletion over new abstractions.
> If an ergonomic helper requires a new durable concept, table, journal shape,
> scheduler branch, or weaker fence, stop: that is evidence that the helper is
> outside this plan.
>
> **Release sequencing:** Plan 9 may be committed, reviewed, and merged as an
> intermediate implementation milestone, but it must not be tagged or
> published independently. Hand its verified completion SHA directly to Plan
> 10 at `specs/projects/flow/plans/10-inline-command-calls.md`. The next
> release tag is v0.3.0 only after both plans and the combined consumer/release
> gates pass.
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
>   compile_contract_test.go durable_contract_test.go examples \
>   README.md flow.go specs/projects/flow
> ```
>
> Reconcile any accepted change-set rename and later work before implementation.
> New public API, durable-duration, event-ingress, schema, or transaction-order
> changes not described here are a STOP condition until this plan is amended.

## 1. Purpose and recommendation

Flow is already conceptually small:

```text
execution
  -> commands
       -> workers
       -> staged child commands
       -> execution-scoped events
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
3. optional event input should not be inferred from a command-key suffix.

Plan 9 applies those lessons while explicitly rejecting an Absurd-style
procedural checkpoint/replay layer. It consolidates targeted event ingress,
supports both direct and intentionally client-bound command starts, changes the
existing typed event-input reader to report presence, and supplies one
realistic multi-queue example.

The target after Plan 9 is an intermediate API foundation for the combined
v0.3.0 release. Plan 10 adds `flow.Call` before that release is tagged:

```text
start once:        command.Execute(ctx, client, key, args, ...)
bind and reuse:    command.With(client).Execute(ctx, key, args, ...)
stage a child:     flow.Execute(work, key, command, args)
stage a fact:      flow.Emit(work, event, key, payload)
emit a targeted fact: event.Emit(ctx, client, executionID, key, payload)
read a fact input: value, found, err := flow.GetEventValue(work, event, key)
```

No schema migration, storage rewrite, or performance project is justified by
this work.

## 2. What Flow should and should not learn from Absurd

### 2.1 The two engines optimize for different workflow shapes

Absurd presents one task handler as a procedural program. Named steps persist
checkpoint values so a retry can skip completed code, and a task can await a
child task's result. This is concise for a mostly linear workflow whose steps
share one queue and worker lifecycle.

Flow presents the durable structure directly. A command is an independently
queued and retried unit; a successful command may atomically create multiple
children and events; exact event gates wait without consuming a worker; and
all commands remain owned by one execution. That is a better representation
for fan-out, joins, external signals, different worker pools, version drains,
and application/Flow transaction boundaries.

| Concern | Absurd tendency | Flow decision |
|---|---|---|
| Workflow shape | procedural handler with persisted steps | explicit command/event graph |
| Durable unit | task/run/checkpoint | execution/command/attempt/journal |
| Queue | owns tasks and often the whole handler | delivery property of each command |
| Waiting | checkpointed wait or child-task wait | durable event gate with no worker slot |
| Composition | replay code from the top and skip checkpoints | atomically stage children/events at success |
| Result flow | step values and awaited task result | typed command result and exact event payload |
| Best fit | concise linear durable functions | multi-queue graphs, joins, external facts, inspection |

Neither model is universally simpler. The mistake would be to place both in
Flow. A new `Task`, `Step`, checkpoint, replay cursor, or workflow function
would create two composition models, two sets of retry expectations, and an
unclear boundary between a command result and a step result.

Plan 10 deliberately narrows the linear-workflow syntax gap without changing
that conclusion: `flow.Call` invokes a command as a synchronous durable
subroutine, and every accepted call is still an execution-owned command with
ordinary identity, history, and fencing. It does not add procedural replay or
a second durable unit. Plan 9 must leave that seam clean, but must not implement
it early.

### 2.2 Trails demonstrates why Flow's model stays

The relevant Trails shape is approximately:

```text
intent.run execution (live key = intent)
  -> stage command
       -> intent.txn.send       queue intent.txn.send
       -> intent.txn.mine       queue intent.txn.mine
       -> intent.txn.confirm    queue intent.txn.confirm
       -> intent.txn.join       exact terminal-event join
  -> external bridge/OIF/edge gate
  -> next stage or settlement
```

Those queues intentionally have different local concurrency. The execution is
the domain operation; the queue is only where a particular command can run.
Collapsing this to one Absurd task would lose queue-level isolation. Splitting
it into unrelated tasks would lose the execution aggregate, atomic child/event
settlement, built-in cancellation, exact version ownership, and one trace.
Waiting for a child task inside a handler would also risk consuming a worker
slot, whereas Flow event gates consume none.

Trails also relies on behavior that remains first-class:

- `Runtime.InTx(tx)` couples application row changes to execution starts,
  cancellation, and event delivery;
- live keys admit one current intent execution while allowing later
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
   That form is useful when a service binds definitions once and starts them
   repeatedly, while a direct `command.Execute(ctx, client, ...)` form avoids
   a throwaway bound value for one-shot starts. Both usages are legitimate and
   should share one implementation.
4. Trails still requires immediate targeted emission from inside a Flow worker,
   but its current call sites fall into two different lifecycle categories.
   `lib/jobqueue.Queue.Handle` adapts every queue handler through `flow.Handle`.
   The separately rooted `txn.confirm` compatibility handler calls
   `DeliverStageChildTerminalTx`, whose shared
   `lib/intentmachine/deliverIntentRunEventTx` helper looks up the separate live
   `intent.run` execution and calls `Event.Deliver` through `Runtime.InTx(tx)`.
   Trails Plan 004 removes that V1 compatibility root and its legacy terminal
   fan-in; it is transitional evidence, not the enduring reason for this Flow
   capability. Independently rooted bridge, edge, OIF, and provider monitors
   use the same targeted path and are deliberately retained because their
   observation lifecycle must survive replacement or cancellation of the
   parent intent execution. Those monitor facts remain cross-execution: the
   source is a monitor/jobqueue execution and the target is the exact live
   `intent.run` generation, so top-level `flow.Emit(work, ...)` cannot replace
   the targeted operation after the V1 roots are gone.

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
  `LookupLiveExecution` and then `Event.Emit`. The two calls have a meaningful
  race: the observed live execution may settle before delivery, and Flow must
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

1. The schema remains exactly the existing six Flow tables: executions,
   commands, command queue, command event waits, journal, and migration ledger.
2. One execution remains the aggregate and first semantic lock.
3. Commands remain the only durable executable unit.
4. Queue remains an immutable command-definition delivery property, not the
   owner or identity of an execution.
5. Worker success continues to journal the attempt, result, events, children,
   waits, queue projections, counters, and `WithCommit` effects atomically.
6. Event identity remains `(execution ID, event name, event key)` with the
   existing idempotency and conflict rules.
7. Waits remain exact AND gates and consume no worker or connection while
   waiting.
8. Attempt ID, lease token, queue state, execution lock, and lease expiry keep
   fencing stale settlement. At-least-once handler invocation remains explicit;
   exactly-once side effects are not promised.
9. PostgreSQL database time remains authoritative for durable transitions.
10. `Runtime.InTx(tx)` retains transaction ownership and execution-first lock
    ordering; Flow never commits or rolls back the caller transaction.
11. Command and event canonical encodings, declaration fingerprints, journal
    entry bodies, replay, and trace remain unchanged.
12. Existing permanent-key and live-key semantics remain unchanged.

### 3.2 Keep one composition model

Do not add any of the following:

- `Task`, `Step`, `Workflow`, `Plan`, `Coordinator`, or state-machine APIs;
- replay-from-the-top handler execution;
- mutable checkpoint state or a seventh checkpoint table;
- automatic command-result dataflow;
- a worker-blocking child-result await primitive;
- global event routing, pub/sub discovery, or first-listener-wins semantics;
- queue-owned execution identity or physical tables per queue; or
- a second retry, cancellation, or leasing subsystem.

A useful intermediate rule belongs in the Plan 9 documentation: keep
deterministic, inexpensive microsteps in one worker function, and use
`flow.Execute` when work needs its own retry boundary, queue/concurrency lane,
side-effect fence, timeout, external wait, observability identity, or
fan-out/join lifecycle. Plan 10 then adds `flow.Call` for a different case: a
synchronous durable subroutine whose result is needed now and whose accepted
progress should survive parent retry. Do not pre-implement `Call` in Plan 9;
do not write Plan 9 docs so absolutely that Plan 10 must reverse them.

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
err := TxnTerminal.Emit(ctx, client, executionID, key, payload)

// Parent worker: declare the exact rendezvous in the same execution.
flow.Execute(parentWork, "join/"+key, JoinTxn, args).WaitFor(TxnTerminal, key)

// JoinTxn worker: derive the same key from its durable arguments.
key = txnTerminalKey(joinWork.Args.TxnID, joinWork.Args.Generation)
payload, found, err := flow.GetEventValue(joinWork, TxnTerminal, key)
// found must be true because this command declared WaitFor(TxnTerminal, key).
```

Logically, Absurd uses one flattened identity while Flow uses the tuple
`(execution ID, event definition name, event key)`. The stable event definition
preserves compile-time payload typing and a useful trace/filter category. The
dynamic key carries transaction ID, generation, or any other correlation fence.
Execution scope prevents an identical key in another execution from colliding.

Document and demonstrate this convention:

- define one typed event per semantic fact kind, not one definition per entity;
- derive the event key in one small domain helper;
- use the exact same key at delivery, `WaitFor`, and value lookup;
- include generation/attempt identity when stale external facts must not satisfy
  later work; and
- do not add an untyped string-event API or concatenate dynamic identity into
  `DefineEvent` names.

### 3.4 Use one combined v0.x compatibility window deliberately

`Command.With` and `Event.Deliver` are public in v0.2.0. Plan 9 retains the
useful bound-command form, adds a direct one-shot form, and removes only the
redundant delivery verb. Changing `Command.With` to return an explicit bound
adapter and changing `GetEventValue` are still real source changes for some
callers. Plan 10 adds the inline command API and migration 004 immediately
afterward. Therefore:

- complete and review Plan 9 as an untagged intermediate milestone;
- continue directly into Plan 10 and ship both plans only in v0.3.0;
- do not move or rewrite the v0.2.0 tag;
- draft Plan 9's migration table and before/after examples now, then fold them
  into the combined v0.2-to-v0.3 migration guide in Plan 10;
- do not retain `Event.Deliver` as a deprecated wrapper merely to avoid a
  pre-v1 break; and
- do not create a v0.2.x, v0.3.0-preview, or other interim tag merely to mark
  the Plan 9 handoff;
- do not publish v0.3.0 until Plan 10 and a Trails migration branch against the
  combined API compile and their Flow integration tests pass.

Trails Plan 004 is a separate application-graph cleanup, not a dependency of
the Flow engine change. Prefer completing its V1-root deletion before the final
Trails v0.3 upgrade so call sites scheduled for deletion are not migrated only
to be removed. If Plan 004's production observation window is still running,
Flow Plan 9 may proceed independently, but the disposable Trails migration must
classify each targeted-emission call site as either transitional V1
compatibility or an enduring independent-monitor path. Do not combine the
Trails graph deletion and the Flow dependency/API migration in one production
PR.

## 4. Target public API

### 4.1 Support both direct and bound command starts

Support the direct one-shot form:

```go
execution, err := SendEmail.Execute(
    ctx,
    client,
    "email/order-42",
    SendEmailArgs{OrderID: "42"},
    flow.WithLiveKey(),
)
```

Retain the bound form for a component that repeatedly uses the same client:

```go
sendEmail := SendEmail.With(client)

execution, err := sendEmail.Execute(
    ctx,
    "email/order-42",
    SendEmailArgs{OrderID: "42"},
    flow.WithLiveKey(),
)
```

The target shapes are:

```go
func (cmd Command[A, R]) Execute(
    ctx context.Context,
    client Client,
    key string,
    args A,
    opts ...ExecutionOption,
) (Execution, error)

func (cmd Command[A, R]) With(client Client) BoundCommand[A, R]

func (cmd BoundCommand[A, R]) Execute(
    ctx context.Context,
    key string,
    args A,
    opts ...ExecutionOption,
) (Execution, error)
```

`BoundCommand` is an ergonomic client-bound view of the same immutable command
definition, not a second durable command kind. It owns only the definition and
client association needed by the bound `Execute`; it is never registered,
staged, called inline, persisted, journaled, or passed where a `Command` is
required. `Command` itself should no longer carry a hidden client field.

Both `Execute` methods must delegate immediately to one private typed start
implementation. They must have identical validation, option normalization,
start identity, transactions, observations, and errors. A bound command may be
stored in a service struct or local variable and reused concurrently whenever
its client supports that use. As today, a value bound to `Runtime.InTx(tx)` is
transaction-scoped and must not outlive or be used concurrently beyond that
transaction.

Why this is worthwhile:

- one-shot code can pass its dependency explicitly at the operation site;
- services can bind a runtime once and keep concise repeated starts;
- the direct start API matches `Event.Emit`, inspection, cancellation, and
  trace without taking away the existing ergonomic form;
- the immutable definition cannot accidentally acquire a client through a
  copied `Command`; the association is visible in `BoundCommand`; and
- one shared implementation prevents the two conveniences from acquiring
  different durable behavior.

This changes no start identity, store request, lock, transaction, notification,
or error behavior. Passing `runtime.InTx(tx)` directly must behave exactly as
binding that same client and calling the bound form.

### 4.2 Reduce event ingress from three forms to two

Retain only these concepts:

| API | Meaning |
|---|---|
| `flow.Emit(work, event, key, value)` | stage an event in the active worker change set; commit or discard it with fenced settlement |
| `event.Emit(ctx, client, executionID, key, value)` | immediate targeted ingress in its own or a caller-owned transaction; detached from any worker result |

Delete `Event.Deliver(ctx, client, executionID, key, value)` and remove the
active-attempt rejection from method `Event.Emit`.

Today `Event.Emit` and `Event.Deliver` use the same target-side implementation;
the only difference is an active-attempt guard. Having both makes external
ingress vocabulary harder to explain and leaves developers deciding between
two nearly identical verbs. `Emit` is the natural event verb and matches the
terminology developers already expect. Its explicit `Client` and target
`ExecutionID` arguments distinguish immediate targeted ingress from the
attempt-scoped top-level helper.

The targeted capability itself is required. The enduring proof is Trails's
independently rooted bridge, edge, OIF, and provider monitors, which deliver an
exact generation-fenced fact into a different live `intent.run` execution.
They often use `Runtime.InTx(tx)` so the event and application-row changes
commit or roll back together. Trails Plan 004 removes the legacy separately
rooted receipt-transaction fan-in, but explicitly retains these monitor roots
and their targeted exact-event delivery. Removing the Flow capability would
therefore require a larger Trails redesign, such as a separate transactional
outbox publisher, and would add a durable concept rather than simplify Flow.
Plan 9 removes only the redundant `Deliver` name and keeps its behavior under
method `Event.Emit`.

After deletion:

- external/application publishers use `Event.Emit`;
- worker facts that must be atomic with success use top-level `flow.Emit`;
- deliberately detached cross-execution ingress from a worker uses method
  `Event.Emit` with an explicit client and target; and
- docs must warn that method emission to the current execution is legal but
  detached and is almost never the right replacement for `flow.Emit`.

The distinction is therefore structural, not a second vocabulary word:

```go
flow.Emit(work, TxnTerminal, key, payload) // current decision, atomic

TxnTerminal.Emit(ctx, client, targetID, key, payload) // named target, immediate
```

Remove the active-attempt rejection from method `Event.Emit`, but retain the
minimal private association between the handler context and its `Work` scope.
Plan 10 immediately uses that seam to validate `flow.Call` context ownership,
detect commit-callback re-entry, and install a private inline-dispatch bridge.
Deleting it in this untagged intermediate only to recreate it in Plan 10 adds
churn and risks mismatched worker/flowtest behavior. This does not authorize
broad context plumbing or a public context protocol: preserve only the current
private scope carrier and the worker scope needed by staged commands/events,
poisoning, snapshots, `WithCommit`, and test parity. Compile-contract tests
must continue proving typed event payload safety through method `Event.Emit`.

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

- `WithExecutionDeadline`;
- `WithTimeout`;
- `WithStartDelay`;
- root `Within`;
- `Node.Delay` and `Node.Within`;
- `RetryFor` elapsed bounds;
- `RetryPolicy.Backoff` entries; and
- `RetryAfter` explicit delays.

Do not silently change runtime-only operational tuning such as
`WithPollInterval` or `WithShutdownGrace`; those values are not part of an
execution's durable identity. Keep store-layer `ExactMilliseconds` validation
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
durations. Draft its entry for the combined v0.3.0 migration notes; Plan 10
finalizes those notes at the actual release boundary.

## 5. Example and documentation improvements

### 5.1 Add one realistic multi-queue example

Add `examples/pipeline` (or another neutral name) demonstrating the shape that
matters to Trails without copying Trails domain code:

1. start one live-key order execution;
2. stage independent payment and inventory commands on distinct queues;
3. emit exact completion events atomically from those workers;
4. join both events in a command that consumes no worker while gated;
5. deliver one external approval through `Runtime.InTx(tx)` alongside an
   application row update;
6. derive a dynamic successor from accepted results; and
7. inspect or trace the final execution.

The example must keep every executable unit a `Command`, use the `found` result
from `GetEventValue` for optional first-pass versus resumed behavior, compose
`LookupLiveExecution` and `Event.Emit` explicitly where live-owner emission
is useful, and demonstrate both a reusable bound command and a one-shot direct
`Command.Execute` call. It should
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
- a draft Plan 9 section for the combined v0.2-to-v0.3 migration guide; Plan
  10 finalizes the release document after its additive API/schema work.

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
- method `Event.Emit` is targeted and detached while top-level `flow.Emit` is
  attempt-atomic;
- fractional positive durable durations are rounded upward;
- Flow provides at-least-once handler invocation with exactly-once fenced
  durable settlement, not exactly-once remote side effects.

## 6. Deletions and migration map

| v0.2 form | v0.3 form | Nature | Durable effect |
|---|---|---|---|
| one-shot `cmd.With(client).Execute(ctx, key, args, opts...)` | either retain the bound form or use `cmd.Execute(ctx, client, key, args, opts...)` | direct form additive; inferred bound values remain natural | none |
| explicitly typed or rebound client-bearing `Command` values | `BoundCommand[A, R]` returned by `Command.With(client)` | breaking only for callers that name or rebind the old copied-command shape | none |
| `event.Deliver(ctx, client, id, key, value)` | `event.Emit(ctx, client, id, key, value)` | breaking, mechanical for deliberately detached ingress | none |
| `value, err := GetEventValue(...)` and command-key suffix tests | `value, found, err := GetEventValue(...)` | breaking, mechanical; `found` replaces presentation-key inference | none |
| app-local millisecond ceiling helper | pass positive duration directly | compatible input expansion | same integer-ms form |

The repository-wide migration is mechanical but broad. Update production,
tests, benchmarks, examples, compile fixtures, comments, specs, and evidence
commands. Historical completed plans and historical benchmark evidence must
not be rewritten to pretend the old API never existed; add a short historical
note only where a command would otherwise be impossible to reproduce.

For Trails, first determine whether Plan 004's V1 compatibility-root deletion
has landed. The migration checklist is then specifically:

1. do not migrate or restore V1 compatibility call sites already deleted by
   Plan 004;
2. classify any remaining V1 receipt-root delivery as transitional if Plan 004
   is still observing or draining it;
3. keep reusable `.With(client).Execute` values where binding improves the
   component, and use direct `Command.Execute(ctx, client, ...)` for genuinely
   one-shot starts;
4. convert every remaining `Event.Deliver` call to method `Event.Emit`;
5. replace the two `exactFlowDuration` helpers with direct duration values;
6. replace `/await/<gate>` suffix branching with the `found` result from
   `GetEventValue`; and
7. retain the domain-local live-owner delivery helper, including its existing
   error context and explicit generation behavior, for independently rooted
   bridge, edge, OIF, and provider monitors.

The disposable migration must prove an enduring monitor-to-`intent.run` path;
a legacy `txn.confirm` compatibility path alone is not sufficient evidence that
the targeted API still serves the post-Plan-004 architecture. Keep the Trails
Plan 004 deletion and the Flow v0.3 dependency/API migration in separate
production changes, even if their disposable verification branches overlap.

## 7. Phased implementation

### Phase 0 — Baseline, API inventory, and proof examples

Before editing production code:

1. reconcile any accepted change-set rename and obtain a clean implementation
   base;
2. record `go list -m all`, Go/PostgreSQL versions, schema version, and the
   current exported API;
3. inventory every `Command.With`, directly started command, `Event.Deliver`,
   method `Event.Emit`,
   durable-duration validation,
   and attempt-scope use in production, tests, examples, and docs;
4. inventory the corresponding Trails call sites without editing that
   repository, classify each targeted delivery as V1 compatibility or an
   enduring independent-monitor path, and record whether Trails Plan 004 has
   landed;
5. write small compile-only target examples for direct and reusable bound
   starts, staged/detached event ingress, and presence-aware event-input reads;
   and
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

### Phase 1 — Add start ergonomics and delete redundant event ingress

Implement Sections 4.1 and 4.2 together so the public vocabulary changes once:

- remove the hidden client field from `Command`, add the explicit
  `BoundCommand` adapter returned by `Command.With`, and retain bound starts;
- add the direct-client `Command.Execute` signature and route both start forms
  through one private implementation;
- remove `Event.Deliver`, convert its call sites to method `Event.Emit`, and
  remove method `Event.Emit`'s active-attempt rejection;
- retain top-level worker `flow.Emit` unchanged;
- retain the minimal private context-to-work scope carrier for Plan 10 while
  removing only the event-ingress rejection that consumed it here; and
- update generic compile-misuse tests and the removed-API AST guard.

Required tests:

- direct and bound Runtime starts produce identical creation, rediscovery,
  conflict, observation, and error behavior;
- direct and bound `Runtime.InTx(tx)` starts retain identical commit/rollback
  visibility;
- reusable bound commands work across sequential and supported concurrent
  starts, while transaction-bound lifetime rules are documented and tested;
- zero/nil/mismatched direct and bound clients retain current error
  classification;
- typed command arguments still fail at compile time;
- typed event payloads still fail at compile time through method `Emit`;
- targeted method emission from an active independent-monitor worker can
  release an exact generation-fenced gate in a different `intent.run`
  execution and remains independent of the source worker settlement;
- targeted method emission through `Runtime.InTx(tx)` follows caller-owned
  transaction commit and rollback visibility;
- staged `flow.Emit` still commits and rolls back with settlement; and
- compile-contract coverage prevents `With` and `Deliver` from being
  accidentally reintroduced.

Do not add deprecated forwarding methods. One clean combined v0.3.0 break
after Plans 9 and 10 is simpler than two permanent ways to do the same
operation.

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
optional input branch, and final trace. Keep execution time deterministic and
short.

### Phase 5 — Documentation, consumer proof, and Plan 10 handoff

1. synchronize the documentation listed in Section 5.2;
2. draft Plan 9's section of the combined v0.2-to-v0.3 migration guide; Plan
   10 owns its final release form;
3. ensure examples and doc snippets compile;
4. in a disposable Trails worktree or branch, perform the mechanical API
   migration against the repository's actual Plan 004 state and run its
   Flow-focused unit/integration tests; do not restore deleted V1 call sites,
   do not use a legacy-only delivery test as the compatibility proof, and do
   not commit cross-repository changes without separate authorization;
5. run source scans for deleted and forbidden concepts;
6. run the complete PostgreSQL and race gates; and
7. report results for human review, record the exact Plan 9 completion SHA,
   and hand it to Plan 10 without creating a tag or publishing a release.

The Trails proof is a Plan 10 progression gate because Trails is the concrete
compatibility target. It must include a retained bridge, edge, OIF, or provider
monitor delivering into the exact live `intent.run` generation; a separately
rooted V1 receipt transaction scheduled for deletion by Plan 004 is not enough.
If the migration requires a lost engine capability rather than a mechanical/
API-helper adjustment, stop and amend this plan.

These complete gates certify that Plan 9 is a safe intermediate, not that a
release is ready. Plan 10 must rerun the supported PostgreSQL matrix, the
combined Trails proof, documentation/API scans, migration upgrade tests, and
release review before v0.3.0 is tagged.

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
from an API-only project.

## 8. Explicit non-goals and deferred work

### 8.1 Retention needs its own design

Absurd provides task cleanup. Flow will eventually need a deliberate retention
story, but it is unsafe to bolt one onto this plan. Permanent keys currently
promise identity over retained Flow data, journal history powers replay/trace,
and executions own commands, waits, queues, and journal entries. Naive deletion
can permit an old permanent operation to run again or erase the evidence needed
for diagnosis.

Do not add purge SQL, partitions, tombstones, archive tables, or retention
options here. A later plan must first choose among retaining permanent-key
tombstones, compacting terminal executions, limiting cleanup to unkeyed/live-
key generations, or explicitly changing the key contract. It must include
bounded deletion, concurrent inspection/start behavior, foreign-key order,
vacuum impact, and operator recovery.

### 8.2 No speculative engine or schema tuning

This plan does not authorize:

- table collapse or a seventh table;
- new indexes, columns, constraints, triggers, or migration;
- queue partitions or queue-specific tables;
- journal compaction or format changes;
- scheduler, lease, maintenance, notification, or claim changes;
- a public lease-duration or renewal-timeout option;
- exactly-once handler or remote-effect claims;
- a Flow CLI, dashboard, or other operator application;
- dependency upgrades unrelated to a real build/security need;
- a new serialization library or framework; or
- broad renaming beyond the separately reviewed change-set work.

## 9. Acceptance criteria

Plan 9 is complete only when all of the following are true:

1. Flow still exposes one execution/command/event composition model and no
   task/step/checkpoint/workflow abstraction. The Plan 9 completion head does
   not pre-implement `flow.Call`, inline delivery, or migration 004.
2. The schema remains the same six tables and the latest migration/checksum is
   unchanged.
3. Journal encodings, canonical command/event/result data, replay, and trace
   remain compatible with v0.2.0 data.
4. `Command` no longer stores a hidden client; `Command.With(client)` returns
   an explicit `BoundCommand` that may be retained and reused.
5. Direct `Command.Execute(ctx, client, key, args, options...)` and bound
   `Command.With(client).Execute(ctx, key, args, options...)` share one start
   implementation and have identical transactional, idempotency, validation,
   observation, and error behavior.
6. `Event` has one targeted `Emit` method and no `Deliver` method.
7. Top-level `flow.Emit(work, ...)` retains fenced atomic settlement behavior.
   Method `Event.Emit` remains usable from an active worker when the caller
   deliberately supplies a client and target execution.
8. Removed API names are guarded against accidental reintroduction.
9. `GetEventValue` is the only event-input reader. It is O(1), in-memory,
   returns `(zero, false, nil)` for absence, returns `found=true` for a present
   typed value, and remains strict and decision-poisoning for malformed input.
10. No `LookupEventValue`, strict companion, or `Must` variant is added; normal
    gated callers explicitly reject an unexpected `found=false` result.
11. No `Action`, `DefineAction`, `HandleAction`, or `DeliverToLive` API is added;
    `None`-result work remains visibly a command and live delivery remains an
    explicit application composition.
12. Runtime and caller-owned transaction clients both work with every relevant
    new API, preserving lock-order guards and rollback visibility.
13. Every positive public durable scheduling duration is rounded upward once,
    and zero/negative/overflow behavior is tested.
14. Equivalent rounded durations produce identical durable fingerprints and
    rediscovery behavior.
15. Store-layer exact-millisecond validation remains strict.
16. The multi-queue example demonstrates live ownership, per-command queues,
    exact gates, a non-worker wait, transactional delivery, dynamic composition,
    a shared composed event key, stale-generation isolation, and final
    inspection without a DSL.
17. README, package docs, normative specs, component docs, examples, and the
    draft combined-migration guidance agree on the Plan 9 intermediate API
    without making statements that Plan 10 must reverse.
18. Historical plans/evidence remain historically accurate, and a disposable
    Trails migration compiles with its Flow-focused tests passing against the
    repository's actual Plan 004 state. The proof includes an enduring
    independent-monitor-to-`intent.run` targeted event, not only a V1
    compatibility path.
19. All named Flow tests run with no unintended skips against supported
    PostgreSQL majors; ordinary and race suites pass.
20. Build, vet, formatting, module verification/tidiness, vulnerability, and
    retained performance non-regression gates pass before human approval of
    the exact untagged Plan 9 completion SHA and handoff to Plan 10.

## 10. STOP conditions

Stop implementation and report the evidence if any of these occurs:

1. the working tree contains overlapping unreviewed changes that cannot be
   separated safely;
2. direct and bound `Execute` cannot share one implementation or differ in
   start identity, transaction ownership, validation, observation, error
   classification, or lock order;
3. deleting `Event.Deliver` or removing method `Event.Emit`'s attempt guard
   changes target-side identity, transaction, lock, idempotency, conflict, or
   cross-execution behavior instead of only consolidating the public verb;
4. `GetEventValue` cannot report ordinary absence without poisoning the
   decision, or cannot distinguish absence from present corruption;
5. duration normalization changes already exact values, stored integer-ms
   shapes, retry determinism, or permanent-start identity;
6. a public duration can round down, wrap, or overflow timestamp arithmetic;
7. the realistic example needs a task/step/checkpoint abstraction to remain
   understandable;
8. the Trails migration reveals loss of per-command queues, exact event gates,
   transactional ingress, independently rooted monitor delivery, version
   drains, cancellation, or generation fencing;
9. a migration/schema change or any Plan 10 `flow.Call`/inline-delivery work
   appears necessary in this intermediate; or
10. any full PostgreSQL/race gate fails repeatedly for an in-scope reason.

Do not weaken an invariant, preserve a redundant API indefinitely, or expand
the engine to work around a STOP condition. Amend the plan with the newly
discovered constraint.

## 11. Punchlist

### Baseline and decisions

- [ ] Reconcile any accepted change-set rename and start from a clean reviewed base.
- [ ] Record the implementation commit, Go version, PostgreSQL versions, schema version, and exported API inventory.
- [ ] Inventory all Flow and Trails uses of bound/direct command starts, `Event.Deliver`, method `Event.Emit`, duration normalization, and command-key suffix input detection; classify Trails targeted delivery as transitional V1 compatibility or enduring independent-monitor behavior.
- [ ] Confirm the Plan 9 intermediate signatures with compile-only examples
  before production edits; reserve final v0.3.0 API approval for Plan 10.
- [ ] Run and record the complete pre-change ordinary and race baseline.

### Public API ergonomics and deletion

- [ ] Remove the hidden client field from `Command`; add explicit `BoundCommand` storage while retaining `Command.With(client)`.
- [ ] Add `Command.Execute(ctx, client, key, args, options...)` and keep bound `Execute(ctx, key, args, options...)`.
- [ ] Route both forms through one private typed start implementation.
- [ ] Prove reusable bound commands and direct/bound Runtime and `Runtime.InTx(tx)` parity for semantics, errors, observations, and lock order.
- [ ] Remove `Event.Deliver(ctx, client, executionID, key, payload)`.
- [ ] Make method `Event.Emit` the sole immediate targeted ingress and retain top-level worker `flow.Emit` unchanged.
- [ ] Permit deliberate method `Event.Emit` calls from an active worker while preserving detached transaction semantics.
- [ ] Add a cross-execution worker test matching an enduring Trails bridge/edge/OIF/provider-monitor-to-`intent.run` delivery shape, including exact generation fencing and caller-owned transaction commit and rollback.
- [ ] Remove only the method-`Event.Emit` attempt rejection; retain the minimal
  private context/work-scope carrier required by immediately following Plan 10.
- [ ] Add compile/AST guards for removed APIs and compile-contract coverage for both command-start forms while retaining generic type-safety checks.
- [ ] Verify Runtime and `InTx` start/delivery commit, rollback, lock-order, and notification behavior.

### Durable duration ergonomics

- [ ] Add one overflow-safe upward millisecond-normalization helper for public durable inputs.
- [ ] Normalize execution deadline, attempt timeout, initial delay, and root/child wait budgets.
- [ ] Normalize retry elapsed bounds, backoff entries, and explicit retry-after delays.
- [ ] Preserve strict exact-millisecond store and decode validation.
- [ ] Prove equivalent normalized values have identical fingerprints and rediscovery behavior.
- [ ] Add the full positive/fractional/zero/negative/maximum/overflow test matrix.

### Optional event input

- [ ] Change `GetEventValue` to return `(value, found, error)` without adding a second reader.
- [ ] Update required-input callers to reject unexpected absence explicitly.
- [ ] Prove the single helper remains O(1), in-memory, deterministic, and strict for corruption.
- [ ] Guard against adding action aliases or a live-delivery routing abstraction.

### Example

- [ ] Add and test the realistic multi-queue pipeline example.
- [ ] Keep every executable unit a command, including `None`-result work.
- [ ] Use one deterministic entity/generation event-key helper at delivery, wait declaration, and lookup.
- [ ] Prove an event for an earlier generation cannot release a later generation's wait.
- [ ] Demonstrate explicit `LookupLiveExecution` plus `Event.Emit` composition and its terminal race.
- [ ] Explain every command boundary and keep the example free of a DSL.

### Documentation and consumer proof

- [ ] Update README and package documentation to the two-ingress/direct-and-bound-start vocabulary.
- [ ] Update project overview, functional spec, architecture, and component docs.
- [ ] Document presence-aware snapshot reads, explicit live lookup/emission races, and upward duration rounding.
- [ ] Document stable typed event definitions plus deterministic composed rendezvous keys.
- [ ] Draft Plan 9's section of the combined v0.2-to-v0.3 migration guide and
  the explicit Trails migration checklist for Plan 10 to finalize.
- [ ] Document the linear-workflow boundary rule and retained at-least-once/fencing semantics.
- [ ] Preserve historical plans and benchmark evidence as historical records.
- [ ] Perform the disposable Trails API migration against its actual Plan 004 state, without restoring deleted V1 call sites, and run its Flow-focused tests including an enduring independent-monitor delivery path.

### Final verification and Plan 10 handoff

- [ ] Confirm exactly six Flow tables and no migration/checksum changes.
- [ ] Scan for removed APIs and forbidden task/step/checkpoint/workflow concepts.
- [ ] Confirm Plan 9 did not add `flow.Call`, inline delivery fields, journal
  changes, or migration 004; those begin only after the reviewed handoff.
- [ ] Run gofmt, diff check, module verify/tidy, build, vet, and vulnerability gates.
- [ ] Run every named ordinary and race test with zero unintended PostgreSQL skips.
- [ ] Run the supported PostgreSQL-major matrix with durability settings enabled.
- [ ] Run retained performance shapes and record a non-regression conclusion.
- [ ] Review every changed hunk against this plan and all 20 acceptance criteria.
- [ ] Obtain human approval of the exact Plan 9 completion SHA, record it for
  Plan 10's drift check, and create no tag or release.
