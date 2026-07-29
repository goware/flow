---
status: draft
---

# Plan: make Flow smaller and lighter

## 1. Purpose

This plan reduces Flow before its public API and durable formats are frozen. The goal is not to turn Flow into a basic job queue. The goal is to preserve its defining value—durable, distributed, event-driven command execution with an ordered journal, composable graphs, crash recovery, and traceability—while removing mechanisms, names, and configuration choices that do not earn their cost.

The intended developer model remains:

```text
Commands instruct work.
Workers do the work.
Command completion records a durable terminal event.
Plans optionally declare bounded command graphs.
Plans, workers, and coordinators request further command execution through one operation.
External systems may emit durable facts into an execution.
Coordinators optionally drive adaptive agents and open-ended loops.
```

This is a pre-release reduction. Removed names receive no compatibility aliases, and removed behavior is deleted from the implementation, schema, documentation, tests, observations, replay model, and examples rather than being left behind as hidden legacy machinery.

## 2. Reduction principles

Every proposed deletion must satisfy all of these rules:

1. The direct command, dynamic fan-out, external monitor, and durable agent examples remain naturally expressible.
2. The ordered journal, leases, fencing, takeover, retries, atomic settlement, and replay guarantees remain intact.
3. The replacement is clearer than the removed feature rather than merely more verbose.
4. Adding the feature again later would be additive and would not require reinterpreting retained history.
5. A reduction in exported names must not be bought by replacing typed distinctions with `any`, raw strings where identity matters, or runtime-only invalid states.
6. Internal PostgreSQL complexity may remain when it provides an indexed access path, short lock duration, or narrow hot row. Application developers do not pay for table count in their API.

## 3. Target vocabulary

The primary action vocabulary becomes:

| Verb | Meaning |
|---|---|
| `Execute` | Durably request asynchronous command execution. The supplied scope determines whether it starts a new execution, reconciles a plan node, or stages a worker/coordinator child. |
| `Emit` | Record one externally observed application fact in an existing execution. |
| `Cancel` | Administratively terminate a command or execution. |

The supporting plan vocabulary becomes:

```text
After
AfterSettled
AfterFailed
WaitFor
Within
Delay
Optional
Children
Fact / Facts
Outcome
```

The worker and coordinator vocabulary remains:

```text
ResultOf / OutcomeOf
Execute
Node.Optional / Node.Delay
On / OnOutcome / OnStart
SucceedExecution / FailExecution
```

`Execute` has one developer-facing meaning everywhere:

> Durably request that this command be executed asynchronously. Never invoke its worker inline.

The receiver or scope supplies the durability boundary:

- `Command.Execute`, `PlanDef.Execute`, and `Coordinator.Execute` create or idempotently find a new execution, journal, deadline, and root authority;
- `flow.Execute(plan, ...)` records pure desired topology and reconciles it by stable command key across repeated evaluations;
- `flow.Execute(work, ...)` stages a child whose creation and membership closure commit atomically with the parent's successful terminal event;
- `flow.Execute(coordination, ...)` stages a command atomically with the coordinator state transition and inbox advance.

Those distinctions remain explicit in the journal's origin, parent, membership, causation, and transition records. They do not need separate public verbs because the scope already carries them.

## 4. Public API removals

### 4.1 Remove `Issue`

Delete external command injection into an existing execution:

```go
func Issue[A, R any](...)
```

An execution must have one orchestration authority:

- a plan reacts to an emitted fact and calls `flow.Execute(plan, ...)`;
- a coordinator reacts to an emitted fact and calls `flow.Execute(coordination, ...)`;
- independent work starts a separate direct execution with `Command.Execute`.

This removes a side door that bypasses the execution's plan or coordinator and has deliberately weak semantics: no dependencies, required-only classification, direct-mode rejection, separate ceiling errors, and command origins that the orchestration authority did not create.

Implementation consequences:

- delete the public function and store ingress request;
- delete the `external_issue` command origin;
- remove `Issue` from command-ceiling accounting prose and branches;
- remove `Issue` from `Runtime.InTx` documentation and acceptance criteria;
- remove its observer operation, SQL tests, replay cases, and error paths;
- keep `Command.Execute` for independent background work.

### 4.2 Remove `AfterAny`

Delete plan-level quorum dependencies:

```go
func (n *Node) AfterAny(count int, keys ...string) *Node
```

The first release retains the dependency predicates that have clear, common meanings:

- `After`: every predecessor succeeded;
- `AfterSettled`: every predecessor reached any terminal state;
- `AfterFailed`: every predecessor ended unsuccessfully.

Threshold behavior is expressed by one of two existing shapes:

1. For a settle-all partial result, execute candidates as `Optional`, join with `AfterSettled`, and inspect each with `OutcomeOf` in the joined worker.
2. For an early quorum, race, changing membership, or action before all stragglers settle, use a coordinator. That behavior is adaptive coordination rather than a static dependency predicate.

Implementation consequences:

- delete the method and `at_least` dependency kind;
- delete threshold evaluation and permanently-unsatisfiable calculations;
- remove the dependency-group `threshold` column and constraints;
- remove threshold fields from store values, journal bodies, replay, Trace, and flowtest;
- retain dependency groups and members for the three remaining predicates.

### 4.3 Remove additional staged application events from handlers

Delete:

```go
func Emit[T any](scope Scope, event Event[T], key string, payload T) error
```

Workers already produce the event required by the core model: returning `(result, nil)` atomically records the command's successful terminal event carrying the typed result. Coordinators already journal their state transition and in-execution command requests. No worked example requires a worker or coordinator to record an additional application event in the same decision.

When another part of the same execution needs to react to command completion, it uses:

- an exact command dependency or `Node.Outcome()` in a plan;
- `ResultOf` or `OutcomeOf` in a dependent worker;
- `OnOutcome` in a coordinator.

External observations remain first-class through the single `Event.Emit` ingress operation described in §5.1.

Implementation consequences:

- remove staged-event maps, order lists, codecs, validation, poisoning, and duplicate checks from worker/coordinator scopes;
- remove staged application events from the settlement batch order;
- remove staged-event output from flowtest;
- remove worker/coordinator event-size and cross-version-conflict test cases;
- retain automatic command terminal events and externally emitted application events.

If a future concrete workload requires several application facts to commit atomically with command success, staged emission can return as an additive handler capability.

### 4.4 Move plan command reads onto typed nodes

Delete the free plan-read helpers:

```go
func Result[A, R any](p *Plan, key string, cmd Command[A, R]) (R, bool)
func Outcome[A, R any](p *Plan, key string, cmd Command[A, R]) (CommandOutcome[R], bool)
func Children(p *Plan, parentKey string) ([]string, bool)
```

Make the node returned by `flow.Execute` carry the command's result type and expose its own plan reads:

```go
type Node[R any] struct { /* sealed scope, key, and definition */ }

func (n *Node[R]) Outcome() (CommandOutcome[R], bool)
func (n *Node[R]) Children() ([]string, bool)
```

`Node[R]` is an ephemeral handle owned by the current plan evaluation or handler decision. It must not be retained in application state, shared across goroutines, or reused after the callback returns. Durable references remain stable command keys, obtainable through `Key()`.

Example:

```go
analyze := flow.Execute(p, "analyze", Analyze, args)

outcome, settled := analyze.Outcome()
if !settled {
    return
}
if outcome.Status == flow.StatusSucceeded {
    // outcome.Result has type AnalyzeResult
}
```

`Result` is not reproduced as a node method. It maps both “not terminal yet” and “terminally unsuccessful, therefore no result can ever exist” to `false`, which is easy to misuse and previously created a completion-deadlock class. `Node.Outcome` makes terminal availability explicit while preserving the typed result on success.

`Node.Children` reads the runtime's authoritative child membership for that exact command instance. It returns `false` while membership may still change and `true` only after successful parent settlement closes membership.

Both reads are plan-only. Calling either method on a node returned from `flow.Execute(work, ...)` or `flow.Execute(coordination, ...)` poisons that enclosing decision as an invalid-scope programming defect. A handler-created command does not exist durably until settlement, so reading its outcome or children during the same handler invocation would be meaningless.

Plan-side typed reads are intentionally limited to nodes returned by `flow.Execute(plan, ...)` during the current evaluation. Worker-spawned child outcomes are consumed by a dependent worker through `ResultOf`/`OutcomeOf`, by a coordinator through `OnOutcome`, or structurally through `After*` using the authoritative keys returned by a plan parent's `Children()` call. Flow does not add a second API for manufacturing arbitrary node handles from strings.

Routine data plumbing remains outside plans. A dependent worker declares a dependency and reads its value with `ResultOf`, or reads every terminal state with `OutcomeOf`.

Implementation consequences:

- make `Node` generic on the command result type and return `*Node[R]` from package `flow.Execute`;
- delete the three free plan-read functions and the duplicated `(plan, key, command)` validation paths;
- retain the same consulted-read, lazy snapshot, terminal-availability, and failure-completion semantics behind node methods;
- update flowtest and plan simulation to record node `Outcome` and `Children` reads;
- retain worker `ResultOf`/`OutcomeOf` and coordinator `OnOutcome`.

### 4.5 Remove the public `Command.Done()` event descriptor

Delete:

```go
func (c Command[A, R]) Done() Event[R]
```

This removes only the generic application-event view over command success. It does **not** remove the automatic successful terminal event from the journal.

Command completion remains observable through the APIs that preserve command-instance identity and all terminal states:

- plan: `After`, `AfterSettled`, `AfterFailed`, and `Node.Outcome()`;
- worker: `ResultOf` and `OutcomeOf`;
- coordinator: `OnOutcome`;
- operator: `Trace` and `History`.

Coordinator success-only handlers use `OnOutcome` and inspect `CommandOutcome.Status`. That is one explicit branch and avoids two subscriptions that overlap for successful commands.

Implementation consequences:

- remove the derived event descriptor from `Command` definitions;
- remove the `command_success` event namespace from the public event system;
- reject command definitions as `WaitFor` operands;
- remove `On(command.Done(), ...)` and success/outcome overlap validation;
- retain exactly one terminal journal event for every command;
- retain typed successful results in `CommandOutcome[R]` and in the journal body;
- preserve the 256 KiB command-result limit for that terminal event.

### 4.6 Remove per-node retry overrides from plans

Delete:

```go
func (n *Node) MaxAttempts(max int) *Node
func (n *Node) RetryPolicy(policy RetryPolicy) *Node
```

Plans describe topology; command definitions describe how that kind of work executes. Retry policy, per-attempt timeout, and queue lane remain creation-time defaults on `DefineCommand`.

When two uses require materially different retry behavior, define two commands with distinct durable names and register the same Go worker implementation for both. The durable graph then makes the operational distinction visible rather than hiding it in one plan node.

Implementation consequences:

- remove node override state and validation;
- remove override fields from declaration equivalence, fingerprints, reconciliation, PlanReconciled summaries, flowtest, and Trace;
- preserve the command's accepted creation-time policy in `CommandCreated` and the command projection;
- preserve command-level `WithMaxAttempts`, `WithRetryPolicy`, `WithTimeout`, and `WithQueue`.

### 4.7 Remove separate plan and child-command verbs

Delete:

```go
func Do[A, R any](p *Plan, key string, cmd Command[A, R], args A) *Node
func Spawn[A, R any](scope Scope, key string, cmd Command[A, R], args A, opts ...SpawnOption) error

type SpawnOption interface { /* sealed */ }
func Optional() SpawnOption
func StartAfter(time.Duration) SpawnOption
```

Do not introduce the previously proposed `Declare` name. Replace every in-execution command request with the package function from §5.2:

```go
func Execute[A, R any](scope Scope, key string, cmd Command[A, R], args A) *Node[R]
```

The function returns a result-typed `Node[R]` builder in every in-execution scope. Plan declarations retain `After`, `AfterSettled`, `AfterFailed`, `WaitFor`, and `Within`. `Optional`, `Delay`, and `Key` are meaningful on nodes created in every scope:

```go
flow.Execute(p, "destination", SendDestination, args).
    After(origin.Key()).
    WaitFor(BridgeDelivered, correlationKey).
    Within(time.Hour)

flow.Execute(w, "analysis/1", AnalyzePart, args).
    Optional()

flow.Execute(c, "turn/2", Think, args).
    Optional().
    Delay(time.Minute)
```

To avoid enlarging M1 while unifying the verb, command-dependency and fact-wait modifiers remain plan-only. Applying `After*`, `WaitFor`, or `Within` to a worker/coordinator node is a deterministic scope defect that poisons the enclosing decision. The invalid decision cannot settle even if the returned `Node` is ignored. Supporting handler-created children with arbitrary dependencies may be added later only when a concrete workload justifies the extra engine semantics.

The error contract deliberately changes:

- external definition methods perform a PostgreSQL ingress transaction immediately and continue returning `(ExecutionHandle, error)`;
- in-execution `flow.Execute` performs no SQL and returns a `Node`;
- invalid in-execution definitions, duplicate conflicts, or modifiers record the first error on the plan/worker/coordinator scope;
- a poisoned plan records `PlanFailed`, a poisoned worker becomes a permanently failed attempt decision, and a poisoned coordinator records its existing deterministic failure path;
- callers no longer write repetitive `if err := flow.Spawn(...); err != nil` blocks for declaration defects that cannot be meaningfully recovered inside a handler.

The internal graph semantics are retained. A worker-created command is still recorded with its parent, causation, closed child membership, and atomic settlement boundary even though the public operation is named `Execute`.

## 5. Renames and consistency changes

### 5.1 Replace `Publish` with the sole explicit `Event.Emit`

Delete:

```go
func Publish[T any](ctx context.Context, c Client, id ExecutionID, event Event[T], key string, payload T) error
```

Add:

```go
func (e Event[T]) Emit(
    ctx context.Context,
    c Client,
    executionID ExecutionID,
    key string,
    payload T,
) error
```

Example:

```go
err := BridgeDelivered.Emit(
    ctx,
    rt,
    executionID,
    "intent/"+intentID,
    BridgeDelivery{TransactionHash: transactionHash},
)
```

This is the only explicit application-event write operation in M1. It always performs durable ingress; it never means an in-memory handler staging operation. `Runtime.InTx(tx)` remains accepted as the `Client`, so an application-table write and the event may still commit atomically.

No `Event.With(client)` binding is added in this reduction. Event emission already requires an execution ID and is less frequent than command execution; another binding method would save one argument while adding more state and API.

`Event.Emit` is an ingress operation for processes outside a handler: API services, webhooks, and monitors. Worker and coordinator handler code must not call it — or any other client ingress operation — from inside an attempt. Such a write commits immediately, outside the staged decision and outside the settlement fence, and survives even when the attempt later fails, retries, or loses its lease; with staged handler events removed, it is the one remaining way to leak an unfenced write. A fact a handler produces belongs in its typed result; a write inseparable from its success belongs in the registered commit function; a genuinely external fact belongs to the external process that observed it. The functional specification must state this prohibition alongside the `Event.Emit` contract.

### 5.2 Use `Execute` for every command request

Keep the existing definition methods that start executions:

```go
func (c Command[A, R]) Execute(ctx context.Context, key string, args A, opts ...ExecutionOption) (ExecutionHandle, error)
func (p PlanDef[A]) Execute(ctx context.Context, key string, args A, opts ...ExecutionOption) (ExecutionHandle, error)
func (c Coordinator[S]) Execute(ctx context.Context, key string, initial S, opts ...ExecutionOption) (ExecutionHandle, error)
```

Add one generic package function for work inside an existing execution:

```go
func Execute[A, R any](scope Scope, key string, cmd Command[A, R], args A) *Node[R]
```

`Scope` is a sealed capability implemented only by `*Plan`, `*Work[A]`, and `*Coordination[S]`. Application types cannot invent execution semantics. The runtime selects behavior by the concrete library-owned scope:

| Scope | Accepted meaning |
|---|---|
| `*Plan` | Reconciled desired command keyed within the execution. Re-evaluation is idempotent and never invokes the worker inline. |
| `*Work[A]` | Staged child command committed only with the parent's successful fenced settlement. |
| `*Coordination[S]` | Staged command committed only with the coordinator state transition and inbox advance. |

Go does not support overloaded methods, so a type-safe API cannot make both root and in-execution forms methods on `Command[A, R]` with different parameter lists. The definition method plus generic package function preserves static argument/result typing while presenting the same verb:

```go
SendReceipt.With(rt).Execute(ctx, "receipt/123", args)
flow.Execute(p, "origin", SendOrigin, args)
flow.Execute(w, "analysis/1", AnalyzePart, args)
flow.Execute(c, "turn/2", Think, args)
```

The package documentation must define `Execute` as “request durable asynchronous command execution,” never “call the worker now.” In particular, calling `flow.Execute` from a pure plan records a declaration in the plan recorder; it performs no application I/O and can be repeated safely during reconciliation.

Do not retain `Do`, `Declare`, or `Spawn` as aliases. Update error operations and observations to semantic terms such as `execute`, `plan_command`, and `child_command`; durable command origins remain explicit (`direct_root`, `plan`, `worker_child`, `coordinator_command`) because inspection still needs to explain how the command entered the graph.

### 5.3 Rename `Await` to exact-key `WaitFor`

Delete:

```go
func (n *Node) Await(events ...EventName) *Node
```

Add:

```go
func (n *Node[R]) WaitFor(event EventName, key string) *Node[R]
```

Each call names one exact application fact identity. Repeated calls combine with AND:

```go
flow.Execute(p, "destination", SendDestination, args.Destination).
    After(origin.Key()).
    WaitFor(BridgeDelivered, args.BridgeDeliveryKey).
    Within(time.Hour)
```

The wait identity is:

```text
(ExecutionID, event name, event version, event key)
```

`Within` remains a fact-wait deadline. Its clock starts after command dependencies settle. A previously emitted fact satisfies the exact wait immediately; a fact with the same definition and another key does not.

Exact keys make the event key a **contract between the publisher and the plan**, which changes what a valid key is:

- An event key must be a correlation identity both sides can derive before the fact occurs — an intent ID, an order ID, the execution key. Runtime-generated values such as transaction hashes or provider IDs can no longer serve as wait keys; they belong in the event payload. The previous monitor pattern of publishing under a natural key the plan never sees (`"delivery/" + txHash`) does not work under exact-key waits, and the rewritten monitor example must demonstrate the correlation-key form instead.
- When a wait key genuinely derives from a runtime value only a prior command produced, the plan must call that prior node's `Outcome()` before executing the waiting node. This reintroduces the result-gated declaration that the original `Within` design deliberately avoided; it is an accepted cost of exact correlation, and the documented mitigation is to design correlation keys so gating is unnecessary.
- Exact-key `Fact` and `WaitFor` selectors stay cheap only because of dirty-plan reconciliation: every event marks the execution dirty regardless of key, so no keyed routing or subscription set is maintained. Any future return to consulted-input routing must account for keyed selectors before it is considered.

### 5.4 Make singular `Fact` exact-key

Replace the unkeyed shape with:

```go
func Fact[T any](p *Plan, event Event[T], key string) (T, bool)
func Facts[T any](p *Plan, event Event[T]) []T
```

`Fact` reads one known durable fact. `Facts` deliberately folds all retained facts of a definition. There is no implicit “first fact of this type” behavior.

### 5.5 Use only `Node.Delay`

Delete the spawned-command option and its replacement alias:

```go
func StartAfter(time.Duration) SpawnOption
func Delay(time.Duration) SpawnOption
```

Keep the single node modifier:

```go
func (n *Node[R]) Delay(duration time.Duration) *Node[R]
```

It has the same meaning for plan, worker, and coordinator scopes:

```go
flow.Execute(c, key, Think, args).
    Delay(20 * time.Second)
```

The PostgreSQL-time anchor, immutable accepted schedule, retry-budget behavior, and execution-deadline cap remain unchanged.

### 5.6 Expose an in-execution node's key

Add the small accessor:

```go
func (n *Node[R]) Key() string
```

This avoids repeating dependency literals while preserving the required distinction between a command definition name and an execution-local command instance key:

```go
origin := flow.Execute(p, "origin", SendOrigin, args.Origin)
flow.Execute(p, "destination", SendDestination, args.Destination).
    After(origin.Key())
```

Explicit keys remain mandatory. Flow must support several instances of one command definition, dynamic child keys, stable retry identity, readable traces, and references across separate plan evaluations. Defaulting a key from `Command.Name()` would hide collisions and make command renames alter graph identity.

## 6. Target examples

### 6.1 Direct background work

No orchestration vocabulary is required:

```go
handle, err := SendReceipt.With(rt).Execute(
    ctx,
    "receipt/"+orderID,
    SendReceiptArgs{OrderID: orderID},
)
```

`Execute` durably enqueues and may be handled by another compatible replica.

### 6.2 Planned dependency and external fact

```go
func planIntent(p *flow.Plan, args IntentArgs) {
    origin := flow.Execute(p, "origin", SendOrigin, args.Origin)

    flow.Execute(p, "destination", SendDestination, args.Destination).
        After(origin.Key()).
        WaitFor(BridgeDelivered, args.BridgeDeliveryKey).
        Within(time.Hour)
}

err := BridgeDelivered.Emit(
    ctx,
    monitorClient,
    executionID,
    bridgeDeliveryKey,
    delivery,
)
```

### 6.3 Dynamic fan-out and fan-in

```go
func planReport(p *flow.Plan, args ReportArgs) {
    prepare := flow.Execute(p, "prepare", PrepareReport, PrepareArgs{
        CompanyID: args.CompanyID,
    })

    children, closed := prepare.Children()
    if !closed {
        return
    }

    flow.Execute(p, "generate", GenerateReport, GenerateArgs{
        AnalysisKeys: children,
    }).After(children...)
}

func prepareReport(ctx context.Context, w *flow.Work[PrepareArgs]) (PrepareResult, error) {
    analyses, err := determineAnalyses(ctx, w.Args.CompanyID)
    if err != nil {
        return PrepareResult{}, err
    }

    for _, analysis := range analyses {
        flow.Execute(
            w,
            "analysis/"+analysis.ID,
            AnalyzePart,
            analysis.Args,
        )
    }

    return PrepareResult{Count: len(analyses)}, nil
}
```

The generate worker reads successful dependency values with `ResultOf`. A partial-result worker uses `OutcomeOf` after `AfterSettled`.

### 6.4 Durable adaptive agent

```go
var ResearchAgent = flow.DefineCoordinator[AgentState](
    "research_agent",
    1,
    flow.OnStart(startAgent),
    flow.On(UserMessage, onUserMessage),
    flow.OnOutcome(Think, onThought),
    flow.OnOutcome(RunTool, onToolOutcome),
)

func onToolOutcome(
    ctx context.Context,
    c *flow.Coordination[AgentState],
    received flow.Received[flow.CommandOutcome[ToolResult]],
) error {
    // Success, failure, cancellation, expiry, and skip all arrive here.
    // Update durable agent state and request the next bounded command.
    flow.Execute(
        c,
        nextThinkKey(c.State),
        Think,
        nextThinkArgs(c.State),
    ).Optional().Delay(2 * time.Second)
    return nil
}
```

No `Command.Done()` subscription is needed. `OnOutcome` is the one coordinator view over command terminality.

## 7. Behavior intentionally retained

The following are not part of this reduction:

- the per-execution ordered journal and `CommandCreated` plus exactly-one-terminal-event invariant;
- attempt lifecycle history, because exact operational retracing is a core product goal;
- leases, renewal, fencing, ambiguous-commit recovery, takeover, and `SKIP LOCKED` claims;
- automatic successful command events and runtime failure/cancellation/expiry/skip events;
- direct, plan, and coordinator execution modes;
- plans and dirty reconciliation;
- coordinators, `OnOutcome`, serialized state, and open-ended durable-agent execution;
- atomic worker-child membership and closure behind `flow.Execute(work, ...)`;
- `After`, `AfterSettled`, and `AfterFailed`;
- `Optional`, `Within`, and one-shot durable `Delay`;
- `WithCommit`, `Commit[A, R]`, and narrow `flow.Tx`;
- transaction-free workers and short fenced settlement transactions;
- command-level retry policies, elapsed budgets, timeouts, queues, and runtime concurrency controls;
- `WithFailFast(false)`, which provides settle-all behavior for direct fan-outs as well as plan executions;
- `CancelCommand` and `CancelExecution`;
- execution metadata and filtering for the future operational UI;
- optional local affinity;
- PostgreSQL `LISTEN`/`NOTIFY` wake hints with polling as the correctness fallback;
- `WithoutExecutionDeadline`, because intentionally open-ended durable agents may require it;
- `Trace`, `History`, replay, simulation, observers, and flowtest;
- all nine prefixed PostgreSQL tables.

## 8. Rejected simplifications

### 8.1 Do not erase scope semantics while unifying `Execute`

One verb does not mean one storage transition. The implementation must keep separate internal acceptance paths for plan reconciliation, worker settlement, and coordinator decisions. Inspection and history must continue to expose whether a command was a plan node, worker child, or coordinator output, including parentage, causation, and atomic batch membership.

The universal `Node` necessarily exposes plan-only modifiers on nodes returned for worker/coordinator scopes. M1 resolves that language limitation with deterministic scope poisoning rather than adding separate public node types or restoring separate verbs. This trade is acceptable only because:

- the invalid call is a declaration/programming defect, not a recoverable application condition;
- the first error is retained even when the caller ignores the returned node;
- no partial child set, coordinator state, application write, or terminal success can commit;
- flowtest and compile examples document which modifiers each scope accepts.

Do not respond by adding `PlanNode`, `WorkNode`, `CoordinatorNode`, bound-call wrappers, `any`, or variadic argument decoding. Those would recover nominal type separation by making the surface larger or less type-safe than the three verbs being removed.

### 8.2 Do not derive command instance keys from command names

A definition name identifies a kind of work. A command key identifies one occurrence within one execution. Trails-style workflows commonly execute the same command kind for origin, destination, edge, and multiple dynamic children. Keep explicit keys and use `Node.Key()` to avoid duplicated literals.

### 8.3 Do not fold command dependencies and event waits into one `After`

They differ materially:

- a command dependency names an exact command instance and can become permanently unsatisfiable;
- an application-event wait names an append-only external fact and may expire under `Within`.

`After` and `WaitFor` should look related but remain distinct.

### 8.4 Keep the registered commit function; do not add `Work.Tx()` or dynamic `OnCommit`

A worker must not hold a PostgreSQL transaction or connection while performing slow or external work. The registered commit function runs later in a short fenced settlement transaction and receives only durable arguments, result, and command metadata — its inputs are constrained by construction rather than by documentation. A closure can capture values that are absent from the journal and application result, weakening the promise that retained history explains accepted work.

The registered function is also the only placement for an application write that carries all three completion guarantees at once:

- **atomic** — the application row and the terminal event commit or roll back together, with no window in either direction;
- **fenced** — a stale or superseded worker's write is rejected by the same lease check that rejects its result; a mid-work write on a worker-owned connection escapes that fence entirely;
- **exactly-once relative to the accepted outcome** — the write itself needs no idempotency key.

A follow-up command remains the durable-but-eventually-consistent alternative for writes that deserve their own identity and retry policy. The commit function is the completion-side twin of `Runtime.InTx` ingress and part of the single-PostgreSQL value proposition; deleting it would forfeit a guarantee that RPC-completed systems structurally cannot offer.

Two documentation obligations replace any API change:

1. The functional specification presents the adjacency pattern as the first-class form — `Work` and `Commit` methods on one worker type, registered together — so one command's logic reads as one unit:

```go
type PrepareReport struct{ reports *reports.Store }

func (p PrepareReport) Work(ctx context.Context, w *flow.Work[PrepareArgs]) (PrepareResult, error) { ... }
func (p PrepareReport) Commit(ctx context.Context, tx flow.Tx, c flow.Commit[PrepareArgs, PrepareResult]) error { ... }

rt.Register(flow.Handle(PrepareReportCmd, p.Work, flow.WithCommit(p.Commit)))
```

2. The `WithCommit` documentation leads with the three-line decision rule, in this order:
   - **nothing** — the typed result is the record; most workers stop here;
   - **a follow-up command** — the write deserves its own retry policy, identity, and trace vertex, and may be eventually consistent with success;
   - **a commit function** — the write is part of what success *means*, and no state may exist where Flow and the application table disagree.

### 8.5 Do not merge tables for cosmetic reduction

The nine tables are eight runtime tables plus the migration ledger. The queue and coordinator tables isolate hot lease churn; dependency and event-wait tables provide different reverse indexes and resolution semantics; the journal and projections have different mutation and retention behavior. Removing an internal table is not a developer-experience improvement when it produces wider hot rows, polymorphic nullable indexes, or graph scans.

## 9. PostgreSQL and durable-format changes

The implementation is still pre-release. Unless a durable external database is declared before this work starts, update the initial migration in place and recreate development/test schemas. Do not carry compatibility aliases or dead columns into M1.

Required schema changes:

1. Add `event_key` to `flow_command_event_waits` and include it in uniqueness and reverse-lookup indexes.
2. Remove the dependency-group `threshold` column and restrict group kinds to `all_succeeded`, `all_settled`, and `all_failed`.
3. Remove `external_issue` from command-origin constraints.
4. Remove schema constraints or namespace cases that exist only for `command_success` as an application-event selector.
5. Retain all nine table names and the `flow_` prefix.

Required durable-body changes:

- `CommandCreated` continues to contain canonical arguments, origin, parent, dependencies, wait selectors including event keys, accepted schedule, policy, and causation;
- plan reconciliation bodies no longer contain quorum thresholds or node retry overrides;
- worker/coordinator transition bodies no longer contain staged application events;
- command terminal journal entries remain unchanged in purpose and reconstructibility;
- application-event identity remains `(execution_id, event_name, event_key)` across versions;
- replay must reconstruct the same projections after every retained entry.

If preserving an existing non-test database becomes a requirement, stop before editing the initial migration and write a separate compatibility migration and retained-journal conversion plan.

## 10. Implementation workstreams

### Phase 1 — synchronize the specifications

1. Mark `functional_spec.md`, `architecture.md`, component documents, and `implementation_plan.md` as draft because this amendment changes their contracts.
2. Update the project overview's developer model and examples.
3. Apply every removal and rename to the functional specification, including signatures, behavior, examples, limits, acceptance criteria, and surface inventory.
4. Update architecture and component documents only after the functional contract is coherent.
5. Replace the original implementation-plan entries for `Issue`, `Publish`, staged `Emit`, `Do`, `Spawn`, `SpawnOption`, `AfterAny`, `Command.Done`, and the free plan `Result`/`Outcome`/`Children` helpers with the reduced design.
6. Add the commit-function decision rule and the worker-object adjacency example from §8.4 to the functional specification's `WithCommit` section.
7. Audit every occurrence of the word `Emit` in the specifications, documentation, and code comments for the new meaning. `Emit` previously named the staged handler operation and now names external ingress; a name search alone cannot catch prose that survived with the old semantics attached to the same word.

Exit condition: searching the normative specifications finds no old API name except in an explicit historical rationale, and every remaining `Emit` reference describes external ingress.

### Phase 2 — reduce and rename the public contracts

1. Add package `flow.Execute`, broaden the sealed `Scope` to the three library-owned in-execution scopes, make `Node[R]` result-typed, and make it record scope defects.
2. Add `Event.Emit`, `Node.WaitFor`, `Node.Key`, `Node.Children`, `Node.Outcome`, and keyed `Fact`; retain `Node.Optional` and `Node.Delay` as the only option forms.
3. Remove `Publish`, plan `Do`, `Spawn`, `SpawnOption`, free `Optional`, `Await`, and `StartAfter` without aliases; do not introduce `Declare` or free `Delay`.
4. Remove `Issue`, staged `Emit`, free plan `Result`/`Outcome`/`Children`, `Command.Done`, `AfterAny`, and plan-node retry override methods.
5. Simplify sealed interfaces, decision errors, and option state made unnecessary by those deletions.
6. Update compile-contract tests and `go doc` assertions, including accepted modifiers and deterministic invalid-scope cases.

Exit condition: the package compiles with the target vocabulary, and none of the removed exported identifiers appears in `go doc github.com/goware/flow`.

### Phase 3 — simplify the engine and deterministic test harness

1. Replace separate plan declaration and staged-child recorders with one typed `flow.Execute` entry point while retaining their distinct internal decision representations and acceptance paths.
2. Remove staged application events from decision buffers and canonical decision comparison.
3. Remove quorum group evaluation and threshold state.
4. Remove command-success event descriptors from definitions and event-selector validation.
5. Make exact event key part of plan facts, reads, waits, declaration equivalence, and simulation.
6. Move plan child-membership and command-outcome reads behind typed `Node[R]` methods without changing consulted-read or lazy-snapshot behavior.
7. Remove node retry override resolution and reconciliation identity.
8. Update flowtest worker, plan, coordinator, and direct simulations, including scope poisoning after ignored invalid `Execute` modifiers or node reads.

Exit condition: database-free property tests cover only the retained dependency and event semantics, including exact-key fact selection and waits.

### Phase 4 — simplify PostgreSQL storage and ingress

1. Update the initial migration and constraints from §9.
2. Replace `Publish` store paths with the implementation behind `Event.Emit`.
3. Delete external command ingress and `Issue` SQL.
4. Delete staged application-event settlement writes.
5. Update keyed wait insertion, emit-before-wait resolution, wait-before-emit resolution, expiry, and indexes.
6. Update journal codecs and replay together with each changed write path.

Exit condition: fresh migration, replay-vs-projection conformance, and real-PostgreSQL store tests pass with no legacy branches.

### Phase 5 — simplify runtime scheduling and coordination

1. Remove wake-up, observation, fault, and command-ceiling paths that existed only for `Issue` or staged events.
2. Remove coordinator success-only subscription selection and overlap checks.
3. Keep `OnOutcome` indexed delivery for every command terminal state.
4. Rename delayed-child observations and diagnostics to `Delay` while preserving stored timestamps.
5. Verify external event emitters remain lightweight runtimes that need not register a plan, worker, or coordinator.

Exit condition: distributed command, plan, monitor, and coordinator tests pass across multiple replicas and poll-only mode.

### Phase 6 — rewrite examples, documentation, and end-to-end tests

Update all four examples and their shared E2E scenarios:

1. direct command: unchanged apart from documentation;
2. fan-out: `flow.Execute` in plan and worker scopes, `Node.Key`, `Node.Children`, and `ResultOf`;
3. monitor: exact-key `WaitFor` and external `Event.Emit`, including `Runtime.InTx` atomic publication;
4. agent: `OnOutcome` only for command completion and `Delay` for the next turn.

Retain the application-table `WithCommit` example proving atomic application writes, command success, and staged children while the worker holds no database connection. Present it in the worker-object form (`Work` and `Commit` methods on one type) so one command's logic reads as one unit, preceded by the §8.4 decision rule.

Exit condition: examples compile as public API clients, run against real PostgreSQL, and directly assert Trace, History, journal order, graph edges, retries, and terminal state.

### Phase 7 — final reduction audit

1. Run the complete unit, integration, E2E, race, fault-injection, replay, and benchmark suites with PostgreSQL enabled.
2. Run repository-wide searches for removed names and internal concepts.
3. Inspect `go doc` for the intended progressive-disclosure surface.
4. Record fresh query plans for exact keyed wait resolution and ensure the new index avoids execution-wide scans.
5. Confirm there are still exactly nine prefixed tables and no obsolete threshold or issue columns/constraints.
6. Mark the synchronized specifications and this amendment complete only after human review.

## 11. Acceptance checklist

### Public API

- [ ] `Command.Execute`, `PlanDef.Execute`, `Coordinator.Execute`, and package `flow.Execute` all mean durable asynchronous execution, never invoke a worker inline, and retain their scope-specific idempotency rules.
- [ ] Plans, workers, and coordinators use package `flow.Execute`; `Do`, `Declare`, and `Spawn` do not exist.
- [ ] Package `flow.Execute` preserves plan reconciliation, atomic worker-child membership, and atomic coordinator decision semantics according to its sealed scope.
- [ ] Invalid definitions, modifiers, or node reads poison an in-execution scope even when the returned `Node` is ignored; no partial command set, application write, coordinator state, or successful terminal event commits.
- [ ] External facts use `Event.Emit`; neither `Publish` nor staged `flow.Emit` exists.
- [ ] In-execution options use `Node.Optional` and `Node.Delay`; `SpawnOption`, free `Optional`, free `Delay`, and `StartAfter` do not exist.
- [ ] `WaitFor` and singular `Fact` require an exact event key.
- [ ] `Node[R]` preserves its command result type; `Key()` works in every in-execution scope, while `Children()` and `Outcome()` are plan-only reads.
- [ ] `Node[R]` is evaluation/decision-local and cannot be retained or shared as durable application state; `Key()` is the durable reference.
- [ ] Free plan `Result`, `Outcome`, and `Children` helpers, `Command.Done`, `AfterAny`, `Issue`, and plan-node retry overrides do not exist.
- [ ] `WithCommit` remains the only application-write hook — no `OnCommit` or `Work.Tx()` — and its documentation leads with the §8.4 decision rule and worker-object adjacency example.

### Semantics

- [ ] Successful worker return still records exactly one typed terminal event with its result.
- [ ] Failed, cancelled, expired, and skipped commands still record exactly one terminal event.
- [ ] `OnOutcome` delivers every command terminal state exactly once to the coordinator.
- [ ] `Node.Outcome()` returns unavailable only while its plan command is non-terminal and returns a typed `CommandOutcome[R]` for every terminal state, so unsuccessful commands cannot create a permanently absent result read.
- [ ] `Node.Children()` returns the authoritative stable child set only after successful membership closure; pending membership is a consulted-but-absent plan read and unsuccessful parent terminality cannot block execution failure.
- [ ] Two facts with the same definition and different keys release only their exact matching waits.
- [ ] Wait and `Fact` keys in examples are correlation identities known before the fact occurs; no example uses a runtime-generated value as an event key.
- [ ] Handler code performs no client ingress; the specification prohibits `Event.Emit` from inside an attempt and no example or test violates it.
- [ ] Emit-before-wait and wait-before-emit both progress without a lost wake-up.
- [ ] `Within` begins only after command dependencies settle and a late fact cannot resurrect an expired wait.
- [ ] Partial-result joins work with `Optional`, `AfterSettled`, and `OutcomeOf`.
- [ ] Early quorum behavior remains expressible through a coordinator.
- [ ] Retry policy accepted from the command definition remains durable and unchanged across deployment default changes.
- [ ] Dynamic fan-out membership still closes atomically with successful parent settlement.

### Durability and operations

- [ ] `Event.Emit` is idempotent by execution, event name, and key across versions and works through `Runtime.InTx`.
- [ ] The journal alone plus its reducer reconstructs command existence, topology, attempts, facts, and terminal outcomes.
- [ ] Claims remain capacity-bounded, use `SKIP LOCKED`, and never hold a database connection while a worker runs.
- [ ] Lease loss, takeover, retries, cancellation, and ambiguous commits preserve their existing guarantees.
- [ ] Notifications remain hints and poll-only operation remains correct.
- [ ] Direct, plan, coordinator, API-only publisher, and split worker deployments remain supported.
- [ ] The four real-PostgreSQL examples and their E2E variants pass without unexpected skips.

### Size reduction

- [ ] Removed API names are absent from `go doc` and repository source outside historical review documents.
- [ ] The `external_issue`, staged-event, command-success-selector, quorum-threshold, and node-policy-override branches are deleted rather than disabled.
- [ ] The dependency threshold column and obsolete constraints are absent from a fresh schema.
- [ ] No compatibility aliases or deprecated wrappers are introduced before M1.

## 12. Expected result

Flow remains a durable execution engine rather than shrinking into a queue, but the application-facing grammar becomes substantially smaller:

```text
Execute a run.
Execute commands from plans, workers, and coordinators.
Emit external facts.
React to exact dependencies and terminal outcomes.
Trace everything from the journal.
```

The reduction removes several advanced or overlapping paths while preserving the four product-defining scenarios. It also makes the implementation lighter: one command-ingress path disappears, handler decisions no longer stage arbitrary events, command success is no longer duplicated into the application-event abstraction, plan reconciliation loses quorum and per-node policy branches, and event correlation becomes exact rather than implicit.
