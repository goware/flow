---
status: draft
---

# Functional Spec: flow

## 1. Purpose

`flow` is a PostgreSQL-backed Go library for running an execution graph of durable work.

It targets services that already use PostgreSQL and need to run multi-step processes that take minutes or hours, wait on external systems, fan out and join, retry on failure, handle their own failures, and remain traceable while running.

The library provides three concepts at the top level:

| Concept | Meaning |
|---|---|
| **Flow** | One durable execution of a state machine. Has identity, optional typed state, a graph of steps, and immutable history. Its steps execute in parallel but commit one at a time. |
| **Step** | One unit of work in a flow. Durable, retriable, fenced, individually traceable. Handlers are ordinary Go functions with typed arguments and typed results. |
| **Signal** | An immutable external fact delivered to a flow, which can release steps waiting on it. |

## 2. Scope

### 2.1 Single backend

`flow` supports PostgreSQL only. There is no backend abstraction, no backend-neutral contract layer, and no conformance suite for alternative stores. The library imports `pgx` and `pgkit` directly and exposes PostgreSQL transactions where composition requires them.

This is a deliberate simplification. Portability is not a goal; a small, concrete API is.

### 2.2 Milestone 1

- flow start, identity, and optional typed state;
- versioned step definitions, typed handlers, typed results, and dispositions;
- graph construction, dependencies, fan-out, joins, and failure-handling branches;
- immutable keyed signals and durable waiting with mandatory deadlines;
- retries, failure classification, and retry-budget accounting;
- cancellation at flow and step level, deadlines, and expiry;
- atomic completion including application writes;
- worker runtime with lease renewal and graceful shutdown;
- inspection: flow lookup, full trace, history, listing, and await;
- embedded migrations.

### 2.3 Later capabilities

Not required for the initial release: recurring/scheduled flows, per-key rate limits, tenant quotas, an HTTP admin API or UI, history archival/export, OpenTelemetry adapters, cross-flow subscriptions, durable pub/sub fan-out, and mutable last-write-wins state cells as a primitive distinct from signals.

### 2.4 Explicit non-goals

- transparent replay or determinism requirements on handler code;
- pinning in-flight work to a build of the executable;
- exactly-once effects against external systems;
- a general message broker, topic fan-out, or event-sourcing store;
- strict global FIFO ordering;
- multi-region or multi-database coordination.

## 3. Public API

This section is the developer-experience contract.

### 3.1 Runtime

```go
type Runtime struct{ /* ... */ }

func New(db *pgkit.DB, opts ...Option) (*Runtime, error)
func (r *Runtime) Run(ctx context.Context) error
func (r *Runtime) Stop(ctx context.Context) error
func (r *Runtime) Client() Client
func (r *Runtime) InTx(tx pgx.Tx) Client

func Migrate(ctx context.Context, db *pgkit.DB, opts ...MigrateOption) error
```

`New` validates configuration and schema compatibility; it never migrates implicitly and starts no goroutines. `Run` starts workers and blocks until the context ends or `Stop` is called. `Run` may be called once.

### 3.2 Definitions

```go
type None = struct{}

type Handler[A, R any] func(context.Context, *Step[A, R]) error

type StepRef[A, R any]   struct{ /* ... */ }
type SignalRef[T any]    struct{ /* ... */ }
type StateRef[T any]     struct{ /* ... */ }

func Define[A, R any](kind string, version int, h Handler[A, R], opts ...StepOption) StepRef[A, R]
func DefineSignal[T any](name string, version int) SignalRef[T]
func DefineState[T any](name string) StateRef[T]

func (r StepRef[A, R]) Register(rt *Runtime) error
func (r StepRef[A, R]) Kind() string
func (r StepRef[A, R]) Version() int

type Registrable interface{ Register(*Runtime) error }
func RegisterAll(rt *Runtime, defs ...Registrable) error

func WithMaxAttempts(int) StepOption
func WithRetryPolicy(RetryPolicy) StepOption
func WithTimeout(time.Duration) StepOption        // per-execution wall clock
func WithQueue(string) StepOption                 // worker lane
func WithState[T any](StateRef[T]) StepOption     // root steps: declares the flow's state type
```

`A` is the step's argument type and `R` its result type; use `flow.None` for either when unused. A step definition is **not bound to a flow state type**, so the same definition may be used by different flow types.

`Define` is normally called at package level and produces a **typed step reference**. Everywhere a step is named — starting a flow, declaring a graph node — the reference is used rather than a string. This makes three classes of mistake fail to compile: a mistyped or renamed step kind, arguments of the wrong type for the target step, and a result decoded at the wrong type.

`Define` performs no registration and touches no global state. `Register` attaches a definition to a runtime and is valid only before `Run`.

### 3.3 Versioning

Every step definition carries an explicit integer `version` alongside its kind. The pair `(kind, version)` is persisted on every step, and the root step's pair is persisted on every flow.

A worker claims only the `(kind, version)` pairs it has registered. A process may register several versions of one kind simultaneously:

```go
var (
    SendTxnV1 = flow.Define("send_txn", 1, sendTxnV1)
    SendTxnV2 = flow.Define("send_txn", 2, sendTxnV2)
)
```

This is **durable-schema versioning, not executable pinning**. It exists so that argument, result, and state schemas may evolve while in-flight work created under an older schema is still decodable by a handler that understands it. A new binary registers both versions until old work drains, then drops the old definition in a later release. Handler code for a given version may still be changed and redeployed freely.

Version `0` is reserved and rejected; the first version is `1`.

### 3.4 Flow operations

```go
type FlowID string

type Handle struct {
    ID      FlowID
    Type    string
    Key     string
    Created bool
}

func Start[A, R any](ctx context.Context, c Client, root StepRef[A, R], key string, args A, opts ...StartOption) (Handle, error)
func Signal[T any](ctx context.Context, c Client, id FlowID, ref SignalRef[T], payload T, opts ...SignalOption) error
func Cancel(ctx context.Context, c Client, id FlowID, reason string) error
func CancelStep(ctx context.Context, c Client, id FlowID, stepKey string, reason string) error
func Retry(ctx context.Context, c Client, id FlowID, stepKey string, opts ...RetryOption) error

func WithFlowDeadline(time.Duration) StartOption
func WithoutFlowDeadline() StartOption
func WithFlowMetadata(map[string]string) StartOption
func WithFailFast(bool) StartOption
func WithSignalID(string) SignalOption
func RetryWithArgs[A any](A) RetryOption
func RetryWithMaxAttempts(int) RetryOption
```

`Start` creates a flow from a root step reference. The flow's type is that reference's kind. Starting the same `(type, key)` again returns the existing flow with `Handle.Created == false`.

### 3.5 Inspection

```go
func Get(ctx context.Context, c Client, id FlowID) (Flow, error)
func Lookup(ctx context.Context, c Client, typ, key string) (Flow, error)
func Trace(ctx context.Context, c Client, id FlowID) (FlowTrace, error)
func History(ctx context.Context, c Client, id FlowID, opts ...HistoryOption) ([]Event, error)
func List(ctx context.Context, c Client, f Filter) (Page, error)
func Await(ctx context.Context, c Client, id FlowID, opts ...AwaitOption) (Flow, error)

func ResultOf[R any](src ResultSource, node NodeRef[R]) (R, error)
func ResultByKey[R any](src ResultSource, key string) (R, error)
func Read[T any](src StateSource, ref StateRef[T]) (T, error)
```

`ResultSource` and `StateSource` are satisfied by both `FlowTrace` and `*Step[A, R]`, so typed reads work identically from an inspection call and from inside a handler. `ResultOf` takes a node reference and needs no explicit type argument; `ResultByKey` is the escape hatch for a step declared in an earlier, out-of-scope declaration.

### 3.6 Step context

```go
type Step[A, R any] struct {
    Args A
}

func (s *Step[A, R]) Done(result R) error
func (s *Step[A, R]) Wait(sig SignalName, within time.Duration) error
func (s *Step[A, R]) Graph() *Graph
func (s *Step[A, R]) OnCommit(func(context.Context, pgx.Tx) error)
func (s *Step[A, R]) Info() StepInfo

type SignalName interface{ Name() string }   // satisfied by SignalRef[T]

func Update[T any](s StepScope, ref StateRef[T], fn func(*T) error) error
func Received[T any](s StepScope, ref SignalRef[T]) (T, bool)
func ReceivedAll[T any](s StepScope, ref SignalRef[T]) ([]T, error)
```

### 3.7 Graph construction

```go
type Graph   struct{ /* ... */ }
type NodeRef[R any] struct{ /* ... */ }
type Dep     interface{ /* ... */ }

func Add[A, R any](g *Graph, key string, ref StepRef[A, R], args A) NodeRef[R]
func Key(stepKey string) Dep                  // depend on a node from an earlier declaration
func (g *Graph) Commit() error

func (n NodeRef[R]) After(deps ...Dep) NodeRef[R]
func (n NodeRef[R]) AfterFailed(deps ...Dep) NodeRef[R]
func (n NodeRef[R]) AfterSettled(deps ...Dep) NodeRef[R]
func (n NodeRef[R]) Optional() NodeRef[R]
func (n NodeRef[R]) StartWithin(time.Duration) NodeRef[R]
func (n NodeRef[R]) Delay(time.Duration) NodeRef[R]
func (n NodeRef[R]) MaxAttempts(int) NodeRef[R]
func (n NodeRef[R]) RetryPolicy(RetryPolicy) NodeRef[R]
```

`Add` is a free function because Go methods cannot declare their own type parameters. Chaining is unaffected. `NodeRef[R]` satisfies `Dep`, so dependencies are expressed with references rather than strings wherever the predecessor is in scope.

### 3.8 Error classification

```go
func Permanent(err error) error                  // no retry; step fails
func RetryAfter(d time.Duration, err error) error
func Skip(reason string) error                   // step ends skipped, not failed
```

### 3.9 Surface

| Category | Exported |
|---|---|
| Runtime and migration | `New`, `Run`, `Stop`, `Client`, `InTx`, `Migrate` |
| Definitions | `Define`, `DefineSignal`, `DefineState`, `Register`, `RegisterAll`, `Kind`, `Version` |
| Flow operations | `Start`, `Signal`, `Cancel`, `CancelStep`, `Retry` |
| Inspection | `Get`, `Lookup`, `Trace`, `History`, `List`, `Await`, `ResultOf`, `ResultByKey`, `Read` |
| Step context | `Done`, `Wait`, `Graph`, `OnCommit`, `Info`, `Update`, `Received`, `ReceivedAll` |
| Graph | `Add`, `Key`, `Commit`, and 8 node builder methods |
| Errors | `Permanent`, `RetryAfter`, `Skip` |

Roughly 45 exported functions and methods. Supporting data types — `Flow`, `FlowTrace`, `Event`, `StepInfo`, `Filter`, `Page`, `RetryPolicy`, `SignalValue`, option types, and error values — are additional but carry no behavior a caller must learn. A service that defines steps, starts flows, and writes handlers touches roughly ten of the above.

## 4. Worked example

```go
// ---- definitions, declared once at package level ----

type ExecuteArgs struct{ IntentID string; Route Route }
type IntentStateData struct{ DepositHash string; Legs map[string]string }

var IntentState = flow.DefineState[IntentStateData]("intent_state")

var DepositConfirmed = flow.DefineSignal[Deposit]("deposit_confirmed", 1)

var (
    ExecuteIntent = flow.Define("execute_intent", 1, executeIntent, flow.WithState(IntentState))
    AwaitDeposit  = flow.Define("await_deposit", 1, awaitDeposit, flow.WithQueue("monitors"))
    AwaitCCTP     = flow.Define("await_cctp", 1, awaitCCTP, flow.WithQueue("monitors"))
    SendTxn       = flow.Define("send_txn", 1, sendTxn, flow.WithMaxAttempts(8))
    RefundIntent  = flow.Define("refund_intent", 1, refundIntent)
)

// ---- the root step builds the graph ----

func executeIntent(ctx context.Context, s *flow.Step[ExecuteArgs, flow.None]) error {
    g := s.Graph()

    deposit := flow.Add(g, "deposit", AwaitDeposit, DepositArgs{IntentID: s.Args.IntentID}).
        StartWithin(15 * time.Minute)
    origin := flow.Add(g, "origin", SendTxn, originTxn(s.Args)).After(deposit)

    var dest flow.NodeRef[SendResult]
    switch s.Args.Route.Provider {
    case "cctp":
        attest := flow.Add(g, "attest", AwaitCCTP, cctpArgs(s.Args)).After(origin)
        dest = flow.Add(g, "dest", SendTxn, destTxn(s.Args)).After(attest)
    default:
        dest = flow.Add(g, "dest", SendTxn, destTxn(s.Args)).After(origin)
    }
    if s.Args.Route.HasEdge {
        flow.Add(g, "edge", SendTxn, edgeTxn(s.Args)).After(dest)
    }

    // Failure handling: runs only if the destination leg fails, and is not
    // cancelled by fail-fast (§5.3).
    flow.Add(g, "refund", RefundIntent, refundArgs(s.Args)).AfterFailed(dest)

    return g.Commit()
}

// ---- a step that waits on an external fact ----

func awaitDeposit(ctx context.Context, s *flow.Step[DepositArgs, Deposit]) error {
    d, ok := flow.Received(s, DepositConfirmed)
    if !ok {
        return s.Wait(DepositConfirmed, 15*time.Minute)
    }
    if err := flow.Update(s, IntentState, func(st *IntentStateData) error {
        st.DepositHash = d.Hash
        return nil
    }); err != nil {
        return err
    }
    return s.Done(d)
}

// ---- a step with an external effect and an atomic application write ----

func sendTxn(ctx context.Context, s *flow.Step[SendArgs, SendResult]) error {
    hash, err := relayer.Send(ctx, s.Args.Txn)   // slow work, no transaction held
    if err != nil {
        return err                               // retried per policy
    }
    if err := flow.Update(s, IntentState, func(st *IntentStateData) error {
        st.Legs[s.Args.Leg] = hash
        return nil
    }); err != nil {
        return err
    }
    s.OnCommit(func(ctx context.Context, tx pgx.Tx) error {
        return db.MarkSent(ctx, tx, s.Args.TxnID, hash)
    })
    return s.Done(SendResult{Hash: hash})
}
```

Wiring a worker process:

```go
rt, err := flow.New(db)
if err != nil {
    return err
}
if err := flow.RegisterAll(rt,
    ExecuteIntent, AwaitDeposit, AwaitCCTP, SendTxn, RefundIntent,
); err != nil {
    return err
}
return rt.Run(ctx)
```

Starting and signalling from an RPC process that runs no workers:

```go
h, err := flow.Start(ctx, c, ExecuteIntent, intentID, ExecuteArgs{IntentID: intentID, Route: route})
err = flow.Signal(ctx, c, h.ID, DepositConfirmed, deposit, flow.WithSignalID(txHash))
```

A mistyped step name, wrong argument type, wrong signal payload type, or wrong result type will not compile. Registration, graph declaration, and signal delivery cannot drift apart.

## 5. Flows

### 5.1 Identity

A flow is identified by an opaque `FlowID` and by the natural key `(type, key)`, which is unique. The type is the kind of the flow's root step, taken from the `StepRef` passed to `Start`. `key` may be empty for flows with no natural identity, in which case no uniqueness is enforced.

Starting an existing `(type, key)` returns the existing flow rather than creating a second one, provided the request matches on type, root version, key, root arguments, deadline, fail-fast setting, and metadata. A mismatch returns `ErrConflict`. This makes `Start` safe to call from a retried RPC.

### 5.2 States

```
running → succeeded | failed | cancelled | expired
```

A flow is `running` from creation and becomes terminal when its graph resolves, when it is cancelled, or when its deadline passes.

### 5.3 Outcome and fail-fast

A flow succeeds when every step is terminal and no required step ended in a failure state. It fails when a required step fails or expires, or when a required step is cancelled individually.

Steps marked `Optional()` never determine the flow outcome.

**Fail-fast is the default and does not suppress failure handling.** When a required step fails, the following happens in one transaction, in this order:

1. the failing step becomes terminal;
2. **all of its outgoing edges resolve first**, so successors selected by `AfterFailed` or `AfterSettled` become runnable and successors that required success become `skipped`;
3. the flow is marked failing, and remaining work is cancelled **except** the failure-handling subgraph — the transitive closure of steps reachable through edges that the failure itself satisfied, together with any step already running;
4. the flow becomes terminal only once that failure-handling subgraph has resolved.

This is what makes refunds, compensations, reconciliation, and cleanup expressible. A `AfterFailed` node is guaranteed the chance to run.

Fail-fast may be disabled with `WithFailFast(false)`, in which case unrelated branches also run to completion and the outcome is computed once every step is terminal.

A flow that was already `cancelled` or `expired` never transitions to another terminal state.

### 5.4 State

A flow may carry one typed state document, declared by the root step definition via `WithState`. Flows whose root declares no state have none, and steps that never touch state need no knowledge of it.

Two access modes are offered, with different concurrency behavior:

**`Update` — merge, no conflicts.** `flow.Update(s, ref, fn)` records a mutation. The callback runs inside the completion transaction, after the flow row is locked, against the latest committed state. It therefore cannot conflict and never causes re-execution. The callback must be short, deterministic, and perform no I/O or external calls; it may be invoked in a transaction that later rolls back for an unrelated reason. **This is the recommended default.**

**`Read` + version-checked write.** `flow.Read(s, ref)` returns the current state and records the version observed. If the handler subsequently calls `Update`, that update is applied against fresh state; if the handler's decision *depends* on the value it read, it may request that the version be enforced, in which case completion requires the version to be unchanged. A conflict rolls the completion back and the handler runs again against fresh state.

A state-version conflict is an **infrastructure interruption, not an application failure**:

- it consumes no retry budget;
- it can never make a step terminal;
- re-execution is scheduled with bounded jitter, and repeated contention backs off;
- it is recorded in history and metrics as a distinct outcome, separate from handler failures.

Because a conflicting completion re-invokes the handler, external effects performed before the conflict may repeat. This is within the at-least-once contract, but it makes contention a routine source of re-execution rather than a rare one. Prefer `Update` for aggregation, keep per-branch data in the step's own **result** (private to that step, never contended), and reserve version-checked reads for decisions that genuinely depend on a prior snapshot.

State is size-limited (default 256 KiB) and stored as JSON.

A `StateRef` is matched against the flow's declared state at runtime; using a reference the flow was not started with returns `ErrInvalid`. This is the one type relationship the compiler cannot check, and it is the cost of step definitions being reusable across flow types.

### 5.5 Deadlines

A flow carries a deadline, defaulting to 30 days and overridable with `WithFlowDeadline`. It may be removed explicitly with `WithoutFlowDeadline`, which opts that flow out of the bounded-completion guarantee in §11.6.

On expiry the flow becomes `expired`, remaining non-terminal steps are cancelled, and the reason is recorded. Every step deadline is capped by the flow deadline at creation, so a step can never outlive its flow.

## 6. Steps

### 6.1 Identity

A step is identified by `(flow_id, step_key)`. Step keys are caller-supplied, stable, and unique within a flow. Re-declaring the same key with a byte-identical definition is an idempotent no-op; re-declaring it with different content returns `ErrConflict` and rolls back the declaring step's completion.

Stable keys are what make retried graph construction safe. The library never generates them.

### 6.2 States

| State | Meaning | Exit |
|---|---|---|
| `blocked` | dependencies unresolved | dependencies resolve |
| `waiting` | parked on a signal | signal arrives, or wait deadline passes |
| `ready` | eligible to run, or scheduled for a future time | claimed by a worker |
| `running` | a handler is executing | handler returns, or lease is lost |
| `retrying` | failed, scheduled to run again | retry time arrives |
| `succeeded` | completed successfully | terminal |
| `failed` | permanently failed or exhausted attempts | terminal |
| `skipped` | a branch condition was not satisfied, or the handler called `Skip` | terminal |
| `cancelled` | cancelled explicitly, or by flow cancellation | terminal |
| `expired` | a deadline passed before the step could start or finish waiting | terminal |

Ordinary transitions are monotonic toward a terminal state. Only an explicit administrative retry moves a terminal step back to `ready`.

There is no persisted `scheduled` state: a `ready` step with a future run time is scheduled, and inspection derives that classification.

### 6.3 Dispositions

| Return | Meaning | Retry budget |
|---|---|---|
| `s.Done(result)` | success; buffered outcome commits | — |
| plain `error` | retryable failure | consumed |
| `flow.RetryAfter(d, err)` | retry at an explicit delay | consumed |
| `flow.Permanent(err)` | terminal failure | consumed |
| `flow.Skip(reason)` | step ends `skipped` | not consumed |
| `s.Wait(signal, within)` | park until the signal arrives | not consumed |
| panic | recovered, treated as a retryable failure | consumed |

Infrastructure outcomes — shutdown interruption, lease loss, state-version conflict, and unregistered-version deferral — never consume retry budget and never make a step terminal.

`s.Done` may be called once, and not in combination with `s.Wait`.

### 6.4 Execution model

Handlers are **re-invoked, never replayed**. A parked or retried step runs its handler again from the top. There is no step memoization, no call-ordering requirement, and no constraint that handler code be deterministic. Handlers may be freely changed and redeployed while flows are in flight.

A handler is therefore written as a function of durable inputs: its arguments, its flow's state, the signals it has received, and the results of steps it depends on. Handlers are not pure — they perform I/O, which is their purpose — but they are **re-invocable and independently testable**: given the same durable inputs, a handler can be exercised in isolation and asserted on its disposition, its state mutation, and the graph it declares.

### 6.5 Timeouts

Two independent bounds apply:

- **`StartWithin(d)`** bounds how long a step may remain eligible before starting. The clock starts when the step becomes eligible — when its dependencies resolve, or at creation for a step with none. Time spent `blocked` does not consume it. On expiry the step becomes `expired`.
- **`WithTimeout(d)`** bounds one execution. On expiry the handler context is cancelled and the execution is recorded as a retryable failure consuming retry budget.

## 7. Graph

### 7.1 Construction

A graph is declared by a handler via `s.Graph()` and materializes atomically with that handler's success. A flow's initial graph is therefore always produced by its root step.

Declared work becomes durable only if the declaring step commits. A failed handler leaves no partial graph.

### 7.2 Dependencies

An edge carries a condition:

| Builder | Satisfied when the predecessor is |
|---|---|
| `After(d)` | `succeeded` |
| `AfterFailed(d)` | `failed` or `expired` |
| `AfterSettled(d)` | any terminal state |

When a step becomes terminal, **every one of its outgoing edges resolves**, and each edge's condition is evaluated. A successor becomes runnable exactly once, when all of its incoming edges have resolved and all their conditions are satisfied. If any condition is unsatisfied, the successor becomes `skipped` — never blocked forever — and its own outgoing edges resolve so downstream branches continue.

Edge resolution commits in the same transaction as the predecessor's terminal transition. A release can never be lost to a crash, and it always happens before fail-fast cancellation is applied (§5.3).

### 7.3 Growth rule

A new edge may originate from an existing node or from a node created in the current declaration, but its **destination must be a node created in the current declaration**. Existing nodes and edges are immutable.

This lets a completing step attach successors to work already in the graph while making cycles impossible by construction, and it keeps history meaningful — an edge never changes meaning after the fact.

### 7.4 Validation

Step kinds, versions, argument types, and result types are checked by the compiler through typed references (§3.2), so runtime validation covers only what types cannot express: unique keys within the flow, all referenced predecessors exist, no self-edges, and configured size limits on node count, edge count, and payload size.

### 7.5 Fan-out and joins

Fan-out is repeated `Add`. A join is an ordinary node with edges from each branch. Neither is a special execution mechanism.

Large fan-out is bounded by configured per-declaration and per-flow limits.

## 8. Signals

### 8.1 Model

A signal is an **immutable fact** delivered to a flow, identified by `(flow_id, signal_name, signal_id)`. It carries a typed JSON payload, a version, and an arrival time. Signals are never overwritten and never silently replaced.

| Delivery | Outcome |
|---|---|
| new `signal_id` | a new, distinct fact is recorded |
| same `signal_id`, identical payload | idempotent no-op |
| same `signal_id`, different payload | `ErrConflict`; nothing is written |
| omitted `signal_id` | a fresh identifier is generated; the fact is distinct |

Multiple facts may therefore exist under one signal name — several deposits, several attestations — and none can destroy another. Where a caller has a natural identifier, such as a transaction hash or a message ID, supplying it via `WithSignalID` makes delivery idempotent across retries.

A mutable last-write-wins cell is a genuinely different primitive and is deliberately not offered under this name (§2.3).

### 8.2 Delivery

`flow.Signal(ctx, c, id, ref, payload)` stores the fact and releases every step in that flow waiting on that signal name. Both effects commit together.

Signals may be delivered before any step waits on them. `flow.Received(s, ref)` observes facts that have already arrived, returning the earliest; `flow.ReceivedAll(s, ref)` returns all of them in arrival order. It is therefore never possible for a signal to arrive "too early", which removes the lost-release race entirely.

Delivering a signal to a terminal flow returns `ErrTerminal` and stores nothing.

### 8.3 Waiting

`s.Wait(ref, within)` parks the step in `waiting`, releases the worker slot, and records a wait deadline.

**Every wait has a deadline.** `within` is mandatory and must be positive; there is no unbounded wait. On expiry the step becomes `expired`, and its dependents resolve through the failure branch. A parked step holds no worker, no connection, and no lease.

When a matching fact arrives, the step returns to `ready` and its handler is invoked again, this time observing it.

## 9. Retries and failure

The default policy allows 5 attempts — one initial execution and 4 retries — with delays of 1s, 5s, 30s, and 2m before the final attempt, each with proportional jitter. Both the attempt count and the policy are configurable per step definition and per node.

The chosen retry time is persisted, so inspection shows exactly when a step will run again.

Attempt records are retained for every execution and store the worker identity, timings, structured error, and whether the attempt consumed retry budget. Errors are size-bounded and pass through a configurable redaction hook.

A step that exhausts its attempts becomes `failed`, with its full attempt history preserved.

## 10. Cancellation

`Cancel` cancels a whole flow; `CancelStep` cancels one step. Both are durable and cooperative.

Cancelling a flow marks it `cancelled`, marks non-terminal steps `cancelled`, removes pending work, and delivers context cancellation to running handlers. Cancelling a single required step causes the flow to fail under the rules in §5.3, including its failure-handling branches.

Cancellation and completion race on the flow. Whichever commits first wins. A handler whose step was cancelled cannot commit its result, its state mutation, its graph declarations, or its `OnCommit` callbacks — completion is rejected with a lease-lost or terminal-state error.

Cancellation cannot undo external side effects already performed, and cannot forcibly stop a non-cooperative goroutine. Fencing guarantees only that such a goroutine cannot commit **flow-managed state or its `OnCommit` writes**; writes it makes directly to external systems, or to the database outside `OnCommit`, are beyond the library's control.

Cancelling an already-cancelled flow is idempotent; cancelling a differently-terminal flow returns `ErrTerminal`.

## 11. Guarantees

### 11.1 PostgreSQL is authoritative

All durable state is recoverable by querying PostgreSQL. Notifications reduce latency but are never required for correctness; polling alone is sufficient to resume all eligible work after a crash, missed notification, or listener failure.

### 11.2 At-least-once execution

A step handler may run more than once, including after an apparently successful run. Handlers must tolerate re-execution. Effects against external systems require idempotency keys or reconciliation; the library never claims globally exactly-once behavior.

Sources of re-execution are: retry after failure, lease loss, shutdown interruption, state-version conflict, and administrative retry.

### 11.3 Fencing

Each execution holds a lease. Completion, failure recording, state mutation, graph declaration, and `OnCommit` callbacks all require the current lease. A stalled or superseded worker receives a lease-lost error and commits nothing.

Leases renew automatically while a handler runs. Handlers never implement heartbeats.

### 11.4 Atomic completion

When a step succeeds, one short transaction commits all of:

- the step's result and terminal state;
- the flow state mutation;
- newly declared steps and edges;
- edge resolution and any releases it triggers;
- the flow's own outcome transition if this was the last step;
- immutable history entries;
- every `OnCommit` callback, including application table writes.

If any part fails, none of it commits and the step is retried.

### 11.5 Serialized flow commits

Commits within one flow are serialized: two steps of the same flow never commit concurrently, because completion takes the flow's row lock. Flows are independent of one another and commit fully in parallel.

Serialization applies to **commits, not execution**. Steps of one flow may run concurrently on any number of workers — parallel fan-out is a first-class case — and they queue only at the moment they commit.

Together with the state rules in §5.4, every durable transition of a flow is applied against a consistent view of that flow, so application code needs no locking of its own and no documented lock order to write flow state safely.

### 11.6 Bounded completion

Every non-terminal step has a bounded path to a terminal state: a dependency that will resolve, a signal wait with a mandatory deadline, a retry schedule, a start deadline, or the flow deadline. A background reconciler repairs dispatch state after crashes without duplicating logical work.

This holds as long as the flow retains a deadline. Two situations lie outside it and are reported rather than silently absorbed:

- a flow started with `WithoutFlowDeadline` whose steps also declare no deadlines may remain non-terminal indefinitely;
- a step whose `(kind, version)` no live worker has registered will not run, and is surfaced as unclaimable backlog (§11.7) until a capable worker appears or a deadline expires it.

### 11.7 Rolling deployments

A worker claims only steps whose `(kind, version)` it has registered. Old and new versions of a service may share a database: a worker that does not know a step's kind or version leaves it for one that does, consuming no retry budget and never failing it.

Unclaimable backlog — a `(kind, version)` with pending work and no live worker registering it — is surfaced through inspection and observability rather than silently failing.

## 12. Inspection and tracing

Tracing is a first-class requirement.

`Trace(ctx, c, id)` returns, in one call:

- the flow: type, root version, key, state document, status, deadline, timings, outcome, and failure;
- every step: key, kind, version, state, arguments, result, last error, retry schedule, deadlines, and current running duration;
- every edge: predecessor, successor, condition, and whether it is resolved and satisfied;
- every signal received, with name, version, identifier, and arrival time;
- attempt summaries per step, including infrastructure interruptions distinguished from application failures.

`History(ctx, c, id)` returns the flow's immutable, ordered event log — every meaningful transition with timestamp, actor, and cause. Supplying an after-sequence returns only newer entries, so a UI or CLI can poll a live flow incrementally.

`List` supports bounded filtering by type, key prefix, status, time range, and metadata, with stable cursor pagination.

`Await` blocks the *caller* until a flow reaches a terminal state, by polling. It never blocks a worker and is not a second execution path.

Every record carries correlation and causation identifiers so a flow can be joined to external tracing.

## 13. Worker runtime

A single `Runtime` per process owns registration, workers, lease renewal, and background maintenance.

- Concurrency is configured per process and optionally per queue lane. Steps declare a lane; the default lane requires no configuration.
- The runtime claims only work it can start immediately; it never builds a local backlog whose leases could expire while queued.
- Wake-up uses PostgreSQL notifications when a session-capable connection is available, always with polling fallback. Poll-only operation is fully correct.
- Graceful shutdown stops claiming, lets running handlers finish within a grace period, renews their leases meanwhile, then cancels the remainder and releases their work for immediate re-execution. Interrupted executions consume no retry budget.

## 14. Distribution and replicas

`flow` is distributed by default. Running more replicas increases capacity with no configuration, no coordination protocol, and no changes to handler code.

### 14.1 Replica model

Every replica runs the same loop against the same database: wake on notification or poll, then claim eligible steps with row-level locking that skips rows another replica is already claiming.

There is no leader election, no partition assignment, no consistent hashing, no sticky routing, and no rebalancing. Two replicas claiming at the same instant simply take different work. Scaling out is starting another process; scaling in is stopping one.

### 14.2 Where a step runs

Steps are not pinned to replicas. Successive steps of one flow may execute on different replicas, and a retried step may run somewhere other than where it previously failed.

Parallel steps of one flow may execute simultaneously on different replicas. They contend only at commit (§11.5).

### 14.3 Takeover after failure

Every running step holds a lease that its worker renews while the handler executes. If a replica crashes, is killed, is partitioned, or is descheduled, its leases stop renewing and expire. Any other replica then claims the affected steps and re-invokes their handlers.

Recovery is anonymous: it does not require the failed replica to return, does not depend on its identity, and needs no operator action or external control plane. An interrupted execution consumes no retry budget.

Fencing (§11.3) makes takeover safe. A replica that was merely slow or partitioned cannot commit after its lease is lost, while the new owner proceeds.

### 14.4 Producers, workers, and mixed roles

`Client` and `Runtime` are separate. A process holding a `Client` can start flows, deliver signals, cancel, and inspect without running workers. A deployment may run one binary that both serves requests and executes steps, separate request-serving and worker replicas, or several worker pools registering different step kinds and versions.

Because claims filter on registered `(kind, version)` pairs (§11.7), specialized pools scale independently against one database.

### 14.5 Signals across replicas

Any process with a `Client` may deliver a signal, whether or not it runs workers. Delivery is durable and releases waiters wherever they are; notification wakes listening replicas, and polling covers any that missed it.

### 14.6 Limits

- **Per-flow commit rate** is bounded by that flow's row lock. This suits flows with tens to hundreds of steps, not a single flow fanning out to tens of thousands of concurrently completing steps.
- **PostgreSQL is the ceiling.** Aggregate throughput is limited by claim query rate and transactional notification cost, not replica count.
- **One database is the authority.** There is no cross-region or multi-database coordination.

## 15. Time and clocks

All durable scheduling and lease decisions use PostgreSQL time. Application clocks never determine ownership or eligibility.

Timestamps follow a strict taxonomy; each answers exactly one question, and no timeout is anchored on a column the loop reading it can write:

| Column class | Question | Written by |
|---|---|---|
| creation time | when was this row created? | insert only, immutable |
| update time | when did anything last write it? | every write, including claim; used only for crash recovery |
| status time | when did the state last change? | state transitions only |
| eligibility time | when was this permitted to run? | grants only — creation, release, retry scheduling, administrative retry |

Claiming a step is a write. It updates lease and update times, and never eligibility or deadline anchors. This is what prevents work that retries forever without ever timing out.

## 16. Configuration and defaults

| Setting | Default |
|---|---|
| step lease duration | 60 seconds, renewed automatically |
| attempts per step | 5 (one execution plus 4 retries) |
| retry delays | 1s, 5s, 30s, 2m, jittered |
| execution timeout | none unless configured |
| flow deadline | 30 days; removable per flow |
| flow state size | 256 KiB |
| step arguments / result size | 256 KiB |
| signal payload size | 64 KiB |
| nodes per declaration / per flow | 100 / 10,000 |
| edges per declaration / per flow | 1,000 / 100,000 |
| idle poll interval | 1 second |
| terminal flow retention | indefinite in v1 |
| shutdown grace period | 30 seconds |

Configuration is supplied through typed options. Environment parsing is the application's concern.

## 17. Errors

Public sentinel categories support `errors.Is`, and a typed error carries safe structured context (operation, resource, identifier, reason). Recognizable categories:

`ErrNotFound`, `ErrConflict`, `ErrInvalid`, `ErrInvalidState`, `ErrTerminal`, `ErrLeaseLost`, `ErrStateConflict`, `ErrPayloadTooLarge`, `ErrClosed`, `ErrSchema`.

Error messages carry identifiers, never payloads, arguments, secrets, or connection strings.

## 18. Observability

The runtime emits optional, no-op-by-default observations for flow start and outcome, step transitions, handler duration, retries, waits and wait expiry, signals delivered, state-version conflicts, lease renewal and loss, claim activity, unclaimable backlog, reconciliation repairs, and long-running executions.

State-version conflicts are reported as a distinct outcome, never aggregated with handler failures, so contention is visible as its own signal.

Observations carry flow type, flow ID, step key, step kind, step version, worker identity, correlation and causation IDs, and outcome category — never payload data. No logging, metrics, or tracing vendor is imposed.

## 19. Testing

The library ships a test package providing an in-process harness so handlers can be exercised without a database: given arguments, flow state, and received signals, assert the returned disposition, the state mutation, the result, and the graph declared.

Integration behavior is verified against real PostgreSQL: concurrent claims, lease expiry and fencing, cancellation races, crash recovery at every commit boundary, signal-before-wait and wait-before-signal ordering, distinct signals under one name, deterministic re-declaration under retry, failure-handling branches surviving fail-fast, state contention and conflict retries, deadline expiry, and rolling deployments with divergent registered versions.

## 20. Acceptance criteria

Milestone 1 is complete when:

- a flow can be started, traced, signalled, and cancelled through the documented API;
- the worked example in §4 compiles and runs against PostgreSQL;
- referring to a step that does not exist, or passing arguments, results, or signal payloads of the wrong type, fails to compile;
- commits within a flow are observably serialized while its parallel steps still execute concurrently;
- `Update` mutations from parallel steps all apply, in commit order, with no conflicts and no re-execution;
- a version-checked state conflict re-invokes the handler, consumes no retry budget, cannot make a step terminal, and appears in tracing as its own outcome;
- a step's result, flow state mutation, declared graph, edge resolution, and application writes commit atomically or not at all;
- a stalled worker cannot commit after losing its lease;
- a signal delivered before a wait is observed by that wait, and two signals with different identifiers both survive;
- a failure-handling branch selected by `AfterFailed` runs to completion under fail-fast, and the flow becomes terminal only after it resolves;
- every wait, step, and deadline-bearing flow reaches a terminal state without operator intervention;
- re-declaring an identical graph fragment after a retry creates no duplicate steps;
- workers registering different `(kind, version)` sets share a database without failing each other's work;
- crash at any commit boundary leaves the flow recoverable and internally consistent;
- `Trace` returns a complete, self-consistent picture of a live flow in one call;
- handler unit tests run without a database.
