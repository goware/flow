---
status: draft
---

# Functional Spec: flow

## 1. Purpose

`flow` is a PostgreSQL-backed Go library for durable, event-driven execution.

Its core loop is:

```text
command  →  worker  →  event
```

A command is a durable request for work. A worker performs it and returns a typed result. That success records an immutable **completion event**. Every successful command produces exactly one such fact, so progress is always observable and "wait for this work" and "wait for this fact" become the same operation.

Commands and events belong to an **execution**. What work an execution needs is normally declared by a **plan** — a pure function re-evaluated as facts arrive. Where a plan cannot express the logic, a hand-written **coordinator** reacts to events directly.

Commands are the executable vertices of the runtime graph, events explain progression, and causation supplies the edges. The graph is a projection of durable history, extended by the plan's record of work that is declared but not yet runnable.

## 2. Scope

### 2.1 PostgreSQL only

PostgreSQL is the sole required backend. `flow` has no broker abstraction and does not attempt to make PostgreSQL, Kafka, and SQS interchangeable.

This is a product feature: application writes, command completion, plan reconciliation, emitted events, and emitted commands can share one transaction. PostgreSQL notifications may reduce latency, but polling is always sufficient for correctness.

### 2.2 Milestone 1

- durable executions with idempotent start, deadlines, and explicit terminal outcomes;
- typed, versioned commands carrying both a payload type and a **result** type;
- automatic completion events recorded from worker results;
- worker registration, command scheduling, leases, attempts, retries, timeouts, and fencing;
- typed, versioned events, including facts published from outside the execution;
- **plans**: declarative command graphs reconciled by key, with dependencies, waits, fan-out, joins, and failure branches;
- hand-written coordinators with durable typed state and ordered event inboxes;
- historical matching-event delivery to plans and coordinators;
- delayed commands as the durable timer primitive;
- execution and command cancellation;
- atomic worker, plan, and coordinator outputs including application writes;
- inspection, causal trace, immutable history, listing, and waiting;
- embedded migrations;
- a database-free handler and plan test harness;
- vendor-neutral observability hooks.

### 2.3 Near-term follow-ons

- an operational UI for execution timelines, causal graphs, pending waits, retries, and failures;
- OpenTelemetry, metrics, and structured-logging adapters;
- administrative retry, execution fork, repair, and compensation tools.

### 2.4 Later capabilities

- child coordinators for decomposing very large executions;
- cancel-remaining join policies;
- arbitrary subtree cancellation;
- recurring schedules;
- archival and configurable terminal-history retention;
- cross-execution subscriptions and event export to Kafka or analytics systems;
- backend implementations other than PostgreSQL;
- multi-region execution.

### 2.5 Explicit non-goals

- a general-purpose message broker, cross-execution pub/sub, or event-streaming platform;
- framework-owned copies of application/domain state;
- deterministic replay of arbitrary Go code;
- exactly-once external side effects;
- distributed ACID transactions with external services;
- executable pinning to a deployed build;
- a visual workflow designer in the core package.

## 3. Core terminology

| Term | Meaning |
|---|---|
| **Execution** | One durable run, identified by `ExecutionID` and an idempotency key. |
| **Command** | One immutable logical request for work, with typed payload and typed result. Keeps one `CommandID` across attempts. |
| **Attempt** | One invocation of a command handler, identified separately from the command. |
| **Worker** | A registered typed handler for one command name and version. |
| **Event** | An immutable fact in an execution's ordered log, never destructively consumed. |
| **Completion event** | The event recorded automatically when a command succeeds, carrying its typed result. |
| **Plan** | A pure function declaring the commands an execution needs and what each one waits for. |
| **Node** | One command declared by a plan, identified by a stable key. |
| **Coordinator** | Durable typed state reacting to events. A plan is the built-in coordinator. |
| **Causation** | The direct durable record or decision responsible for creating another record. |

## 4. Public developer surface

This section defines the intended developer experience. Architecture may refine concrete field layout, but it must preserve these concepts, type checks, and operations.

### 4.1 Runtime and client

```go
type Runtime struct{}
type Client  struct{}

func New(db *pgkit.DB, opts ...Option) (*Runtime, error)
func Migrate(ctx context.Context, db *pgkit.DB, opts ...MigrateOption) error

func (r *Runtime) Register(defs ...Registration) error
func (r *Runtime) Run(ctx context.Context) error
func (r *Runtime) Stop(ctx context.Context) error
func (r *Runtime) Client() Client
func (r *Runtime) InTx(tx pgx.Tx) Client
```

`New` validates configuration and schema compatibility, starts no goroutines, and never migrates implicitly. Registrations are accepted only before `Run`, which may be called once.

A process may hold a `Client` without running workers. API processes, mixed processes, and specialized worker pools may all share one database.

### 4.2 Typed definitions

```go
type None = struct{}

type Command[A, R any]  struct{}
type Event[T any]       struct{}
type PlanDef[A any]     struct{}
type Coordinator[S any] struct{}

func DefineCommand[A, R any](name string, version int, opts ...CommandOption) Command[A, R]
func DefineEvent[T any](name string, version int) Event[T]

func (c Command[A, R]) Done() Event[R]        // this command's completion event
func (c Command[A, R]) Name() string
func (c Command[A, R]) Version() int

func Handle[A, R any](
    cmd Command[A, R],
    worker func(context.Context, *Work[A]) (R, error),
    opts ...WorkerOption,
) Registration

func DefinePlan[A any](name string, version int, plan func(*Plan, A)) PlanDef[A]

func DefineCoordinator[S any](name string, version int, handlers ...Handler[S]) Coordinator[S]
func On[S, T any](event Event[T], h func(context.Context, *Coordination[S], Received[T]) error) Handler[S]

func WithMaxAttempts(int) CommandOption
func WithRetryPolicy(RetryPolicy) CommandOption
func WithTimeout(time.Duration) CommandOption   // per-attempt wall clock
func WithQueue(string) CommandOption            // worker lane
```

A command declares both what it takes and what it returns. `Command.Done()` is the event carrying that result; it shares the command's name and version, needs no separate declaration, and is what `After` waits on.

Names are stable durable identifiers. Every definition carries an explicit positive integer version; `0` is invalid. A `(name, version)` pair has immutable payload and result meaning once used, while its handler implementation may change and redeploy freely. A runtime claims only work for pairs it has registered; unknown pairs stay pending for a compatible process and consume no retry budget.

Registration is explicit and runtime-local; definitions mutate no package-global state. A runtime rejects duplicate workers for one command pair and duplicate handlers for one event pair within a coordinator.

### 4.3 Plans

```go
type Plan struct{}
type Node struct{}

func Do[A, R any](p *Plan, key string, cmd Command[A, R], args A) *Node

func Fact[T any](p *Plan, event Event[T]) (T, bool)
func Facts[T any](p *Plan, event Event[T]) []T
func Result[A, R any](p *Plan, key string, cmd Command[A, R]) (R, bool)

func (n *Node) After(keys ...string) *Node          // all named nodes succeeded
func (n *Node) AfterSettled(keys ...string) *Node   // all named nodes terminal
func (n *Node) AfterFailed(keys ...string) *Node    // all named nodes unsuccessful
func (n *Node) AfterAny(count int, keys ...string) *Node
func (n *Node) Await(events ...EventName) *Node     // named facts have arrived
func (n *Node) Within(time.Duration) *Node          // bound on waiting to become runnable
func (n *Node) Delay(time.Duration) *Node           // earliest start once runnable
func (n *Node) Optional() *Node                     // does not determine execution outcome
func (n *Node) MaxAttempts(int) *Node
func (n *Node) RetryPolicy(RetryPolicy) *Node
```

`EventName` is satisfied by both `Event[T]` and `Command[A, R].Done()`, so `After("origin")` and `Await(DepositConfirmed)` are the same mechanism expressed two ways: waiting for a fact to exist.

`Do` is a free function because Go methods cannot declare their own type parameters. Chaining is unaffected.

### 4.4 Execution operations

```go
type ExecutionID string
type CommandID   string

type ExecutionHandle struct {
    ID      ExecutionID
    Type    string
    Key     string
    Created bool
}

func Start[A any](
    ctx context.Context, c Client,
    plan PlanDef[A], key string, args A,
    opts ...StartOption,
) (ExecutionHandle, error)

func StartWith[S any](
    ctx context.Context, c Client,
    coordinator Coordinator[S], key string, initial S,
    opts ...StartOption,
) (ExecutionHandle, error)

func Issue[A, R any](ctx context.Context, c Client, id ExecutionID, key string, cmd Command[A, R], args A) (CommandID, error)
func Publish[T any](ctx context.Context, c Client, id ExecutionID, event Event[T], key string, payload T) error

func CancelExecution(ctx context.Context, c Client, id ExecutionID, reason string) error
func CancelCommand(ctx context.Context, c Client, id CommandID, reason string) error

func WithExecutionDeadline(time.Duration) StartOption
func WithoutExecutionDeadline() StartOption
func WithMetadata(map[string]string) StartOption
func WithFailFast(bool) StartOption
```

`Start` is the common path; the execution's type is the plan's name. `StartWith` drives an execution with a hand-written coordinator instead. `Issue` and `Publish` are available to any process holding a client and may participate in a caller-owned transaction through `InTx`.

### 4.5 Handler scope and outputs

```go
type Work[A any] struct {
    Payload A
}

type Coordination[S any] struct {
    State S
}

func Emit[T any](s Scope, event Event[T], key string, payload T) error
func Send[A, R any](s Scope, key string, cmd Command[A, R], args A) error

func SucceedExecution(s CoordinatorScope, resultRef string) error
func FailExecution(s CoordinatorScope, reason error) error

func (w *Work[A]) Info() CommandInfo
func (w *Work[A]) OnCommit(func(context.Context, pgx.Tx) error)
func (c *Coordination[S]) OnCommit(func(context.Context, pgx.Tx) error)
```

A worker returns `(R, error)`. Returning successfully records the completion event carrying `R`, together with any additional events, commands, and `OnCommit` writes it staged — all in one short transaction. Returning an error discards every staged output.

Requirements:

- worker and coordinator outputs are buffered until successful return;
- output payloads are type-checked through their definitions;
- `Emit` and `Send` are available to both worker and coordinator scopes;
- execution completion functions are available only to coordinator scopes; plan-driven executions complete automatically (§6.4);
- `OnCommit` is for application-table writes. Nested `flow` operations use staged outputs or a caller-owned transaction, never a recursive call from inside a callback.

### 4.6 Inspection

```go
func GetExecution(ctx context.Context, c Client, id ExecutionID) (Execution, error)
func LookupExecution(ctx context.Context, c Client, typ, key string) (Execution, error)
func Trace(ctx context.Context, c Client, id ExecutionID, opts ...TraceOption) (ExecutionTrace, error)
func History(ctx context.Context, c Client, id ExecutionID, opts ...HistoryOption) ([]HistoryEntry, error)
func ListExecutions(ctx context.Context, c Client, f ExecutionFilter) (ExecutionPage, error)
func AwaitExecution(ctx context.Context, c Client, id ExecutionID) (Execution, error)

func ResultOf[A, R any](src ResultSource, key string, cmd Command[A, R]) (R, error)
```

Inspection never mutates execution state.

### 4.7 Error classification

```go
func Permanent(err error) error
func RetryAfter(d time.Duration, err error) error
```

### 4.8 Surface

| Category | Exported |
|---|---|
| Runtime | `New`, `Migrate`, `Register`, `Run`, `Stop`, `Client`, `InTx` |
| Definitions | `DefineCommand`, `DefineEvent`, `DefinePlan`, `DefineCoordinator`, `Handle`, `On`, `Done`, `Name`, `Version` |
| Plans | `Do`, `Fact`, `Facts`, `Result`, plus 10 node builder methods |
| Execution | `Start`, `StartWith`, `Issue`, `Publish`, `CancelExecution`, `CancelCommand` |
| Handler output | `Emit`, `Send`, `OnCommit`, `Info`, `SucceedExecution`, `FailExecution` |
| Inspection | `GetExecution`, `LookupExecution`, `Trace`, `History`, `ListExecutions`, `AwaitExecution`, `ResultOf` |
| Errors | `Permanent`, `RetryAfter` |

The common path is `DefineCommand`, `DefineEvent`, `DefinePlan`, `Handle`, `Do`, `Start`, `Publish`, and `Run`. Coordinators, cancellation, transaction composition, and policy customization form the operational surface and need not be learned to write a working execution.

## 5. Worked example

```go
// ---- definitions ----

var (
    AwaitDeposit = flow.DefineCommand[DepositArgs, Deposit]("await_deposit", 1, flow.WithQueue("monitors"))
    AwaitCCTP    = flow.DefineCommand[CCTPArgs, Attestation]("await_cctp", 1, flow.WithQueue("monitors"))
    SendTxn      = flow.DefineCommand[SendArgs, SendResult]("send_txn", 1, flow.WithMaxAttempts(8))
    RefundIntent = flow.DefineCommand[RefundArgs, flow.None]("refund_intent", 1)

    DepositConfirmed = flow.DefineEvent[Deposit]("deposit_confirmed", 1)
    RouteSelected    = flow.DefineEvent[RouteData]("route_selected", 1)
)

// ---- the plan: what this execution needs ----

var ExecuteIntent = flow.DefinePlan[ExecuteArgs]("execute_intent", 1, planIntent)

func planIntent(p *flow.Plan, args ExecuteArgs) {
    flow.Do(p, "deposit", AwaitDeposit, DepositArgs{IntentID: args.IntentID}).
        Await(DepositConfirmed).
        Within(15 * time.Minute)

    flow.Do(p, "origin", SendTxn, originTxn(args)).After("deposit")

    if route, ok := flow.Fact(p, RouteSelected); ok {
        switch route.Provider {
        case "cctp":
            flow.Do(p, "attest", AwaitCCTP, cctpArgs(args)).After("origin")
            flow.Do(p, "dest", SendTxn, destTxn(args, route)).After("attest")
        default:
            flow.Do(p, "dest", SendTxn, destTxn(args, route)).After("origin")
        }
    }

    if args.HasEdge {
        flow.Do(p, "edge", SendTxn, edgeTxn(args)).After("dest")
    }

    // Failure handling: runs only if the destination leg is unsuccessful.
    flow.Do(p, "refund", RefundIntent, refundArgs(args)).AfterFailed("dest")
}

// ---- workers: one command in, one typed result out ----

func awaitDeposit(ctx context.Context, w *flow.Work[DepositArgs]) (Deposit, error) {
    return confirmDeposit(ctx, w.Payload.IntentID)
}

func sendTxn(ctx context.Context, w *flow.Work[SendArgs]) (SendResult, error) {
    hash, err := relayer.Send(ctx, w.Payload.Txn)   // slow work, no transaction held
    if err != nil {
        return SendResult{}, err                    // retried per policy
    }
    w.OnCommit(func(ctx context.Context, tx pgx.Tx) error {
        return db.MarkSent(ctx, tx, w.Payload.TxnID, hash)
    })
    return SendResult{Hash: hash}, nil
}
```

Wiring a worker process:

```go
rt, err := flow.New(db)
if err != nil {
    return err
}
if err := rt.Register(
    flow.Handle(AwaitDeposit, awaitDeposit),
    flow.Handle(AwaitCCTP, awaitCCTP),
    flow.Handle(SendTxn, sendTxn),
    flow.Handle(RefundIntent, refundIntent),
    ExecuteIntent,
); err != nil {
    return err
}
return rt.Run(ctx)
```

Starting and publishing facts from an RPC process that runs no workers:

```go
h, err := flow.Start(ctx, c, ExecuteIntent, intentID, ExecuteArgs{IntentID: intentID, HasEdge: hasEdge})

err = flow.Publish(ctx, c, h.ID, DepositConfirmed, txHash, deposit)
err = flow.Publish(ctx, c, h.ID, RouteSelected, "route", RouteData{Provider: "cctp"})
```

The `dest` branch does not exist until `RouteSelected` arrives. No worker knows about any other command. The whole shape is one readable function, and a mistyped command reference or a wrong payload, result, or event type will not compile.

## 6. Executions

### 6.1 Identity and idempotent start

An execution has a generated `ExecutionID`, an execution type, and a caller-supplied key. The type is the plan name, or the coordinator name for `StartWith`. `(execution_type, execution_key)` is unique for as long as the execution is retained; an empty key enforces no uniqueness.

Repeating `Start` with the same type, key, definition version, canonical arguments, and materially relevant options returns the existing execution with `Created == false`. Reusing the key with materially different content returns `ErrConflict` and changes nothing.

The idempotency check happens before terminal-state rejection, so a caller retrying after a lost response receives the existing terminal execution rather than an error.

### 6.2 Lifecycle

```text
running  → succeeded
running  → failing → failed
running  → cancelled
running  → expired
failing  → cancelled
failing  → expired
```

An execution is `running` from creation, becomes `failing` while selected failure-handling work is still active, and is terminal thereafter. Terminal states never reopen in Milestone 1; administrative retry and fork are later capabilities that create new records rather than mutating history.

### 6.3 Outcome and fail-fast

An execution succeeds when every declared node is terminal, nothing remains pending or awaited, and no required node ended unsuccessfully. Nodes marked `Optional()` never determine the outcome.

**Fail-fast is the default and does not suppress failure handling.** When a required node becomes unsuccessful, one transaction, in this order:

1. records that node's terminal state;
2. **resolves every dependency naming it**, so nodes selected by `AfterFailed` or `AfterSettled` become runnable and nodes that required its success become `skipped`;
3. marks the execution `failing` and cancels remaining non-terminal work **except** the failure-handling subgraph — the nodes just made runnable, their descendants, and anything already running;
4. leaves the execution terminal only once that failure-handling subgraph resolves.

This is what makes refunds, compensation, reconciliation, and cleanup expressible; an `AfterFailed` node is guaranteed its chance to run. Execution-level cancellation is different: it cancels everything and selects no failure branches.

`WithFailFast(false)` lets unrelated branches finish, and the outcome is computed once every node is terminal.

### 6.4 Completion

For a plan-driven execution, completion is derived: every declared node is terminal, no node is pending or awaiting an unarrived fact, the latest evaluation consulted no input that did not exist (§10.1), and re-evaluating the plan declares nothing new. Because the plan records what is still expected, quiescence is unambiguous — an execution waiting on an external fact has a declared node that is not yet runnable, so it is not complete.

For a coordinator-driven execution, completion is an explicit `SucceedExecution` or `FailExecution` decision, and temporary quiescence never completes it.

Success may commit only when no command is non-terminal and nothing staged by the same decision would make that false. On completion the coordinator is closed and pending work cancelled atomically, and one immutable `ExecutionSucceeded` or `ExecutionFailed` event is recorded.

### 6.5 Deadlines

An execution carries a deadline, defaulting to 30 days, overridable with `WithExecutionDeadline` and removable with `WithoutExecutionDeadline` — which opts out of the bounded-completion guarantee (§12.6). On expiry the execution becomes `expired`, non-terminal commands are cancelled, and the reason is recorded. Every command and wait deadline is capped by the execution deadline, so nothing outlives its execution.

### 6.6 External additions

`Issue` and `Publish` may add a command or a fact to a running execution. They are rejected for a terminal execution unless the operation is an idempotent retry of an already-stored key with equivalent content.

A command added by `Issue` is required for outcome purposes in Milestone 1. Detached work belongs in a separate execution.

## 7. Commands

### 7.1 Identity

Every command has a `CommandID` stable across attempts, an `ExecutionID`, a name and version, a `CommandKey` unique within its execution, a canonical typed payload, and causation identifying what created it.

A plan node's key is its command key. Re-declaring the same key with an equivalent definition and canonical payload is a no-op; different content returns `ErrConflict`.

### 7.2 Lifecycle

```text
pending → ready → running → succeeded
                  running → retry_wait → running
                  running → failed
pending | ready | running → cancelled
pending | ready → expired
pending → skipped
```

`pending` means declared but not yet runnable — dependencies unresolved or awaited facts not yet arrived. `ready` means runnable and claimable, possibly at a future time. There is no persisted "scheduled" state; a `ready` command with a future eligibility time is scheduled, and inspection derives that classification.

`skipped` means a dependency condition became permanently unsatisfiable, so the command will never run. It is terminal and unsuccessful, but it is not a failure and does not by itself fail the execution.

### 7.3 Completion event

A successful worker return records exactly one completion event for that command, carrying its typed result and sharing the command's name and version. This is automatic and cannot be suppressed. It is the fact that `After` and `Command.Done()` refer to, and it is what makes "wait for work" and "wait for a fact" one mechanism.

A worker may additionally `Emit` domain events. Those are recorded at earlier positions than the completion event, so any reader observing the completion has already observed them.

Terminal unsuccessful outcomes record `CommandFailed`, `CommandCancelled`, or `CommandExpired` instead.

### 7.4 Waiting and scheduling

A command becomes runnable when all declared dependencies are satisfied and all awaited facts have arrived. `Within` bounds how long it may remain pending, measured from when it was declared; on expiry it becomes `expired` and dependents resolve through the failure branch. `Await` without `Within` inherits the execution deadline, so no wait is ever unbounded.

`Delay` sets the earliest time a runnable command may be claimed. **A delayed command is the durable timer primitive**; there is no separate timer concept. A waiting or delayed command holds no worker, connection, goroutine, or lease.

### 7.5 Claiming and rolling deployments

A worker claims only `(name, version)` pairs it has registered, using row-level locking that skips rows another process is claiming. A process that does not recognize a command leaves it pending, consumes no retry budget, and never fails it, so old and new versions of a service may share one database.

Unclaimable backlog — a `(name, version)` with pending work and no live worker registering it — is surfaced through inspection and observability rather than stalling silently.

## 8. Attempts, retries, and failure

### 8.1 Separate attempt identity

Each claimed execution of a command creates an attempt record with its own identity, worker and process identity, timings, structured error, and whether it consumed retry budget. The logical command keeps one `CommandID` throughout.

### 8.2 Default behavior

The default policy allows 5 attempts — one execution and 4 retries — with delays of 1s, 5s, 30s, and 2m, each with proportional jitter. Attempt count and policy are configurable per command definition and per plan node. The chosen retry time is persisted, so inspection shows exactly when a command runs again.

| Worker return | Effect | Retry budget |
|---|---|---|
| `(result, nil)` | completion event; command succeeds | — |
| plain `error` | retry per policy | consumed |
| `flow.RetryAfter(d, err)` | retry at an explicit delay | consumed |
| `flow.Permanent(err)` | command fails immediately | consumed |
| panic | recovered, treated as retryable | consumed |

### 8.3 Operational interruption

Shutdown interruption, lease loss, and unregistered-version deferral never consume retry budget and never make a command terminal. They are retained as operational history and observations, not as domain progression.

### 8.4 Terminal failure

A command that exhausts its attempts, or returns a permanent error, becomes `failed` and records `CommandFailed`, with its full attempt history preserved.

Attempt failures are not domain outcomes. A negative *domain* result is a **successful** command whose typed result says so — a distinction that keeps retry mechanics out of application semantics.

## 9. Events

### 9.1 Immutable facts

Every event has an `EventID`, `ExecutionID`, name and version, optional key, canonical typed payload, an immutable per-execution position, occurrence time, causation, and where applicable the originating command and attempt.

Events are append-only and never destructively consumed. Unlike a command, which one worker handles, an event is observed independently by the plan and by any coordinator subscribing to it.

### 9.2 Kinds

- **Completion events** — recorded automatically when a command succeeds, carrying its result.
- **Domain events** — emitted by workers, or published from outside by application code, webhooks, and monitors.
- **Runtime events** — `CommandFailed`, `CommandCancelled`, `CommandExpired`, `CoordinatorFailed`, `ExecutionSucceeded`, `ExecutionFailed`, `ExecutionCancelled`, `ExecutionExpired`.

Attempt failures, lease renewals, and lease loss remain operational history rather than events, so transient mechanics never masquerade as permanent facts.

### 9.3 Idempotency

`Publish` requires a non-empty event key. Identity is `(ExecutionID, event_name, EventKey)`, scoped across versions so a publisher retrying the same natural fact after a deployment cannot create duplicate progression under a newer schema.

| Repeated key | Result |
|---|---|
| equivalent canonical payload and material metadata | returns the existing event |
| different payload, version, or material metadata | `ErrConflict`; nothing written |

Idempotency is checked before terminal-execution rejection: retrying an existing equivalent event succeeds even after the execution becomes terminal, while a genuinely new event is rejected with `ErrTerminal`.

### 9.4 Ordering

Events receive a durable total position within their execution, reflecting commit order rather than the time an external fact occurred.

Plans and coordinators observe matching events in increasing position order. A failed delivery blocks later events for that reader until it succeeds or the reader becomes terminal.

Checkpointing must never permanently skip an event whose creating transaction becomes visible later; architecture must either make per-execution positions gap-free at commit or use a cursor that revisits unresolved gaps. Because positions are scoped to one execution — which is already the serialization point for its own commits — this is materially simpler than database-wide ordering.

### 9.5 Payloads and result references

Small durable results may be carried in payloads. Large or sensitive outputs belong in application-owned tables or object storage, referenced by a stable identifier. Changing data behind a reference does not change the historical event; use immutable, versioned, or content-addressed references where historical reproducibility matters.

## 10. Plans

### 10.1 The plan function

A plan is a pure function of the execution's root arguments and the facts observed so far. It declares nodes; it never performs work. It must not do I/O, read clocks, or use randomness, and must be deterministic given identical inputs.

`Fact` and `Facts` read events already recorded in this execution. `Result` reads a completed node's typed result. A branch that depends on runtime information is an ordinary Go conditional over those reads, which is why dynamic composition needs no special API.

**Reads are recorded.** Every `Fact`, `Facts`, and `Result` call during an evaluation is registered as an input that evaluation consulted. Because the plan is pure, its declared output is a function of exactly those reads plus the root arguments — which makes the consulted set both a correctness signal (§10.2) and the basis for skipping needless evaluations (§10.3).

**`Result` may only read a declared key.** Reading a key the plan has not declared in this evaluation returns `ErrInvalid`; it is a plan defect, not a runtime condition. Reading a declared node that has not yet succeeded returns `(zero, false)`.

Reading a node's result does not by itself create a dependency edge. A node whose arguments derive from another node's result should also name it with `After`, so the trace can explain the ordering. Correctness does not depend on this — a node consulted before it completed keeps the execution incomplete — but inspection quality does.

#### Consulted-but-absent reads block completion

A read that finds nothing — a fact not yet published, or a declared node not yet succeeded — means the plan's output would differ once that input exists. An execution therefore **cannot complete while its most recent evaluation consulted an input that did not exist.**

This closes an otherwise silent hole. Given:

```go
if route, ok := flow.Fact(p, RouteSelected); ok {
    flow.Do(p, "dest", SendTxn, destTxn(args, route)).After("origin")
}
```

if `RouteSelected` never arrives, no `dest` node is ever declared. Without this rule, an execution whose declared nodes had all succeeded would report success while the destination transaction was never sent. With it, the consulted-but-absent `RouteSelected` keeps the execution running until the fact arrives or the execution deadline expires.

The rule is automatic and cannot be forgotten, because it derives from the read itself rather than from a separate declaration. Absent reads are bounded by the execution deadline; a plan wanting a tighter bound declares a node with `Await(...).Within(...)` instead of branching on a bare `Fact`.

### 10.2 Evaluation and reconciliation

The plan is evaluated at execution start and again whenever an event is appended to the execution. Evaluation runs in the same transaction as that event's delivery and reconciles the declared set against what already exists:

| Declared key | Action |
|---|---|
| does not exist, dependencies satisfied | its command is created and becomes `ready` |
| does not exist, dependencies unsatisfied | recorded as a `pending` node with its dependencies |
| already exists | verified against the stored definition and canonical payload; a mismatch returns `ErrConflict` |
| previously declared, no longer declared | retained unchanged |

**A plan only grows.** It cannot withdraw, rewrite, or re-point work it already declared. Because facts are append-only, a correct plan's conditions are monotonic and this never arises in practice; a mismatch is treated as a plan defect.

Every command made runnable by one evaluation is created in that single transaction, so a crash exposes either all of them or none.

The consulted-input set from the latest evaluation is persisted alongside the declared set. It determines both whether the execution may complete (§10.1) and when the plan must next be evaluated (§10.3).

### 10.3 When the plan is evaluated

A plan's declared output is a pure function of its root arguments, the inputs it consulted, and the states of the nodes it declared. Evaluation is therefore required only when one of those can have changed:

- at execution start;
- when an event arrives whose name the latest evaluation consulted, or which the plan has never had the chance to consult;
- when a declared node changes state.

Events of names no evaluation has ever consulted cannot change the plan's output and do not trigger evaluation. Because plans are pure, skipping is sound rather than an optimization that risks divergence.

Evaluation is idempotent, so an implementation may always evaluate more often than required — including on every event — without changing behavior.

### 10.4 Dependencies

| Builder | Runnable when |
|---|---|
| `After(k…)` | every named node has succeeded |
| `AfterSettled(k…)` | every named node is terminal |
| `AfterFailed(k…)` | every named node is unsuccessful — failed, expired, cancelled, or skipped |
| `AfterAny(n, k…)` | at least `n` of the named nodes have succeeded |
| `Await(e…)` | every named event has arrived in this execution |

Conditions combine: a node may name predecessors and awaited facts together and becomes runnable only when all of them hold. When a named node reaches a terminal state that makes a condition permanently unsatisfiable, the dependent becomes `skipped` rather than waiting forever, and its own dependents resolve in turn.

Fan-out is repeated `Do`. A join is one node naming several predecessors. Neither is a special execution mechanism, and neither needs a separate fan-out or join API.

### 10.5 Cost model and limits

Each evaluation costs:

| Work | Cost |
|---|---|
| running the plan function in Go | O(declared nodes) — in-memory, no I/O |
| loading declared node states and the consulted input set | O(declared nodes), narrow indexed columns |
| database writes | O(newly runnable nodes) only |

Writes are proportional to the delta, but the read and the Go evaluation are proportional to the whole declared set, and an execution produces at least one event per command. Total plan work over an execution's life is therefore approximately **O(nodes²)**, which is what bounds plan size.

The limit is **1,000 nodes per execution**, with 100 dependencies per node. At that ceiling an execution performs on the order of a million narrow row reads across its lifetime, which is acceptable; an order of magnitude higher is not. This is well above the intended workload — executions of tens of commands — and larger fan-outs belong in separate executions, one per unit of work, with a parent execution coordinating them.

Coordinator-driven executions are not re-evaluated and are not bound by this limit; they are bounded only by inbox delivery cost.

Architecture must make the per-evaluation state load a single narrow indexed query and must benchmark evaluation at the documented ceiling.

## 11. Coordinators

A coordinator is durable typed state that reacts to events. It exists because deciding "wait for three things" requires somewhere to remember that two have been seen.

**One coordinator drives an execution**: either the plan, or an application-defined coordinator started with `StartWith`. Child coordinators are a later capability.

### 11.1 Definition and instance

A definition has a stable name, positive version, typed state schema, and exact typed event subscriptions declared with `On`. Its instance holds typed canonical state, a durable inbox position, and a lifecycle of `active → completed | failed | cancelled`.

### 11.2 Historical matching-event delivery

An instance begins with its inbox at the start of the execution and receives **every matching retained event in position order**, including facts recorded before the instance existed. An external fact therefore cannot be lost by arriving early. The same rule governs plan evaluation, which sees every fact recorded so far.

### 11.3 Serialized processing

At most one handler runs per coordinator instance at a time; workers and other executions run concurrently. On a `nil` return, one transaction records the event as processed, persists new state, commits staged commands, events, and `OnCommit` writes, and advances the inbox. On error or lease loss none of it commits, and redelivery cannot apply a decision twice.

### 11.4 Failure

Coordinator handler errors retry under the default policy. A permanent or exhausted error marks the coordinator failed, records `CoordinatorFailed`, fails the execution, and cancels outstanding work.

### 11.5 State boundary

Coordinator state may store orchestration facts such as selected route, expected keys, observed outcome flags, or local progress. It must never become a second source of truth for application entities; balances, intent status, transaction records, and report contents remain in application-owned tables, with `OnCommit` keeping coordination and domain writes atomic when they must change together.

Coordinator handlers are for short decisions. External work belongs in commands.

### 11.6 Rolling deployments

A coordinator delivery is claimed only by a runtime registering the instance's exact coordinator name and version and the matching event name and version. A process understanding neither side leaves the delivery pending without consuming retry budget, and unclaimable coordinator backlog is visible through inspection and observability.

## 12. Transactional guarantees

### 12.1 PostgreSQL is authoritative

All execution state is recoverable from PostgreSQL. Correctness never depends on notifications, local queues, process identity, or in-memory state. Polling alone suffices to resume all eligible work after a crash, missed notification, or listener failure.

### 12.2 At-least-once execution

Worker and coordinator handlers may run more than once, including after apparently successful user code. `flow` guarantees idempotent durable progression, not exactly-once execution of arbitrary code. External side effects require stable idempotency keys or reconciliation; `CommandID` is available as a natural one.

Sources of re-execution are retry after failure, lease loss, and shutdown interruption.

### 12.3 Fencing

Attempts and coordinator deliveries hold renewable leases. Every completion verifies current ownership and non-terminal execution state. A stalled, partitioned, cancelled, or superseded handler cannot commit its staged outputs or `OnCommit` writes. Leases renew automatically; handlers never implement heartbeats.

Fencing guarantees only that such a handler cannot commit **flow-managed records or its `OnCommit` writes**. Effects it already performed against external systems are beyond the library's control.

### 12.4 Short atomic completion

User handlers never hold a PostgreSQL transaction for the duration of their work. They perform work and stage outputs; the runtime opens a short transaction after a successful return.

That one transaction commits the command's completion event, its emitted events and commands, plan reconciliation and every command that reconciliation creates, dependency resolution, execution outcome transitions, history, and `OnCommit` writes. If any part fails, none commits and the command is retried.

If an application deliberately writes outside `OnCommit`, those writes are outside `flow` fencing and atomicity.

### 12.5 Serialized execution commits

Commits within one execution are serialized by its row lock; different executions commit fully in parallel.

Serialization applies to **commits, not work**. Commands of one execution run concurrently across any number of workers and queue only at the moment they commit, so every durable transition is applied against a consistent view of the execution.

### 12.6 Bounded completion

Every non-terminal command has a bounded path to terminal: dependencies that will resolve, an awaited fact with a deadline, a retry schedule, a `Within` bound, or the execution deadline. A background reconciler repairs dispatch state after crashes without duplicating work.

Two situations lie outside this and are reported rather than absorbed: an execution started with `WithoutExecutionDeadline` whose commands declare no bounds, and a command whose `(name, version)` no live worker registers.

### 12.7 Caller-owned transactions

`Runtime.InTx(tx)` allows `Start`, `Issue`, `Publish`, and cancellation to commit atomically with caller-owned application writes. The library defines and enforces one transaction ordering discipline across execution, command, event, plan, coordinator, and application operations; its exact lock order belongs in the architecture.

### 12.8 Causation

Outputs created inside handlers automatically inherit execution identity and causation: worker events and commands are caused by the current command; plan and coordinator outputs are caused by the event being processed; outcome events are caused by the terminal transition. Callers cannot forge an origin that contradicts the active handler scope.

## 13. Cancellation, deadlines, and terminal races

`CancelExecution` marks the execution `cancelled`, cancels non-terminal commands, closes the coordinator, and records `ExecutionCancelled`. `CancelCommand` cancels one command; if it is required, the execution fails under §6.3 including its failure-handling branches.

Cancellation and completion race on the execution row, and whichever commits first wins. A handler whose command was cancelled cannot commit its result, staged outputs, or `OnCommit` writes.

Cancellation cannot undo external side effects already performed and cannot forcibly stop a non-cooperative goroutine; fencing only guarantees such a goroutine commits nothing.

Cancelling an already-cancelled target is idempotent; cancelling a differently-terminal target returns `ErrTerminal`.

## 14. Serialization, encoding, and limits

Command payloads, command results, event payloads, coordinator state, and all identity comparisons use deterministic canonical JSON. Idempotency compares canonical stored bytes, not caller memory layout or database formatting. The architecture defines the encoder and its treatment of custom marshalers; the functional requirement is that the same logical value always produces the same identity bytes.

Payload, state, node-count, and dependency-count limits are enforced before any durable write. Violations return `ErrPayloadTooLarge` or `ErrInvalid` with no partial effect.

## 15. Inspection and graph projection

### 15.1 Execution trace

`Trace` returns, in one call:

- the execution: type, key, status, deadline, timings, outcome, and failure;
- every command: key, name, version, state, payload, result, last error, retry schedule, deadlines, and current running duration;
- every declared node not yet runnable, with the dependencies and awaited facts it is still waiting for;
- every event: name, version, key, position, payload, arrival time, and causation;
- attempt summaries per command, distinguishing operational interruptions from application failures;
- the causal edges linking all of the above.

Because the plan records what is still expected, a trace answers both *what happened* and *what this execution is waiting for* — the latter being something pure causation cannot express.

### 15.2 History

`History` returns the immutable ordered log. Supplying an after-position returns only newer entries, so a UI or CLI can poll a live execution incrementally.

### 15.3 List and await

`ListExecutions` supports bounded filtering by type, key prefix, status, time range, and metadata, with stable cursor pagination. `AwaitExecution` polls until terminal, never blocks a worker, and is not a second execution path.

Every record carries correlation and causation identifiers for joining to external tracing.

### 15.4 Retention

Terminal executions and their history are retained indefinitely in Milestone 1. Archival and configurable retention are later capabilities.

## 16. Runtime and distribution

`flow` is distributed by default. Given eligible work and database headroom, more replicas mean more capacity, with no configuration, coordination protocol, or handler changes.

### 16.1 Replica model

Every replica runs the same loop against the same database: wake on notification or poll, then claim eligible work with row-level locking that skips rows another replica is claiming. There is no leader election, partition assignment, consistent hashing, sticky routing, or rebalancing. Scaling out is starting a process; scaling in is stopping one.

### 16.2 Placement and takeover

Commands are not pinned to replicas; successive commands of one execution may run anywhere, and a retried command may run somewhere new.

Every running attempt holds a lease its worker renews. A replica that crashes, is killed, is partitioned, or is descheduled stops renewing; its leases expire and any other replica claims the work. Recovery is anonymous — no operator action, no control plane, and no return of the failed process — and fencing (§12.3) makes it safe against a merely slow replica.

### 16.3 Roles

`Client` and `Runtime` are separate. A process may start executions, issue commands, publish events, cancel, and inspect without running workers. Deployments may mix one binary doing both, split request-serving from worker replicas, or run several pools registering different command names and versions, which scale independently against one database.

### 16.4 Concurrency and wake-up

Concurrency is configured per process and optionally per queue lane. The runtime claims only work it can begin immediately and never builds a local backlog whose leases could expire while queued.

Wake-up uses PostgreSQL notifications when a session-capable connection is available, always with polling fallback. Poll-only operation is fully correct and is the supported mode behind transaction-pooling proxies.

### 16.5 Graceful shutdown

Shutdown stops claiming, lets running handlers finish within a grace period while renewing their leases, then cancels the remainder and releases their work for immediate re-execution. Interrupted attempts consume no retry budget.

### 16.6 Limits

Per-execution commit rate is bounded by its row lock, suiting executions of tens to hundreds of commands. Aggregate throughput is bounded by PostgreSQL — claim query rate and transactional notification cost — not by replica count. One database is the authority; there is no cross-region coordination.

## 17. Time and clocks

All durable scheduling and lease decisions use PostgreSQL time. Application clocks never determine ownership or eligibility, and are used only for local timers, jitter, deadlines, and duration measurement.

Timestamps follow a strict taxonomy; each answers exactly one question, and no timeout is anchored on a column the loop reading it can write:

| Column class | Question | Written by |
|---|---|---|
| creation time | when was this row created? | insert only, immutable |
| update time | when did anything last write it? | every write including claim; crash recovery only |
| status time | when did the state last change? | state transitions only |
| eligibility time | when was this permitted to run? | grants only — declaration, release, retry scheduling |

Claiming a command is a write: it updates lease and update times and never eligibility or deadline anchors. This is what prevents work that retries forever without ever timing out.

## 18. Configuration and defaults

| Setting | Default |
|---|---|
| attempt lease duration | 60 seconds, renewed automatically |
| attempts per command | 5 (one execution plus 4 retries) |
| retry delays | 1s, 5s, 30s, 2m, jittered |
| per-attempt timeout | none unless configured |
| execution deadline | 30 days; removable per execution |
| command payload / result size | 256 KiB |
| event payload size | 64 KiB |
| coordinator state size | 256 KiB |
| nodes per plan-driven execution | 1,000 |
| dependencies per node | 100 |
| idle poll interval | 1 second |
| terminal execution retention | indefinite in v1 |
| shutdown grace period | 30 seconds |

Configuration uses typed options; environment parsing is the application's concern. Invalid combinations fail at configuration or request validation time.

## 19. Errors and safety

Public sentinel categories support `errors.Is`, and a typed error carries safe structured context (operation, resource, identifier, reason):

`ErrNotFound`, `ErrConflict`, `ErrInvalid`, `ErrInvalidState`, `ErrTerminal`, `ErrLeaseLost`, `ErrPayloadTooLarge`, `ErrClosed`, `ErrSchema`.

Messages carry identifiers, never payloads, arguments, secrets, or connection strings. Stored errors are size-bounded and pass through a configurable redaction hook.

## 20. Observability and UI readiness

The runtime emits optional, no-op-by-default observations for execution start and outcome, command transitions, handler duration, retries, waits and wait expiry, events published, plan evaluation size and duration, lease renewal and loss, claim activity, unclaimable backlog, reconciliation repairs, and long-running attempts.

Observations carry execution type and ID, command key, name and version, worker identity, correlation and causation IDs, and outcome category — never payload data. No logging, metrics, or tracing vendor is imposed; adapters are near-term follow-ons (§2.3).

The durable model is deliberately sufficient for an operational UI without a UI existing in the core runtime.

## 21. Testing support

The library ships a test package so workers and plans are testable without a database:

- a worker is an ordinary function — given a payload, assert its returned result or error, its staged events and commands, and its registered application writes;
- a plan is a pure function — given root arguments, a set of facts, and node states, assert the declared node set, its dependencies, and its waits.

Integration behavior is verified against real PostgreSQL: concurrent claims, lease expiry and fencing, cancellation races, crash recovery at every commit boundary, publish-before-declare and declare-before-publish ordering, repeated plan evaluation creating no duplicate commands, failure branches surviving fail-fast, `Await` expiry, and rolling deployments with divergent registered versions.

## 22. Acceptance criteria

Milestone 1 is complete when:

- an execution can be started, traced, published to, and cancelled through the documented API;
- the worked example in §5 compiles and runs against PostgreSQL;
- a mistyped command or event reference, or a wrong payload, result, or event payload type, fails to compile;
- every successful command records exactly one completion event carrying its typed result;
- re-evaluating a plan many times creates each declared command exactly once;
- a plan branch appears only once the fact deciding it exists, and never withdraws work already declared;
- every command made runnable by one plan evaluation is created in a single transaction;
- a node with `Await` becomes runnable when its fact arrives, whether that fact was published before or after the node was declared;
- an awaited fact that never arrives expires its node within the declared bound, and dependents resolve through the failure branch;
- a failure branch declared with `AfterFailed` runs to completion under fail-fast, and the execution becomes terminal only after it resolves;
- a plan-driven execution completes exactly when nothing is declared, pending, awaited, or consulted-but-absent, and never on temporary quiescence;
- a plan that branches on a fact which never arrives keeps its execution running until its deadline rather than reporting success;
- `Result` on an undeclared key is rejected as a plan defect;
- plan evaluation at the documented node ceiling stays within its benchmarked budget, and events of never-consulted names trigger no evaluation;
- a command result, emitted events, newly created commands, dependency resolution, and application writes commit atomically or not at all;
- a stalled worker cannot commit after losing its lease;
- an event published before its plan node or coordinator existed is still observed by it;
- an idempotent republish of a stored event succeeds after the execution becomes terminal, while a genuinely new event is rejected;
- crash at any commit boundary leaves the execution recoverable and internally consistent;
- workers registering different `(name, version)` sets share a database without failing each other's work;
- `Trace` returns both what happened and what the execution is waiting for, in one call;
- worker and plan unit tests run without a database.
