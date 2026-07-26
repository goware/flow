---
status: draft
---

# Functional Spec: flow

## 1. Purpose

`flow` is a PostgreSQL-backed Go library for event-driven, durable, distributed work execution.

Its core loop is:

```text
command  →  worker  →  event
                     └→ optional child commands
```

A command is a durable instruction to perform work. A worker handles the command and performs that work. Events record facts about what happened. Plans, when used, react to recorded events and coordinate the overall execution. Workers may spawn child commands when their work reveals more work.

Execution is distributed by default. Calling `.Execute` durably enqueues work in PostgreSQL and does not assign it to the caller. Any compatible replica running `Runtime.Run` may claim the command; events and child commands committed by that worker may wake and be handled by other replicas. No execution requires one process to remain alive or retain in-memory state.

There is one event concept. Conceptually, workers emit events. In the API, returning `(result, nil)` automatically records an immutable event carrying the command's typed result; workers call `flow.Emit` only for additional application facts. Failure, cancellation, expiry, and skipping are also recorded as ordinary events. Every command that ends therefore produces exactly one final fact, so progress is observable and "wait for this work" and "wait for this fact" use the same durable mechanism. A retryable error records attempt history but no final event because the command has not finished.

Commands and events belong to an **execution**. A plan is optional. The simplest execution starts one root command directly; its workers may form a bounded command tree with `Spawn`, and the execution finishes when that tree settles. When progression requires dependencies, joins, waits, or branches across commands, a **plan** declares that orchestration as a pure function re-evaluated over all relevant events and command results recorded so far. "React" does not mean that the plan receives one event callback. Where membership is open-ended or a plan cannot express the logic, a hand-written **coordinator** reacts to events directly.

Application code begins any mode by binding its command, plan, or coordinator definition with `With(runtime)` and calling `.Execute` on the returned immutable copy. The call durably schedules the execution and returns an `ExecutionHandle`; it never runs a worker or coordinator handler inline.

Commands are the executable vertices of the runtime graph, events explain progression, and causation supplies the edges. The graph is a projection of durable history, extended by the plan's record of work that is declared but not yet runnable.

## 2. Scope

### 2.1 PostgreSQL only

PostgreSQL is the sole required backend. `flow` has no broker abstraction and does not attempt to make PostgreSQL, Kafka, and SQS interchangeable.

This is a product feature: application writes, command completion, plan reconciliation, emitted events, and spawned commands can share one transaction. PostgreSQL notifications may reduce latency, but polling is always sufficient for correctness.

### 2.2 Milestone 1

- durable executions with idempotent start, deadlines, and explicit final states;
- direct root-command execution requiring no plan or coordinator;
- one `.Execute` verb on command, plan, and coordinator definitions, using immutable runtime binding through `With`;
- typed, versioned commands carrying both a payload type and a **result** type;
- exactly one event recording how each command ends, with successful worker results recorded automatically as typed events;
- worker registration, command scheduling, leases, attempts, retries, timeouts, and fencing;
- bounded worker-spawned child commands committed atomically with the event recording the parent's success;
- authoritative plan reads of closed direct-child membership, without duplicating membership into application result payloads;
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
- cross-execution subscriptions and event export to Kafka or analytics systems through an explicit idempotent boundary that preserves the source `(ExecutionID, position)` and neither merges execution logs nor promises cross-execution order (§9.4);
- plan simulation and dry-run tooling that uses the exact plan version and a retained execution snapshot to preview declarations and reads after historical or candidate transitions, without executing workers or external effects (§9.6);
- optional soft local affinity with bounded preference for the replica that starts an execution and automatic takeover by another replica;
- backend implementations other than PostgreSQL;
- multi-region execution.

### 2.5 Explicit non-goals

- a general-purpose message broker or event-streaming platform, or implicit cross-execution pub/sub inside the core event log;
- a database-wide event log, global event position, or total ordering guarantee across executions;
- framework-owned copies of application/domain state;
- deterministic replay of arbitrary Go code;
- exactly-once external side effects;
- distributed ACID transactions with external services;
- executable pinning to a deployed build;
- hard replica pinning or correctness that depends on instance-local memory;
- a visual workflow designer in the core package.

## 3. Core terminology

| Term | Meaning |
|---|---|
| **Execution** | One durable run, identified by `ExecutionID` and an idempotency key. |
| **Command** | One immutable logical request for work, with typed payload and typed result. Keeps one `CommandID` across attempts. |
| **Attempt** | One invocation of a command handler, identified separately from the command. |
| **Worker** | A registered typed handler for one command name and version. |
| **Event** | An immutable fact in an execution's ordered log, never destructively consumed. A successful worker return records one automatically; the runtime records one when a command ends another way; workers and applications may record additional facts. |
| **Plan** | An optional pure function declaring the commands an execution needs and what each one waits for. Used for dependencies, joins, waits, and branching across commands. |
| **Spawn** | A worker or coordinator staging an asynchronous command; a worker-spawned command is a direct child of the current command. |
| **Coordinator** | Durable typed state reacting to events for orchestration that is open-ended or unsuitable for a plan. |
| **Causation** | The direct durable record or decision responsible for creating another record. |

## 4. Public developer surface

This section defines the intended developer experience. Architecture may refine concrete field layout, but it must preserve these concepts, type checks, and operations.

### 4.1 Runtime and client

```go
type Runtime struct{}
type Client interface{ /* sealed capability */ }

func New(db *pgkit.DB, opts ...Option) (*Runtime, error)
func Migrate(ctx context.Context, db *pgkit.DB, opts ...MigrateOption) error

func (r *Runtime) Register(defs ...Registration) error
func (r *Runtime) Run(ctx context.Context) error
func (r *Runtime) Stop(ctx context.Context) error
func (r *Runtime) InTx(tx pgx.Tx) Client

func WithMaxPlanCommands(int) Option // default 1,000; 0 disables
```

`New` validates configuration and schema compatibility, starts no goroutines, and never migrates implicitly. `*Runtime` implements `Client`, so it can be passed directly to every operation whether or not `Run` is called. Registrations are accepted only before `Run`, which may be called once.

`Client` is a small sealed capability implemented by `*Runtime` and the transaction-scoped value returned by `InTx`. Application code does not construct one. API processes create a runtime and simply do not call `Run`; mixed processes and specialized worker pools may use the same type against one database.

### 4.2 Typed definitions

```go
type None = struct{}

type Command[A, R any]  struct{}
type Event[T any]       struct{}
type PlanDef[A any]     struct{}
type Coordinator[S any] struct{}

func DefineCommand[A, R any](name string, version int, opts ...CommandOption) Command[A, R]
func DefineEvent[T any](name string, version int) Event[T]

func (c Command[A, R]) Done() Event[R]        // event recorded when this command succeeds
func (c Command[A, R]) Name() string
func (c Command[A, R]) Version() int
func (c Command[A, R]) With(client Client) Command[A, R]

func Handle[A, R any](
    cmd Command[A, R],
    worker func(context.Context, *Work[A]) (R, error),
    opts ...WorkerOption,
) Registration

func DefinePlan[A any](name string, version int, plan func(*Plan, A)) PlanDef[A]
func (p PlanDef[A]) With(client Client) PlanDef[A]

func DefineCoordinator[S any](name string, version int, handlers ...Handler[S]) Coordinator[S]
func (c Coordinator[S]) With(client Client) Coordinator[S]
func OnStart[S any](h func(context.Context, *Coordination[S]) error) Handler[S]
func On[S, T any](event Event[T], h func(context.Context, *Coordination[S], Received[T]) error) Handler[S]

func WithMaxAttempts(int) CommandOption
func WithRetryPolicy(RetryPolicy) CommandOption
func WithTimeout(time.Duration) CommandOption   // per-attempt wall clock
func WithQueue(string) CommandOption            // worker lane
```

A command declares both what it takes and what it returns. `Command.Done()` is the event carrying that result; it shares the command's name and version, needs no separate declaration, and is what `After` waits on. It is an ordinary `Event[R]`, not a separate event category.

Names are stable durable identifiers. Every definition carries an explicit positive integer version; `0` is invalid. A `(name, version)` pair has immutable payload and result meaning once used, while its handler implementation may change and redeploy freely. A runtime claims only work for pairs it has registered; unknown pairs stay pending for a compatible process and consume no retry budget.

Registration is explicit and runtime-local; definitions mutate no package-global state. A runtime rejects duplicate workers for one command pair, more than one `OnStart` handler for a coordinator, and duplicate handlers for one event pair within a coordinator.

`With` returns a new immutable, concurrency-safe value of the **same definition type**, carrying the client capability in private non-durable state. It never mutates or registers the original definition, performs no I/O, and creates no execution. Definition identity, serialization, event references, and registration ignore the binding.

### 4.3 Plans

```go
type Plan struct{}
type Node struct{}

type CommandStatus string
const (
    StatusSucceeded CommandStatus = "succeeded"
    StatusFailed    CommandStatus = "failed"
    StatusCancelled CommandStatus = "cancelled"
    StatusExpired   CommandStatus = "expired"
    StatusSkipped   CommandStatus = "skipped"
)

type CommandFailure struct {
    Code    string
    Message string // size-bounded and redacted
}

type CommandOutcome[R any] struct {
    Status  CommandStatus
    Result  R               // populated only on success
    Failure *CommandFailure // populated only on an unsuccessful final state
}

func Do[A, R any](p *Plan, key string, cmd Command[A, R], args A) *Node

func Fact[T any](p *Plan, event Event[T]) (T, bool)
func Facts[T any](p *Plan, event Event[T]) []T
func Children(p *Plan, parentKey string) ([]string, bool)
func Result[A, R any](p *Plan, key string, cmd Command[A, R]) (R, bool)
func Outcome[A, R any](p *Plan, key string, cmd Command[A, R]) (CommandOutcome[R], bool)

func (n *Node) After(keys ...string) *Node          // all named commands succeeded
func (n *Node) AfterSettled(keys ...string) *Node   // all named commands terminal
func (n *Node) AfterFailed(keys ...string) *Node    // all named commands unsuccessful
func (n *Node) AfterAny(count int, keys ...string) *Node
func (n *Node) Await(events ...EventName) *Node     // named facts have arrived
func (n *Node) Within(time.Duration) *Node          // bound on waiting to become runnable
func (n *Node) Delay(time.Duration) *Node           // earliest start once runnable
func (n *Node) Optional() *Node                     // does not determine execution outcome
func (n *Node) MaxAttempts(int) *Node
func (n *Node) RetryPolicy(RetryPolicy) *Node
```

`Children`, `Result`, and `Outcome` address a command declared earlier with `Do` in the current evaluation or already present in the execution from `Do`, `Spawn`, or `Issue`. `Children` returns the authoritative direct-child keys in deterministic key order once the parent succeeds and its membership is closed; it returns false while the parent is non-terminal or if the parent ends unsuccessfully. `Result` returns true only for success; `Outcome` returns true for any final state. The supplied command definition for `Result` or `Outcome` must match the key's stored name and version or evaluation fails as a plan defect. These are typed views over command state and its recorded event, not event abstractions of their own.

A read key that is neither currently declared nor durably present is a plan defect rather than an absent read. Dependency builders likewise name command keys in the execution-wide key namespace, not only commands created by the current plan evaluation. Forward references within one evaluation are valid, because dependency-key validation occurs after the plan function returns (§10.2).

`EventName` is satisfied by both `Event[T]` and `Command[A, R].Done()`, so `After("origin")` and `Await(DepositConfirmed)` are the same mechanism expressed two ways: waiting for a fact to exist.

`Do` is a free function because Go methods cannot declare their own type parameters. Chaining is unaffected.

### 4.4 Execution operations

```go
type ExecutionID string
type CommandID   string

type ExecutionHandle struct {
    ID            ExecutionID
    Type          string
    Key           string
    RootCommandID CommandID // set only by Command.Execute
    Created       bool
}

func (cmd Command[A, R]) Execute(
    ctx context.Context, key string, args A,
    opts ...ExecutionOption,
) (ExecutionHandle, error)

func (plan PlanDef[A]) Execute(
    ctx context.Context, key string, args A,
    opts ...ExecutionOption,
) (ExecutionHandle, error)

func (coordinator Coordinator[S]) Execute(
    ctx context.Context, key string, initial S,
    opts ...ExecutionOption,
) (ExecutionHandle, error)

func Issue[A, R any](ctx context.Context, c Client, id ExecutionID, key string, cmd Command[A, R], args A) (CommandID, error)
func Publish[T any](ctx context.Context, c Client, id ExecutionID, event Event[T], key string, payload T) error

func CancelExecution(ctx context.Context, c Client, id ExecutionID, reason string) error
func CancelCommand(ctx context.Context, c Client, id CommandID, reason string) error

func WithExecutionDeadline(time.Duration) ExecutionOption
func WithoutExecutionDeadline() ExecutionOption
func WithMetadata(map[string]string) ExecutionOption
func WithFailFast(bool) ExecutionOption
```

Every executable definition uses the same verb and returns the same handle:

- `Command.Execute` creates an execution and enqueues its root command atomically. Its root command key is the reserved key `root`, exposed through `RootCommandID`.
- `PlanDef.Execute` creates a plan-driven execution, evaluates the initial pure plan declaration, and enqueues every initially ready command in the same transaction.
- `Coordinator.Execute` creates a coordinator-driven execution and enqueues a durable initial activation. The runtime later invokes its optional `OnStart` handler.

All three calls require the receiver to carry a client from `With`; calling `.Execute` on an unbound definition returns `ErrInvalid` without writing. A successful call returns after durable scheduling and never invokes a worker or coordinator handler inline. The caller-supplied `key` is the execution idempotency key. The methods select mutually exclusive execution modes. `Issue` and `Publish` remain available to any process holding a client and may participate in a caller-owned transaction through `InTx`.

Calling `With(client)` on any definition returns the same static type with that capability attached. Calling `With` again replaces the capability only in the new copy, so `SendTxn.With(runtimeOverride).Execute(...)` is the explicit per-call override. Long-lived runtime-bound values are safe for concurrent use. A value bound to `runtime.InTx(tx)` is valid only for that transaction's lifetime and returns `ErrClosed` after the transaction ends.

### 4.5 Handler scope and outputs

```go
type Work[A any] struct {
    Payload A
}

type ResultSource interface{ /* sealed by flow */ }

type Coordination[S any] struct {
    State S
}

type SpawnOption interface{ /* sealed by flow */ }

func Emit[T any](s Scope, event Event[T], key string, payload T) error
func Spawn[A, R any](s Scope, key string, cmd Command[A, R], args A, opts ...SpawnOption) error
func Optional() SpawnOption

func SucceedExecution(s CoordinatorScope, resultRef string) error
func FailExecution(s CoordinatorScope, reason error) error

func (w *Work[A]) Info() CommandInfo
func (w *Work[A]) OnCommit(func(context.Context, pgx.Tx) error)
func (c *Coordination[S]) OnCommit(func(context.Context, pgx.Tx) error)
```

A worker returns `(R, error)`. Conceptually, the worker emits an event when its command finishes. The API makes the common success path automatic: returning `(result, nil)` records the event carrying `R`, together with any additional events, spawned commands, and `OnCommit` writes it staged — all in one short transaction. Returning an error discards every staged output. A retryable error produces attempt history but no final event; a final failure records `CommandFailed` after the retry policy is exhausted or the error is classified permanent.

`Do` and `Spawn` are both asynchronous, but intentionally use different verbs. `Do` is a repeatable declarative operation evaluated many times and reconciled by key. `Spawn` is an imperative staged output of one successful handler decision. It never calls the child handler inline.

From a worker, every spawned command is a direct child of the current command and automatically inherits execution identity and causation. The successful parent return closes that parent's direct-child membership: all children staged by that logical command become visible with the event recording its success, and that parent can never add another child later. Closure describes membership only; it does not mean that the children have finished or succeeded. Each child may subsequently succeed, retry, fail, expire, or be cancelled independently. From a coordinator, spawned commands are caused by the currently handled event.

Spawned commands are required by default and therefore determine execution outcome. `flow.Optional()` makes one spawned command optional. The runtime's authoritative parent-child relationship is derived from staged `Spawn` calls, never from an application payload. Plans read that relationship with `Children`; an application result carries child keys only when they are domain data or identify a semantic subset distinct from all direct children.

Every `*Work[A]` implements the sealed `ResultSource` capability used by `ResultOf` (§4.6). Inside a worker, `ResultOf(work, key, cmd)` may read only a command explicitly named as a dependency of the current command, and the supplied definition must match that dependency's durable name and version. The dependency key remains explicit in the current command's payload when application logic needs to select it. A successful dependency returns its immutable typed result; a non-dependency, mismatched definition, or dependency without a successful result returns a structured permanently classified error. Workers cannot inspect arbitrary commands or the wider execution graph.

Requirements:

- worker and coordinator outputs are buffered until successful return;
- output payloads are type-checked through their definitions;
- `Emit` and `Spawn` are available to both worker and coordinator scopes; a plan uses `Do`, never `Spawn`;
- duplicate equivalent `Spawn` calls for one key within one handler decision coalesce, while different content for one key returns `ErrConflict` and commits nothing;
- execution completion functions are available only to coordinator scopes; direct and plan-driven executions complete automatically (§6.4);
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

`ResultSource` is sealed and implemented by every `*Work[A]` and by `ExecutionTrace`, the immutable inspection snapshot containing command results. For a worker source, dependency results are immutable before the handler becomes runnable; the runtime may batch or preload them so repeated typed `ResultOf` calls do not imply one database query each. Inspection never mutates execution state.

### 4.7 Error classification

```go
func Permanent(err error) error
func RetryAfter(d time.Duration, err error) error
```

### 4.8 Surface

| Category | Exported |
|---|---|
| Runtime | `New`, `Migrate`, `Register`, `Run`, `Stop`, `InTx` |
| Definitions | `DefineCommand`, `DefineEvent`, `DefinePlan`, `DefineCoordinator`, `Handle`, `OnStart`, `On`, `Done`, `Name`, `Version`, `With` |
| Plans | `Do`, `Fact`, `Facts`, `Children`, `Result`, `Outcome`, plus 10 command builder methods |
| Execution | `Execute` on `Command`, `PlanDef`, and `Coordinator`; `Issue`, `Publish`, `CancelExecution`, `CancelCommand` |
| Handler output | `Emit`, `Spawn`, `Optional`, `OnCommit`, `Info`, `SucceedExecution`, `FailExecution` |
| Inspection | `GetExecution`, `LookupExecution`, `Trace`, `History`, `ListExecutions`, `AwaitExecution`, `ResultOf` |
| Errors | `Permanent`, `RetryAfter` |

The smallest path is `DefineCommand`, `Handle`, `With(runtime)`, `Command.Execute`, and `Run`. Store the returned same-type copy when a definition is executed repeatedly. Add `Spawn` when a worker discovers bounded children. Add `DefinePlan`, `Do`, `PlanDef.Execute`, and event reads only when the execution needs cross-command dependencies, joins, waits, or branching. Coordinators, cancellation, transaction composition, and policy customization form the advanced operational surface.

## 5. Worked examples

### 5.1 Direct background command

A plan is not required for ordinary durable background work:

```go
type ReceiptArgs struct {
    OrderID string
    Email   string
}

var SendReceipt = flow.DefineCommand[ReceiptArgs, flow.None]("send_receipt", 1)

func sendReceipt(ctx context.Context, w *flow.Work[ReceiptArgs]) (flow.None, error) {
    if err := mailer.SendReceipt(ctx, w.Payload.OrderID, w.Payload.Email); err != nil {
        return flow.None{}, err
    }
    return flow.None{}, nil
}

rt, err := flow.New(db)
if err != nil {
    return err
}
if err := rt.Register(flow.Handle(SendReceipt, sendReceipt)); err != nil {
    return err
}

// With returns a copy; Execute persists and queues without calling sendReceipt inline.
h, err := SendReceipt.With(rt).Execute(
    ctx,
    "receipt/"+orderID,
    ReceiptArgs{OrderID: orderID, Email: email},
)
```

`Runtime.Run` claims the queued command and invokes the registered `sendReceipt` worker. `Command.Execute` returns an `ExecutionHandle` immediately. When the command succeeds, its result event and the execution's `ExecutionSucceeded` event are recorded. If the worker spawns required children, the execution remains running until those descendants also settle.

An application may bind its frequently used definitions once:

```go
type AppFlows struct {
    SendReceipt flow.Command[ReceiptArgs, flow.None]
    Report      flow.PlanDef[ReportArgs]
    Intent      flow.Coordinator[IntentState]
}

func NewAppFlows(rt *flow.Runtime) AppFlows {
    return AppFlows{
        SendReceipt: SendReceipt.With(rt),
        Report:      ReportExecution.With(rt),
        Intent:      IntentCoordinator.With(rt),
    }
}

appFlows := NewAppFlows(rt)
h, err := appFlows.SendReceipt.Execute(
    ctx,
    "receipt/"+orderID,
    ReceiptArgs{OrderID: orderID, Email: email},
)
```

Definitions remain available separately for worker registration, typed result access, event references, other runtimes, and tests. `DefineCommand`, `DefinePlan`, and `DefineCoordinator` do not accept or retain a runtime. Binding is deliberately separate from definition: references used only for registration or inspection need no client, while calling `.Execute` requires a copy produced by `With(client)`.

### 5.2 Planned fan-out and join

```go
// ---- definitions ----

var (
    PrepareReport = flow.DefineCommand[PrepareArgs, flow.None]("prepare_report", 1)
    AnalyzePart   = flow.DefineCommand[AnalysisArgs, AnalysisResult](
        "analyze_report_part", 1,
        flow.WithMaxAttempts(5),
        flow.WithTimeout(10*time.Minute),
    )
    GenerateReport        = flow.DefineCommand[GenerateArgs, ReportResult]("generate_report", 1)
    RecordAnalysisFailure = flow.DefineCommand[FailureArgs, flow.None]("record_analysis_failure", 1)
)

type GenerateArgs struct {
    CompanyID    string
    AnalysisKeys []string
}

// ---- the plan: high-level progression and joins ----

var ReportExecution = flow.DefinePlan[ReportArgs]("report_execution", 1, planReport)

func planReport(p *flow.Plan, args ReportArgs) {
    flow.Do(p, "prepare", PrepareReport, PrepareArgs{CompanyID: args.CompanyID})

    analysisKeys, ok := flow.Children(p, "prepare")
    if !ok {
        return
    }

    for _, key := range analysisKeys {
        flow.Do(
            p, "record-failure/"+key,
            RecordAnalysisFailure,
            FailureArgs{AnalysisKey: key},
        ).AfterFailed(key)
    }

    dependencies := append([]string{"prepare"}, analysisKeys...)
    flow.Do(p, "generate", GenerateReport, GenerateArgs{
        CompanyID:   args.CompanyID,
        AnalysisKeys: analysisKeys,
    }).After(dependencies...)
}

// ---- a worker may expand one command into bounded direct children ----

func prepareReport(ctx context.Context, w *flow.Work[PrepareArgs]) (flow.None, error) {
    analyses, err := determineAnalyses(ctx, w.Payload.CompanyID)
    if err != nil {
        return flow.None{}, err
    }

    for _, analysis := range analyses {
        key := "analysis/" + analysis.ID
        if err := flow.Spawn(w, key, AnalyzePart, analysis.Args); err != nil {
            return flow.None{}, err
        }
    }

    w.OnCommit(func(ctx context.Context, tx pgx.Tx) error {
        return reports.MarkPrepared(ctx, tx, w.Payload.CompanyID, len(analyses))
    })
    return flow.None{}, nil
}

func analyzePart(ctx context.Context, w *flow.Work[AnalysisArgs]) (AnalysisResult, error) {
    return analyzers.For(w.Payload.Kind).Analyze(ctx, w.Payload)
}

func generateReport(ctx context.Context, w *flow.Work[GenerateArgs]) (ReportResult, error) {
    results := make([]AnalysisResult, 0, len(w.Payload.AnalysisKeys))
    for _, key := range w.Payload.AnalysisKeys {
        result, err := flow.ResultOf(w, key, AnalyzePart)
        if err != nil {
            return ReportResult{}, err
        }
        results = append(results, result)
    }
    return reports.Generate(ctx, w.Payload.CompanyID, results)
}
```

Wiring a worker process:

```go
rt, err := flow.New(db)
if err != nil {
    return err
}
if err := rt.Register(
    flow.Handle(PrepareReport, prepareReport),
    flow.Handle(AnalyzePart, analyzePart),
    flow.Handle(GenerateReport, generateReport),
    flow.Handle(RecordAnalysisFailure, recordAnalysisFailure),
    ReportExecution,
); err != nil {
    return err
}
return rt.Run(ctx)
```

Starting from an RPC process that runs no workers:

```go
flowDB, err := flow.New(db)
if err != nil {
    return err
}

h, err := ReportExecution.With(flowDB).Execute(
    ctx,
    reportID,
    ReportArgs{CompanyID: companyID},
    flow.WithFailFast(false), // let independent analyses settle before final failure
)
```

The first plan evaluation declares only `prepare`. Its worker discovers the complete analysis membership and stages every child with `Spawn`; the children and the event returned by `PrepareReport.Done()` become visible atomically. `Children` then exposes that authoritative closed membership, and the next evaluation declares every failure branch and the pending `generate` command together. Analysis workers run in parallel. Once every `After` dependency succeeds, `generate` becomes runnable and its worker reads the immutable typed dependency results through `ResultOf`.

If the preparation handler fails partway through staging, no child is committed. If one analysis exhausts retries, it records `CommandFailed`, the already-declared corresponding `AfterFailed` branch runs, `generate` becomes `skipped`, and the execution ultimately fails after the remaining analyses settle. Child membership is never duplicated in `PrepareReport`'s result, and routine value plumbing introduces no result-loop early-return trap. The plan needs no coordinator because this fan-out membership closes with one successful parent return.

## 6. Executions

### 6.1 Identity and idempotent start

An execution has a generated `ExecutionID`, a driver mode, an execution type, and a caller-supplied key. The driver mode is `direct`, `plan`, or `coordinator`. The type is the receiver definition's name: command, plan, or coordinator respectively. `(driver_mode, execution_type, execution_key)` is unique for as long as the execution is retained; an empty key enforces no uniqueness.

Repeating `.Execute` on the same definition with the same key, definition version, canonical arguments or initial state, and materially relevant options returns the existing execution with `Created == false`. Reusing that identity with materially different content returns `ErrConflict` and changes nothing.

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

An execution succeeds when every command is terminal, nothing remains pending or awaited, and no required command ended `failed`, `expired`, or `cancelled`. `skipped` resolves graph structure but never fails an execution by itself. Commands made optional through the plan builder or spawned with `flow.Optional()` never determine the outcome. Direct roots, plan-declared, worker-spawned, coordinator-spawned, and externally issued commands otherwise participate in the same outcome rules.

**Fail-fast is the default and does not suppress failure handling.** When a required command becomes `failed`, `expired`, or `cancelled`, one transaction, in this order:

1. records that command's final state;
2. **resolves every dependency naming it**, so commands selected by `AfterFailed` or `AfterSettled` become runnable and commands that required its success become `skipped`;
3. marks the execution `failing` and cancels remaining non-terminal work **except** the failure-handling subgraph — the commands just made runnable, their descendants, and anything already running;
4. leaves the execution terminal only once that failure-handling subgraph resolves.

This is what makes refunds, compensation, reconciliation, and cleanup expressible; a command selected by `AfterFailed` is guaranteed its chance to run. Execution-level cancellation is different: it cancels everything and selects no failure branches.

`WithFailFast(false)` lets unrelated branches finish, and the outcome is computed once every command is terminal.

The plan-read gate is a condition of **successful** completion, not terminal failure. Once a required command has ended unsuccessfully, temporarily unavailable plan reads do not keep a doomed execution alive. Failure waits only for the explicitly declared failure-handling subgraph described above, or for all existing commands to settle when fail-fast is disabled. If failure handling must wait for an external fact, the plan declares that work with `AfterFailed` together with `Await` and, when appropriate, `Within`; a bare conditional read is not an implicit failure-handling command.

### 6.4 Completion

For a direct execution, completion is derived from its closed command tree: the root and every spawned descendant are terminal, no required command failed, and no successful command can add another child because each command's child membership closed atomically with its successful return. Children staged by the same commit are counted before completion is evaluated, so temporary quiescence cannot report false success. Direct mode has no joins across sibling results, event waits, or later conditional branches; those require a plan or coordinator.

For a plan-driven execution, **successful** completion is derived: every command is terminal, no plan-declared command is pending or awaiting an unarrived fact, the latest evaluation has no temporarily unavailable read (§10.1), and re-evaluating the plan declares nothing new. Terminal failure follows §6.3 and is not blocked by temporarily unavailable reads after its explicit failure-handling work has resolved. Because spawned children increment the same open-command count and the plan records what is still expected, neither unfinished fan-out nor temporary quiescence can report false success.

For a coordinator-driven execution, completion is an explicit `SucceedExecution` or `FailExecution` decision, and temporary quiescence never completes it.

Success may commit only when no command is non-terminal and nothing staged by the same decision would make that false. On completion any coordinator is closed and pending work is cancelled atomically, and one immutable `ExecutionSucceeded` or `ExecutionFailed` event is recorded.

### 6.5 Deadlines

An execution carries a deadline, defaulting to 30 days, overridable with `WithExecutionDeadline` and removable with `WithoutExecutionDeadline` — which opts out of the bounded-completion guarantee (§12.6). On expiry the execution becomes `expired`, non-terminal commands are cancelled, and the reason is recorded. Every command and wait deadline is capped by the execution deadline, so nothing outlives its execution.

### 6.6 External additions

`Issue` may add a command to a running plan- or coordinator-driven execution. `Publish` may add a fact to any running execution. Both are rejected for a terminal execution unless the operation is an idempotent retry of an already-stored key with equivalent content.

`Issue` is rejected for direct executions. Every command in direct mode must descend from the root through an atomic `Spawn`, which is what makes its topology closed and automatic completion sound. Independently submitted background work starts its own direct execution with `Command.Execute`. `Publish` is accepted for a running direct execution as immutable history but cannot alter progression because no plan or coordinator observes it.

A command added by `Issue` is required for outcome purposes in Milestone 1. Detached work belongs in a separate execution.

## 7. Commands

### 7.1 Identity

Every command has a `CommandID` stable across attempts, an `ExecutionID`, a name and version, a `CommandKey` unique within its execution, a canonical typed payload, a required/optional classification, and causation identifying what created it. A worker-spawned command additionally records the current command as its direct parent.

A plan-declared command's key is its command key. All creation paths share one execution-wide key namespace. Re-declaring the same plan-owned key with an equivalent definition, canonical payload, and policy is a no-op; different content returns `ErrConflict`. A plan may read or depend on an existing spawned command, but attempting to take ownership of its key with `Do` is a plan defect.

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

### 7.3 Events recorded for commands

When a command reaches a final state, `flow` records exactly one event describing how it ended.

For success, returning `(result, nil)` records the event returned by `Command.Done()`, carrying the typed result and sharing the command's name and version. This is automatic and cannot be suppressed. It is the fact that `After` waits for, which makes "wait for work" and "wait for a fact" one mechanism.

A worker may additionally call `Emit` to record application events. Those are recorded at earlier positions than the event recording success, so any reader observing success has already observed them.

Failure, cancellation, expiry, or skipping records exactly one `CommandFailed`, `CommandCancelled`, `CommandExpired`, or `CommandSkipped` event instead. All are ordinary events in the same log. A database constraint prevents more than one event recording the final state of a command. Retryable attempt failures are history, not events, because the command has not ended.

This invariant makes every terminal command transition plan-observable and auditable, and lets plan simulation follow terminal progress when combined with retained command, dependency, and child-membership records. It does **not** make the event log alone a complete source for command or execution state: the immutable command row remains the issuance record (§9.6).

### 7.4 Bounded child spawning

`Spawn` stages a command for asynchronous delivery. It does not invoke a handler, block for a result, or open a nested transaction.

For a worker handler:

- every spawned command is a direct child of the current logical command;
- stable child keys are unique across the execution and remain stable across parent retries;
- all staged children, emitted application events, the event recording the parent's success, and `OnCommit` writes commit atomically on successful return;
- if the handler returns an error, panics, loses its lease, is cancelled, or the settle transaction rolls back, none of its staged children become visible;
- after the parent succeeds, its direct-child membership is closed permanently, although the children remain independently active;
- spawned children are required unless created with `flow.Optional()`.

The runtime derives authoritative child membership from command causation, and a plan joining all direct children reads it with `Children`. A typed parent result carries child keys only when those keys are themselves domain data or identify a semantic subset of a heterogeneous child set; correctness and tracing never infer membership from arbitrary result payload fields.

Before accepting the worker result, settlement validates the entire staged set against the execution-wide key namespace and applicable command limit. A key already owned by another creation decision, different content for one buffered key, or a deterministic limit violation is a permanent structured output failure: no staged output or success event commits, the parent records `CommandFailed`, and rerunning the same worker is not attempted because it cannot repair that decision.

For a coordinator handler, `Spawn` has the same buffering and atomicity but the current inbox event is the cause; no parent command is implied.

### 7.5 Waiting and scheduling

A command becomes runnable when all declared dependencies are satisfied and all awaited facts have arrived. `Within` bounds how long it may remain pending, measured from when it was declared; on expiry it becomes `expired` and dependents resolve through the failure branch. `Await` without `Within` inherits the execution deadline, so no wait is ever unbounded.

`Delay` sets the earliest time a runnable command may be claimed. **A delayed command is the durable timer primitive**; there is no separate timer concept. A waiting or delayed command holds no worker, connection, goroutine, or lease.

### 7.6 Claiming and rolling deployments

A worker claims only `(name, version)` pairs it has registered, using row-level locking that skips rows another process is claiming. A process that does not recognize a command leaves it pending, consumes no retry budget, and never fails it, so old and new versions of a service may share one database.

Unclaimable backlog — a `(name, version)` with pending work and no live worker registering it — is surfaced through inspection and observability rather than stalling silently.

## 8. Attempts, retries, and failure

### 8.1 Separate attempt identity

Each claimed execution of a command creates an attempt record with its own identity, worker and process identity, timings, structured error, and whether it consumed retry budget. The logical command keeps one `CommandID` throughout.

### 8.2 Default behavior

The default policy allows 5 attempts — one execution and 4 retries — with delays of 1s, 5s, 30s, and 2m, each with proportional jitter. Attempt count and policy are configurable per command definition and per plan-declared command. The chosen retry time is persisted, so inspection shows exactly when a command runs again. A per-attempt timeout cancels the handler context and is treated as a retryable error unless the command policy classifies it otherwise; exhaustion ends in `failed`, not `expired`. `expired` means the command never became or remained eligible within its waiting or execution deadline.

| Worker return | Effect | Retry budget |
|---|---|---|
| `(result, nil)` | event carrying the result; command succeeds | — |
| plain `error` | retry per policy | consumed |
| `flow.RetryAfter(d, err)` | retry at an explicit delay | consumed |
| `flow.Permanent(err)` | command fails immediately | consumed |
| panic | recovered, treated as retryable | consumed |

### 8.3 Operational interruption

Shutdown interruption, lease loss, and unregistered-version deferral never consume retry budget and never make a command terminal. They are retained as operational history and observations, not as domain progression.

### 8.4 Terminal failure

A command that exhausts its attempts, or returns a permanent error, becomes `failed` and records `CommandFailed`, with its full attempt history preserved.

Attempt failures are not application results. A negative application result is a **successful** command whose typed result says so — a distinction that keeps retry mechanics out of application semantics.

### 8.5 Joining after child failures

The application chooses failure behavior with existing command policies rather than a separate fan-out state machine:

- **all must succeed** — spawn required children (the default), declare the join with `After`, and let the joined worker read successful dependency results through `ResultOf`; a terminal child failure skips the success join and fails the execution;
- **wait for all before failing** — start with `WithFailFast(false)` so independent required siblings settle before outcome calculation;
- **partial result** — spawn children with `flow.Optional()`, read each with `Outcome`, and declare the final command only after all are terminal;
- **compensation** — declare keyed commands with `AfterFailed(childKey)`; fail-fast preserves that failure-handling closure.

For a partial result, `Outcome` is the durable join input:

```go
outcomes := make([]flow.CommandOutcome[AnalysisResult], 0, len(keys))
for _, key := range keys {
    outcome, settled := flow.Outcome(p, key, AnalyzePart)
    if !settled {
        return
    }
    outcomes = append(outcomes, outcome)
}
flow.Do(p, "partial-report", GeneratePartialReport, PartialArgs{Analyses: outcomes}).
    AfterSettled(keys...)
```

## 9. Events

### 9.1 Immutable facts

Every event has an `EventID`, `ExecutionID`, name and version, optional key, canonical typed payload, an immutable per-execution position, occurrence time, causation, and where applicable the originating command and attempt.

Events are append-only and never destructively consumed. Unlike a command, which one worker handles, an event is observed independently by the plan and by any coordinator subscribing to it.

### 9.2 How events are recorded

There is one event model and one typed abstraction: `Event[T]`. Events enter the log in three ways:

- a successful worker return automatically records the event returned by `Command.Done()`, carrying its result;
- workers and coordinators call `Emit`, while application code, webhooks, and monitors call `Publish`, to record additional facts;
- the runtime records facts such as `CommandFailed`, `CommandCancelled`, `CommandExpired`, `CommandSkipped`, `PlanFailed`, `CoordinatorFailed`, `ExecutionSucceeded`, `ExecutionFailed`, `ExecutionCancelled`, and `ExecutionExpired`.

These are different event names and payloads, not different event systems or developer-facing categories. Attempt failures, lease renewals, and lease loss remain operational history rather than events, so transient mechanics never masquerade as permanent facts.

The runtime deliberately does not append a `CommandStarted` event. `Trace` derives whether a command is currently running from command state and its active lease, and reads when and where attempts ran from attempt records and operational history. An attempt start is durable operational history, while the command's single terminal event is the permanent plan-visible fact. This avoids duplicate representations of running state and prevents plans from reacting to retry mechanics.

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

There is no position or ordering relationship across executions. A future export, subscription, or execution-start bridge may interleave events from several executions in any order, but it must retain each event's source `(ExecutionID, position)`, cross an explicit idempotent boundary, and make no claim that the resulting stream is one merged `flow` log.

### 9.5 Payloads and result references

Small durable results may be carried in payloads. Large or sensitive outputs belong in application-owned tables or object storage, referenced by a stable identifier. Changing data behind a reference does not change the historical event; use immutable, versioned, or content-addressed references where historical reproducibility matters.

### 9.6 Replay and recovery boundary

`flow` preserves replayable orchestration history without being a strict event-sourced runtime. Immutable command rows record requested work and parentage; events record how commands ended and any additional application facts; attempts record transient execution mechanics; materialized command and execution states make claiming and inspection efficient.

Plans may be re-evaluated from the retained arguments, command states, results, closed child memberships, and events, and inspection projections may be rebuilt from commands, dependencies, attempts, events, and causation. Accurate historical plan simulation additionally requires the exact plan version and the retained plan-visible snapshot as it existed at each simulated transition; an event prefix alone is insufficient because command issuance, dependencies, and closed child membership are not all events. Recovery and simulation never replay arbitrary Go handlers or repeat their historical external side effects. A command row is itself the durable record that the command was issued, so Milestone 1 does not add a redundant `CommandIssued` event.

## 10. Plans

### 10.1 The plan function

A plan is optional. Applications use one only when a direct root command and its spawned descendants cannot express the required dependencies, joins, waits, or branching.

The public name is intentionally `Plan`, not `Workflow`: the execution is the whole durable workflow, while a plan is only the optional pure declaration function that coordinates some executions.

When present, a plan is a pure function of the execution's root arguments and the events, command states, and results recorded so far. It declares commands; it never performs work. It must not do I/O, read clocks, use randomness, start goroutines, or depend on mutable globals, and must be deterministic given identical inputs. Although a plan reacts to events, it does not receive one event callback; each evaluation sees the relevant durable snapshot accumulated so far.

Ordinary Go cannot be sandboxed completely. `flow` therefore enforces purity by capability and verification: a plan receives no context, client, database, transaction, clock, or worker scope; declarations are reconciled by canonical identity; panics and conflicts are plan defects; and `flowtest` can evaluate the same snapshot repeatedly and compare declarations and reads. A debug runtime option may perform the same double evaluation. A plan defect records `PlanFailed`, fails the execution, and cancels outstanding work; it never consumes a worker's retry budget or reruns a worker whose result was already accepted.

The terminal plan-defect rule also applies to the initial evaluation in `PlanDef.Execute` and evaluations caused by ingress. The execution and accepted triggering command or fact remain durable for inspection; the operation returns a typed error carrying the `ExecutionID`, and an idempotent retry finds the same terminal execution.

`Fact` and `Facts` read events already recorded in this execution. `Children` reads a command's authoritative direct-child membership after that membership closes successfully. `Result` reads a successful command's typed result. `Outcome` reads any terminal result or structured failure, including for worker-spawned commands. A branch that genuinely changes topology according to runtime information is an ordinary Go conditional over those reads. Routine value plumbing belongs in a dependent worker through `ResultOf`, as shown in §5.2.

**Reads are recorded.** Every `Fact`, `Facts`, `Children`, `Result`, and `Outcome` call during an evaluation is registered as an input that evaluation consulted. The library records more than the public value and boolean: it classifies each read as available, temporarily unavailable, or permanently unavailable.

| Read | Available | Temporarily unavailable | Permanently unavailable |
|---|---|---|---|
| `Fact` / `Facts` | matching facts exist | no matching fact has arrived | — |
| `Children` | parent succeeded and membership is closed, including an empty set | parent is non-terminal | parent ended unsuccessfully |
| `Result` | command succeeded | command is non-terminal | command ended unsuccessfully |
| `Outcome` | command is terminal | command is non-terminal | — |

The public boolean remains deliberately small: `Children` and `Result` return false for either unavailable state, while `Outcome` is the operation for code that must distinguish success from terminal failure. The internal distinction prevents a result that can never exist from being mistaken for one that may arrive later.

**`Children`, `Result`, and `Outcome` may only read a key declared earlier with `Do` in the current evaluation or an existing command key.** A durable command may have been created by `Do`, `Spawn`, or `Issue`. Reading any other key, or supplying a command definition whose name and version do not match the key, returns `ErrInvalid`; it is a plan defect, not a runtime condition.

Reading a command's children, result, or outcome does not by itself create a dependency edge. A command that consumes those keys or values must also name the source commands with `After` or `AfterSettled`, so scheduling and traceability do not depend on an implicit data read. A joined worker may then read the successful immutable results of its explicitly named dependencies through `ResultOf`.

Plans are ordinary Go functions, so statements reached before an early return are the declarations produced by that evaluation; the runtime does not pretend source order is irrelevant. Applications should declare structural work as soon as its keys are known and reserve value reads for topology decisions. `Children`, explicit dependencies, and worker-side `ResultOf` remove the common need to loop over unfinished results merely to construct the next payload.

Plan fragments compose as ordinary Go functions taking `*Plan`; the library needs no separate composition primitive:

```go
func planPayout(p *flow.Plan, prefix string, args PayoutArgs) {
    flow.Do(p, prefix+"/send", SendTxn, args.Send)
    flow.Do(p, prefix+"/confirm", ConfirmTxn, args.Confirm).
        After(prefix + "/send")
}

func planIntent(p *flow.Plan, args IntentArgs) {
    planPayout(p, "origin", args.Origin)
    planPayout(p, "destination", args.Destination)
}
```

A fragment intended for repeated use accepts a caller-chosen key prefix, and each logical instance uses a distinct prefix. A conflicting collision is a plan defect. Equivalent duplicate declarations intentionally coalesce, so `flowtest` should assert the complete expected key set rather than assuming every accidental reuse fails loudly.

#### Temporarily unavailable reads block successful completion

A temporarily unavailable read means the plan's output may differ once that input arrives. An execution therefore **cannot succeed while its most recent evaluation contains a temporarily unavailable read.** Permanently unavailable reads do not block completion: a required unsuccessful command drives the failure rules in §6.3, while plans handling an optional or otherwise accepted failure use `Outcome`, `AfterFailed`, or `AfterSettled` to make that branch explicit.

This closes an otherwise silent hole. Given:

```go
if route, ok := flow.Fact(p, RouteSelected); ok {
    flow.Do(p, "dest", SendTxn, destTxn(args, route)).After("origin")
}
```

if `RouteSelected` never arrives, no `dest` command is ever declared. Without this rule, an execution whose declared commands had all succeeded would report success while the destination transaction was never sent. With it, the temporarily unavailable `RouteSelected` read keeps the execution running until the fact arrives or the execution deadline expires.

The success gate is automatic and cannot be forgotten, because it derives from the read itself rather than from a separate declaration. Temporarily unavailable reads are bounded by the execution deadline; a plan wanting a tighter bound declares a command with `Await(...).Within(...)` instead of branching on a bare `Fact`. Once the execution is failing, unresolved reads do not postpone terminal failure; work that must run or wait during failure is represented by the explicit failure-handling subgraph from §6.3.

### 10.2 Evaluation and reconciliation

The plan is evaluated at execution start and again whenever a relevant event is appended or an observed command changes state. Evaluation runs in the same transaction as that durable transition and reconciles the declared set against what already exists. Worker-spawned children are inserted before evaluation, so the parent's successful result and every child key are visible in the same snapshot.

| Declared key | Action |
|---|---|
| does not exist, dependencies satisfied | its command is created and becomes `ready` |
| does not exist, dependencies unsatisfied | recorded as a `pending` command with its dependencies |
| does not exist, a dependency is permanently unsatisfiable | recorded directly as `skipped` with `CommandSkipped` |
| already exists and owned by this plan | verified against the stored definition, policy, dependencies, and canonical payload; a mismatch is a plan defect |
| already exists from `Spawn` or `Issue` | may be read or named as a dependency, but `Do` using its key is a plan defect |
| previously declared, no longer declared | retained unchanged |

**A plan only grows.** It cannot withdraw, rewrite, or re-point work it already declared. Application plan logic must therefore be additive: new durable facts may reveal more declarations but may not invalidate an earlier one. A mismatch is treated as a plan defect.

Every command made runnable by one evaluation is created in that single transaction, so a crash exposes either all of them or none.

After the plan function returns, every command key named by a dependency builder is validated against the union of commands already durable and commands declared anywhere in that evaluation. This permits forward references within the function but rejects a typo or a reference to work that does not exist as a plan defect; such a dependency can never sit pending until the execution deadline. Future externally supplied facts use `Await`, and future externally issued commands cannot be anticipated by an undeclared dependency key.

The consulted-input set and each read's availability classification from the latest evaluation are persisted alongside the declared set. They determine both whether the execution may succeed (§10.1) and when the plan must next be evaluated (§10.3).

### 10.3 When the plan is evaluated

A plan's declared output is a pure function of its root arguments and the durable events and final command states it consulted. Evaluation is therefore required only when one of those can have changed:

- at execution start;
- when an event arrives whose name the latest evaluation consulted;
- when a final event is appended for a command read by the plan or named by one of its dependencies.

Claim, lease renewal, `running`, and `retry_wait` transitions do not trigger plan evaluation because no plan API can observe them. The event recorded when the command ends does.

Events of names the latest evaluation did not consult cannot change that evaluation's control path by themselves and do not trigger evaluation. Skipping is sound because plans are pure and facts are immutable, append-only, and durably re-readable; terminal command outcomes and closed child memberships are likewise immutable. If another consulted input later opens a branch that reads an older, previously ignored fact, that consulted input triggers evaluation and the older fact is still present for the newly reached branch.

Evaluation is idempotent, so an implementation may always evaluate more often than required — including on every event — without changing behavior.

A debug or test verification mode may deliberately evaluate on a durable transition that normal routing would skip and assert that canonical declarations, dependencies, consulted reads, and read-availability classifications remain unchanged. A difference is an invariant failure and none of the verification evaluation's output is committed.

### 10.4 Dependencies

| Builder | Runnable when |
|---|---|
| `After(k…)` | every named command has succeeded |
| `AfterSettled(k…)` | every named command is terminal |
| `AfterFailed(k…)` | every named command is unsuccessful — failed, expired, cancelled, or skipped |
| `AfterAny(n, k…)` | at least `n` of the named commands have succeeded |
| `Await(e…)` | every named event has arrived in this execution |

Conditions combine: a plan-declared command may name other plan-declared, spawned, or externally issued commands together with awaited facts and becomes runnable only when all conditions hold. When a named command reaches a terminal state that makes a condition permanently unsatisfiable, the dependent becomes `skipped`, records `CommandSkipped`, and its own dependents resolve in turn.

Known fan-out is repeated `Do`. Bounded fan-out discovered during work is repeated `Spawn`; the successful parent return atomically closes its direct-child membership. A join is one command naming the resulting stable command keys. Neither path needs a separate fan-out-group runtime or coordinator.

### 10.5 Cost model and limits

Each evaluation costs:

| Work | Cost |
|---|---|
| running the plan function in Go | O(commands observed or declared) — in-memory, no I/O |
| loading command states and the consulted input set | O(commands), narrow indexed columns |
| database writes | O(newly runnable commands) only |

Writes are proportional to the delta, but the read and the Go evaluation are proportional to the whole command set, and an execution produces at least one terminal event per command. Total plan work over an execution's life is therefore approximately **O(commands²)**, which is what bounds plan-driven execution size.

The default safety limit is **1,000 total commands within one plan-driven execution**, including commands created by `Do`, `Spawn`, and `Issue`, with 100 dependencies per plan-declared command. It is configurable, and `0` explicitly disables it. The limit is validated against the complete staged batch before any child or plan command is inserted, so a fan-out cannot commit partially at the configured ceiling. At the default ceiling an execution performs on the order of a million narrow row reads across its lifetime; operators raising or disabling the limit accept the plan's approximately O(commands²) cost.

This is not a queue-capacity, backlog, or concurrency limit. The database may hold any number of executions and queued commands, subject only to operational capacity. Direct and coordinator-driven executions do not use the plan graph-size limit.

Coordinator-driven executions are not re-evaluated and are not bound by this limit; they are bounded only by inbox delivery cost.

Architecture must make the per-evaluation state load a single narrow indexed query and must benchmark evaluation at the documented ceiling.

## 11. Coordinators

A coordinator is durable typed state that reacts to events. Direct-child records handle bounded command trees, and plans handle bounded joins; a coordinator exists for open-ended membership, cycles, and multi-event decisions that need durable mutable orchestration memory.

**Every execution selects exactly one driver mode.** `Command.Execute` uses a direct root command and no coordinator. `PlanDef.Execute` uses a plan as its built-in orchestration authority. `Coordinator.Execute` uses one application-defined coordinator. The modes cannot be combined within one execution, and child coordinators are a later capability.

### 11.1 Definition, start activation, and instance

A definition has a stable name, positive version, typed state schema, an optional start handler declared with `OnStart`, and exact typed event subscriptions declared with `On`. Its instance holds typed canonical state, a durable inbox position, and a lifecycle of `active → completed | failed | cancelled`.

`Coordinator.Execute` durably creates the instance and enqueues one initial activation in the same transaction. It never invokes coordinator code inline. A runtime registering the exact coordinator name and version later claims the activation and invokes `OnStart`, when present; without `OnStart`, the activation is acknowledged as a no-op and the coordinator waits for events. Events, commands, state changes, and `OnCommit` writes staged by `OnStart` follow the same atomic processing and retry rules as an event handler.

### 11.2 Historical matching-event delivery

An instance begins with its inbox at the start of the execution and receives **every matching retained event in position order**, including facts recorded before the instance existed. An external fact therefore cannot be lost by arriving early. The same rule governs plan evaluation, which sees every fact recorded so far.

### 11.3 Serialized processing

At most one handler runs per coordinator instance at a time; workers and other executions run concurrently. On a `nil` return, one transaction records the event as processed, persists new state, commits spawned commands, events, and `OnCommit` writes, and advances the inbox. On error or lease loss none of it commits, and redelivery cannot apply a decision twice.

### 11.4 Failure

Coordinator handler errors retry under the default policy. A permanent or exhausted error marks the coordinator failed, records `CoordinatorFailed`, fails the execution, and cancels outstanding work.

### 11.5 State boundary

Coordinator state may store orchestration facts such as selected route, expected keys, observed outcome flags, or local progress. It must never become a second source of truth for application entities; balances, intent status, transaction records, and report contents remain in application-owned tables, with `OnCommit` keeping coordination and domain writes atomic when they must change together.

Coordinator handlers are for short decisions. External work belongs in commands.

### 11.6 Rolling deployments

A coordinator delivery is claimed only by a runtime registering the instance's exact coordinator name and version and, for an event delivery, the matching event name and version. A process understanding neither side leaves the delivery pending without consuming retry budget, and unclaimable coordinator backlog is visible through inspection and observability.

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

That one transaction commits the event carrying the command result, its additionally emitted events, its complete staged child set, plan reconciliation and every command that reconciliation creates, dependency resolution, execution outcome transitions, history, and `OnCommit` writes. If ordinary settlement fails, none commits and the command is retried.

A recovered plan panic, nondeterministic conflict, or invalid plan read is different: the accepted worker result and its staged outputs commit, `PlanFailed` and `ExecutionFailed` are appended, and outstanding commands are cancelled. A plan defect never turns successful application work into another worker attempt.

If an application deliberately writes outside `OnCommit`, those writes are outside `flow` fencing and atomicity.

### 12.5 Serialized execution commits

Commits within one execution are serialized by its row lock; different executions commit fully in parallel.

Serialization applies to **commits, not work**. Commands of one execution run concurrently across any number of workers and queue only at the moment they commit, so every durable transition is applied against a consistent view of the execution.

### 12.6 Bounded completion

Every non-terminal command has a bounded path to terminal: direct eligibility, dependencies that will resolve, an awaited fact with a deadline, a retry schedule, a `Within` bound, or the execution deadline. Direct executions additionally have bounded topology because every worker publishes its complete direct-child set and closes membership in one successful return. A background reconciler repairs dispatch state after crashes without duplicating work.

Two situations lie outside this and are reported rather than absorbed: an execution started with `WithoutExecutionDeadline` whose commands declare no bounds, and a command whose `(name, version)` no live worker registers.

### 12.7 Caller-owned transactions

`Runtime.InTx(tx)` returns a transaction-scoped `Client` and allows every definition's `.Execute`, plus `Issue`, `Publish`, and cancellation, to commit atomically with caller-owned application writes. Definitions may bind that capability with `With`, but the resulting value must not outlive the transaction. The library defines and enforces one transaction ordering discipline across execution, command, event, plan, coordinator, and application operations; its exact lock order belongs in the architecture.

### 12.8 Causation

Outputs created inside handlers automatically inherit execution identity and causation: worker events and spawned direct children are caused by the current command; plan outputs are caused by the durable transition that triggered evaluation; coordinator outputs are caused by the start activation or event being processed; an event recording a command's final state identifies that command and transition. Callers cannot forge an origin that contradicts the active handler scope.

## 13. Cancellation, deadlines, and terminal races

`CancelExecution` marks the execution `cancelled`, cancels non-terminal commands, closes the coordinator, and records `ExecutionCancelled`. `CancelCommand` cancels one command; if it is required, the execution fails under §6.3 including its failure-handling branches.

Cancellation and completion race on the execution row, and whichever commits first wins. A handler whose command was cancelled cannot commit its result, staged outputs, or `OnCommit` writes.

Cancellation cannot undo external side effects already performed and cannot forcibly stop a non-cooperative goroutine; fencing only guarantees such a goroutine commits nothing.

Cancelling an already-cancelled target is idempotent; cancelling a differently-terminal target returns `ErrTerminal`.

## 14. Serialization, encoding, and limits

Command payloads, command results, event payloads, coordinator state, and all identity comparisons use deterministic canonical JSON. Idempotency compares canonical stored bytes, not caller memory layout or database formatting. The architecture defines the encoder and its treatment of custom marshalers; the functional requirement is that the same logical value always produces the same identity bytes.

Payload, state, configured plan-command-count, and dependency-count limits are enforced against the complete staged transaction before any durable write. Violations return `ErrPayloadTooLarge` or `ErrInvalid` with no partial effect.

## 15. Inspection and graph projection

### 15.1 Execution trace

`Trace` returns, in one call:

- the execution: driver mode, type, key, status, deadline, timings, outcome, and failure;
- every command: key, name, version, state, payload, result, last error, retry schedule, deadlines, and current running duration;
- every command not yet runnable, with its creation source, parent where applicable, dependencies, and awaited facts;
- every event: name, version, key, position, payload, arrival time, originating command and final-state metadata where applicable, and causation;
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

Every replica runs the same loop against the same database: wake on notification or poll, then claim eligible work with row-level locking that skips rows another replica is claiming. Milestone 1 has no leader election, partition assignment, consistent hashing, sticky routing, or rebalancing. Scaling out is starting a process; scaling in is stopping one.

### 16.2 Placement and takeover

Commands are not pinned to replicas; successive commands of one execution may run anywhere, and a retried command may run somewhere new.

Every running attempt holds a lease its worker renews. A replica that crashes, is killed, is partitioned, or is descheduled stops renewing; its leases expire and any other replica claims the work. Recovery is anonymous — no operator action, no control plane, and no return of the failed process — and fencing (§12.3) makes it safe against a merely slow replica.

A later optional **local affinity** mode may prefer the replica that starts an execution for its root command and causally related plan, coordinator, and child-command work. Affinity is soft placement metadata, not ownership: the preferred replica receives only a short, configurable opportunity to claim eligible work before every compatible replica may claim it. The default remains placement-neutral, affinity adds no correctness guarantee, and no handler may depend on local cache contents or process memory. Once another replica claims work, the ordinary lease and fencing rules apply. This bounds failover delay and preserves progress if the preferred replica is unavailable, incompatible, overloaded, or terminated. Large fan-outs may deliberately omit or override the preference so locality does not defeat parallelism.

### 16.3 Roles

`Runtime` is the ordinary configured application handle and also implements the lightweight `Client` capability used by execution operations. Calling `Run` adds worker and coordinator processing; omitting it leaves an API-only handle. Deployments may mix one binary doing both, split request-serving from worker replicas, or run several pools registering different command names and versions, which scale independently against one database. `InTx` supplies the same operations through a transaction-scoped capability.

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
| commands per plan-driven execution | 1,000; configurable, `0` disables |
| dependencies per plan-declared command | 100 |
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

The library ships a test package so direct command trees, workers, plans, and coordinators are testable without a database:

- a worker is an ordinary function — given a payload and an immutable set of explicitly declared dependency results, assert `ResultOf` reads, its returned result or error, its staged events and spawned children, direct-child keys, optional classification, and registered application writes;
- a direct-execution harness begins with one root worker decision and settles staged descendants, allowing completion and failure policy to be asserted without defining a plan;
- a plan is a pure function — given root arguments, facts, command states, and closed child memberships, assert its declarations, dependencies, read availability, outcomes, waits, and complete expected key set; a determinism assertion evaluates the identical snapshot repeatedly and compares canonical output;
- a plan harness may advance through an ordered sequence of synthetic durable snapshots — including facts, terminal command outcomes, and closed child memberships — and return or assert the canonical declarations, dependencies, consulted inputs, and read-availability classifications after each transition, without running workers or external effects;
- a plan-routing assertion mode evaluates selected transitions that normal consulted-input routing would skip and verifies that the canonical output and reads remain unchanged;
- a coordinator harness delivers the durable start activation and events in order, allowing `OnStart`, state changes, outputs, retry, and completion decisions to be asserted.

Integration behavior is verified against real PostgreSQL: concurrent claims, lease expiry and fencing, cancellation races, crash recovery at every commit boundary, publish-before-declare and declare-before-publish ordering, all-or-nothing worker fan-out, authoritative child reads, batched worker dependency results, repeated plan evaluation creating no duplicate commands, unknown dependency rejection, terminally unavailable results not blocking failure, failure branches surviving fail-fast, `Await` expiry, and rolling deployments with divergent registered versions.

## 22. Acceptance criteria

Milestone 1 is complete when:

- an execution can be started, traced, published to, and cancelled through the documented API;
- `*Runtime` can be passed directly wherever `Client` is accepted, without calling `rt.Client()`;
- binding a definition with `With(runtime)` returns the same static definition type, produces a concurrency-safe executable copy, and does not mutate or register the original;
- calling `With` on an already bound definition replaces the client only in the returned copy, enabling an explicit per-call runtime override without mutating a shared definition;
- calling `.Execute` on an unbound definition returns `ErrInvalid` without writing;
- the same definition may be bound independently to multiple runtimes, while a transaction-bound value cannot execute after its transaction closes;
- `Command.Execute` durably queues one typed root command and returns immediately without requiring a plan or coordinator;
- `PlanDef.Execute` durably creates the execution and atomically enqueues every command made ready by its initial pure evaluation;
- `Coordinator.Execute` durably creates the instance and queues its start activation without invoking `OnStart` inline;
- a direct execution remains running through every required spawned descendant, then completes without relying on temporary quiescence;
- a direct required-command failure produces the configured fail-fast or settle-all result without plan evaluation;
- the worked example in §5 compiles and runs against PostgreSQL;
- a mistyped command or event reference, or a wrong payload, result, or event payload type, fails to compile;
- every command that ends records exactly one event describing how it ended, with success carrying its typed result and transient attempt failures excluded;
- a worker that successfully spawns several children commits the complete direct-child set, the event recording parent success, additionally emitted events, and `OnCommit` writes atomically;
- a worker that errors, panics, loses its lease, or exceeds the configured plan-command limit after staging children commits none of them;
- equivalent repeated child keys within one handler decision coalesce, conflicting content fails atomically, and no parent retry can duplicate a committed child;
- spawned children are required by default, `flow.Optional()` removes them from execution outcome, and both classifications remain visible in `Trace`;
- `Children` returns the authoritative deterministically ordered direct-child keys after successful membership closure, including a successful empty fan-out, without relying on an application result payload;
- `ResultOf` in a worker returns typed immutable results only for commands explicitly named as that command's dependencies, rejects arbitrary graph reads, and can serve repeated reads without one database query per call;
- re-evaluating a plan many times creates each declared command exactly once;
- a plan branch appears only once the fact deciding it exists, and never withdraws work already declared;
- every command made runnable by one plan evaluation is created in a single transaction;
- a command with `Await` becomes runnable when its fact arrives, whether that fact was published before or after the command was declared;
- an awaited fact that never arrives expires its command within the declared bound, and dependents resolve through the failure branch;
- a failure branch declared with `AfterFailed` runs to completion under fail-fast, and the execution becomes terminal only after it resolves;
- a plan-driven execution succeeds exactly when every declared command is terminal, nothing remains pending or awaited, no read is temporarily unavailable, and re-evaluation declares nothing new; it never succeeds on temporary quiescence;
- a required terminal command failure reaches terminal execution failure after its explicit failure-handling subgraph resolves, even when the latest plan evaluation contains temporarily unavailable reads;
- a plan that branches on a fact which never arrives keeps its execution running until its deadline rather than reporting success;
- `Children`, `Result`, and `Outcome` read commands declared earlier in the evaluation or durably created by `Do`, `Spawn`, or `Issue`, while any other read key is rejected as a plan defect;
- plan reads distinguish available, temporarily unavailable, and permanently unavailable inputs internally: `Result` becomes available only on success, `Outcome` on any terminal state, and an unsuccessful terminal `Result` cannot block completion as though it might later succeed;
- after each evaluation, dependency keys are validated against durable commands plus every declaration in that evaluation, so forward references work and a nonexistent key fails immediately as a plan defect;
- a plan panic, conflicting declaration, or invalid read records `PlanFailed` and fails the execution without consuming worker retry budget or rerunning accepted work;
- the plan determinism harness detects different declarations or consulted reads from an identical snapshot, and fragment tests can assert the complete intended key set;
- the database-free plan harness can advance synthetic facts, terminal outcomes, and closed child memberships and expose canonical declarations, dependencies, consulted inputs, and read availability after each transition without executing workers;
- plan evaluation at the documented command ceiling stays within its benchmarked budget; events whose names the latest evaluation did not consult trigger no normal evaluation, a later consulted transition still exposes any stored fact to a newly reached branch, and routing assertion mode detects if a skipped evaluation would differ;
- a command result, emitted events, spawned children, plan-created commands, dependency resolution, and application writes commit atomically or not at all;
- a stalled worker cannot commit after losing its lease;
- an event published before its plan-declared command or coordinator existed is still observed by it;
- event positions are total only within one execution, and no API or projection implies a total order across executions;
- an idempotent republish of a stored event succeeds after the execution becomes terminal, while a genuinely new event is rejected;
- crash at any commit boundary leaves the execution recoverable and internally consistent;
- workers registering different `(name, version)` sets share a database without failing each other's work;
- `Trace` returns both what happened and what the execution is waiting for, including parent-child edges, every final command state, current running state, and attempt start history in one call, without a `CommandStarted` event;
- worker and plan unit tests run without a database.
