---
status: draft
---

# Functional Spec: flow

## 1. Purpose

`flow` is a PostgreSQL-backed Go library for durable, event-driven execution.

Its core loop is:

```text
command  →  worker  →  event
                     └→ optional direct child commands
```

A command is a durable instruction to perform work. A worker handles the command and performs that work. Events record facts about what happened. Plans react to recorded events and coordinate the overall execution. Workers may spawn child commands when their work reveals more work.

There is one event concept. Conceptually, workers emit events. In the API, returning `(result, nil)` automatically records an immutable event carrying the command's typed result; workers call `flow.Emit` only for additional application facts. Failure, cancellation, expiry, and skipping are also recorded as ordinary events. Every command that ends therefore produces exactly one final fact, so progress is observable and "wait for this work" and "wait for this fact" use the same durable mechanism. A retryable error records attempt history but no final event because the command has not finished.

Commands and events belong to an **execution**. What high-level work an execution needs is normally declared by a **plan** — a pure function re-evaluated over all relevant events and command results recorded so far. "React" does not mean that the plan receives one event callback. A worker may atomically **spawn** a bounded set of direct child commands when performing one command reveals more work. Where membership is open-ended or a plan cannot express the logic, a hand-written **coordinator** reacts to events directly.

Commands are the executable vertices of the runtime graph, events explain progression, and causation supplies the edges. The graph is a projection of durable history, extended by the plan's record of work that is declared but not yet runnable.

## 2. Scope

### 2.1 PostgreSQL only

PostgreSQL is the sole required backend. `flow` has no broker abstraction and does not attempt to make PostgreSQL, Kafka, and SQS interchangeable.

This is a product feature: application writes, command completion, plan reconciliation, emitted events, and spawned commands can share one transaction. PostgreSQL notifications may reduce latency, but polling is always sufficient for correctness.

### 2.2 Milestone 1

- durable executions with idempotent start, deadlines, and explicit final states;
- typed, versioned commands carrying both a payload type and a **result** type;
- exactly one event recording how each command ends, with successful worker results recorded automatically as typed events;
- worker registration, command scheduling, leases, attempts, retries, timeouts, and fencing;
- bounded worker-spawned child commands committed atomically with the event recording the parent's success;
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
| **Event** | An immutable fact in an execution's ordered log, never destructively consumed. A successful worker return records one automatically; the runtime records one when a command ends another way; workers and applications may record additional facts. |
| **Plan** | A pure function declaring the commands an execution needs and what each one waits for. |
| **Spawn** | A worker or coordinator staging an asynchronous command; a worker-spawned command is a direct child of the current command. |
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

func (c Command[A, R]) Done() Event[R]        // event recorded when this command succeeds
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

A command declares both what it takes and what it returns. `Command.Done()` is the event carrying that result; it shares the command's name and version, needs no separate declaration, and is what `After` waits on. It is an ordinary `Event[R]`, not a separate event category.

Names are stable durable identifiers. Every definition carries an explicit positive integer version; `0` is invalid. A `(name, version)` pair has immutable payload and result meaning once used, while its handler implementation may change and redeploy freely. A runtime claims only work for pairs it has registered; unknown pairs stay pending for a compatible process and consume no retry budget.

Registration is explicit and runtime-local; definitions mutate no package-global state. A runtime rejects duplicate workers for one command pair and duplicate handlers for one event pair within a coordinator.

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

`Result` and `Outcome` address a command declared earlier with `Do` in the current evaluation or already present in the execution from `Do`, `Spawn`, or `Issue`. The supplied command definition must match the key's stored name and version or evaluation fails as a plan defect. `Result` returns true only for success; `Outcome` returns true for any final state. They are typed views over command state and its recorded event, not event abstractions of their own. A key that is neither currently declared nor durably present is also a plan defect rather than an absent read. Dependency builders likewise name command keys in the execution-wide key namespace, not only commands created by the current plan evaluation.

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

Spawned commands are required by default and therefore determine execution outcome. `flow.Optional()` makes one spawned command optional. A worker may return the stable child keys in its typed result when the plan needs to join or collect their results; the runtime's authoritative parent-child relationship is derived from the staged `Spawn` calls, not from an application payload.

Requirements:

- worker and coordinator outputs are buffered until successful return;
- output payloads are type-checked through their definitions;
- `Emit` and `Spawn` are available to both worker and coordinator scopes; a plan uses `Do`, never `Spawn`;
- duplicate equivalent `Spawn` calls for one key within one handler decision coalesce, while different content for one key returns `ErrConflict` and commits nothing;
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
| Plans | `Do`, `Fact`, `Facts`, `Result`, `Outcome`, plus 10 command builder methods |
| Execution | `Start`, `StartWith`, `Issue`, `Publish`, `CancelExecution`, `CancelCommand` |
| Handler output | `Emit`, `Spawn`, `Optional`, `OnCommit`, `Info`, `SucceedExecution`, `FailExecution` |
| Inspection | `GetExecution`, `LookupExecution`, `Trace`, `History`, `ListExecutions`, `AwaitExecution`, `ResultOf` |
| Errors | `Permanent`, `RetryAfter` |

The common path is `DefineCommand`, `DefineEvent`, `DefinePlan`, `Handle`, `Do`, `Spawn`, `Start`, `Publish`, and `Run`. `Do` declares high-level work from a plan; `Spawn` is learned only by a worker that discovers direct children. Coordinators, cancellation, transaction composition, and policy customization form the operational surface.

## 5. Worked example

```go
// ---- definitions ----

var (
    PrepareReport = flow.DefineCommand[PrepareArgs, PrepareResult]("prepare_report", 1)
    AnalyzePart   = flow.DefineCommand[AnalysisArgs, AnalysisResult](
        "analyze_report_part", 1,
        flow.WithMaxAttempts(5),
        flow.WithTimeout(10*time.Minute),
    )
    GenerateReport        = flow.DefineCommand[GenerateArgs, ReportResult]("generate_report", 1)
    RecordAnalysisFailure = flow.DefineCommand[FailureArgs, flow.None]("record_analysis_failure", 1)
)

type PrepareResult struct {
    AnalysisKeys []string
}

// ---- the plan: high-level progression and joins ----

var ReportExecution = flow.DefinePlan[ReportArgs]("report_execution", 1, planReport)

func planReport(p *flow.Plan, args ReportArgs) {
    flow.Do(p, "prepare", PrepareReport, PrepareArgs{CompanyID: args.CompanyID})

    prepared, ok := flow.Result(p, "prepare", PrepareReport)
    if !ok {
        return
    }

    for _, key := range prepared.AnalysisKeys {
        // Declare every failure branch before waiting on any one result.
        flow.Do(
            p, "record-failure/"+key,
            RecordAnalysisFailure,
            FailureArgs{AnalysisKey: key},
        ).AfterFailed(key)
    }

    results := make([]AnalysisResult, 0, len(prepared.AnalysisKeys))
    for _, key := range prepared.AnalysisKeys {
        // This command was spawned by prepareReport. Its key shares the
        // execution-wide namespace used by commands declared with Do.
        result, succeeded := flow.Result(p, key, AnalyzePart)
        if !succeeded {
            return
        }
        results = append(results, result)
    }

    flow.Do(p, "generate", GenerateReport, GenerateArgs{
        CompanyID: args.CompanyID,
        Analyses:  results,
    }).After(prepared.AnalysisKeys...)
}

// ---- a worker may expand one command into bounded direct children ----

func prepareReport(ctx context.Context, w *flow.Work[PrepareArgs]) (PrepareResult, error) {
    analyses, err := determineAnalyses(ctx, w.Payload.CompanyID)
    if err != nil {
        return PrepareResult{}, err
    }

    keys := make([]string, 0, len(analyses))
    for _, analysis := range analyses {
        key := "analysis/" + analysis.ID
        if err := flow.Spawn(w, key, AnalyzePart, analysis.Args); err != nil {
            return PrepareResult{}, err
        }
        keys = append(keys, key)
    }

    w.OnCommit(func(ctx context.Context, tx pgx.Tx) error {
        return reports.MarkPrepared(ctx, tx, w.Payload.CompanyID, len(keys))
    })
    return PrepareResult{AnalysisKeys: keys}, nil
}

func analyzePart(ctx context.Context, w *flow.Work[AnalysisArgs]) (AnalysisResult, error) {
    return analyzers.For(w.Payload.Kind).Analyze(ctx, w.Payload)
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
h, err := flow.Start(
    ctx, c,
    ReportExecution,
    reportID,
    ReportArgs{CompanyID: companyID},
    flow.WithFailFast(false), // let independent analyses settle before final failure
)
```

The first plan evaluation declares only `prepare`. Its worker discovers the complete analysis membership and stages every child with `Spawn`; the children and the event returned by `PrepareReport.Done()` become visible atomically. Analysis workers run in parallel. Each successful result records the event returned by `AnalyzePart.Done()`, which wakes plan reconciliation. Only after every child result exists does the plan declare `generate`.

If the preparation handler fails partway through staging, no child is committed. If one analysis exhausts retries, it records `CommandFailed`, the corresponding `AfterFailed` branch runs, `generate` is never declared, and the execution ultimately fails after the remaining analyses settle. The plan needs no coordinator because this fan-out membership closes with one successful parent return.

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

An execution succeeds when every command is terminal, nothing remains pending or awaited, and no required command ended `failed`, `expired`, or `cancelled`. `skipped` resolves graph structure but never fails an execution by itself. Commands made optional through the plan builder or spawned with `flow.Optional()` never determine the outcome. Plan-declared, worker-spawned, coordinator-spawned, and externally issued commands otherwise participate in the same outcome rules.

**Fail-fast is the default and does not suppress failure handling.** When a required command becomes `failed`, `expired`, or `cancelled`, one transaction, in this order:

1. records that command's final state;
2. **resolves every dependency naming it**, so commands selected by `AfterFailed` or `AfterSettled` become runnable and commands that required its success become `skipped`;
3. marks the execution `failing` and cancels remaining non-terminal work **except** the failure-handling subgraph — the commands just made runnable, their descendants, and anything already running;
4. leaves the execution terminal only once that failure-handling subgraph resolves.

This is what makes refunds, compensation, reconciliation, and cleanup expressible; a command selected by `AfterFailed` is guaranteed its chance to run. Execution-level cancellation is different: it cancels everything and selects no failure branches.

`WithFailFast(false)` lets unrelated branches finish, and the outcome is computed once every command is terminal.

### 6.4 Completion

For a plan-driven execution, completion is derived: every command is terminal, no plan-declared command is pending or awaiting an unarrived fact, the latest evaluation consulted no input that did not exist (§10.1), and re-evaluating the plan declares nothing new. Because spawned children increment the same open-command count and the plan records what is still expected, neither unfinished fan-out nor temporary quiescence can report false completion.

For a coordinator-driven execution, completion is an explicit `SucceedExecution` or `FailExecution` decision, and temporary quiescence never completes it.

Success may commit only when no command is non-terminal and nothing staged by the same decision would make that false. On completion the coordinator is closed and pending work cancelled atomically, and one immutable `ExecutionSucceeded` or `ExecutionFailed` event is recorded.

### 6.5 Deadlines

An execution carries a deadline, defaulting to 30 days, overridable with `WithExecutionDeadline` and removable with `WithoutExecutionDeadline` — which opts out of the bounded-completion guarantee (§12.6). On expiry the execution becomes `expired`, non-terminal commands are cancelled, and the reason is recorded. Every command and wait deadline is capped by the execution deadline, so nothing outlives its execution.

### 6.6 External additions

`Issue` and `Publish` may add a command or a fact to a running execution. They are rejected for a terminal execution unless the operation is an idempotent retry of an already-stored key with equivalent content.

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

### 7.4 Bounded child spawning

`Spawn` stages a command for asynchronous delivery. It does not invoke a handler, block for a result, or open a nested transaction.

For a worker handler:

- every spawned command is a direct child of the current logical command;
- stable child keys are unique across the execution and remain stable across parent retries;
- all staged children, emitted application events, the event recording the parent's success, and `OnCommit` writes commit atomically on successful return;
- if the handler returns an error, panics, loses its lease, is cancelled, or the settle transaction rolls back, none of its staged children become visible;
- after the parent succeeds, its direct-child membership is closed permanently, although the children remain independently active;
- spawned children are required unless created with `flow.Optional()`.

The runtime derives authoritative child membership from command causation. A typed parent result may carry child keys so the plan can collect results or declare joins, but correctness and tracing never infer membership from arbitrary result payload fields.

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

- **all must succeed** — spawn required children (the default) and collect them with `Result`; a terminal child failure prevents the success join and fails the execution;
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

### 9.6 Replay and recovery boundary

`flow` preserves replayable orchestration history without being a strict event-sourced runtime. Immutable command rows record requested work and parentage; events record how commands ended and any additional application facts; attempts record transient execution mechanics; materialized command and execution states make claiming and inspection efficient.

Plans may be re-evaluated from the retained arguments, command states, results, and events, and inspection projections may be rebuilt from commands, dependencies, attempts, events, and causation. Recovery never replays arbitrary Go handlers or repeats their historical external side effects. A command row is itself the durable record that the command was issued, so Milestone 1 does not add a redundant `CommandIssued` event.

## 10. Plans

### 10.1 The plan function

A plan is a pure function of the execution's root arguments and the events, command states, and results recorded so far. It declares commands; it never performs work. It must not do I/O, read clocks, use randomness, start goroutines, or depend on mutable globals, and must be deterministic given identical inputs. Although a plan reacts to events, it does not receive one event callback; each evaluation sees the relevant durable snapshot accumulated so far.

Ordinary Go cannot be sandboxed completely. `flow` therefore enforces purity by capability and verification: a plan receives no context, client, database, transaction, clock, or worker scope; declarations are reconciled by canonical identity; panics and conflicts are plan defects; and `flowtest` can evaluate the same snapshot repeatedly and compare declarations and reads. A debug runtime option may perform the same double evaluation. A plan defect records `PlanFailed`, fails the execution, and cancels outstanding work; it never consumes a worker's retry budget or reruns a worker whose result was already accepted.

The terminal plan-defect rule also applies to the initial evaluation in `Start` and evaluations caused by ingress. The execution and accepted triggering command or fact remain durable for inspection; the operation returns a typed error carrying the `ExecutionID`, and an idempotent retry finds the same terminal execution.

`Fact` and `Facts` read events already recorded in this execution. `Result` reads a successful command's typed result. `Outcome` reads any terminal result or structured failure, including for worker-spawned commands. A branch that depends on runtime information is an ordinary Go conditional over those reads.

**Reads are recorded.** Every `Fact`, `Facts`, `Result`, and `Outcome` call during an evaluation is registered as an input that evaluation consulted. Because the plan is pure, its declared output is a function of exactly those reads plus the root arguments — which makes the consulted set both a correctness signal (§10.2) and the basis for skipping needless evaluations (§10.3).

**`Result` and `Outcome` may only read a key declared earlier with `Do` in the current evaluation or an existing command key.** A durable command may have been created by `Do`, `Spawn`, or `Issue`. Reading any other key, or supplying a command definition whose name and version do not match the key, returns `ErrInvalid`; it is a plan defect, not a runtime condition. A newly declared or existing non-succeeded command makes `Result` return `(zero, false)`. A newly declared or existing non-terminal command makes `Outcome` return `(zero, false)`.

`Result` remains absent after an unsuccessful final state because no value of `R` exists. If failure is an accepted branch — especially for an optional child — the plan must use `Outcome` or a keyed command selected with `AfterFailed` / `AfterSettled` rather than wait forever for `Result`.

Reading a command's result or outcome does not by itself create a dependency edge. A command whose arguments derive from another command should also name it with `After` or `AfterSettled`, so the trace can explain the ordering. Correctness does not depend on this — a command consulted before it completed keeps the execution incomplete — but inspection quality does.

#### Consulted-but-absent reads block completion

A read that finds nothing — a fact not yet published, a command not yet succeeded for `Result`, or a command not yet terminal for `Outcome` — means the plan's output may differ once that input exists. An execution therefore **cannot complete while its most recent evaluation consulted an input that did not exist.**

This closes an otherwise silent hole. Given:

```go
if route, ok := flow.Fact(p, RouteSelected); ok {
    flow.Do(p, "dest", SendTxn, destTxn(args, route)).After("origin")
}
```

if `RouteSelected` never arrives, no `dest` command is ever declared. Without this rule, an execution whose declared commands had all succeeded would report success while the destination transaction was never sent. With it, the consulted-but-absent `RouteSelected` keeps the execution running until the fact arrives or the execution deadline expires.

The rule is automatic and cannot be forgotten, because it derives from the read itself rather than from a separate declaration. Absent reads are bounded by the execution deadline; a plan wanting a tighter bound declares a command with `Await(...).Within(...)` instead of branching on a bare `Fact`.

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

The consulted-input set from the latest evaluation is persisted alongside the declared set. It determines both whether the execution may complete (§10.1) and when the plan must next be evaluated (§10.3).

### 10.3 When the plan is evaluated

A plan's declared output is a pure function of its root arguments and the durable events and final command states it consulted. Evaluation is therefore required only when one of those can have changed:

- at execution start;
- when an event arrives whose name the latest evaluation consulted, or which the plan has never had the chance to consult;
- when a final event is appended for a command read by the plan or named by one of its dependencies.

Claim, lease renewal, `running`, and `retry_wait` transitions do not trigger plan evaluation because no plan API can observe them. The event recorded when the command ends does.

Events of names no evaluation has ever consulted cannot change the plan's output and do not trigger evaluation. Because plans are pure, skipping is sound rather than an optimization that risks divergence.

Evaluation is idempotent, so an implementation may always evaluate more often than required — including on every event — without changing behavior.

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

The limit is **1,000 total commands per plan-driven execution**, including commands created by `Do`, `Spawn`, and `Issue`, with 100 dependencies per plan-declared command. The limit is validated against the complete staged batch before any child or plan command is inserted, so a fan-out cannot commit partially at the ceiling. At that ceiling an execution performs on the order of a million narrow row reads across its lifetime, which is acceptable; an order of magnitude higher is not. This is well above the intended workload — executions of tens of commands — and larger fan-outs belong in separate executions, one per unit of work, with a parent execution coordinating them.

Coordinator-driven executions are not re-evaluated and are not bound by this limit; they are bounded only by inbox delivery cost.

Architecture must make the per-evaluation state load a single narrow indexed query and must benchmark evaluation at the documented ceiling.

## 11. Coordinators

A coordinator is durable typed state that reacts to events. Plans and direct-child records already handle bounded joins; a coordinator exists for open-ended membership, cycles, and multi-event decisions that need durable mutable orchestration memory.

**One coordinator drives an execution**: either the plan, or an application-defined coordinator started with `StartWith`. Child coordinators are a later capability.

### 11.1 Definition and instance

A definition has a stable name, positive version, typed state schema, and exact typed event subscriptions declared with `On`. Its instance holds typed canonical state, a durable inbox position, and a lifecycle of `active → completed | failed | cancelled`.

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

That one transaction commits the event carrying the command result, its additionally emitted events, its complete staged child set, plan reconciliation and every command that reconciliation creates, dependency resolution, execution outcome transitions, history, and `OnCommit` writes. If ordinary settlement fails, none commits and the command is retried.

A recovered plan panic, nondeterministic conflict, or invalid plan read is different: the accepted worker result and its staged outputs commit, `PlanFailed` and `ExecutionFailed` are appended, and outstanding commands are cancelled. A plan defect never turns successful application work into another worker attempt.

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

Outputs created inside handlers automatically inherit execution identity and causation: worker events and spawned direct children are caused by the current command; plan outputs are caused by the durable transition that triggered evaluation; coordinator outputs are caused by the event being processed; an event recording a command's final state identifies that command and transition. Callers cannot forge an origin that contradicts the active handler scope.

## 13. Cancellation, deadlines, and terminal races

`CancelExecution` marks the execution `cancelled`, cancels non-terminal commands, closes the coordinator, and records `ExecutionCancelled`. `CancelCommand` cancels one command; if it is required, the execution fails under §6.3 including its failure-handling branches.

Cancellation and completion race on the execution row, and whichever commits first wins. A handler whose command was cancelled cannot commit its result, staged outputs, or `OnCommit` writes.

Cancellation cannot undo external side effects already performed and cannot forcibly stop a non-cooperative goroutine; fencing only guarantees such a goroutine commits nothing.

Cancelling an already-cancelled target is idempotent; cancelling a differently-terminal target returns `ErrTerminal`.

## 14. Serialization, encoding, and limits

Command payloads, command results, event payloads, coordinator state, and all identity comparisons use deterministic canonical JSON. Idempotency compares canonical stored bytes, not caller memory layout or database formatting. The architecture defines the encoder and its treatment of custom marshalers; the functional requirement is that the same logical value always produces the same identity bytes.

Payload, state, total-command-count, and dependency-count limits are enforced against the complete staged transaction before any durable write. Violations return `ErrPayloadTooLarge` or `ErrInvalid` with no partial effect.

## 15. Inspection and graph projection

### 15.1 Execution trace

`Trace` returns, in one call:

- the execution: type, key, status, deadline, timings, outcome, and failure;
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
| total commands per plan-driven execution | 1,000 |
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

The library ships a test package so workers and plans are testable without a database:

- a worker is an ordinary function — given a payload, assert its returned result or error, its staged events and spawned children, direct-child keys, optional classification, and registered application writes;
- a plan is a pure function — given root arguments, facts, and command states, assert its declarations, dependencies, reads, outcomes, and waits; a determinism assertion evaluates the identical snapshot repeatedly and compares canonical output.

Integration behavior is verified against real PostgreSQL: concurrent claims, lease expiry and fencing, cancellation races, crash recovery at every commit boundary, publish-before-declare and declare-before-publish ordering, all-or-nothing worker fan-out, repeated plan evaluation creating no duplicate commands, failure branches surviving fail-fast, `Await` expiry, and rolling deployments with divergent registered versions.

## 22. Acceptance criteria

Milestone 1 is complete when:

- an execution can be started, traced, published to, and cancelled through the documented API;
- the worked example in §5 compiles and runs against PostgreSQL;
- a mistyped command or event reference, or a wrong payload, result, or event payload type, fails to compile;
- every command that ends records exactly one event describing how it ended, with success carrying its typed result and transient attempt failures excluded;
- a worker that successfully spawns several children commits the complete direct-child set, the event recording parent success, additionally emitted events, and `OnCommit` writes atomically;
- a worker that errors, panics, loses its lease, or exceeds the total-command limit after staging children commits none of them;
- equivalent repeated child keys within one handler decision coalesce, conflicting content fails atomically, and no parent retry can duplicate a committed child;
- spawned children are required by default, `flow.Optional()` removes them from execution outcome, and both classifications remain visible in `Trace`;
- re-evaluating a plan many times creates each declared command exactly once;
- a plan branch appears only once the fact deciding it exists, and never withdraws work already declared;
- every command made runnable by one plan evaluation is created in a single transaction;
- a command with `Await` becomes runnable when its fact arrives, whether that fact was published before or after the command was declared;
- an awaited fact that never arrives expires its command within the declared bound, and dependents resolve through the failure branch;
- a failure branch declared with `AfterFailed` runs to completion under fail-fast, and the execution becomes terminal only after it resolves;
- a plan-driven execution completes exactly when nothing is declared, pending, awaited, or consulted-but-absent, and never on temporary quiescence;
- a plan that branches on a fact which never arrives keeps its execution running until its deadline rather than reporting success;
- `Result` and `Outcome` read commands declared earlier in the evaluation or durably created by `Do`, `Spawn`, or `Issue`, while any other key is rejected as a plan defect;
- `Result` becomes available only on success, while `Outcome` becomes available for success, failure, expiry, cancellation, or skip;
- a plan panic, conflicting declaration, or invalid read records `PlanFailed` and fails the execution without consuming worker retry budget or rerunning accepted work;
- the plan determinism harness detects different declarations or consulted reads from an identical snapshot;
- plan evaluation at the documented command ceiling stays within its benchmarked budget, and events of never-consulted names trigger no evaluation;
- a command result, emitted events, spawned children, plan-created commands, dependency resolution, and application writes commit atomically or not at all;
- a stalled worker cannot commit after losing its lease;
- an event published before its plan-declared command or coordinator existed is still observed by it;
- an idempotent republish of a stored event succeeds after the execution becomes terminal, while a genuinely new event is rejected;
- crash at any commit boundary leaves the execution recoverable and internally consistent;
- workers registering different `(name, version)` sets share a database without failing each other's work;
- `Trace` returns both what happened and what the execution is waiting for, including parent-child edges and every final command state, in one call;
- worker and plan unit tests run without a database.
