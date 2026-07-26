---
status: complete
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

There is one developer-facing event concept. Conceptually, workers emit events. In the API, returning `(result, nil)` automatically records an immutable event carrying the command's typed result; workers call `flow.Emit` only for additional application facts. Failure, cancellation, expiry, and skipping are also recorded as ordinary events. Every command that ends therefore produces exactly one final fact, so progress is observable and "wait for this work" and "wait for this fact" use the same durable mechanism. A retryable error records attempt history but no final event because the command has not finished.

Every execution also has one immutable ordered **journal**. It contains the broader durable history needed to explain the run: execution transitions, creation of every command, attempt lifecycle, application and terminal events, dependencies, parentage, and causation. An `Event[T]` is a plan- and coordinator-visible fact recorded in that journal; `CommandCreated`, `AttemptStarted`, and similar operational entries are history, not additional developer-facing event concepts. The journal is the complete account of execution decisions and lifecycle transitions accepted by `flow`; it does not claim to observe an external effect whose result never reached PostgreSQL.

Commands and events belong to an **execution**. A plan is optional. The simplest execution starts one root command directly; its workers may form a bounded command tree with `Spawn`, and the execution finishes when that tree settles. When progression requires dependencies, joins, waits, or branches across commands, a **plan** declares that orchestration as a pure function re-evaluated over all relevant events and command results recorded so far. "React" does not mean that the plan receives one event callback. Where membership is open-ended or a plan cannot express the logic, a hand-written **coordinator** reacts to events directly.

Long-running durable agents use the same model rather than a separate framework. One agent run or bounded episode is an execution, its coordinator is the durable control loop, and model calls, tools, and bounded sub-agent tasks are commands. A long-lived agent therefore progresses through bounded worker invocations and durable coordinator decisions instead of retaining one in-memory worker loop. Large transcripts and artifacts remain application data referenced by command arguments, results, events, and coordinator state.

Application code begins any mode by binding its command, plan, or coordinator definition with `With(runtime)` and calling `.Execute` on the returned immutable copy. The call durably schedules the execution and returns an `ExecutionHandle`; it never runs a worker or coordinator handler inline.

Commands are the executable vertices of the runtime graph, events explain progression, and causation supplies the edges. Because command creation itself is journaled, the graph is reconstructible from retained durable history, extended by the plan's record of work that is declared but not yet runnable.

## 2. Scope

### 2.1 PostgreSQL only

PostgreSQL is the sole required backend. `flow` has no broker abstraction and does not attempt to make PostgreSQL, Kafka, and SQS interchangeable.

This is a product feature: a declared short application commit function, command completion, plan reconciliation, emitted events, and spawned commands can share one transaction. PostgreSQL notifications may reduce latency, but polling is always sufficient for correctness.

### 2.2 Milestone 1

- durable executions with idempotent start, deadlines, and explicit final states;
- direct root-command execution requiring no plan or coordinator;
- one `.Execute` verb on command, plan, and coordinator definitions, using immutable runtime binding through `With`;
- typed, versioned commands carrying both a payload type and a **result** type;
- one immutable ordered journal entry for every accepted command creation, including its payload, origin, parent where applicable, dependencies, classification, and causation;
- exactly one event recording how each command ends, with successful worker results recorded automatically as typed events;
- worker registration, command scheduling, leases, attempts, retries, timeouts, and fencing;
- immutable declarative retry policies bounded by attempts, total elapsed retry duration, or both, with a PostgreSQL-time budget anchor that retries and crash recovery never reset;
- bounded worker-spawned child commands committed atomically with the event recording the parent's success;
- authoritative plan reads of closed direct-child membership, without duplicating membership into application result payloads;
- typed, versioned events, including facts published from outside the execution;
- **plans**: declarative command graphs reconciled by key, with dependencies, waits, fan-out, joins, and failure branches;
- hand-written coordinators with durable typed state, ordered event inboxes, and typed observation of every terminal command outcome;
- historical matching-event delivery to plans and coordinators;
- plan-declared delays and delayed worker or coordinator spawns as the one-shot durable timer primitive;
- one configurable total-command safety ceiling for every execution mode, distinct from queue capacity and concurrency;
- execution and command cancellation;
- optional declared worker commit functions for short application-table writes that must commit atomically with command success;
- atomic worker, plan, and coordinator outputs, including writes made by declared worker commit functions;
- inspection, causal trace, immutable history, listing, and waiting;
- embedded migrations;
- a database-free worker, plan, and coordinator test harness;
- vendor-neutral observability hooks.

### 2.3 Near-term follow-ons

- child executions for independently durable sub-agents and other nested work, with an idempotent parent link, a separate execution-scoped journal, exactly one terminal outcome delivered back across an explicit boundary, and defined completion, failure, cancellation, and recursion policies;
- an operational UI for execution timelines, causal graphs, pending waits, retries, and failures;
- OpenTelemetry, metrics, and structured-logging adapters;
- administrative retry, execution fork, explicit policy amendment, repair, and compensation tools;
- archival and configurable terminal-journal retention, including two-stage removal of bulky payloads before causal skeletons.

### 2.4 Later capabilities

- cancel-remaining join policies;
- arbitrary subtree cancellation;
- recurring schedules;
- cross-execution subscriptions and event export to Kafka or analytics systems through an explicit idempotent boundary that preserves the source `(ExecutionID, position)` and neither merges execution logs nor promises cross-execution order (§9.4);
- plan simulation and dry-run tooling that uses the exact plan version and a retained execution snapshot to preview declarations and reads after historical or candidate transitions, without executing workers or external effects (§9.6);
- optional soft local affinity with bounded preference for the replica that starts an execution and automatic takeover by another replica;
- backend implementations other than PostgreSQL;
- multi-region execution.

### 2.5 Explicit non-goals

- a general-purpose message broker or event-streaming platform, or implicit cross-execution pub/sub inside an execution journal;
- a database-wide journal, global position, or total ordering guarantee across executions;
- framework-owned copies of application/domain state;
- deterministic replay of arbitrary Go code;
- exactly-once external side effects;
- distributed ACID transactions with external services;
- executable pinning to a deployed build;
- hard replica pinning or correctness that depends on instance-local memory;
- a visual workflow designer in the core package.
- agent-specific `Agent`, `Subagent`, `Tool`, `Memory`, or conversation abstractions in the core package; applications model them using commands, events, coordinators, and application-owned data.

## 3. Core terminology

| Term | Meaning |
|---|---|
| **Execution** | One durable run, identified by `ExecutionID` and an idempotency key. |
| **Command** | One immutable logical request for work, with typed payload and typed result. Keeps one `CommandID` across attempts. |
| **Attempt** | One invocation of a command handler, identified separately from the command. |
| **Worker** | A registered typed handler for one command name and version. |
| **Event** | An immutable fact in an execution's ordered journal, never destructively consumed. A successful worker return records one automatically; the runtime records one when a command ends another way; workers and applications may record additional facts. |
| **Journal** | The complete immutable per-execution sequence of accepted execution decisions and lifecycle transitions. It includes command creation, attempts, events, outcomes, and causation; only event entries are exposed as `Event[T]` facts. |
| **Plan** | An optional pure function declaring the commands an execution needs and what each one waits for. Used for dependencies, joins, waits, and branching across commands. |
| **Spawn** | A worker or coordinator staging an asynchronous command; a worker-spawned command is a direct child of the current command. |
| **Coordinator** | Durable typed state reacting to application events and typed terminal command outcomes for orchestration that is open-ended or unsuitable for a plan. |
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

func WithMaxCommandsPerExecution(int) Option // default 1,000; 0 disables
```

`New` validates configuration and schema compatibility, starts no goroutines, and never migrates implicitly. `*Runtime` implements `Client`, so it can be passed directly to every operation whether or not `Run` is called. Registrations are accepted only before `Run`, which may be called once.

`WithMaxCommandsPerExecution` configures the default ceiling accepted by executions started through that runtime. The chosen value is copied onto the execution and recorded in `ExecutionStarted`; every replica subsequently enforces the stored value rather than its own runtime default. Changing the runtime option affects only newly created executions. An existing execution, including one returned by an idempotent repeated `.Execute`, keeps its accepted ceiling.

`Client` is a small sealed capability implemented by `*Runtime` and the transaction-scoped value returned by `InTx`. Application code does not construct one. API processes create a runtime and simply do not call `Run`; mixed processes and specialized worker pools may use the same type against one database.

### 4.2 Typed definitions

```go
type None = struct{}

type Command[A, R any]  struct{}
type Event[T any]       struct{}
type PlanDef[A any]     struct{}
type Coordinator[S any] struct{}
type RetryPolicy        struct{ /* immutable value sealed by flow */ }

func DefineCommand[A, R any](name string, version int, opts ...CommandOption) Command[A, R]
func DefineEvent[T any](name string, version int) Event[T]

func (c Command[A, R]) Done() Event[R]        // event recorded when this command succeeds
func (c Command[A, R]) Name() string
func (c Command[A, R]) Version() int
func (c Command[A, R]) With(client Client) Command[A, R]

func Handle[A, R any](
    cmd Command[A, R],
    worker func(context.Context, *Work[A]) (R, error),
    opts ...WorkerOption[A, R],
) Registration

type Commit[A, R any] struct {
    Args   A
    Result R
    Info   CommandInfo
}

type Tx interface{ /* narrow Exec, Query, and QueryRow transaction capability */ }
type WorkerOption[A, R any] interface{ /* sealed by flow */ }

func WithCommit[A, R any](
    fn func(context.Context, Tx, Commit[A, R]) error,
) WorkerOption[A, R]

func DefinePlan[A any](name string, version int, plan func(*Plan, A)) PlanDef[A]
func (p PlanDef[A]) With(client Client) PlanDef[A]

func DefineCoordinator[S any](name string, version int, handlers ...Handler[S]) Coordinator[S]
func (c Coordinator[S]) With(client Client) Coordinator[S]
func OnStart[S any](h func(context.Context, *Coordination[S]) error) Handler[S]
func On[S, T any](event Event[T], h func(context.Context, *Coordination[S], Received[T]) error) Handler[S]
func OnOutcome[S, A, R any](
    cmd Command[A, R],
    h func(context.Context, *Coordination[S], Received[CommandOutcome[R]]) error,
) Handler[S]

func RetryFor(maxElapsed time.Duration) RetryPolicy
func (p RetryPolicy) Attempts(max int) RetryPolicy
func (p RetryPolicy) Backoff(delays ...time.Duration) RetryPolicy
func (p RetryPolicy) Jitter(fraction float64) RetryPolicy

func WithMaxAttempts(int) CommandOption
func WithRetryPolicy(RetryPolicy) CommandOption
func WithTimeout(time.Duration) CommandOption   // per-attempt wall clock
func WithQueue(string) CommandOption            // worker lane
```

A command declares both what it takes and what it returns. `Command.Done()` is the event carrying that result; it shares the command's name and version, needs no separate declaration, and is what `After` waits on. It is an ordinary `Event[R]`, not a separate event category.

Names are stable durable identifiers. Every definition carries an explicit positive integer version; `0` is invalid. A `(name, version)` pair has immutable schema and orchestration meaning once used. Behavior-preserving command-handler and commit-function implementation changes may redeploy freely, while a material change to command payload, result, or commit semantics requires a new command version. A material plan change that can alter declarations or consulted reads for the same durable snapshot requires a new plan version. A material coordinator change to state meaning, subscriptions, or decisions requires a new coordinator version. Replicas cannot compare Go function bodies, so reusing one version for divergent plan or coordinator logic is an invalid deployment rather than a supported rollout. A runtime claims only work for pairs it has registered; unknown pairs stay pending for a compatible process and consume no retry budget.

Registration is explicit and runtime-local; definitions mutate no package-global state. A runtime rejects duplicate workers for one command pair, more than one declared commit function for a worker, more than one `OnStart` handler for a coordinator, duplicate `On` handlers for one event pair, duplicate `OnOutcome` handlers for one command pair, and overlapping `On(command.Done(), ...)` and `OnOutcome(command, ...)` subscriptions within one coordinator.

`RetryPolicy` is declarative library data rather than an application-implemented callback. `RetryFor` starts with the default backoff and an elapsed-time bound; its immutable builder methods return modified copies. `Attempts` adds an attempt-count bound, `Backoff` supplies the ordered retry delays, and `Jitter` supplies a proportional fraction in `[0, 1]`. When an elapsed policy needs more retries than its delay list contains, the last delay repeats. A policy must contain a positive attempt bound, a positive elapsed bound, or both. Invalid durations, attempts, jitter, or an empty explicit backoff fail definition or plan validation. `WithMaxAttempts` is the concise attempt-only form using default backoff and cannot be combined with `WithRetryPolicy` on one command definition. A plan node may override the definition with either `MaxAttempts` or `RetryPolicy`, but not both.

When accepting a command, the runtime resolves its definition defaults together with any explicit plan-node override, then copies and canonicalizes the resulting policy. That accepted policy is immutable for the command's lifetime and is recorded in `CommandCreated` history and inspection. Application clocks, closures, arbitrary interfaces, and mutable policy state therefore cannot influence a retry decision.

Definition-level execution options — retry policy, per-attempt timeout, and queue lane — are **creation-time operational defaults**, not command schema identity. Changing one does not require a command-version bump, rewrite an existing command, or make its later plan reconciliation fail; only commands created after the change receive the new default. An explicit plan-node `MaxAttempts` or `RetryPolicy` is different: its presence and canonical value are part of that plan declaration and must remain stable for an existing key. Duplicate declarations of one key within the same evaluation must also resolve to the same effective operational settings. A future administrative change to an already-created command's accepted policy must be an explicit journaled operation, never a side effect of redeployment.

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
func (n *Node) Within(time.Duration) *Node          // awaited-fact deadline after command dependencies settle
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
type EventID     string
type JournalPosition uint64

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
    Args A
}

type CommandInfo struct {
    ExecutionID ExecutionID
    CommandID   CommandID
    CommandKey  string
    Name        string
    Version     int

    CreatedAt        time.Time
    BudgetStartedAt  time.Time
    Attempt          int
    AttemptStartedAt time.Time
}

type Received[T any] struct {
    EventID    EventID
    Key        string
    Position   JournalPosition
    RecordedAt time.Time
    Payload    T
}

type ResultSource interface{ /* sealed by flow */ }

type Coordination[S any] struct {
    State S
}

type SpawnOption interface{ /* sealed by flow */ }

func Emit[T any](s Scope, event Event[T], key string, payload T) error
func Spawn[A, R any](s Scope, key string, cmd Command[A, R], args A, opts ...SpawnOption) error
func Optional() SpawnOption
func StartAfter(time.Duration) SpawnOption

func SucceedExecution(s CoordinatorScope, resultRef string) error
func FailExecution(s CoordinatorScope, reason error) error

func (w *Work[A]) Info() CommandInfo
```

`Work.Args` is the typed argument value supplied when the command was created. The API uses **arguments** for what a worker receives, **result** for what it returns, **payload** for serialized command or event data, and **state** for coordinator memory.

`CommandInfo` exposes identity plus durable database timing for the current logical command and attempt. `CreatedAt` is immutable command creation time. `BudgetStartedAt` is set once to the command's first claim-eligible time after dependencies, waits, and an initial plan `Delay` or spawn `StartAfter`; it never moves on retry, claim, interruption, lease recovery, or replica takeover. `Attempt` is the one-based durable invocation ordinal; it may be greater than the number of retry-budget-consuming attempts because an interrupted invocation still has operational identity. `AttemptStartedAt` is the PostgreSQL time at which this invocation began. Every field is fixed before the handler starts. Mutable update time and the moving next-attempt schedule are deliberately not exposed through `CommandInfo`: update time is maintenance data, while retry timing belongs to policy and inspection. `Commit.Info` carries the same accepted values.

`Received[T]` is the coordinator's typed view of one event. Position, key, and recorded time are retained metadata. For `On`, `Key` is the event's idempotency key and `Payload` is its canonical typed value. For `OnOutcome`, `Key` is the command key and `Payload` is the lossless typed `CommandOutcome[R]` view of that terminal event. `RecordedAt` is PostgreSQL acceptance time. Application-domain occurrence time belongs in `T` in Milestone 1; no separate caller-supplied occurrence-time option is implied. Coordinators use `Position` for ordering.

A worker returns `(R, error)`. Conceptually, the worker emits an event when its command finishes. The API makes the common success path automatic: returning `(result, nil)` records the event carrying `R`, together with any additional events and spawned commands it staged — all in one short transaction. Returning an error discards every staged output. A retryable error produces attempt history but no final event; a final failure records `CommandFailed` after the retry policy is exhausted or the error is classified permanent.

Most workers register no commit function. A worker that must make a short PostgreSQL application write inseparable from its successful command outcome may register one declared function with `WithCommit` (§4.2). The runtime invokes it only after the handler returns `(result, nil)`, output and fence validation succeed, and the short settlement transaction has begun. Its `Commit[A, R]` value contains canonical durable arguments, the successful typed result, and command metadata. The application write, result event, journal entries, staged outputs, plan reconciliation, and materialized state then commit or roll back together.

A commit function is a statically registered transactional tail, not a dynamically captured worker output. Values that drive its write must appear in `Args`, `Result`, or `Info`; in particular, a worker-local value must be added to `R` before the commit function may use it. The function must be deterministic from those durable inputs, perform only bounded local PostgreSQL work through the supplied narrow `Tx`, and avoid external I/O, clocks, randomness, mutable globals, goroutines, and nested `flow` operations. `Tx` exposes query execution but not commit, rollback, or nested-transaction control and is intentionally easy for application repositories to accept and tests to replace. Go cannot completely prohibit global access, so this is a contract reinforced by direct testing and optional static analysis rather than a sandbox.

A commit-function error rolls back ordinary settlement and is classified with the command's retry rules; the worker may consequently run again. External work in that worker must therefore still use `CommandID` or another stable key for idempotency or reconciliation. A database write that is part of the meaning of one command's success belongs in the commit function. Work needing its own identity, retry policy, event, or graph vertex is a separate command instead. Coordinator handlers have no commit function: application work belongs in commands, while a coordinator atomically changes only its orchestration state, inbox, events, and spawned commands.

`Do` and `Spawn` are both asynchronous, but intentionally use different verbs. `Do` is a repeatable declarative operation evaluated many times and reconciled by key. `Spawn` is an imperative staged output of one successful handler decision. It never calls the child handler inline.

From a worker, every spawned command is a direct child of the current command and automatically inherits execution identity and causation. The successful parent return closes that parent's direct-child membership: all children staged by that logical command become visible with the event recording its success, and that parent can never add another child later. Closure describes membership only; it does not mean that the children have finished or succeeded. Each child may subsequently succeed, retry, fail, expire, or be cancelled independently. From a coordinator, spawned commands are caused by the currently handled event.

Spawned commands are required by default and therefore determine execution outcome. `flow.Optional()` makes one spawned command optional. The runtime's authoritative parent-child relationship is derived from staged `Spawn` calls, never from an application payload. Plans read that relationship with `Children`; an application result carries child keys only when they are domain data or identify a semantic subset distinct from all direct children.

`flow.StartAfter(d)` makes a spawned command claim-eligible no earlier than `d` after the PostgreSQL transaction accepting that handler decision. The command and its schedule are visible immediately, but the command consumes no worker capacity, lease, or retry budget before that time. The option is valid from both worker and coordinator scopes, accepts only a positive finite duration, and may appear at most once for one staged key; invalid or duplicate scheduling options make `Spawn` return `ErrInvalid` before staging that child. Scheduling is part of staged-command equivalence: duplicate spawns with the same key must agree on the accepted delay as well as definition, arguments, and classification.

Every `*Work[A]` implements the sealed `ResultSource` capability used by `ResultOf` and `OutcomeOf` (§4.6). Inside a worker, either operation may read only a command explicitly named as a dependency of the current command, and the supplied definition must match that dependency's durable name and version. `ResultOf` returns the immutable typed result only after success. `OutcomeOf` returns `CommandOutcome[R]` after any terminal state, including failure, cancellation, expiry, or skipping. A non-dependency, mismatched definition, unavailable success result, or non-terminal outcome returns a structured permanently classified error; dependency conditions such as `After`, `AfterSettled`, and `AfterFailed` are what make the corresponding access valid before the handler starts. Workers cannot inspect arbitrary commands or the wider execution graph.

Dependency edges and argument keys deliberately serve different purposes. Edges determine **when** the command may run; keys in `Work.Args` identify **which** inputs the worker consumes and their application role or order. Argument keys are not an authoritative copy of graph membership. `ResultOf` and `OutcomeOf` verify every accessed key against the durable dependency edges, so a copied key cannot grant wider graph access. The library does not add a worker-side dependency-enumeration or `ChildrenOf` API in Milestone 1.

Requirements:

- worker and coordinator outputs are buffered until successful return;
- output payloads are type-checked through their definitions;
- `Emit` and `Spawn` are available to both worker and coordinator scopes; a plan uses `Do`, never `Spawn`;
- duplicate equivalent `Spawn` calls for one key within one handler decision coalesce, while different content or spawn options for one key return `ErrConflict` and commit nothing;
- execution completion functions are available only to coordinator scopes; direct and plan-driven executions complete automatically (§6.4);
- a declared commit function is available only on worker registration; coordinator application work is expressed as commands;
- nested `flow` operations use staged outputs or a caller-owned transaction, never a recursive call from inside a commit function.

### 4.6 Inspection

```go
func GetExecution(ctx context.Context, c Client, id ExecutionID) (Execution, error)
func LookupExecution(ctx context.Context, c Client, typ, key string) (Execution, error)
func Trace(ctx context.Context, c Client, id ExecutionID, opts ...TraceOption) (ExecutionTrace, error)
func History(ctx context.Context, c Client, id ExecutionID, opts ...HistoryOption) ([]HistoryEntry, error)
func ListExecutions(ctx context.Context, c Client, f ExecutionFilter) (ExecutionPage, error)
func AwaitExecution(ctx context.Context, c Client, id ExecutionID) (Execution, error)

func ResultOf[A, R any](src ResultSource, key string, cmd Command[A, R]) (R, error)
func OutcomeOf[A, R any](src ResultSource, key string, cmd Command[A, R]) (CommandOutcome[R], error)
```

`ResultSource` is sealed and implemented by every `*Work[A]` and by `ExecutionTrace`, the immutable inspection snapshot containing command results and terminal outcomes. For a worker source, `ResultOf` and `OutcomeOf` enforce the explicit-dependency and matching-definition rules above. Any successful result or terminal outcome available when the handler starts is immutable; the runtime may batch or preload them so repeated typed reads do not imply one database query each. For an `ExecutionTrace` source, either function reads a matching command in that immutable snapshot without a worker dependency boundary. Inspection never mutates execution state.

### 4.7 Error classification

```go
func Permanent(err error) error
func RetryAfter(d time.Duration, err error) error
```

### 4.8 Surface

| Category | Exported |
|---|---|
| Runtime | `New`, `Migrate`, `Register`, `Run`, `Stop`, `InTx` |
| Definitions | `DefineCommand`, `DefineEvent`, `DefinePlan`, `DefineCoordinator`, `Handle`, `WithCommit`, `OnStart`, `On`, `OnOutcome`, `Done`, `Name`, `Version`, `With` |
| Plans | `Do`, `Fact`, `Facts`, `Children`, `Result`, `Outcome`, plus 10 command builder methods |
| Execution | `Execute` on `Command`, `PlanDef`, and `Coordinator`; `Issue`, `Publish`, `CancelExecution`, `CancelCommand` |
| Handler output | `Emit`, `Spawn`, `Optional`, `StartAfter`, `Info`, `SucceedExecution`, `FailExecution` |
| Inspection | `GetExecution`, `LookupExecution`, `Trace`, `History`, `ListExecutions`, `AwaitExecution`, `ResultOf`, `OutcomeOf` |
| Retry policy | `RetryFor`, `RetryPolicy.Attempts`, `RetryPolicy.Backoff`, `RetryPolicy.Jitter`, `WithMaxAttempts`, `WithRetryPolicy` |
| Errors | `Permanent`, `RetryAfter` |

The smallest path is `DefineCommand`, `Handle`, `With(runtime)`, `Command.Execute`, and `Run`. Store the returned same-type copy when a definition is executed repeatedly. Add `Spawn` when a worker discovers bounded children, and `StartAfter` only when that staged child must become eligible later. Add `WithCommit` only when one command's successful meaning includes a short atomic application-table write. Add `DefinePlan`, `Do`, `PlanDef.Execute`, and event reads only when the execution needs cross-command dependencies, joins, waits, or branching. Coordinators, cancellation, transaction composition, and policy customization form the advanced operational surface.

## 5. Worked examples

### 5.1 Direct background command

A plan is not required for ordinary durable background work:

```go
type ReceiptArgs struct {
    OrderID string
    Email   string
}

type ReceiptSent struct {
    ProviderMessageID string
}

var SendReceipt = flow.DefineCommand[ReceiptArgs, ReceiptSent]("send_receipt", 1)

func sendReceipt(ctx context.Context, w *flow.Work[ReceiptArgs]) (ReceiptSent, error) {
    sent, err := mailer.SendReceiptOnce(
        ctx,
        string(w.Info().CommandID), // stable across attempts
        w.Args.OrderID,
        w.Args.Email,
    )
    if err != nil {
        return ReceiptSent{}, err
    }
    return ReceiptSent{ProviderMessageID: sent.MessageID}, nil
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

`Runtime.Run` claims the queued command and invokes the registered `sendReceipt` worker. `Command.Execute` returns an `ExecutionHandle` immediately. The journal first records creation of the root command with its canonical arguments and causation. When the command succeeds, it records the typed `ReceiptSent` event and the execution's `ExecutionSucceeded` event. Attempt starts and interruptions remain operational history rather than plan-visible events. If the worker spawns required children, the execution remains running until those descendants also settle. Because sending mail is external, the worker uses the stable `CommandID` as an application/provider idempotency key.

An application may bind its frequently used definitions once:

```go
type AppFlows struct {
    SendReceipt flow.Command[ReceiptArgs, ReceiptSent]
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
    PrepareReport = flow.DefineCommand[PrepareArgs, PrepareResult]("prepare_report", 1)
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

type PrepareResult struct {
    AnalysisCount int
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

func prepareReport(ctx context.Context, w *flow.Work[PrepareArgs]) (PrepareResult, error) {
    analyses, err := determineAnalyses(ctx, w.Args.CompanyID)
    if err != nil {
        return PrepareResult{}, err
    }

    for _, analysis := range analyses {
        key := "analysis/" + analysis.ID
        if err := flow.Spawn(w, key, AnalyzePart, analysis.Args); err != nil {
            return PrepareResult{}, err
        }
    }

    return PrepareResult{AnalysisCount: len(analyses)}, nil
}

// This short local write is part of what PrepareReport succeeding means.
// Its inputs are already durable in the command arguments and result.
func commitPrepareReport(
    ctx context.Context,
    tx flow.Tx,
    c flow.Commit[PrepareArgs, PrepareResult],
) error {
    return reports.MarkPrepared(
        ctx,
        tx,
        c.Args.CompanyID,
        c.Result.AnalysisCount,
    )
}

func analyzePart(ctx context.Context, w *flow.Work[AnalysisArgs]) (AnalysisResult, error) {
    return analyzers.For(w.Args.Kind).Analyze(ctx, w.Args)
}

func generateReport(ctx context.Context, w *flow.Work[GenerateArgs]) (ReportResult, error) {
    results := make([]AnalysisResult, 0, len(w.Args.AnalysisKeys))
    for _, key := range w.Args.AnalysisKeys {
        result, err := flow.ResultOf(w, key, AnalyzePart)
        if err != nil {
            return ReportResult{}, err
        }
        results = append(results, result)
    }
    return reports.Generate(ctx, w.Args.CompanyID, results)
}
```

Wiring a worker process:

```go
rt, err := flow.New(db)
if err != nil {
    return err
}
if err := rt.Register(
    flow.Handle(PrepareReport, prepareReport, flow.WithCommit(commitPrepareReport)),
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

The first plan evaluation declares only `prepare`, whose creation is appended to the execution journal with its arguments, dependencies, origin, and causation. Its worker discovers the complete analysis membership and stages every child with `Spawn`. The declared commit function then marks the application report prepared using only the durable command arguments and result. That write, every child-creation journal entry and command row, the event returned by `PrepareReport.Done()`, and closed membership become visible atomically. `Children` then exposes that authoritative membership, and the next evaluation declares every failure branch and the pending `generate` command together. Analysis workers run in parallel. Once every `After` dependency succeeds, `generate` becomes runnable and its worker reads the immutable typed dependency results through `ResultOf`.

If the preparation handler, commit function, or settlement fails, no child, result event, or application write is committed; the command retries under its policy. Any slow or external operation in that worker must therefore be idempotent even though the commit function itself is local. If one analysis exhausts retries, it records `CommandFailed`, the already-declared corresponding `AfterFailed` branch runs and may inspect that structured failure through `OutcomeOf`, `generate` becomes `skipped`, and the execution ultimately fails after the remaining analyses settle. Child membership is never duplicated in `PrepareReport`'s result, and routine value plumbing introduces no result-loop early-return trap. The plan needs no coordinator because this fan-out membership closes with one successful parent return.

### 5.3 External monitor publishes a fact

Long external waits should not normally occupy one Flow worker per execution. An existing webhook or batch monitor can observe many external operations efficiently, publish one typed fact into the matching execution, and release a plan-declared command that was waiting without holding a worker or lease.

```go
type BridgeDelivery struct {
    IntentID string
    TxHash   string
}

var (
    BridgeDelivered = flow.DefineEvent[BridgeDelivery]("bridge_delivered", 1)
    SendOrigin = flow.DefineCommand[OriginArgs, OriginResult](
        "send_origin", 1,
        flow.WithRetryPolicy(flow.RetryFor(10*time.Minute)),
    )
    SendDestination = flow.DefineCommand[DestinationArgs, DestinationResult]("send_destination", 1)
    IntentExecution = flow.DefinePlan[IntentArgs]("intent_execution", 1, planIntent)
)

func planIntent(p *flow.Plan, args IntentArgs) {
    flow.Do(p, "origin", SendOrigin, args.Origin)
    flow.Do(p, "destination", SendDestination, args.Destination).
        After("origin").
        Await(BridgeDelivered).
        Within(time.Hour)
}
```

`SendOrigin` may retry as often as its backoff permits, but not beyond ten minutes from its first claim-eligible time. Process restart, lease recovery, and replica takeover do not restart that budget. The destination's one-hour `Within` bound is different: its clock begins only after `origin` succeeds and limits the remaining wait for the bridge fact. The plan can therefore declare the complete structure in its natural order without a timing gate or a result read.

The external monitor publishes through the transaction-scoped client in the same PostgreSQL transaction as its application-table update. The Flow operation appears first so the transaction follows the library-wide lock order defined by architecture; if either write fails, neither commits.

```go
func recordBridgeDelivery(
    ctx context.Context,
    db *pgkit.DB,
    rt *flow.Runtime,
    executionID flow.ExecutionID,
    delivery BridgeDelivery,
) error {
    return pgx.BeginFunc(ctx, db.Conn, func(tx pgx.Tx) error {
        if err := flow.Publish(
            ctx,
            rt.InTx(tx),
            executionID,
            BridgeDelivered,
            delivery.TxHash, // stable natural idempotency key
            delivery,
        ); err != nil {
            return err
        }
        return transfers.MarkDelivered(ctx, tx, delivery.IntentID, delivery.TxHash)
    })
}
```

The fact and application update become visible together. If the fact arrives before `origin` succeeds, retained historical delivery satisfies `Await` immediately when the command dependency resolves. Repeated equivalent publication coalesces by event key. A normal provider response of “still pending” produces no Flow attempt and no event; the external monitor continues its own bounded batch polling. Provider failure, refund, or delivery is published only when it becomes a meaningful execution fact.

`BridgeDelivered` is used only by the already-materialized `Await`, so this monitor needs the event definition and client but does not register or execute `IntentExecution`. The engine can satisfy the stored wait and make `destination` ready without calling the Go plan. If a plan also consults that event through `Fact` or `Facts`, the publishing runtime must register the exact plan version, or route the publication through an application service that does, because the fact and resulting declarations must then commit atomically.

### 5.4 Durable adaptive agent

A long-running agent is a coordinator-driven execution, not one worker that retains an agent loop in memory. Model calls, tools, and bounded sub-agent tasks are commands. Their terminal results are events consumed through `OnOutcome`, so failure, cancellation, expiry, or replica loss cannot leave an in-memory pending counter stranded.

The model worker returns proposed actions. The coordinator validates those actions and remains the authority that spawns tools, applies budgets, and decides whether the execution succeeds or fails:

```go
type AgentState struct {
    GoalRef        string
    TranscriptRef  string
    Turn           int
    ThinkPending   bool
    WaitingForUser bool
    PendingTools   map[string]bool
    Observations   []AgentObservation
}

type AgentObservation struct {
    CommandKey string
    Status     flow.CommandStatus
    ResultRef  string
    FailureCode string
}

type ThinkArgs struct {
    GoalRef       string
    TranscriptRef string
    Observations  []AgentObservation
}

type ToolCall struct {
    ID       string
    Name     string
    InputRef string
}

type ThinkResult struct {
    FinalResultRef string
    WaitForUser    bool
    ToolCalls      []ToolCall
}

type ToolArgs struct {
    Name     string
    InputRef string
}

type ToolResult struct {
    OutputRef string
}

type AgentUserMessage struct {
    MessageRef string
}

var (
    Think = flow.DefineCommand[ThinkArgs, ThinkResult](
        "agent_think", 1,
        flow.WithRetryPolicy(flow.RetryFor(10*time.Minute).Attempts(3)),
    )
    RunAgentTool = flow.DefineCommand[ToolArgs, ToolResult]("agent_tool", 1)
    UserMessage  = flow.DefineEvent[AgentUserMessage]("agent_user_message", 1)
)
```

The coordinator uses deterministic keys for logical turns and tool calls. Commands it intends to interpret are optional so ordinary fail-fast does not outrun the coordinator's recovery decision:

```go
var ResearchAgent = flow.DefineCoordinator[AgentState](
    "research_agent",
    1,
    flow.OnStart(startAgent),
    flow.OnOutcome(Think, handleThought),
    flow.OnOutcome(RunAgentTool, handleToolOutcome),
    flow.On(UserMessage, handleUserMessage),
)

func startAgent(ctx context.Context, c *flow.Coordination[AgentState]) error {
    c.State.Turn = 1
    c.State.PendingTools = make(map[string]bool)
    return spawnNextThought(c, 0)
}

func spawnNextThought(c *flow.Coordination[AgentState], after time.Duration) error {
    args := ThinkArgs{
        GoalRef:       c.State.GoalRef,
        TranscriptRef: c.State.TranscriptRef,
        Observations:  append([]AgentObservation(nil), c.State.Observations...),
    }

    opts := []flow.SpawnOption{flow.Optional()}
    if after > 0 {
        opts = append(opts, flow.StartAfter(after))
    }

    key := fmt.Sprintf("turn/%d/think", c.State.Turn)
    if err := flow.Spawn(c, key, Think, args, opts...); err != nil {
        return err
    }

    c.State.ThinkPending = true
    c.State.WaitingForUser = false
    c.State.Observations = nil
    return nil
}
```

A completed model call either finishes the agent, waits for an external user fact, or stages a bounded fan-out of tool commands. Application validation must reject unknown tools, duplicate or invalid call IDs, excessive fan-out, and arguments outside the agent's authority before any spawn is accepted:

```go
func handleThought(
    ctx context.Context,
    c *flow.Coordination[AgentState],
    received flow.Received[flow.CommandOutcome[ThinkResult]],
) error {
    c.State.ThinkPending = false
    outcome := received.Payload

    if outcome.Status != flow.StatusSucceeded {
        return flow.FailExecution(c, fmt.Errorf(
            "agent model command ended %s: %s",
            outcome.Status,
            failureCode(outcome.Failure),
        ))
    }

    switch {
    case outcome.Result.FinalResultRef != "":
        return flow.SucceedExecution(c, outcome.Result.FinalResultRef)

    case outcome.Result.WaitForUser:
        c.State.WaitingForUser = true
        return nil

    case len(outcome.Result.ToolCalls) == 0:
        return flow.FailExecution(c, errors.New("agent returned no final result or action"))
    }

    if err := validateToolCalls(outcome.Result.ToolCalls); err != nil {
        return flow.FailExecution(c, err)
    }

    c.State.PendingTools = make(map[string]bool, len(outcome.Result.ToolCalls))
    for _, call := range outcome.Result.ToolCalls {
        key := fmt.Sprintf("turn/%d/tool/%s", c.State.Turn, call.ID)
        if err := flow.Spawn(
            c,
            key,
            RunAgentTool,
            ToolArgs{Name: call.Name, InputRef: call.InputRef},
            flow.Optional(),
        ); err != nil {
            return err
        }
        c.State.PendingTools[key] = true
    }
    return nil
}
```

Every terminal tool outcome arrives exactly once through the command's existing terminal event. A final tool failure becomes an observation the next model turn can reason about rather than a stalled join:

```go
func handleToolOutcome(
    ctx context.Context,
    c *flow.Coordination[AgentState],
    received flow.Received[flow.CommandOutcome[ToolResult]],
) error {
    if !c.State.PendingTools[received.Key] {
        return flow.Permanent(fmt.Errorf("unexpected tool outcome %q", received.Key))
    }
    delete(c.State.PendingTools, received.Key)

    observation := AgentObservation{
        CommandKey: received.Key,
        Status:     received.Payload.Status,
    }
    if received.Payload.Status == flow.StatusSucceeded {
        observation.ResultRef = received.Payload.Result.OutputRef
    } else {
        observation.FailureCode = failureCode(received.Payload.Failure)
    }
    c.State.Observations = append(c.State.Observations, observation)

    if len(c.State.PendingTools) != 0 {
        return nil
    }

    c.State.Turn++
    return spawnNextThought(c, 2*time.Second)
}
```

An external user response is an idempotent event. It may arrive while tools are still running and remain in state for the next turn, or release an agent that explicitly parked for input:

```go
func handleUserMessage(
    ctx context.Context,
    c *flow.Coordination[AgentState],
    received flow.Received[AgentUserMessage],
) error {
    c.State.Observations = append(c.State.Observations, AgentObservation{
        CommandKey: "user/" + received.Key,
        Status:     flow.StatusSucceeded,
        ResultRef:  received.Payload.MessageRef,
    })

    if !c.State.WaitingForUser || c.State.ThinkPending || len(c.State.PendingTools) != 0 {
        return nil
    }

    c.State.Turn++
    return spawnNextThought(c, 0)
}
```

`StartAfter` persists the two-second pause using PostgreSQL time; it does not sleep inside the coordinator or reserve a worker. Recording a tool outcome, updating the pending set, and spawning the next thought commit atomically. If the process dies anywhere between turns, another compatible replica resumes from the durable inbox and state. If it dies during a model or tool command, ordinary lease takeover re-executes that command under its policy.

Coordinator state contains only bounded orchestration facts and durable references. Full transcripts, documents, embeddings, and tool artifacts remain in application tables or object storage; commands may use declared commit functions to write short local records atomically with their successful result. Model and tool workers remain at-least-once: they should use `CommandID` as an external idempotency key where supported and reconcile side effects where it is not. A successful activity repeated later receives a new deterministic command key such as `turn/42/think`; retries of the same logical activity retain one `CommandID` and create new attempts instead.

A bounded sub-agent is another command such as `ResearchTopic`. A sub-agent that itself needs an adaptive durable loop requires the near-term child-execution boundary in §2.3 rather than hiding an entire coordinator inside one worker or copying all of its state into the parent.

The application starts the coordinator like every other execution mode and may publish user input from any replica:

```go
h, err := ResearchAgent.With(rt).Execute(
    ctx,
    "research/"+requestID,
    AgentState{GoalRef: goalRef, TranscriptRef: transcriptRef},
    flow.WithExecutionDeadline(7*24*time.Hour),
)
if err != nil {
    return err
}

err = flow.Publish(
    ctx,
    rt,
    h.ID,
    UserMessage,
    messageID,
    AgentUserMessage{MessageRef: messageRef},
)
```

The call queues the start activation; it does not run the agent inline. A deployment expecting more than the default 1,000 logical commands in one agent execution configures a larger `WithMaxCommandsPerExecution` value on the runtime that starts it. That accepted ceiling is stored on the execution, so whichever replicas later process its turns enforce the same value.

## 6. Executions

### 6.1 Identity and idempotent start

An execution has a generated `ExecutionID`, a driver mode, an execution type, and a caller-supplied key. The driver mode is `direct`, `plan`, or `coordinator`. The type is the receiver definition's name: command, plan, or coordinator respectively. `(driver_mode, execution_type, execution_key)` is unique for as long as the execution is retained; an empty key enforces no uniqueness.

Creating an execution appends one `ExecutionStarted` journal entry in the same transaction. It records the execution identity, driver definition and version, canonical root arguments or initial coordinator state, deadline, accepted command ceiling, materially relevant options, metadata allowed by the serialization policy, and causation. It is operational history rather than a plan-visible event. An idempotent repeated start that finds equivalent content appends nothing.

Repeating `.Execute` on the same definition with the same key, definition version, canonical arguments or initial state, and materially relevant options returns the existing execution with `Created == false`. Reusing that identity with materially different content returns `ErrConflict` and changes nothing.

Current command-definition operational defaults are not re-compared on an idempotent direct-command start. The existing root retains the retry, timeout, and queue settings accepted when it was created, just as an existing plan command does during reconciliation.

The runtime's current command-ceiling default is likewise not re-compared on an idempotent start. The existing execution retains the value accepted at creation, preventing deployment configuration differences from changing or conflicting with in-flight work.

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

Accepting a command by any creation path appends one immutable `CommandCreated` journal entry in the same transaction that materializes the command and its dependencies. The entry records its ID and key, name and version, canonical payload, creation origin, parent where applicable, required/optional classification, declared dependencies and waits, accepted initial schedule, policy that affects execution semantics, and causation. Reconciliation that finds an equivalent existing command appends nothing; creation and idempotent rediscovery remain distinguishable.

A plan-declared command's key is its command key. All creation paths share one execution-wide key namespace. Re-declaring the same plan-owned key with an equivalent definition, canonical payload, topology, classification, and explicit node override is a no-op; different content returns `ErrConflict`. A change only to the command definition's current operational defaults leaves the existing command and its accepted settings untouched. A plan may read or depend on an existing spawned command, but attempting to take ownership of its key with `Do` is a plan defect.

### 7.2 Lifecycle

```text
pending → ready → running → succeeded
                  running → retry_wait → running
                  running → failed
pending | ready | running → cancelled
pending | ready → expired
pending → skipped
```

`pending` means declared but not yet runnable — dependencies unresolved or awaited facts not yet arrived. `ready` means those prerequisites are resolved and the initial attempt is scheduled; its next-attempt time may be now or in the future because of plan `Delay` or spawn `StartAfter`. There is no persisted `scheduled` state: inspection derives it from a `ready` command whose next-attempt time has not arrived. For an initially delayed command, `BudgetStartedAt` is that future first claim-eligible time, not the earlier transition to `ready`.

`skipped` means a dependency condition became permanently unsatisfiable, so the command will never run. It is terminal and unsuccessful, but it is not a failure and does not by itself fail the execution.

### 7.3 Events recorded for commands

When a command reaches a final state, `flow` records exactly one event describing how it ended.

For success, returning `(result, nil)` records the event returned by `Command.Done()`, carrying the typed result and sharing the command's name and version. Final-state metadata records whether the command's declared commit function was applied in that same transaction. This is automatic and cannot be suppressed. It is the fact that `After` waits for, which makes "wait for work" and "wait for a fact" one mechanism.

A worker may additionally call `Emit` to record application events. Those are recorded at earlier positions than the event recording success, so any reader observing success has already observed them.

Failure, cancellation, expiry, or skipping records exactly one `CommandFailed`, `CommandCancelled`, `CommandExpired`, or `CommandSkipped` event instead. All are ordinary events in the same journal. A database constraint prevents more than one event recording the final state of a command. Retryable attempt failures are history, not events, because the command has not ended.

Together, the `CommandCreated` journal entry and exactly one terminal event make every command's existence, topology, arguments, and final outcome reconstructible from retained history. This makes terminal progress plan-observable and auditable and lets historical plan simulation fold one ordered source. The command, dependency, and child tables remain indexed materializations for claiming and current-state queries rather than a second logical history (§9.6).

### 7.4 Bounded child spawning

`Spawn` stages a command for asynchronous delivery. It does not invoke a handler, block for a result, or open a nested transaction.

For a worker handler:

- every spawned command is a direct child of the current logical command;
- stable child keys are unique across the execution and remain stable across parent retries;
- all staged children and their `CommandCreated` entries, emitted application events, the event recording the parent's success, and any declared commit-function write commit atomically on successful return;
- if the handler returns an error, panics, loses its lease, is cancelled, or the settle transaction rolls back, none of its staged children become visible;
- after the parent succeeds, its direct-child membership is closed permanently, although the children remain independently active;
- spawned children are required unless created with `flow.Optional()`.

The runtime derives authoritative child membership from command causation, and a plan joining all direct children reads it with `Children`. A typed parent result carries child keys only when those keys are themselves domain data or identify a semantic subset of a heterogeneous child set; correctness and tracing never infer membership from arbitrary result payload fields.

Before accepting the worker result, settlement validates the entire staged set against the execution-wide key namespace and applicable command limit. A key already owned by another creation decision, different content for one buffered key, or a deterministic limit violation is a permanent structured output failure: no staged output or success event commits, the parent records `CommandFailed`, and rerunning the same worker is not attempted because it cannot repair that decision.

For a coordinator handler, `Spawn` has the same buffering and atomicity but the current inbox event is the cause; no parent command is implied.

### 7.5 Waiting and scheduling

A command becomes runnable when all declared command dependencies are satisfied and all awaited facts have arrived. `Within` is specifically an awaited-fact deadline and is valid only on a node that also declares `Await`; using it without `Await` is a plan defect. Its clock starts, using PostgreSQL time, when every non-`Await` command dependency on that node has been satisfied. With no command dependency it starts when the node is first declared. If the awaited facts already exist at that point, the command becomes runnable immediately and no wait expires. Otherwise the deadline is persisted once, capped by the execution deadline, and the command becomes `expired` if any awaited fact remains absent when it passes; dependents then resolve through the failure branch.

Deadline outcome depends on PostgreSQL acceptance time under the execution lock, not on when a maintenance sweep happens. A matching fact whose recorded acceptance time is no later than the persisted `Within` deadline satisfies the wait even if expiry maintenance runs afterward. A fact first accepted after that deadline remains immutable execution history but does not satisfy or resurrect the wait; plan-capable maintenance records the command's expiry. An `Await`-only publisher does not need plan code merely because it observes a late fact.

`Within` does not consume time spent running or waiting for predecessor commands. Those commands have their own retry bounds and share the execution deadline. `Await` without `Within` inherits the execution deadline; only an execution explicitly started with `WithoutExecutionDeadline` opts out of that bound and the bounded-completion guarantee (§12.6).

Plan `Delay` and spawn `StartAfter` set the earliest time a runnable command may be claimed. **A delayed command is the one-shot durable timer primitive**; there is no separate timer record or sleeping handler. A waiting or delayed command holds no worker, connection, goroutine, or lease.

`StartAfter(d)` is available only while staging `Spawn` from a worker or coordinator and requires `d > 0`. The runtime computes and persists the first claim-eligible time from PostgreSQL time in the transaction accepting the handler decision. That accepted timestamp is immutable: replay or redelivery after commit cannot move it. If a handler decision rolls back before acceptance, a later invocation creates no history and may naturally receive a later database-relative schedule. `BudgetStartedAt` is the resulting claim-eligible time, so intentional delay consumes no elapsed retry budget. The execution deadline still applies; a command that cannot begin before that deadline expires rather than extending the execution.

`StartAfter` is not recurring scheduling. An open-ended coordinator schedules another uniquely keyed command after each completed turn when repetition is desired; cron-like schedules independent of an active execution remain a later capability.

For a long wait owned by an external system, the preferred pattern is §5.3: keep an efficient webhook or batch monitor outside the execution, publish an idempotent fact through `InTx` when meaningful progression occurs, and let a command wait with `Await`. Normal external “still pending” observations are polling maintenance, not failed Flow attempts or events.

### 7.6 Claiming and rolling deployments

A worker claims only `(name, version)` pairs it has registered, using row-level locking that skips rows another process is claiming. Plan-driven command claims additionally require the execution's exact plan registration (§16.3). A process lacking the complete capability leaves the command pending, consumes no retry budget, and never fails it, so old and new versions of a service may share one database.

Unclaimable backlog—a missing command worker, exact plan, coordinator definition, or matching coordinator handler—is surfaced through inspection and observability rather than stalling silently.

## 8. Attempts, retries, and failure

### 8.1 Separate attempt identity

Each claimed execution of a command creates an attempt record with its own identity, worker and process identity, timings, structured error, and whether it consumed retry budget. The logical command keeps one `CommandID` throughout.

Attempt start and conclusion are also appended to the ordered journal. A conclusion records success handoff, retryable failure and chosen next-attempt time, permanent failure, timeout, shutdown interruption, or lease loss as applicable. These entries explain operational execution but are not `Event[T]` facts and cannot drive a plan or coordinator.

### 8.2 Default behavior

The default policy allows 5 attempts — one execution and 4 retries — with delays of 1s, 5s, 30s, and 2m, each with proportional jitter. Attempt count and policy are configurable per command definition and per plan-declared command. The chosen next-attempt time is persisted, so inspection shows exactly when a command may run again.

An effective retry policy is immutable declarative data with an optional positive maximum attempt count, an optional positive maximum elapsed duration, an ordered non-empty backoff, and proportional jitter. At least one bound is required. `RetryFor(d)` uses the default backoff and no attempt-count bound unless `Attempts` adds one. `WithMaxAttempts(n)` uses the default backoff, no elapsed bound, and is shorthand for the common attempt-only case. If both bounds exist, reaching either one stops retrying. Policy builders return copies; command creation copies and canonicalizes the resulting value.

Elapsed retry time is measured using PostgreSQL time from the command's immutable `BudgetStartedAt`, set to its first claim-eligible time after dependencies, waits, and initial delay. Claims, retries, shutdown interruption, lease loss, and crash recovery never move this anchor. Retry scheduling changes only the separately stored next-attempt time. At attempt start and conclusion the runtime evaluates the remaining elapsed budget using database time. It caps the handler context by the earliest of the configured per-attempt timeout, retry-budget deadline, and execution deadline. A retryable conclusion that has exhausted either policy bound ends the command in `failed`; it is not rescheduled beyond the deadline.

The runtime classifies the worker conclusion before applying policy. `Permanent` always stops immediately and no policy may convert it into a retry. `RetryAfter` supplies the desired delay for a retryable conclusion but remains subject to attempt and elapsed bounds. Plain errors, panics, and per-attempt timeouts use the policy backoff unless explicitly wrapped otherwise. Jitter may randomize the chosen delay, but the resulting next-attempt time is recorded once and never recomputed after restart or takeover.

| Worker return | Effect | Retry budget |
|---|---|---|
| `(result, nil)` | event carrying the result; command succeeds | — |
| plain `error` | retry per policy | consumed |
| `flow.RetryAfter(d, err)` | retry at an explicit delay | consumed |
| `flow.Permanent(err)` | command fails immediately | consumed |
| panic | recovered, treated as retryable | consumed |

Retry policy is for failures while attempting work. A successful external query whose result is merely not ready should ordinarily leave the command pending on a published fact as in §5.3, rather than return an error repeatedly. A workload that intentionally performs its own bounded polling inside one handler still uses the per-attempt context deadline and must honor cancellation.

### 8.3 Operational interruption

Shutdown interruption, lease loss, and unregistered-version deferral never consume retry budget and never make a command terminal. They are retained as operational history and observations, not as domain progression.

### 8.4 Terminal failure

A command that exhausts its attempt count or elapsed retry duration, or returns a permanent error, becomes `failed` and records `CommandFailed`, with its full attempt history preserved.

Attempt failures are not application results. A negative application result is a **successful** command whose typed result says so — a distinction that keeps retry mechanics out of application semantics.

### 8.5 Joining after child failures

The application chooses failure behavior with existing command policies rather than a separate fan-out state machine:

- **all must succeed** — spawn required children (the default), declare the join with `After`, and let the joined worker read successful dependency results through `ResultOf`; a terminal child failure skips the success join and fails the execution;
- **wait for all before failing** — start with `WithFailFast(false)` so independent required siblings settle before outcome calculation;
- **partial result** — spawn children with `flow.Optional()`, declare the final command with `AfterSettled`, and let its worker read each terminal dependency through `OutcomeOf`;
- **compensation** — declare keyed commands with `AfterFailed(childKey)`; fail-fast preserves that failure-handling closure, and the compensation worker reads the structured reason through `OutcomeOf`.

For a partial result, the plan remains structural and passes only the semantic input keys:

```go
type PartialArgs struct {
    AnalysisKeys []string
}

flow.Do(p, "partial-report", GeneratePartialReport, PartialArgs{AnalysisKeys: keys}).
    AfterSettled(keys...)
```

The worker reads the immutable terminal outcomes from those explicit dependencies:

```go
func generatePartialReport(
    ctx context.Context,
    w *flow.Work[PartialArgs],
) (ReportResult, error) {
    outcomes := make([]flow.CommandOutcome[AnalysisResult], 0, len(w.Args.AnalysisKeys))
    for _, key := range w.Args.AnalysisKeys {
        outcome, err := flow.OutcomeOf(w, key, AnalyzePart)
        if err != nil {
            return ReportResult{}, err
        }
        outcomes = append(outcomes, outcome)
    }
    return reports.GeneratePartial(ctx, outcomes)
}
```

Plan-side `Outcome` remains appropriate when a terminal result changes which commands should exist. Worker-side `OutcomeOf` handles routine failure/result plumbing after topology and scheduling have already been declared.

## 9. Events and the execution journal

### 9.1 Immutable journal and facts

Every journal entry has an `ExecutionID`, immutable per-execution position, recorded time, entry kind, and causation. Entry kinds cover the durable semantic and operational history needed to explain topology, progress, attempts, and outcome: execution lifecycle, command creation, attempt start and conclusion, event recording, coordinator processing, and execution transitions. A command's final transition is represented by its required terminal event rather than a duplicate operational entry. High-frequency maintenance such as lease-renewal heartbeats, polling, and notifications is deliberately excluded.

Every event entry additionally has an `EventID`, name and version, optional key, canonical typed payload, and where applicable the originating command and attempt. Application-domain occurrence time belongs in the typed payload in Milestone 1; the event metadata's `RecordedAt` is authoritative PostgreSQL acceptance time.

The journal is append-only and never destructively consumed. Events are one kind of journal entry and the only kind exposed through the typed `Event[T]`, `Fact`, `Facts`, and coordinator-subscription APIs; `OnOutcome` is a typed view over a command's terminal event, not exposure of an operational entry. Unlike a command, which one worker handles, an event is observed independently by the plan and by any coordinator subscribing to it. `CommandCreated`, `AttemptStarted`, and other operational entries appear in `History` and `Trace` but do not create additional event abstractions and cannot be awaited as application facts.

### 9.2 How events are recorded

There is one event model and one typed abstraction: `Event[T]`. Event entries enter the journal in three ways:

- a successful worker return automatically records the event returned by `Command.Done()`, carrying its result;
- workers and coordinators call `Emit`, while application code, webhooks, and monitors call `Publish`, to record additional facts;
- the runtime records facts such as `CommandFailed`, `CommandCancelled`, `CommandExpired`, `CommandSkipped`, `PlanFailed`, `CoordinatorFailed`, `ExecutionSucceeded`, `ExecutionFailed`, `ExecutionCancelled`, and `ExecutionExpired`.

These are different event names and payloads, not different event systems or developer-facing categories. Attempt failures and lease loss remain operational history rather than events, while lease renewals are omitted maintenance; transient mechanics never masquerade as permanent facts.

Command creation is also not an event. Every accepted `Do`, `Spawn`, `Issue`, and direct root creation appends the internal `CommandCreated` journal entry required by §7.1. Plans already observe command state through typed command reads and dependencies; exposing creation again as `Event[T]` would create a duplicate progression mechanism.

The runtime deliberately does not append a `CommandStarted` event. `Trace` derives whether a command is currently running from command state and its active lease, and reads when and where attempts ran from attempt records and operational history. An attempt start is durable operational history, while the command's single terminal event is the permanent plan-visible fact. This avoids duplicate representations of running state and prevents plans from reacting to retry mechanics.

### 9.3 Idempotency

`Publish` requires a non-empty event key. Identity is `(ExecutionID, event_name, EventKey)`, scoped across versions so a publisher retrying the same natural fact after a deployment cannot create duplicate progression under a newer schema.

| Repeated key | Result |
|---|---|
| equivalent canonical payload and material metadata | returns the existing event |
| different payload, version, or material metadata | `ErrConflict`; nothing written |

Idempotency is checked before terminal-execution rejection: retrying an existing equivalent event succeeds even after the execution becomes terminal, while a genuinely new event is rejected with `ErrTerminal`.

### 9.4 Ordering

All journal entries receive a durable total position within their execution, reflecting commit order rather than the time an external fact or operational activity occurred. An event's position is its journal position; plans and coordinators skip non-event entries without creating a second position namespace.

Plans and coordinators observe matching events in increasing position order. A failed delivery blocks later events for that reader until it succeeds or the reader becomes terminal.

Checkpointing must never permanently skip an event whose creating transaction becomes visible later; architecture must either make per-execution positions gap-free at commit or use a cursor that revisits unresolved gaps. Because positions are scoped to one execution — which is already the serialization point for its own commits — this is materially simpler than database-wide ordering.

There is no position or ordering relationship across executions. A future export, subscription, or execution-start bridge may interleave events from several executions in any order, but it must retain each event's source `(ExecutionID, position)`, cross an explicit idempotent boundary, and make no claim that the resulting stream is one merged `flow` journal.

### 9.5 Payloads and result references

Small durable results may be carried in payloads. Large or sensitive outputs belong in application-owned tables or object storage, referenced by a stable identifier. Changing data behind a reference does not change the historical event; use immutable, versioned, or content-addressed references where historical reproducibility matters.

### 9.6 Replay and recovery boundary

`flow` is journal-first for its orchestration control plane without event-sourcing application state. `CommandCreated` entries record requested work and topology; event entries record how commands ended and additional facts; attempt entries record transient execution mechanics; materialized command and execution state makes claiming and current-state inspection efficient. The retained journal is the logical source for the causal graph and settled orchestration projections. Architecture may normalize or share immutable payload storage rather than physically duplicating large canonical bytes, provided `History` preserves the same complete logical record.

Plans may be re-evaluated by folding the retained journal: command creation establishes declarations, dependencies, and closed membership; events establish results and facts; attempts explain operational execution without becoming plan inputs. Accurate historical plan simulation additionally requires the exact plan version that produced the historical declarations. Recovery and simulation never replay arbitrary Go handlers or declared commit functions, repeat historical external side effects, or rebuild application tables. Application-owned tables remain authoritative for business state; the journal records the durable inputs and accepted outcome of a commit function, not a promise that arbitrary domain state is a projection of events.

## 10. Plans

### 10.1 The plan function

A plan is optional. Applications use one only when a direct root command and its spawned descendants cannot express the required dependencies, joins, waits, or branching.

The public name is intentionally `Plan`, not `Workflow`: the execution is the whole durable workflow, while a plan is only the optional pure declaration function that coordinates some executions.

When present, a plan is a pure function of the execution's root arguments and the events, command states, and results recorded so far. It declares commands; it never performs work. It must not do I/O, read clocks, use randomness, start goroutines, or depend on mutable globals, and must be deterministic given identical inputs. Although a plan reacts to events, it does not receive one event callback; each evaluation sees the relevant durable snapshot accumulated so far. Command creation, retry-budget, next-attempt, attempt-start, and recorded-time metadata are deliberately unavailable to plans. Time-based progression is expressed through `Delay`, `Within`, execution deadlines, and declarative retry policy rather than branches over a clock.

Ordinary Go cannot be sandboxed completely. `flow` therefore enforces purity by capability and verification: a plan receives no context, client, database, transaction, clock, or worker scope; declarations are reconciled by canonical identity; panics and conflicts are plan defects; and `flowtest` can evaluate the same snapshot repeatedly and compare declarations and reads. A debug runtime option may perform the same double evaluation. A plan defect records `PlanFailed`, fails the execution, and cancels outstanding work; it never consumes a worker's retry budget or reruns a worker whose result was already accepted.

The terminal plan-defect rule also applies to the initial evaluation in `PlanDef.Execute` and evaluations caused by ingress. With a library-owned transaction, the execution and accepted triggering command or fact remain durable for inspection; the operation returns a typed error carrying the `ExecutionID`, and an idempotent retry finds the same terminal execution.

`InTx` cannot make an error durable independently of its caller. A plan-defect change set produced there remains pending in the caller-owned transaction like every other Flow write. The typed error still carries the affected execution identity, but the caller chooses whether to commit that terminal history or roll the entire composition back; returning the error from a `pgx.BeginFunc` callback normally chooses rollback. This is ordinary transaction ownership, not a different plan outcome.

`Fact` and `Facts` read events already recorded in this execution. `Children` reads a command's authoritative direct-child membership after that membership closes successfully. `Result` reads a successful command's typed result. `Outcome` reads any terminal result or structured failure, including for worker-spawned commands. A branch that genuinely changes topology according to runtime information is an ordinary Go conditional over those reads. Routine success and failure value plumbing belongs in a dependent worker through `ResultOf` or `OutcomeOf`, as shown in §5.2 and §8.5.

**Reads are recorded.** Every `Fact`, `Facts`, `Children`, `Result`, and `Outcome` call during an evaluation is registered as an input that evaluation consulted. The library records more than the public value and boolean: it classifies each read as available, temporarily unavailable, or permanently unavailable.

| Read | Available | Temporarily unavailable | Permanently unavailable |
|---|---|---|---|
| `Fact` / `Facts` | matching facts exist | no matching fact has arrived | — |
| `Children` | parent succeeded and membership is closed, including an empty set | parent is non-terminal | parent ended unsuccessfully |
| `Result` | command succeeded | command is non-terminal | command ended unsuccessfully |
| `Outcome` | command is terminal | command is non-terminal | — |

The public boolean remains deliberately small: `Children` and `Result` return false for either unavailable state, while `Outcome` is the operation for code that must distinguish success from terminal failure. The internal distinction prevents a result that can never exist from being mistaken for one that may arrive later.

**`Children`, `Result`, and `Outcome` may only read a key declared earlier with `Do` in the current evaluation or an existing command key.** A durable command may have been created by `Do`, `Spawn`, or `Issue`. Reading any other key, or supplying a command definition whose name and version do not match the key, returns `ErrInvalid`; it is a plan defect, not a runtime condition.

Reading a command's children, result, or outcome does not by itself create a dependency edge. A command that consumes those keys or values must also name the source commands with `After`, `AfterSettled`, or `AfterFailed`, so scheduling and traceability do not depend on an implicit data read. A joined worker may then read successful immutable results through `ResultOf` or any terminal outcomes through `OutcomeOf`, restricted to those explicitly named dependencies.

Plans are ordinary Go functions, so statements reached before an early return are the declarations produced by that evaluation; the runtime does not pretend source order is irrelevant. Applications should declare structural work as soon as its keys are known and reserve value reads for topology decisions. `Children`, explicit dependencies, and worker-side `ResultOf` or `OutcomeOf` remove the common need to loop over unfinished results merely to construct the next payload.

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

The exact plan name and version are therefore a capability of any runtime committing a transition that requires evaluation. Plan-declared command settlement always has that capability because plan-driven work is claimed only by replicas registering both the worker and plan. Ingress requires it only when the stored plan-read routing set says the mutation changes an input the plan reads. Merely satisfying a persisted dependency or `Await` condition is ordinary engine resolution and does not execute the plan.

| Declared key | Action |
|---|---|
| does not exist, dependencies satisfied | its command is created and becomes `ready` |
| does not exist, dependencies unsatisfied | recorded as a `pending` command with its dependencies |
| does not exist, a dependency is permanently unsatisfiable | recorded directly as `skipped` with `CommandSkipped` |
| already exists and owned by this plan | verify definition, canonical payload, topology, classification, and explicit node override; changed definition defaults leave the accepted command untouched; any other mismatch is a plan defect |
| already exists from `Spawn` or `Issue` | may be read or named as a dependency, but `Do` using its key is a plan defect |
| previously declared, no longer declared | retained unchanged |

**A plan only grows.** It cannot withdraw, rewrite, or re-point work it already declared. Application plan logic must therefore be additive: new durable facts may reveal more declarations but may not invalidate an earlier one. A mismatch is treated as a plan defect.

Operational defaults are resolved only when a command is created. Re-evaluating a key after a retry, timeout, or queue default has changed neither mutates the accepted settings nor creates a conflict. Explicit `MaxAttempts` and `RetryPolicy` node overrides remain declaration data: adding, removing, or changing one for an existing key is a mismatch. Within one evaluation, repeated declarations of a key must agree on their complete effective settings so contradictory code cannot coalesce silently.

Every command made runnable by one evaluation is created in that single transaction, so a crash exposes either all of them or none.

Reconciliation continues to a bounded fixed point only when applying a declaration batch itself creates a new plan-observable terminal transition, such as a command that is immediately skipped or expired. The engine re-evaluates against that in-transaction outcome and repeats until no such transition opens another branch. Creating ordinary `ready` or `pending` work does not force a redundant second pass: that open work already prevents completion, and its later terminal transition supplies the next required evaluation. One durable transition may therefore invoke the pure plan more than once, but invocation count is never application-visible behavior.

After the plan function returns, every command key named by a dependency builder is validated against the union of commands already durable and commands declared anywhere in that evaluation. This permits forward references within the function but rejects a typo or a reference to work that does not exist as a plan defect; such a dependency can never sit pending until the execution deadline. Future externally supplied facts use `Await`, and future externally issued commands cannot be anticipated by an undeclared dependency key.

The consulted-input set and each read's availability classification from the latest evaluation are persisted alongside the declared set. They determine both whether the execution may succeed (§10.1) and when the plan must next be evaluated (§10.3).

### 10.3 When the plan is evaluated

A plan's declared output is a pure function of its root arguments and the durable events and final command states it consulted. Evaluation is therefore required only when one of those can have changed:

- at execution start;
- when an event arrives whose name the latest evaluation consulted;
- when a final event is appended for a command read by the plan or named by one of its dependencies; or
- when the latest evaluation introduced work and a later terminal transition must establish the next no-new-command pass.

Claim, lease renewal, `running`, and `retry_wait` transitions do not trigger plan evaluation because no plan API can observe them. The event recorded when the command ends does.

Events of names the latest evaluation did not consult cannot change that evaluation's control path by themselves and do not trigger evaluation. Skipping is sound because plans are pure and facts are immutable, append-only, and durably re-readable; terminal command outcomes and closed child memberships are likewise immutable. If another consulted input later opens a branch that reads an older, previously ignored fact, that consulted input triggers evaluation and the older fact is still present for the newly reached branch.

An event named only by a node's `Await` is not a plan read. Its arrival updates the stored wait and command readiness directly; it triggers the Go plan only when the same event is also present in the latest `Fact` or `Facts` read set. This distinction lets external monitors publish ordinary release facts without linking orchestration code.

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

Writes are proportional to the delta, but the read and the Go evaluation are proportional to the whole command set, and an execution produces at least one creation entry and one terminal event per command. Total plan work over an execution's life is therefore approximately **O(commands²)**, which is what bounds plan-driven execution size.

The default safety limit is **1,000 total logical commands within one execution in every driver mode**, configured with `WithMaxCommandsPerExecution`; `0` explicitly disables it. The count includes the direct root and commands first accepted through `Do`, worker or coordinator `Spawn`, and `Issue`. Attempts, retries, equivalent plan reconciliation, duplicate-equivalent staged spawns, events, coordinator activations, and journal entries do not increase it.

This is not a queue-capacity, database-backlog, or concurrency limit. The database may hold any number of executions and queued commands, subject only to operational capacity. It protects one execution from an accidental unbounded command tree or coordinator loop. Applications intentionally running larger agents or graphs may raise or disable it; token, monetary, turn, and other domain budgets remain application policy.

The runtime validates `existing command count + genuinely new staged commands` under the execution lock before inserting any member of a batch. A worker fan-out, coordinator decision, or plan evaluation therefore never commits partially at the ceiling. Exceeding the limit is a deterministic permanent decision error rather than a retryable infrastructure error: a plan records `PlanFailed`; a worker records `CommandFailed` without committing its staged success or outputs; a coordinator records `CoordinatorFailed`; and an external `Issue` returns `ErrInvalid` without writing. `Trace` exposes the configured ceiling, accepted count, and structured limit reason.

The shared ceiling has an additional performance role for plans. At 1,000 commands a plan-driven execution performs on the order of a million narrow row reads across its lifetime; operators raising or disabling the limit for a plan accept its approximately O(commands²) cost. Direct and coordinator-driven executions do not pay repeated whole-graph evaluation cost, but use the same ceiling as a runaway-topology guard. The 100-dependency limit remains specific to each plan-declared command.

Immediate terminal cascades and lazy durable-value loading may invoke a plan more than once in one semantic transaction, but every additional fixed-point pass must follow at least one genuinely new terminal declaration. This does not change the approximately O(commands²) lifetime bound when the command ceiling is enabled. A high implementation safety guard bounds total plan invocations in one transaction, including value-loading and fixed-point passes; it is a transaction-resource guard, not a queue, concurrency, or total-command limit, and a plan that exceeds it fails deterministically rather than holding the execution lock indefinitely.

Architecture must maintain or validate the execution command count without scanning the command table, make each plan-evaluation state load one narrow indexed query, and benchmark evaluation at the documented default ceiling.

## 11. Coordinators

A coordinator is durable typed state that reacts to events. Direct-child records handle bounded command trees, and plans handle bounded joins; a coordinator exists for open-ended membership, cycles, and multi-event decisions that need durable mutable orchestration memory.

**Every execution selects exactly one driver mode.** `Command.Execute` uses a direct root command and no coordinator. `PlanDef.Execute` uses a plan as its built-in orchestration authority. `Coordinator.Execute` uses one application-defined coordinator. The modes cannot be combined within one execution. A near-term child execution will preserve this rule by giving nested work its own driver and journal rather than placing a second authority inside the parent (§2.3).

### 11.1 Definition, start activation, and instance

A definition has a stable name, positive version, typed state schema, an optional start handler declared with `OnStart`, exact typed event subscriptions declared with `On`, and typed command-terminal subscriptions declared with `OnOutcome`. Its instance holds typed canonical state, a durable inbox position, and a lifecycle of `active → completed | failed | cancelled`.

`Coordinator.Execute` durably creates the instance and enqueues one initial activation in the same transaction. It never invokes coordinator code inline. A runtime registering the exact coordinator name and version later claims the activation and invokes `OnStart`, when present; without `OnStart`, the activation is acknowledged as a no-op and the coordinator waits for events. Events, command creation, and orchestration-state changes staged by `OnStart` follow the same atomic processing and retry rules as an event handler.

`OnOutcome(command, handler)` observes the existing exactly-one terminal event for every command with that name and version and presents it as `CommandOutcome[R]`. Success carries the typed result; failure, cancellation, expiry, and skipping carry their status and structured reason. `Received.Key` is the execution-wide command key and the remaining `Received` metadata comes from that same persisted terminal event. This is a typed subscription over the ordinary event already in the journal, not another event category or a second row.

`On(command.Done(), ...)` remains the success-only convenience. Because one coordinator must make one deterministic decision for a matching journal position, registering it together with `OnOutcome(command, ...)` for the same command pair is rejected as an overlapping subscription. A coordinator that intends to decide how child failure affects the execution normally spawns that child with `Optional()` and consumes `OnOutcome`; otherwise the required child's ordinary fail-fast policy may fail the execution before a coordinator-managed fallback can complete.

### 11.2 Historical matching-event delivery

An instance begins with its inbox at the start of the execution and receives **every matching retained event in position order**, including facts recorded before the instance existed. An external fact therefore cannot be lost by arriving early. The same rule governs plan evaluation, which sees every fact recorded so far.

### 11.3 Serialized processing

At most one handler runs per coordinator instance at a time; workers and other executions run concurrently. An `On` event handler receives `Received[T]`; an `OnOutcome` handler receives `Received[CommandOutcome[R]]`. Both carry the source event's ID, key, execution-local journal position, and PostgreSQL recorded time; `On` carries the canonical event payload, while `OnOutcome` carries its lossless typed terminal-outcome view. Position rather than a timestamp determines delivery order. On a `nil` return, one transaction appends a `CoordinatorTransition` journal entry recording the activation or handled event position, prior state revision, resulting canonical state or durable state reference, and causation; appends spawned-command creation and event entries; materializes the new orchestration state and outputs; and advances the inbox. On error or lease loss none of it commits, and redelivery cannot apply a decision twice. Architecture may normalize or content-address large state versions, but retained history must preserve the same logical transition.

### 11.4 Failure

Coordinator handler errors retry under the default policy. A permanent or exhausted error marks the coordinator failed, records `CoordinatorFailed`, fails the execution, and cancels outstanding work. A deterministic staged-output violation such as exceeding the execution command ceiling is permanent immediately: none of that decision's state, inbox advance, events, or commands commit, and the coordinator fails with a structured reason rather than repeating an unrepairable decision.

### 11.5 State boundary

Coordinator state may store orchestration facts such as selected route, expected keys, observed outcome flags, or local progress. It must never become a second source of truth for application entities; balances, intent status, transaction records, and report contents remain in application-owned tables.

Coordinator handlers are for short orchestration decisions and have no application-transaction hook. Application work belongs in commands. A local domain write that is inseparable from one command's success uses that worker's declared commit function; independently retryable or externally visible work is its own command.

### 11.6 Rolling deployments

A coordinator delivery is claimed only by a runtime registering the instance's exact coordinator name and version and either the matching event name and version for `On` or the matching command name and version for `OnOutcome`. A process understanding neither side leaves the delivery pending without consuming retry budget, and unclaimable coordinator backlog is visible through inspection and observability.

## 12. Transactional guarantees

### 12.1 PostgreSQL is authoritative

All execution state is recoverable from PostgreSQL. Correctness never depends on notifications, local queues, process identity, or in-memory state. Polling alone suffices to resume all eligible work after a crash, missed notification, or listener failure.

### 12.2 At-least-once execution

Worker and coordinator handlers may run more than once, including after apparently successful user code. `flow` guarantees idempotent durable progression, not exactly-once execution of arbitrary code. External side effects require stable idempotency keys or reconciliation; `CommandID` is available as a natural one.

Sources of re-execution are retry after failure, lease loss, and shutdown interruption.

### 12.3 Fencing

Attempts and coordinator deliveries hold renewable leases. Every completion verifies current ownership and non-terminal execution state. A stalled, partitioned, cancelled, or superseded handler cannot commit its staged outputs or declared commit-function write. Leases renew automatically; handlers never implement heartbeats.

Fencing guarantees only that such a handler cannot commit **flow-managed records or its declared commit-function write**. Effects it already performed against external systems are beyond the library's control.

### 12.4 Short atomic completion

User handlers never hold a PostgreSQL transaction for the duration of their work. They perform work and stage outputs; the runtime opens a short transaction after a successful return.

That one transaction appends the event carrying the command result, its additionally emitted events, a `CommandCreated` entry for every staged child and plan-created command, attempt conclusion, and any execution outcome transition; executes the worker's optional declared commit function; and materializes the complete child set, plan reconciliation, dependencies, command state, and execution state. If ordinary settlement fails, none commits and the command is retried.

A recovered plan panic, nondeterministic conflict, or invalid plan read is different: provided the candidate success transaction itself can commit, the worker result, staged outputs, and optional commit-function write are accepted, `PlanFailed` and `ExecutionFailed` are appended, and outstanding commands are cancelled. That committed application work never becomes another worker attempt. If the commit function itself fails, however, no part of the candidate success—including the plan defect—was accepted; the whole transaction rolls back and the commit-function error follows the ordinary command retry rules.

If a worker deliberately writes application state outside its declared commit function, those writes are outside `flow` fencing and settlement atomicity. Caller-owned transactions remain atomic at execution ingress (§12.7), and external effects remain subject to at-least-once execution (§12.2).

### 12.5 Serialized execution commits

Commits within one execution are serialized by its row lock; different executions commit fully in parallel.

Serialization applies to **commits, not work**. Commands of one execution run concurrently across any number of workers and queue only at the moment they commit, so every durable transition is applied against a consistent view of the execution.

### 12.6 Bounded completion

Every non-terminal command has a bounded path to terminal: direct eligibility, dependencies that will resolve, an awaited fact with a deadline, a retry schedule, a `Within` bound, or the execution deadline. Direct executions additionally have bounded topology because every worker publishes its complete direct-child set and closes membership in one successful return and the execution-wide command ceiling bounds recursive generations unless the operator explicitly disables it. A background reconciler repairs dispatch state after crashes without duplicating work.

Three situations lie outside this and are reported rather than absorbed: an execution started with `WithoutExecutionDeadline` whose commands declare no bounds; an execution whose command ceiling is explicitly disabled and whose handlers or coordinator continue creating work indefinitely; and work for which no live replica registers the complete required capability—its command worker, its exact plan as additionally required in plan mode, or its coordinator definition. Unknown work remains durable and consumes no retry budget while that deployment gap exists.

### 12.7 Caller-owned transactions

`Runtime.InTx(tx)` returns a transaction-scoped `Client` and allows every definition's `.Execute`, plus `Issue`, `Publish`, and cancellation, to commit atomically with caller-owned application writes. Definitions may bind that capability with `With`, but the resulting value must not outlive the transaction.

The cross-boundary ordering invariant is **Flow-owned operation first, application-table writes second**. Library-owned settlement follows the same order by acquiring every required Flow lock before invoking a declared commit function. A caller-owned transaction must call its Flow operation before acquiring application-row locks and must not interleave the two categories; this is why §5.3 publishes before `MarkDelivered`. `InTx` cannot detect arbitrary locks the caller acquired earlier, so violating this documented discipline may deadlock and returns the database error rather than being retried invisibly. Architecture defines the exact Flow-internal lock order but may not reverse this invariant.

When one caller-owned transaction performs Flow operations against multiple existing executions, it must invoke them in ascending lexical `ExecutionID` order before beginning application writes. The transaction-scoped client tracks requested execution locks and returns `ErrInvalidState` before requesting a lower ID after a higher one. The ordinary one-execution case requires no sorting or additional API.

### 12.8 Causation

Outputs created inside handlers automatically inherit execution identity and causation: worker events and spawned direct children are caused by the current command; plan outputs are caused by the durable transition that triggered evaluation; coordinator outputs are caused by the start activation or event being processed; an event recording a command's final state identifies that command and transition. Callers cannot forge an origin that contradicts the active handler scope.

## 13. Cancellation, deadlines, and terminal races

`CancelExecution` marks the execution `cancelled`, cancels non-terminal commands, closes the coordinator, and records `ExecutionCancelled`. `CancelCommand` cancels one command; if it is required, the execution fails under §6.3 including its failure-handling branches.

Cancellation and completion race on the execution row, and whichever commits first wins. A handler whose command was cancelled cannot commit its result, staged outputs, or declared commit-function write.

Cancellation cannot undo external side effects already performed and cannot forcibly stop a non-cooperative goroutine; fencing only guarantees such a goroutine commits nothing.

Cancelling an already-cancelled target is idempotent; cancelling a differently-terminal target returns `ErrTerminal`.

## 14. Serialization, encoding, and limits

Command payloads, command results, event payloads, coordinator state, and all identity comparisons use deterministic canonical JSON. Idempotency compares canonical stored bytes, not caller memory layout or database formatting. The architecture defines the encoder and its treatment of custom marshalers; the functional requirement is that the same logical value always produces the same identity bytes.

Payload, state, configured execution-command-count, and dependency-count limits are enforced against the complete staged transaction before any durable write. Violations return `ErrPayloadTooLarge` or `ErrInvalid` with no partial effect, or produce the structured permanent handler-decision failure defined in §10.5 when discovered during settlement.

## 15. Inspection and graph projection

### 15.1 Execution trace

`Trace` returns, in one call:

- the execution: driver mode, type, key, status, deadline, accepted command ceiling and current count, timings, outcome, and failure;
- every command: key, name, version, state, payload, result, last error, retry schedule, deadlines, and current running duration;
- every command not yet runnable, with its creation source, parent where applicable, dependencies, and awaited facts;
- every event: name, version, key, position, payload, PostgreSQL recorded time, originating command and final-state metadata where applicable, and causation;
- attempt summaries per command, distinguishing operational interruptions from application failures;
- the causal edges linking all of the above.

The causal graph and every settled command state are derivable from retained journal entries; command, dependency, and child tables are indexed materializations used to answer the query efficiently. Live lease ownership, current running duration, and other mutable delivery details come from current operational materializations and are not reconstructed from lease-renewal noise. Because the plan records what is still expected, a trace answers both *what happened* and *what this execution is waiting for* — the latter being something pure causation cannot express.

### 15.2 History

`History` returns the immutable execution journal in per-execution commit order. It includes command creation with canonical arguments and topology, attempt starts and conclusions, application and terminal events, coordinator processing, and execution transitions. Entries have distinct kinds, but only event entries are typed application facts or plan/coordinator inputs. Supplying an after-position returns only newer entries, so a UI or CLI can poll a live execution incrementally and build a timeline or causal graph without reconciling a separately ordered command source.

### 15.3 List and await

`ListExecutions` supports bounded filtering by type, key prefix, status, time range, and metadata, with stable cursor pagination. `AwaitExecution` polls until terminal, never blocks a worker, and is not a second execution path.

Every record carries correlation and causation identifiers for joining to external tracing.

### 15.4 Retention

Terminal executions and their complete journals are retained indefinitely in Milestone 1. Archival and configurable retention are a near-term operational follow-on. A future policy may discard bulky canonical command payloads before retaining command-creation skeletons, causation, and terminal outcomes for longer; once payloads are removed, full historical plan simulation and complete projection rebuilding are no longer promised for that prefix.

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

For plan-driven executions, a pool settling a command also registers every exact plan version under which that command may run; pools still scale independently in capacity and may register only their relevant workers and plans. An API-only process needs a plan definition only for an operation that changes a currently plan-observed input. Publishing a fact used solely by a persisted `Await` remains plan-free. Coordinator-driven command settlement has no corresponding coordinator-code requirement because its terminal event may wait durably for a compatible coordinator replica.

### 16.4 Concurrency and wake-up

Concurrency is configured per process and optionally per queue lane. The runtime claims only work it can begin immediately and never builds a local backlog whose leases could expire while queued.

Wake-up uses PostgreSQL notifications when a session-capable connection is available, always with polling fallback. Poll-only operation is fully correct and is the supported mode behind transaction-pooling proxies.

### 16.5 Graceful shutdown

Shutdown stops claiming, lets running handlers finish within a grace period while renewing their leases, then cancels the remainder and releases their work for immediate re-execution. Interrupted attempts consume no retry budget.

### 16.6 Limits

Per-execution commit rate is bounded by its row lock, suiting executions of tens to hundreds of commands and agent turns with short serialized coordinator decisions. Parallel worker execution remains unconstrained by that lock until settlement. Very large independently adaptive sub-agents should eventually use child executions so their commits and journals do not all serialize through one parent row.

`WithMaxCommandsPerExecution` bounds accepted logical commands in every driver mode as specified in §10.5. The default prevents accidental infinite direct-command recursion and coordinator turn loops; it does not reserve capacity or limit commands belonging to other executions. Aggregate throughput is bounded by PostgreSQL — claim query rate and transactional notification cost — not by replica count. One database is the authority; there is no cross-region coordination.

## 17. Time and clocks

All durable scheduling, retry-budget, deadline, and lease decisions use PostgreSQL time. Application clocks never determine durable ownership, eligibility, or elapsed budget. Local clocks may drive cancellable in-process timers and duration observations, but the corresponding durable decision is validated against database time before it commits.

Timestamps follow a strict taxonomy; each answers exactly one question, and no timeout is anchored on a column the loop enforcing it can move:

| Column class | Question | Written by |
|---|---|---|
| creation time | when was this command created? | insert only, immutable |
| update time | when did anything last write this row? | every write, including claim and renewal; recovery/inspection only |
| status time | when did the durable state last change? | actual state transitions only |
| budget-start time | when did the command first become claim-eligible after dependencies, waits, and initial delay? | set once, immutable thereafter |
| next-attempt time | what is the earliest time the next claim may begin? | initial scheduling and retry scheduling; intentionally moves |
| attempt-start time | when did this particular attempt begin? | each successful claim, immutable for that attempt |

`Within` has a persisted wait-start and deadline set once when the node's command dependencies resolve, while the execution deadline has the anchor defined in §6.5. Claiming updates lease and update time and creates an attempt with its own start time; it never moves creation time, fact-wait start, budget-start time, next-attempt time, or a deadline. Retry scheduling moves only next-attempt time. Lease expiry, shutdown interruption, crash recovery, and takeover move neither the retry budget's anchor nor its deadline. This separation prevents the retry loop from granting itself a fresh elapsed budget on every pass.

The runtime supplies the accepted database values to a worker through `CommandInfo` before invoking it. `UpdatedAt` and next-attempt time are intentionally absent from that handler view: the former is maintenance state and the latter is a scheduling decision. Plans receive no clock or timing metadata (§10.1).

## 18. Configuration and defaults

| Setting | Default |
|---|---|
| attempt lease duration | 60 seconds, renewed automatically |
| attempts per command | 5 (one execution plus 4 retries) |
| elapsed retry budget | none unless configured by `RetryFor` or an explicit policy |
| retry delays | 1s, 5s, 30s, 2m, jittered |
| per-attempt timeout | none unless configured |
| execution deadline | 30 days; removable per execution |
| command payload / result size | 256 KiB |
| event payload size | 64 KiB |
| coordinator state size | 256 KiB |
| commands per execution | 1,000 across every driver mode; configurable, `0` disables |
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

The runtime emits optional, no-op-by-default observations for execution start and outcome, command creation and transitions, initial delay and eligibility, handler and commit-function duration, retries, waits and wait expiry, events published, plan evaluation size and duration, execution-command-limit usage and rejection, lease renewal and loss, claim activity, unclaimable backlog, reconciliation repairs, and long-running attempts.

Observations carry execution type and ID, command key, name and version, worker identity, correlation and causation IDs, and outcome category — never payload data. No logging, metrics, or tracing vendor is imposed; adapters are near-term follow-ons (§2.3).

The durable model is deliberately sufficient for an operational UI without a UI existing in the core runtime.

## 21. Testing support

The library ships a test package so direct command trees, workers, plans, and coordinators are testable without a database:

- a worker is an ordinary function — given command arguments and an immutable set of explicitly declared dependency results and terminal outcomes, assert `ResultOf` and `OutcomeOf` reads, its returned result or error, its staged events and spawned children, direct-child keys, optional classification, and requested `StartAfter` duration;
- a retry policy is immutable data — given synthetic database timestamps, attempt counts, and error classifications, assert its retry/stop decision and persisted next-attempt time without calling a clock or application callback;
- a declared commit function is an ordinary function of durable `Args`, `Result`, and `Info` plus a transaction capability; tests invoke it directly, assert those inputs, and use an application transaction double where available, while SQL behavior is integration-tested against PostgreSQL;
- a direct-execution harness begins with one root worker decision and settles staged descendants, allowing completion and failure policy to be asserted without defining a plan;
- a plan is a pure function — given root arguments, facts, command states, and closed child memberships, assert its declarations, dependencies, read availability, outcomes, waits, and complete expected key set; a determinism assertion evaluates the identical snapshot repeatedly and compares canonical output;
- a plan harness may advance through an ordered sequence of synthetic durable snapshots — including facts, terminal command outcomes, and closed child memberships — and return or assert the canonical declarations, dependencies, consulted inputs, and read-availability classifications after each transition, without running workers or external effects;
- a plan-routing assertion mode evaluates selected transitions that normal consulted-input routing would skip and verifies that the canonical output and reads remain unchanged;
- a coordinator harness delivers the durable start activation, ordinary events, and typed command outcomes in order, allowing `OnStart`, `On`, `OnOutcome`, state changes, outputs, delayed spawns, retry, and completion decisions to be asserted;
- direct, plan, and coordinator harnesses accept a synthetic per-execution command ceiling and assert accepted logical-command count, duplicate coalescing, and all-or-nothing rejection without treating attempts as new commands;
- the durable-agent harness scenario fans out optional tool commands, delivers mixed successful and unsuccessful outcomes in position order, parks for an external event, schedules a later turn, and reaches an explicit result without any in-memory wait.

Integration behavior is verified against real PostgreSQL: concurrent claims, lease expiry and fencing, cancellation races, crash recovery at every commit boundary, immutable retry-budget anchors across retry, interruption, restart, and takeover, elapsed-budget exhaustion independent of attempt count, changed definition defaults leaving existing commands untouched, explicit node-policy conflict detection, dependency-gated `Within` anchors, PostgreSQL-anchored `StartAfter` scheduling and deadline expiry, publish-before-declare and declare-before-publish ordering, atomic external-monitor fact publication with its application write, plan-free `Await`-only publication, exact-plan rejection for routed plan inputs, Flow-before-application and ascending multi-execution transaction ordering, exactly one command-creation journal entry per accepted command, execution-wide command-ceiling enforcement for root, `Do`, `Spawn`, and `Issue`, journal/graph reconstruction, all-or-nothing worker and coordinator fan-out and commit-function writes, authoritative child reads, batched worker dependency results and terminal outcomes, immediate-terminal fixed-point reconciliation, repeated plan evaluation creating no duplicate commands or history, unknown dependency rejection, terminally unavailable results not blocking failure, failure branches surviving fail-fast, typed coordinator fan-in across successful and unsuccessful command outcomes, durable-agent continuation after process loss, `Await` expiry, and rolling deployments with divergent registered versions.

## 22. Acceptance criteria

Milestone 1 is complete when:

- an execution can be started, traced, published to, and cancelled through the documented API;
- `*Runtime` can be passed directly wherever `Client` is accepted, without calling `rt.Client()`;
- binding a definition with `With(runtime)` returns the same static definition type, produces a concurrency-safe executable copy, and does not mutate or register the original;
- calling `With` on an already bound definition replaces the client only in the returned copy, enabling an explicit per-call runtime override without mutating a shared definition;
- calling `.Execute` on an unbound definition returns `ErrInvalid` without writing;
- the same definition may be bound independently to multiple runtimes, while a transaction-bound value cannot execute after its transaction closes;
- every newly accepted execution records exactly one ordered `ExecutionStarted` entry with its driver definition and version, canonical input, deadline, accepted command ceiling, material options, and causation; an equivalent idempotent start records no duplicate;
- each execution enforces its stored command ceiling regardless of the defaults configured on processing replicas, and an idempotent repeated start under a changed runtime default returns the existing execution without rewriting or conflicting with that ceiling;
- `Command.Execute` durably queues one typed root command and returns immediately without requiring a plan or coordinator;
- `PlanDef.Execute` durably creates the execution and atomically enqueues every command made ready by its initial pure evaluation;
- `Coordinator.Execute` durably creates the instance and queues its start activation without invoking `OnStart` inline;
- `WithMaxCommandsPerExecution` applies one accepted-logical-command ceiling to direct, plan, and coordinator executions; `0` disables it, and the ceiling never limits other executions, total database backlog, or runtime concurrency;
- the command ceiling counts each accepted root, `Do`, `Spawn`, and `Issue` command once while excluding attempts, retries, equivalent reconciliation, duplicate-equivalent staged output, events, and coordinator activations;
- a batch that would cross the ceiling commits no partial command or output; worker and coordinator decision violations are permanently classified, plan violations record `PlanFailed`, and an external `Issue` returns `ErrInvalid` without writing;
- a direct execution remains running through every required spawned descendant, then completes without relying on temporary quiescence;
- a direct required-command failure produces the configured fail-fast or settle-all result without plan evaluation;
- the worked examples in §5 compile and run against PostgreSQL;
- a mistyped command or event reference, or a wrong payload, result, or event payload type, fails to compile;
- every command that ends records exactly one event describing how it ended, with success carrying its typed result and transient attempt failures excluded;
- every accepted root, `Do`, worker- or coordinator-spawned, or `Issue` command records exactly one ordered `CommandCreated` journal entry with canonical payload, origin, parent where applicable, classification, dependencies, accepted initial schedule, policy, and causation; equivalent reconciliation appends no duplicate;
- changing a command definition's retry, per-attempt-timeout, or queue default requires no command-version bump, leaves every existing command and its accepted journaled settings unchanged, does not fail plan reconciliation, and affects only commands created afterward;
- a material plan declaration/read change or coordinator state/subscription/decision change uses a new definition version; rolling replicas never intentionally run divergent orchestration logic under one name/version pair;
- adding, removing, or changing an explicit plan-node retry override for an existing key is a plan defect, while duplicate declarations within one evaluation must agree on their effective operational settings;
- every claimed attempt records ordered start and conclusion entries, including retry scheduling or interruption where applicable, without exposing transient attempt mechanics through `Event[T]`;
- `CommandInfo` supplies immutable PostgreSQL creation, budget-start, current-attempt number, and attempt-start values; retry scheduling, claim renewal, recovery, and takeover never move the budget start, and mutable update or next-attempt timestamps are not exposed to handlers;
- a declarative `RetryPolicy` can bound retries by attempts, elapsed PostgreSQL time, or both; `RetryFor` remains bounded across process restarts regardless of attempt count, permanent errors cannot be made retryable, and the chosen next-attempt time is persisted rather than recomputed;
- a running handler's context is bounded by the earliest applicable per-attempt timeout, elapsed retry-budget deadline, and execution deadline;
- a worker that successfully spawns several children commits the complete direct-child set, every child-creation journal entry, the event recording parent success, additionally emitted events, and its optional declared commit-function write atomically;
- `StartAfter` is accepted only with a positive finite duration on worker or coordinator `Spawn`, is anchored using PostgreSQL time in the accepting transaction, persists one immutable first claim-eligible time, and records that schedule with command creation;
- a `StartAfter` command is visible immediately but holds no worker, goroutine, connection, or lease and consumes no elapsed retry budget before its first claim-eligible time; the execution deadline still expires it when applicable;
- duplicate spawns of one key must agree on `StartAfter` as well as definition, arguments, and classification; committed replay cannot move the schedule, while a handler decision that rolled back before acceptance leaves no schedule or history;
- a declared commit function receives only durable command arguments, successful result, and metadata plus the supplied transaction; its write and ordinary settlement commit or roll back together, its error follows command retry classification, and it cannot be registered dynamically from a worker;
- a successful terminal event records whether its declared commit function was applied, without exposing that operational metadata as a second application event;
- coordinator handlers expose no application-transaction hook; application work is expressed as commands, with worker commit functions reserved for local writes inseparable from command success;
- `OnOutcome(command, handler)` receives the existing exactly-one terminal event for that command name and version as a typed `CommandOutcome[R]`, including unsuccessful states without creating a second event; overlapping `On(command.Done(), ...)` and `OnOutcome(command, ...)` registration is rejected;
- a coordinator can spawn managed children as `Optional()`, consume all of their terminal outcomes through `OnOutcome`, durably fan in their results in position order, and decide the aggregate execution result without an unsuccessful child triggering automatic fail-fast first;
- the §5.4 agent can durably fan out optional tools, treat every unsuccessful tool outcome as an observation, wait for an external user event, and schedule another uniquely keyed turn without holding an in-memory loop or worker while idle;
- crashing an agent replica during a model or tool command permits ordinary lease takeover, while crashing between coordinator transitions resumes from the committed state and inbox without losing or applying a tool outcome twice;
- every successful coordinator decision records its handled activation or event, prior state revision, resulting state or durable reference, causation, inbox advance, and staged outputs atomically;
- a worker that errors, panics, loses its lease, or exceeds the configured execution-command limit after staging children commits none of them;
- equivalent repeated child keys within one handler decision coalesce, conflicting content fails atomically, and no parent retry can duplicate a committed child;
- spawned children are required by default, `flow.Optional()` removes them from execution outcome, and both classifications remain visible in `Trace`;
- `Children` returns the authoritative deterministically ordered direct-child keys after successful membership closure, including a successful empty fan-out, without relying on an application result payload;
- `ResultOf` in a worker returns typed immutable results only for commands explicitly named as that command's dependencies, rejects arbitrary graph reads, and can serve repeated reads without one database query per call;
- `OutcomeOf` in a worker returns typed immutable success or structured unsuccessful outcomes only for terminal commands explicitly named as dependencies, supports `AfterSettled` and `AfterFailed` workers, rejects non-terminal or arbitrary graph reads, and shares batched loading with `ResultOf`;
- dependency edges determine command eligibility while keys carried in `Args` select semantic inputs and order; argument keys confer no graph access, and neither `ResultOf` nor `OutcomeOf` accepts one unless it is also a durable dependency;
- re-evaluating a plan many times creates each declared command exactly once;
- a plan branch appears only once the fact deciding it exists, and never withdraws work already declared;
- every command made runnable by one plan evaluation is created in a single transaction;
- a declaration that becomes immediately skipped or expired is visible to another pure plan pass in the same transaction, while ordinary ready or pending declarations do not cause an otherwise redundant pass;
- a command with `Await` becomes runnable when its fact arrives, whether that fact was published before or after the command was declared;
- an external monitor can publish an idempotent fact through `InTx` atomically with its application-table update; a command awaiting that fact occupies no worker or lease while pending, and retained early publication still releases it when declared later;
- publishing a fact used only by a persisted `Await` does not require the monitor to register the plan; publishing a fact in the latest `Fact`/`Facts` read set requires the exact plan and writes nothing when that capability is absent;
- `Within` is rejected without `Await`, starts exactly once when all of the node's command dependencies become satisfied, does not consume predecessor runtime, and is capped by the execution deadline;
- an awaited fact that already exists when the `Within` clock would start satisfies the wait immediately; one that never arrives expires the command within the declared bound, and dependents resolve through the failure branch;
- a fact accepted under the execution lock by its `Within` deadline wins even if maintenance runs later, while a fact accepted after the deadline remains history but cannot satisfy or resurrect that wait;
- a failure branch declared with `AfterFailed` runs to completion under fail-fast, and the execution becomes terminal only after it resolves;
- a plan-driven execution succeeds exactly when every declared command is terminal, nothing remains pending or awaited, no read is temporarily unavailable, and re-evaluation declares nothing new; it never succeeds on temporary quiescence;
- a required terminal command failure reaches terminal execution failure after its explicit failure-handling subgraph resolves, even when the latest plan evaluation contains temporarily unavailable reads;
- a plan that branches on a fact which never arrives keeps its execution running until its deadline rather than reporting success;
- `Children`, `Result`, and `Outcome` read commands declared earlier in the evaluation or durably created by `Do`, `Spawn`, or `Issue`, while any other read key is rejected as a plan defect;
- plan reads distinguish available, temporarily unavailable, and permanently unavailable inputs internally: `Result` becomes available only on success, `Outcome` on any terminal state, and an unsuccessful terminal `Result` cannot block completion as though it might later succeed;
- after each evaluation, dependency keys are validated against durable commands plus every declaration in that evaluation, so forward references work and a nonexistent key fails immediately as a plan defect;
- a plan panic, conflicting declaration, or invalid read records `PlanFailed` and fails the execution without consuming worker retry budget or rerunning work whose complete success transaction committed; a commit-function error still rolls that candidate success back and follows ordinary command retry rules;
- plans cannot read command timing, retry-budget, next-attempt, attempt-start, or event recorded-time metadata and therefore cannot branch on a clock;
- the plan determinism harness detects different declarations or consulted reads from an identical snapshot, and fragment tests can assert the complete intended key set;
- the database-free plan harness can advance synthetic facts, terminal outcomes, and closed child memberships and expose canonical declarations, dependencies, consulted inputs, and read availability after each transition without executing workers;
- plan evaluation at the documented command ceiling stays within its benchmarked budget; events whose names the latest evaluation did not consult trigger no normal evaluation, a later consulted transition still exposes any stored fact to a newly reached branch, and routing assertion mode detects if a skipped evaluation would differ;
- a command result, emitted events, spawned and plan-created command journal entries and materializations, dependency resolution, execution transitions, and declared commit-function writes commit atomically or not at all;
- caller-owned transactions perform their Flow operation before acquiring application-row locks; the §5.3 composition commits both categories or neither, and the architecture never prescribes the inverse order;
- caller-owned transactions touching several existing executions invoke their Flow operations in ascending lexical `ExecutionID` order, and the transaction-scoped client rejects a reverse request before acquiring it;
- a plan defect reached through `InTx` returns its typed execution identity but remains subject to the caller's commit or rollback; Flow never commits terminal failure history behind the caller's transaction;
- a stalled worker cannot commit after losing its lease;
- an event published before its plan-declared command or coordinator existed is still observed by it, including a terminal command event matched later through `OnOutcome`;
- a coordinator receives each event with its stable event ID, execution-local journal position, and PostgreSQL recorded time; `On` exposes the event key and typed payload, `OnOutcome` exposes the command key and typed terminal-outcome view, position determines delivery order, and domain occurrence time, when needed, is part of an application payload;
- event positions are total only within one execution, and no API or projection implies a total order across executions;
- all retained journal entries share that execution-local position order; `History` can reconstruct command existence, arguments, topology, attempts, events, causation, and settled outcomes without reconciling a separately ordered command source, while lease heartbeats remain excluded maintenance;
- an idempotent republish of a stored event succeeds after the execution becomes terminal, while a genuinely new event is rejected;
- crash at any commit boundary leaves the execution recoverable and internally consistent;
- workers registering different `(name, version)` sets share a database without failing each other's work;
- `Trace` returns both what happened and what the execution is waiting for, including parent-child edges, every final command state, current running state, and attempt start history in one call, without a `CommandStarted` application event;
- worker, declared commit-function input, plan, and coordinator unit tests run without requiring a running Flow runtime; SQL integration tests use PostgreSQL.
