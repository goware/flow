---
status: complete
---

# Functional Spec: flow

## 1. Purpose

`flow` is a Go library for event-driven, durable, distributed work execution on PostgreSQL.

The basic model is:

```text
command → worker → events
                   └→ optional child commands
```

Commands instruct work. Workers perform it and may stage the application events their accepted decision produces. Plans optionally declare bounded graphs. Coordinators optionally drive adaptive or open-ended executions. Plans, workers, and coordinators request commands with one `Execute` operation. Worker/coordinator decisions stage facts with `flow.Emit`; external processes record facts with `Event.Emit`.

The runtime provides durable queuing, attempts, retry budgets, leases, fencing, dependencies, exact keyed event waits, cancellation, execution completion, ordered history, inspection, and crash takeover while application code remains ordinary typed Go.

## 2. Scope

### 2.1 Milestone 1

Milestone 1 includes:

- direct command executions and bounded worker-created command trees;
- optional pure plans with dependencies, joins, branches, external fact waits, delays, and worker-discovered fan-out;
- optional stateful coordinators for adaptive loops and open-ended membership;
- PostgreSQL command claiming using capacity bounds, leases, fencing, `SKIP LOCKED`, notification hints, and polling fallback;
- immutable per-execution journals plus current-state projections;
- typed command arguments/results, event payloads, and terminal outcomes;
- declarative retry policies with attempt and elapsed bounds;
- cancellation, deadlines, timeouts, queue lanes, local transaction composition, inspection, replay, observers, and deterministic test support;
- four real-PostgreSQL examples: direct work, fan-out/fan-in, external monitor, and durable agent.

### 2.2 Near-term follow-ons

- operational UI and telemetry adapters;
- journal archival and configurable retention;
- administrative retry, fork, repair, compensation, and explicit policy amendment;
- child executions for independently durable sub-agents;
- plan simulation over history and candidate facts;
- optional local affinity.

### 2.3 Non-goals

- global ordering or broadcast dispatch across executions;
- a Kafka replacement or high-throughput streaming system;
- event-sourcing application/domain tables;
- exactly-once execution of handlers or external effects;
- arbitrary Go-code replay;
- long-lived PostgreSQL transactions around workers;
- hard replica affinity;
- public configuration for every internal scheduler or lease constant;
- cross-database distributed transactions.

The journal is permanently execution-scoped. Future cross-execution delivery crosses an explicit idempotent export, subscription, or child-execution boundary and does not imply global ordering.

## 3. Core terminology

- **Execution:** one durable run and its journal, commands, attempts, facts, and outcome.
- **Command definition:** a typed durable work kind identified by name and version.
- **Command:** one immutable execution-local occurrence identified by `CommandID` and stable key.
- **Attempt:** one leased invocation of a command worker.
- **Worker:** the registered handler for one command name and version.
- **Event:** an immutable fact. Application events are defined with `Event[T]`; runtime terminal events are created automatically.
- **Outcome:** the typed terminal view of a command, including success and every unsuccessful state.
- **Plan:** an optional pure function declaring a monotonic bounded graph.
- **Node:** an ephemeral typed builder/read handle returned by in-execution `Execute`.
- **Coordinator:** an optional durable state machine reacting to facts for adaptive or open-ended work.
- **Causation:** the direct durable reason a command, event, or transition exists.
- **Journal:** one gap-free, commit-ordered execution history.

## 4. Public API

### 4.1 Definitions

```go
type Command[A, R any] struct { /* sealed */ }
type Event[T any] struct { /* sealed */ }
type PlanDef[A any] struct { /* sealed */ }
type Coordinator[S any] struct { /* sealed */ }

func DefineCommand[A, R any](name string, version int, opts ...CommandOption) Command[A, R]
func DefineEvent[T any](name string) Event[T]
func DefinePlan[A any](name string, version int, fn func(*Plan, A)) PlanDef[A]
func DefineCoordinator[S any](name string, version int, handlers ...Handler[S]) Coordinator[S]
```

Commands, plans, and coordinators are versioned because a running replica must execute exact registered code. Events are unversioned because they are immutable named schemas, not claimed work. A materially incompatible event payload uses a new event name. During that transition, publishers retain or route the old name until old executions waiting for it drain.

Definitions expose `Name`; command, plan, and coordinator definitions also expose `Version`. `With(Client)` returns an immutable same-type copy carrying an execution capability.

There is no public `Command.Done()` descriptor. Command terminality is observed through dependencies, `Outcome[R]`, `OutcomeOf`, `OnOutcome`, `Trace`, or `History`.

### 4.2 Retry configuration

```go
type RetryPolicy struct { /* immutable declarative data */ }

func RetryFor(maxElapsed time.Duration) RetryPolicy
func Attempts(max int) RetryPolicy
func (p RetryPolicy) Attempts(max int) RetryPolicy
func (p RetryPolicy) Backoff(delays ...time.Duration) RetryPolicy
func WithRetry(policy RetryPolicy) CommandOption
func WithTimeout(timeout time.Duration) CommandOption
func WithQueue(name string) CommandOption
```

At least one positive bound is required. `RetryFor` supplies the default backoff and elapsed bound. `Attempts` supplies an attempt bound. Either builder may add the other bound. `Backoff` accepts a non-empty ordered list of positive delays; its last element repeats.

Jitter is a fixed 20% proportional policy value and has no public builder. The effective canonical policy, including jitter, is snapshotted and persisted when the command is created. A deployment changing definition defaults affects new commands only.

No plan-node retry override exists. Materially distinct retry behavior uses a separately named command definition, which may register the same Go worker.

### 4.3 Runtime

```go
func New(db *pgkit.DB, opts ...Option) (*Runtime, error)
func (rt *Runtime) Register(registrations ...Registration) error
func (rt *Runtime) Run(ctx context.Context) error
func (rt *Runtime) InTx(tx pgx.Tx) Client

func WithWorkerConcurrency(int) Option
func WithPlanConcurrency(int) Option
func WithCoordinatorConcurrency(int) Option
func WithQueueConcurrency(queue string, concurrency int) Option
func WithPollInterval(time.Duration) Option
func WithNotifications(bool) Option
func WithShutdownGrace(time.Duration) Option
func WithObserver(Observer) Option
func WithMaxCommandsPerExecution(int) Option
func WithPlanVerification(bool) Option
```

The production command lease is fixed at 60 seconds and automatically renewed. There is no public lease option. The implementation retains an unexported in-package configuration seam so lease fault tests can run quickly.

`Runtime` implements `Client`; request-serving processes may call APIs without running background workers. `Runtime.InTx` returns a transaction-scoped client that never commits or rolls back the caller's transaction.

### 4.4 Workers and commit functions

```go
type Work[A any] struct {
    Args A
}

func (w *Work[A]) Info() CommandInfo

type Commit[A, R any] struct {
    Args   A
    Result R
    Info   CommandInfo
}

type Tx interface { /* narrow pgx-compatible database handle */ }

func Handle[A, R any](
    command Command[A, R],
    worker func(context.Context, *Work[A]) (R, error),
    opts ...WorkerOption[A, R],
) Registration

func WithCommit[A, R any](
    fn func(context.Context, Tx, Commit[A, R]) error,
) WorkerOption[A, R]
```

Workers receive no transaction and hold no Flow database connection while running. A registered commit function runs inside the short fenced settlement transaction after successful worker return. It may use only durable arguments, result, and command metadata. It performs local PostgreSQL work only and must be retry-safe if the transaction rolls back before commit.

Use no commit function when the typed result is enough. Execute a follow-up command when the work deserves its own identity and retry lifecycle. Use a commit function only when the application write and Flow success must commit atomically.

### 4.5 Outcomes and dependency reads

```go
type Outcome[R any] struct {
    Status  CommandStatus
    Result  R
    Failure *CommandFailure
}

func ResultOf[A, R any](src ResultSource, key string, cmd Command[A, R]) (R, error)
func OutcomeOf[A, R any](src ResultSource, key string, cmd Command[A, R]) (Outcome[R], error)
```

`ResultOf` succeeds only when an explicitly declared dependency succeeded. `OutcomeOf` returns every terminal state. The definition must match the dependency's durable name and version. Non-dependencies, mismatches, non-terminal reads, and unavailable success results return structured permanent errors.

### 4.6 Starting executions

```go
func (c Command[A, R]) Execute(
    ctx context.Context, key string, args A, opts ...ExecutionOption,
) (ExecutionHandle, error)

func (p PlanDef[A]) Execute(
    ctx context.Context, key string, args A, opts ...ExecutionOption,
) (ExecutionHandle, error)

func (c Coordinator[S]) Execute(
    ctx context.Context, key string, initial S, opts ...ExecutionOption,
) (ExecutionHandle, error)
```

Each method requires a bound client from `With`. It returns after durable scheduling and never runs application code inline.

Execution options are:

```go
func WithExecutionDeadline(time.Duration) ExecutionOption
func WithoutExecutionDeadline() ExecutionOption
func WithMetadata(map[string]string) ExecutionOption
func WithFailFast(bool) ExecutionOption
func WithLiveKey() ExecutionOption
func WithStartDelay(time.Duration) ExecutionOption // direct roots only
```

Stable execution keys are idempotent against equivalent starts. Live keys deduplicate only while a matching execution is non-terminal. The returned `ExecutionID` is the application integration identity.

There is no API for externally injecting a command into an existing execution. Independent work starts another direct execution; plan- or coordinator-owned topology must pass through that execution's authority.

### 4.7 In-execution command execution

```go
type Scope interface { /* sealed: Plan, Work, or Coordination */ }
type EventRef interface { /* sealed; implemented only by Event[T] */ }
type Node[R any] struct { /* ephemeral */ }

func Execute[A, R any](scope Scope, key string, cmd Command[A, R], args A) *Node[R]

func (n *Node[R]) Key() string
func (n *Node[R]) Optional() *Node[R]
func (n *Node[R]) Delay(time.Duration) *Node[R]

func (n *Node[R]) After(keys ...string) *Node[R]
func (n *Node[R]) AfterSettled(keys ...string) *Node[R]
func (n *Node[R]) AfterFailed(keys ...string) *Node[R]
func (n *Node[R]) WaitFor(event EventRef, key string) *Node[R]
func (n *Node[R]) Within(time.Duration) *Node[R]

func (n *Node[R]) Outcome() (Outcome[R], bool)
func (n *Node[R]) Children() ([]string, bool)
```

`Execute` stages or declares work; it performs no handler invocation. `Key`, `Optional`, and `Delay` work in all three scopes. Dependency methods, `WaitFor`, `Within`, `Outcome`, and `Children` are plan-only. Calling a plan-only method on a worker/coordinator node records a deterministic scope defect. The entire enclosing decision then fails even if application code ignores the node.

Worker and coordinator decisions may also stage typed application events:

```go
func Emit[T any](scope Scope, event Event[T], key string, payload T) error
```

`flow.Emit` performs no SQL. The key must be stable and non-empty, and the canonical payload must fit the 64 KiB application-event bound. Multiple events are allowed. Identical repetitions coalesce; differing content for the same `(event name,key)` poisons the decision. Plans are not an emitting scope: attempted use poisons plan reconciliation.

A successful fenced worker settlement commits staged events, staged children, the typed result and command terminal event, and an optional commit-function write in one transaction. Worker failure, panic, timeout, cancellation, lease loss, settlement fault, or commit-function rollback exposes none of those staged events or children. Coordinator events commit with state, inbox advancement, commands, transition history, and optional terminal decision. Event journal order is deterministic by event name and key.

`Node[R]` is local to one plan evaluation or handler decision and must not be retained or shared. `Key()` returns its durable reference.

Plan `Outcome()` is unavailable only while the command is non-terminal. Once terminal it returns an `Outcome[R]`, including failure. `Children()` becomes available only when a successful parent has atomically closed its child membership.

### 4.8 Plans and facts

```go
func Fact[T any](p *Plan, event Event[T], key string) (T, bool)
func Facts[T any](p *Plan, event Event[T]) []T
```

`Fact` reads one exact `(event name, key)`. `Facts` returns all retained facts for the definition in journal order. Reads and node outcome/children availability are recorded for testing, diagnostics, and historical simulation; no separate plan-read table is required.

Plans compose through ordinary Go functions. Reusable fragments accept `*Plan` and an explicit command-key prefix.

### 4.9 External event ingress

```go
func (e Event[T]) Emit(
    ctx context.Context,
    client Client,
    executionID ExecutionID,
    key string,
    payload T,
) error
```

`Event.Emit` is external application-event ingress. It is for API processes, webhooks, and external monitors, not worker or coordinator handlers; unlike `flow.Emit`, it commits independently of attempt settlement and fencing. The runtime rejects use through an attempt context and poisons that decision. The key is mandatory, non-empty, and known by publisher and waiter before occurrence. A runtime-generated transaction hash or provider ID belongs in the payload unless it was already the shared correlation key.

Identity is `(ExecutionID, event name, key)`. Equivalent canonical repetition succeeds without another row; different content conflicts. A new fact against a terminal execution is rejected, while an equivalent retry of an already accepted fact succeeds.

In plan mode, acceptance resolves exact stored waits and marks the execution dirty in the same transaction. Publisher processes do not register the plan. In coordinator mode, the retained journal position becomes available to the coordinator inbox. In direct mode the fact is retained history but does not alter topology.

### 4.10 Coordinators

```go
type Coordination[S any] struct {
    State S
}

func OnStart[S any](handler func(context.Context, *Coordination[S]) error) Handler[S]
func On[S, T any](
    event Event[T],
    handler func(context.Context, *Coordination[S], Received[T]) error,
) Handler[S]
func OnOutcome[S, A, R any](
    cmd Command[A, R],
    handler func(context.Context, *Coordination[S], Received[Outcome[R]]) error,
) Handler[S]

func (c *Coordination[S]) Succeed()
func (c *Coordination[S]) Fail(reason error)
```

`OnOutcome` is a typed subscription over the command's one existing terminal journal event. It receives success, failure, cancellation, expiry, and skip exactly once per command occurrence. It creates no second event type.

Coordinator handlers may mutate `State`, call `Emit` and `Execute`, and stage exactly one compatible terminal decision. They return `nil` to accept state, events, commands, inbox advance, and terminality atomically. Handler errors discard the decision and retry the same delivery under the coordinator's persisted accepted retry policy.

`Succeed` is invalid while required commands remain non-terminal after the staged decision. An accepted `Succeed` journals and cancels optional outstanding or newly staged commands in the same transaction; `Fail` does the same for every outstanding or newly staged command before terminalizing the execution. Calling both, calling either incompatibly more than once, or mutating after terminality poisons the decision.

The accepted coordinator retry policy is the runtime default at creation and is persisted in the coordinator projection and journal. M1 exposes no coordinator retry option; restarts and rolling deployments cannot change existing delivery behavior.

### 4.11 Inspection and cancellation

```go
func GetExecution(ctx context.Context, c Client, id ExecutionID) (Execution, error)
func ListExecutions(ctx context.Context, c Client, filter ExecutionFilter) ([]Execution, error)
func Trace(ctx context.Context, c Client, id ExecutionID) (ExecutionTrace, error)
func History(ctx context.Context, c Client, id ExecutionID, page HistoryPage) (HistoryResult, error)
func AwaitExecution(ctx context.Context, c Client, id ExecutionID) (Execution, error)
func CancelCommand(ctx context.Context, c Client, id CommandID, reason string) error
func CancelExecution(ctx context.Context, c Client, id ExecutionID, reason string) error
```

Application code persists `ExecutionID` with its domain object, preferably in the same transaction as the start. `GetExecution` is exact. `ListExecutions` is an operational browsing/filter API, not application identity lookup. Repeating an identical keyed start recovers from an ambiguous response.

## 5. Worked examples

### 5.1 Direct background command

```go
var SendReceipt = flow.DefineCommand[SendArgs, SendResult]("send_receipt", 1)

func sendReceipt(ctx context.Context, work *flow.Work[SendArgs]) (SendResult, error) {
    fmt.Printf("sending receipt for %s\n", work.Args.OrderID)
    time.Sleep(25 * time.Millisecond) // stand-in for application work
    return SendResult{Sent: true}, nil
}

rt.Register(flow.Handle(SendReceipt, sendReceipt))

handle, err := SendReceipt.With(rt).Execute(
    ctx,
    "receipt/"+orderID,
    SendArgs{OrderID: orderID},
)
```

Another compatible replica may execute the worker. The direct execution succeeds when its required closed command tree settles.

### 5.2 Dynamic fan-out and fan-in

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

func prepareReport(ctx context.Context, work *flow.Work[PrepareArgs]) (PrepareResult, error) {
    analyses, err := determineAnalyses(ctx, work.Args.CompanyID)
    if err != nil {
        return PrepareResult{}, err
    }
    for _, analysis := range analyses {
        flow.Execute(work, "analysis/"+analysis.ID, AnalyzePart, analysis.Args)
    }
    return PrepareResult{Count: len(analyses)}, nil
}

func generateReport(ctx context.Context, work *flow.Work[GenerateArgs]) (ReportResult, error) {
    results := make([]AnalysisResult, 0, len(work.Args.AnalysisKeys))
    for _, key := range work.Args.AnalysisKeys {
        value, err := flow.ResultOf(work, key, AnalyzePart)
        if err != nil {
            return ReportResult{}, err
        }
        results = append(results, value)
    }
    return buildReport(results), nil
}
```

The parent success atomically closes membership. The plan reads the authoritative child keys and declares the join. A settle-all partial report uses optional children, `AfterSettled`, and `OutcomeOf`.

### 5.3 External monitor releases a wait

```go
var BridgeDelivered = flow.DefineEvent[BridgeDelivery]("bridge_delivered")

func planIntent(p *flow.Plan, args IntentArgs) {
    origin := flow.Execute(p, "origin", SendOrigin, args.Origin)
    flow.Execute(p, "confirm", ConfirmBridge, args.Confirm).
        After(origin.Key()).
        WaitFor(BridgeDelivered, args.DeliveryKey).
        Within(time.Hour)
}

err := withTx(ctx, db, func(tx pgx.Tx) error {
    if err := intents.MarkBridgeDelivered(ctx, tx, intentID, delivery.TxHash); err != nil {
        return err
    }
    return BridgeDelivered.Emit(
        ctx,
        rt.InTx(tx),
        executionID,
        "intent/"+intentID,
        delivery,
    )
})
```

The waiting command holds no worker, connection, goroutine, or lease. Emit-before-wait and wait-before-emit both progress because facts are retained and wait resolution is transactional.

### 5.4 Durable adaptive agent

```go
var ResearchAgent = flow.DefineCoordinator[AgentState](
    "research_agent",
    1,
    flow.OnStart(func(ctx context.Context, c *flow.Coordination[AgentState]) error {
        flow.Execute(c, "turn/1", Think, ThinkArgs{Turn: 1}).Optional()
        return nil
    }),
    flow.On(UserMessage, onUserMessage),
    flow.OnOutcome(Think, onThought),
    flow.OnOutcome(RunTool, onToolOutcome),
)

func onThought(
    ctx context.Context,
    c *flow.Coordination[AgentState],
    received flow.Received[flow.Outcome[ThinkResult]],
) error {
    if received.Payload.Status != flow.StatusSucceeded {
        c.Fail(errors.New("model command did not succeed"))
        return nil
    }
    if received.Payload.Result.Final {
        c.State.FinalResultRef = received.Payload.Result.ResultRef
        c.Succeed()
        return nil
    }
    for _, tool := range received.Payload.Result.Tools {
        flow.Execute(c, "tool/"+tool.ID, RunTool, tool.Args).Optional()
    }
    return nil
}
```

State, child commands, inbox position, and terminal decisions are atomic. A later turn uses `Node.Delay`; process loss between turns or during a command is recovered from PostgreSQL.

## 6. Execution and command lifecycles

### 6.1 Driver modes

Every execution selects exactly one mode:

- **direct:** one root and its recursively closed child tree;
- **plan:** a pure monotonic declaration function;
- **coordinator:** explicit durable state and terminal decision.

The modes do not mix authorities. External command injection is absent.

### 6.2 Execution state

```text
running → succeeded
running → failing → failed
running|failing → cancelled|expired
```

Required command failure enters `failing`. With fail-fast enabled, outstanding required work is cancelled while declared failure handling may settle. With fail-fast disabled, existing work settles before failure completes. Optional command failure does not determine the execution outcome.

Direct success requires its root and every required descendant to succeed and all child memberships to close. Plan success additionally requires no open command, wait, or dirty reconciliation work and a quiescent evaluation. Coordinator success requires an explicit staged `Succeed` decision with no required open work.

### 6.3 Command state

```text
pending → ready → running → succeeded
                    └→ retry_wait → ready
pending|ready|running|retry_wait → failed|cancelled|expired|skipped
```

Scheduled/delayed commands remain pending with a future eligible time. Dependencies and exact event waits materialize separately and move the command to ready only when all requirements are satisfied.

Every accepted command has one stable ID and key. Attempts use separate IDs and ordinals. Interruption and lease loss create operational attempt conclusions without consuming retry budget. Handler error, panic, or attempt timeout may consume budget. Permanent errors terminate immediately.

### 6.4 Retry timing

`BudgetStartedAt` is set once when the command first becomes claim-eligible after dependencies, waits, and initial delay. `NextAttemptAt` moves on retry. `AttemptStartedAt` is per invocation. All are PostgreSQL times.

The runtime caps handler context by the earliest applicable attempt timeout, retry elapsed deadline, and execution deadline. A retry choice and absolute next-attempt time are persisted once; restarts never recompute jitter.

## 7. Graph semantics

### 7.1 Command keys

Keys are non-empty, UTF-8, bounded, and unique within one execution. A definition name identifies a kind; a key identifies an occurrence. Equivalent repeated declaration or staging coalesces. Different definition, version, arguments, classification, schedule, or dependencies under one key is a conflict.

### 7.2 Dependencies

- `After(keys...)`: all named commands succeeded.
- `AfterSettled(keys...)`: all named commands reached any terminal state.
- `AfterFailed(keys...)`: all named commands ended unsuccessfully.

Forward references within one plan evaluation are legal. At evaluation end, every dependency key must exist durably or be declared in that evaluation. Missing keys are plan defects rather than permanent waits.

There is no quorum predicate in M1. Settle-all partial results use optional commands and `AfterSettled`; early quorum or race behavior uses a coordinator.

### 7.3 Event waits

`WaitFor(event, key)` waits for one exact application fact. Repeated calls are AND conditions. `Within(d)` is valid only with at least one wait and starts after all command dependencies have become satisfied. Facts already retained at that point satisfy immediately.

The persisted wait deadline decides races. A fact committed after it cannot resurrect an expired command even if maintenance observes both later.

### 7.4 Dynamic fan-out

A worker's successful decision atomically creates every child and closes membership. Partial membership never commits. A plan reads `parent.Children()` only after successful closure. Failed parents have no usable successful membership and cannot keep a failing execution alive through an impossible read.

## 8. Journal and event semantics

### 8.1 Ordering

Each execution owns a position counter locked on its execution row. Every semantic transition allocates journal positions while holding that lock. Positions are gap-free and commit-ordered within the execution. No ordering exists across executions.

### 8.2 Required history

The journal includes:

- `ExecutionStarted`;
- `CommandCreated` for every command, with arguments, origin, parent, classification, dependencies, waits, accepted schedule/policy, and causation;
- attempt start/conclusion and retry schedule entries;
- application events from staged decisions or external ingress;
- exactly one terminal event per command;
- plan reconciliation/defect entries;
- coordinator transitions/failure;
- execution terminal event.

Running/started is derived from command projection and attempt history and is deliberately not an application event.

### 8.3 Terminal outcome rule

Every command records exactly one terminal event. Success carries `R`; unsuccessful terminality carries status and structured failure where applicable. `Outcome[R]` is the typed view over that one entry. This invariant plus `CommandCreated` makes graph existence and settled state reconstructible from the journal.

### 8.4 Application event identity

Application facts use `(execution, event name, key)` uniqueness whether staged or externally ingressed. Event bodies are canonical JSON. Equivalent repetition coalesces; disagreement returns `ErrConflict` and poisons an enclosing decision. Event names are schema identities and never mutate stored history.

## 9. Plan reconciliation

Starting a plan execution and every later plan-visible fact or command terminal event set `plan_dirty` under the execution lock. A dedicated plan scheduler claims dirty executions with `SKIP LOCKED`, loads one consistent snapshot, evaluates the exact registered plan, validates its complete declaration, and accepts the delta atomically.

Triggers and reconciliation serialize on the execution row. Clearing dirty therefore cannot miss a committed input: any input before the clear is in the snapshot; any later input re-dirties.

Plans are pure, monotonic, and may be over-evaluated. Facts and outcomes are immutable and re-readable. A debug/test mode may double-evaluate identical snapshots and may always evaluate when a skip optimization would otherwise apply, asserting the declaration remains unchanged.

Plan defects occur only in the library-owned reconciliation transaction. They never roll back a committed worker result or application commit function. A defect records `PlanFailed` and fails the execution.

Plan reads are ephemeral evaluation diagnostics, not a durable table. `PlanReconciled` records the accepted declaration keys/fingerprints without duplicating command payloads already in `CommandCreated` entries.

## 10. Coordinator processing

One coordinator instance exists per coordinator-driven execution. Delivery is serialized by its inbox position and fenced lease. The runtime finds the next matching application event or command terminal outcome above the inbox, invokes exactly one handler, and atomically commits state, staged events, staged commands, inbox advance, and optional terminal decision.

Unmatched positions do not require application callbacks. Indexed selection must avoid repeatedly scanning a large unmatched prefix for sparse command kinds.

Handler error preserves inbox position and retries the stable delivery. Exhaustion records coordinator failure, fails the execution, and cancels outstanding work. The accepted retry policy is persisted at execution creation.

## 11. PostgreSQL transactions and locking

Flow uses `READ COMMITTED` and one ordered semantic transaction executor.

For one execution, the blocking lock order is:

```text
execution → command/coordinator/plan rows → graph/wait rows → journal/projections → application rows
```

The claim path acquires only skip-locked locks and never waits, so it is exempt from the blocking lock order. Lease renewal is a fenced single-statement update.

Caller-owned composition performs Flow operations first, then application writes. Multi-execution transactions acquire execution rows in ascending `ExecutionID`; requesting a lower ID after a higher one is rejected before blocking.

Worker settlement validates the fence, obtains PostgreSQL time, records attempt and terminal semantics, creates children, resolves graph consequences, invokes the registered application commit function, and commits once. A stale worker writes neither Flow nor application state.

## 12. Distributed runtime

Each runtime uses one notification listener for wake hints and bounded scheduler loops for commands, plans, coordinators, and maintenance. Notifications carry no correctness state. Every scheduler also polls.

Command claiming:

1. respects global and queue-lane capacity before borrowing work;
2. selects eligible rows with `FOR UPDATE SKIP LOCKED`;
3. claims only registered name/version pairs;
4. commits the lease and attempt start;
5. releases the connection before invoking the worker.

Multiple replicas may claim different commands concurrently. One command is owned by at most one valid lease token at a time. If a process disappears, expiry recovery returns the command to its durable pre-attempt schedule without moving its budget anchor.

## 13. Serialization, limits, and defaults

All typed durable values use canonical JSON. Unknown fields follow Go JSON decoding behavior established by the codec. Invalid or non-canonicalizable values fail before durable acceptance.

Default limits:

- command arguments: 256 KiB;
- command results and terminal event carrying the result: 256 KiB;
- application event payload, staged or external: 64 KiB;
- coordinator state: 256 KiB;
- execution metadata: 16 KiB;
- command/execution key: 1024 bytes;
- command lease: 60 seconds;
- retry: 5 attempts with 1s, 5s, 30s, and 2m delays plus fixed 20% jitter;
- execution deadline: 30 days;
- maximum commands per execution: 1,000;
- plan scheduler concurrency: 1 per runtime;
- poll interval: 1 second.

The command ceiling is per execution topology, not database backlog or concurrency. `0` disables it. Attempts, events, retries, and equivalent declarations do not increment it.

## 14. Errors and guarantees

Public errors support `errors.Is` against stable categories including invalid input, conflict, not found, invalid state, terminal, payload too large, and lease lost.

Worker errors are retryable by default. `Permanent(err)` prevents retry; `RetryAfter(d, err)` requests a delay while remaining subject to policy bounds. Attempt timeout and panic are classified explicitly.

Flow guarantees:

- durable at-least-once scheduling;
- one accepted terminal outcome per command;
- fenced settlement and application commit function;
- no lost wake-up for exact event waits;
- no missed plan input under dirty reconciliation;
- idempotent starts, command declarations, child staging, and staged/external facts by stable identity;
- crash takeover without relying on process memory.

Flow does not guarantee exactly-once execution of workers or external effects. Workers use stable command identity when reconciling external calls.

## 15. Deployment

Deployments may combine API and runtime roles or split them into independently scaled pools. Worker pools register only the command versions they execute. Plan replicas register plan versions. External event emitters need only event definitions and a client. Coordinator command workers do not need coordinator code merely to settle commands; retained terminal events wait for a coordinator-capable replica.

Rolling deployments may contain several command, plan, or coordinator versions. Claims match exact versions. Event schema transitions use distinct names and retain old-name publisher support until old executions drain.

## 16. Testing and examples

`flowtest` provides deterministic in-memory decision tests for workers, staged events, plans, coordinators, commits, outcome delivery, scope poisoning, and simulation. It exposes staged events in worker, coordinator, and direct results. It does not replace PostgreSQL integration tests.

The repository must include executable and E2E forms of all four §5 examples. E2E tests use real PostgreSQL, run Flow itself without fakes, and assert public results, `Trace`, `History`, journal order, graph edges, retries where relevant, and terminal state. Application services may use short sleep/print stubs.

The replay reducer grows with every journal write path. Tests compare replayed state with stored projections. Fault injection covers ingress, claim, settlement, plan, coordinator, maintenance, and ambiguous commit boundaries.

## 17. Acceptance criteria

### 17.1 Public surface

- `Command.Execute`, `PlanDef.Execute`, `Coordinator.Execute`, and package `flow.Execute` all mean durable asynchronous execution.
- Plans, workers, and coordinators use `flow.Execute`; `Do`, `Spawn`, `Issue`, and compatibility aliases do not exist.
- `flow.Emit` stages worker/coordinator application events and `Event.Emit` performs external ingress; `Publish` does not exist.
- `EventRef` is sealed and implemented only by `Event[T]`; event definitions have no version.
- `Node[R]` preserves result typing and exposes `Key`, `Optional`, `Delay`, plus plan-only dependency/wait/read methods.
- `Outcome[R]` is the single terminal value used by `Node.Outcome`, `OutcomeOf`, and `OnOutcome`; `CommandOutcome` does not exist.
- `AfterAny`, plan-node retry overrides, `Command.Done`, external execution lookup by type/key, configurable jitter, and public command-lease configuration do not exist.
- `WithRetry` is the sole command retry option; `RetryFor` and `Attempts` are the policy constructors.
- Coordinator terminality uses `c.Succeed()` and `c.Fail(err)`; free completion functions and a public coordinator terminal scope do not exist.
- `WithCommit` remains the sole application-write hook; no `Work.Tx` or dynamic commit closure exists.

### 17.2 Execution behavior

- Direct background work needs no plan or coordinator and may execute on another replica.
- Worker child creation and membership closure commit atomically with successful parent outcome.
- Worker events, children, typed terminal output, and optional commit-function writes share one fenced transaction; every unsuccessful boundary exposes none of the staged output.
- Plan declarations are pure, monotonic, exact-keyed, and reconcile without duplicates.
- Missing dependency keys are defects rather than silent permanent waits.
- Unsuccessful commands always yield `Outcome[R]` and cannot create a permanently absent plan read.
- Exact event keys release only matching waits; emit-before-wait and wait-before-emit both succeed.
- `Within` starts after command dependencies settle and a late event cannot resurrect expiry.
- Coordinator `OnOutcome` observes every command terminal state exactly once.
- Coordinator `Succeed`/`Fail` commits atomically with state, events, commands, and inbox; invalid combinations poison the whole decision.
- Retry policy, jitter, budget anchor, and coordinator delivery policy remain stable across restarts and rolling deployments.

### 17.3 PostgreSQL and distribution

- All tables use the `flow_` prefix and share the application's database.
- Capacity-bounded claims use `SKIP LOCKED` and hold no connection while a worker runs.
- The public production lease is fixed at 60 seconds; in-package tests use only an unexported seam.
- Notifications are hints and poll-only operation is correct.
- A stale lease cannot settle Flow or application state.
- Caller-owned transactions use Flow-before-application and ascending multi-execution lock order.
- External facts work through `Runtime.InTx` and publishers need no plan registration.
- Direct, plan, coordinator, API-only publisher, and split worker deployments pass multi-replica tests.

### 17.4 History and operations

- Every command has one `CommandCreated` entry and exactly one terminal event.
- History plus replay reconstructs command existence, topology, attempts, facts, and terminal state.
- `Trace` shows causes, parents, dependencies, waits, attempts, and execution outcome.
- Applications persist `ExecutionID`; `ListExecutions` remains operational filtering only.
- Stored coordinator retry policy is retained even though it is not publicly configurable.
- All four real-PostgreSQL examples and their E2E variants pass without unexpected skips.
