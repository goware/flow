---
status: complete
---

# flow

## Summary

`flow` is a Go library for event-driven, durable, distributed work execution backed by PostgreSQL.

Its core loop is deliberately small:

```text
command  →  worker  →  event
                     └→ optional child commands
```

The developer model is:

- **Commands** are durable instructions to perform work.
- **Workers** handle commands and perform that work.
- **Events** record facts about what happened.
- **Plans**, when used, react to recorded events and coordinate the overall execution.
- **Workers** may spawn child commands when their work reveals more work.

When a worker returns successfully, `flow` automatically records an event carrying its typed result. When a command instead ends in failure, cancellation, expiry, or skipping, `flow` records an event describing that fact. A worker may explicitly emit additional application facts. All are ordinary events.

Every execution also has one immutable, ordered **journal**. The journal is broader than the developer-facing event abstraction: it records command creation, attempt lifecycle, events, terminal outcomes, execution transitions, causation, and the graph edges needed to explain why work exists. An `Event[T]` is a plan- and coordinator-visible fact recorded in that journal; operational entries such as `CommandCreated` and `AttemptStarted` are history, not additional kinds of application event. The journal is the complete durable account of the execution decisions and lifecycle transitions accepted by `flow`, not a claim that `flow` can observe every effect in an external system.

Plans are optional. The simplest use starts one command directly as durable background work; its worker may spawn bounded child commands, and the execution finishes after the root and all required descendants finish. When progression needs dependencies, joins, waits, or branches across commands, the application adds a **plan**: a small pure function that declares commands by durable key. The plan does not receive one event callback. It is re-evaluated over all relevant events and command results recorded so far; it never sleeps in memory. For open-ended processes whose membership cannot close with one worker return, a hand-written **coordinator** reacts to events directly.

This also makes `flow` suitable for long-running durable agents without adding an agent-specific framework. One agent run or bounded episode is an execution; its coordinator is the durable control loop; model calls, tools, and bounded sub-agent tasks are commands; and their results and external observations are events. A long-lived execution is composed of bounded worker invocations rather than one worker retaining an agent loop in memory. A coordinator may schedule its next command for later without occupying a worker, and the journal preserves every accepted turn, tool call, failure, retry, and causal decision across replica failure and takeover.

The intended experience is closer to using an in-process Go library than operating Kafka or a separate workflow platform: a small type-safe API, ordinary Go handlers, PostgreSQL transactions, and one operational backend.

`flow` is distributed by design. Calling `.Execute` durably queues work in PostgreSQL rather than assigning it to the calling process. Any compatible application replica running `Runtime.Run` may claim that command, record its event, and enqueue the next commands; successive parts of one execution may therefore run on different replicas. Leases, retries, and fencing provide automatic takeover when a process disappears, while PostgreSQL remains the durable authority for the complete execution.

## Motivation

Real application workflows are rarely simple linear job chains. A cross-chain intent may need to wait for a deposit, create origin and destination transactions in parallel, monitor external systems, send bridge gas, join multiple outcomes, create edge transactions, and select additional work according to the route provider. Failures can occur at every stage, and part of the shape only becomes known while the execution is running.

Hand-rolling this produces a status column per table, a poll loop per status, a bespoke claim query, a stale-recovery sweep, and timeout rules that must be documented so nobody breaks them. The failure modes are silent: work that waits forever because a release signal was lost, or a timeout that never fires because the clock it reads is reset by the loop that polls it.

`flow` makes that infrastructure a library, and keeps each unit of application code small:

- A **command** is a durable instruction to perform work.
- A **worker** handles one command kind, may atomically spawn direct child commands, and returns its typed result.
- An **event** records a fact about what happened and may be observed by anything interested.
- An optional **plan** purely declares high-level commands and joins over recorded events and command states.
- A **coordinator** is the escape hatch: durable memory that reacts to events when a plan is not enough.

## Product model

### Commands

A command is a durable, immutable request for work. It has a typed name, version, payload type, and **result type**. It belongs to an execution and is delivered to a compatible worker with lease, attempt, and retry semantics. One logical command keeps the same `CommandID` across its separately recorded attempts.

A command is the executable vertex of the projected graph; `flow` needs no second node abstraction. A deterministic `CommandKey` gives it a stable, human-readable identity within its execution and an idempotency boundary for repeated decisions. Plan-declared, worker-spawned, coordinator-spawned, and externally issued commands share one key namespace and one lifecycle.

For ordinary background work, calling `.Execute` on a runtime-bound copy of a command definition creates an execution and enqueues its root command together. No plan or coordinator is involved. The root worker may spawn direct children, those children may spawn their own children, and each successful worker return permanently closes that command's child membership. This lets the runtime know when the whole command tree is finished without guessing from temporary quiescence.

```go
h, err := SendReceipt.With(rt).Execute(ctx, "receipt/"+orderID, args)
```

The same verb starts every execution mode:

```go
receipt, err := SendReceipt.With(rt).Execute(ctx, receiptID, receiptArgs)
report,  err := ReportPlan.With(rt).Execute(ctx, reportID, reportArgs)
intent,  err := IntentCoordinator.With(rt).Execute(ctx, intentID, initialState)
```

Each call returns after durable scheduling. A command enqueues its root command. A plan evaluates its initial pure declaration and enqueues every command that is ready. A coordinator enqueues its initial activation. No worker or coordinator handler runs inline in the caller.

Frequently used definitions may be bound once to a runtime:

```go
sendReceipt := SendReceipt.With(rt)
h, err := sendReceipt.Execute(ctx, receiptID, receiptArgs)
```

`With` returns an immutable copy of the same definition type carrying the runtime capability; it never mutates the global definition or introduces a wrapper type. Calling `With` again replaces the capability only in the new copy, so an explicit one-call override remains concise:

```go
h, err := SendReceipt.With(runtimeOverride).Execute(ctx, receiptID, receiptArgs)
```

Applications can collect runtime-bound commands, plans, and coordinators in one dependency struct:

```go
type AppFlows struct {
    SendReceipt flow.Command[ReceiptArgs, ReceiptSent]
    Report      flow.PlanDef[ReportArgs]
    Intent      flow.Coordinator[IntentState]
}

func NewAppFlows(rt *flow.Runtime) AppFlows {
    return AppFlows{
        SendReceipt: SendReceipt.With(rt),
        Report:      ReportPlan.With(rt),
        Intent:      IntentCoordinator.With(rt),
    }
}

appFlows := NewAppFlows(rt)
h, err := appFlows.SendReceipt.Execute(ctx, receiptID, receiptArgs)
```

`Runtime` itself satisfies the lightweight client capability, whether or not `Run` is called. `InTx` returns a transaction-scoped capability for the same APIs. The separate capability matters for atomic composition, but ordinary application code passes or binds the runtime directly and never needs an `rt.Client()` step.

### Workers

A worker registers a handler for one command kind and version. The handler receives typed arguments and returns a typed result.

Both types are the application's own — `flow` supplies no wrapper around either:

```go
type SendArgs struct {
    TxnID string
    Txn   Transaction
}

type SendResult struct {
    Hash    string
    GasUsed uint64
}

var SendTxn = flow.DefineCommand[SendArgs, SendResult]("send_txn", 1)

func sendTxn(ctx context.Context, work *flow.Work[SendArgs]) (SendResult, error) {
    sent, err := relayer.SendOnce(ctx, string(work.Info().CommandID), work.Args.Txn)
    if err != nil {
        return SendResult{}, err
    }
    return SendResult{Hash: sent.Hash, GasUsed: sent.GasUsed}, nil
}

func commitSendTxn(ctx context.Context, tx flow.Tx, c flow.Commit[SendArgs, SendResult]) error {
    return transactions.MarkSent(ctx, tx, c.Args.TxnID, c.Result.Hash, c.Result.GasUsed)
}

rt.Register(flow.Handle(SendTxn, sendTxn, flow.WithCommit(commitSendTxn)))
```

A command declares its payload type and its result type together, and the handler is an ordinary Go function over them. Use `flow.None` as the result type for a command that produces nothing meaningful.

Conceptually, workers emit events. In the API, returning `(result, nil)` automatically records the command's event carrying that result. Workers call `flow.Emit` only for additional application facts. A retryable error does not record a final event because the command has not finished.

Most workers need nothing beyond that return value. When a command must make a short PostgreSQL application write inseparable from its success, its registration may add one declared commit function with `WithCommit`. The function receives only the command's durable arguments, successful result, metadata, and the settlement transaction. It runs after the worker returns, inside the same short transaction that records the result event and staged outputs. It replaces dynamic `OnCommit` closures: values that drive the write belong in the typed result, where history can explain them. The function is optional, deterministic, directly testable, and restricted to local database work; it must not call external services, read mutable process state, or recursively invoke `flow`.

This transactional tail and an explicit follow-up command serve different purposes. A database write that is part of the meaning of one command's success belongs in its commit function. Work that deserves its own identity, retry policy, event, or graph vertex is another command. Slow or external effects stay in the worker and use the stable `CommandID` for idempotency or reconciliation.

When work discovers a complete bounded fan-out, the worker may spawn those children directly. The worker does not need to duplicate their keys in its result merely so orchestration can find them:

```go
func prepareReport(ctx context.Context, work *flow.Work[PrepareArgs]) (flow.None, error) {
    analyses, err := determineAnalyses(ctx, work.Args.CompanyID)
    if err != nil {
        return flow.None{}, err
    }

    for _, analysis := range analyses {
        key := "analysis/" + analysis.ID
        if err := flow.Spawn(work, key, AnalyzeReportPart, analysis.Args); err != nil {
            return flow.None{}, err
        }
    }
    return flow.None{}, nil
}
```

`Spawn` is asynchronous: it stages a direct child rather than calling its handler. On success the journal records every child creation, and all children, the parent's result event, extra events, and any declared commit-function write become visible together. On error none do. The successful return closes that parent's direct-child membership — it says no more children will be added by that command, not that the children have finished. A plan reads that authoritative membership with `Children`; it never reconstructs membership from an application result payload.

Plans normally pass the resulting child keys to a dependent command and declare the dependency with `After` or `AfterSettled`. That command's worker uses typed `ResultOf` for successful results and `OutcomeOf` for any terminal outcome, including failure, cancellation, expiry, or skipping. Both operations may read only commands named as dependencies. Dependency edges answer when the command may run; keys in `Args` identify which inputs the worker consumes and in what semantic role or order. Those argument keys are not another authority for graph membership, and every access is checked against the durable dependency edges. This keeps routine value plumbing out of the plan while keeping every input explicit; arbitrary execution-wide reads from workers are not allowed.

Because every command that finishes produces exactly one event recording how it ended, the rest of the system never has to guess whether work finished. Waiting on successful work and waiting on an external fact use the same event mechanism; all-terminal joins additionally recognize failure, cancellation, expiry, and skipping.

The runtime owns leases, attempts, retry policy, backoff, and timeouts. A retryable handler error records an attempt failure and retries the same logical command; only exhausted retry policy ends the command and records `CommandFailed`. Retry policies are immutable declarative values, not application callbacks. They may bound work by attempt count, total elapsed retry duration, or both. The elapsed budget starts once, when the command first becomes eligible for its initial claim after dependencies, waits, and initial delay, and never resets when an attempt retries, a lease expires, or another replica takes over; the separately stored next-attempt time moves as retries are scheduled. Workers may inspect their immutable PostgreSQL creation, budget-start, attempt-number, and attempt-start values, while plans receive no clock or timing capability. A negative application result is a successful command whose typed result says so.

Retry, timeout, and queue settings on a command definition are creation-time operational defaults. Each command snapshots and journals the accepted settings for its lifetime. Tuning a default affects newly created commands without changing the command version, rewriting existing work, or causing an in-flight plan to fail reconciliation. Changing the policy of an already-created command, if later supported administratively, is an explicit journaled operation rather than a deployment side effect.

### Events and execution history

An event is an immutable fact recorded in the execution journal. Events are never consumed destructively: unlike a command, which one worker handles, an event may be observed independently by the plan and by any number of coordinators. There is one developer-facing event abstraction: `flow.Event[T]`.

The runtime automatically records an event carrying a successful worker result. It records facts such as `CommandFailed`, `CommandCancelled`, `CommandExpired`, or `CommandSkipped` when a command ends another way. Workers may emit additional application events, and external integrations may publish events into a running execution — for example, a webhook recording a confirmed deposit or a monitor recording a bridge delivery. Facts such as `ExecutionSucceeded` and `PlanFailed` use the same event model. These names describe what happened; they do not define separate event systems or developer concepts.

Long external waits normally remain outside a claimed worker. A webhook or efficient batch monitor observes the external system, then uses `Runtime.InTx` to publish one idempotent fact in the same PostgreSQL transaction as its application-table update. A plan-declared command waiting with `Await` holds no worker, connection, goroutine, or lease; the published fact makes it eligible atomically at commit. Facts retained before the plan reaches that branch are still observed later, so this pattern replaces fragile one-shot release writes without requiring one polling command per execution.

An event used only by a durably stored `Await` condition is resolved by the engine and does not require the publishing monitor to register or execute the Go plan. If the plan itself reads that event through `Fact` or `Facts`, the publishing runtime must have the exact plan version so the fact and any resulting declarations can commit together. This keeps ordinary monitors small without weakening atomic plan decisions.

The journal additionally records the creation of every command, including its canonical arguments, origin, parent where applicable, dependencies, required/optional classification, and causation. Together with the exactly-one-terminal-event rule, those entries make the execution graph and its final command states reconstructible from retained history. Attempt starts, retryable failures, interruptions, and retry scheduling remain operational journal entries rather than events because the command has not finished yet. Lease-renewal heartbeats and polling noise are maintenance, not history.

### Plans

A plan is an optional pure function that declares the commands an execution needs when progression spans more than one independently completing branch:

```go
func planIntent(p *flow.Plan, args ExecuteArgs) {
    flow.Do(p, "deposit", AwaitDeposit, depositArgs(args)).Await(DepositConfirmed).Within(15 * time.Minute)
    flow.Do(p, "origin",  SendTxn, originTxn(args)).After("deposit")

    route, ok := flow.Fact(p, RouteSelected)
    if !ok {
        return
    }

    if route.Provider == "cctp" {
        flow.Do(p, "attest", AwaitCCTP, cctpArgs(args)).After("origin")
        flow.Do(p, "dest",   SendTxn, destTxn(args)).After("attest")
    } else {
        flow.Do(p, "dest",   SendTxn, destTxn(args)).After("origin")
    }

    flow.Do(p, "refund", RefundIntent, refundArgs(args)).AfterFailed("dest")
}
```

The name is deliberate: the whole durable run is the workflow in ordinary language, represented by an `Execution`. `Plan` names only the optional pure function that plans cross-command progression; calling that function `Workflow` would blur it with the execution it helps coordinate.

The plan is re-evaluated whenever a relevant event is recorded or an observed command reaches a final state, and reconciled by command key: declaring a command that already exists does nothing, and declaring a new one whose prerequisites are met issues it. "React" does not mean that the plan receives a single event callback. Each evaluation is a pure function over the execution arguments and all relevant events and command results recorded so far. Dynamic branches need no separate API — the plan simply declares more work once the fact that decides the branch exists. `Children` reads a worker's authoritative closed child membership; `Result` and `Outcome` remain available for genuine value-dependent branching. A plan only ever grows; it never withdraws work it already asked for.

`After` waits for the event recording another command's success. `AfterSettled` waits for the command to reach any final state, and `Outcome` exposes the typed result or structured reason. `Await` waits for any event, including one published from outside the execution. Plan reads internally distinguish an input that may still arrive from one that resolved without a value, so an unsuccessful command can never leave an execution waiting forever for an impossible result. These are plan-reading operations over durable state and events, not additional event categories.

`Within` gives an `Await` a fact-wait deadline. Its clock starts only after that command's `After`, `AfterSettled`, `AfterFailed`, and `AfterAny` prerequisites are satisfied, so a natural declaration such as `After("origin").Await(BridgeDelivered).Within(time.Hour)` gives the bridge a full hour without hiding the declaration behind a result-reading gate. An early fact is retained and satisfies the wait immediately once the command prerequisites resolve.

Purity is a contract rather than a Go sandbox: the plan receives no context, database, client, clock, or transaction capability, but Go cannot prevent a function from calling a package global. Reconciliation rejects conflicting declarations, plan panics and conflicts fail the execution as plan defects rather than retrying completed work, and `flowtest` evaluates plans repeatedly against identical snapshots to detect nondeterminism.

Plans compose through ordinary Go functions rather than another orchestration abstraction. A reusable plan fragment accepts `*flow.Plan` plus a caller-chosen key prefix, and its caller invokes it like any other function. Each logical fragment instance should use a distinct prefix: conflicting reuse fails as a plan defect, while equivalent duplicate declarations intentionally coalesce, so tests should assert the complete intended key set.

### Coordinators

A coordinator is durable memory that reacts to events. It is not needed for a direct command tree or a bounded fan-out returned by one worker; the authoritative direct-child records already preserve that membership. It exists for open-ended processes — work discovered over time, cycles, or several event streams — where no single command completion can close the decision.

An execution uses exactly one driver mode: a direct root command, a plan, or a hand-written coordinator. A runtime-bound copy of each definition exposes the same `.Execute` method and returns an `ExecutionHandle`. Direct executions need no coordinator. A plan is the built-in orchestration authority for most multi-command graphs; writing a coordinator by hand is the escape hatch for logic a declarative plan cannot express. Its state is typed, its event inbox is durable, and recording an event as processed, updating state, and spawning commands and emitting events all commit atomically. Coordinator state holds orchestration facts only — never a second copy of application data.

A coordinator subscribes to application facts with `On` and to every terminal result of a typed command with `OnOutcome`. The latter receives the existing command event as `CommandOutcome[R]`: success includes `R`, while failure, cancellation, expiry, and skipping include their structured terminal reason. It creates no additional event. Commands whose failures the coordinator intends to interpret are normally spawned `Optional()`, leaving the coordinator to decide the aggregate result instead of triggering automatic fail-fast first.

Coordinator and worker spawns may attach `StartAfter`, a one-shot durable delay measured from PostgreSQL time in the transaction that accepts the child. The child exists immediately for tracing but holds no worker, connection, goroutine, or lease before its claim-eligible time. This is enough for an agent to pause between turns, respect a provider delay, or revisit work later; recurring calendar schedules remain a separate future capability.

For an adaptive agent, the model worker should normally return proposed actions and let the coordinator validate and spawn the corresponding tool or subtask commands. The coordinator stores only orchestration state and durable references; transcripts, documents, embeddings, and other application data remain in application tables or object storage. Bounded sub-agents are ordinary commands in the same execution. A sub-agent that needs its own durable adaptive loop will eventually use a child execution with its own driver and journal rather than expanding the parent coordinator into a second framework.

### Execution identity and causation

An `ExecutionID` groups all commands, attempts, events, decisions, and outcomes for one run. Every derived record identifies its direct cause.

```text
execution start
    -> plan decision -> prepare command
prepare command
    -> Prepare.Done
    -> child command A -> event A
    -> child command B -> event B
events A + B
    -> plan decision -> final command -> event
```

The runtime graph is therefore a projection of the ordered execution journal: command-creation entries establish its vertices, events record its facts and outcomes, and dependencies, parent-child relationships, and causation provide its edges. A plan additionally records work that is declared but not yet runnable, so an execution can be asked not only what happened but what it is currently waiting for.

`flow` is journal-first for its **orchestration control plane**, but it does not event-source application state. Command and execution tables remain indexed materializations used for claiming and current-state queries; the retained journal is sufficient to rebuild the causal graph and settled orchestration projections. Application tables remain authoritative for business data, and recovery never replays arbitrary Go handlers, declared commit functions, or historical external side effects.

## Core user experience

1. Define typed commands (payload and result) and typed events.
2. Register a worker for each command kind; a worker may spawn bounded direct children.
3. Bind a command, plan, or coordinator with `With(runtime)`, then call `.Execute`; the requested work is durably enqueued.
4. Inspect any `ExecutionID` for its graph, pending work, attempts, events, waits, and outcome.

Handlers are ordinary Go and may call normal application services. Business data stays in application-owned tables; `flow` owns execution, delivery, coordination, and journal data. Sharing one PostgreSQL database lets a declared short application commit function and `flow` settlement commit atomically.

## Goals

- **Small and intuitive:** one command can run durably without defining orchestration; advanced concepts are introduced only when needed, and bookkeeping such as child membership, result loading, and failure-safe completion stays inside the library.
- **Local reasoning:** a worker understands one command, its explicit dependencies, and its direct children; a plan reads high-level progression and joins top to bottom.
- **Type safety:** command payloads, command results, event payloads, and handler signatures checked by Go.
- **Dynamic composition:** runtime branching, worker-spawned child commands, fan-out, fan-in, waits, and long-running executions without a fully predeclared graph.
- **Durable adaptive execution:** long-running agents and other open-ended loops progress as bounded commands around durable coordinator decisions, with no process-local loop required for correctness.
- **Durability:** commands, attempts, events, decisions, and causal relationships survive process and machine failure.
- **Failure correctness:** bounded retries, backoff, leases, timeouts, cancellation, terminal failure, and explicit operator-visible outcomes.
- **Atomic progression:** a command result, its declared short application write, emitted events, spawned commands, plan reconciliation, coordinator state, and inbox progress commit under one settlement discipline.
- **Traceability:** explain what is running, what is waiting, what failed, what was retried, and why every command exists.
- **Horizontal operation:** many API processes and workers cooperating on one PostgreSQL database.
- **Testability:** workers, optional plans, and coordinator-driven agent loops unit-testable without starting a distributed system.

## Technical requirements

- Go module: `github.com/goware/flow`.
- PostgreSQL is the sole required durable backend.
- PostgreSQL tables, transactions, indexes, and notifications or polling for dispatch and wakeups.
- First-class support for sharing a caller-owned PostgreSQL transaction.
- Delivery is asynchronous and at least once; the API must not imply exactly-once execution of user code or external effects.
- Declarative retry policies with attempt-count and elapsed-time bounds, using an immutable PostgreSQL-time budget anchor distinct from the moving next-attempt schedule.
- Command-definition retry, timeout, and queue defaults are snapshotted at command creation; tuning affects new commands without rewriting existing commands or causing reconciliation conflicts.
- Worker metadata exposes accepted PostgreSQL creation, retry-budget, and attempt timing without exposing mutable update or scheduling timestamps; plans remain clock-free.
- Commands and events are versioned durable data whose schemas may outlive the code version that created them.
- Rolling deployments where processes temporarily recognize different command or event versions.
- Runtime correctness must never depend on a process retaining in-memory state.
- A replica settling plan-driven commands must register the execution's exact plan version. Ingress processes need that plan only when their transition changes an input the plan actually reads; satisfying a materialized `Await` alone remains plan-free.
- `Runtime` directly satisfies the lightweight client capability, and definitions bind to that capability immutably for concise execution and application wiring.
- A direct command execution requires neither a plan nor a coordinator and completes from its closed command tree.
- Every accepted command creation has one immutable ordered journal entry recording its payload, origin, dependencies, classification, and causation; every command that ends has exactly one persisted event recording how it ended.
- The retained execution journal must be sufficient to rebuild the causal graph and settled orchestration projections; indexed command and execution tables remain operational materializations for claiming and current-state queries.
- Transient attempt lifecycle is separately identifiable in the journal and never exposed as application events; lease-renewal and polling maintenance is not journaled.
- External monitors and webhooks may publish idempotent execution facts atomically with application-table writes; commands awaiting those facts consume no runtime worker capacity while pending.
- `Within` bounds only an awaited-fact stage and starts after the command's non-event prerequisites resolve.
- Coordinators can consume typed success and unsuccessful command outcomes through one subscription over the command's existing terminal event.
- Workers and coordinators can durably delay a spawned command from PostgreSQL time without occupying runtime capacity or consuming its retry budget before eligibility.
- Every execution mode has a configurable total-command safety ceiling, snapshotted when the execution starts so all replicas enforce the same value; it limits one execution's topology, not database backlog or concurrency.
- Transactions that compose Flow operations with application writes acquire Flow-owned state first and application rows second.

## Non-goals

- Reimplementing Kafka or providing a general-purpose high-throughput streaming platform.
- Requiring Kafka, Redis, a separate control plane, or a hosted service.
- A database-wide journal or cross-execution ordering guarantee. Each execution journal and its total ordering are permanently execution-scoped. Future cross-execution delivery must cross an explicit idempotent export, subscription, or execution-start boundary that retains the source execution and position; it must not merge execution journals or imply a global order.
- Treating application/domain state as framework-owned flow state.
- Transparent replay of arbitrary Go code or deterministic-workflow sandboxing.
- Exactly-once external side effects.
- Distributed ACID transactions across PostgreSQL and external services.
- Multi-region active-active execution in the initial milestones.
- Hard replica pinning or correctness that depends on instance-local memory.
- A visual workflow designer in the core library.
- Agent-specific `Agent`, `Subagent`, `Tool`, `Memory`, or conversation abstractions; applications model those concepts with commands, events, coordinators, and application-owned data.

## Future direction

- child executions for independently durable sub-agents and other nested work, with an explicit parent link, separate execution-scoped journal, terminal outcome boundary, and cancellation policy rather than merged ordering;
- an operational UI with execution timelines, causal graph views, pending waits, retries, and failures;
- OpenTelemetry, metrics, and structured-logging adapters;
- administrative retry, fork, explicit policy amendment, repair, and compensation tools;
- optional local affinity that makes a bounded best effort to keep causally related work on the replica that started it, while always allowing another replica to take over;
- near-term archival and configurable journal retention, including the option to discard bulky command payloads before retaining their causal skeletons for longer;
- optional event export and cross-execution subscriptions through explicit idempotent boundaries that preserve each source execution's identity and order without promising order across executions;
- plan simulation and dry-run tooling that, when the exact plan version and retained execution snapshot are available, shows declarations and consulted inputs after historical or candidate transitions without executing workers or external effects.
