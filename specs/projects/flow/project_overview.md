---
status: complete
---

# flow

## Summary

`flow` is a Go library for event-driven, durable, distributed work execution backed by PostgreSQL.

Its core loop is deliberately small:

```text
command → worker → events
                   └→ optional child commands
```

The developer model is:

- **Commands** are durable instructions to perform work.
- **Workers** execute commands and produce typed results plus any staged application events.
- **Events** record immutable facts, including how commands ended.
- **Plans**, when needed, purely declare bounded command graphs.
- **Coordinators**, when needed, react to facts with durable state for adaptive or open-ended work.
- Plans, workers, and coordinators request command execution with the same `Execute` verb.

Plans and coordinators are optional. The smallest useful program defines one command, registers one worker, and calls `Execute`. PostgreSQL queues the command; any compatible replica may claim it. Workers may discover bounded children, plans may express dependencies and joins, and coordinators may run durable agent loops. Leases, retries, fencing, and journaled decisions allow another process to continue after a crash or restart.

Every execution has one immutable ordered journal. It records execution start, command creation, attempt lifecycle, worker/coordinator and external facts, terminal outcomes, causation, and execution transitions. Indexed mutable tables materialize the current state needed for efficient claiming and inspection, but the retained journal explains the complete Flow control-plane history and reconstructs the execution graph.

The intended experience is closer to an in-process Go library than a separate workflow platform: ordinary typed Go functions, a small vocabulary, one shared PostgreSQL database, and no Kafka, Redis, control plane, or hosted service.

## Motivation

Real application work rarely remains one linear job. A cross-chain intent may wait for a deposit, create origin and destination transactions, observe bridge delivery, fan out provider-specific work, join outcomes, compensate failures, and continue after a deploy or process crash. A durable agent may repeatedly call models and tools, pause between turns, receive external input, and recover on another replica.

Hand-building these systems produces status columns, poll loops, claim queries, stale-recovery sweeps, timeout anchors, and one-off release signals. Small mistakes create silent stalls or duplicate work. `flow` centralizes that infrastructure while letting application code focus on small commands and their composition.

## Product model

### Executions and commands

An execution groups one durable run. It has an `ExecutionID`, exactly one driver mode, an ordered journal, a configurable command safety ceiling, and a terminal outcome.

A command is an immutable typed instruction within an execution. Its definition provides a durable name and version plus argument and result types. Its caller provides an execution-local command key identifying one occurrence. The same definition may appear several times under distinct keys.

```go
var SendReceipt = flow.DefineCommand[SendReceiptArgs, ReceiptSent](
    "send_receipt",
    1,
)

handle, err := SendReceipt.With(rt).Execute(
    ctx,
    "receipt/"+orderID,
    SendReceiptArgs{OrderID: orderID},
)
```

Calling a definition's `Execute` method starts or idempotently rediscovers an execution. It durably enqueues work and never invokes a handler inline. A runtime-bound definition remains the same static type; `With` returns an immutable copy and adds no wrapper type.

`Command.Execute`, `PlanDef.Execute`, and `Coordinator.Execute` start the three driver modes. Inside an existing execution, one package function requests every further command:

```go
flow.Execute(plan, "origin", SendOrigin, args)
flow.Execute(work, "analysis/finance", AnalyzePart, args)
flow.Execute(coordination, "turn/2", Think, args)
```

The scope gives `Execute` its exact semantics:

- a plan records desired topology reconciled by key across repeated pure evaluations;
- a worker stages a direct child that commits atomically with the parent's successful fenced settlement;
- a coordinator stages a command with its state transition and inbox advance.

In every case `Execute` means “durably request asynchronous command execution,” never “call the worker now.”

### Workers

A worker handles one command name and version. It receives typed immutable arguments and returns a typed result.

```go
type SendArgs struct {
    TxnID string
    Txn   Transaction
}

type SendResult struct {
    Hash    string
    GasUsed uint64
}

var SendTxn = flow.DefineCommand[SendArgs, SendResult](
    "send_txn",
    1,
    flow.WithRetry(flow.RetryFor(10*time.Minute).Attempts(8)),
)

func sendTxn(ctx context.Context, work *flow.Work[SendArgs]) (SendResult, error) {
    sent, err := relayer.SendOnce(ctx, string(work.Info().CommandID), work.Args.Txn)
    if err != nil {
        return SendResult{}, err
    }
    return SendResult{Hash: sent.Hash, GasUsed: sent.GasUsed}, nil
}

rt.Register(flow.Handle(SendTxn, sendTxn))
```

Successful return records exactly one terminal event carrying the typed result. Exhausted failure, cancellation, expiry, and skipping each record exactly one terminal event describing that final state. Retryable attempt failures remain attempt history rather than final facts.

A worker may also stage application facts with `flow.Emit(work, event, key, payload)`. The call performs no SQL. On successful fenced settlement, all staged events, children, the typed result/terminal event, and an optional commit-function write commit together. Any unsuccessful attempt boundary exposes none of them.

A worker discovering bounded work uses the same `Execute` operation:

```go
func prepareReport(ctx context.Context, work *flow.Work[PrepareArgs]) (flow.None, error) {
    analyses, err := determineAnalyses(ctx, work.Args.CompanyID)
    if err != nil {
        return flow.None{}, err
    }
    for _, analysis := range analyses {
        flow.Execute(work, "analysis/"+analysis.ID, AnalyzePart, analysis.Args)
    }
    return flow.None{}, nil
}
```

The successful parent settlement atomically records its result, creates every child, and closes the authoritative child membership. No partial child set commits. `Node.Optional()` makes a child non-outcome-determining, and `Node.Delay(d)` gives it a one-shot durable delay without occupying a worker.

Workers read only explicitly declared dependency results. `ResultOf` is the success-only convenience; `OutcomeOf` returns `Outcome[R]` for every terminal state. Arbitrary execution-wide reads are intentionally unavailable.

### Atomic application writes

Most workers need only return their result. When a short PostgreSQL application write is inseparable from what command success means, registration may attach one declared commit function:

```go
type SendTxnWorker struct{ transactions *TransactionStore }

func (w SendTxnWorker) Work(ctx context.Context, work *flow.Work[SendArgs]) (SendResult, error) {
    // Slow or external work happens without a database transaction.
    return sendThroughRelayer(ctx, work.Args)
}

func (w SendTxnWorker) Commit(
    ctx context.Context,
    tx flow.Tx,
    commit flow.Commit[SendArgs, SendResult],
) error {
    return w.transactions.MarkSent(ctx, tx, commit.Args.TxnID, commit.Result.Hash)
}

worker := SendTxnWorker{transactions: transactions}
rt.Register(flow.Handle(SendTxn, worker.Work, flow.WithCommit(worker.Commit)))
```

The decision rule is:

1. do nothing when the typed result is the complete record;
2. stage an event when the accepted decision produces an application fact for other orchestration or history consumers;
3. execute another command when work deserves its own identity and retry lifecycle;
4. use a commit function only when an application-table write and command success must be one fenced PostgreSQL commit.

Workers never receive a long-lived transaction. The commit function runs later in the short settlement transaction and may use only durable arguments, result, and metadata. This keeps database connections free during slow work and prevents stale workers from committing application state after losing their lease.

### Events and external facts

An `Event[T]` is a typed immutable application fact. Event definitions are unversioned:

```go
var BridgeDelivered = flow.DefineEvent[BridgeDelivery]("bridge_delivered")
```

A materially incompatible payload is a new event name. Existing executions may still wait for the old name, so publishers retain or route the old name until those executions drain.

External systems emit facts into an existing execution:

```go
err := BridgeDelivered.Emit(
    ctx,
    rt,
    executionID,
    "intent/"+intentID,
    delivery,
)
```

The event key is a stable correlation identity both publisher and plan know before occurrence. Runtime-generated values belong in the payload. Repeating the same `(execution, event name, key)` and canonical payload is idempotent; disagreement is a conflict.

`Event.Emit` is ingress for API processes, webhooks, and external monitors. Handler code does not call ingress from inside an attempt, because that would escape the handler's fence; it uses staged `flow.Emit` instead. An inseparable application-table write uses `WithCommit`.

`Runtime.InTx(tx)` lets a monitor update an application table and emit the Flow fact in the same caller-owned PostgreSQL transaction.

### Plans

A plan is an optional pure Go function for bounded dependencies, joins, waits, and branches:

```go
func planIntent(p *flow.Plan, args IntentArgs) {
    origin := flow.Execute(p, "origin", SendOrigin, args.Origin)

    flow.Execute(p, "destination", SendDestination, args.Destination).
        After(origin.Key()).
        WaitFor(BridgeDelivered, args.BridgeDeliveryKey).
        Within(time.Hour)
}
```

The plan is repeatedly evaluated over immutable retained facts and command outcomes. It performs no I/O and receives no context, database, clock, or client. Reconciliation is monotonic and keyed: equivalent declarations coalesce, disagreement is a plan defect, and a plan never withdraws accepted work.

`After`, `AfterSettled`, and `AfterFailed` name exact command-instance keys. `WaitFor` names one exact `EventRef` and event key. `Within` bounds only the fact wait and starts after command dependencies settle. `Fact` reads one exact keyed fact; `Facts` reads all retained facts of one definition.

`flow.Execute` returns a typed ephemeral `Node[R]`. `Key()` is available in every scope. In plans, `Outcome()` returns the typed terminal `Outcome[R]`, and `Children()` returns the authoritative closed child membership. Nodes are evaluation-local handles and must not be stored; durable references are command keys.

Routine result plumbing belongs in the dependent worker. Plans use `Outcome()` only for genuine topology decisions and `Children()` only when the graph depends on worker-discovered membership.

### Coordinators and durable agents

A coordinator is the advanced driver for open-ended membership, cycles, adaptive agents, and multi-event decisions. It owns typed durable orchestration state and processes one matching journal position at a time.

```go
var ResearchAgent = flow.DefineCoordinator[AgentState](
    "research_agent",
    1,
    flow.OnStart(startAgent),
    flow.On(UserMessage, onUserMessage),
    flow.OnOutcome(RunTool, onToolOutcome),
)
```

`On` receives external application facts. `OnOutcome` receives one `Outcome[R]` for every terminal state of a command definition. Commands whose failures the coordinator interprets are normally marked `Optional()` so ordinary fail-fast does not decide the execution first.

Coordinator handlers mutate bounded state, call `flow.Emit` for produced facts, call `flow.Execute` for next commands, and finish with `c.Succeed()` or `c.Fail(err)`. State mutation, events, requested commands, inbox advance, and terminal decision commit atomically. `Node.Delay(d)` persists a future turn without sleeping or reserving a worker.

For a durable agent, one execution is an episode; the coordinator is control memory; model, tool, and bounded sub-agent calls are commands; external user messages are events; and large transcripts or artifacts remain application-owned data referenced from coordinator state. A future child-execution primitive may give recursively adaptive sub-agents separate authorities and journals.

### Retry, distribution, and recovery

Retry policy is immutable inspectable data configured with one option:

```go
flow.WithRetry(flow.Attempts(5))
flow.WithRetry(flow.RetryFor(20*time.Minute).Attempts(12).Backoff(time.Second, 5*time.Second))
```

Flow uses a fixed 20% proportional jitter. The effective accepted policy is persisted with each command; tuning a definition affects only newly created commands. The elapsed budget anchor is set once when the command first becomes claim-eligible and never moves on retry, interruption, lease expiry, or takeover.

The public attempt lease is fixed at 60 seconds. The runtime renews it automatically. Claims use PostgreSQL `FOR UPDATE SKIP LOCKED`; handlers hold no database connection while working. `LISTEN`/`NOTIFY` provides wake hints while bounded polling remains the correctness fallback.

Optional local affinity may prefer the originating replica later, but correctness never depends on locality. Any compatible replica can take over expired work.

### Identity, inspection, and application integration

Applications persist the returned `ExecutionID` with their domain object, preferably in the same transaction through `Runtime.InTx`. Repeating the identical keyed `Execute` safely rediscovers an ambiguously committed start. `GetExecution` reads by exact ID; `ListExecutions` is for operational filtering, not application point lookup.

`Trace` exposes the current graph, attempts, waits, causes, and terminal state. `History` exposes ordered journal entries. Replay checks that retained history reconstructs the same settled orchestration projection.

Flow and application tables intentionally share one PostgreSQL database. Every Flow table is prefixed `flow_`.

## Core user experience

1. Define typed commands and, only when needed, typed external events.
2. Register ordinary Go workers.
3. Bind a command, plan, or coordinator with `With(runtime)` and call `Execute`.
4. Compose further work with `flow.Execute` and typed node modifiers.
5. Inspect any returned `ExecutionID` through `Trace` and `History`.

The smallest path is five concepts: `DefineCommand`, `Handle`, `With`, `Execute`, and `Run`.

## Goals

- **Small API:** one command verb, one staged fact function, one external fact method, and progressive disclosure of plans and coordinators.
- **Local reasoning:** workers understand one command and explicit dependencies; plans describe bounded topology; coordinators own adaptive state.
- **Type safety:** command arguments/results, event payloads, outcomes, and handler signatures are checked by Go.
- **Durability:** accepted work, attempts, facts, decisions, and causes survive process or machine loss.
- **Failure correctness:** retries, deadlines, leases, fencing, cancellation, and terminal outcomes are explicit.
- **Traceability:** every command's existence and ending are journaled, making the graph explainable.
- **Horizontal operation:** many replicas cooperate against one PostgreSQL database.
- **Atomic integration:** caller ingress and short success-defining application writes compose with Flow transactions.
- **Testability:** pure engine tests plus real-PostgreSQL E2E examples exercise the same public API.

## Technical requirements

- Go module `github.com/goware/flow`.
- PostgreSQL is the only required durable backend.
- All tables use the `flow_` prefix and coexist with application tables.
- At-least-once handler execution; no exactly-once claim for external effects.
- Per-execution ordered journal; no database-wide ordering promise.
- Exactly one `CommandCreated` entry and exactly one terminal event per accepted command.
- Immutable command/event payloads encoded canonically with explicit size limits.
- `SKIP LOCKED` capacity-bounded claiming, leases, renewal, fencing, takeover, and poll-only correctness.
- Definition-level command retry, timeout, and queue defaults snapshot at creation.
- Fixed production command lease of 60 seconds with no public tuning option.
- Plans remain pure, monotonic, clock-free, and reconciled from durable dirty work.
- External `Event.Emit` resolves exact keyed waits and dirties plan executions atomically without requiring plan code in the publisher.
- Staged `flow.Emit` resolves the same exact keyed waits and commits atomically with worker/coordinator decisions.
- Direct, plan, coordinator, API-only publisher, and split worker deployments remain supported.
- Flow-owned rows are locked before application rows in composed transactions.

## Non-goals

- Reimplementing Kafka or a general-purpose streaming platform.
- Requiring Redis, Kafka, a hosted control plane, or another durable backend.
- Global event ordering or shared dispatch across executions.
- Event-sourcing application/domain state.
- Deterministic replay of arbitrary Go handlers or external side effects.
- Exactly-once external effects or distributed ACID with external services.
- Hard replica pinning or correctness based on process memory.
- A visual workflow designer or agent-specific framework in the core package.
- Public tuning of every internal scheduler and lease constant before a workload justifies it.

## Future direction

- child executions for independently durable sub-agents and nested workflows;
- an operational UI with timelines, causal graphs, waits, retries, and failures;
- OpenTelemetry, metrics, and structured logging adapters;
- administrative retry, fork, compensation, repair, and explicit policy amendment;
- configurable archival and journal retention;
- optional local affinity as a best-effort optimization;
- explicit idempotent cross-execution export/subscription boundaries;
- plan simulation and dry-run tooling over retained history.
