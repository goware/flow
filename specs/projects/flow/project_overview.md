---
status: draft
---

# flow

## Summary

`flow` is a Go library for durable, event-driven execution backed entirely by PostgreSQL.

Its core loop is deliberately small:

```text
command  →  worker  →  event
                     └→ optional child commands
```

The developer model is:

- **Commands** are durable instructions to perform work.
- **Workers** handle commands and perform that work.
- **Events** record facts about what happened.
- **Plans** react to recorded events and coordinate the overall execution.
- **Workers** may spawn child commands when their work reveals more work.

There is one event concept. When a worker returns successfully, `flow` automatically records an event carrying its typed result. When a command instead ends in failure, cancellation, expiry, or skipping, `flow` records an event describing that fact. A worker may explicitly emit additional application facts. All are ordinary events in the same execution log.

Most executions have a high-level shape the application knows how to describe, so the primary way to compose work is a **plan**: a small pure function that declares commands and joins by durable key. The plan does not receive one event callback. It is re-evaluated over all relevant events and command results recorded so far; it never sleeps in memory. A worker may also **spawn** a bounded set of direct child commands when performing one command reveals more work. For open-ended processes whose membership cannot close with one worker return, a hand-written **coordinator** reacts to events directly.

The intended experience is closer to using an in-process Go library than operating Kafka or a separate workflow platform: a small type-safe API, ordinary Go handlers, PostgreSQL transactions, and one operational backend.

## Motivation

Real application workflows are rarely simple linear job chains. A cross-chain intent may need to wait for a deposit, create origin and destination transactions in parallel, monitor external systems, send bridge gas, join multiple outcomes, create edge transactions, and select additional work according to the route provider. Failures can occur at every stage, and part of the shape only becomes known while the execution is running.

Hand-rolling this produces a status column per table, a poll loop per status, a bespoke claim query, a stale-recovery sweep, and timeout rules that must be documented so nobody breaks them. The failure modes are silent: work that waits forever because a release signal was lost, or a timeout that never fires because the clock it reads is reset by the loop that polls it.

`flow` makes that infrastructure a library, and keeps each unit of application code small:

- A **command** is a durable instruction to perform work.
- A **worker** handles one command kind, may atomically spawn direct child commands, and returns its typed result.
- An **event** records a fact about what happened and may be observed by anything interested.
- A **plan** purely declares high-level commands and joins over recorded events and command states.
- A **coordinator** is the escape hatch: durable memory that reacts to events when a plan is not enough.

## Product model

### Commands

A command is a durable, immutable request for work. It has a typed name, version, payload type, and **result type**. It belongs to an execution and is delivered to a compatible worker with lease, attempt, and retry semantics. One logical command keeps the same `CommandID` across its separately recorded attempts.

A command is the executable vertex of the projected graph; `flow` needs no second node abstraction. A deterministic `CommandKey` gives it a stable, human-readable identity within its execution and an idempotency boundary for repeated decisions. Plan-declared, worker-spawned, coordinator-spawned, and externally issued commands share one key namespace and one lifecycle.

### Workers

A worker registers a handler for one command kind and version. The handler receives the typed payload and returns a typed result.

Both types are the application's own — `flow` supplies no wrapper around either:

```go
type SendArgs   struct{ TxnID string; Txn Transaction }   // what this command needs
type SendResult struct{ Hash string }                     // what it produces

var SendTxn = flow.DefineCommand[SendArgs, SendResult]("send_txn", 1)

func sendTxn(ctx context.Context, work *flow.Work[SendArgs]) (SendResult, error) {
    hash, err := relayer.Send(ctx, work.Payload.Txn)
    if err != nil {
        return SendResult{}, err
    }
    return SendResult{Hash: hash}, nil
}
```

A command declares its payload type and its result type together, and the handler is an ordinary Go function over them. Use `flow.None` as the result type for a command that produces nothing meaningful.

Conceptually, workers emit events. In the API, returning `(result, nil)` automatically records the command's event carrying that result. Workers call `flow.Emit` only for additional application facts. A retryable error does not record a final event because the command has not finished.

The result event is recorded atomically with any application writes the handler registered and any additional events or child commands it staged. If the transaction does not commit, none of it becomes visible.

When work discovers a complete bounded fan-out, the worker may spawn those children directly:

```go
func prepareReport(ctx context.Context, work *flow.Work[PrepareArgs]) (PrepareResult, error) {
    analyses, err := determineAnalyses(ctx, work.Payload.CompanyID)
    if err != nil {
        return PrepareResult{}, err
    }

    keys := make([]string, 0, len(analyses))
    for _, analysis := range analyses {
        key := "analysis/" + analysis.ID
        if err := flow.Spawn(work, key, AnalyzeReportPart, analysis.Args); err != nil {
            return PrepareResult{}, err
        }
        keys = append(keys, key)
    }
    return PrepareResult{AnalysisKeys: keys}, nil
}
```

`Spawn` is asynchronous: it stages a direct child rather than calling its handler. On success all children, the parent's result event, extra events, and `OnCommit` writes become visible together. On error none do. The successful return closes that parent's direct-child membership — it says no more children will be added by that command, not that the children have finished.

Because every command that finishes produces exactly one event recording how it ended, the rest of the system never has to guess whether work finished. Waiting on successful work and waiting on an external fact use the same event mechanism; all-terminal joins additionally recognize failure, cancellation, expiry, and skipping.

The runtime owns leases, attempts, retry policy, backoff, and timeouts. A retryable handler error records an attempt failure and retries the same logical command; only exhausted retry policy ends the command and records `CommandFailed`. A negative application result is a successful command whose typed result says so.

### Events

An event is an immutable fact in the execution log. Events are never consumed destructively: unlike a command, which one worker handles, an event may be observed independently by the plan and by any number of coordinators. There is one event abstraction: `flow.Event[T]`.

The runtime automatically records an event carrying a successful worker result. It records facts such as `CommandFailed`, `CommandCancelled`, `CommandExpired`, or `CommandSkipped` when a command ends another way. Workers may emit additional application events, and external integrations may publish events into a running execution — for example, a webhook recording a confirmed deposit or a monitor recording a bridge delivery. Facts such as `ExecutionSucceeded` and `PlanFailed` use the same event model. These names describe what happened; they do not define separate event systems or developer concepts.

Retryable attempt errors remain attempt history rather than events because the command has not finished yet.

### Plans

A plan is a pure function that declares the commands an execution needs:

```go
func planIntent(p *flow.Plan, args ExecuteArgs) {
    flow.Do(p, "deposit", AwaitDeposit, depositArgs(args)).Await(DepositConfirmed).Within(15 * time.Minute)
    flow.Do(p, "origin",  SendTxn, originTxn(args)).After("deposit")

    if route, ok := flow.Fact(p, RouteSelected); ok && route.Provider == "cctp" {
        flow.Do(p, "attest", AwaitCCTP, cctpArgs(args)).After("origin")
        flow.Do(p, "dest",   SendTxn, destTxn(args)).After("attest")
    } else if ok {
        flow.Do(p, "dest",   SendTxn, destTxn(args)).After("origin")
    }

    flow.Do(p, "refund", RefundIntent, refundArgs(args)).AfterFailed("dest")
}
```

The plan is re-evaluated whenever a relevant event is recorded or an observed command reaches a final state, and reconciled by command key: declaring a command that already exists does nothing, and declaring a new one whose prerequisites are met issues it. "React" does not mean that the plan receives a single event callback. Each evaluation is a pure function over the execution arguments and all relevant events and command results recorded so far. Dynamic branches need no separate API — the plan simply declares more work once the fact that decides the branch exists. `Result` and `Outcome` can read a command declared earlier in that evaluation or any existing command key, including a direct child spawned by a worker. A plan only ever grows; it never withdraws work it already asked for.

`After` waits for the event recording another command's success. `AfterSettled` waits for the command to reach any final state, and `Outcome` exposes the typed result or structured reason. `Await` waits for any event, including one published from outside the execution. `Result` and `Outcome` are plan-reading operations over command state and its recorded event; they are not additional event categories.

Purity is a contract rather than a Go sandbox: the plan receives no context, database, client, clock, or transaction capability, but Go cannot prevent a function from calling a package global. Reconciliation rejects conflicting declarations, plan panics and conflicts fail the execution as plan defects rather than retrying completed work, and `flowtest` evaluates plans repeatedly against identical snapshots to detect nondeterminism.

### Coordinators

A coordinator is durable memory that reacts to events. It is not needed for a bounded fan-out returned by one worker; the parent result and direct-child records already preserve that membership. It exists for open-ended processes — work discovered over time, cycles, or several event streams — where no single command completion can close the decision.

One coordinator runs per execution. A plan is the coordinator most applications will use; writing one by hand is the escape hatch for logic a declarative plan cannot express. Its state is typed, its event inbox is durable, and recording an event as processed, updating state, and spawning commands and emitting events all commit atomically. Coordinator state holds orchestration facts only — never a second copy of application data.

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

The runtime graph is therefore a projection of durable history: commands are its vertices, events are its facts, and dependencies, parent-child relationships, and causation provide its edges. A plan additionally records work that is declared but not yet runnable, so an execution can be asked not only what happened but what it is currently waiting for.

This is deliberately not strict event sourcing. Immutable command records, attempt history, events, and materialized command state together are the durable authority. They are sufficient to rebuild inspection projections and re-evaluate plans, but recovery never replays arbitrary Go handlers or repeats historical external side effects.

## Core user experience

1. Define typed commands (payload and result) and typed events.
2. Register a worker for each command kind; a worker may spawn bounded direct children.
3. Write a plan for high-level progression and joins, or a coordinator where the process is open-ended.
4. Start an execution.
5. Inspect any `ExecutionID` for its graph, pending work, attempts, events, waits, and outcome.

Handlers are ordinary Go and may call normal application services. Business data stays in application-owned tables; `flow` owns execution, delivery, coordination, and history data. Sharing one PostgreSQL database lets application writes and `flow` outputs commit atomically.

## Goals

- **Small and intuitive:** few core concepts and a low public method count.
- **Local reasoning:** a worker understands one command and its direct children; a plan reads high-level progression and joins top to bottom.
- **Type safety:** command payloads, command results, event payloads, and handler signatures checked by Go.
- **Dynamic composition:** runtime branching, worker-spawned child commands, fan-out, fan-in, waits, and long-running executions without a fully predeclared graph.
- **Durability:** commands, attempts, events, decisions, and causal relationships survive process and machine failure.
- **Failure correctness:** bounded retries, backoff, leases, timeouts, cancellation, terminal failure, and explicit operator-visible outcomes.
- **Atomic progression:** handler state, application writes, inbox progress, emitted events, and spawned commands commit together.
- **Traceability:** explain what is running, what is waiting, what failed, what was retried, and why every command exists.
- **Horizontal operation:** many API processes and workers cooperating on one PostgreSQL database.
- **Testability:** workers and plans unit-testable without starting a distributed system.

## Technical requirements

- Go module: `github.com/goware/flow`.
- PostgreSQL is the sole required durable backend.
- PostgreSQL tables, transactions, indexes, and notifications or polling for dispatch and wakeups.
- First-class support for sharing a caller-owned PostgreSQL transaction.
- Delivery is asynchronous and at least once; the API must not imply exactly-once execution of user code or external effects.
- Commands and events are versioned durable data whose schemas may outlive the code version that created them.
- Rolling deployments where processes temporarily recognize different command or event versions.
- Runtime correctness must never depend on a process retaining in-memory state.
- Every command that ends has exactly one persisted event recording how it ended; transient attempt failures remain separately inspectable history.
- Durable commands, events, attempts, dependencies, child relationships, and causation must be sufficient to rebuild inspection, telemetry, and a graph view without a UI in the core runtime.

## Non-goals

- Reimplementing Kafka or providing a general-purpose high-throughput streaming platform.
- Requiring Kafka, Redis, a separate control plane, or a hosted service.
- Cross-execution or cross-service event fan-out in the initial milestones; the event log is scoped to one execution.
- Treating application/domain state as framework-owned flow state.
- Transparent replay of arbitrary Go code or deterministic-workflow sandboxing.
- Exactly-once external side effects.
- Distributed ACID transactions across PostgreSQL and external services.
- Multi-region active-active execution in the initial milestones.
- A visual workflow designer in the core library.

## Future direction

- an operational UI with execution timelines, causal graph views, pending waits, retries, and failures;
- OpenTelemetry, metrics, and structured-logging adapters;
- administrative retry, fork, repair, and compensation tools;
- child coordinators for decomposing very large executions;
- archival and configurable history retention;
- optional event export to Kafka or other analytics systems, without making them runtime dependencies.
