---
status: draft
---

# flow

## Summary

`flow` is a Go library for durable, event-driven execution backed entirely by PostgreSQL.

The model is one sentence: **a command asks for work, a worker performs it, and the worker's success records an event.** Every command yields exactly one completion fact, so progress is always visible as an immutable event. Commands and events belong to an execution, and causation links record which command or event produced each subsequent record.

Most executions have a shape the application knows how to describe, so the primary way to compose work is a **plan**: a small function that declares the commands an execution needs and what each one waits for. The plan is re-evaluated whenever a new fact arrives, so branches that depend on runtime information simply appear once the fact that decides them exists. For the cases a plan cannot express, a hand-written **coordinator** reacts to events directly.

The intended experience is closer to using an in-process Go library than operating Kafka or a separate workflow platform: a small type-safe API, ordinary Go handlers, PostgreSQL transactions, and one operational backend.

## Motivation

Real application workflows are rarely simple linear job chains. A cross-chain intent may need to wait for a deposit, create origin and destination transactions in parallel, monitor external systems, send bridge gas, join multiple outcomes, create edge transactions, and select additional work according to the route provider. Failures can occur at every stage, and part of the shape only becomes known while the execution is running.

Hand-rolling this produces a status column per table, a poll loop per status, a bespoke claim query, a stale-recovery sweep, and timeout rules that must be documented so nobody breaks them. The failure modes are silent: work that waits forever because a release signal was lost, or a timeout that never fires because the clock it reads is reset by the loop that polls it.

`flow` makes that infrastructure a library, and keeps each unit of application code small:

- A **command** says what work should be attempted.
- A **worker** knows how to perform one kind of work and returns its typed result.
- An **event** says what has happened, and can be observed by anything interested.
- A **plan** declares which commands an execution needs and what each waits for.
- A **coordinator** is the escape hatch: durable memory that reacts to events when a plan is not enough.

## Product model

### Commands

A command is a durable, immutable request for work. It has a typed name, version, payload type, and **result type**. It belongs to an execution and is delivered to a compatible worker with lease, attempt, and retry semantics. One logical command keeps the same `CommandID` across its separately recorded attempts.

A command is the executable vertex of the projected graph; `flow` needs no second node abstraction. A deterministic `CommandKey` gives it a stable, human-readable identity within its execution and an idempotency boundary for repeated decisions.

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

Returning successfully records the command's **completion event** carrying that result, atomically with any application writes the handler registered and any additional events or commands it staged. If the transaction does not commit, none of it becomes visible.

Because every successful command produces exactly one completion fact, the rest of the system never has to guess whether work finished. Waiting on a command and waiting on an external fact are the same operation.

The runtime owns leases, attempts, retry policy, backoff, and timeouts. A retryable handler error records an attempt failure and retries the same logical command; only exhausted retry policy produces a terminal command failure. A negative *domain* outcome is a successful command whose result says so.

### Events

An event is an immutable fact in the execution log. Events are never consumed destructively: unlike a command, which one worker handles, an event may be observed independently by the plan and by any number of coordinators.

Completion events are recorded automatically from worker results. Workers may emit additional domain events. Application code and external integrations may publish events into a running execution — a webhook recording a confirmed deposit, a monitor recording a bridge delivery. Runtime events record durable execution facts such as terminal command failure or execution termination.

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

The plan is re-evaluated whenever a new fact arrives and reconciled by node key: declaring a node that already exists does nothing, and declaring a new one whose prerequisites are met issues its command. Dynamic branches need no separate API — the plan simply declares more work once the fact that decides the branch exists. A plan only ever grows; it never withdraws work it already asked for.

`After` waits for another node's completion fact. `Await` waits for any event, including one published from outside the execution. They are the same mechanism, because a command's completion is itself an event.

### Coordinators

A coordinator is durable memory that reacts to events. It exists because "wait for three things" needs somewhere to remember you have seen two.

One coordinator runs per execution. A plan is the coordinator most applications will use; writing one by hand is the escape hatch for logic a declarative plan cannot express. Its state is typed, its event inbox is durable, and recording an event as processed, updating state, and emitting commands and events all commit atomically. Coordinator state holds orchestration facts only — never a second copy of application data.

### Execution identity and causation

An `ExecutionID` groups all commands, attempts, events, decisions, and outcomes for one run. Every derived record identifies its direct cause.

```text
root command
    -> completion event
        -> plan decision
            -> command A -> completion event A
            -> command B -> completion event B
                -> plan decision
                    -> final command -> completion event
```

The runtime graph is therefore a projection of durable history: commands are its vertices, events are its facts, and causation provides its edges. A plan additionally records work that is declared but not yet runnable, so an execution can be asked not only what happened but what it is currently waiting for.

## Core user experience

1. Define typed commands (payload and result) and typed events.
2. Register a worker for each command kind.
3. Write a plan, or a coordinator where a plan is not enough.
4. Start an execution.
5. Inspect any `ExecutionID` for its graph, pending work, attempts, events, waits, and outcome.

Handlers are ordinary Go and may call normal application services. Business data stays in application-owned tables; `flow` owns execution, delivery, coordination, and history data. Sharing one PostgreSQL database lets application writes and `flow` outputs commit atomically.

## Goals

- **Small and intuitive:** few core concepts and a low public method count.
- **Local reasoning:** a worker understands one command; a plan reads top to bottom.
- **Type safety:** command payloads, command results, event payloads, and handler signatures checked by Go.
- **Dynamic composition:** runtime branching, chaining, fan-out, fan-in, waits, and long-running executions without a fully predeclared graph.
- **Durability:** commands, attempts, events, decisions, and causal relationships survive process and machine failure.
- **Failure correctness:** bounded retries, backoff, leases, timeouts, cancellation, terminal failure, and explicit operator-visible outcomes.
- **Atomic progression:** handler state, application writes, inbox progress, emitted events, and emitted commands commit together.
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
- Durable history must be sufficient to build inspection, telemetry, and a graph view without a UI in the core runtime.

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
