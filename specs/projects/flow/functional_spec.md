---
status: complete
completed_at: 2026-08-04
---

# Functional specification: flow

## Purpose and scope

Flow provides event-driven, durable, distributed execution for typed Go commands on PostgreSQL. The public orchestration grammar is limited to commands, workers, execution-scoped application events, and exact event gates.

Flow supports direct background work, worker-created command trees, sequence, dynamic fan-out, all-of fan-in, repeated joins, bounded loops, delays, external signals, retries, leases, fencing, cancellation, deadlines, inspection, and replay. It intentionally omits a declarative graph, mutable coordinator state, event handlers, outcome subscriptions, OR/quorum/race gates, arbitrary external command injection, global events, and exactly-once external effects.

## Definitions and registration

`DefineCommand[A,R]` creates an immutable typed command definition. `DefineEvent[T]` creates an immutable typed application-event definition. Names are stable and command versions are positive.

`Handle(command, worker)` registers one exact name/version. A worker receives `*Work[A]`, containing typed arguments, immutable command information, and private decision state. Runtime registration freezes when `Run` begins.

## Starting and composing commands

`command.With(client).Execute(ctx, executionKey, args, options...)` durably starts or rediscovers one asynchronous root command. Execution options include deadline, metadata, fail-fast policy, key scope, initial delay, exact waits, and a wait budget.

Inside a worker, `Execute(work, key, command, args)` stages a sub-command and returns an ephemeral `Node`. `Optional`, `Delay`, `WaitFor`, and `Within` modify its declaration. `Emit(work, event, key, payload)` stages an application event. The worker result, commit callback, events, and sub-commands are accepted atomically only when the fenced decision succeeds.

Equivalent duplicate declarations coalesce. Repeated declarations of one key within a single decision merge distinct waits. Arguments, optionality, delay, or wait-budget disagreement poisons the complete decision. A later decision cannot amend an already-durable command declaration.

## Exact event inputs

Event identity is `(execution ID, event name, event key)`. Events are immutable; equivalent publication is idempotent and conflicting content is rejected. External publishers use `Event.Emit`. Application code, including an active worker, may use `Event.Deliver` for deliberately detached ingress into a known execution. `runtime.InTx(tx)` joins delivery to caller-owned writes; a regular client commits it independently. Committed delivery survives source failure and retry, while ordinary same-execution worker events remain staged through `flow.Emit`.

All waits are exact AND conditions. A command is claimable only after every wait is satisfied and its initial delay has elapsed. `Within` begins at command creation; expiry is terminal and late events cannot resurrect the command.

At most 256 waits may be declared for one command. Claim materialization loads all satisfying journal positions and canonical payloads in one bounded query, then releases the connection. `ReadEvent(work, event, key)` performs an O(1) in-memory typed read and accepts only an input declared on that command. Undeclared, invalid, missing, or wrongly typed reads fail deterministically and poison settlement. Retries observe the same immutable snapshots.

## Lifecycle and failure

Every execution begins with one root command. Sub-commands retain a single parent for provenance; gates may synchronize commands across branches. An execution succeeds when all commands are terminal and no required command failed or expired. Application events alone do not keep it open; a predeclared gated command does.

Required terminal failure enters reduced fail-fast by default, cancelling work without active attempts while preserving valid running settlements. Optional failure does not fail the execution, but optional work still contributes to liveness. Externally gated optional commands should normally have a `Within` budget.

A failed, cancelled, or expired command emits no success application event. Flow provides no in-execution reaction to unsuccessful outcomes. Expected business alternatives should be represented as successful typed results/events; infrastructure failures remain execution failures.

## Storage, runtime, and inspection

Flow owns exactly six prefixed tables: executions, commands, command queue, command event waits, journal, and schema migrations. There is one execution kind. Permanent key identity is `(root definition name, execution key)`; live keys apply that identity only to non-terminal executions.

The journal records execution start/failing, command creation, attempt start/conclusion, and application/command-terminal/execution-terminal events. Replay is a pure fold over these facts.

`Run` starts one bounded command scheduler, command lease renewal, wait/deadline/recovery maintenance, optional notification listening, and observer delivery. Polling is sufficient for correctness. Handlers run without holding PostgreSQL connections.

Inspection exposes execution lookup/list/await, paginated history, and trace. Trace includes command provenance, attempts, failures/results, events, declared waits, and their satisfying journal positions. `ResultOf(trace, key, command)` decodes successful results only.

## Bounds

- canonical arguments/results/event payload: 64 KiB each;
- metadata: 16 KiB;
- exact waits per command: 256;
- commands per execution: configurable, default 1000;
- public listing/history pages: bounded;
- command keys, definition names, queues, schemas, and cursor values: validated.
