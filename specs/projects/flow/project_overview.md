---
status: complete
---

# flow

## Summary

`flow` is a typed Go library for durable, distributed work execution on PostgreSQL.

Its complete orchestration model is:

```text
commands + workers + coordinators + execution-scoped events
```

- Commands are durable instructions.
- Workers perform one command and return a typed result.
- Workers may atomically stage bounded children and application events.
- Coordinators consume retained events and command outcomes, keep typed durable state, and decide what to stage next.
- Exact event gates delay a command until named facts exist.

There is no declarative workflow graph. Data flow is explicit in command arguments or coordinator state, and dynamic joins are explicit coordinator logic.

## Smallest useful program

Define one command, register one worker, start the runtime, and execute the command. `Execute` queues work; it never invokes application code inline. Any compatible replica may claim it.

Definitions are immutable typed values. Registration is runtime-local and freezes when `Run` begins. Command and coordinator claims match exact name/version pairs, allowing rolling deployments and independently scaled pools.

## Two execution modes

### Direct

A direct execution starts with one root command. A successful worker may stage a bounded set of child commands and events in the same settlement transaction. This is the preferred mode for background jobs and short, locally discovered command trees.

### Coordinator

A coordinator execution starts with typed state and an `OnStart` delivery. It can handle typed application events with `On` and terminal command outcomes with `OnOutcome`. Each accepted delivery atomically updates state, stages commands/events, and advances the inbox.

Coordinators own joins, branching, races, loops, and adaptive work. They must explicitly call `Succeed` or `Fail` to become terminal.

## One staging API

Workers and coordinators stage commands with:

```go
node := flow.Execute(scope, key, command, args)
```

The returned ephemeral `Node` supports:

- `Key()`;
- `Optional()`;
- `Delay(duration)`;
- `WaitFor(event, key)`;
- `Within(duration)`.

Command keys are the durable identity inside an execution. Repeating an equivalent declaration in one decision coalesces; disagreement is a deterministic defect. Nodes are decision-local builders and must not be retained.

Results needed by future work are copied into command arguments or coordinator state. `ResultOf` and `OutcomeOf` are inspection conveniences over `ExecutionTrace`; `Work` does not expose other commands' results.

## Events and gates

Application events are named, typed, immutable facts scoped to one execution. Their identity is `(execution ID, event name, event key)`.

- `flow.Emit(scope, event, key, payload)` stages an event in a worker/coordinator decision.
- `event.Emit(ctx, client, executionID, key, payload)` publishes from an external process.

Equivalent publication is idempotent and conflicting content is rejected. Events are retained even when no current waiter or coordinator handler uses them.

Direct starts use top-level `WaitFor` and `Within`. Staged commands use the corresponding `Node` methods. Multiple waits are additive AND conditions. A wait is satisfied whether the event arrives before or after command creation. `Within` measures from command creation and is independent of an initial delay.

## Failure and completion

Command attempts use typed retry policy, attempt timeouts, queues, and renewable leases. Handler execution is at-least-once, while settlement is fenced by attempt and lease identity.

By default, a required terminal failure moves the execution to `failing` and cancels pending, ready, or retrying commands. Running attempts survive and may settle; new commands staged by those surviving attempts are durably created and immediately cancelled. The execution becomes failed after all open work closes. With fail-fast disabled, unrelated work continues before final failure.

Optional failures are retained and observable but do not fail a direct execution. Coordinators usually stage orchestration commands as optional and decide terminal policy from `Outcome` values.

## Durability model

Every semantic mutation locks the execution row, allocates gap-free journal positions, appends immutable journal entries, and updates projections in the same PostgreSQL transaction. The journal records starts, command creation, attempts, events, coordinator transitions, and terminal outcomes.

Projection tables make claiming and inspection efficient. Replay folds retained journal rows into semantic state; operational queue/lease fields are overlaid for `Trace`.

The schema contains seven tables:

1. `flow_executions`;
2. `flow_commands`;
3. `flow_command_queue`;
4. `flow_command_event_waits`;
5. `flow_coordinators`;
6. `flow_journal`;
7. `flow_schema_migrations`.

## Operational model

`New` validates a schema and starts nothing. `Run` owns bounded command/coordinator schedulers, lease renewal, maintenance, optional notification hints, and graceful shutdown. Polling is always the correctness path.

Deployments can combine roles or split them:

- API/publisher processes need definitions and a client only;
- worker pools register the command versions they handle;
- coordinator pools register coordinator definitions;
- command workers do not need coordinator definitions merely to settle commands.

## Inspection and testing

`GetExecution`, `LookupLiveExecution`, `ListExecutions`, `AwaitExecution`, `History`, and `Trace` expose durable state without running application code. `Trace` includes commands, exact waits, attempts, events, coordinator state, causation, and ordered history.

`flowtest` exercises production decision recording without PostgreSQL. PostgreSQL integration tests cover migrations, idempotency, claims, fencing, retries, waits, fail-fast, coordinators, replay conformance, multi-replica behavior, and all four runnable examples.

## Product boundary

Flow deliberately does not provide a declarative dependency language, global event bus, arbitrary command injection, exactly-once external side effects, or automatic dataflow between commands. The smaller command/event/coordinator grammar is the product.
