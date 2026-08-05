# Plan 4: Cross-execution event delivery

Status: Complete

## 1. Summary

Add one deliberately small capability to Flow: allow application code to deliver a typed event to a known execution.

```go
err := StageCompleted.Deliver(
	ctx,
	runtime.InTx(tx),
	targetExecutionID,
	eventKey,
	payload,
)
```

This closes the gap identified by PR 10 while preserving the command-only model established by Plan 3:

```text
command -> worker -> events
                    |
                    +-> gated commands in the same execution

application transaction
    |
    +-> deliver event to another known execution
```

Delivery is targeted event ingress. It is not a coordinator, a global event bus, a command dependency graph, or a second scheduling model.

The implementation should remain close to PR 10 in size and shape: factor the existing event-ingress path behind a private helper, expose `Event.Deliver`, and add focused tests and documentation. It must not introduce new attempt context, tables, scheduler loops, public interfaces, or durable record types.

## 2. Controlling decisions

### 2.1 Keep `Deliver` as the name

The public API is:

```go
func (event Event[T]) Deliver(
	ctx context.Context,
	client Client,
	target ExecutionID,
	key string,
	payload T,
) error
```

`Deliver` is preferred because it distinguishes the operation from both existing forms of emission:

- `flow.Emit(work, event, key, payload)` stages an event as part of the current worker decision.
- `event.Emit(ctx, client, executionID, key, payload)` emits an external event and is rejected from inside an active worker attempt.
- `event.Deliver(ctx, client, target, key, payload)` deliberately performs detached event ingress, including from inside a worker attempt and normally into another known execution.

Other names are less precise:

- `EmitTo` is too easy to confuse with staged `Emit`.
- `Publish` implies a global subscriber or topic model.
- `Send` and `Notify` imply message-delivery semantics that Flow does not provide.
- `Relay` and `Forward` imply that the event must originate in the source execution.

### 2.2 Do not require `Work`

Cross-execution delivery commonly happens in application helpers several call layers below a worker. Those helpers may naturally have only:

- a `context.Context`,
- a Flow `Client`,
- the target `ExecutionID`, and
- the event value.

Requiring the current `Work` value would couple otherwise ordinary application code to the source worker and make the API harder to use without strengthening its guarantees.

`Deliver` does not need to inspect the active attempt or expose the full `Work` object. It is the explicit escape hatch from attempt-staged emission.

### 2.3 Delivery targets one execution

Delivery requires an explicit `ExecutionID`. It does not discover an execution by command name, business key, event name, or subscription.

The application is responsible for retaining or resolving the target execution ID. This keeps routing explicit and avoids adding a global event namespace or subscriber registry.

### 2.4 Delivery is immediate ingress, not a staged worker decision

`Deliver` uses the same target-side event-ingress semantics as `Event.Emit`. It does not add the event to the source worker's staged decision.

Consequently:

- with a regular runtime client, delivery commits independently and immediately;
- with `runtime.InTx(tx)`, delivery participates in the caller-owned database transaction;
- a committed delivery is not undone if the source worker later fails, retries, or loses its lease;
- Flow does not establish a durable lifecycle relationship between the source and target executions.

This behavior is intentional. Applications should deliver durable business facts, not tentative progress whose validity depends on the source command settling successfully.

### 2.5 Same-execution work should remain staged

Inside an active worker attempt, normal events for that execution should use `flow.Emit(work, ...)` so they remain fenced and atomic with the worker decision.

`Deliver` deliberately does not inspect or compare the source execution. If a caller explicitly targets its own execution:

```go
event.Deliver(ctx, client, work.Info().ExecutionID, key, payload)
```

the event has detached ingress semantics: it may commit even if the worker attempt later fails. This is a sharp edge and must be documented, but preventing it would require source-execution tracking that the primitive otherwise does not need.

Ordinary external callers should generally use `Event.Emit` when detached or cross-execution intent does not need to be emphasized.

## 3. User model

### 3.1 Receiving execution

The receiving side is an ordinary command whose execution contains an event-gated sub-command:

```go
func runStage(ctx context.Context, client flow.Client, stageRunKey string) (flow.Execution, error) {
	return awaitStage.With(client).Execute(
		ctx,
		stageRunKey,
		AwaitStageArgs{EventKey: stageRunKey},
		flow.WaitFor(stageCompleted, stageRunKey),
		flow.Within(30*time.Minute),
	)
}
```

Its worker reads the already-satisfied event normally:

```go
func awaitStageWorker(_ context.Context, work *flow.Work[AwaitStageArgs]) (flow.None, error) {
	completed, err := flow.GetEventValue(work, stageCompleted, work.Args.EventKey)
	if err != nil {
		return flow.None{}, err
	}

	// React to completed.
	_ = completed
	return flow.None{}, nil
}
```

The command definition supplies the worker above, and `AwaitStageArgs.EventKey` is the same stable key passed to `WaitFor`. No coordinator or mutable coordination row is introduced.

### 3.2 Producing transaction

A producer that already owns a database transaction delivers the event with the transaction-bound Flow client:

```go
txClient := runtime.InTx(tx)

if err := writeApplicationState(ctx, tx, update); err != nil {
	return err
}

if err := stageCompleted.Deliver(
	ctx,
	txClient,
	targetExecutionID,
	stageRunKey,
	StageCompleted{StageID: stageID},
); err != nil {
	return err
}

return tx.Commit(ctx)
```

The application must use the same transaction client for all Flow calls in that transaction so the existing lock-order discipline is preserved.

If the transaction rolls back, neither the application write nor the delivered event is committed. If it commits, both become visible atomically.

### 3.3 Delivery without an application transaction

Delivery through the regular runtime is also valid:

```go
err := stageCompleted.Deliver(
	ctx,
	runtime,
	targetExecutionID,
	stageRunKey,
	payload,
)
```

This commits the target event independently. It is appropriate only when no application write needs to be atomic with the event.

### 3.4 Fan-in from multiple executions

Multiple producer executions may deliver distinct exact events to one target execution. The target can use existing `WaitFor` gates to express an AND-join:

```go
execution, err := joinResults.With(runtime).Execute(ctx, "join/42", args,
	flow.WaitFor(resultReady, "part-a"),
	flow.WaitFor(resultReady, "part-b"),
	flow.WaitFor(resultReady, "part-c"),
	flow.Within(30*time.Minute),
)
```

Each producer delivers its own event to the join's execution. The join command becomes runnable only after every declared wait is satisfied.

This composes existing target-local gates; it does not add cross-execution command nesting or a coordinator.

## 4. Semantics

### 4.1 Event identity and idempotency

Delivery uses the existing target-local event identity:

```text
(target execution ID, event name, event key)
```

Existing ingress rules continue to apply:

- repeating the same identity with the same encoded payload is an idempotent no-op;
- repeating the same identity with a different payload returns `ErrConflict`;
- an event cannot reopen a terminal execution;
- delivery does not create the target execution.

No source execution ID is added to event identity or persistence.

### 4.2 Gate satisfaction

A delivered event is an ordinary application event in the target execution. It can satisfy an exact `WaitFor` gate using the existing name, key, class, and payload-hash rules.

The event may arrive before or after its target command is declared, subject to the existing target execution and terminal-state rules.

No new gate type is required.

### 4.3 Transaction boundaries

`Deliver` accepts the existing `Client` abstraction:

- a runtime client owns and commits its ingress transaction;
- `runtime.InTx(tx)` uses the caller's transaction and never commits or rolls it back.

An error returned by `Deliver` is caller-owned. A worker that requires atomicity must return the error and roll back or decline to commit the application transaction.

Flow does not automatically poison the source attempt for target-ingress errors. The caller controls whether those errors are fatal, retryable, or handled. A worker that ignores a delivery error has explicitly chosen not to make successful delivery part of that attempt's success condition.

### 4.4 Source failure and retries

Once a delivery transaction commits, the target event persists independently of the source command outcome.

Therefore:

- retrying the source may repeat delivery;
- existing event idempotency makes an identical repeat harmless;
- a different payload for the same target event identity conflicts;
- source failure after commit does not retract the event;
- source lease loss after commit does not retract the event.

Applications must choose stable target event keys and deterministic payloads for retried producers.

### 4.5 Delivery is not exactly-once execution

Atomic application write plus event insertion prevents a committed write from being separated from its corresponding Flow event when both use the same PostgreSQL transaction.

It does not make the target worker exactly once. Target workers retain Flow's existing at-least-once attempt and lease behavior and must remain retry-safe.

### 4.6 Target lifecycle

Delivery requires an existing non-terminal target execution. It must not:

- start a missing target implicitly;
- reopen a completed, failed, expired, or cancelled target;
- extend the target deadline;
- cancel or otherwise modify the source execution;
- create parent/child ownership between source and target.

If an application needs to create the target and deliver its first event atomically, it may do both explicitly through the same `runtime.InTx(tx)` client, provided the existing lock-order contract permits that sequence.

## 5. Minimal implementation

### 5.1 Public API

Add only this exported method:

```go
func (event Event[T]) Deliver(
	ctx context.Context,
	client Client,
	target ExecutionID,
	key string,
	payload T,
) error
```

Do not add:

- a `Delivery` type;
- delivery options;
- a delivery registry;
- new client or runtime interfaces;
- a `Work` parameter;
- command-address lookup;
- broadcast or subscription APIs.

### 5.2 Reuse existing ingress

Factor the common external ingress code currently used by `Event.Emit` into an unexported helper. `Emit` retains its existing attempt guard; `Deliver` intentionally calls the helper directly:

```go
func (event Event[T]) Emit(...) error {
	if attemptScope(ctx) != nil {
		// Existing rejection: use flow.Emit(work, ...).
	}
	return event.emitExternal(...)
}

func (event Event[T]) Deliver(...) error {
	return event.emitExternal(...)
}
```

The exact private helper names are implementation details.

### 5.3 Do not add source-attempt tracking

`Deliver` must not add the source execution ID to the attempt context, delivery request, event body, journal identity, or schema. The target `ExecutionID` is the only routing information the operation needs.

### 5.4 No storage or scheduler changes

The existing event-ingress store operation remains authoritative. This plan requires:

- no schema migration;
- no new table or column;
- no new journal record class;
- no new scheduler scan;
- no new goroutine;
- no changes to replay or claim materialization;
- no restoration of coordinator state.

If implementation appears to require any of these, stop and reassess the design before expanding scope.

## 6. PR 10 use case under the command-only model

PR 10's core requirement is valid: a transaction running in one execution needs to atomically write application state and notify another durable Flow execution.

Its original target was a long-lived coordinator because it was built against `master`. After Plan 3, model the receiving workflow as a bounded command execution instead:

1. Start a stage-run execution containing a command gated on the exact terminal event for that run.
2. Persist its `ExecutionID` with the application's stage-run identity.
3. A producer transaction performs the business write and calls `terminalEvent.Deliver` with that target ID using `runtime.InTx(tx)`.
4. The target gated command becomes runnable after commit and gets the event value with `GetEventValue`.
5. If the application needs another run, start a new run identity rather than reopening mutable coordinator state.

This preserves the useful part of PR 10 without reintroducing:

- coordinator handlers;
- mutable awaken sequence state;
- terminal/reopen protocols;
- outcome subscriptions;
- a long-lived serialized coordination row.

## 7. Documentation updates

Update the public documentation to explain the three event paths together:

| API | Intended caller | Target | Transaction behavior |
| --- | --- | --- | --- |
| `flow.Emit(work, ...)` | active worker | current execution | staged with the worker decision |
| `event.Emit(ctx, client, id, ...)` | external application code | named execution | immediate or caller transaction |
| `event.Deliver(ctx, client, id, ...)` | application code, including an active worker | another named execution | immediate or caller transaction |

Document prominently that:

- `Deliver` needs only a target execution ID, not `Work`;
- `runtime.InTx(tx)` is what makes delivery atomic with application writes;
- callers should reuse one transaction client;
- committed delivery survives source failure and retry;
- normal same-execution worker events should use staged `flow.Emit`;
- explicitly delivering to the current execution is detached and may survive a failed attempt;
- target workers remain at-least-once;
- delivery is targeted ingress, not publish/subscribe.

Review and update at least:

- `README.md`;
- `doc.go`;
- the event API comments;
- the Flow project overview and functional specification;
- architecture, engine, runtime, and schema component specs;
- acceptance criteria and implementation plan;
- Plan 3 where its external-ingress wording needs refinement.

Completed historical plans should retain their original decisions except for a short supersession note if needed to avoid a direct contradiction.

## 8. Tests

### 8.1 API and guard tests

Add tests proving:

- `Event.Deliver` works outside a worker attempt;
- `Event.Deliver` works inside a worker when the target is a different execution;
- `Event.Deliver` is not rejected merely because the context belongs to an active attempt;
- the existing `Event.Emit` inside-attempt rejection remains unchanged;
- no `Work` value is required by the delivery API.

### 8.2 Transaction tests

Using PostgreSQL, prove:

- an application row and delivered event become visible together after commit;
- rolling back the caller transaction persists neither;
- the target command cannot run before the delivery transaction commits;
- the target command can run after commit;
- using the regular runtime client commits delivery independently;
- a source failure after a committed delivery does not remove the target event;
- a repeated source attempt delivering the same payload is idempotent.

### 8.3 Identity and lifecycle tests

Prove:

- the same target/name/key/payload is an idempotent no-op;
- the same target/name/key with a different payload returns `ErrConflict`;
- different targets may receive the same name/key independently;
- delivery to a missing target fails;
- delivery cannot reopen a terminal target;
- target event and gate behavior is identical whether the event arrived through `Emit` or `Deliver`.

### 8.4 Command-composition acceptance tests

Replace PR 10's coordinator-shaped example with command-only tests proving:

- a producer execution delivers a terminal event to a separate gated execution;
- the gated worker gets the delivered payload with `GetEventValue`;
- several producer executions can satisfy an exact multi-event join;
- a new stage run uses a new execution identity rather than reopening the old target.

### 8.5 Regression suite

Run:

```text
go test ./...
go test -race ./...
go vet ./...
```

Run database-backed tests against the local PostgreSQL instance, not in skip mode.

Retain repository scans that ensure coordinator APIs, tables, and terminology do not return.

## 9. Acceptance criteria

This plan is complete when:

1. The public `Event.Deliver(ctx, client, targetExecutionID, key, payload)` API exists.
2. It requires no `Work` value and no source execution argument.
3. It can be called from inside a worker and does not require source-attempt metadata.
4. The existing `Event.Emit` attempt guard and staged `flow.Emit` behavior remain unchanged.
5. `runtime.InTx(tx)` makes application writes and target delivery commit or roll back together.
6. A regular runtime client provides explicit independently committed delivery.
7. Existing target-local event identity, idempotency, gate, and terminal-state rules are preserved.
8. A committed delivery survives source failure, retry, and lease loss.
9. The receiving examples and tests use only commands, workers, exact events, `WaitFor`, and `GetEventValue`.
10. No schema, coordinator, scheduler, subscription, broadcast, or replay machinery is added.
11. Public documentation clearly distinguishes staged `Emit`, external `Emit`, and cross-execution `Deliver`.
12. The full PostgreSQL-backed and race test suites pass.

## 10. Non-goals

This plan does not add:

- global events or topics;
- broadcast, subscriptions, or event discovery;
- coordinator state or handlers;
- OR, quorum, or race gates;
- automatic target creation or lookup;
- target reopening;
- cross-database atomicity;
- an application outbox;
- source-to-target cancellation or ownership;
- event retraction;
- exactly-once worker execution;
- a generalized messaging API.

For cross-database or external-system delivery, applications should continue to use an outbox or another integration mechanism. `Deliver` is specifically for placing a typed event into a known Flow execution through the existing PostgreSQL transaction boundary.

## 11. Alternatives rejected

### 11.1 Require `Work`

Rejected because it prevents natural use from nested application helpers and supplies source-attempt context that delivery does not need.

### 11.2 Restore coordinators

Rejected because the receiver can be represented as an event-gated command. Restoring coordinators would reintroduce a second decision model, mutable hot-row state, leases, handler registration, and additional tables.

### 11.3 Add publish/subscribe

Rejected because PR 10 has an explicit target. Subscription discovery would require new durable routing, ownership, replay, and fan-out semantics.

### 11.4 Stage cross-execution delivery in the source decision

Rejected for this capability. It would require cross-execution journal references, multi-execution settlement, deterministic replay rules, and lock-order changes. `runtime.InTx(tx)` already provides the required atomic boundary for application writes and target ingress.

### 11.5 Require a transaction-bound client

Rejected because PR 10's small API usefully supports both cases. A transaction-bound client is required for atomicity with application writes, but independent delivery through the runtime remains valid and explicit.

### 11.6 Use only an application outbox

An outbox remains appropriate across databases or external systems, but it is unnecessarily indirect when the application write and Flow store already share a PostgreSQL transaction. `Deliver` provides the narrow same-database primitive without attempting to replace the general outbox pattern.

## 12. Implementation sequence

1. Add focused failing tests for cross-execution delivery, transaction commit/rollback, retry idempotency, and preservation of the existing `Event.Emit` attempt guard.
2. Factor the existing external event-ingress helper and add `Event.Deliver`.
3. Add the command-only receiving acceptance test based on an exact `WaitFor` gate and `GetEventValue`.
4. Update API documentation and active specifications.
5. Run the full PostgreSQL-backed, race, and vet suites.
6. Confirm the production diff contains no attempt-context, storage-schema, scheduler, replay, or coordinator changes.

The implementation should remain a small extension of existing ingress. If any step begins creating a parallel delivery subsystem, return to the controlling decisions in Section 2 and reduce the design.
