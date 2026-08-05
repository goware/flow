---
status: complete
historical: true
superseded_by: ../plans/2-remove-plan.md
---

# Phase 7: Coordinators and durable agents

> Historical delivery record. The retained coordinator contract is synchronized in the active specifications.

## Goal

Deliver coordinator-driven executions as a complete PostgreSQL-backed vertical slice: durable typed state, start activation, exact event and command-outcome subscriptions, one serialized fenced delivery per instance, retry and takeover, atomic staged outputs, and explicit execution completion.

## Work

1. Complete the erased coordinator descriptor and deterministic decision preparation, including typed state and received-value decoding.
2. Add indexed coordinator probing, historical next-match selection, execution-first claim, attempt journaling, delivery fencing, retry, lease renewal, and expired-lease recovery to the store.
3. Add a separately capacity-bounded coordinator scheduler to `Runtime`, with observations, named fault points, graceful interruption, and cross-replica takeover.
4. Settle state revision, inbox/start acknowledgement, emitted events, spawned commands, and explicit terminal decisions in one semantic transaction; preserve the same delivery on any unsuccessful attempt.
5. Extend journal codecs, replay, history/trace projections, and database-free tests for coordinator transitions.
6. Implement the durable adaptive-agent example as shared runnable scenario and real-PostgreSQL end-to-end test, including parallel tools, a controlled failure observed through `OnOutcome`, a delayed next turn, and final explicit success.

## Exit checks

- Start is asynchronous and is processed exactly once as a durable decision despite retry or crash.
- Every retained matching event is delivered in execution position order; early events are not lost and unmatched entries do not stall progress.
- One instance never runs two handlers concurrently, while separate executions do.
- Handler error, panic, shutdown, and lease loss commit no state/output/inbox progress and safely redeliver the same delivery.
- Success commits `AttemptConcluded`, `CoordinatorTransition`, state/inbox, events, commands, and optional terminal execution outcome atomically.
- `OnOutcome` observes success, failure, cancellation, expiry, and skip exactly once; overlapping success/outcome registrations remain rejected.
- Exhausted/permanent coordinator failure fails the execution and cancels outstanding work.
- The adaptive-agent runnable example and e2e test use real PostgreSQL and assert public trace/history plus direct `flow_` table invariants.
- Full tests, PostgreSQL tests, race detector, and vet pass.
