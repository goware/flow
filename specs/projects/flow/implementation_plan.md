---
status: complete
---

# Implementation Plan: flow

## Delivery discipline

Each phase begins with a focused `phase_plans/phase_N.md`, implements one coherent dependency layer, and ends with spec-aware review. A phase may land as several small, reviewable changes rather than one oversized change; each change includes the tests for the behavior it introduces, and their union satisfies the phase plan. The relevant portions of functional specification §22 and each component's acceptance conditions are the exit checklist for that phase; acceptance testing is not deferred to the end.

Inspection, replay, observation, and fault injection grow with the implementation. Phase 1 establishes the observer and test fault-hook contracts. Phase 3 starts the replay reducer when the first durable journal projections exist. Every later phase extends those threads alongside each new write path or state transition, so no phase leaves instrumentation, fault coverage, or replay semantics to be retrofitted.

## Examples and end-to-end contract

The implementation includes at least four runnable examples corresponding to functional specification §5:

1. a direct background command;
2. a planned dynamic fan-out and fan-in;
3. an external monitor publishing a fact that releases waiting work; and
4. a durable adaptive agent driven by a coordinator.

Example application work may use deterministic stubs that print, sleep briefly, and inject controlled success or failure. Flow itself is never faked: every example runs against a real PostgreSQL database using the embedded migrations, durable queue, journal, leases, and runtime. Where relevant, an example runs multiple runtime replicas to demonstrate distributed claiming and takeover.

Each example is also an automated end-to-end test. Shared scenario code prevents the runnable example and its test from drifting. Tests assert the public result, `Trace`, and `History`, then query PostgreSQL to verify the expected `flow_` rows, journal order and causation, queue cleanup, terminal projections, and exactly-once committed progression despite at-least-once handler invocation. Crash, retry, timeout, and restart variants are added where they materially prove the scenario's durability.

## Phases

- [x] Phase 1: Build the public contracts and deterministic foundation — package layout, typed IDs and definitions, immutable binding and registration metadata, canonical JSON and fingerprints, codecs, structured errors, declarative retry policy, pure engine value types, the no-op observer contract, internal named fault-hook mechanism, and the initial database-free `flowtest` harness.
- [ ] Phase 2: Build the PostgreSQL schema and store — embedded migrations, all nine `flow_` tables, constraints and indexes, row codecs, SQL error mapping, execution-first locking, gap-free journal allocation, deterministic append batches, the foundational bounded `History` journal scan, and the internal ordered semantic-transaction executor that every later write path extends.
- [ ] Phase 3: Implement execution creation and ingress — idempotent direct/plan/coordinator starts, command identity and creation, stored command ceilings and counters, queue materialization, `Execute`, `Issue`, `Publish`, cancellation, `Runtime.InTx`, application-event idempotency, the public `History` path, and the initial replay reducer for `ExecutionStarted`, `CommandCreated`, ingress events, and their materialized projections.
- [ ] Phase 4: Deliver direct distributed command execution — frozen runtime registry, capacity-bounded `SKIP LOCKED` claims, attempt history, leases, renewal and takeover, worker invocation, fencing, retries, timeouts and deadlines, declared commit functions, poll-first wake-up with optional notifications, graceful shutdown, minimal public `Trace`, replay of command attempts and terminal outcomes, and the real-PostgreSQL direct background-command example and end-to-end test. Exit evidence includes claim-probe query plans at required table scales plus same-execution burst and head-of-lane-kind benchmarks.
- [ ] Phase 5: Add durable graph semantics — staged `Emit` and `Spawn`, atomic child-membership closure, dependency groups, event waits, delays and `Within`, dependency-scoped `ResultOf` and `OutcomeOf`, skip cascades, fail-fast, cancellation/expiry races, and mode-correct completion for closed direct command trees.
- [ ] Phase 6: Implement plan-driven execution — the pure plan recorder and reads, declaration validation, lazy snapshot loading, reconciliation by key, dirty-plan scheduling and takeover, bounded fixed-point processing, compact `PlanReconciled` history and replay, purity verification, and the real-PostgreSQL planned fan-out/join and external-monitor examples with end-to-end tests. Exit evidence includes dirty-probe and plan-snapshot query plans at required scales plus 10/100/1,000-command reconciliation benchmarks.
- [ ] Phase 7: Implement coordinators and durable agents — typed coordinator definitions and state, start activation, `On` and `OnOutcome`, ordered historical delivery, serialized and fenced decisions, retryable handler failures, staged events and commands, explicit terminal decisions, and the real-PostgreSQL adaptive-agent example and end-to-end test.
- [ ] Phase 8: Complete inspection, testing, and operational surfaces — finish `Get`, `Lookup`, rich `Trace`, `List`, and `AwaitExecution` around the `History` foundation; complete `flowtest` worker/plan/coordinator support and the replay-vs-live conformance harness; finish observer coverage, migration compatibility checks, safe diagnostics, and documentation for the supported deployment roles.
- [ ] Phase 9: Harden and release Milestone 1 — run the complete functional-spec §22 suite, every runnable example as a real-PostgreSQL end-to-end test, direct database invariant checks, PostgreSQL concurrency and fault injection, ambiguous-commit and crash recovery, race tests, poll-only operation, rolling-version deployments, query-plan and workload benchmarks, journal-growth measurements, worked-example conformance, and final public API/documentation review.
