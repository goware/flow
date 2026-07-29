---
status: complete
---

# Implementation Plan: flow smaller API migration

## Controlling contract

[`1-smaller.md`](1-smaller.md) is the controlling amendment. The synchronized overview, functional specification, architecture, and component documents describe its integrated end state. Removed pre-release APIs receive no compatibility aliases or deprecated wrappers.

The existing Milestone 1 implementation is the tested baseline. This migration changes it in coherent layers while preserving journal ordering, fencing, retries, distributed operation, and real-PostgreSQL examples.

## Delivery discipline

Each phase includes implementation, unit/integration tests, replay updates, observer/fault updates, formatting, and relevant acceptance checks. A phase may land in several reviewable changes. The worktree's untracked review documents are user material and remain untouched.

Inspection and replay evolve with each write path. Database migration and row codecs change together. Public examples compile at every phase after the public-contract switch; temporary internal adapters are allowed only within one phase and are deleted before its exit.

## Examples and E2E contract

The repository retains four runnable examples and matching E2E tests:

1. direct background command;
2. dynamic fan-out/fan-in plan;
3. external monitor using exact-key `Event.Emit`;
4. durable adaptive coordinator agent.

Examples may print and sleep briefly for fake application work. Flow itself is never mocked in E2E tests. Tests use real PostgreSQL, exercise embedded migrations and runtime schedulers, and assert public result, `Trace`, `History`, graph/journal order, queue cleanup, and terminal projections. Multi-replica/takeover variants remain where relevant.

## Phases

- [x] Baseline Milestone 1: original nine implementation phases completed and tests passing before the smaller-API migration.

- [x] Phase 1 — synchronize design artifacts: incorporate `1-smaller.md` into project overview, functional spec, architecture, engine/runtime/schema components, and this implementation plan; mark them draft during implementation and complete only after final verification.

- [x] Phase 2 — reduce public contracts and pure values: introduce `Outcome[R]`, unversioned `Event[T]`, sealed `EventRef`, `WithRetry`/`Attempts`, fixed persisted jitter, typed `Node[R]`, universal in-execution `Execute`, node `Key`/`Optional`/`Delay`, node plan reads, exact-key `Fact`/`WaitFor`, and `Coordination.Succeed`/`Fail`; remove legacy public names without aliases; update compile contracts and deterministic tests.

- [x] Phase 3 — simplify decision engines: retain small staged worker/coordinator application events while removing separate plan/child verbs and options, quorum dependencies, free plan reads, plan-node retry overrides, command-success event descriptors, and external command injection; preserve atomic worker membership, outcome reads, scope poisoning, plan purity, coordinator fan-in, and replay semantics.

- [x] Phase 4 — migrate PostgreSQL storage and ingress: edit the initial migration and all SQL/codecs to remove event versions, dependency thresholds, external-issue origin, command-success selector namespace, and execution `outcome_ref`; add exact event keys to waits; retain coordinator retry policy/hash; implement `Event.Emit`; update store/replay/migration/concurrency tests and query-plan evidence.

- [x] Phase 5 — update distributed runtime and operations: remove legacy wake/observer/fault paths, make claims and settlement consume reduced records, keep a fixed 60-second public lease with an unexported test seam, persist fixed effective retry/coordinator policies, use staged coordinator methods, and preserve multi-replica, poll-only, cancellation, deadline, fencing, and transaction-order guarantees.

- [x] Phase 6 — rewrite examples, documentation, and E2E tests: migrate all public/internal examples and tests to `Execute`, `Event.Emit`, exact keys, `Outcome[R]`, node reads, `Node.Delay`, and coordinator methods; ensure direct/fan-out/monitor/agent programs run against real PostgreSQL and assert trace/history/database state.
- [x] Follow-up — restore `flow.Emit(scope,event,key,payload)` for worker/coordinator decisions; settle staged events atomically with children, terminal output, state/inbox, and commit functions; extend flowtest, replay/trace assertions, fault coverage, examples, and docs.

- [x] Phase 7 — final hardening and signoff: run gofmt, static analysis, complete unit/integration/E2E suite, PostgreSQL fault/race/replay tests, query-plan/benchmark checks, removed-symbol and schema audits, `go doc` review, and requirement-by-requirement acceptance evidence; then mark all synchronized specs and `1-smaller.md` complete.

## Global exit checklist

- No removed exported identifier appears in `go doc github.com/goware/flow`.
- No legacy implementation branch remains for `Issue`, quorum, event versions, node retry overrides, command-success selectors, or free coordinator completion. Staged handler events are a retained first-class settlement path.
- Exactly nine `flow_` tables remain with the reduced columns/constraints.
- Stored command and coordinator retry behavior is stable across restarts and rolling defaults.
- Full replay equals live projections.
- All four real-PostgreSQL E2E examples pass without unexpected skips.
- The worktree contains no unintended modifications to user review documents.
