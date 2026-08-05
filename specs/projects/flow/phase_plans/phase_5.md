---
status: complete
historical: true
superseded_by: ../plans/2-remove-plan.md
---

# Phase 5: Durable Graph Semantics

> Historical delivery record. Its dependency-graph portions were deleted by `../plans/2-remove-plan.md`.

## Overview

Extend the distributed command runtime from isolated commands into durable,
closed command trees and persisted dependency graphs. Worker and coordinator
scopes stage typed application events and child commands entirely in memory;
only a successful fenced decision may atomically append and materialize those
outputs. The store gains one terminal-transition path that resolves dependency
groups and event waits, cascades skips, initializes readiness and wait clocks,
and derives mode-correct completion. Direct executions can therefore perform
bounded dynamic fan-out through `Spawn` without a plan, while the same graph
machinery is ready for plan reconciliation in Phase 6 and coordinators in
Phase 7.

## Steps

1. Implement the deterministic decision buffer shared by worker and coordinator scopes: typed `Emit`, typed `Spawn`, mandatory stable event keys, canonical payload limits, required/optional classification, positive `StartAfter`, duplicate coalescing, conflict poisoning, and all-or-nothing validation.
2. Extend worker preparation with one batched graph-input snapshot: explicitly declared dependency identities, immutable results and terminal outcomes, and scoped `ResultOf`/`OutcomeOf` decoding with permanent structured errors for unauthorized, mismatched, or unavailable reads.
3. Add store descriptors and ordered operations for child creation, application events, dependency groups/members, event waits, initial readiness, authoritative child membership closure, command-ceiling enforcement, and deterministic journal causation.
4. Refactor successful settlement into one atomic graph transition: attempt conclusion, emitted facts, spawned `CommandCreated` records/materializations, parent success, dependency/wait resolution, skip cascades, plan dirtying, direct-tree counters/completion, and the declared commit function.
5. Route unsuccessful terminal settlement, cancellation, expiry, and publication through the same dependency/wait resolver so `After`, `AfterSettled`, `AfterFailed`, `AtLeast`, and exact-version `Await` have once-only durable transitions.
6. Implement PostgreSQL-anchored `StartAfter`, dependency-gated `Within`, wait-expiry maintenance, the fact-at-deadline race, execution-deadline capping, and poll-only wake-up without sleeping handlers or leased local timers.
7. Extend journal codecs, replay, `Trace`, and observations for application events, child topology, required/optional classification, dependency/wait status, delayed eligibility, skip/expiry cascades, and direct-tree completion.
8. Extend `flowtest` with worker decision assertions for staged events, children, schedules, conflicts, and dependency-scoped result/outcome reads.
9. Add real-PostgreSQL integration and fault tests for atomic fan-out, duplicate retries, command ceilings, dependency groups, early/late facts, wait expiry, failure branches, cancellation races, delayed children, and replay-versus-projection equality.

## Tests

- Worker errors, panics, lease loss, invalid output, commit-function failure, and injected rollback persist no staged event or child; a successful retry commits one equivalent decision.
- Equivalent repeated `Emit`/`Spawn` calls coalesce within a decision; conflicting content or options poison the full decision and return structured conflict/invalid errors.
- Parent success, complete direct-child membership, children, emitted events, application commit write, parent terminal event, dependency transitions, counters, and completion commit atomically in deterministic order.
- `StartAfter` uses accepting PostgreSQL time, keeps the child visible without an early queue claim, preserves its budget anchor through retry/restart, and cannot extend the execution deadline.
- Required and optional descendants affect direct completion correctly; temporary quiescence cannot complete a tree before a successful parent closes membership and creates all children.
- All dependency group kinds resolve once; impossible success dependencies skip transitively while settled/failed branches remain eligible, including fail-fast survivor behavior.
- Worker `ResultOf`/`OutcomeOf` reads only explicitly declared dependencies, checks durable definition identity, decodes batched immutable values, and distinguishes success from structured terminal failure.
- Exact-version waits work publish-before-declare and declare-before-publish; `Within` begins only after command dependencies settle, and PostgreSQL acceptance on the correct side of its persisted deadline deterministically wins or loses.
- Cancellation and expiry of running work conclude the active attempt once, resolve dependents, remove fences, and reject a stale handler settlement.
- Replay, `Trace`, and direct SQL agree on topology, events, schedules, waits, attempts, terminal states, counters, and journal positions after every fault variant.
