---
status: complete
---

# Implementation plan: command/event/coordinator architecture

## Controlling amendment

[`plans/2-remove-plan.md`](plans/2-remove-plan.md) replaces the earlier declarative-graph design. This is a pre-release breaking change: removed API and storage formats have no aliases or migration path.

## Completed delivery

- [x] Add exact `WaitFor`/`Within` gates to direct root commands.
- [x] Add exact gates to worker/coordinator-staged commands through non-generic `Node`.
- [x] Define additive multi-wait identity, retained-event satisfaction, delay independence, expiry, and retry-budget timing.
- [x] Extend production decision recording and `flowtest` with wait/delay/optional fields.
- [x] Rewrite monitor as a direct event-gated command.
- [x] Rewrite fan-out as an explicit coordinator-owned dynamic join.
- [x] Keep all example logic self-documenting inside each `examples/*` package.
- [x] Remove public definitions, registrations, runtime scheduling, decisions, facts, reads, and modifiers for the declarative graph feature.
- [x] Remove command dependency groups, worker dependency snapshots, failure scopes, skip propagation, and graph maintenance.
- [x] Retain `ResultOf`/`OutcomeOf` only for inspection snapshots.
- [x] Implement reduced fail-fast for already-running attempts and cancel children staged after failure begins.
- [x] Rewrite baseline storage to seven tables and two execution modes.
- [x] Remove obsolete journal/replay/trace/history/observer/fault fields and kinds.
- [x] Update replaytest, migrations, examples, benchmarks, README, package docs, and active specifications.
- [x] Add database-free and PostgreSQL coverage for event gates, coordinator joins, and reduced fail-fast.
- [x] Run formatting, package tests, PostgreSQL integration tests, vet, and repository scans.

## Vertical slices used

1. Event gates landed while the old model still compiled, proving readiness semantics independently.
2. Monitor and fan-out migrated to the retained API, establishing product replacements.
3. The complete obsolete runtime vertical was deleted in one buildable cut.
4. Dependency/result plumbing and failure scopes were removed; `Node` became non-generic.
5. Schema, replay, inspection, and journal codecs moved together to the seven-table format.
6. Active specifications and acceptance evidence were synchronized after the full implementation passed.

## Compatibility and rollout

Development/test schemas created by the pre-release design must be recreated. Production rollout requires applying the current embedded baseline to a clean Flow schema. Application tables are unaffected.

All replicas using one Flow schema must run code compatible with the current migration checksum. Worker/coordinator version rolling remains supported after the schema cut because claims match exact registered definition versions.

## Historical artifacts

Phase plans 1–9 and the first reduction proposal are retained as historical implementation records. Where they mention the removed workflow model, this completed amendment and the active specifications are authoritative. Historical review files are intentionally untouched.
