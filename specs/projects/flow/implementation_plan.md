---
status: complete
completed_at: 2026-08-04
---

# Implementation plan: command-only architecture

## Controlling amendment

[`plans/3-remove-coordinator.md`](plans/3-remove-coordinator.md) supersedes the earlier plan/coordinator designs. This is a pre-release breaking change; removed APIs and durable formats have no compatibility aliases or data migration.

## Completed delivery

- [x] Make commands the only execution root and durable orchestration unit.
- [x] Add bounded exact event snapshots and typed `ReadEvent` access.
- [x] Enforce at most 256 exact waits per command and expose satisfying positions in trace.
- [x] Keep worker-staged events/sub-commands/result/application commit atomic and fenced.
- [x] Rewrite fan-out as two command-owned fan-out/join phases.
- [x] Rewrite agent as a bounded self-composing command loop.
- [x] Make monitor consume its externally published gated event.
- [x] Remove coordinator definitions, handlers, state, runtime scheduling, outcomes, observers, faults, testing helpers, and storage.
- [x] Remove the public `Scope`, `Outcome`, `OutcomeOf`, and `ResultSource` abstractions.
- [x] Remove execution modes and command origin; derive provenance from parent identity.
- [x] Rewrite baseline storage to six tables and simplify journal/replay/history/trace.
- [x] Replace coordinator benchmarks with event-input and command-only workloads.
- [x] Synchronize README, package docs, examples, and active specifications.
- [x] Verify formatting, static analysis, package/PostgreSQL/race tests, migration inventory, and removed-symbol scans.

## Product boundary

The retained model supports sequence, fan-out, all-of fan-in, repeated join phases, external exact signals, and bounded loops. It intentionally does not support unsuccessful-outcome subscriptions, first-of-N, quorum/race gates, or open-ended mutable event handlers. Any future expansion requires separate evidence and design.
