---
status: complete
completed_at: 2026-08-04
---

# Implementation plan: command-only architecture

## Controlling amendments

[`plans/3-remove-coordinator.md`](plans/3-remove-coordinator.md) supersedes the earlier plan/coordinator designs. [`plans/4-cross-execution-delivery.md`](plans/4-cross-execution-delivery.md) extends that command-only model with deliberately detached event ingress to a known execution. [`plans/5-hot-path-efficiency.md`](plans/5-hot-path-efficiency.md) reduces hot-path work without changing the public command/event model or six-table durability boundary. Plan 6 was a discarded `hardening-2` planning draft and was never committed; [`plans/7-lease-and-maintenance-bug-fixes.md`](plans/7-lease-and-maintenance-bug-fixes.md) retained only its necessary bug-fix subset. [`plans/8-v0.1.0-release-hardening.md`](plans/8-v0.1.0-release-hardening.md) is the controlling v0.1 release-hardening amendment. [`plans/9-simpler-flow-developer-experience.md`](plans/9-simpler-flow-developer-experience.md) and [`plans/10-run-ownership-and-transactional-ergonomics.md`](plans/10-run-ownership-and-transactional-ergonomics.md) are implemented together for the v0.3 candidate; they retain the command-only architecture and six-table schema. [`plans/11-inline-command-calls.md`](plans/11-inline-command-calls.md) remains a separate, unimplemented proposal. The Go API may still change intentionally during v0.x with release notes, while published migrations remain immutable and forward-only after v0.1.0.

## Implemented Plans 9–10 outcomes

- [x] Rename the public aggregate and live schema vocabulary from Execution to Run.
- [x] Replace bound/root and worker `Execute` forms with direct `Enqueue`.
- [x] Keep staged `flow.Emit` and detached targeted `Event.Deliver` as the two event paths.
- [x] Make `GetEventValue` presence-aware and round public durable durations upward once.
- [x] Add `CommandInfo.RunKey`, a named transaction client, and an explicit application-write phase boundary.
- [x] Add expected-ID-first atomic replacement for live-key run generations.
- [x] Prove the migration, concurrency, PostgreSQL 17/18, race, performance, and disposable Trails consumer gates.

The verification record is in [Plans 9–10 implementation evidence](benchmark_evidence/plans_9_10_release.md). Human review, publication, and tagging remain separate release decisions.

## Completed Plan 5 outcomes

- [x] Narrow the command-key index and reserve journal positions in one operation.
- [x] Resolve readiness through matching reverse waits and notify only for immediate runnable work.
- [x] Persist normalized child/event decisions and same-execution claims in bounded sets.
- [x] Claim independent execution groups concurrently within a pool-aware internal bound.
- [x] Reduce event-input materialization while retaining write, claim, and replay integrity checks.
- [x] Document efficient command, execution, event, payload, join, and transaction granularity.
- [x] Verify all 18 acceptance criteria with full PostgreSQL 17/18 ordinary and race gates.

Plan 5 is complete. Its final same-environment claim comparison, complete
before/after workload matrix, retained-journal cost, architecture/schema scans,
manual persistence-loop audit, version coverage, bounded variance, and
criterion-by-criterion release evidence are recorded in the
[Plan 5 benchmark evidence](benchmark_evidence/plan_5_hot_path.md#final-release-verification).

## Completed delivery

- [x] Make commands the only execution root and durable orchestration unit.
- [x] Return one `Execution` snapshot type from `Execute` and inspection; `Created` distinguishes creation from rediscovery.
- [x] Add bounded exact event snapshots and typed `GetEventValue` access.
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
- [x] Add `Event.Deliver` by reusing external ingress, with active-worker and caller-transaction coverage.

## Product boundary

The retained model supports sequence, fan-out, all-of fan-in, repeated join phases, external exact signals, and bounded loops. It intentionally does not support unsuccessful-outcome subscriptions, first-of-N, quorum/race gates, or open-ended mutable event handlers. Any future expansion requires separate evidence and design.
