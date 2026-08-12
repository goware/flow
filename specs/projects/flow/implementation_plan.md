---
status: complete
completed_at: 2026-08-04
---

# Implementation plan: command-only architecture

## Controlling amendments

[`plans/13-simpler-core-retention-and-typed-reads.md`](plans/13-simpler-core-retention-and-typed-reads.md)
was independently reviewed and accepted at `c450ff4`. It keeps the command-only
architecture while removing unused semantic modes and duplicate storage,
adding definition-bound reads, batched queue statistics, decision bounds, and
explicit bounded retention. Existing development data is disposable, so the
schema is one clean Run-named baseline rather than a compatibility chain.

[`plans/11-inline-command-calls.md`](plans/11-inline-command-calls.md) remains
deferred. [`plans/12-fast-lease-recovery-for-idempotent-commands.md`](plans/12-fast-lease-recovery-for-idempotent-commands.md)
has been reconciled against accepted Plan 13 commit `c450ff4`; it remains
planned and unimplemented.

## Implemented Plans 9–10 outcomes

- [x] Rename the public aggregate and live schema vocabulary from Execution to Run.
- [x] Replace bound/root and worker `Execute` forms with direct `Enqueue`.
- [x] Keep staged `flow.Emit` and detached targeted `Event.Deliver` as the two event paths.
- [x] Make `GetEventValue` presence-aware and round public durable durations upward once.
- [x] Add `CommandInfo.RunKey`, a named transaction client, and an explicit application-write phase boundary.
- [x] Add expected-ID-first atomic replacement for live-key run generations.
- [x] Prove the migration, concurrency, PostgreSQL 17/18, race, performance, and disposable Trails consumer gates.

The verification record is in [Plans 9–10 release evidence](benchmark_evidence/plans_9_10_release.md). The reviewed implementation was released as v0.3.0 at `0ad8f4064425ba535b848853149b3f453992a850`.

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
- [x] Return compact start/replacement operation results; inspection returns
  full `Run` snapshots.
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
