---
status: complete
historical: true
superseded_by: ../plans/3-remove-coordinator.md
---

# Phase 6: Plan-Driven Execution

> Historical delivery record. This complete feature vertical was removed by `../plans/2-remove-plan.md`.

## Overview

Complete the pure, additive plan driver on top of the Phase 5 graph engine.
Plan starts remain cheap durable ingress operations; compatible replicas claim
dirty executions independently, evaluate against immutable history and command
state, and commit one deterministic reconciliation decision. Several triggers
may coalesce safely, crashes leave the durable dirty bit available for another
replica, and plan defects fail only the orchestration execution rather than
rolling back already-committed worker or application work.

## Steps

1. Complete the in-memory plan recorder and typed reads for `Do`, facts,
   results, outcomes, child membership, dependency groups, exact-version waits,
   delays, retry overrides, optional nodes, and declaration validation.
2. Split snapshot loading into structural state plus resumable exact-selector
   event loading through the journal lookup index; retain one locked journal
   high-water position and never scan unrelated event history.
3. Add deterministic evaluation output and debug/test double-evaluation,
   comparing declarations, topology, read availability, and selector discovery
   without exposing clocks, clients, transactions, or worker scopes to plans.
4. Reconcile only genuinely new keys, preserve accepted creation-time defaults,
   reject changed explicit declaration identity, enforce ownership and command
   ceilings, and record compact `PlanReconciled` identity deltas.
5. Process immediate skip/expiry cascades to a bounded fixed point inside the
   same execution transaction. Clear `plan_dirty` only after a complete final
   pass; persist exact quiescence and temporary-read diagnostics.
6. Carry required-failure scope through plan-declared failure branches and
   descendants so fail-fast cannot cancel recovery work while unrelated new
   declarations cannot escape cancellation.
7. Finish the skip-locked dirty-plan scheduler, coalescing, rollback/takeover,
   observations, wake hints, replay, and terminal completion for successful and
   failing plan executions.
8. Extend database-free plan testing and add real-PostgreSQL end-to-end tests
   for dynamic fan-out/fan-in, external monitor publication, early/late facts,
   failure branches, invalid plans, coalesced triggers, and reconciler restart.
9. Record dirty-probe and exact plan-snapshot `EXPLAIN` evidence and benchmark
   10/100/1,000-command reconciliation before closing the phase.

## Exit checks

- Repeated equivalent evaluation creates no duplicate command or journal row.
- Missing or changed dependencies, cycles, ownership conflicts, divergent
  explicit policy, panic, and command-ceiling violations record `PlanFailed`.
- A crash before reconciliation commit leaves the execution dirty and another
  compatible runtime reaches the same result.
- Immediate terminal declarations cannot stall waiting for a future trigger.
- Plan success is impossible while a temporary read, dirty work, open command,
  or non-quiescent declaration remains; failure ignores permanently unavailable
  happy-path values but waits for durable failure handling.
- Dynamic fan-out/join and monitor-published fact examples pass against real
  PostgreSQL, and replay agrees with every settled live projection.

## Completion evidence

- The runnable fan-out/join and external-monitor examples share their scenario
  code with real-PostgreSQL end-to-end tests; both assert public results,
  `Trace`, history, queue cleanup, journal creation/terminal counts, and wait
  satisfaction directly in PostgreSQL.
- Dirty-plan rollback/takeover, coalesced immutable reads, deterministic double
  evaluation, failure handling, immediate skip and initial-schedule expiry
  fixed points, and replay/projection equality are covered by integration
  tests. Database-free recorder tests cover forward references, missing keys,
  cycles, invalid `Within`, permanent result absence, and read-order stability.
- The 10K, 1M, and 10M dirty-probe and exact-event snapshot plans plus the
  10/100/1,000-command reconciliation baselines are recorded in
  `benchmark_evidence/phase_6_plan.md`.
- `go test ./...`, `go test -race ./...`, `go vet ./...`, all three current
  example e2e tests, and the plan benchmarks pass against real PostgreSQL.
