---
status: complete
recorded_at: 2026-07-26
---

# Phase 6 plan-reconciliation evidence

`TestDirtyPlanAndEventSnapshotQueryPlans` builds isolated, production-shaped
PostgreSQL fixtures. The dirty-plan population contains only ten executions for
the locally registered definition among the requested scale. The journal
population contains 1% matching facts and 99% unrelated events. The test runs
the production query shapes with `EXPLAIN (ANALYZE, BUFFERS, WAL)` and requires
the intended partial indexes to appear.

| Aggregate rows | Dirty-plan access path | Dirty probe | Exact-event access path | Matching facts | Exact-event scan |
|---:|---|---:|---|---:|---:|
| 10,000 | index-only scan on `flow_executions_plan_queue_idx` | 0.039 ms | index scan on `flow_journal_event_lookup_idx` | 100 | 0.077 ms |
| 1,000,000 | index-only scan on `flow_executions_plan_queue_idx` | 0.072 ms | index scan on `flow_journal_event_lookup_idx` | 10,000 | 38.279 ms |
| 10,000,000 | index-only scan on `flow_executions_plan_queue_idx` | 0.059 ms | index scan on `flow_journal_event_lookup_idx` | 100,000 | 2,604.014 ms |

The 10M event measurement was a cold-buffer run and read 101,045 buffers. It
measures returning and materializing all 100,000 matching facts, so it is not
expected to be constant as the selected history itself grows. The important
planner property is that unrelated history is reached through the exact
execution/namespace/name/version/position index rather than a sequential scan.
Applications with an unbounded number of facts under one selector must still
budget for the selected values they ask a plan to consume.

The 10M fixture took about 19 minutes and roughly 11 GB at peak to build on the
development machine. It is intentionally opt-in; the ordinary integration
suite exercises the same assertions at 10K.

```text
FLOW_PLAN_QUERY_SCALE=1000000 go test -timeout=10m -run TestDirtyPlanAndEventSnapshotQueryPlans -count=1 -v .
FLOW_PLAN_QUERY_SCALE=10000000 go test -timeout=20m -run TestDirtyPlanAndEventSnapshotQueryPlans -count=1 -v .
```

## Reconciliation baseline

`BenchmarkPlanReconciliation` measures one complete real-PostgreSQL plan
transaction after execution ingress. It includes the locked structural
snapshot, verified pure evaluation, `PlanReconciled`, all `CommandCreated`
entries, command/queue projections, and graph resolution.

| Newly declared commands | One reconciliation |
|---:|---:|
| 10 | 53.545 ms |
| 100 | 88.201 ms |
| 1,000 | 353.666 ms |

These are single-iteration development-machine baselines, not service-level
objectives. They exist to catch query or per-command regressions as the schema
and engine evolve.

```text
go test -run '^$' -bench '^BenchmarkPlanReconciliation$' -benchtime=1x -count=1 .
```
