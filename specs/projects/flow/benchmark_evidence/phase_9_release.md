---
status: complete
historical: true
superseded_by: remove_plan.md
recorded_at: 2026-07-26
---

# Phase 9 release benchmark evidence

> Historical baseline for the superseded architecture. Current retained workload evidence is in `remove_plan.md`; workflow-reconciliation measurements below are not a current product claim.

These development-machine measurements are regression baselines, not service-level objectives. They ran on Linux/amd64 with an Intel Core Ultra 7 255H and a local PostgreSQL server. `FLOW_TEST_DATABASE_PASSWORD` was configured without being printed.

## Ordinary workload sweep

```text
go test -run '^$' -bench 'Benchmark(ClaimProbeUnhandledHead10K|SameExecutionClaimBurst1000|PlanReconciliation|ExecutionIngressNotification|CoordinatorSparseOutcomeScan10K|Inspection100Commands)$' -benchtime=1x -count=1 .
```

| Workload | Result |
|---|---:|
| handled-kind claim probe through a 10K adversarial queue | 1.082 ms |
| claim 1,000 commands from one execution in batches of 32 | 885.632 ms |
| poll-only execution ingress commit | 5.717 ms |
| notify-enabled execution ingress commit | 5.443 ms |
| one sparse coordinator scan across 10K unmatched events | 5.133 ms |
| `History` for a 100-command execution | 0.545 ms |
| `Trace` for a 100-command execution | 2.976 ms |
| reconcile 10 new commands | 39.469 ms |
| reconcile 100 new commands | 77.251 ms |
| reconcile 1,000 new commands | 345.738 ms |

Notification commit cost is noisy at single-iteration scale. A 20-iteration, three-sample run produced poll-only means of 3.832–6.400 ms and notify-enabled means of 6.435–6.888 ms. This is the expected trade: transactional `pg_notify` may add commit work, while the listener materially reduces distributed wake latency when the correctness poll is long. `TestDistributedNotificationAndReconnectCatchUp` proves completion under a five-second poll interval in under two seconds, including reconnect catch-up.

The sparse coordinator scan measured 4.114–7.148 ms over repeated 20-iteration samples. More importantly, `scan_position` is advanced durably after that scan, so the unchanged coordinator is excluded by the partial idle probe and does not rescan the same 10K-entry prefix on each scheduler poll. `TestCoordinatorScanCursorAvoidsRepeatedIdleHistoryScans` proves that behavior.

Repeated 100-command inspection samples measured `History` at 0.381–0.528 ms and `Trace` at 2.102–2.631 ms.

## Journal growth

`TestJournalGrowthMeasurement100Commands` declares 100 commands in one plan revision and measures the retained rows after reconciliation:

| Measure | Value |
|---|---:|
| journal rows | 102 |
| tuple bytes (`sum(pg_column_size(row))`) | 88,037 bytes |
| encoded body bytes | 72,055 bytes |
| tuple bytes per declared command | 880.4 bytes |

The 102 rows are one `ExecutionStarted`, 100 `CommandCreated`, and one compact `PlanReconciled`. Attempts and terminal events are deliberately absent from this declaration-only measurement and grow with actual execution history.

## Ordinary query plans

The default 10K query-plan gates passed with the intended indexes:

| Query | Required access path | Execution |
|---|---|---:|
| command claim probe | `flow_command_queue_claim_idx` index-only scan | 0.046 ms |
| dirty plan probe | `flow_executions_plan_queue_idx` index-only scan | 0.039 ms |
| exact fact snapshot | `flow_journal_event_lookup_idx` index scan | 0.091 ms |

The exact commands and the opt-in 1M/10M results remain in [`phase_4_claim.md`](phase_4_claim.md) and [`phase_6_plan.md`](phase_6_plan.md). The large fixtures are not repeated in every release run; they are rerun when their SQL or supporting index changes.

## Interpretation

- PostgreSQL connections are bounded by the configured worker/plan/coordinator concurrency plus one optional dedicated notification session per running runtime; handlers hold no database connection while doing application work.
- The execution row remains the intentional serialization point for semantic changes within one execution. Independent executions proceed concurrently.
- Transactional notification hints are latency hints only. Poll-only mode passes the same correctness suite.
- The retained journal is the dominant history cost. Payload/body retention and archival remain the near-term operational follow-on identified in the architecture.

## Runnable example release run

All four actual binaries completed against one freshly created PostgreSQL database:

```text
go run ./examples/direct
go run ./examples/fanout
go run ./examples/monitor
go run ./examples/agent
```

The database inspections contained four succeeded executions (one direct, two plan, one coordinator), 11 terminal commands, zero queue rows, one coordinator, one satisfied event-wait row, and 78–79 ordered journal rows. The one-row variation is an allowed difference in how closely timed dirty-plan triggers coalesce into reconciliation revisions; command creation and terminal outcomes were identical. All nine migrated `flow_` tables existed. Each temporary database was dropped after inspection.
