---
status: complete
historical: true
superseded_by: remove_plan.md
recorded_at: 2026-07-26
---

# Phase 4 claim-probe evidence

> Historical baseline. The original dedicated query-plan fixture was removed during the pre-release architecture reduction; current retained workload evidence is in `remove_plan.md`.

`TestClaimProbeQueryPlan` builds an isolated real-PostgreSQL queue with 90% of
the oldest rows assigned to an unregistered command kind and the remaining 10%
assigned to the locally handled kind. The production probe asks for 32 handled
candidates and the test requires `flow_command_queue_claim_idx` to appear in
`EXPLAIN (ANALYZE, BUFFERS, WAL)`.

| Queue rows | Access path | Rows returned | Planning | Execution |
|---:|---|---:|---:|---:|
| 10,000 | index-only scan on `flow_command_queue_claim_idx` | 32 | 0.328 ms | 0.051 ms |
| 1,000,000 | index-only scan on `flow_command_queue_claim_idx` | 32 | 0.485 ms | 0.079 ms |
| 10,000,000 | index-only scan on `flow_command_queue_claim_idx` | 32 | 0.426 ms | 0.071 ms |

All three plans performed one index search and 32 heap fetches. The 10M
fixture took about 166 seconds to construct on the development machine; that
load time is not included in the query execution value. The fixture table is
unlogged because this benchmark measures the read plan, not ingestion WAL, and
is dropped with its private test schema.

Reproduce a scale explicitly with:

```text
FLOW_CLAIM_PLAN_SCALE=10000000 go test -timeout=20m -run TestClaimProbeQueryPlan -count=1 -v .
```

The default scale is 10,000 rows so ordinary integration runs remain fast. The
larger scales are opt-in gates to rerun whenever the claim SQL or supporting
index changes.

## Store-level benchmark baseline

The phase also ships two explicit `-benchtime=1x` benchmarks. On the same
development machine:

| Benchmark | Result |
|---|---:|
| handled-kind probe through the store with a 10K adversarial backlog | 1.082 ms |
| 1,000 batched claims from one execution | 885.632 ms |

The burst result includes all 1,000 durable `AttemptStarted` entries. The
scheduler groups immediately runnable siblings into capacity-sized commits, so
they share the execution lock without weakening the execution-local journal
order. This is intentionally not presented as a global throughput limit:
unrelated executions lock and claim concurrently.

```text
go test -run '^$' -bench 'Benchmark(ClaimProbeUnhandledHead10K|SameExecutionClaimBurst1000)$' -benchtime=1x -count=1 .
```
