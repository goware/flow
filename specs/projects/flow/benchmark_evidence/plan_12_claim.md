---
status: phase-gate
recorded_at: 2026-08-12
---

# Plan 12 claim projection evidence

Phase 1 of Plan 12 widens the one-per-batch claim-head SELECT from
`status,deadline_at,run_key` to `status,deadline_at,run_key,definition_name`
and stamps `ClaimedCommand.DefinitionName`. The row is already locked and
already read, so the change adds no statement, lock, or table access. This
record is the Section 5 Phase 1 gate for that widening.

## Reproduction note

The retained Plan 5 baseline file
(`plan_5_claim_baseline.go.txt`) targets the pre-v0.3 `flow_executions`
schema and the removed `Execute`/`WithMaxCommandsPerExecution` API, so it does
not compile against this tree and its original hardware is not available here.
The in-tree successor of that measurement, `BenchmarkSameRunClaimBatch`
(`hardening_benchmark_test.go:187`), measures the same shape: one
16-command same-run claim transaction, with fixture creation, probing, and
projection reset excluded from the timed region. It was therefore run
back-to-back on this machine before and after the change — an adjacent
before/after, not a reproduction of the Plan 5 baseline conditions.

## Environment

- Linux/amd64, Intel Core Ultra 9 285H, Go toolchain from the module
- PostgreSQL on 127.0.0.1:5432, local `flow_test` database at schema version 4
- `go test -run '^$' -bench BenchmarkSameRunClaimBatch -benchtime=2s -count=6 -p 1 .`
- Before: this worktree at `a569cf9` with no source changes
- After: the same worktree with the Phase 1 claim-projection change applied

## Result

| Sample | Before ns/op | After ns/op |
|---|---:|---:|
| 1 | 9,669,057 | 10,450,706 |
| 2 | 9,678,861 | 14,039,314 |
| 3 | 14,812,689 | 9,320,831 |
| 4 | 10,683,611 | 9,581,001 |
| 5 | 10,696,134 | 9,649,819 |
| 6 | 11,403,243 | 9,312,513 |

- Median: 10.690 ms before, 9.615 ms after (-10.1%)
- Minimum: 9.669 ms before, 9.313 ms after
- Allocations: 13,877 allocs/op and ~595 KB/op before; 13,881 allocs/op and
  ~597 KB/op after

Both series carry one outlier above 14 ms, and the spread within each series
(about 5 ms) is far wider than the difference between their medians. The
candidate is at or slightly better than the baseline on every summary
statistic and allocation counts are unchanged, so the added column shows no
regression beyond noise. Plan 12 STOP condition 1 does not apply and the
lazy-population fallback is not needed.

These are regression checks on one developer machine, not service guarantees.
