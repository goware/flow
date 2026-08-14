# Plan 16 event-watch evidence

Status: Complete

Measured on 2026-08-14. The baseline was detached at `4550d6e` (the reviewed
Plan 16 text before implementation) with the external-event benchmark's
runtime changed evidence-only from `WithNotifications(false)` to
`WithNotifications(true)`. The after samples used the same command and server
with the Plan 16 implementation in the `plan-16` worktree.

## Environment

- Go: `go1.26.5 linux/amd64`
- CPU: Intel Core Ultra 7 255H
- PostgreSQL: 18.1 Debian, x86-64
- durability: `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`
- application pool: 12 connections

## Event-only ingress

Command:

```sh
go test -run '^$' \
  -bench '^BenchmarkExternalEventIngress/hot_live/no_match$' \
  -benchtime=3s -count=5 .
```

| Shape | Baseline range; median | After range; median | Change |
|---|---:|---:|---:|
| latency | 3.927–4.215 ms; 4.139 ms | 4.050–4.463 ms; 4.186 ms | +1.1% |
| rate | 237.3–254.6; 241.6 events/s | 224.1–246.9; 238.9 events/s | -1.1% |
| bytes/op | 22,473–23,106; 22,750 | 22,791–23,751; 23,127 | +1.7% |
| allocs/op | 419 | 429 | +2.4% |

The event-watch hint adds one transactional `pg_notify` statement to a newly
accepted event-only delivery: the focused query tracer records 8 statements
with notifications disabled and 9 with them enabled, exactly one of which is
`pg_notify`. An event that releases immediately runnable work emits only the
existing stronger `run` hint. Equivalent redelivery, rollback, and a rejected
worker `WithCommit` emit no committed event or hint. Two identical hints issued
for one run by separate semantic operations in one caller transaction are
folded into one delivered notification by PostgreSQL. Median latency remains
well inside the plan's 10% investigation gate.

## Sparse post-cursor read

`TestEventWatchSparsePostCursorPlan` runs the exact production query with the
only matching event last. PostgreSQL used the `(run_id, position)` primary-key
range for every shape; no schema/index change was adopted.

| Post-cursor rows | Execution time | Shared buffers | Rows filtered |
|---:|---:|---:|---:|
| 100 | 0.041 ms | 6 | 99 |
| 1,000 | 0.144 ms | 39 | 999 |
| 10,000 | 1.082 ms | 340 | 9,999 |

The 10,000-row shape used a bitmap scan of the same primary key and completed
in about one millisecond. This is acceptable for the target run sizes and does
not justify another index.

## Watch resource shape

`TestEventWatchThousandIdleWatchersDoNotPoll` constructs 1,000 watches for one
run and starts 1,000 caller-owned `Next` goroutines. After each immediate
durable read completes, an idle interval produces zero additional queries,
the application pool has zero acquired connections, exactly one dedicated
listener connection serves the runtime, command/queue/lease counts are
unchanged, registration itself adds no Flow goroutine, and closing all watches
removes the one shared run entry.

Focused race coverage also proves targeted unrelated-run isolation,
cross-runtime/multi-watcher broadcast, listener-disconnect catch-up, malformed
hint broad catch-up, disabled-writer characterization, pre-Run listener
startup catch-up, terminal/pruned-run behavior, corruption rejection,
sequential cursor ordering, cancellation reuse, live-run replacement, and
runtime shutdown cleanup. Worker-staged events wake a remote watch only after
successful settlement. `Close` also cancels a `Next` blocked on application-
pool acquisition, and stopping a runtime cancels watch construction blocked on
its initial database read.

Final verification used a reset PostgreSQL database. The complete ordinary and
race suites, build, vet, formatting, module tidy/verification, and diff checks
passed. The named-test audit ran 531 tests with zero named skips or failures.
