# Plan 5 hot-path benchmark evidence

## Baseline identity

- Commit: `d2713d8b400702a59ecc1e7786a492ca6824abb3`
- Recorded: 2026-08-06
- Host: Linux 6.18.41, amd64
- CPU: Intel Core Ultra 7 255H, 16 logical CPUs (`GOMAXPROCS=16`)
- Memory: 62 GiB usable, no swap
- PostgreSQL: 18.1 (Debian 18.1-1.pgdg13+2), local NVMe-backed instance
- Durability: `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`
- PostgreSQL `max_connections`: 100
- Flow test pool: 12 connections per isolated benchmark schema
- Lifecycle/fan-out runtime: 16 workers, notifications disabled, 5 ms correctness poll

These measurements are development-machine regression evidence, not service
level objectives. They must not become wall-clock assertions in ordinary tests.
Machine load and PostgreSQL cache state can move the results between runs.

The historical measurements that motivated this work remain in the approved
[Plan 5 document](../plans/5-hot-path-efficiency.md#31-development-machine-baseline).
They were not overwritten.

## Commands

The test database connection was supplied through the existing
`FLOW_TEST_DATABASE_URL` / `FLOW_TEST_DATABASE_PASSWORD` configuration. No
credential value is recorded here.

```text
go test -count=1 -run '^TestJournalGrowthMeasurement100Commands$' -v .

go test -run '^$' \
  -bench 'Benchmark(IndependentCommandLifecycle|SameExecutionFanout|StagedDecisionBatch|ExternalEventIngress|ExecutionIngressNotification|EventSnapshotMaterialization)' \
  -benchmem -benchtime=3s -count=5 .

go test -run '^$' -bench '^BenchmarkExternalEventIngress$' \
  -benchmem -benchtime=3s -count=5 .

FLOW_BENCHMARK_STRESS=1 go test -run '^$' \
  -bench '^BenchmarkSameExecutionFanoutStress1000$' \
  -benchmem -benchtime=1x -count=1 .
```

The five-sample command completed successfully in 828.278 seconds. Every
PostgreSQL-backed shape executed; the explicitly opt-in 1,000-command workload
was the only skip in that command and was run separately as shown above. After
review corrected the external targets to use finite deadlines and a separate
unresolved hold gate, the focused external-event command replaced those rows;
it completed successfully in 155.539 seconds.

## Workload configuration

`None` is Go's `struct{}` and encodes as canonical JSON `{}`. Unless a row says
otherwise, command definitions use version 1, the `default` queue, default retry
policy, no attempt timeout, no delay, required children, fail-fast enabled, empty
metadata, and distinct permanent execution keys.

| Workload | Durable payloads | Execution options | Runtime shape |
|---|---|---|---|
| execution ingress | `None` root arguments; root is not executed | `WithoutExecutionDeadline`; default 1,000-command ceiling | runtime not run; notifications false/true by sub-benchmark; 12-connection pool |
| independent lifecycle | `None` root arguments and results | `WithoutExecutionDeadline`; default 1,000-command ceiling | notifications off; 16 workers; 5 ms poll; 64 executions per timed batch; 1/4/16 producers |
| same-execution fan-out | `None` root/child arguments and results | `WithoutExecutionDeadline`; command ceiling disabled with `WithMaxCommandsPerExecution(0)` | notifications off; 16 workers; 5 ms poll; 10/100 commands including root, or opt-in 1,000 |
| staged decision | `None` root/child arguments/results and event payloads | `WithoutExecutionDeadline`; command ceiling disabled | runtime not run; direct root claim; 1/10/100 children and 0/10/100 staged events |
| external event ingress | `None` root/child arguments/results and event payloads | finite 30-minute execution deadline; command ceiling disabled; one separate `benchmark/hold` wait is never emitted | runtime not run; notifications off; distinct, hot, or 100-command retained target |
| snapshot materialization | `None` root arguments; string event payload whose canonical JSON is exactly 1 KiB or 64 KiB | `WithoutExecutionDeadline`; default 1,000-command ceiling; 1/32/256 exact root waits | runtime not run; notifications off; direct one-command claim |
| journal growth | `None` root/child arguments and results | `WithoutExecutionDeadline`; command ceiling disabled | notifications off; 16 workers; 5 ms poll; exactly 100 commands including root |

Every external target is checked outside the timer to remain `running`, have a
non-null finite deadline, and retain exactly one unresolved
`benchmark.external.event` / `benchmark/hold` wait after measured ingress.

## Timed-region boundaries and shapes

- Execution ingress excludes migration and runtime construction. It measures
  one fresh permanent-key start per operation.
- Independent lifecycle excludes migration, registration, and runtime startup.
  Each timed operation starts and awaits 64 distinct one-command executions.
  The producer concurrency is 1, 4, or 16; the reported rate divides all 64
  completed commands by timed duration.
- Same-execution fan-out includes execution ingress, root/child claims, no-op
  workers, settlement, and terminal observation. Counts include the root. The
  10- and 100-command shapes use ordinary repeated calibration; 1,000 commands
  is an explicit one-shot stress workload.
- Staged-decision timing isolates `SettleCommandSuccess`. Execution ingress,
  root claim, in-memory staging, normalization, and later child execution are
  outside the timer. Child declarations rotate through zero waits, one wait,
  and three waits. When events exist, waits select staged events; with zero
  events they remain deliberately unsatisfied.
- External event targets remain live through a deliberate unsatisfied gate;
  no unbounded execution or sleeping worker is used. The retained shape has
  exactly 100 command rows (one settled root and 99 waiting children) and 99
  event-specific reverse-wait rows plus one separate hold row. `match_one`
  records 99 distinct events per fixture;
  `match_several` records 11 distinct events, each satisfying nine children.
  Custom `ns/event` and `events/s` metrics normalize those batched benchmark
  operations.
- Event snapshot timing includes `ClaimCommand` only. Execution creation,
  accepted event ingress, and candidate preparation are outside the timer. One
  immutable accepted event fixture is reused; after each claim, benchmark-only
  SQL removes exactly that attempt-started journal row and restores its command,
  queue, and journal allocator projection before the next timed claim. This
  reset does not alter event bodies or wait positions. The benchmark therefore
  isolates repeatable claim materialization and does not measure ingress,
  retries, settlement, or end-to-end worker latency.

Where an operation cannot be separated cleanly, notably complete lifecycle and
fan-out, the result is presented as end-to-end completion rather than settlement
latency.

## Five-sample baseline

Times are medians with the complete five-sample range in parentheses. Rates are
custom metrics from the same samples. Snapshot allocation columns also show
median and range.

### Ingress and complete command lifecycles

| Workload | Median | Five-sample range |
|---|---:|---:|
| execution ingress, poll-only | 4.167 ms/op | 4.060-4.341 ms/op |
| execution ingress, notification mode | 4.261 ms/op | 4.083-4.320 ms/op |
| independent lifecycle, 1 producer | 162.2 commands/s | 161.2-165.5 commands/s |
| independent lifecycle, 4 producers | 176.1 commands/s | 174.4-180.4 commands/s |
| independent lifecycle, 16 producers | 170.2 commands/s | 167.2-173.7 commands/s |
| same execution, 10 commands | 78.08 ms/op; 128.1 commands/s | 74.22-83.37 ms/op; 120.0-134.7 commands/s |
| same execution, 100 commands | 671.0 ms/op; 149.0 commands/s | 664.7-673.1 ms/op; 148.6-150.4 commands/s |

The one-shot 1,000-command stress workload completed in 8.553 seconds at 116.9
commands/s. Because it is intentionally one-shot, it is not presented as a
five-sample median: its purpose is to expose scale behavior without making the
short benchmark command spend minutes on every calibration cycle.

### Isolated staged-decision settlement

| Children | Events | Median | Five-sample range |
|---:|---:|---:|---:|
| 1 | 0 | 4.672 ms/op | 4.450-4.719 ms/op |
| 1 | 10 | 5.437 ms/op | 5.271-5.524 ms/op |
| 1 | 100 | 14.905 ms/op | 14.370-15.114 ms/op |
| 10 | 0 | 8.966 ms/op | 8.701-9.121 ms/op |
| 10 | 10 | 10.174 ms/op | 9.682-10.502 ms/op |
| 10 | 100 | 21.363 ms/op | 20.592-22.113 ms/op |
| 100 | 0 | 59.042 ms/op | 56.755-60.752 ms/op |
| 100 | 10 | 55.329 ms/op | 54.875-57.152 ms/op |
| 100 | 100 | 68.531 ms/op | 65.941-71.072 ms/op |

The 100-child/10-event median being slightly below the 100-child/0-event median
is within the observed run-to-run spread and is not interpreted as an event
performance benefit.

### External event ingress

| Target shape | Match shape | Median | Five-sample range |
|---|---|---:|---:|
| distinct small live executions | no wait | 3.650 ms/event | 3.460-3.825 ms/event |
| one small hot execution | no wait | 3.465 ms/event | 3.389-3.476 ms/event |
| one execution, 100 retained commands | no wait | 3.823 ms/event | 3.793-3.968 ms/event |
| 100 retained commands | one command per event | 4.302 ms/event; 232.4 events/s | 4.127-4.363 ms/event; 229.2-242.3 events/s |
| 100 retained commands | nine commands per event | 6.885 ms/event; 145.2 events/s | 6.492-7.026 ms/event; 142.3-154.0 events/s |

All event identities are distinct, so these results measure accepted ingress,
not idempotent rediscovery.

### Event-input snapshot claim materialization

| Inputs and canonical payload | Median time | Time range | Median B/op | B/op range | Median allocs/op | allocs/op range |
|---|---:|---:|---:|---:|---:|---:|
| 1 x 1 KiB | 4.108 ms | 4.065-4.271 ms | 66,947 | 66,617-67,755 | 1,208 | 1,207-1,208 |
| 32 x 1 KiB | 5.457 ms | 5.290-5.570 ms | 687,873 | 685,147-691,331 | 4,074 | 4,073-4,075 |
| 256 x 1 KiB | 13.820 ms | 13.773-14.061 ms | 4,989,091 | 4,989,063-4,989,141 | 24,708 | 24,707-24,708 |
| 256 x 64 KiB | 380.581 ms | 354.008-407.058 ms | 389,806,639 | 389,773,943-389,817,132 | 31,161 | 31,147-31,167 |

The maximum shape materializes 16 MiB of canonical event payload into one
immutable worker snapshot. Its approximately 390 MiB allocation is the primary
Phase 6 regression anchor.

## Retained journal cost

`TestJournalGrowthMeasurement100Commands` passed and reported:

| Metric | Result |
|---|---:|
| journal rows | 402 |
| journal tuple bytes | 199,620-199,639 |
| journal body bytes | 124,068-124,087 |
| journal tuple bytes per command | 1,996.2-1,996.4 |

The small byte range comes from two successful verification runs. Timestamp
text in canonical journal bodies can differ slightly in encoded length, while
the row count and semantic shape remain fixed.

This is retained storage cost, not only a hot-path timing metric. Later Plan 5
phases must preserve the semantic journal and report whether throughput gains
change this retention baseline.

## Phase 1 command-key index evidence

The baseline migration remains editable for this phase. The repository has no
release tags, the controlling implementation plan explicitly describes the API
and durable formats as pre-release, and `001_initial.sql` has been revised
throughout the current hardening line. The Plan 5 released-migration STOP
condition therefore did not require a forward migration or compatibility
design.

The Phase 1 schema test creates one completed 100-command execution plus 900
unrelated one-command executions, analyzes the command and queue tables, and
captures `EXPLAIN (ANALYZE, BUFFERS)` for the three planned query shapes. It
then installs the former `INCLUDE` shape only inside the isolated test schema
and repeats the same plans for comparison:

```text
env GOCACHE=/tmp/go-llm-go-build make test \
  TEST=TestSchemaCommandKeyQueryPlans \
  TEST_FLAGS='-count=1 -p 1 -parallel 4 -v'
```

| Query shape | Narrow command access | Former `INCLUDE` command access | Narrow / former execution time |
|---|---|---|---:|
| execution scan ordered by command key | bitmap index + heap scan, then sort | bitmap index + heap scan, then sort | 0.105 / 0.087 ms |
| child-key conflict lookup with `ANY` | index-only scan; 3 heap fetches | index-only scan; 3 heap fetches | 0.036 / 0.019 ms |
| trace command scan and queue join | bitmap index + heap scan feeding hash join | bitmap index + heap scan feeding hash join | 0.198 / 0.175 ms |

In the recorded sample all three narrow plans used
`flow_commands_execution_key_uq`. Repeated verification sometimes selected the
separate `(execution_id, command_id)` ownership index for the two whole-execution
scans because it has the same leading execution key and an equivalent estimated
cost; the selective command-key lookup continued to use the command-key index.
Heap access for the execution and trace scans is expected because both select
projection data outside the uniqueness key. These are single structural samples
with different cache state, not throughput measurements; the small
execution-time differences are not treated as regressions or improvements. No
query needed a replacement covering index.

Across repeated runs of the same 1,000-command fixture, `pg_relation_size`
reported 65,536-81,920 bytes for the narrow index and 147,456 bytes for the
former `INCLUDE` form. The exact size is fixture- and insertion-order-specific,
but it confirms the expected reduction in retained index payload. Catalog
coverage also proves that the index is unique on exactly
`(execution_id, command_key)`, contains no `INCLUDE`, has no duplicate, and
retains the separate unique ownership key on `(execution_id, command_id)`.
Cross-execution mutation tests continue to exercise the parent, queue, wait,
journal, and root ownership foreign keys.

Removing `unsatisfied_waits` from every index makes a state-preserving wait
counter update eligible for a PostgreSQL HOT update when its heap page has
space. This is structural eligibility rather than a cumulative-statistics
assertion; updates that also change `state` can still require index maintenance
because `state` participates in the partial wait-deadline index predicate.

## Phase 2 journal and notification evidence

The Phase 2 working tree was measured from parent commit `2ddc834` with the
same machine, PostgreSQL durability settings, 12-connection test pool, and
16-worker lifecycle runtime as the baseline. The database password was supplied
through the existing test environment and was not printed or recorded.

```text
go test -run '^$' \
  -bench 'Benchmark(ExecutionIngressNotification|IndependentCommandLifecycle)$' \
  -benchmem -benchtime=3s -count=5 .
```

| Workload | Baseline median (range) | Phase 2 median (range) |
|---|---:|---:|
| execution ingress, poll-only | 4.167 ms (4.060-4.341) | 4.342 ms (4.057-4.468) |
| execution ingress, notification mode | 4.261 ms (4.083-4.320) | 4.257 ms (4.130-4.477) |
| independent lifecycle, 1 producer | 162.2 commands/s (161.2-165.5) | 160.5 commands/s (157.0-164.4) |
| independent lifecycle, 4 producers | 176.1 commands/s (174.4-180.4) | 176.0 commands/s (174.8-178.6) |
| independent lifecycle, 16 producers | 170.2 commands/s (167.2-173.7) | 169.6 commands/s (168.0-173.0) |

The samples overlap and show no material lifecycle-throughput change in this
phase. Poll-only ingress had a higher median but overlapping range; notification
ingress and all three complete-lifecycle rates remained within ordinary
repeated-sample variance. The Phase 2 benefit is therefore recorded as a
structural hot-path reduction and semantic notification correction, not as a
throughput multiplier. Later readiness and batching phases target the dominant
remaining lifecycle work.

Source scans and PostgreSQL tests establish the structural result:

- journal allocation has no next-position pre-read and reserves each batch with
  one allocator `UPDATE ... RETURNING`;
- successful settlement maps normalized staged events, child declarations, and
  the parent terminal event from the accepted journal rows before projection
  updates;
- generic journal append contains no `pg_notify` call;
- the only store-level `pg_notify` is behind the explicit transactional
  runnable-command helper; and
- commit/rollback, remote root and event wake, unrelated-event no-op, claim,
  terminal settlement, immediate retry, lease recovery, gap-free rollback, and
  same-decision staged-event/new-child position tests pass against PostgreSQL.

## Phase 3 incremental-readiness evidence

The Phase 3 working tree was measured from parent commit `1f4677c` on the same
machine, PostgreSQL 18.1 instance, durability settings, 12-connection test pool,
and 16-worker fan-out runtime as the baseline. The database password was
supplied through the existing test environment and was not printed or recorded.

```text
go test -run '^$' \
  -bench 'Benchmark(SameExecutionFanout|ExternalEventIngress)$' \
  -benchmem -benchtime=3s -count=5 .
```

| Workload | Baseline median (range) | Phase 3 median (range) |
|---|---:|---:|
| same execution, 10 commands | 78.08 ms; 128.1 commands/s (74.22-83.37 ms; 120.0-134.7 commands/s) | 76.65 ms; 130.5 commands/s (75.31-79.25 ms; 126.2-132.8 commands/s) |
| same execution, 100 commands | 671.0 ms; 149.0 commands/s (664.7-673.1 ms; 148.6-150.4 commands/s) | 673.6 ms; 148.5 commands/s (665.9-692.7 ms; 144.4-150.2 commands/s) |
| distinct small live executions, no wait | 3.650 ms/event (3.460-3.825) | 3.695 ms/event (3.508-3.834) |
| one small hot execution, no wait | 3.465 ms/event (3.389-3.476) | 3.569 ms/event (3.547-3.772) |
| 100 retained commands, no wait | 3.823 ms/event (3.793-3.968) | 3.656 ms/event (3.480-3.710) |
| 100 retained commands, one match per event | 4.302 ms/event; 232.4 events/s (4.127-4.363 ms; 229.2-242.3 events/s) | 4.025 ms/event; 248.4 events/s (3.933-4.095 ms; 244.2-254.3 events/s) |
| 100 retained commands, nine matches per event | 6.885 ms/event; 145.2 events/s (6.492-7.026 ms; 142.3-154.0 events/s) | 4.395 ms/event; 227.6 events/s (4.354-4.559 ms; 219.4-229.7 events/s) |

The no-match and same-execution ranges overlap ordinary machine variance. The
nine-command match shape improved materially because one accepted event now
updates only matching reverse-wait rows, groups decrements by affected command,
and inserts released queue rows in sets instead of scanning every command and
issuing per-wait projection writes. The one-match shape also improved in this
sample, while the 100-command lifecycle result stayed within overlapping
variance.

`TestSparseEventWaitUpdateUsesProductionReverseIndexQuery` adds 10,000 unrelated
unresolved wait rows plus one matching selector, analyzes the wait table, and
runs the production wait-update shape under `EXPLAIN (ANALYZE, BUFFERS)`. PostgreSQL used
`flow_command_event_waits_reverse_idx`; the index scan returned one row. Phase 3
verification also covered exact-deadline reconciliation, grouped multi-wait and
multi-command release, equivalent/conflicting duplicate ingress, runnable-only
queue insertion, fail-fast running survivors, and replay/trace equivalence.

## Phase 4 batched-decision evidence

The Phase 4 working tree was measured from parent commit `16f844d` on the same
machine, PostgreSQL 18.1 instance, durability settings, 12-connection test pool,
and 16-worker fan-out runtime as the baseline. Database credentials were
supplied through the existing test environment and were not printed or
recorded.

```text
go test -run '^$' \
  -bench 'Benchmark(SameExecutionFanout|StagedDecisionBatch)$' \
  -benchmem -benchtime=3s -count=5 .

go test -run '^$' -bench '^BenchmarkExecutionIngressNotification$' \
  -benchmem -benchtime=3s -count=5 .
```

The additional ingress run verifies that routing one-command root creation
through the prepared batch helper did not regress the ordinary start path:

| Workload | Baseline median (range) | Phase 4 median (range) |
|---|---:|---:|
| execution ingress, poll-only | 4.167 ms (4.060-4.341) | 3.978 ms (3.881-4.074) |
| execution ingress, notification mode | 4.261 ms (4.083-4.320) | 4.036 ms (4.004-4.089) |

### Same-execution completion

| Workload | Baseline median (range) | Phase 3 median (range) | Phase 4 median (range) |
|---|---:|---:|---:|
| 10 commands | 78.08 ms; 128.1 commands/s (74.22-83.37 ms; 120.0-134.7 commands/s) | 76.65 ms; 130.5 commands/s (75.31-79.25 ms; 126.2-132.8 commands/s) | 70.81 ms; 141.2 commands/s (69.01-70.92 ms; 141.0-144.9 commands/s) |
| 100 commands | 671.0 ms; 149.0 commands/s (664.7-673.1 ms; 148.6-150.4 commands/s) | 673.6 ms; 148.5 commands/s (665.9-692.7 ms; 144.4-150.2 commands/s) | 582.3 ms; 171.7 commands/s (576.1-586.8 ms; 170.4-173.6 commands/s) |

### Isolated staged-decision settlement

| Children | Events | Baseline median (range) | Phase 4 median (range) |
|---:|---:|---:|---:|
| 1 | 0 | 4.672 ms (4.450-4.719) | 4.488 ms (4.355-4.535) |
| 1 | 10 | 5.437 ms (5.271-5.524) | 4.963 ms (4.884-5.167) |
| 1 | 100 | 14.905 ms (14.370-15.114) | 9.289 ms (9.169-9.444) |
| 10 | 0 | 8.966 ms (8.701-9.121) | 6.598 ms (6.516-6.796) |
| 10 | 10 | 10.174 ms (9.682-10.502) | 7.528 ms (7.418-7.575) |
| 10 | 100 | 21.363 ms (20.592-22.113) | 11.634 ms (11.230-11.839) |
| 100 | 0 | 59.042 ms (56.755-60.752) | 23.107 ms (22.890-23.229) |
| 100 | 10 | 55.329 ms (54.875-57.152) | 24.818 ms (24.533-25.031) |
| 100 | 100 | 68.531 ms (65.941-71.072) | 29.532 ms (28.101-30.144) |

The improvement grows with decision size: the 100-child shapes settle in
approximately 23-30 ms instead of 55-69 ms. End-to-end 100-command completion
also improved by about 13% in these samples. These remain development-machine
measurements rather than timing contracts.

The structural query shape is now bounded by store operation rather than item
count:

- all normalized staged-event identities are compared with one `unnest` query;
- all deduplicated child-wait identities use one retained-event position query;
- child command, wait, and initially ready queue projections use one `CopyFrom`
  operation per table, with exact affected-row checks;
- fail-fast survivor children use one position-aware command update and one
  queue delete; and
- whole-execution cancellation and expiry projection updates are likewise
  position-aware sets rather than one update per command.

PostgreSQL tests cover 100 no-wait children, a 100-child mixed batch with
retained/shared/distinct staged/missing waits, exact journal-to-command and
wait-to-event position mapping, final counters and queue shapes, `WithCommit`,
fail-fast survivor child cancellation, fault rollback/ambiguous commit seams,
caller-owned root insertion, and replay/trace equivalence.
