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

## Phase 5 claim-throughput evidence

The Phase 5 working tree was measured from parent commit `23fcc85` on the same
host, PostgreSQL 18.1 instance, durability settings, 12-connection test pool,
and 16-worker runtime shape described above. Database credentials were supplied
through the existing test environment and were not printed or recorded.

```text
go test -run '^$' \
  -bench 'Benchmark(IndependentCommandLifecycle|SameExecutionFanout)$' \
  -benchmem -benchtime=3s -count=5 .
```

Times are medians with the complete five-sample range. The post-change command
completed successfully in 135.016 seconds.

| Workload | Earlier five-sample evidence | Phase 5 median (range) |
|---|---:|---:|
| independent lifecycle, 1 producer | 162.2 commands/s (161.2-165.5) | 158.7 commands/s (156.5-162.6); 403.2 ms/batch (393.6-408.9) |
| independent lifecycle, 4 producers | 176.1 commands/s (174.4-180.4) | 400.0 commands/s (387.3-409.0); 160.0 ms/batch (156.5-165.2) |
| independent lifecycle, 16 producers | 170.2 commands/s (167.2-173.7) | 359.1 commands/s (353.4-366.7); 178.2 ms/batch (174.5-181.1) |
| same execution, 10 commands | Phase 4: 70.81 ms; 141.2 commands/s (69.01-70.92; 141.0-144.9) | 89.54 ms; 111.7 commands/s (88.29-91.12; 109.8-113.3) |
| same execution, 100 commands | Phase 4: 582.3 ms; 171.7 commands/s (576.1-586.8; 170.4-173.6) | 680.5 ms; 147.0 commands/s (636.4-705.0; 141.8-157.1) |

Independent execution throughput now uses several claim transactions at once:
the four- and sixteen-producer shapes materially exceed the earlier samples,
while the deliberately serial one-producer shape remains within the earlier
range. The internal bound is eight claim transactions and leaves two
application-pool connections available when the pool is large enough; pools of
one or two use one claimer. No public tuning option was added.

Review also requested a contemporaneous parent comparison for all independent
producer shapes. The parent and revised Phase 5 working tree were therefore run
back-to-back against the same active PostgreSQL instance with five samples each:

```text
git worktree add --detach <temporary-directory> 23fcc85
go test -run '^$' -bench '^BenchmarkIndependentCommandLifecycle$' \
  -benchmem -benchtime=3s -count=5 .
```

The parent command completed in 74.551 seconds and the revised Phase 5 command
completed in 82.515 seconds.

| Workload | Parent `23fcc85` median (range) | Revised Phase 5 median (range) |
|---|---:|---:|
| independent lifecycle, 1 producer | 145.2 commands/s (144.7-150.1); 440.6 ms/batch (426.5-442.2) | 146.2 commands/s (115.9-151.9); 437.7 ms/batch (421.3-552.3) |
| independent lifecycle, 4 producers | 160.7 commands/s (156.0-163.0); 398.3 ms/batch (392.7-410.3) | 399.7 commands/s (378.4-404.4); 160.1 ms/batch (158.2-169.1) |
| independent lifecycle, 16 producers | 150.4 commands/s (148.3-154.5); 425.4 ms/batch (414.3-431.4) | 360.1 commands/s (355.8-360.7); 177.7 ms/batch (177.4-179.9) |

The direct parent comparison confirms the concurrency gain at four and sixteen
producers. The one-producer medians are effectively unchanged; one slower Phase
5 sample widens that range and is retained rather than discarded.

The historical Phase 4 fan-out ranges did not overlap the first Phase 5 run, so
the parent commit was rebuilt in a detached worktree and measured immediately
against the same active PostgreSQL instance rather than attributing the
difference to code without evidence:

```text
git worktree add --detach <temporary-directory> 23fcc85
go test -run '^$' -bench '^BenchmarkSameExecutionFanout$' \
  -benchmem -benchtime=3s -count=3 .
```

| Workload | Parent `23fcc85` current-environment median (range) | Phase 5 five-sample median (range) |
|---|---:|---:|
| same execution, 10 commands | 90.38 ms; 110.6 commands/s (89.86-96.14; 104.0-111.3) | 89.54 ms; 111.7 commands/s (88.29-91.12; 109.8-113.3) |
| same execution, 100 commands | 784.4 ms; 127.5 commands/s (783.1-785.4; 127.3-127.7) | 680.5 ms; 147.0 commands/s (636.4-705.0; 141.8-157.1) |

That direct comparison shows no 10-command regression and a material
100-command improvement in the current environment. The older Phase 4 timing
difference is therefore retained transparently as machine/database variance,
not hidden by changing the workload. The diagnostic parent run has three
samples and is not used to claim a precise multiplier.

Structural coverage proves the corresponding safety properties: independent
execution claims overlap behind deterministic barriers; one execution remains
one transaction; an execution-locked oldest candidate releases its reservation
and boundedly yields past at least four older siblings to later fair candidates;
an at-capacity named lane likewise yields past at least four older queued
executions to a runnable command in another lane; a strict continuation cursor
passes a stable prefix of 256 distinct locked executions without resetting to
the same bounded exclusion window. Continuation is capped at 257 probe/claim
rounds per scheduling turn, carries its tail cursor across turns, and alternates
a bounded head revisit; an earlier execution becoming available is therefore
claimed even while the fault-driven test keeps appending later locked work;
selected slots transfer exactly to returned workers; shutdown drains claim
goroutines before worker WaitGroup accounting can begin waiting, and a claim
whose commit completes concurrently with shutdown is still transferred once to
worker accounting and explicitly concluded. Prepared fence metadata survives a
real commit-call error, and a post-commit ambiguity resolver timeout retains the
possibly owned fence in worker accounting instead of dropping it. The
conservative false-ambiguity case is also covered end to end: a rolled-back
commit with an unavailable resolver transfers its prepared claim, rejects its
phantom settlement, releases accounting, and permits the later durable attempt
to settle exactly once. A held claim on a two-connection pool leaves maintenance
able to expire unrelated work.
Same-execution coverage claims 16
siblings with one contiguous `attempt_started` batch, one queue update set, one
command update set, and exact per-command causation positions. It also covers a
locked sibling, mixed versions and queues, zero and 256 event-input snapshots,
required elapsed-budget fail-fast with survivor/counter/journal-order assertions,
malformed durable policy and malformed event-input rollback, ambiguous multi-fence
ownership resolution, and two competing replicas without duplicate active fences.

## Phase 6 event-input snapshot evidence

The Phase 6 working tree was measured from parent commit `2be3c8e` on the same
host, PostgreSQL 18.1 instance, durability settings, and 12-connection test pool
described above. Database credentials were supplied through the existing test
environment and were not printed or recorded.

```text
go test -run '^$' \
  -bench 'Benchmark(EventSnapshotMaterialization|GetEventValueLookup)' \
  -benchmem -benchtime=3s -count=5 .
```

The final command completed successfully in 194.790 seconds. Times and allocation
figures are medians with complete five-sample ranges.

| Inputs and canonical payload | Baseline median | Phase 6 median | Phase 6 five-sample range |
|---|---:|---:|---:|
| 1 x 1 KiB | 4.108 ms; 66,947 B/op; 1,208 allocs/op | 3.862 ms; 55,902 B/op; 1,187 allocs/op | 3.722-4.144 ms; 55,705-55,978 B/op; 1,187-1,188 allocs/op |
| 32 x 1 KiB | 5.457 ms; 687,873 B/op; 4,074 allocs/op | 4.215 ms; 233,134 B/op; 2,373 allocs/op | 4.011-4.347 ms; 231,308-233,500 B/op; 2,373-2,374 allocs/op |
| 256 x 1 KiB | 13.820 ms; 4,989,091 B/op; 24,708 allocs/op | 6.864 ms; 1,508,570 B/op; 10,908 allocs/op | 6.715-6.937 ms; 1,507,708-1,509,167 B/op; 10,907-10,908 allocs/op |
| 256 x 64 KiB | 380.581 ms; 389,806,639 B/op; 31,161 allocs/op | 84.854 ms; 103,026,359 B/op; 12,536 allocs/op | 81.401-88.824 ms; 103,003,653-103,082,471 B/op; 12,530-12,537 allocs/op |

`BenchmarkGetEventValueLookup256` remained an in-memory typed lookup and measured
701.1 ns/op, 2,448 B/op, and 7 allocations/op at the median (636.4-747.8 ns/op;
bytes and allocation count were identical across all samples).

The adversarial 16 MiB snapshot now uses about 74% fewer allocated bytes and
78% less claim time than the baseline; its allocation count fell by about 60%.
The hot path hashes the retained body directly, performs one typed versioned
envelope decode, validates the nested payload's canonical form without producing
a second canonical payload copy, and transfers the decoded allocation into the
private attempt snapshot. Flow's journal append still canonicalizes and verifies
every body at the accepted write boundary, while replay deliberately reconstructs
canonical history for stronger corruption diagnostics. No payload table or
column was added.

## Phase 7 documentation synchronization

The active README, package documentation, overview, functional specification,
architecture, and schema/engine component designs now describe the implemented
composition and hot-path boundaries. They distinguish command lifecycle
boundaries from deterministic worker logic, one serialized execution from
independent shard executions, bounded fan-out chunks and hierarchical joins,
direct child data and stable external payload references, and short
`WithCommit`/caller-owned transactions.

The design descriptions also match the structural evidence above: delta
readiness uses reverse waits and `unsatisfied_waits`; notifications are limited
to newly immediate runnable work; normalized decisions and same-execution
claims persist in bounded sets; independent execution groups may claim within a
pool-aware bound; and journal integrity is divided across accepted write, hot
claim, and full replay diagnostics. These are implementation and usage
descriptions, not new hard limits, service-level objectives, or throughput
promises. Final release-gate and full before/after evidence remain separate.

## Final release verification

The final verification ran on 2026-08-07 from the working tree based on
`2ed40d18bb4dbeec650365f00272ef86c35edb2e`. The final documentation and
focused claim-benchmark changes were not committed by the verification agent.
Historical phase plans and their evidence were not edited.

The host remained the same Intel Core Ultra 7 255H Linux/amd64 machine with 16
logical CPUs and the same 12-connection benchmark pool and 16-worker runtime
shapes. The final host kernel was Linux 6.18.42 and Go was 1.26.5. The final
benchmark server was PostgreSQL 18.4 in the locally supplied Alpine container,
with `fsync=on`, `synchronous_commit=on`, `full_page_writes=on`, and
`max_connections=100`. The Phase 0 baseline used PostgreSQL 18.1 on the same
host. This patch-level/container difference is recorded rather than presenting
the final samples as a laboratory-identical PostgreSQL environment.

The repository does not declare an older PostgreSQL-major support floor or a
CI version matrix. To avoid inventing a broader compatibility promise, final
coverage used the locally supplied adjacent release matrix: PostgreSQL 17.10
as the oldest exercised major and PostgreSQL 18.4 as the newest. Both reported
all three durability settings above as enabled.

### Final commands and functional gates

Database URLs were supplied without embedding a password in any command or
evidence. The PostgreSQL containers used local trust authentication only for
these isolated release-verification databases.

```text
gofmt -w <all Go files changed since d2713d8>
git diff --check
make build
go vet ./...
FLOW_TEST_DATABASE_URL=<local PostgreSQL URL without credentials> go test -count=1 ./...
make test

FLOW_TEST_DATABASE_URL=<local PostgreSQL 17 URL without credentials> go test -count=1 ./...
FLOW_TEST_DATABASE_URL=<local PostgreSQL 17 URL without credentials> make test

FLOW_TEST_DATABASE_URL=<local PostgreSQL 18 URL without credentials> go test -count=1 ./...
FLOW_TEST_DATABASE_URL=<local PostgreSQL 18 URL without credentials> make test
```

The ordinary and complete race suites passed on PostgreSQL 17.10 and 18.4.
`make test` expanded to `go test -race -count=1 -p 1 -parallel 4 ./...`.
A JSON event audit of the ordinary PostgreSQL 18 suite counted 293 named test
runs and zero named test skips. Go also emitted five package-level skip events
for packages with no test files; those were not test skips.

The exact final benchmark and retained-journal commands were:

```text
go test -count=1 -run '^TestJournalGrowthMeasurement100Commands$' -v .

GOMAXPROCS=16 go test -run '^$' \
  -bench 'Benchmark(IndependentCommandLifecycle|SameExecutionFanout|StagedDecisionBatch|ExternalEventIngress|ExecutionIngressNotification|EventSnapshotMaterialization)' \
  -benchmem -benchtime=3s -count=5 .

GOMAXPROCS=16 go test -run '^$' \
  -bench '^BenchmarkSameExecutionClaimBatch$' \
  -benchmem -benchtime=3s -count=5 .
```

The complete multi-workload command passed in 899.474 seconds. The final
reproducibility-review claim command passed in 36.647 seconds. Every named
shape ran; the opt-in 1,000-command stress benchmark was not selected by these
commands and its historical one-shot result remains recorded above.

`BenchmarkSameExecutionClaimBatch` creates 16 no-wait, default-queue,
default-retry siblings under one execution. Parent execution, fixture
settlement, candidate probing, and reset of attempt journal rows,
queue/command eligibility projections, and the execution allocator to a
repeatable claim state are outside the timer. The timed region is exactly one
`ClaimCommands` call and verifies that all 16 attempts are returned with
contiguous positions.

The exact baseline source is retained as
[`plan_5_claim_baseline.go.txt`](plan_5_claim_baseline.go.txt), SHA-256
`431f81cde702eda366c456ce0064e220d87127059ca63f6a0ca3bf5e29f08883`.
It is stored with a `.go.txt` suffix so ordinary current-tree package discovery
does not compile a historical-only helper. From a clean repository root, the
following commands create the detached baseline, verify the applied bytes, run
five samples, and remove the temporary source/worktree. The database URL shown
contains no credential; the verification server used local trust
authentication.

```text
flow_plan5_repo="$(pwd)"
test ! -e /tmp/flow-plan5-claim-repro
git worktree add --detach /tmp/flow-plan5-claim-repro d2713d8
cp "$flow_plan5_repo/specs/projects/flow/benchmark_evidence/plan_5_claim_baseline.go.txt" \
  /tmp/flow-plan5-claim-repro/plan5_claim_benchmark_test.go
sha256sum /tmp/flow-plan5-claim-repro/plan5_claim_benchmark_test.go

cd /tmp/flow-plan5-claim-repro
FLOW_TEST_DATABASE_URL='postgres://postgres@127.0.0.1:55418/flow_test?sslmode=disable' \
  GOMAXPROCS=16 go test -run '^$' \
  -bench '^BenchmarkPlan5BaselineSameExecutionClaimBatch$' \
  -benchmem -benchtime=3s -count=5 .

cd "$flow_plan5_repo"
FLOW_TEST_DATABASE_URL='postgres://postgres@127.0.0.1:55418/flow_test?sslmode=disable' \
  GOMAXPROCS=16 go test -run '^$' \
  -bench '^BenchmarkSameExecutionClaimBatch$' \
  -benchmem -benchtime=3s -count=5 .

rm /tmp/flow-plan5-claim-repro/plan5_claim_benchmark_test.go
git worktree remove /tmp/flow-plan5-claim-repro
```

The reproducibility review executed these steps against a fresh `d2713d8`
worktree. The copied file checksum matched the versioned artifact, the baseline
command passed in 34.638 seconds, and a back-to-back current command passed in
36.647 seconds on the same PostgreSQL 18.4 server with `fsync=on`,
`synchronous_commit=on`, and `full_page_writes=on`.

The artifact and current benchmark use the same parent/child definitions,
16 default-queue/default-retry no-wait siblings, notifications disabled,
unlimited command ceiling, one fixture worker, 5 ms poll, and one-minute claim
lease. Both wait for the succeeded parent and 17 persisted commands, stop the
runtime, and probe the same 16 child candidates before timing. The timed region
in both contains only `ClaimCommands`; both require 16 contiguous attempts and
run the same verified journal/queue/command/allocator batch reset while the
timer is stopped. The resets deliberately leave command `updated_at`/`status_at`
and execution `updated_at` advanced; those timestamps are not claim-eligibility
inputs, and every timed claim overwrites them. Only the benchmark/helper names
differ so the baseline source can compile independently at `d2713d8`.

### Final before/after results

Medians and complete five-sample ranges follow. The before column is the
Phase 0 `d2713d8` evidence unless the row explicitly identifies the detached
same-environment claim comparison.

| Workload | Before median (range) | Final median (range) |
|---|---:|---:|
| execution ingress, poll-only | 4.167 ms (4.060-4.341) | 4.410 ms (4.248-4.506) |
| execution ingress, notification mode | 4.261 ms (4.083-4.320) | 4.367 ms (4.103-4.448) |
| independent lifecycle, 1 producer | 162.2 commands/s (161.2-165.5) | 172.4 commands/s (170.1-173.1); 371.2 ms/batch (369.7-376.1) |
| independent lifecycle, 4 producers | 176.1 commands/s (174.4-180.4) | 463.4 commands/s (457.1-465.6); 138.1 ms/batch (137.5-140.0) |
| independent lifecycle, 16 producers | 170.2 commands/s (167.2-173.7) | 417.6 commands/s (400.9-421.5); 153.2 ms/batch (151.8-159.6) |
| same execution, 10 commands | 78.08 ms; 128.1 commands/s (74.22-83.37; 120.0-134.7) | 69.63 ms; 143.6 commands/s (68.98-71.14; 140.6-145.0) |
| same execution, 100 commands | 671.0 ms; 149.0 commands/s (664.7-673.1; 148.6-150.4) | 571.5 ms; 175.0 commands/s (564.8-576.1; 173.6-177.1) |

The final execution-ingress medians are modestly higher than the original
PostgreSQL 18.1 baseline. Notification ranges overlap, and the poll-only range
overlaps the previously recorded Phase 2 range of 4.057-4.468 ms. Root ingress
received no later production change after the Phase 4 same-environment samples
showed 3.978/4.036 ms medians. The final difference is therefore recorded as
bounded host/PostgreSQL-container variance, not hidden and not claimed as an
ingress improvement. The targeted concurrent and same-execution workloads
improve materially.

#### Staged-decision settlement

| Children | Events | Before median (range) | Final median (range) |
|---:|---:|---:|---:|
| 1 | 0 | 4.672 ms (4.450-4.719) | 4.710 ms (4.689-4.899) |
| 1 | 10 | 5.437 ms (5.271-5.524) | 5.523 ms (5.357-5.772) |
| 1 | 100 | 14.905 ms (14.370-15.114) | 10.708 ms (10.454-10.740) |
| 10 | 0 | 8.966 ms (8.701-9.121) | 7.720 ms (7.303-7.946) |
| 10 | 10 | 10.174 ms (9.682-10.502) | 8.918 ms (8.669-9.179) |
| 10 | 100 | 21.363 ms (20.592-22.113) | 14.240 ms (13.871-14.805) |
| 100 | 0 | 59.042 ms (56.755-60.752) | 28.548 ms (28.384-29.910) |
| 100 | 10 | 55.329 ms (54.875-57.152) | 31.667 ms (31.261-31.855) |
| 100 | 100 | 68.531 ms (65.941-71.072) | 35.713 ms (34.904-36.057) |

The two smallest shapes remain within ordinary repeated-sample variance. The
gain grows with the batch: 100 children and 100 events settle in about 35.7 ms
instead of 68.5 ms without changing the payload or timed boundary.

#### External event ingress

| Target and match shape | Before median (range) | Final median (range) |
|---|---:|---:|
| distinct small live executions, no match | 3.650 ms/event (3.460-3.825) | 3.684 ms/event (3.657-3.924) |
| one small hot execution, no match | 3.465 ms/event (3.389-3.476) | 3.577 ms/event (3.476-3.670) |
| 100 retained commands, no match | 3.823 ms/event (3.793-3.968) | 3.696 ms/event (3.495-3.769) |
| 100 retained commands, one match/event | 4.302 ms/event; 232.4 events/s (4.127-4.363; 229.2-242.3) | 4.126 ms/event; 242.4 events/s (4.054-4.346; 230.1-246.7) |
| 100 retained commands, nine matches/event | 6.885 ms/event; 145.2 events/s (6.492-7.026; 142.3-154.0) | 4.750 ms/event; 210.5 events/s (4.692-4.936; 202.6-213.1) |

No-match costs remain bounded independently of the retained 100-command shape.
The several-match case retains the delta-readiness gain. The small-hot median is
higher than the old narrow range but effectively matches the same-environment
Phase 3 median of 3.569 ms. The final retained/matching directions and
structural index gate remain positive.

#### Same-execution claim batch

| Metric | `d2713d8` on final environment | Final working tree |
|---|---:|---:|
| latency for 16-command claim | 18.103 ms (17.198-18.213) | 6.368 ms (6.263-6.942) |
| claimed commands per second | 883.8 (878.5-930.3) | 2,513 (2,305-2,554) |
| allocated bytes/op | 704,864 (700,732-711,755) | 592,551 (589,810-596,642) |
| allocations/op | 16,458 (16,454-16,463) | 13,872 (13,870-13,879) |

The reproducible same-environment comparison shows about 65% lower claim
latency and roughly 2.8 times the command rate. It directly measures the set-oriented
attempt journal and projection update rather than relabeling end-to-end fan-out.

#### Event-input claim materialization

| Inputs and canonical payload | Before median | Final median | Final range |
|---|---:|---:|---:|
| 1 x 1 KiB | 4.108 ms; 66,947 B/op; 1,208 allocs/op | 4.162 ms; 55,882 B/op; 1,188 allocs/op | 4.117-4.401 ms; 55,655-56,975 B/op; 1,186-1,189 allocs/op |
| 32 x 1 KiB | 5.457 ms; 687,873 B/op; 4,074 allocs/op | 4.732 ms; 234,475 B/op; 2,374 allocs/op | 4.623-5.151 ms; 232,746-236,966 B/op; 2,373-2,374 allocs/op |
| 256 x 1 KiB | 13.820 ms; 4,989,091 B/op; 24,708 allocs/op | 7.867 ms; 1,511,259 B/op; 10,910 allocs/op | 7.682-7.990 ms; 1,510,610-1,512,730 B/op; 10,909-10,910 allocs/op |
| 256 x 64 KiB | 380.581 ms; 389,806,639 B/op; 31,161 allocs/op | 90.111 ms; 103,030,008 B/op; 12,534 allocs/op | 89.391-96.715 ms; 103,021,746-103,045,065 B/op; 12,531-12,537 allocs/op |

The maximum final runtime is above the Phase 6 median of 84.854 ms, but its
allocated bytes and allocation count are effectively identical to Phase 6.
Only `doc.go` changed between the snapshot implementation commit and the final
tree, while PostgreSQL moved from the local 18.1 installation to an 18.4
container and the host kernel changed. The timing difference is therefore
documented as environment variance. Relative to the planned-at baseline, the
adversarial claim still uses about 74% fewer allocated bytes and 76% less time,
with no durability or integrity relaxation.

#### Retained journal cost

| Metric | Before | Final |
|---|---:|---:|
| journal rows | 402 | 402 |
| journal tuple bytes | 199,620-199,639 | 199,613 |
| journal body bytes | 124,068-124,087 | 124,061 |
| journal tuple bytes per command | 1,996.2-1,996.4 | 1,996.1 |

The semantic journal shape and retained storage cost are unchanged within the
documented timestamp-encoding byte variance.

### Architecture, schema, and loop audit

The final required scans produced no matches for global readiness resolution,
the position pre-read, or a mutable command-key `INCLUDE` payload. The sole
production `pg_notify` occurrence is the statement inside
`SemanticTx.NotifyRunnableCommands`:

```text
rg -n 'resolveReadinessLocked|loadReadinessCommands' internal/store
rg -n 'nextJournalPosition' internal/store
rg -n 'INCLUDE .*state|unsatisfied_waits.*terminal_position' migrations
rg -n 'pg_notify' internal/store
```

`SemanticTx.Apply` validates first, reserves the complete batch with one
`UPDATE ... RETURNING`, and performs one journal `CopyFrom`; it contains no
notification. `NotifyRunnableCommands` shares one `notificationSent` owner
across continued semantic batches, so one store operation emits at most one
transactional hint. Source inspection classified all `Apply` call sites: starts,
event release, immediate child release, immediate retry, and immediate lease
recovery notify only when runnable at `DBNow`; claims, conclusions without
follow-up work, cancellation, expiry, unrelated events, and future scheduling
do not.

The migrations contain exactly the six planned `CREATE TABLE` statements, and
`TestMigrateAndCheckSchema` independently counted six catalog tables and 28
indexes on clean PostgreSQL 17 and 18 schemas. Catalog and adversarial tests
prove the narrow unique `(execution_id, command_key)` index, the separate
`(execution_id, command_id)` ownership key, root/parent/queue/wait/journal
same-execution foreign keys, command/event identity guards, unique creation,
terminal, execution-failing, and attempt-kind lifecycle entries, and positive
journal positions.

Every remaining loop in `internal/store/{ingress,commands,graph}.go` was
inspected. Staged identity and retained-event lookup execute once per batch;
command, wait, and queue insertion use one `CopyFrom` per table; matching wait
satisfaction, command counter updates, released queue insertion, fail-fast
cancellation, whole-execution command cancellation, and queue deletion are set
operations. Loops prepare, validate, encode, map journal positions, or scan a
single result set. The two deliberate exceptional full-aggregate paths still
lock active-attempt details per running command during explicit whole-execution
cancellation or expiry; they perform no per-item projection write and are not
ordinary event/child/wait persistence. No prohibited per-item retained-event
lookup, wait/child insert, wait update, cancellation update, or queue delete
remains.

The production diff contains no durability-setting statement or unlogged
table. The only changed public-package implementation declaration after
`d2713d8` substitutes internal journal-codec constants in `execute.go`; `doc.go`
adds guidance. No exported public declaration was added, removed, or changed.
Active-code and compile-contract scans show no coordinator, plan runtime,
state-machine, outcome subscription, or compatibility alias; active docs mention
those names only to state that they are unsupported or superseded.

### Acceptance-criterion audit

| # | Final evidence | Result |
|---:|---|:---:|
| 1 | The retained benchmark file covers independent lifecycles, 10/100/opt-in-1,000 fan-out, the complete staged matrix, five external-ingress shapes, the focused 16-command claim batch, and four snapshot shapes; the exact historical claim fixture is retained as a checksummed source artifact. | PASS |
| 2 | This file records commits, host/server/settings, pool/workers, exact apply/run/revert commands, artifact checksum, timer boundaries, payloads, five samples, limitations, and before/after results. | PASS |
| 3 | Migration source plus `TestMigrationPrunesAndNarrowsIndexes` prove exactly two key columns, uniqueness, no `INCLUDE`, and no duplicate. | PASS |
| 4 | `reserveJournal` is one allocator `UPDATE ... RETURNING`; source scan finds no `nextJournalPosition`; semantic rollback/gap tests pass. | PASS |
| 5 | `SemanticTx.Apply` has no notification and the sole `pg_notify` is in the explicit helper. | PASS |
| 6 | The shared notification owner caps one hint per operation; runnable/no-op/claim/terminal/retry/recovery/commit/rollback/reconnect tests pass. | PASS |
| 7 | Global resolver symbols are absent; no-event success bypasses readiness and event ingress calls the delta resolver only with accepted positions. | PASS |
| 8 | The reverse-index update filters unresolved exact matches and returns each newly satisfied command; duplicate/conflict/deadline tests pass. | PASS |
| 9 | Grouped counter update and one released-queue insert handle only affected commands; multi-wait/multi-command and sparse 10,000-wait index tests pass. | PASS |
| 10 | Fail-fast alone loads affected open commands; cancellation command/queue writes are batched with exact journal positions; survivor/disabled tests pass. | PASS |
| 11 | Staged identity lookup, retained child-wait lookup, and command/wait/queue writes are one bounded operation each; the manual loop audit found no prohibited per-item persistence. | PASS |
| 12 | Large mixed decision, conflict, ceiling, fault, caller-transaction, `WithCommit`, counter, replay, and trace tests pass atomically. | PASS |
| 13 | Bounded pool-aware independent claims and barrier-based overlap, fairness, small-pool, shutdown, ambiguity, and multi-replica tests pass under race. | PASS |
| 14 | Sixteen siblings use one journal batch and one queue/command update set; structural tests and the dedicated repeated benchmark prove the batch path. | PASS |
| 15 | Claims hash the stored body, decode once, validate/copy the canonical payload once, and retain corrupt/version/size/selector/retry/takeover/replay coverage. | PASS |
| 16 | Permanent/live identity, event identity, deadline, retry, fencing, fail-fast, cancellation, caller transaction, history, trace, replay, notification, and migration tests all pass in the full ordinary/race suites. | PASS |
| 17 | README, package docs, overview, functional spec, architecture, and schema/engine docs describe efficient command, execution, event, payload, join, and transaction granularity. | PASS |
| 18 | Clean migration inventory is exactly six tables; the public API diff is compatible; PostgreSQL 17/18 ordinary and race suites plus format/diff/build/vet pass with durability enabled. | PASS |

All 18 criteria are verified. The directional gains are evidence rather than
SLOs, and the bounded timing variance above is retained explicitly rather than
weakened into a timing assertion.
