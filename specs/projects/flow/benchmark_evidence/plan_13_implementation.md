---
status: implemented_pending_independent_review
recorded_at: 2026-08-12
---

# Plan 13 implementation evidence

This record covers Plan 13, developed on `plans-12-13` from the source
baseline `826367df55682f2cfdfd5c601fc03e5de81d8382`. That commit differs from
its parent only in planning documents, so the implemented source baseline is
also the tagged-master source at `7abfb8cd8b09be2b0b6811aaae4f8034fdd7a6d6`.
The phase commits are:

- `e76a9ad`: definition-bound reads and compact operation results;
- `ea964b2`: one failure rule, removed metadata/input projections, and the
  clean schema reset;
- `1e611ec`: Run vocabulary and public-surface cleanup;
- `e909c75`: batched queue statistics and the staged-event bound; and
- `921f37a`: bounded aggregate pruning;
- `4b5e3ff`: synchronized public and architecture documentation;
- `39d7cef`: performance, plan-shape, and lock regression gates; and
- `bf218d8`: final current-only durable cleanup and replay validation.

No release or tag was created. The target after acceptance is a breaking
v0.4.0 candidate. Plan 11 remains deferred, and Plan 12 must be amended to the
accepted Plan 13 commit before implementation begins.

## Baseline and environment

- Flow baseline: `826367df55682f2cfdfd5c601fc03e5de81d8382`, clean when
  implementation began.
- Trails API consumer baseline:
  `c240d5d97368d632066f86b7ce51377eb8b9e632`, branch
  `feat/intent-run-coordinator-2`, clean before and after the disposable proof.
- Absurd comparison baseline:
  `5b97022ac6d1523cda92470db3b6731909cae571`, clean `main`.
- Go: `go1.26.5 linux/amd64`.
- Primary local PostgreSQL: 18.1 Debian, with `fsync=on`,
  `synchronous_commit=on`, and `full_page_writes=on`.
- Clean migration checksum:
  `001_initial.sql` SHA-256
  `49e3e9d783783342d5ea512d98731a1d09e1857d65e2d8b0a8fdf9e0a86fe3c0`.

The operator explicitly authorized treating all Flow databases as disposable
development data. Plan 13 therefore supplies one current schema and no row-
preserving upgrade or compatibility decoder: drop the old Flow schema,
`Migrate`, and recreate work.

Production Trails source contains no use of `Optional`, `WithFailFast`, or
Flow run metadata. Its domain tables remain the searchable source of domain
attributes.

The retained pre-change performance record is `plans_9_10_release.md`. Plan
13 also uses the exact source baseline above for the adjacent measurements in
the final-verification section.

## Implemented simplification

Plan 13 retains Flow's command graph, typed definitions, queue lanes, event
gates, transaction join, live-key ownership/replacement, leases, retry,
cancellation, tracing, and replay. It removes behavior that did not serve the
primary consumer:

- optional-command and configurable fail-fast modes;
- run metadata and its GIN index;
- the duplicate run-input projection;
- five projection columns in total;
- legacy migration files and compatibility durable formats;
- execution-era public and current durable vocabulary;
- dead aliases and redundant public inspection types; and
- per-lane queue-stat round trips.

The clean catalog remains six tables. It adds no queue table, task façade,
workflow/action concept, or `flow.Call`.

## Disposable Trails API proof

A detached disposable worktree at the recorded Trails commit used:

```bash
go mod edit -replace=github.com/goware/flow=/home/peter/Dev/go/src/github.com/goware/flow
```

The adaptation touched 24 files including `go.mod`: 129 insertions and 113
deletions. Its final uncommitted patch, excluding only the local `go.mod`
replace directive, had SHA-256
`f5e414f7acc3c977194b49718de1f0ee787493eea18e08db7c8cb26cb20ac0d0`.
No adaptation was committed or pushed.

Every required consumer change was mechanical:

- replace `EnqueueResult.ID` with `RunID` and remove the obsolete nested
  `ReplaceRunResult.Run.Created` assertion;
- call `GetRun` only in test helpers that genuinely need a full snapshot;
- use `Command.GetCurrentRun` in statically typed production paths, retaining
  the top-level form only in the dynamic operator loop;
- pass command names, never queue names, to dynamic run inspection;
- rename `Run.Type/Key`, `TraceCommand.State`, `RunFilter.Type`,
  `ListLiveWork`, `ListHistoryByKeys`, and the active-page `Work` field to their
  current Run/command forms;
- replace the production queue-stat loop with one `GetQueueStats` call for the
  complete lane list; and
- batch multi-lane test polling while adapting singleton assertions to the new
  result map.

No deleted optional/fail-fast/metadata behavior, job façade, or compatibility
root was recreated in Trails.

After resetting the disposable Trails test database and installing its normal
application migrations, the complete package compile passed:

```bash
CONFIG=/tmp/trails-plan13-proof/etc/api.test.conf \
E2E_TEST_CONFIG=../etc/api.e2e-test.conf.sample \
go test -run '^$' ./...
```

The following PostgreSQL-backed Flow consumer selection passed under the race
detector with `-p 1 -parallel 1`:

- production batched queue inspection and its failure behavior;
- active-command and history inspection;
- intent progress projection;
- the intent-run happy path;
- atomic owner supersession and cancellation-versus-reconciliation;
- concurrent live-key start deduplication;
- multi-command fan-out/fan-in in one run; and
- event-gated transaction mine/confirm settlement.

This proves the retained Trails capabilities: command graphs, event delivery,
transactional settlement, owner replacement, retries, queue concurrency, and
operator visibility.

## Final verification

### Environment and commands

The final production source under measurement was
`bf218d867d977ebd7ec1af2a795c6d15c5af7c31`. The later closure commit changes
only this evidence/status and replaces a retired field name in a rejection
test with a generic unknown field; production code is unchanged. The retained
baseline and candidate ran from separate worktrees against the same PostgreSQL
18.1 server on the same host. Durability remained enabled: `fsync=on`,
`synchronous_commit=on`, and `full_page_writes=on`.

The exact retained command in each worktree was:

```bash
FLOW_TEST_DATABASE_URL='postgres://postgres@127.0.0.1:5432/flow_test?sslmode=disable' \
FLOW_TEST_DATABASE_PASSWORD='postgres' FLOW_TEST_ADMIN_DATABASE='postgres' \
go test -run '^$' \
  -bench '^(BenchmarkRunIngressNotification|BenchmarkIndependentCommandLifecycle|BenchmarkSameRunFanout|BenchmarkSameRunClaimBatch|BenchmarkStagedDecisionBatch|BenchmarkEventSnapshotMaterialization)$' \
  -benchmem -benchtime=3s -count=5 .
```

The baseline worktree was created with:

```bash
git worktree add --detach /tmp/flow-plan13-baseline \
  826367df55682f2cfdfd5c601fc03e5de81d8382
```

These measurements are local regression evidence, not service-level promises.
Absolute results include PostgreSQL commit latency, container/storage behavior,
and this desktop's current load.

### Retained performance

Lower is better for latency rows; higher is better for command-rate rows. Each
cell is the five-sample median followed by the sample range.

| Shape | Baseline | Plan 13 | Change |
|---|---:|---:|---:|
| Ingress, polling | 4.262 ms (4.204-4.395) | 4.427 ms (4.265-4.553) | +3.9% latency |
| Ingress, notifications | 4.354 ms (4.151-4.499) | 4.353 ms (4.316-4.506) | unchanged |
| Independent, 1 producer | 165.8 cmd/s (162.6-168.0) | 166.4 cmd/s (162.7-167.9) | +0.4% rate |
| Independent, 4 producers | 463.6 cmd/s (455.0-464.1) | 461.3 cmd/s (439.7-462.5) | -0.5% rate |
| Independent, 16 producers | 421.5 cmd/s (359.4-433.5) | 422.1 cmd/s (418.8-424.8) | +0.1% rate |
| Same-run fan-out, 10 commands | 71.24 ms / 140.4 cmd/s (69.17-73.05 ms) | 71.68 ms / 139.5 cmd/s (71.25-75.60 ms) | +0.6% latency |
| Same-run fan-out, 100 commands | 560.25 ms / 178.5 cmd/s (555.97-571.25 ms) | 575.30 ms / 173.8 cmd/s (562.82-589.24 ms) | +2.7% latency |
| One 16-command claim transaction | 6.203 ms / 2,580 cmd/s (6.127-6.298 ms) | 6.160 ms / 2,598 cmd/s (5.783-6.507 ms) | -0.7% latency |

The baseline's low 16-producer sample is retained rather than discarded. It
does not change the median conclusion, and the candidate samples are notably
tighter.

| Staged decision | Baseline median (range) | Plan 13 median (range) | Latency change |
|---|---:|---:|---:|
| 1 child / 0 events | 4.829 ms (4.760-5.176) | 4.779 ms (4.637-4.913) | -1.0% |
| 1 child / 10 events | 5.486 ms (5.402-5.514) | 5.366 ms (5.330-5.541) | -2.2% |
| 1 child / 100 events | 9.968 ms (9.637-10.012) | 10.118 ms (9.879-10.181) | +1.5% |
| 10 children / 0 events | 7.284 ms (7.031-7.631) | 7.222 ms (6.996-7.267) | -0.9% |
| 10 children / 10 events | 8.284 ms (8.043-8.492) | 8.071 ms (7.990-8.300) | -2.6% |
| 10 children / 100 events | 12.546 ms (12.395-12.664) | 12.474 ms (12.204-12.673) | -0.6% |
| 100 children / 0 events | 23.969 ms (23.804-24.693) | 23.963 ms (23.554-24.232) | unchanged |
| 100 children / 10 events | 26.558 ms (25.904-26.896) | 26.182 ms (25.962-27.102) | -1.4% |
| 100 children / 100 events | 30.622 ms (30.093-30.793) | 29.857 ms (29.623-30.248) | -2.5% |

| Snapshot materialization | Baseline median (range) | Plan 13 median (range) | Latency change |
|---|---:|---:|---:|
| 1 x 1 KiB | 4.212 ms (4.029-4.377) | 4.191 ms (3.928-4.545) | -0.5% |
| 32 x 1 KiB | 4.527 ms (4.445-4.555) | 4.523 ms (4.491-4.712) | unchanged |
| 256 x 1 KiB | 7.655 ms (7.399-7.843) | 7.660 ms (7.537-7.711) | unchanged |

The largest retained regression is 3.9%, safely below the 10% stop gate. No
retained path was removed from the comparison.

### Focused Plan 13 measurements

The evidence-only permanent-key baseline is versioned at
`plan_13_baseline_benchmark.go.txt`, SHA-256
`687e234651b26fba95a708ab935fcd9dc2b5f36e9c9e6a6e1e83a200a7bd0eee`.
It was copied unchanged into the detached baseline worktree, checked with
`gofmt -d` and `sha256sum`, and run with:

```bash
go test -run '^$' \
  -bench '^BenchmarkPlan13BaselinePermanentKeyRediscovery$' \
  -benchmem -benchtime=1s -count=5 .
```

The candidate focused command was:

```bash
go test -run '^$' \
  -bench '^(BenchmarkPermanentKeyRediscovery|BenchmarkQueueStats16|BenchmarkPruneTerminalRuns)$' \
  -benchmem -benchtime=1s -count=5 .
```

Results:

- permanent-key rediscovery was 3.283 ms median (3.150-3.432) at baseline and
  3.312 ms (3.149-3.344) after Plan 13: +0.9% latency, while median bytes fell
  from 37,759 to 30,324 and allocations from 1,001 to 846;
- one 16-lane `GetQueueStats` call was 108.2 us median (104.4-117.0) and one
  round trip, versus 859.7 us (834.6-895.5) and 16 round trips for the retained
  single-lane-call shape: 87.4% lower latency, about 7.9x faster;
- pruning 100 small terminal runs was 7.539 ms median (7.143-7.662); and
- pruning 1,000 small terminal runs was 34.711 ms median (34.150-35.061).

Prune fixture creation was outside the timer. The measured region was one
bounded `PruneTerminalRuns` transaction and each iteration verified exact run,
command, and journal counts.

### Query, lock, and concurrency audit

Production-query `EXPLAIN (ANALYZE, BUFFERS)` gates passed and prove:

- active-command and keyed-history reads use `flow_runs_key_lookup_idx`, while
  current-run lookup is shaped for the partial `flow_runs_live_key_uq` guard;
- batched queue statistics use `flow_command_queue_stats_idx`;
- the selective 10,003-row prune fixture uses `flow_runs_prune_idx`, applies
  the requested `Limit`, and changes exactly the bounded batch.

The final source audit confirms permanent-key rediscovery locks the run before
its root command and then reads canonical root args; current-run lookup uses no
row lock; queue statistics are read-only; pruning locks only selected terminal
runs with `FOR UPDATE SKIP LOCKED`; and journal, command, then run deletion
addresses the selected UUID array through retained ownership indexes in one
store-owned transaction, with queue/wait cleanup owned by command FKs.

This race selection passed three consecutive times on PostgreSQL 18:

```text
TestClaimBatchCompetingReplicasCreateOneFencePerCommand
TestRuntimeCapacityLeaseRenewalAndTakeover
TestReplaceCurrentRunRacingTerminalSettlementHasOneWinner
TestPruneTerminalRunsConcurrentWorkersDoNotDoubleCount
TestPruneTerminalRunsKeepsTraceCoherentAndUnrelatedRuntimeProgressing
TestPruneTerminalRunsValidatesBoundsAndUsesDeterministicSkipLockedBatches
TestGetQueueStatsDoesNotLockClaimableRows
```

It proves one durable fence, stale-attempt takeover behavior, atomic
replacement versus settlement, bounded/pruner-safe deletion, live-run safety,
unrelated runtime progress, and nonblocking queue reads.

### Build, database, and inventory gates

Final code gates passed:

- `gofmt`, `git diff --check`, `make build`, and `go vet ./...`;
- `go mod verify` and `go mod tidy -diff`;
- PostgreSQL 17.10 Alpine: exact ordinary suite and full `make test` race suite;
- PostgreSQL 18.1 Debian: exact ordinary suite and full `make test` race suite;
- durability settings `fsync`, `synchronous_commit`, and `full_page_writes`
  were `on` on both majors; and
- the PostgreSQL 18 machine-readable audit recorded 460 named runs, 460 named
  passes, zero named skips, zero named failures, and zero package failures.

The final catalog/source/API scan found:

- exactly one migration file and exactly six `CREATE TABLE` statements;
- no `flow_executions`, `execution_id`, `execution_key`, old execution journal
  kinds/classes, retired projection JSON tags, metadata index, migration 002-005,
  compatibility alias, or old journal decoder in current Go/SQL;
- no `flow.Call` or seventh table;
- the five intended projection columns and metadata GIN index absent;
- the pruning index present with its exact eligibility predicate; and
- public `Work` reserved for the attempt-local handler scope, while operational
  reads expose `ActiveCommandPage.Commands`.

The implementation diff from the source baseline spans 84 files, with 3,104
insertions and 2,133 deletions. It was reviewed phase by phase, then again as a
cold final source/schema/API/journal diff. That final pass corrected the last
old start-fingerprint label, internal live-work vocabulary, active-page field,
queue-stat index name, run-first rediscovery lock shape, and strict current-only
journal/replay validation before the complete gates above were rerun.

### Acceptance mapping

All 17 implementation acceptance criteria are satisfied:

1. both typed and dynamic current-run reads share one implementation;
2. both typed and top-level result reads share identical semantics;
3. run and command keys remain strings;
4. enqueue/replacement results are compact operation outcomes;
5. optional commands, configurable fail-fast, and run metadata are absent;
6. current journal/replay/fingerprints contain no retired compatibility fields;
7. root input has one durable copy and permanent-key comparison remains exact;
8. the specified public renames/removals have no aliases;
9. queue statistics use one set-oriented statement;
10. a decision accepts at most 256 distinct staged events atomically;
11. pruning is bounded and excludes permanent/nonterminal runs;
12. the reset baseline is one Run-named migration and six tables;
13. Plans 11 and 12 remain deferred;
14. the disposable Trails adaptation compiles and its focused race tests pass;
15. PostgreSQL 17/18, race, module, migration, replay, and concurrency gates pass;
16. every retained performance median is within the 10% gate; and
17. the final hunk review found no unplanned API, schema, journal, lock, or
    consumer behavior.

Plan 13 is implemented and ready for independent final review. It deliberately
remains untagged and unreleased. The independent-review and final-accepted-
commit punchlist items remain open, and Plan 12 has not been amended yet.
