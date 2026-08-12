---
status: implementation
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
- `921f37a`: bounded aggregate pruning.

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
  `50c3feb820ddd63417d7de97a8f5f46689a0381de4d78ed278512ce05fbf7a3a`.

The operator explicitly authorized treating all Flow databases as disposable
development data. Plan 13 therefore supplies one current schema and no row-
preserving upgrade or compatibility decoder: drop the old Flow schema,
`Migrate`, and recreate work.

Production Trails source contains no use of `Optional`, `WithFailFast`, or
Flow run metadata. Its domain tables remain the searchable source of domain
attributes.

The retained pre-change performance record is
`plans_9_10_release.md`. Final adjacent Plan 13 measurements and PostgreSQL
17/18 acceptance results are appended below during Phase 8.

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

The adaptation touched 24 files including `go.mod`: 107 insertions and 89
deletions. Its final uncommitted patch SHA-256 was
`9e15defb6dc61d31cf50ae2467639c4f92014d0b80882bfc4b7f9e9c6d4d643e`.
No adaptation was committed or pushed.

Every required consumer change was mechanical:

- replace `EnqueueResult.ID` with `RunID` and remove the obsolete nested
  `ReplaceRunResult.Run.Created` assertion;
- call `GetRun` only in test helpers that genuinely need a full snapshot;
- use `Command.GetCurrentRun` in statically typed production paths, retaining
  the top-level form only in the dynamic operator loop;
- pass command names, not queue names, to dynamic run inspection;
- rename `Run.Type/Key`, `TraceCommand.State`, `RunFilter.Type`,
  `ListLiveWork`, and `ListHistoryByKeys` to their current Run/command forms;
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

Pending Phase 8: adjacent performance measurements, query plans, concurrency
stress, PostgreSQL 17/18 ordinary and race suites, named-test audit, complete
source/API scans, and final hunk review.
