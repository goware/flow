# Plan 14 scheduler latency and round-trip evidence

Status: Complete

Measured on 2026-08-12. The baseline was checked out at `ea0d62b`; the after
worktree was `refactor/scheduler-latency` at `effb66c` plus the uncommitted Plan
14 implementation. All before/after throughput and latency samples used the
same PostgreSQL 18 server and host.

## Environment

- Go: `go1.26.5 linux/amd64`
- CPU: Intel Core Ultra 7 255H, 16 online cores
- PostgreSQL benchmark server: 18.1, x86_64, `fsync=on`,
  `synchronous_commit=on`, `full_page_writes=on`
- Application pool: 12 connections unless the benchmark explicitly overrides
  runtime worker concurrency
- Cross-version verification: PostgreSQL 17.10 and 18.1, both with the three
  durability settings above enabled

## Five-sample throughput and allocation matrix

The tables show complete sample ranges and the median. Rates are commands/s;
latencies are ms/op.

| Shape | Baseline range; median | After range; median | Baseline bytes/alloc median | After bytes/alloc median |
|---|---:|---:|---:|---:|
| Independent, 1 producer | 150.3–155.2; 154.7 | 149.1–154.8; 150.2 | 18,438,408 / 333,622 | 18,715,928 / 329,639 |
| Independent, 4 producers | 397.6–416.4; 406.2 | 286.6–417.0; 411.9 | 19,510,860 / 329,695 | 19,833,754 / 325,718 |
| Independent, 16 producers | 351.0–382.9; 369.7 | 379.7–383.7; 380.9 | 19,738,932 / 331,440 | 19,965,184 / 328,867 |
| Same-run fan-out, 10 commands | 91.12–105.8; 97.07 | 102.1–114.2; 104.9 | 2,441,924 / 44,143 | 2,339,100 / 43,911 |
| Same-run fan-out, 100 commands | 127.8–141.9; 135.3 | 137.4–152.6; 145.6 | 24,083,372 / 451,119 | 23,613,984 / 470,604 |
| Same-run claim, 16 commands | 2,046–2,307; 2,235 | 2,037–2,298; 2,080 | 603,563 / 13,912 | 598,985 / 13,893 |

The isolated claim median moved -6.9%, within the plan's 10% investigation
gate, while eliminating protocol operations. One four-producer sample was a
286.6 commands/s host outlier; the median remained +1.4%. The 100-command
fan-out allocation count rose 4.3%, while its median completion rate improved
7.6%; all other retained Phase 5 hot shapes reduced allocations.

## Phase 5 isolated allocation retention

Each Phase 5 item was also measured independently in the same PostgreSQL 18.1
environment. `BenchmarkPlan14RetainedAllocationChanges` ran five 10,000-iteration
samples with database/runtime setup outside the timed region. The paired
sub-benchmarks reproduce the removed operation and the retained operation in
the same binary. The single-group comparison isolates the former channel,
goroutine, and wait-group dispatch while keeping pool-capacity caching in both
variants; the UUID comparison sorts the same 16 identifiers; and the observer
comparison uses the same representative observation construction.

| Retained item | Before bytes / allocs | After bytes / allocs | Median before → after |
|---|---:|---:|---:|
| Pool capacity: `Config()` copy → cached integer | 912 / 4 | 0 / 0 | 379.5 → 1.778 ns/op |
| Replica: construct from UUID → cached string | 96 / 2 | 0 / 0 | 86.48 → 0.3948 ns/op |
| One run group: worker dispatch → direct call | 304 / 4 | 80 / 1 | 911.9 → 53.92 ns/op |
| UUID sort: string comparison → byte comparison | 3,256 / 69 | 88 / 3 | 2,915 → 483.8 ns/op |
| Default observer: adapter enqueue → nil guard | 48 / 1 | 0 / 0 | 122.5 → 1.016 ns/op |

All five items independently reduced allocations, so each remains retained.
The full throughput matrix above is the material-regression guard for their
combined production shape.

## Latency shapes

Fixture creation and reset were outside the timed regions.

| Shape | Baseline range; median | After range; median | Direction |
|---|---:|---:|---:|
| 150 ms scheduled command, 2 s poll | 2,261–2,266; 2,262 ms | 170.3–174.1; 171.8 ms | 92.4% lower |
| Same-runtime `AwaitRun`, 250 ms fallback | 252.1–253.7; 252.7 ms | 26.68–28.63; 28.54 ms | 88.7% lower |
| Timer-only `AwaitRun`, 250 ms fallback | n/a | 251.6–252.6; 251.7 ms | fallback retained |

Scheduled-command allocation medians moved from 248,656 bytes / 3,677 allocs
to 147,953 bytes / 2,835 allocs. Same-runtime Await moved from 164,526 bytes /
2,935 allocs to 134,216 bytes / 2,704 allocs. Timer-only Await recorded 486,070
bytes / 6,623 allocs at its median sample; this shape intentionally includes
the independently running remote worker.

## Protocol-operation census

`plan14ProtocolTracer` implements pgx query, batch, and CopyFrom tracing. The
test resets the tracer immediately before claim and settlement and repeats each
region three times; every sample was identical.

| Region | `ea0d62b` | After | Net protocol reduction |
|---|---:|---:|---:|
| One-command claim | 10 query, 0 batch, 1 CopyFrom | 6 query, 1 batch, 1 CopyFrom | 3 operations |
| Simple-success settlement | 11 query, 0 batch, 1 CopyFrom | 6 query, 1 batch, 1 CopyFrom | 4 operations |

The reductions come from the one-statement lock/time/initial snapshot and one
batch for each adjacent projection-write pair. CopyFrom, journal application,
fault hooks, callbacks, notifications, and commit remain at their prior
boundaries.

## Phase evidence

- The command probe captures PostgreSQL time once and returns due candidates
  plus a global future duration in one result set. Cursor, run, and lane
  exclusions apply only to due candidates. Scheduler sleep remains capped by
  `pollInterval` and is interruptible by the existing wake hub. A regression
  waits for a completed future-work probe, inserts earlier work, and proves the
  enqueue wake interrupts that calculated sleep.
- `AwaitRun` snapshots the local generation before every durable read and uses
  the existing 250 ms-capped timer as its correctness fallback. Remote and
  notification-disabled completion remains timer-observed. The terminal-run
  regression observes exactly one query, and the cancellation regression
  proves all 16 concurrent waiter goroutines exit.
- `AttachSemantic` uses a `MATERIALIZED` locking CTE and evaluates
  `clock_timestamp()` only in the outer query. The contended-lock regression
  proves the second transaction's `DBNow` follows lock acquisition; skip-locked
  behavior is unchanged.
- Claim and settlement use the explicitly named initial locked snapshot only
  before projection mutation. Later `LoadRunHead` reads remain live reads.
- Run expiry locks open commands first, then locks all running queue rows in one
  ordered query and validates one complete attempt/journal fence per running
  command. Zero- and one-running cases, 100 running commands, 100 mixed
  running/waiting commands, missing queue and journal fences, rollback after
  the bulk read, and concurrent settlement/expiry all have dedicated
  regressions; every nonempty case observes one bulk delivery query and no
  per-command delivery read.
- Runtime construction caches pool capacity and the replica name; a single run
  group claims directly; UUID ordering compares bytes; and the default observer
  creates no adapter. Profiled claim/attempt sites guard observation
  construction when the adapter is absent. The isolated table above records
  the before/after allocation result for every retained item.

## Conditional decisions

### Maintenance horizons and cross-run fan-out — DEFER

No post-Phase-4 benchmark or regression demonstrated a remaining maintenance
transition bottleneck at the plan's 10% gate. Separate horizons plus bounded
cross-run concurrency would put category-local drain behavior and pool
headroom at risk without measured benefit, so no maintenance code changed.

### Scheduler probe/query/index redesign — DEFER

The required duration horizon reduced the target delayed-command median by
92.4%, and the existing bounded fairness/cursor suite remains green. A
capacity-aware/global rewrite or overlapping claim index would increase
fairness and write-amplification risk without a demonstrated remaining target
bottleneck, so the existing lateral due-candidate query and claim index remain.

### Wait-expiry baseline predicate — DEFER

The exact production predicate was tested on PostgreSQL 17.10 and 18.1 with
200,000 rows, 100,000 pending unresolved waits, pseudo-random deadline order,
and a vacuumed visibility map. Both old and candidate indexes were 5,898,240
bytes. The old predicate produced an Index Scan with 131 buffers and 0.289 ms
(PG17) / 0.360 ms (PG18). Adding `unsatisfied_waits > 0` produced an Index Only
Scan with 4 buffers, zero heap fetches, and 0.031 ms (PG17) / 0.044 ms (PG18).
These were preliminary single samples: the experiment did not record the
required five same-environment executions per version and index shape, and the
saved plans omitted `Actual Rows`. It therefore cannot support `ADOPT` under
the Plan 14 gate despite the promising buffer and latency direction. `DEFER`
until a separate experiment records all five samples, complete ranges, and
actual rows before review. The consolidated baseline index remains unchanged.

## Final gates

- PostgreSQL 18.1: build, vet, format/diff check, ordinary `./...`, and full
  race `./...` passed with zero named skips and zero failures.
- PostgreSQL 17.10: ordinary `./...` and full race `./...` passed with zero
  named skips and zero failures.
- The original PostgreSQL 18 container was restored after the isolated
  PostgreSQL 17 gate.
