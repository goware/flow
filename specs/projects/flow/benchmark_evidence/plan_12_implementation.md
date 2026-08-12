---
status: implemented
base_commit: d9125dca7bf4894280a5a25f7bca7eb1735c46cd
implemented_at: 2026-08-12
---

# Plan 12 implementation evidence

Plan 12 adds a durable per-command recovery-lease override while retaining the
fixed 60-second fallback. The schema is a development reset: only
`001_initial.sql` changes, no compatibility migration or data conversion is
provided, and existing Flow schemas must be dropped and recreated.

## Result

- `WithRecoveryLease` validates once, rounds positive public values upward to
  whole milliseconds, and enforces the 30ms technical floor.
- The optional value is present in declaration identity, command rows,
  `command_created`, replay, root rediscovery identity, and staged-command
  equivalence.
- One same-run claim transaction resolves mixed durable/default durations,
  appends aligned `attempt_started` rows, and installs aligned queue expiries.
- One renewal statement accepts aligned command/attempt/token/duration arrays.
- The existing lease manager and watchdog use two shared earliest-deadline
  timers plus one registry-change channel. There is no per-attempt service,
  goroutine, timer, table, index, or maintenance category.
- Errors and uncertain renewals retry within the conservative local window.
  A retry can exceed the ordinary five-second cap but remains bounded by the
  shortest lease and remaining local window.
- The watchdog excludes a known in-flight renewal until its result is applied.
  PostgreSQL attempt ID and lease-token fencing remains the settlement
  authority; handler invocation remains at-least-once.

Trails API was audited only as a consumer: polling/status work may be a future
opt-in candidate, but Plan 12 does not alter Trails or infer safety from a
command or queue name.

## Focused behavior

Real PostgreSQL race tests cover:

- option normalization, duplicate/invalid values, declaration identity, root
  and staged-child persistence, journal bodies, and replay corruption;
- mixed short/default commands claimed and renewed together with exact durable
  durations and expiries;
- only-due renewal selection and registry wake-up by a newly registered short
  command;
- repeated healthy short-lease renewal;
- committed renewal awaiting local application without watchdog cancellation;
- pool-starvation cancellation and recovery;
- competing-replica takeover, stale-settlement rejection, and one durable
  terminal result; and
- existing rollback, locked sibling, ambiguous commit, shutdown, queue-slot,
  retry-budget, and maintenance coverage.

## Contemporaneous claim measurement

Environment:

- Linux amd64, Intel Core Ultra 7 255H;
- Go benchmark suffix `-16`;
- PostgreSQL 18.1 with `fsync=on`, `synchronous_commit=on`, and
  `full_page_writes=on`;
- five samples, one second each; and
- the existing 16-command `BenchmarkSameRunClaimBatch` fixture.

Command:

```sh
go test -run '^$' -bench '^BenchmarkSameRunClaimBatch$' -benchtime=1s -count=5 ./
```

| Revision | Median latency | Range | Median rate | Median bytes/op | Median allocs/op |
|---|---:|---:|---:|---:|---:|
| base `d9125dc` | 5.789 ms | 5.663-5.980 ms | 2,764 commands/s | 590,408 | 13,873 |
| Plan 12 worktree | 5.863 ms | 5.699-5.943 ms | 2,729 commands/s | 597,082 | 13,911 |

The measured claim change is approximately +1.3% latency / -1.3% rate, within
the plan's 10% investigation gate. The small allocation increase is consistent
with carrying one nullable duration and one resolved expiry per command while
retaining the same query/transaction count.

## Structural audit

- schema tables: six;
- new indexes: zero;
- new migrations: zero;
- new runtime services: zero;
- per-command timers/goroutines: zero;
- mixed claim SQL: one run lock, one ordered command/queue lock-read, one event
  input read, one journal append, and set-oriented queue/command updates;
- mixed renewal SQL: one statement with aligned duration input; and
- maintenance lease recovery: unchanged.

## Verification

- PostgreSQL 18.1 ordinary and full race suites: pass;
- PostgreSQL 17.10 ordinary and full race suites: pass;
- both servers: `fsync=on`, `synchronous_commit=on`,
  `full_page_writes=on`;
- named-test audits: PostgreSQL 17.10 ran 470 named tests and PostgreSQL 18.1
  ran 471 after the final malformed-durable-lease regression was added; both
  had zero skips and zero failures;
- build, vet, gofmt, and diff checks: pass;
- schema/source audit: six tables, one baseline migration, no added index,
  one mixed renewal statement, and unchanged maintenance categories; and
- implementation self-review: complete with no unresolved Critical or
  Moderate finding. The final independent PR review remains pending, as
  recorded by the one unchecked Plan 12 punchlist item.
