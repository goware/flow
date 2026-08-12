---
status: implementation-verified
recorded_at: 2026-08-11
---

# Plans 9–10 implementation evidence

This record covers the combined v0.3 candidate implemented from `d977533` in
commit `9cbef5eac70a4fadb5230d9737f0ced351ab5ffc` on branch `plan-9-10`.
Plan 9 was reviewed as the logical intermediate API/schema checkpoint inside
that combined implementation and was not tagged separately. Publication and
the v0.3 tag remain human decisions after review of the pull request.

## Environment

- Linux/amd64, Intel Core Ultra 7 255H
- Go 1.26.5; the module directive remains Go 1.26.4
- PostgreSQL 17.10 and 18.1
- `fsync=on`, `synchronous_commit=on`, and `full_page_writes=on` on both majors
- schema version 4 with minimum reader/writer versions 2/2

Migration 004 is the only new migration. The historical migration files remain
byte-identical at these SHA-256 values:

- `001_initial.sql`: `2fe1bde746c99201693de22598bf816fb1b190cc0939f5a8abbf381d18aa7922`
- `002_live_keys.sql`: `f2bc0c65bd775079ee992c25297eab3759983c52a29b95091c173806b97f6d56`
- `003_release_read_paths.sql`: `1c32c4e1d084183a407b24fae9cf4ad8c69ae81dce316ad5170c40f81ab218b5`

## Verification

- PostgreSQL 17.10: bounded ordinary suite and full race suite passed.
- PostgreSQL 18.1: bounded ordinary suite and full race suite passed.
- The PostgreSQL 18 named-test audit ran 451 named tests with zero skips and
  zero failures.
- The complete replacement/transaction/RunKey concurrency selection passed
  five times under the race detector.
- `go mod verify`, `go mod tidy -diff`, `go build ./...`, `go vet ./...`,
  `gofmt`, and `git diff --check` passed.
- `govulncheck ./...` reported `No vulnerabilities found`.
- Catalog tests prove a clean install, populated migration-003 upgrade, six
  Flow tables, renamed columns/constraints/indexes, preserved rows and journal
  bytes, and deliberate old-reader rejection.
- Source/API guards find no old exported `Execution*`, public `Execute`,
  production `Lookup*`, action alias, `DeliverToLive`, or premature `flow.Call`.
  Historical migrations and released journal wire strings remain explicit
  exceptions.

## Adjacent performance comparison

The baseline at `d977533` and the candidate ran back-to-back on the same
PostgreSQL 18.1 server. The broader lifecycle/fan-out sweep used three
one-second samples; ingress was repeated with five three-second samples because
its first short sample was noisy. These are regression checks, not service
guarantees.

| Shape | Baseline median | Candidate median | Change |
|---|---:|---:|---:|
| ingress, polling | 5.000 ms | 5.025 ms | +0.5% |
| ingress, notification | 5.133 ms | 5.037 ms | -1.9% |
| independent lifecycle, 1 producer | 157.0 cmd/s | 151.5 cmd/s | -3.5% |
| independent lifecycle, 4 producers | 405.9 cmd/s | 404.4 cmd/s | -0.4% |
| independent lifecycle, 16 producers | 372.4 cmd/s | 360.8 cmd/s | -3.1% |
| one run, 10 commands | 88.81 ms | 87.14 ms | -1.9% |
| one run, 100 commands | 689.5 ms | 678.4 ms | -1.6% |

The retained one-shot claim measurement completed at about 2,900 commands/s
for one 16-command same-run claim transaction. `GetEventValue` remained an
in-memory lookup with no database query. No retained shape shows a material
regression.

## Disposable Trails proof

A disposable worktree of Trails API commit
`cecf852aa9c18bae4f59d543452f34f2d7fd7097` used a local module replacement
to exact Flow implementation commit
`9cbef5eac70a4fadb5230d9737f0ced351ab5ffc`. No Trails source was committed
or pushed.

The adaptation:

- used `CommandInfo.RunKey` in jobqueue instead of loading the owner run;
- used `ReplaceCurrentRun` for retry/admin replacement in the caller
  transaction;
- created and threaded one `TransactionClient`, then called
  `BeginApplicationWrites` before domain writes;
- retained exact generation-fenced `Event.Deliver` monitor paths and bounded
  anomaly repair; and
- added no `flow.Call` or compatibility root.

The complete `lib/jobqueue` and `lib/intentmachine` packages and the focused
intent/monitor/transaction worker selection passed under the race detector.
The new RunKey and atomic replacement tests also passed five consecutive race
runs.

## Conclusion

Plans 9 and 10 preserve Flow's command-only, six-table, fenced model while
making the public vocabulary and caller transaction rules clearer. Plan 11's
inline durable `Call` proposal remains unimplemented and separately
reviewable.
