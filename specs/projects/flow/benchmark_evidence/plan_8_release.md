---
status: implementation-verified
recorded_at: 2026-08-10
---

# Plan 8 release-hardening evidence

These measurements and checks validate the release-hardening implementation;
they are regression evidence, not a throughput or latency guarantee. The
candidate was an uncommitted `release-hardening` worktree derived from Plan 7's
merged `master` commit `2f4e3f650d452d5bb05d4ed32a85d18cb0bf82d3`.
The final reviewed commit and publication decision remain human-gated.

## Environment

- Linux/amd64, Intel Core Ultra 7 255H
- Go 1.26.5; the supported `go` directive remains 1.26.4
- PostgreSQL 17.10 and 18.1
- `fsync=on`, `synchronous_commit=on`, and `full_page_writes=on` on both
  database majors
- database passwords were supplied through the test environment and were not
  printed or stored in this file

The dependency-upgraded baseline used the same PostgreSQL 18.1 server and the
same worktree base before source or migration changes. Its only dependency
movement from the base commit was the final `go.mod`/`go.sum` state recorded by
SHA-256 `4d5e42d4ce3ae4dc7d1632bc5d3065a0430216e6928ca34014a81f09095c009d`
and `fa859df681a0852a939b73c3421f2062b75f0f656510428930b866b949dd27db`.

## Dependency and security result

- `github.com/goware/pgkit/v2`: v2.9.0 to v2.9.2
- `golang.org/x/sync`: v0.18.0 to v0.22.0
- `golang.org/x/text`: v0.31.0 to v0.40.0
- pgx v5.10.0 and google/uuid v1.6.0 were already current
- `go mod verify` and `go mod tidy -diff` passed
- official `govulncheck ./...` reported `No vulnerabilities found`; the
  planned-at GO-2026-5970 finding is no longer present

`go list -m -u -retracted all` still reports newer versions requested by
upstream modules for packages or test graphs Flow does not import (including
scany database adapters and pgkit's unused module edges). They are not direct
or selected requirements in Flow's tidy module file, so Plan 8 did not add
artificial requirements merely to override them.

## Five-sample write-path comparison

Both sides used this exact command, immediately adjacent on one PostgreSQL
18.1 server:

```text
go test -run '^$' -bench '^(BenchmarkExecutionIngressNotification|BenchmarkIndependentCommandLifecycle|BenchmarkSameExecutionFanout)(/.*)?$' -benchtime=3s -count=5
```

| Shape | Dependency-only baseline median (range) | Candidate median (range) | Median change |
|---|---:|---:|---:|
| ingress, poll only | 4.678 ms (4.589–4.705) | 4.593 ms (4.467–4.661) | -1.8% |
| ingress, notification | 4.652 ms (4.560–4.720) | 4.680 ms (4.575–4.724) | +0.6% |
| independent, 1 producer | 163.8 cmd/s (162.1–167.6) | 162.9 cmd/s (147.4–165.1) | -0.5% |
| independent, 4 producers | 425.1 cmd/s (416.3–425.5) | 424.8 cmd/s (420.9–428.5) | -0.1% |
| independent, 16 producers | 381.7 cmd/s (378.9–384.6) | 381.7 cmd/s (371.2–391.2) | 0.0% |
| same execution, 10 commands | 78.50 ms (77.60–80.61) | 80.07 ms (78.92–80.85) | +2.0% |
| same execution, 100 commands | 617.7 ms (605.7–640.0) | 636.1 ms (634.3–669.1) | +3.0% |

All medians remain inside Plan 8's approximately five-percent stop threshold.
The two fan-out shapes show the expected small write cost from the additive
execution-key and queue-depth indexes; no material ingress or independent-work
regression is hidden.

## Verification matrix

- PostgreSQL 17.10: exact ordinary suite and full race suite passed
- PostgreSQL 18.1: exact ordinary suite and full race suite passed
- exact production-query migration/index plan tests passed on both majors
- the focused claim, ambiguity, takeover, lease, renewal, maintenance,
  observer, and commit-panic set passed ten times under the race detector
- a PostgreSQL 18 named-test audit ran 331 named tests with zero skips
- `make build`, `go vet ./...`, `gofmt`, `git diff --check`, module integrity,
  tidy consistency, actionlint, and vulnerability gates passed

Migration 003 retains exactly six Flow tables. Migrations 001 and 002 remained
byte-for-byte unchanged at SHA-256
`2fe1bde746c99201693de22598bf816fb1b190cc0939f5a8abbf381d18aa7922`
and `f2bc0c65bd775079ee992c25297eab3759983c52a29b95091c173806b97f6d56`.

## Interpretation

Plan 8 closes bounded-read, user-callback isolation, migration validation,
inspection-plan, and release-process gaps without changing Flow's six-table
coordination model, attempt fencing, journal format, or at-least-once worker
contract. Publication, the release date, the annotated tag, and the GitHub
release are deliberately not part of this implementation evidence.

GitHub private vulnerability reporting was checked and was not enabled. No
approved private contact route exists, so `SECURITY.md` was deliberately
omitted instead of publishing a fabricated or public-only reporting address.
