---
status: complete
completed_at: 2026-08-05
---

# Phase 10: Cross-execution event delivery

## Overview

Implement [`../plans/4-cross-execution-delivery.md`](../plans/4-cross-execution-delivery.md) as a narrow extension of existing event ingress. `Event.Deliver` targets a known execution and deliberately bypasses only the active-attempt guard, while preserving all existing event identity, transaction, gate, and lifecycle behavior.

## Steps

1. In `execute.go`, move the existing external event-ingress implementation behind an unexported `Event.emitExternal` helper, keep the active-attempt guard in `Event.Emit`, and add `Event.Deliver(ctx, client, target, key, payload)` as a direct call to the helper.
2. In `deliver_test.go`, exercise delivery from an active producer worker into a separate exact-event-gated execution, including `GetEventValue`, source retry, idempotent re-delivery, independent commit, and preservation of the `Event.Emit` guard.
3. Exercise `runtime.InTx(tx)` delivery with application writes, proving through a separate connection that both records remain invisible and the target remains gated before commit, both become visible after commit, and rollback persists neither.
4. Cover target-local identity, payload conflicts, missing and terminal targets, `Emit`/`Deliver` gate parity, fresh stage-run identities, and multi-producer exact AND fan-in without adding another orchestration abstraction.
5. Update public API guidance and active Flow specifications to distinguish staged `flow.Emit`, external `Event.Emit`, and detached targeted `Event.Deliver`, without adding storage or scheduler concepts.
6. Run formatting, vet, PostgreSQL-backed tests, race tests, diff validation, and coordinator-removal scans.

## Tests

- `TestDeliverFromActiveWorker`: an active producer worker delivers to a separate gated execution, the target gets the payload, a failed first source attempt does not retract it, retry delivery is idempotent, and ordinary `Event.Emit` remains rejected in attempt context.
- `TestDeliverInCallerTransaction`: delivery and an application row remain invisible until commit and become visible together; rollback leaves the target gated and persists neither record.
- `TestDeliverIdentityLifecycleAndGateParity`: delivery preserves exact target-local identity and conflict rules, rejects missing and terminal targets, releases the same gate as `Emit`, and a repeated live-key stage uses a fresh execution ID.
- `TestDeliverMultiProducerFanIn`: three independent producer executions deliver exact events that satisfy one command-owned AND join, whose worker gets every payload.
- Full `go test ./...`, `go test -race ./...`, and `go vet ./...` regression suites against PostgreSQL.
