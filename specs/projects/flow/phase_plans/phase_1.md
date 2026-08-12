---
status: complete
historical: true
superseded_by: ../plans/3-remove-coordinator.md
---

# Phase 1: Public Contracts and Deterministic Foundation

> Historical delivery record for the original milestone. Its plan and coordinator contracts are removed; the active command/worker/event contract is defined by `../plans/3-remove-coordinator.md` and the synchronized component specifications.

## Overview

Establish the compile-time developer surface and database-free primitives on which every durable phase depends. This phase deliberately performs no SQL and starts no goroutines. Definitions are immutable values, application data has stable canonical identity, retry decisions use supplied timestamps, observations are safe/no-op by default, and test fault points have a named internal contract.

## Steps

1. Create the root package layout and foundational public values: typed IDs, statuses/outcomes, execution snapshots, command metadata, safe sentinel/structured errors, `NoRetry`, and `RetryAfter`.
2. Implement RFC 8785 canonical JSON encoding, decoding, size/depth validation, SHA-256 values and equality helpers under `internal/canonical`, with independent conformance vectors.
3. Implement immutable `Command[A,R]`, `Event[T]`, `PlanDef[A]`, and `Coordinator[S]` descriptors, same-type `With(Client)` binding, erased codecs/metadata, command options, handler registrations, commit-function registration, coordinator handler registration, and duplicate/overlap validation.
4. Implement sealed immutable `RetryPolicy` data and builders plus the pure retry decision engine using supplied PostgreSQL-style timestamps, deterministic jitter, attempt/elapsed/deadline bounds, and non-consuming interruptions.
5. Add the no-op `Observer`/safe `Observation` contract and an internal named fault-hook mechanism that later phases extend at transaction boundaries.
6. Add the initial `flowtest` package for canonical-stability and pure retry-policy assertions, keeping production and test semantics on the same codecs and decision engine.
7. Document the initial package surface, format the module, and run unit tests, race tests, vet, and coverage before review.

## Tests

- `TestCanonicalRFC8785Vectors`: canonical key order, escaping, numeric formatting, invalid/duplicate JSON, non-finite values, and digest stability.
- `TestDefinitionIdentityAndBinding`: names/versions/codecs remain unchanged, `With` returns an independent same-type copy, and an unbound definition remains unmodified.
- `TestRegistrationValidation`: duplicate coordinator selectors, success/outcome overlap, invalid definitions/options, and multiple commit functions are rejected without global state.
- `TestRetryPolicyBuilders`: immutable copying, default backoff, validation, attempt-only, elapsed-only, and combined bounds.
- `TestRetryDecision`: permanent errors, explicit delays, deterministic jitter, execution-deadline caps, elapsed expiry, and non-consuming interruptions.
- `TestSafeErrorsAndObservations`: `errors.Is` categories work and safe records contain no payload or SQL fields.
- `TestFlowtestFoundation`: public helpers use the production canonical and retry implementations.
