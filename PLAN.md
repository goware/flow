# Schema and durable-type hardening plan

This is the authoritative staged plan for pruning Flow's pre-release PostgreSQL schema and aligning its durable PostgreSQL types with the public Go model. It records decisions; it does not authorize later stages implicitly. Each unchecked stage should be implemented and reviewed as a focused change.

## Confirmed decisions

1. Keep public and runtime counts and versions as Go `int`. Validate their range before every write to a PostgreSQL `integer`; do not expose database-sized integer types through the public API solely to mirror PostgreSQL.
2. Durable configuration durations have whole-millisecond precision. Reject positive durations that are not exact multiples of one millisecond at the public validation boundary. Do not silently truncate them with `time.Duration.Milliseconds()`, and do not add finer-grained storage until a demonstrated use case requires it.
3. Represent finite public state vocabularies with typed string aliases and constants. Keep PostgreSQL state columns as `text` with named `CHECK` constraints rather than PostgreSQL enums, so vocabulary changes remain ordinary migrations.
4. Use one shared structured failure value for durable/public failure projections. Preserve distinct columns when they represent distinct semantics rather than forcing all error history into one field.
5. Store the retry policy as opaque canonical `bytea`. Its JSON encoding is a Go/library contract, not a database query surface.
6. Harden command ownership and position invariants in the database, including a non-null root command and same-execution references.

## Scope and migration policy

Flow is still using a pre-release baseline. Until a schema version is released, pruning and hardening may rewrite `migrations/001_initial.sql` and all matching SQL call sites. Once users can have the affected schema in durable environments, stop rewriting an applied migration: add a forward migration, bump compatibility metadata where required, and test upgrade as well as clean install.

The semantic journal and mutable projections have different jobs. A projection hash is not retained merely because canonicalization computes one in memory. A hash stays only when a read path, identity contract, corruption check, or replay invariant consumes it.

## Stage 1 — unambiguous column pruning

Remove write-only projection data whose digest can be recomputed but is never read, and remove the queue timestamp that no query observes:

- `flow_executions.input_hash`
- `flow_executions.metadata_hash`
- `flow_commands.args_hash`
- `flow_commands.retry_policy_hash`
- `flow_commands.result_hash`
- `flow_command_queue.updated_at`

The successful-command shape constraint should require `result IS NOT NULL`; it no longer needs a redundant result-hash presence check. Journal `body_hash`, `start_fingerprint`, and `declaration_fingerprint` remain because current integrity or identity paths consume them.

Acceptance criteria:

- clean installation contains none of the six columns;
- start, command creation, queueing, claiming, renewal, retry, recovery, and successful settlement SQL bind no removed value;
- the successful-result shape constraint still rejects a succeeded row without a result and a non-succeeded row with one;
- migration coverage proves the explicitly preserved semantic columns still exist; and
- formatting, build, static analysis, and the complete test suite pass.

## Stage 2 — PostgreSQL `integer` boundaries while retaining Go `int`

Introduce one internal validator for values written to PostgreSQL `integer` columns. It should accept a field name and bounds, work correctly on both 32-bit and 64-bit Go, and return the existing safe `ErrInvalid` shape before SQL is attempted. Do not scatter architecture-dependent casts through SQL call sites.

Inventory every Go `int` that crosses a durable `integer` boundary, including:

- definition and command versions;
- execution `max_commands`, `command_count`, and `open_commands` inputs or computed deltas;
- command `unsatisfied_waits`, `attempt_ordinal`, and `consumed_attempts` inputs or transitions;
- queue command versions; and
- migration ledger versions and reader/writer compatibility values.

Validate caller-controlled values at their public construction/configuration boundary and validate internal persistence requests again at the store boundary. For database-side increments, make the upper-bound behavior deliberate: reject the semantic transition with a mapped Flow error before arithmetic can overflow. Keep the existing positive/non-negative SQL checks as defense in depth.

Tests must cover the largest accepted value and, on platforms where Go `int` can represent it, values below `math.MinInt32` and above `math.MaxInt32`. Tests should prove rejection occurs before a PostgreSQL encoding/range error and that ordinary values retain their current behavior.

## Stage 3 — exact durable duration precision

Inventory every public duration that becomes durable configuration or durable scheduling input, including execution deadlines, attempt timeouts, initial delays, wait deadlines, retry elapsed bounds/backoff entries, and explicit retry-after delays where they are persisted into a transition.

At the public validation boundary, require:

- the feature's existing zero/disabled semantics;
- positive values where the feature is enabled;
- `duration % time.Millisecond == 0`; and
- safe conversion/arithmetic bounds before calculating milliseconds or adding the duration to database time.

Conversion helpers should make exactness visible in their name and contract. Store whole milliseconds in existing `bigint` millisecond columns. Canonical retry policy encoding must use the same precision rule so equivalent policy values have one durable representation. Add table-driven tests for zero, one millisecond, fractional milliseconds, ordinary values, and overflow-adjacent durations.

## Stage 4 — retry policy storage

Change `flow_commands.retry_policy` from `jsonb` to `bytea` and bind/read the canonical bytes directly. PostgreSQL never filters or indexes inside this value, while `jsonb` reparses and reserializes an otherwise opaque library contract. Keep retry-policy validation and decoding in Go.

Acceptance criteria:

- create, claim, retry, expiration, trace/replay comparison, and declaration fingerprint paths use the same canonical bytes;
- SQL contains no `::jsonb` write or `::text::bytea` recovery workaround for retry policy;
- malformed bytes surface as `ErrInvalidState` on a read path; and
- equivalent declarations continue to coalesce while different retry policies conflict.

## Stage 5 — typed state vocabularies and shared failures

Define typed string aliases and constants for each finite vocabulary that crosses a public boundary. At minimum review execution status, command status, queue state exposed by live-work reads, key scope, and terminal status. Internal-only state types may remain internal, but conversions at package boundaries should be explicit and exhaustive.

Keep the database columns as `text` with named `CHECK` constraints. Add tests that keep Go constants, decoders, filter validation, and database vocabularies synchronized.

Replace parallel `{Code, Message}` structs and anonymous decode shapes with one shared structured failure type in the appropriate non-cyclic package, then alias or expose it publicly as needed. Preserve these projection semantics unless a later decision explicitly replaces them:

- `flow_commands.last_error` is the latest retry/operational failure and may exist before terminality;
- `flow_commands.terminal_failure` is the stable terminal failure projection;
- `flow_executions.failure` is the aggregate terminal/failing reason; and
- successful `flow_commands.result` is the point-read result projection.

Do not collapse those fields merely because they share an encoding. Tests should cover retry-to-success clearing, terminal failure retention, cancellation/expiration, and public inspection decoding.

## Stage 6 — relational and position hardening

Strengthen database ownership rather than relying only on execution-first application code:

- make `flow_executions.root_command_id` non-null;
- ensure `(execution_id, root_command_id)` references a command owned by that execution;
- ensure a command's `(execution_id, parent_command_id)` references a parent in the same execution when present;
- add same-execution composite ownership constraints for dependent queue and event-wait rows, and for journal command references where a command is present;
- retain the deferrable root relationship needed by execution/root creation; and
- add positive checks to every nullable or required journal-position reference, especially command terminal positions and journal causation positions, while preserving the stricter causation-before-current-position rule.

Use the smallest supporting unique constraints/index changes needed for composite foreign keys. Do not add duplicated indexes without checking PostgreSQL's existing primary/unique indexes and query plans.

Tests should attempt cross-execution root, parent, queue, wait, and journal references; null roots; zero/negative terminal or causation positions; and valid deferred creation in one transaction. Each failure should assert the named constraint.

## Stage 7 — columns requiring a semantic decision

Do not remove these in a mechanical pruning pass:

- `flow_executions.input` is the accepted root input and is read for idempotent start comparison and worker materialization.
- `flow_executions.metadata_canonical` supports exact canonical start-identity comparison while `metadata` supports indexed JSON inspection. Decide whether one representation can satisfy both contracts before removing either.
- `flow_commands.declaration_fingerprint` is consumed by command redeclaration/coalescing. Remove it only with a specified replacement comparison and benchmark evidence.
- `flow_commands.result`, `last_error`, and `terminal_failure` support different point-read and lifecycle semantics described above. Keep them unless the projection model is explicitly redesigned.

For each candidate, document its writer, reader, invariant, replacement cost, and effect on idempotency/replay before making a schema change. A column with no SQL reader may still participate in an external migration or diagnostic contract, so update active documentation with the decision.

## Punchlist

### Current pruning slice

- [x] Remove `flow_executions.input_hash` and `metadata_hash` from the baseline migration and start SQL.
- [x] Remove `flow_commands.args_hash` and `retry_policy_hash` from the baseline migration and command-create SQL.
- [x] Remove `flow_commands.result_hash` and simplify the successful-result shape/write SQL.
- [x] Remove `flow_command_queue.updated_at` from the baseline migration and every queue insert/update.
- [x] Add migration regression coverage for all six removals and the explicitly retained semantic columns.
- [x] Preserve input, canonical metadata, declaration fingerprint, result, last error, terminal failure, and journal hashes.

### Follow-up stages

- [ ] Inventory every Go `int` to PostgreSQL `integer` write and add shared range validation.
- [ ] Add boundary tests for accepted and rejected PostgreSQL integer values.
- [ ] Inventory all durable duration paths and enforce exact whole milliseconds publicly and internally.
- [ ] Add duration exactness and overflow tests.
- [ ] Convert opaque retry policy storage from `jsonb` to canonical `bytea`.
- [ ] Introduce typed public state aliases/constants and exhaustive conversions.
- [ ] Consolidate structured failures without collapsing distinct lifecycle projections.
- [ ] Make root identity non-null and same-execution.
- [ ] Add composite ownership integrity for parents and dependent rows.
- [ ] Complete positive position constraints and adversarial constraint tests.
- [ ] Decide the future of `metadata_canonical` with an explicit replacement contract.
- [ ] Re-evaluate `declaration_fingerprint` only with replacement and performance evidence.
- [ ] Re-evaluate result/failure projections only as an explicit inspection-model redesign.
- [ ] Run formatting, build/static checks, race-enabled tests, and update active schema/architecture documentation after every stage.
