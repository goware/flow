---
status: draft
---

# Component: PostgreSQL Schema and Statements

## 1. Purpose and scope

This component owns every table, constraint, index, and SQL statement in `flow`. It is the authority on physical layout; `engine.md` and `runtime.md` consume the statements defined here and never write SQL of their own.

Responsibilities: DDL, constraints, indexes, autovacuum settings, every runtime statement, and the benchmark evidence behind index choices.

Non-responsibilities: algorithms above single statements (`engine.md`), process lifecycle (`runtime.md`), and public Go types (functional spec §4).

All DDL below uses the default schema `flow`. Migration rendering substitutes a validated, quoted schema identifier; runtime SQL is fully qualified and never depends on `search_path`.

## 2. Design rules

1. **One row per logical thing.** A declared-but-unrunnable plan node is a `commands` row in state `pending`, not a parallel entity.
2. **Counters on `executions` make hot decisions O(1).** Completion, position allocation, and outcome checks read the row the transaction already locked.
3. **Whole-execution loads, in-memory computation, delta writes.** The engine loads an execution's commands and clauses once per settle transaction — it needs them for plan evaluation anyway — computes dependency resolution in Go, and writes only what changed. No recursive SQL, no reverse-edge index.
4. **Four timestamp classes, never conflated** (FS §17): `created_at` immutable, `updated_at` any write including claim, `status_at` state transitions only, `eligible_at` grants only.
5. **Partial indexes on hot states.** The claim index contains only `ready` rows; pending, running, and terminal rows never enter it.

## 3. Tables

### 3.1 Executions

```sql
CREATE TABLE flow.executions (
    execution_id        uuid PRIMARY KEY,
    execution_type      text NOT NULL,
    execution_key       text NOT NULL DEFAULT '',

    status              text NOT NULL,
    failing             boolean NOT NULL DEFAULT false,
    fail_fast           boolean NOT NULL DEFAULT true,

    -- exactly one driver
    plan_name           text,
    plan_version        integer,
    root_args           jsonb,
    coordinator_name    text,
    coordinator_version integer,

    request_fingerprint bytea NOT NULL CHECK (octet_length(request_fingerprint) = 32),
    metadata            jsonb NOT NULL DEFAULT '{}'::jsonb,

    next_event_position bigint  NOT NULL DEFAULT 1,
    open_commands       integer NOT NULL DEFAULT 0,
    absent_reads        integer NOT NULL DEFAULT 0,

    deadline_at         timestamptz,
    outcome_ref         text,
    failure             jsonb,

    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    status_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at         timestamptz,

    CONSTRAINT executions_status_valid CHECK (status IN
        ('running','succeeded','failed','cancelled','expired')),
    CONSTRAINT executions_driver_exclusive CHECK (
        (plan_name IS NOT NULL AND plan_version IS NOT NULL AND root_args IS NOT NULL
         AND coordinator_name IS NULL AND coordinator_version IS NULL)
        OR
        (coordinator_name IS NOT NULL AND coordinator_version IS NOT NULL
         AND plan_name IS NULL AND plan_version IS NULL)
    ),
    CONSTRAINT executions_position_positive  CHECK (next_event_position >= 1),
    CONSTRAINT executions_open_nonnegative   CHECK (open_commands >= 0),
    CONSTRAINT executions_absent_nonnegative CHECK (absent_reads >= 0),
    CONSTRAINT executions_failing_only_running CHECK (NOT failing OR status = 'running'),
    CONSTRAINT executions_finished_shape CHECK (
        (status = 'running') = (finished_at IS NULL)
    )
);

CREATE UNIQUE INDEX executions_natural_key_idx
    ON flow.executions (execution_type, execution_key)
    WHERE execution_key <> '';

CREATE INDEX executions_list_idx
    ON flow.executions (execution_type, created_at DESC, execution_id DESC);

CREATE INDEX executions_status_list_idx
    ON flow.executions (status, created_at DESC, execution_id DESC);

CREATE INDEX executions_deadline_idx
    ON flow.executions (deadline_at, execution_id)
    WHERE status = 'running' AND deadline_at IS NOT NULL;
```

`next_event_position` is the allocator proved gap-free in architecture §7. `open_commands` and `absent_reads` are the two counters completion reads (§14 there).

### 3.2 Commands

```sql
CREATE TABLE flow.commands (
    command_id          uuid PRIMARY KEY,
    execution_id        uuid NOT NULL
        REFERENCES flow.executions(execution_id) ON DELETE RESTRICT,
    command_key         text NOT NULL,

    name                text NOT NULL,
    version             integer NOT NULL CHECK (version > 0),
    lane                text NOT NULL DEFAULT 'default',

    payload             jsonb NOT NULL,
    payload_hash        bytea NOT NULL CHECK (octet_length(payload_hash) = 32),

    state               text NOT NULL,
    optional            boolean NOT NULL DEFAULT false,
    unsatisfied_clauses integer NOT NULL DEFAULT 0,

    eligible_at         timestamptz NOT NULL,
    start_deadline_at   timestamptz,
    attempt_timeout_ms  bigint,

    lease_id            uuid,
    leased_at           timestamptz,
    lease_expires_at    timestamptz,

    attempt_count       integer NOT NULL DEFAULT 0,
    consumed_attempts   integer NOT NULL DEFAULT 0,
    max_attempts        integer NOT NULL CHECK (max_attempts > 0),
    retry_policy        jsonb,

    result              jsonb,
    last_error          jsonb,

    origin              text NOT NULL,
    causation_event_id  uuid,

    created_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    status_at           timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at          timestamptz,
    finished_at         timestamptz,

    CONSTRAINT commands_state_valid CHECK (state IN
        ('pending','ready','running','retry_wait',
         'succeeded','failed','skipped','cancelled','expired')),
    CONSTRAINT commands_origin_valid CHECK (origin IN ('plan','coordinator','worker','client')),
    CONSTRAINT commands_clauses_nonnegative CHECK (unsatisfied_clauses >= 0),
    CONSTRAINT commands_pending_has_clauses CHECK (state <> 'pending' OR unsatisfied_clauses > 0),
    CONSTRAINT commands_consumed_le_attempts CHECK (consumed_attempts <= attempt_count),
    CONSTRAINT commands_lease_shape CHECK (
        (lease_id IS NULL AND leased_at IS NULL AND lease_expires_at IS NULL)
        OR
        (lease_id IS NOT NULL AND leased_at IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    CONSTRAINT commands_lease_only_running CHECK (lease_id IS NULL OR state = 'running'),
    CONSTRAINT commands_result_only_succeeded CHECK (result IS NULL OR state = 'succeeded'),

    UNIQUE (execution_id, command_key)
);
```

`commands_pending_has_clauses` is the invariant behind the claim index: a `pending` row always has an unsatisfied clause, so reaching zero is exactly the transition to `ready`.

Indexes:

```sql
-- The hot claim index. Partial on 'ready' so pending/running/terminal never enter it.
CREATE INDEX commands_claim_idx
    ON flow.commands (lane, eligible_at, command_id)
    WHERE state = 'ready';

-- Whole-execution load for plan evaluation and dependency resolution.
CREATE INDEX commands_by_execution_idx
    ON flow.commands (execution_id, command_key)
    INCLUDE (state, name, version, payload_hash, unsatisfied_clauses, optional);

-- Stale-lease recovery.
CREATE INDEX commands_lease_expiry_idx
    ON flow.commands (lease_expires_at, command_id)
    WHERE state = 'running';

-- Start-deadline expiry for pending and ready work.
CREATE INDEX commands_start_deadline_idx
    ON flow.commands (start_deadline_at, command_id)
    WHERE start_deadline_at IS NOT NULL AND state IN ('pending','ready');

-- Retry scheduling and unclaimable-backlog reporting.
CREATE INDEX commands_backlog_idx
    ON flow.commands (name, version, state)
    WHERE state IN ('ready','pending','retry_wait');
```

**Open benchmark question (architecture §9.1).** `commands_claim_idx` does not include `(name, version)`, so a worker registering a subset of kinds filters after the index scan. The adversarial distribution is a lane whose `ready` head is dominated by kinds the process does not register — exactly the rolling-deploy case. The measurement in §7.3 decides whether to add `(name, version)` as leading columns, accepting a wider hot index and more write amplification. Until measured, the simpler index stands.

### 3.3 Dependency clauses

```sql
CREATE TABLE flow.command_deps (
    execution_id  uuid    NOT NULL,
    command_key   text    NOT NULL,
    clause_idx    smallint NOT NULL,

    kind          text NOT NULL,
    threshold     integer,
    member_keys   text[],
    member_events jsonb,

    satisfied     boolean NOT NULL DEFAULT false,
    unsatisfiable boolean NOT NULL DEFAULT false,
    resolved_at   timestamptz,

    PRIMARY KEY (execution_id, command_key, clause_idx),
    FOREIGN KEY (execution_id, command_key)
        REFERENCES flow.commands(execution_id, command_key) ON DELETE CASCADE,

    CONSTRAINT command_deps_kind_valid CHECK (kind IN
        ('all_succeeded','all_settled','all_unsuccessful','at_least','await_event')),
    CONSTRAINT command_deps_threshold_shape CHECK (
        (kind = 'at_least') = (threshold IS NOT NULL)
    ),
    CONSTRAINT command_deps_members_shape CHECK (
        (kind = 'await_event' AND member_events IS NOT NULL AND member_keys IS NULL)
        OR
        (kind <> 'await_event' AND member_keys IS NOT NULL AND member_events IS NULL)
    ),
    CONSTRAINT command_deps_not_both CHECK (NOT (satisfied AND unsatisfiable)),
    CONSTRAINT command_deps_resolved_shape CHECK (
        (resolved_at IS NULL) = (NOT satisfied AND NOT unsatisfiable)
    )
);

CREATE INDEX command_deps_by_execution_idx
    ON flow.command_deps (execution_id, command_key, clause_idx);
```

Members are stored as arrays rather than child rows, and there is **no reverse index from member to clause**. Resolution loads every clause for the execution in one query and computes satisfaction in memory (design rule 3). At the 1,000-node / 100-dependency ceiling this is at most a few thousand narrow rows.

`ADD CONSTRAINT commands_exec_key_unique UNIQUE (execution_id, command_key)` on `flow.commands` provides the composite target for the foreign key above; it is created with the table in §3.2.

### 3.4 Attempts

```sql
CREATE TABLE flow.attempts (
    attempt_id            uuid PRIMARY KEY,
    command_id            uuid NOT NULL
        REFERENCES flow.commands(command_id) ON DELETE CASCADE,
    execution_id          uuid NOT NULL,
    attempt_number        integer NOT NULL CHECK (attempt_number > 0),

    lease_id              uuid NOT NULL,
    worker_id             text NOT NULL,
    process_id            text NOT NULL,

    state                 text NOT NULL,
    consumes_retry_budget boolean NOT NULL DEFAULT false,

    started_at            timestamptz NOT NULL DEFAULT clock_timestamp(),
    heartbeat_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at           timestamptz,
    error                 jsonb,

    CONSTRAINT attempts_state_valid CHECK (state IN
        ('running','succeeded','failed','interrupted','lease_lost','cancelled')),

    UNIQUE (command_id, attempt_number),
    UNIQUE (lease_id)
);

CREATE INDEX attempts_by_command_idx
    ON flow.attempts (command_id, attempt_number DESC);

CREATE INDEX attempts_running_heartbeat_idx
    ON flow.attempts (heartbeat_at, attempt_id)
    WHERE state = 'running';
```

`UNIQUE (lease_id)` makes the lease token a global fencing key: no two attempts can ever present the same token.

### 3.5 Events

```sql
CREATE TABLE flow.events (
    event_id           uuid PRIMARY KEY,
    execution_id       uuid NOT NULL
        REFERENCES flow.executions(execution_id) ON DELETE RESTRICT,
    position           bigint NOT NULL CHECK (position >= 1),

    name               text NOT NULL,
    version            integer NOT NULL CHECK (version > 0),
    event_key          text,
    kind               text NOT NULL,

    payload            jsonb NOT NULL,
    payload_hash       bytea NOT NULL CHECK (octet_length(payload_hash) = 32),

    command_id         uuid,
    attempt_id         uuid,
    causation_event_id uuid,
    correlation_id     text,

    occurred_at        timestamptz NOT NULL,
    recorded_at        timestamptz NOT NULL DEFAULT clock_timestamp(),

    CONSTRAINT events_kind_valid CHECK (kind IN ('completion','domain','runtime')),
    CONSTRAINT events_completion_has_command CHECK (kind <> 'completion' OR command_id IS NOT NULL),

    UNIQUE (execution_id, position)
);

-- Idempotency is scoped across versions, per FS §9.3.
CREATE UNIQUE INDEX events_idempotency_idx
    ON flow.events (execution_id, name, event_key)
    WHERE event_key IS NOT NULL;

-- Await-clause satisfaction and plan fact reads.
CREATE INDEX events_by_name_idx
    ON flow.events (execution_id, name, version, position);

-- Ordered log reads and coordinator inbox delivery.
CREATE INDEX events_log_idx
    ON flow.events (execution_id, position);
```

`events_by_name_idx` is what makes both `Await` satisfaction and `Fact` reads single indexed lookups rather than log scans.

### 3.6 Plan reads

```sql
CREATE TABLE flow.plan_reads (
    execution_id uuid NOT NULL
        REFERENCES flow.executions(execution_id) ON DELETE CASCADE,
    read_kind    text NOT NULL,
    name         text NOT NULL,
    version      integer NOT NULL DEFAULT 0,
    present      boolean NOT NULL,

    PRIMARY KEY (execution_id, read_kind, name, version),
    CONSTRAINT plan_reads_kind_valid CHECK (read_kind IN ('fact','facts','result'))
);
```

One row per input the latest evaluation consulted. `name` holds an event name for `fact`/`facts` and a command key for `result`; `version` is `0` for `result` reads. The whole set is replaced on each evaluation, and `executions.absent_reads` is maintained as the count of rows with `present = false`.

### 3.7 Coordinators

```sql
CREATE TABLE flow.coordinators (
    coordinator_id    uuid PRIMARY KEY,
    execution_id      uuid NOT NULL
        REFERENCES flow.executions(execution_id) ON DELETE RESTRICT,
    name              text NOT NULL,
    version           integer NOT NULL CHECK (version > 0),

    state             text NOT NULL,
    inbox_position    bigint NOT NULL DEFAULT 0,
    coord_state       jsonb NOT NULL,

    lease_id          uuid,
    leased_at         timestamptz,
    lease_expires_at  timestamptz,

    attempt_count     integer NOT NULL DEFAULT 0,
    consumed_attempts integer NOT NULL DEFAULT 0,
    max_attempts      integer NOT NULL DEFAULT 5,
    eligible_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_error        jsonb,

    created_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    status_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at       timestamptz,

    CONSTRAINT coordinators_state_valid CHECK (state IN
        ('active','completed','failed','cancelled')),
    CONSTRAINT coordinators_position_nonnegative CHECK (inbox_position >= 0),

    UNIQUE (execution_id, name)
);

CREATE INDEX coordinators_dispatch_idx
    ON flow.coordinators (name, version, eligible_at, coordinator_id)
    WHERE state = 'active';
```

Only hand-written coordinators occupy this table. A plan-driven execution has no row here; its equivalent state is `plan_reads` plus the command set.

### 3.8 Migration metadata

```sql
CREATE TABLE flow.schema_migrations (
    version         integer PRIMARY KEY,
    name            text NOT NULL,
    checksum        bytea NOT NULL CHECK (octet_length(checksum) = 32),
    library_version text NOT NULL,
    applied_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE flow.schema_compatibility (
    singleton          boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    current_version    integer NOT NULL,
    min_reader_version integer NOT NULL,
    min_writer_version integer NOT NULL,
    updated_at         timestamptz NOT NULL DEFAULT clock_timestamp()
);
```

## 4. Autovacuum

`commands` and `events` are the churn-heavy tables. `commands` sees inserts, claim updates, lease renewals, and terminal updates; `events` is insert-only but grows steadily.

```sql
ALTER TABLE flow.commands SET (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor  = 0.01,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_threshold     = 1000,
    autovacuum_analyze_threshold    = 1000
);

ALTER TABLE flow.attempts SET (fillfactor = 90, autovacuum_vacuum_scale_factor = 0.05);
ALTER TABLE flow.events   SET (fillfactor = 100);
```

`fillfactor = 80` on `commands` leaves room for HOT updates so lease renewal and state flips avoid index churn where possible. These are starting points; §7.3 records measured values.

## 5. Statements

### 5.1 Execution lock and position allocation

Every event-appending transaction opens with:

```sql
-- lock_execution
SELECT execution_id, status, failing, fail_fast, next_event_position,
       open_commands, absent_reads, deadline_at,
       plan_name, plan_version, root_args, coordinator_name, coordinator_version
FROM flow.executions
WHERE execution_id = $1
FOR UPDATE;
```

Positions are reserved in one statement once the transaction knows how many events it will append:

```sql
-- allocate_positions
UPDATE flow.executions
SET next_event_position = next_event_position + $2,
    updated_at = clock_timestamp()
WHERE execution_id = $1
RETURNING next_event_position - $2 AS first_position;
```

Rollback restores the counter, so abandoned transactions leave no gap (architecture §7).

### 5.2 Claim

```sql
-- claim_commands
WITH t AS MATERIALIZED (
    SELECT clock_timestamp() AS now
),
candidates AS (
    SELECT c.command_id
    FROM flow.commands c, t
    WHERE c.lane = $1
      AND c.state = 'ready'
      AND c.eligible_at <= t.now
      AND c.name = ANY($2::text[])
      AND (c.name, c.version) IN (SELECT * FROM unnest($2::text[], $3::int[]))
    ORDER BY c.eligible_at, c.command_id
    FOR UPDATE OF c SKIP LOCKED
    LIMIT $4
),
ord AS (
    SELECT command_id, row_number() OVER (ORDER BY command_id)::int AS n
    FROM candidates
),
claimed AS (
    UPDATE flow.commands c
    SET state            = 'running',
        lease_id         = ($5::uuid[])[o.n],
        leased_at        = t.now,
        lease_expires_at = t.now + $6::interval,
        attempt_count    = c.attempt_count + 1,
        started_at       = COALESCE(c.started_at, t.now),
        status_at        = t.now,
        updated_at       = t.now
    FROM ord o, t
    WHERE c.command_id = o.command_id
    RETURNING c.*, o.n
)
SELECT * FROM claimed ORDER BY n;
```

Notes:

- `$2`/`$3` are parallel arrays of the process's registered names and versions. The redundant `name = ANY($2)` predicate is a sargable pre-filter the planner can use against the index before the pair check.
- `$4` is bounded by immediately free local capacity, never by a configured batch size alone.
- `$5` is a Go-generated UUIDv7 array, one per requested slot; PostgreSQL arrays are one-indexed, matching `row_number()`. Surplus tokens are discarded.
- **`eligible_at` is not written here.** Claiming touches `lease_*`, `status_at`, and `updated_at` only, preserving the taxonomy rule that grants alone move eligibility.
- No execution row is locked, and `SKIP LOCKED` means the statement never waits — the deadlock exemption in architecture §8.3.

Attempt insertion follows in the same transaction:

```sql
-- insert_attempts
INSERT INTO flow.attempts
    (attempt_id, command_id, execution_id, attempt_number, lease_id,
     worker_id, process_id, state)
SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::int[], $5::uuid[],
                     $6::text[], $7::text[], array_fill('running'::text, ARRAY[$8]));
```

### 5.3 Whole-execution load

One query per settle transaction feeds both plan evaluation and dependency resolution:

```sql
-- load_execution_graph
SELECT command_key, command_id, name, version, state, payload_hash,
       unsatisfied_clauses, optional, eligible_at, start_deadline_at
FROM flow.commands
WHERE execution_id = $1;

-- load_execution_clauses
SELECT command_key, clause_idx, kind, threshold, member_keys, member_events,
       satisfied, unsatisfiable
FROM flow.command_deps
WHERE execution_id = $1;

-- load_event_index
SELECT name, version, count(*) AS n, min(position) AS first_position
FROM flow.events
WHERE execution_id = $1
GROUP BY name, version;
```

The event index is names and counts only. Payloads are fetched lazily, and only for names the plan actually reads:

```sql
-- load_facts
SELECT name, version, position, payload
FROM flow.events
WHERE execution_id = $1 AND name = $2 AND version = $3
ORDER BY position;
```

### 5.4 Settle

```sql
-- append_events
INSERT INTO flow.events
    (event_id, execution_id, position, name, version, event_key, kind,
     payload, payload_hash, command_id, attempt_id, causation_event_id,
     correlation_id, occurred_at)
SELECT * FROM unnest(...);

-- succeed_command  (fenced)
UPDATE flow.commands
SET state = 'succeeded', result = $3,
    lease_id = NULL, leased_at = NULL, lease_expires_at = NULL,
    finished_at = clock_timestamp(), status_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE command_id = $1
  AND lease_id = $2
  AND state = 'running'
  AND lease_expires_at > clock_timestamp()
RETURNING command_key;

-- finish_attempt
UPDATE flow.attempts
SET state = $2, consumes_retry_budget = $3,
    finished_at = clock_timestamp(), error = $4
WHERE attempt_id = $1;

-- adjust_open_commands
UPDATE flow.executions
SET open_commands = open_commands + $2, updated_at = clock_timestamp()
WHERE execution_id = $1;
```

Zero rows from `succeed_command` means the lease was lost or the command is no longer running; the transaction rolls back and the runtime reports `ErrLeaseLost`.

### 5.5 Failure and retry

```sql
-- retry_command  (fenced)
UPDATE flow.commands
SET state = 'retry_wait',
    eligible_at = $4,                       -- a grant: eligibility legitimately moves
    consumed_attempts = consumed_attempts + 1,
    last_error = $3,
    lease_id = NULL, leased_at = NULL, lease_expires_at = NULL,
    status_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE command_id = $1 AND lease_id = $2 AND state = 'running'
RETURNING command_key;

-- fail_command  (fenced)
UPDATE flow.commands
SET state = 'failed', last_error = $3,
    consumed_attempts = consumed_attempts + 1,
    lease_id = NULL, leased_at = NULL, lease_expires_at = NULL,
    finished_at = clock_timestamp(), status_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE command_id = $1 AND lease_id = $2 AND state = 'running'
RETURNING command_key;
```

`retry_wait → ready` happens through the maintenance sweep or directly when the retry time has already passed; the transition writes `state` only, never `eligible_at`.

An interruption — shutdown or lease loss — writes the attempt row and releases the command without incrementing `consumed_attempts`:

```sql
-- release_command
UPDATE flow.commands
SET state = 'ready',
    lease_id = NULL, leased_at = NULL, lease_expires_at = NULL,
    status_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE command_id = $1 AND lease_id = $2 AND state = 'running';
```

### 5.6 Reconciliation writes

```sql
-- insert_commands  (bulk, ordered by command_key)
INSERT INTO flow.commands (...)
SELECT * FROM unnest(...)
ON CONFLICT (execution_id, command_key) DO NOTHING
RETURNING command_key, command_id;

-- insert_clauses
INSERT INTO flow.command_deps
    (execution_id, command_key, clause_idx, kind, threshold, member_keys, member_events)
SELECT * FROM unnest(...);

-- update_clause_resolution
UPDATE flow.command_deps d
SET satisfied = v.satisfied, unsatisfiable = v.unsatisfiable,
    resolved_at = clock_timestamp()
FROM (SELECT * FROM unnest($2::text[], $3::smallint[], $4::bool[], $5::bool[])
        AS t(command_key, clause_idx, satisfied, unsatisfiable)) v
WHERE d.execution_id = $1
  AND d.command_key = v.command_key
  AND d.clause_idx = v.clause_idx
  AND NOT d.satisfied AND NOT d.unsatisfiable;      -- guard: resolve once

-- promote_ready
UPDATE flow.commands
SET state = 'ready', unsatisfied_clauses = 0,
    status_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE execution_id = $1 AND command_key = ANY($2::text[]) AND state = 'pending';

-- skip_commands
UPDATE flow.commands
SET state = 'skipped', finished_at = clock_timestamp(),
    status_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE execution_id = $1 AND command_key = ANY($2::text[]) AND state = 'pending';
```

`ON CONFLICT DO NOTHING` on insert plus the `state = 'pending'` guards make repeated plan evaluation idempotent at the statement level, independent of the engine's in-memory checks.

### 5.7 Plan reads

```sql
-- replace_plan_reads
DELETE FROM flow.plan_reads WHERE execution_id = $1;

INSERT INTO flow.plan_reads (execution_id, read_kind, name, version, present)
SELECT * FROM unnest(...);

UPDATE flow.executions
SET absent_reads = $2, updated_at = clock_timestamp()
WHERE execution_id = $1;
```

### 5.8 Completion

```sql
-- complete_execution
UPDATE flow.executions
SET status = $2, outcome_ref = $3, failure = $4, failing = false,
    finished_at = clock_timestamp(), status_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE execution_id = $1
  AND status = 'running'
  AND open_commands = 0
  AND ($2 <> 'succeeded' OR absent_reads = 0)
RETURNING execution_id;
```

The predicate enforces both completion conditions in the database, so an engine bug cannot produce a false success. Zero rows means the execution was no longer eligible to complete.

### 5.9 Maintenance

```sql
-- expire_start_deadlines
WITH doomed AS (
    SELECT command_id FROM flow.commands
    WHERE start_deadline_at <= clock_timestamp()
      AND state IN ('pending','ready')
    ORDER BY start_deadline_at, command_id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE flow.commands c SET state = 'expired', ... FROM doomed WHERE c.command_id = doomed.command_id
RETURNING c.execution_id, c.command_key;

-- recover_expired_leases
SELECT command_id, execution_id, lease_id FROM flow.commands
WHERE state = 'running' AND lease_expires_at <= clock_timestamp()
ORDER BY lease_expires_at, command_id
FOR UPDATE SKIP LOCKED
LIMIT $1;

-- due_retries
UPDATE flow.commands
SET state = 'ready', status_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE command_id IN (
    SELECT command_id FROM flow.commands
    WHERE state = 'retry_wait' AND eligible_at <= clock_timestamp()
    ORDER BY eligible_at, command_id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
RETURNING lane;

-- expire_executions
SELECT execution_id FROM flow.executions
WHERE status = 'running' AND deadline_at <= clock_timestamp()
ORDER BY deadline_at, execution_id
FOR UPDATE SKIP LOCKED
LIMIT $1;

-- unclaimable_backlog
SELECT name, version, count(*) AS pending
FROM flow.commands
WHERE state = 'ready' AND eligible_at <= clock_timestamp()
GROUP BY name, version;
```

Every sweep is bounded and uses `SKIP LOCKED`, so duplicate maintenance runners are safe with no leader election. Deadline expiry and lease recovery re-enter the settle path per execution, taking the execution lock in the ordinary way.

### 5.10 Inspection

```sql
-- trace: four bounded queries, all indexed
SELECT ... FROM flow.executions  WHERE execution_id = $1;
SELECT ... FROM flow.commands    WHERE execution_id = $1 ORDER BY created_at, command_id;
SELECT ... FROM flow.command_deps WHERE execution_id = $1;
SELECT ... FROM flow.events      WHERE execution_id = $1 ORDER BY position;
SELECT ... FROM flow.attempts    WHERE execution_id = $1 ORDER BY command_id, attempt_number;

-- history increment
SELECT ... FROM flow.events
WHERE execution_id = $1 AND position > $2
ORDER BY position LIMIT $3;
```

`Trace` runs its queries in one `REPEATABLE READ` transaction so the returned picture is self-consistent.

## 6. Error mapping surface

Constraint names are a stable part of the migration contract and are the sole basis for classification. Renaming one is a breaking change.

| Constraint / condition | Public error |
|---|---|
| `executions_natural_key_idx` | compare fingerprint → existing execution or `ErrConflict` |
| `commands` `UNIQUE (execution_id, command_key)` | compare `payload_hash` → no-op or `ErrConflict` |
| `events_idempotency_idx` | compare `payload_hash` → existing event or `ErrConflict` |
| `events` `UNIQUE (execution_id, position)` | internal invariant violation; never surfaced |
| `attempts` `UNIQUE (lease_id)` | internal invariant violation |
| `coordinators` `UNIQUE (execution_id, name)` | idempotent instance or `ErrConflict` |
| zero rows from a fenced update | diagnostic lookup → `ErrLeaseLost`, `ErrTerminal`, or `ErrNotFound` |
| zero rows from `complete_execution` | not eligible; retry on next settle |
| any `CHECK` violation | `ErrInvalid` with the safe field name |

## 7. Test and benchmark plan

### 7.1 Constraint tests

Every `CHECK` and unique index has a test asserting it rejects the invalid shape: lease without running state, result on a non-succeeded command, pending with zero clauses, both `satisfied` and `unsatisfiable`, driver-exclusivity on executions, `failing` outside `running`, negative counters.

### 7.2 Statement tests

- claim assigns distinct lease tokens and never returns the same command twice under 100 concurrent claimers;
- claim writes no `eligible_at`;
- allocation returns contiguous positions and rollback restores the counter;
- fenced updates reject stale lease tokens;
- reconciliation statements are idempotent when replayed;
- `complete_execution` refuses while `open_commands > 0` or `absent_reads > 0`;
- every maintenance sweep is bounded and safe with concurrent runners.

### 7.3 Benchmarks and plans

`EXPLAIN (ANALYZE, BUFFERS)` for the claim, whole-execution load, and event-name lookup at 10K, 100K, 1M, and 10M command rows, across representative state distributions.

Required measurements before the index set is frozen:

1. **The `(name, version)` claim question** (§3.2) — measured against a lane where 90% of `ready` head rows are unregistered kinds.
2. Whole-execution load cost at 1,000 nodes, confirming the plan cost model in FS §10.5.
3. Write amplification of `commands_claim_idx` under sustained claim/renew/settle churn.
4. HOT-update rate on `commands` at `fillfactor` 80 versus 90 versus default.
5. Dead-tuple and WAL rate on `commands` and `events` under a sustained mixed workload.

## 8. Acceptance conditions

- every table, constraint, and index above exists in migration SQL with the stated names;
- constraint names match the error-mapping table exactly;
- no statement writes `eligible_at` outside a documented grant;
- no statement writes an event without holding the execution lock;
- the claim statement takes no execution lock and never waits;
- resolution requires no reverse-edge index and no recursive SQL;
- all constraint, statement, and concurrency tests pass;
- benchmark evidence is recorded for each item in §7.3, and the index set reflects it.
