---
status: draft
---

# Component: PostgreSQL storage and journal

## 1. Purpose and scope

This component owns the physical PostgreSQL contract for `flow`: tables, constraints, indexes, migrations, statement boundaries, durable time, and SQL error classification. `internal/store` is generated or handwritten against this document; no other package issues Flow SQL.

It does not decide plan semantics, retry outcomes, or process scheduling. Those decisions arrive as validated engine changes and runtime claim/renew requests.

All Flow-owned table names have a fixed `flow_` prefix. Examples use the default `public` schema, so Flow can live beside application tables while remaining visibly namespaced. Migration rendering may substitute a separately validated and quoted schema identifier, but it never removes the table prefix, and every runtime statement remains schema-qualified rather than depending on `search_path`.

The application passes its existing `*pgkit.DB` to `New` and `Migrate`; this component does not create or own another database adapter or pool. A custom schema is an optional organizational choice, not a requirement for isolation.

## 2. Storage rules

1. The journal and materializations commit in one transaction.
2. No journal insert occurs without first locking its execution row.
3. All durable timestamps in one transition come from one `clock_timestamp()` value captured after acquiring the execution lock, never before a blocking wait.
4. Canonical payloads are `bytea`; SQL never interprets application JSON.
5. Digests accelerate comparison, but a conflict path compares canonical bytes too.
6. Pending commands have no dispatch row. Ready, retry-wait, and running commands have exactly one dispatch row. Terminal commands have none.
7. Lease renewal touches only hot delivery rows and is intentionally absent from the journal.
8. Every statement is bounded by an execution, a claim batch, a maintenance batch, or a page limit.
9. Constraint and index names are stable because error mapping and migrations refer to them.
10. No correctness query depends on `LISTEN`, notification delivery, application time, or process-local state.

## 3. Schema

The DDL is the target shape for the initial migration. Small column-type changes discovered during implementation require an architecture amendment; semantic changes require the functional spec to reopen.

The initial physical inventory is ten tables:

| Table | Purpose |
|---|---|
| `flow_executions` | Execution identity, driver, lifecycle, counters, deadline, and journal-position allocator. |
| `flow_commands` | Durable command definitions, current states, accepted policies, results, and timing anchors. |
| `flow_command_dispatch` | Narrow hot queue and lease rows for runnable commands. |
| `flow_command_dependency_groups` | Durable join clauses attached to dependent commands. |
| `flow_command_dependency_members` | Predecessor membership for each dependency group. |
| `flow_command_event_waits` | Persisted `Await` selectors and their satisfying journal positions. |
| `flow_journal` | Immutable execution-local history for commands, attempts, events, outcomes, and coordinator transitions. |
| `flow_plan_reads` | Latest durable plan-read and routing set. |
| `flow_coordinators` | Coordinator state, inbox position, selected delivery, retry state, and lease. |
| `flow_schema_migrations` | Applied migration versions, checksums, library versions, and active reader/writer compatibility. |

There is intentionally no separate queue table, event table, coordinator-inbox table, attempt table, or schema-compatibility table. Queue lanes are immutable columns on command/dispatch rows, events and attempt history live in `flow_journal`, the coordinator inbox cursor and selected delivery live in `flow_coordinators`, and the latest migration row carries the active compatibility range.

### 3.1 Executions

```sql
CREATE TABLE public.flow_executions (
    execution_id          uuid PRIMARY KEY,
    driver_mode           text NOT NULL,
    definition_name       text NOT NULL,
    definition_version    integer NOT NULL CHECK (definition_version > 0),
    execution_key         text NOT NULL DEFAULT '',

    status                text NOT NULL DEFAULT 'running',
    fail_fast             boolean NOT NULL DEFAULT true,

    start_fingerprint     bytea NOT NULL CHECK (octet_length(start_fingerprint) = 32),
    input                 bytea NOT NULL,
    input_hash            bytea NOT NULL CHECK (octet_length(input_hash) = 32),
    metadata              jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata_canonical    bytea NOT NULL,
    metadata_hash         bytea NOT NULL CHECK (octet_length(metadata_hash) = 32),

    deadline_at           timestamptz,
    max_commands          integer NOT NULL CHECK (max_commands >= 0),
    command_count         integer NOT NULL DEFAULT 0 CHECK (command_count >= 0),
    open_commands         integer NOT NULL DEFAULT 0 CHECK (open_commands >= 0),

    temporary_plan_reads  integer NOT NULL DEFAULT 0 CHECK (temporary_plan_reads >= 0),
    plan_quiescent        boolean NOT NULL DEFAULT false,
    plan_revision         bigint NOT NULL DEFAULT 0 CHECK (plan_revision >= 0),

    next_journal_position bigint NOT NULL DEFAULT 1 CHECK (next_journal_position >= 1),
    root_command_id       uuid,
    outcome_ref           text,
    failure               jsonb,

    created_at            timestamptz NOT NULL,
    updated_at            timestamptz NOT NULL,
    status_at             timestamptz NOT NULL,
    finished_at           timestamptz,

    CONSTRAINT flow_executions_driver_mode_ck CHECK
        (driver_mode IN ('direct', 'plan', 'coordinator')),
    CONSTRAINT flow_executions_status_ck CHECK
        (status IN ('running', 'failing', 'succeeded', 'failed', 'cancelled', 'expired')),
    CONSTRAINT flow_executions_terminal_shape_ck CHECK (
        (status IN ('running', 'failing') AND finished_at IS NULL)
        OR
        (status IN ('succeeded', 'failed', 'cancelled', 'expired') AND finished_at IS NOT NULL)
    ),
    CONSTRAINT flow_executions_command_limit_ck CHECK
        (max_commands = 0 OR command_count <= max_commands),
    CONSTRAINT flow_executions_plan_fields_ck CHECK (
        (driver_mode = 'plan') OR
        (temporary_plan_reads = 0 AND plan_revision = 0 AND plan_quiescent = false)
    )
);

CREATE UNIQUE INDEX flow_executions_idempotency_uq
    ON public.flow_executions (driver_mode, definition_name, execution_key)
    WHERE execution_key <> '';

CREATE INDEX flow_executions_list_idx
    ON public.flow_executions (definition_name, created_at DESC, execution_id DESC);

CREATE INDEX flow_executions_key_prefix_idx
    ON public.flow_executions
       (definition_name, execution_key text_pattern_ops, created_at DESC, execution_id DESC);

CREATE INDEX flow_executions_status_idx
    ON public.flow_executions (status, created_at DESC, execution_id DESC);

CREATE INDEX flow_executions_metadata_idx
    ON public.flow_executions USING gin (metadata jsonb_path_ops);

CREATE INDEX flow_executions_deadline_idx
    ON public.flow_executions (deadline_at, execution_id)
    WHERE status IN ('running', 'failing') AND deadline_at IS NOT NULL;
```

`start_fingerprint` covers definition version, canonical input, explicit deadline/fail-fast/metadata options, and driver mode. It deliberately excludes the runtime's accepted `max_commands` default and a direct root command's definition-level retry/timeout/queue defaults: an idempotent repeat returns the already accepted execution under changed deployment defaults. Metadata is duplicated as canonical bytes for identity and `jsonb` for the bounded `ListExecutions` containment filter; keys, values, and total size are validated before insertion.

`status = 'failing'` is the durable form of failure handling in progress. `plan_quiescent` means the latest completed evaluation cycle reached a no-new-command pass. A cycle that creates ordinary open work may stop with it false because that work already blocks completion; its later routed terminal transition supplies the next evaluation. The field is ignored outside plan mode.

### 3.2 Commands

```sql
CREATE TABLE public.flow_commands (
    command_id              uuid PRIMARY KEY,
    execution_id            uuid NOT NULL
        REFERENCES public.flow_executions(execution_id) ON DELETE RESTRICT,
    command_key             text NOT NULL,

    name                    text NOT NULL,
    version                 integer NOT NULL CHECK (version > 0),
    origin                  text NOT NULL,
    parent_command_id       uuid REFERENCES public.flow_commands(command_id) ON DELETE RESTRICT,
    required                boolean NOT NULL DEFAULT true,

    args                    bytea NOT NULL,
    args_hash               bytea NOT NULL CHECK (octet_length(args_hash) = 32),
    declaration_fingerprint bytea NOT NULL CHECK (octet_length(declaration_fingerprint) = 32),

    state                   text NOT NULL,
    unsatisfied_groups      integer NOT NULL DEFAULT 0 CHECK (unsatisfied_groups >= 0),
    unsatisfied_waits       integer NOT NULL DEFAULT 0 CHECK (unsatisfied_waits >= 0),
    child_membership_closed boolean NOT NULL DEFAULT false,
    failure_scope           boolean NOT NULL DEFAULT false,

    queue                   text NOT NULL,
    attempt_timeout_ms      bigint CHECK (attempt_timeout_ms IS NULL OR attempt_timeout_ms > 0),
    retry_policy            jsonb NOT NULL,
    retry_policy_hash       bytea NOT NULL CHECK (octet_length(retry_policy_hash) = 32),

    schedule_kind           text NOT NULL DEFAULT 'none',
    initial_delay_ms        bigint CHECK (initial_delay_ms IS NULL OR initial_delay_ms > 0),
    budget_started_at       timestamptz,
    next_attempt_at         timestamptz,
    wait_started_at         timestamptz,
    wait_deadline_at        timestamptz,

    attempt_ordinal         integer NOT NULL DEFAULT 0 CHECK (attempt_ordinal >= 0),
    consumed_attempts       integer NOT NULL DEFAULT 0 CHECK (consumed_attempts >= 0),

    result                  bytea,
    result_hash             bytea CHECK (result_hash IS NULL OR octet_length(result_hash) = 32),
    last_error              jsonb,
    terminal_failure       jsonb,
    terminal_position       bigint,

    created_position        bigint NOT NULL CHECK (created_position >= 1),
    created_at              timestamptz NOT NULL,
    updated_at              timestamptz NOT NULL,
    status_at               timestamptz NOT NULL,
    finished_at             timestamptz,

    CONSTRAINT flow_commands_execution_key_uq UNIQUE (execution_id, command_key),
    CONSTRAINT flow_commands_origin_ck CHECK
        (origin IN ('direct_root', 'plan', 'worker_spawn', 'coordinator_spawn', 'external_issue')),
    CONSTRAINT flow_commands_state_ck CHECK
        (state IN ('pending', 'ready', 'running', 'retry_wait',
                   'succeeded', 'failed', 'cancelled', 'expired', 'skipped')),
    CONSTRAINT flow_commands_schedule_kind_ck CHECK
        (schedule_kind IN ('none', 'plan_delay', 'spawn_start_after')),
    CONSTRAINT flow_commands_schedule_shape_ck CHECK (
        (schedule_kind = 'none' AND initial_delay_ms IS NULL)
        OR
        (schedule_kind <> 'none' AND initial_delay_ms IS NOT NULL)
    ),
    CONSTRAINT flow_commands_parent_shape_ck CHECK (
        (origin = 'worker_spawn' AND parent_command_id IS NOT NULL)
        OR
        (origin <> 'worker_spawn' AND parent_command_id IS NULL)
    ),
    CONSTRAINT flow_commands_result_shape_ck CHECK (
        (state = 'succeeded' AND result IS NOT NULL AND result_hash IS NOT NULL)
        OR
        (state <> 'succeeded' AND result IS NULL AND result_hash IS NULL)
    ),
    CONSTRAINT flow_commands_pending_shape_ck CHECK (
        state <> 'pending' OR (unsatisfied_groups + unsatisfied_waits > 0)
    ),
    CONSTRAINT flow_commands_child_closure_ck CHECK (
        child_membership_closed = (state = 'succeeded')
    ),
    CONSTRAINT flow_commands_terminal_shape_ck CHECK (
        (state IN ('succeeded', 'failed', 'cancelled', 'expired', 'skipped')
            AND finished_at IS NOT NULL AND terminal_position IS NOT NULL)
        OR
        (state NOT IN ('succeeded', 'failed', 'cancelled', 'expired', 'skipped')
            AND finished_at IS NULL AND terminal_position IS NULL)
    ),
    CONSTRAINT flow_commands_budget_shape_ck CHECK (
        (budget_started_at IS NULL) = (next_attempt_at IS NULL)
    ),
    CONSTRAINT flow_commands_wait_deadline_shape_ck CHECK (
        wait_deadline_at IS NULL OR wait_started_at IS NOT NULL
    ),
    CONSTRAINT flow_commands_attempt_counts_ck CHECK (consumed_attempts <= attempt_ordinal)
);

CREATE INDEX flow_commands_execution_idx
    ON public.flow_commands (execution_id, command_key)
    INCLUDE (command_id, name, version, origin, parent_command_id, required,
             state, unsatisfied_groups, unsatisfied_waits, terminal_position);

CREATE INDEX flow_commands_parent_idx
    ON public.flow_commands (parent_command_id, command_key)
    WHERE parent_command_id IS NOT NULL;

CREATE INDEX flow_commands_terminal_idx
    ON public.flow_commands (execution_id, terminal_position)
    WHERE terminal_position IS NOT NULL;

CREATE INDEX flow_commands_wait_deadline_idx
    ON public.flow_commands (wait_deadline_at, command_id)
    INCLUDE (execution_id)
    WHERE state = 'pending' AND wait_deadline_at IS NOT NULL;
```

`declaration_fingerprint` excludes definition-level operational defaults. For plan nodes it includes whether an explicit retry override was present and its canonical value. The accepted effective policy remains in `retry_policy` for execution and inspection.

`budget_started_at` is null while a pending node has not completed prerequisites. It becomes immutable when the initial `next_attempt_at` is established. A retry modifies only `next_attempt_at`.

### 3.3 Command dispatch

The hot queue row is separated from the wider command projection so lease churn and ready scans do not bloat immutable payload and topology pages.

```sql
CREATE TABLE public.flow_command_dispatch (
    command_id        uuid PRIMARY KEY
        REFERENCES public.flow_commands(command_id) ON DELETE CASCADE,
    execution_id      uuid NOT NULL
        REFERENCES public.flow_executions(execution_id) ON DELETE CASCADE,

    queue             text NOT NULL,
    name              text NOT NULL,
    version           integer NOT NULL CHECK (version > 0),
    plan_name         text NOT NULL DEFAULT '',
    plan_version      integer NOT NULL DEFAULT 0 CHECK (plan_version >= 0),
    state             text NOT NULL,
    next_run_at       timestamptz NOT NULL,

    active_attempt_id uuid,
    lease_token       uuid,
    lease_owner       text,
    lease_started_at  timestamptz,
    lease_expires_at  timestamptz,
    updated_at        timestamptz NOT NULL,

    CONSTRAINT flow_command_dispatch_state_ck CHECK
        (state IN ('ready', 'retry_wait', 'running')),
    CONSTRAINT flow_command_dispatch_plan_shape_ck CHECK
        ((plan_name = '' AND plan_version = 0)
         OR (plan_name <> '' AND plan_version > 0)),
    CONSTRAINT flow_command_dispatch_lease_shape_ck CHECK (
        (state = 'running' AND active_attempt_id IS NOT NULL AND lease_token IS NOT NULL
         AND lease_owner IS NOT NULL AND lease_started_at IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (state <> 'running' AND active_attempt_id IS NULL AND lease_token IS NULL
         AND lease_owner IS NULL AND lease_started_at IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE INDEX flow_command_dispatch_claim_idx
    ON public.flow_command_dispatch
       (name, version, plan_name, plan_version, next_run_at, queue, command_id)
    INCLUDE (execution_id)
    WHERE state IN ('ready', 'retry_wait');

CREATE INDEX flow_command_dispatch_lease_idx
    ON public.flow_command_dispatch (lease_expires_at, command_id)
    INCLUDE (execution_id, active_attempt_id, lease_token)
    WHERE state = 'running';

CREATE INDEX flow_command_dispatch_execution_idx
    ON public.flow_command_dispatch (execution_id, command_id);
```

Denormalizing immutable `(queue, name, version)` and the plan pair (empty outside plan mode) is deliberate. It prevents a replica that handles a subset of kinds or plan versions from repeatedly scanning an unhandled head of a shared lane. Store tests assert these values equal the owning command and execution at insertion; they never change later.

### 3.4 Dependency groups and members

Every builder call is one group; all groups on a dependent combine with AND.

```sql
CREATE TABLE public.flow_command_dependency_groups (
    group_id             uuid PRIMARY KEY,
    execution_id         uuid NOT NULL
        REFERENCES public.flow_executions(execution_id) ON DELETE CASCADE,
    dependent_command_id uuid NOT NULL
        REFERENCES public.flow_commands(command_id) ON DELETE CASCADE,
    ordinal              smallint NOT NULL CHECK (ordinal >= 0),
    kind                 text NOT NULL,
    threshold            integer,
    state                text NOT NULL DEFAULT 'unresolved',
    resolved_at          timestamptz,

    CONSTRAINT flow_command_dependency_groups_ordinal_uq
        UNIQUE (dependent_command_id, ordinal),
    CONSTRAINT flow_command_dependency_groups_kind_ck CHECK
        (kind IN ('all_succeeded', 'all_settled', 'all_failed', 'at_least')),
    CONSTRAINT flow_command_dependency_groups_threshold_ck CHECK (
        (kind = 'at_least' AND threshold IS NOT NULL AND threshold > 0)
        OR
        (kind <> 'at_least' AND threshold IS NULL)
    ),
    CONSTRAINT flow_command_dependency_groups_state_ck CHECK
        (state IN ('unresolved', 'satisfied', 'unsatisfiable')),
    CONSTRAINT flow_command_dependency_groups_resolved_ck CHECK
        ((state = 'unresolved') = (resolved_at IS NULL))
);

CREATE TABLE public.flow_command_dependency_members (
    group_id               uuid NOT NULL
        REFERENCES public.flow_command_dependency_groups(group_id) ON DELETE CASCADE,
    predecessor_command_id uuid NOT NULL
        REFERENCES public.flow_commands(command_id) ON DELETE RESTRICT,
    execution_id           uuid NOT NULL
        REFERENCES public.flow_executions(execution_id) ON DELETE CASCADE,
    predecessor_key        text NOT NULL,

    PRIMARY KEY (group_id, predecessor_command_id)
);

CREATE INDEX flow_command_dependency_reverse_idx
    ON public.flow_command_dependency_members (predecessor_command_id, group_id);

CREATE INDEX flow_command_dependency_execution_idx
    ON public.flow_command_dependency_groups (execution_id, dependent_command_id, ordinal);
```

The application-level insertion validator guarantees predecessor and dependent belong to the same execution. A deferred composite foreign key may enforce that too if the implementation finds the added unique indexes worthwhile; correctness must not rely on a cross-execution key supplied by the caller.

### 3.5 Event waits

`event_namespace` is an internal discriminator carried by `EventName`. It distinguishes an application event definition from a derived command-success selector without adding another developer-facing event concept.

```sql
CREATE TABLE public.flow_command_event_waits (
    command_id         uuid NOT NULL
        REFERENCES public.flow_commands(command_id) ON DELETE CASCADE,
    execution_id       uuid NOT NULL
        REFERENCES public.flow_executions(execution_id) ON DELETE CASCADE,
    event_namespace    text NOT NULL,
    event_name         text NOT NULL,
    event_version      integer NOT NULL CHECK (event_version > 0),
    satisfied_position bigint,

    PRIMARY KEY (command_id, event_namespace, event_name, event_version),
    CONSTRAINT flow_command_event_waits_namespace_ck CHECK
        (event_namespace IN ('application', 'command_success')),
    CONSTRAINT flow_command_event_waits_position_ck CHECK
        (satisfied_position IS NULL OR satisfied_position >= 1)
);

CREATE INDEX flow_command_event_waits_reverse_idx
    ON public.flow_command_event_waits
       (execution_id, event_namespace, event_name, event_version, command_id)
    WHERE satisfied_position IS NULL;
```

The once-only `wait_started_at` and `wait_deadline_at` live on the command because one `Within` bounds the node's complete `Await` set.

### 3.6 Journal

```sql
CREATE TABLE public.flow_journal (
    execution_id       uuid NOT NULL
        REFERENCES public.flow_executions(execution_id) ON DELETE RESTRICT,
    position           bigint NOT NULL CHECK (position >= 1),
    entry_id           uuid NOT NULL,
    entry_kind         text NOT NULL,
    recorded_at        timestamptz NOT NULL,
    causation_position bigint,

    command_id         uuid,
    attempt_id         uuid,
    coordinator_id     uuid,

    event_id           uuid,
    event_namespace    text,
    event_name         text,
    event_version      integer,
    event_key          text,
    event_class        text,
    terminal_status    text,

    body               bytea NOT NULL,
    body_hash          bytea NOT NULL CHECK (octet_length(body_hash) = 32),

    PRIMARY KEY (execution_id, position),
    CONSTRAINT flow_journal_entry_id_uq UNIQUE (entry_id),
    CONSTRAINT flow_journal_position_causation_ck CHECK
        (causation_position IS NULL OR causation_position < position),
    CONSTRAINT flow_journal_entry_kind_ck CHECK
        (entry_kind IN ('execution_started', 'execution_failing', 'command_created',
                        'attempt_started', 'attempt_concluded',
                        'event_recorded', 'coordinator_transition')),
    CONSTRAINT flow_journal_event_shape_ck CHECK (
        (entry_kind = 'event_recorded'
            AND event_id IS NOT NULL AND event_namespace IS NOT NULL
            AND event_name IS NOT NULL AND event_version IS NOT NULL
            AND event_class IS NOT NULL)
        OR
        (entry_kind <> 'event_recorded'
            AND event_id IS NULL AND event_namespace IS NULL
            AND event_name IS NULL AND event_version IS NULL
            AND event_key IS NULL AND event_class IS NULL AND terminal_status IS NULL)
    ),
    CONSTRAINT flow_journal_subject_shape_ck CHECK (
        (entry_kind <> 'command_created' OR command_id IS NOT NULL)
        AND (
            (entry_kind IN ('attempt_started', 'attempt_concluded')
                AND attempt_id IS NOT NULL
                AND num_nonnulls(command_id, coordinator_id) = 1)
            OR
            (entry_kind NOT IN ('attempt_started', 'attempt_concluded')
                AND attempt_id IS NULL)
        )
        AND (entry_kind <> 'coordinator_transition' OR coordinator_id IS NOT NULL)
        AND (event_class IS DISTINCT FROM 'coordinator_terminal' OR coordinator_id IS NOT NULL)
    ),
    CONSTRAINT flow_journal_event_namespace_ck CHECK
        (event_namespace IS NULL OR event_namespace IN ('application', 'command_success', 'runtime')),
    CONSTRAINT flow_journal_event_class_ck CHECK
        (event_class IS NULL OR event_class IN
            ('application', 'command_terminal', 'execution_terminal',
             'plan_terminal', 'coordinator_terminal')),
    CONSTRAINT flow_journal_command_terminal_shape_ck CHECK (
        event_class <> 'command_terminal'
        OR (command_id IS NOT NULL AND terminal_status IS NOT NULL)
    ),
    CONSTRAINT flow_journal_execution_terminal_shape_ck CHECK (
        event_class <> 'execution_terminal'
        OR terminal_status IN ('succeeded', 'failed', 'cancelled', 'expired')
    ),
    CONSTRAINT flow_journal_terminal_status_ck CHECK
        (terminal_status IS NULL OR terminal_status IN
            ('succeeded', 'failed', 'cancelled', 'expired', 'skipped'))
);

CREATE UNIQUE INDEX flow_journal_event_id_uq
    ON public.flow_journal (event_id)
    WHERE event_id IS NOT NULL;

-- User/worker event keys are reserved across versions of the same name.
CREATE UNIQUE INDEX flow_journal_application_event_key_uq
    ON public.flow_journal (execution_id, event_namespace, event_name, event_key)
    WHERE entry_kind = 'event_recorded'
      AND event_class = 'application'
      AND event_key IS NOT NULL;

CREATE UNIQUE INDEX flow_journal_command_created_uq
    ON public.flow_journal (command_id)
    WHERE entry_kind = 'command_created';

CREATE UNIQUE INDEX flow_journal_command_terminal_uq
    ON public.flow_journal (command_id)
    WHERE entry_kind = 'event_recorded' AND event_class = 'command_terminal';

CREATE UNIQUE INDEX flow_journal_execution_terminal_uq
    ON public.flow_journal (execution_id)
    WHERE entry_kind = 'event_recorded' AND event_class = 'execution_terminal';

CREATE UNIQUE INDEX flow_journal_execution_failing_uq
    ON public.flow_journal (execution_id)
    WHERE entry_kind = 'execution_failing';

CREATE UNIQUE INDEX flow_journal_plan_terminal_uq
    ON public.flow_journal (execution_id)
    WHERE entry_kind = 'event_recorded' AND event_class = 'plan_terminal';

CREATE UNIQUE INDEX flow_journal_coordinator_terminal_uq
    ON public.flow_journal (coordinator_id)
    WHERE entry_kind = 'event_recorded' AND event_class = 'coordinator_terminal';

CREATE UNIQUE INDEX flow_journal_attempt_started_uq
    ON public.flow_journal (attempt_id)
    WHERE entry_kind = 'attempt_started';

CREATE UNIQUE INDEX flow_journal_attempt_concluded_uq
    ON public.flow_journal (attempt_id)
    WHERE entry_kind = 'attempt_concluded';

CREATE INDEX flow_journal_event_lookup_idx
    ON public.flow_journal
       (execution_id, event_namespace, event_name, event_version, position)
    WHERE entry_kind = 'event_recorded';

CREATE INDEX flow_journal_command_events_idx
    ON public.flow_journal (command_id, position)
    WHERE command_id IS NOT NULL;
```

`body` is a versioned canonical record. `CommandCreated` includes arguments, declaration topology, accepted schedule/policy, classification, and origin even though current projections repeat those fields. `ExecutionStarted`, attempt entries, coordinator transitions, and runtime events likewise have explicit internal body schemas owned by `internal/store/journalcodec`.

There is no separate attempt table. A running command's current attempt identity, lease, and start time live in `flow_command_dispatch`; a coordinator's equivalent fields live in `flow_coordinators`. `AttemptStarted` and `AttemptConcluded` entries in `flow_journal` are the complete immutable history after an attempt stops being current. Command and coordinator rows hold their monotonic invocation and consumed-budget counters.

`event_namespace = 'command_success'` is the implementation detail behind `Command.Done`; failure/cancellation/expiry/skip use runtime terminal names and are exposed to a coordinator through `OnOutcome`, not by synthesizing a second event.

### 3.7 Plan reads

```sql
CREATE TABLE public.flow_plan_reads (
    execution_id       uuid NOT NULL
        REFERENCES public.flow_executions(execution_id) ON DELETE CASCADE,
    read_kind          text NOT NULL,
    command_key        text NOT NULL DEFAULT '',
    event_namespace    text NOT NULL DEFAULT '',
    event_name         text NOT NULL DEFAULT '',
    event_version      integer NOT NULL DEFAULT 0,
    availability       text NOT NULL,

    PRIMARY KEY
        (execution_id, read_kind, command_key,
         event_namespace, event_name, event_version),
    CONSTRAINT flow_plan_reads_kind_ck CHECK
        (read_kind IN ('fact', 'facts', 'children', 'result', 'outcome',
                       'plan_command', 'dependency')),
    CONSTRAINT flow_plan_reads_availability_ck CHECK
        (availability IN ('available', 'temporary', 'permanent', 'routing')),
    CONSTRAINT flow_plan_reads_selector_ck CHECK (
        (read_kind IN ('fact', 'facts')
            AND command_key = '' AND event_name <> '' AND event_version > 0)
        OR
        (read_kind NOT IN ('fact', 'facts')
            AND command_key <> '' AND event_name = '' AND event_version = 0)
    )
);

CREATE INDEX flow_plan_reads_event_route_idx
    ON public.flow_plan_reads
       (execution_id, event_namespace, event_name, event_version)
    WHERE read_kind IN ('fact', 'facts');

CREATE INDEX flow_plan_reads_command_route_idx
    ON public.flow_plan_reads (execution_id, command_key)
    WHERE read_kind IN ('children', 'result', 'outcome', 'plan_command', 'dependency');
```

The whole set is replaced after a successful evaluation. `temporary_plan_reads` is the count of public reads with `availability = 'temporary'`; implicit routing rows never affect completion.

### 3.8 Coordinators

```sql
CREATE TABLE public.flow_coordinators (
    coordinator_id        uuid PRIMARY KEY,
    execution_id          uuid NOT NULL UNIQUE
        REFERENCES public.flow_executions(execution_id) ON DELETE RESTRICT,
    name                  text NOT NULL,
    version               integer NOT NULL CHECK (version > 0),
    status                text NOT NULL DEFAULT 'active',

    state                 bytea NOT NULL,
    state_hash            bytea NOT NULL CHECK (octet_length(state_hash) = 32),
    state_revision        bigint NOT NULL DEFAULT 0 CHECK (state_revision >= 0),
    state_position        bigint NOT NULL CHECK (state_position >= 1),

    start_pending         boolean NOT NULL DEFAULT true,
    inbox_position        bigint NOT NULL DEFAULT 0 CHECK (inbox_position >= 0),
    delivery_key          text,
    delivery_position     bigint,
    delivery_state        text NOT NULL DEFAULT 'idle',

    retry_policy          jsonb NOT NULL,
    retry_policy_hash     bytea NOT NULL CHECK (octet_length(retry_policy_hash) = 32),
    budget_started_at     timestamptz,
    next_attempt_at       timestamptz,
    attempt_ordinal       integer NOT NULL DEFAULT 0 CHECK (attempt_ordinal >= 0),
    consumed_attempts     integer NOT NULL DEFAULT 0 CHECK (consumed_attempts >= 0),

    active_attempt_id     uuid,
    lease_token           uuid,
    lease_owner           text,
    lease_started_at      timestamptz,
    lease_expires_at      timestamptz,
    last_error            jsonb,

    created_at            timestamptz NOT NULL,
    updated_at            timestamptz NOT NULL,
    finished_at           timestamptz,

    CONSTRAINT flow_coordinators_status_ck CHECK
        (status IN ('active', 'completed', 'failed', 'cancelled')),
    CONSTRAINT flow_coordinators_delivery_state_ck CHECK
        (delivery_state IN ('idle', 'ready', 'retry_wait', 'running')),
    CONSTRAINT flow_coordinators_delivery_shape_ck CHECK (
        (delivery_state = 'idle' AND delivery_key IS NULL AND delivery_position IS NULL
            AND active_attempt_id IS NULL AND lease_token IS NULL)
        OR
        (delivery_state IN ('ready', 'retry_wait') AND delivery_key IS NOT NULL
            AND active_attempt_id IS NULL AND lease_token IS NULL)
        OR
        (delivery_state = 'running' AND delivery_key IS NOT NULL
            AND active_attempt_id IS NOT NULL AND lease_token IS NOT NULL
            AND lease_owner IS NOT NULL AND lease_started_at IS NOT NULL
            AND lease_expires_at IS NOT NULL)
    )
);

CREATE INDEX flow_coordinators_claim_idx
    ON public.flow_coordinators (name, version, next_attempt_at, coordinator_id)
    INCLUDE (execution_id, delivery_key, delivery_position)
    WHERE status = 'active' AND delivery_state IN ('ready', 'retry_wait');

CREATE INDEX flow_coordinators_lease_idx
    ON public.flow_coordinators (lease_expires_at, coordinator_id)
    WHERE status = 'active' AND delivery_state = 'running';

ALTER TABLE public.flow_executions
    ADD CONSTRAINT flow_executions_root_command_fk
    FOREIGN KEY (root_command_id) REFERENCES public.flow_commands(command_id)
    DEFERRABLE INITIALLY DEFERRED;
```

A coordinator inbox event is selected into `delivery_key = 'event/<position>'` before claim. Retry keeps that identity. Success advances `inbox_position`, clears delivery fields, and resets delivery retry counters. Start uses `delivery_key = 'start'` and clears `start_pending` on success.

### 3.9 Migration metadata

```sql
CREATE TABLE public.flow_schema_migrations (
    version            integer PRIMARY KEY,
    name               text NOT NULL,
    checksum           bytea NOT NULL CHECK (octet_length(checksum) = 32),
    library_version    text NOT NULL,
    min_reader_version integer NOT NULL,
    min_writer_version integer NOT NULL,
    applied_at         timestamptz NOT NULL,

    CONSTRAINT flow_schema_migrations_compatibility_ck CHECK (
        min_reader_version > 0
        AND min_writer_version > 0
        AND min_reader_version <= version
        AND min_writer_version <= version
    )
);
```

The highest applied `version` is the current schema version, and that row carries the active minimum reader/writer compatibility range. Keeping checksums and compatibility together makes one table the complete migration ledger; a separate compatibility singleton would duplicate the latest row's meaning.

## 4. Journal body contracts

Internal journal bodies are versioned separately from application definitions. The encoded records contain at least:

| Entry | Required body fields |
|---|---|
| `ExecutionStarted` | body schema version, driver name/version/mode, execution key, canonical input, explicit options, accepted deadline and command ceiling, metadata. |
| `CommandCreated` | body schema version, ID/key/name/version, canonical args, origin/parent, required and failure-scope classification, normalized dependency and wait selectors, explicit schedule declaration, accepted absolute first schedule, accepted retry/timeout/queue, causation. |
| `AttemptStarted` | subject identity, delivery key if coordinator, invocation ordinal, database start, worker/process, lease duration but not reusable lease token. |
| `AttemptConcluded` | subject, classification, consumed-budget flag, database finish, safe error, and persisted next-attempt time when any. |
| `EventRecorded` | canonical typed event payload; indexed columns hold its durable routing metadata. |
| `ExecutionBecameFailing` | triggering required command/event, fail-fast setting, and the survivor-set decision needed to explain cancellations. |
| `CoordinatorTransition` | handled activation/position, previous and new state revision, canonical resulting state, decision kind, and causation. |

Secrets such as lease tokens are never journaled. Worker/process identity is operational metadata and subject to configured redaction/size bounds.

## 5. Store transaction API

The engine and runtime call a narrow store API rather than composing SQL ad hoc:

```go
type Store interface {
    BeginSemantic(ctx context.Context, id ExecutionID, mode LockMode) (*SemanticTx, error)
    ProbeCommands(ctx context.Context, filter ClaimFilter, limit int) ([]Candidate, error)
    ProbeCoordinators(ctx context.Context, filter CoordFilter, limit int) ([]CoordCandidate, error)
    RenewCommandLeases(ctx context.Context, leases []LeaseRenewal) (LeaseRenewalResult, error)
    RenewCoordinatorLeases(ctx context.Context, leases []LeaseRenewal) (LeaseRenewalResult, error)
    Trace(ctx context.Context, id ExecutionID) (TraceRows, error)
    History(ctx context.Context, id ExecutionID, after uint64, limit int) ([]JournalRow, error)
}

type SemanticTx interface {
    DBNow() time.Time
    LoadSnapshot(context.Context, SnapshotMask) (SnapshotRows, error)
    TryLockCommand(context.Context, CommandID) (CommandRows, bool, error)
    TryLockCoordinator(context.Context, CoordinatorID) (CoordinatorRows, bool, error)
    Apply(context.Context, PersistedChangeSet) error
    Notify(context.Context, []WakeHint) error
    Commit(context.Context) error
    Rollback(context.Context) error
}
```

`BeginSemantic` locks the execution and then captures `DBNow`. Library-owned and caller-owned semantic paths use blocking mode; claim alone uses skip-locked mode. Capturing time after the lock prevents time spent waiting for that lock from making deadline or eligibility decisions against a stale timestamp. The transaction-scoped caller client enforces ascending execution-lock requests before the application-write phase. `Apply` validates expected prior states and affected-row counts; it does not accept partially validated engine changes.

The ascending-order check applies only when one caller-owned transaction touches more than one existing execution. Each ordinary one-execution operation uses the same API and requires no batch helper.

## 6. Core statement algorithms

### 6.1 Execution lock and position reservation

```sql
SELECT *
FROM public.flow_executions
WHERE execution_id = $1
FOR UPDATE;                 -- SKIP LOCKED only for the documented claim mode

SELECT clock_timestamp() AS db_now;

UPDATE public.flow_executions
SET next_journal_position = next_journal_position + $2,
    updated_at = $3
WHERE execution_id = $1
RETURNING next_journal_position - $2 AS first_position;
```

Reservation happens after the engine knows the exact journal batch. A rollback restores the counter and leaves no gap.

### 6.2 Idempotent start

Start first attempts the natural-key insert. On `flow_executions_idempotency_uq`, it loads the existing row and compares `start_fingerprint` plus canonical bytes before checking terminal status. An equivalent repeat returns it unchanged, including its accepted command ceiling. A conflict writes nothing.

New direct/plan/coordinator start inserts `ExecutionStarted` at position 1 and sets `next_journal_position` after the complete initial batch. Root or initial plan commands use the same batch insertion routine as later creation. Coordinator start stores initial state and `delivery_key = 'start'`/`delivery_state = 'ready'`.

### 6.3 Command candidate probe

Candidate discovery takes no lock and may return stale rows:

```sql
WITH t AS MATERIALIZED (
    SELECT clock_timestamp() AS db_now
),
registered(name, version, plan_name, plan_version) AS (
    SELECT * FROM unnest(
        $1::text[], $2::integer[], $3::text[], $4::integer[]
    )
)
SELECT d.execution_id, d.command_id, d.queue, d.next_run_at
FROM registered r
CROSS JOIN LATERAL (
    SELECT execution_id, command_id, queue, next_run_at
    FROM public.flow_command_dispatch, t
    WHERE name = r.name
      AND version = r.version
      AND plan_name = r.plan_name
      AND plan_version = r.plan_version
      AND state IN ('ready', 'retry_wait')
      AND next_run_at <= t.db_now
    ORDER BY next_run_at, queue, command_id
    LIMIT $5
) d
ORDER BY d.next_run_at, d.queue, d.command_id
LIMIT $6;
```

Registering a command pair makes that handler capable of every stored queue. Queue affects scheduling and concurrency, not handler compatibility. This is necessary because changing a command definition's queue default affects only new commands, while existing commands retain and must remain claimable on their accepted queue.

The runtime groups probes by execution. For a chosen execution it begins a skip-locked semantic transaction, then locks proposed commands/dispatch rows in `command_id` order with `FOR UPDATE SKIP LOCKED`, revalidates all predicates, and may claim up to immediately free capacity. It appends one `AttemptStarted` per claim and commits before handlers receive work.

### 6.4 Fenced command mutation

Every running-command conclusion includes:

```sql
... WHERE d.command_id = $1
      AND d.state = 'running'
      AND d.active_attempt_id = $2
      AND d.lease_token = $3
      AND d.lease_expires_at > $db_now
```

and a matching `flow_commands.state = 'running'` check. Zero rows aborts the transaction. Success or terminal failure deletes dispatch; retry changes it to `retry_wait`, clears lease fields, and writes the already chosen `next_run_at`.

### 6.5 Command creation batch

Under the execution lock:

1. query all existing keys in the proposed set `FOR UPDATE` in ID order;
2. compare declarations and separate new from equivalent;
3. update execution counters using a guarded predicate:

```sql
UPDATE public.flow_executions
SET command_count = command_count + $new_count,
    open_commands = open_commands + $new_open,
    updated_at = $db_now
WHERE execution_id = $id
  AND (max_commands = 0 OR command_count + $new_count <= max_commands)
RETURNING command_count;
```

4. require one returned row before inserting any proposal;
5. bulk insert commands in key order, dependency groups/members, waits, dispatch rows, and `CommandCreated` journal rows.

All statements share the transaction, so a later failure rolls the counter back. Equivalent proposals neither increment nor append.

### 6.6 Plan snapshot

One narrow command query plus normalized dependency/wait/read queries load the full plan state. Event bodies are loaded only for selectors the plan actually reads and memoized during evaluation. Terminal results come from commands; journal positions provide event metadata.

The snapshot queries run after the execution lock and after the triggering transition's in-transaction materializations are applied, so the plan sees the new terminal result, closed children, and emitted facts before commit.

### 6.7 Event insertion and idempotency

External `Publish` first looks up `(execution_id, namespace, name, key)` before rejecting a terminal execution. Equivalent canonical bytes and version return success; disagreement returns `ErrConflict`. A genuinely new event then requires a running execution and the execution lock. The transaction rechecks idempotency after acquiring that lock, so two concurrent first publications cannot turn a unique-index race into an ambiguous semantic error.

After the lock, the store loads matching unresolved wait rows and the current `flow_plan_reads_event_route_idx` entry. Wait satisfaction and readiness are generic topology changes. A wait with `Within` is updated only when the event transaction's captured `DBNow` is no later than `wait_deadline_at`; a later event is journaled but leaves the wait for expiry maintenance. Exact plan code is requested from the runtime only when the event also matches a current `fact`/`facts` plan read; if that capability is absent, the transaction rolls back before inserting the event. An `Await`-only publication therefore needs no plan registration, including for a late fact.

Worker `Emit` uses the same event-key constraint. Derived command and execution terminal events use their dedicated partial unique indexes, not caller event idempotency. Publishing a derived `Command.Done` descriptor through the public `Publish` path is rejected by descriptor validation.

### 6.8 Coordinator selection and claim

When an active coordinator is idle, selection first chooses `start` if pending. Otherwise it queries the lowest journal event above `inbox_position` matching any immutable registered selector. Ordinary `On` selectors use journal event namespace/name/version. `OnOutcome` selectors scan command-terminal rows in execution-position order and join `flow_journal.command_id` to the immutable command name/version projection. Both paths return candidate positions, and the lowest wins. This keeps one terminal journal row per command instead of adding a second outcome stream or denormalizing more routing columns into the journal.

Selection is persisted as one delivery key before or during the skip-locked claim transaction. Retry leaves it selected. On success, a guarded update requires matching coordinator ID, delivery key, attempt, token, and lease; advances inbox to the event position, clears delivery fields, resets delivery counters, and updates state/revision. Start success clears `start_pending` without moving inbox.

### 6.9 Maintenance

Every sweep first probes a bounded index and then enters the ordinary execution-first semantic path:

- expired command leases from `flow_command_dispatch_lease_idx`;
- expired coordinator leases from `flow_coordinators_lease_idx`;
- `wait_deadline_at` on pending commands;
- `deadline_at` on running/failing executions.

No due-retry or delayed-command state sweep exists: `next_run_at` becomes claimable naturally. Duplicate sweepers are safe because probes are stale hints and semantic locks/state predicates choose one winner.

## 7. Constraints not expressible locally

The store validates these in code under the execution lock and tests them as invariants:

- command and all dependency members share one execution;
- command projection and dispatch state agree;
- `flow_executions.command_count` equals accepted command rows;
- `flow_executions.open_commands` equals non-terminal command rows after each transition;
- every command's `created_position` identifies its `CommandCreated` entry;
- every terminal command's `terminal_position` identifies its sole terminal event;
- every running dispatch/coordinator `active_attempt_id` identifies exactly one earlier `AttemptStarted` entry for that same subject;
- every `AttemptConcluded` entry identifies exactly one earlier `AttemptStarted` in the same execution and for the same command or coordinator, with no other conclusion for that attempt;
- coordinator state position identifies `ExecutionStarted` or its latest `CoordinatorTransition`;
- journal causation remains inside the same execution;
- a parent marked child-membership-closed has a complete immutable child set.

Property and replay tests enforce these after arbitrary operation sequences.

## 8. Autovacuum, growth, and partitioning

Initial table settings prioritize the hot delivery rows:

```sql
ALTER TABLE public.flow_command_dispatch SET (
    fillfactor = 75,
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_threshold = 1000
);

ALTER TABLE public.flow_coordinators SET (
    fillfactor = 80,
    autovacuum_vacuum_scale_factor = 0.02
);

ALTER TABLE public.flow_commands SET (fillfactor = 90);
ALTER TABLE public.flow_journal SET (fillfactor = 100);
```

The exact values are benchmark outputs, not dogma. The migration records chosen values only after HOT-update, WAL, and dead-tuple measurements.

Partitioning is deferred. M1 retains terminal executions indefinitely and supports modest per-execution topology, so premature partitions complicate unique constraints and migrations. Journal archival is the first operational follow-on and the first likely reason to partition later, by time or hash while preserving execution-local reads.

## 9. Migrations and compatibility

Migration units are embedded, monotonically numbered, never rewritten after release, and each applied in its own transaction:

1. acquire `pg_advisory_xact_lock` derived from database identity and schema;
2. re-read migration state under the lock;
3. verify every applied checksum;
4. apply exactly one unit;
5. insert the migration row with its library version, checksum, and compatibility range;
6. commit.

Suggested initial units: migration ledger; executions; commands/dispatch; topology/waits; coordinators; journal/plan reads; indexes/storage settings. `New` calls compatibility check only. It never migrates implicitly.

Expand/migrate/contract governs rolling schema changes. The runtime refuses an unknown future writer version or an incompatible minimum reader/writer range with `ErrSchema`.

## 10. SQL error mapping

| Source | Public meaning |
|---|---|
| `flow_executions_idempotency_uq` | compare stored start identity -> existing or `ErrConflict` |
| `flow_commands_execution_key_uq` | compare declaration -> equivalent or `ErrConflict` |
| `flow_journal_application_event_key_uq` | compare event definition/bytes -> idempotent or `ErrConflict` |
| command/attempt-journal/terminal partial unique indexes | internal invariant violation unless resolving an ambiguous commit |
| known foreign key | `ErrNotFound` or `ErrInvalidState` by operation |
| known check constraint | `ErrInvalid` with safe field/reason |
| zero fenced rows | diagnostic read -> `ErrLeaseLost`, `ErrTerminal`, or `ErrNotFound` |
| SQLSTATE `40001`, `40P01` | internally retry only where authorized; otherwise transient wrapped error |
| SQLSTATE `55P03` | transient lock conflict for no-wait caller/claim path |
| connection loss at commit | uncertain commit; caller re-derives by stable identity |
| compatibility/checksum mismatch | `ErrSchema` |

The mapper never parses PostgreSQL message text and never includes SQL or canonical values in its public error.

## 11. Inspection queries

`History` is one bounded journal scan:

```sql
SELECT ...
FROM public.flow_journal
WHERE execution_id = $1 AND position > $2
ORDER BY position
LIMIT $3;
```

`Trace` runs a small fixed query set in one `REPEATABLE READ, READ ONLY` transaction: execution; commands with dispatch; dependencies/waits; the bounded journal range containing attempt history and events; coordinator. It does not execute N queries per command. Current running-attempt detail comes from dispatch/coordinator rows; concluded attempts come from journal entries. Live state may advance immediately after the snapshot, but the returned view is internally consistent.

`ListExecutions` uses stable `(created_at, execution_id)` cursor pagination and only the indexed filters defined in the functional spec. Metadata filtering is bounded JSON containment over the validated string map and uses `flow_executions_metadata_idx`; arbitrary JSONPath expressions are not exposed.

## 12. Test and benchmark plan

### 12.1 DDL and constraints

Every named check, foreign key, unique index, partial unique index, and terminal/lease shape has a direct rejection test. Migration tests cover clean install, repeated install, concurrent migrators, checksum mismatch, failed unit rollback, custom schema quoting, compatibility ranges, and upgrade from every released fixture.

### 12.2 Statement and concurrency tests

- allocation is contiguous; rollback reuses the abandoned range;
- concurrent appenders produce position order equal to commit order;
- a transaction blocked on the execution lock captures `DBNow` only after acquiring it, so elapsed deadlines cannot use pre-wait time;
- command creation batches increment count once and roll back wholly at the ceiling;
- candidate probes may be stale but claims never double-own work;
- command and coordinator claims append start history before returning;
- active attempt IDs always resolve to one matching start entry, and every conclusion pairs with one earlier start for the same subject without an attempt table;
- stale tokens cannot conclude or renew;
- retry changes next run only; interruption moves neither budget anchor nor consumed count;
- event idempotency is checked before terminal rejection;
- concurrent first publication rechecks idempotency under the execution lock, `Await`-only publication needs no plan capability, and facts on opposite sides of a persisted wait deadline resolve deterministically regardless of sweep timing;
- plan snapshots observe in-transaction trigger state;
- every maintenance runner is bounded and duplicate-safe;
- replay from journal equals settled projections after randomized operations.

### 12.3 Required query-plan benchmarks

Run `EXPLAIN (ANALYZE, BUFFERS, WAL)` at 10K, 1M, and 10M aggregate commands/journal rows with realistic ready/running/terminal distributions:

1. claim with 90% of the oldest lane backlog unregistered locally;
2. claim bursts from one 1,000-command execution and from many executions;
3. whole-plan snapshot at 10, 100, and 1,000 commands with 100 dependencies per node at the adversarial edge;
4. coordinator next-match lookup across mixed `On`/`OnOutcome` selectors with sparse subscriptions and long unmatched prefixes;
5. expired-wait maintenance with a large mostly-unexpired pending set;
6. history paging and full 1,000-command trace;
7. lease renewal at 500 active attempts;
8. journal insert/WAL growth at 1 KiB, 64 KiB, and maximum payload sizes;
9. HOT update and autovacuum behavior for dispatch/coordinator rows.

## 13. Acceptance conditions

- migration DDL implements every table and stable constraint above;
- no semantic or journal statement runs without the execution lock;
- claim uses only skip-locked execution-first locks and commits `AttemptStarted` before invocation;
- renewal writes no journal entry and changes no durable timing anchor;
- one complete journal source reconstructs command existence, topology, attempts, events, outcomes, and coordinator transitions;
- command ceiling and open count are maintained without table scans and verified after randomized transitions;
- materialization/dispatch invariants and exactly-one terminal constraints hold under concurrency and injected crashes;
- all integration tests and the required query-plan benchmarks pass with recorded evidence.
