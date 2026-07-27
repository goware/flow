---
status: complete
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
6. Pending commands have no queue row. Ready, retry-wait, and running commands have exactly one queue row. Terminal commands have none.
7. Lease renewal touches only hot delivery rows and is intentionally absent from the journal.
8. Every statement is bounded by an execution, a claim batch, a maintenance batch, or a page limit.
9. Constraint and index names are stable because error mapping and migrations refer to them.
10. No correctness query depends on `LISTEN`, notification delivery, application time, or process-local state.

## 3. Schema

The DDL is the target shape for the initial migration. Small column-type changes discovered during implementation require an architecture amendment; semantic changes require the functional spec to reopen.

The initial physical inventory is nine tables:

| Table | Purpose |
|---|---|
| `flow_executions` | Execution identity, driver, lifecycle, counters, plan-reconciliation queue state, deadline, and journal-position allocator. |
| `flow_commands` | Durable command definitions, current states, accepted policies, results, and timing anchors. |
| `flow_command_queue` | Narrow hot queue and lease rows for runnable commands. |
| `flow_command_dependency_groups` | Durable join clauses attached to dependent commands. |
| `flow_command_dependency_members` | Predecessor membership for each dependency group. |
| `flow_command_event_waits` | Persisted `Await` selectors and their satisfying journal positions. |
| `flow_journal` | Immutable execution-local history for commands, attempts, events, plan decisions, outcomes, and coordinator transitions. |
| `flow_coordinators` | Coordinator state, inbox position, selected delivery, retry state, and lease. |
| `flow_schema_migrations` | Applied migration versions, checksums, library versions, and active reader/writer compatibility. |

There is intentionally no plan-read subscription table, queue-lane configuration table, event table, coordinator-inbox table, attempt table, or schema-compatibility table. A plan execution exposes one coalescing dirty marker on `flow_executions`; queue lanes are immutable columns on command/queue rows; events and attempt history live in `flow_journal`; the coordinator inbox cursor and selected delivery live in `flow_coordinators`; and the latest migration row carries the active compatibility range.

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

    plan_dirty            boolean NOT NULL DEFAULT false,
    plan_dirty_since      timestamptz,
    plan_quiescent        boolean NOT NULL DEFAULT false,
    plan_revision         bigint NOT NULL DEFAULT 0 CHECK (plan_revision >= 0),
    plan_waiting_count    integer NOT NULL DEFAULT 0 CHECK (plan_waiting_count >= 0),
    plan_waiting_on       jsonb NOT NULL DEFAULT '[]'::jsonb,

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
        (driver_mode = 'plan' AND jsonb_typeof(plan_waiting_on) = 'array'
            AND (plan_dirty = (plan_dirty_since IS NOT NULL))) OR
        (driver_mode <> 'plan' AND plan_dirty = false AND plan_dirty_since IS NULL
            AND plan_revision = 0
            AND plan_quiescent = false AND plan_waiting_count = 0
            AND plan_waiting_on = '[]'::jsonb)
    ),
    CONSTRAINT flow_executions_terminal_plan_ck CHECK (
        status IN ('running', 'failing') OR plan_dirty = false
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

CREATE INDEX flow_executions_plan_queue_idx
    ON public.flow_executions
       (definition_name, definition_version, plan_dirty_since, execution_id)
    WHERE driver_mode = 'plan'
      AND status IN ('running', 'failing')
      AND plan_dirty;
```

`start_fingerprint` covers definition version, canonical input, explicit deadline/fail-fast/metadata options, and driver mode. It deliberately excludes the runtime's accepted `max_commands` default and a direct root command's definition-level retry/timeout/queue defaults: an idempotent repeat returns the already accepted execution under changed deployment defaults. Metadata is duplicated as canonical bytes for identity and `jsonb` for the bounded `ListExecutions` containment filter; keys, values, and total size are validated before insertion.

`status = 'failing'` is the durable form of failure handling in progress. `plan_dirty` is the complete durable queue for pending pure-plan work; multiple triggers may coalesce behind it. `plan_dirty_since` is set only on the clean-to-dirty transition and is cleared with the bit, so repeated triggers do not push a hot execution to the back forever. `plan_quiescent` means the latest completed reconciliation reached a no-new-command pass. `plan_waiting_count` is exact; `plan_waiting_on` contains at most 32 safe summaries and may therefore be truncated. Both are inspection aids, not a routing or subscription set. Only dirty state, not those diagnostics, controls scheduling. Explicit execution cancellation or deadline expiry clears dirty work while terminalizing the execution.

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

### 3.3 Command queue

The hot queue row is separated from the wider command projection so lease churn and ready scans do not bloat immutable payload and topology pages.

```sql
CREATE TABLE public.flow_command_queue (
    command_id        uuid PRIMARY KEY
        REFERENCES public.flow_commands(command_id) ON DELETE CASCADE,
    execution_id      uuid NOT NULL
        REFERENCES public.flow_executions(execution_id) ON DELETE CASCADE,

    queue             text NOT NULL,
    name              text NOT NULL,
    version           integer NOT NULL CHECK (version > 0),
    state             text NOT NULL,
    next_run_at       timestamptz NOT NULL,

    active_attempt_id uuid,
    lease_token       uuid,
    lease_owner       text,
    lease_started_at  timestamptz,
    lease_expires_at  timestamptz,
    updated_at        timestamptz NOT NULL,

    CONSTRAINT flow_command_queue_state_ck CHECK
        (state IN ('ready', 'retry_wait', 'running')),
    CONSTRAINT flow_command_queue_lease_shape_ck CHECK (
        (state = 'running' AND active_attempt_id IS NOT NULL AND lease_token IS NOT NULL
         AND lease_owner IS NOT NULL AND lease_started_at IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (state <> 'running' AND active_attempt_id IS NULL AND lease_token IS NULL
         AND lease_owner IS NULL AND lease_started_at IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE INDEX flow_command_queue_claim_idx
    ON public.flow_command_queue
       (name, version, next_run_at, queue, command_id)
    INCLUDE (execution_id)
    WHERE state IN ('ready', 'retry_wait');

CREATE INDEX flow_command_queue_lease_idx
    ON public.flow_command_queue (lease_expires_at, command_id)
    INCLUDE (execution_id, active_attempt_id, lease_token)
    WHERE state = 'running';

CREATE INDEX flow_command_queue_execution_idx
    ON public.flow_command_queue (execution_id, command_id);
```

Denormalizing immutable `(queue, name, version)` is deliberate. It prevents a replica that handles a subset of command kinds from repeatedly scanning an unhandled head of a shared lane. Plan identity does not belong on the command queue because command workers settle independently of plan code. Store tests assert the denormalized values equal the owning command at insertion; they never change later.

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

Wait selectors match an exact event version, while application event idempotency reserves `(execution, event name, key)` across versions. An in-flight execution must therefore keep publishers capable of producing the version its waits declare; accepting another version under that natural key cannot satisfy the old wait and prevents a later conflicting republish.

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
    plan_revision      bigint,

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
                        'event_recorded', 'plan_reconciled', 'coordinator_transition')),
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
        AND (entry_kind <> 'plan_reconciled'
             OR (command_id IS NULL AND coordinator_id IS NULL))
        AND (event_class IS DISTINCT FROM 'coordinator_terminal' OR coordinator_id IS NOT NULL)
    ),
    CONSTRAINT flow_journal_plan_revision_shape_ck CHECK (
        (entry_kind = 'plan_reconciled' AND plan_revision IS NOT NULL AND plan_revision > 0)
        OR (entry_kind <> 'plan_reconciled' AND plan_revision IS NULL)
    ),
    CONSTRAINT flow_journal_event_namespace_ck CHECK
        (event_namespace IS NULL OR event_namespace IN ('application', 'command_success', 'runtime')),
    CONSTRAINT flow_journal_event_class_ck CHECK
        (event_class IS NULL OR event_class IN
            ('application', 'command_terminal', 'execution_terminal',
             'plan_terminal', 'coordinator_terminal')),
    CONSTRAINT flow_journal_application_event_key_ck CHECK (
        event_class IS DISTINCT FROM 'application'
        OR (event_key IS NOT NULL AND event_key <> '')
    ),
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

CREATE UNIQUE INDEX flow_journal_plan_reconciled_uq
    ON public.flow_journal (execution_id, plan_revision)
    WHERE entry_kind = 'plan_reconciled';

CREATE INDEX flow_journal_event_lookup_idx
    ON public.flow_journal
       (execution_id, event_namespace, event_name, event_version, position)
    WHERE entry_kind = 'event_recorded';

CREATE INDEX flow_journal_command_events_idx
    ON public.flow_journal (command_id, position)
    WHERE command_id IS NOT NULL;
```

`body` is a versioned canonical record. `CommandCreated` includes arguments, declaration topology, accepted schedule/policy, classification, and origin even though current projections repeat those fields. `ExecutionStarted`, attempt entries, coordinator transitions, and runtime events likewise have explicit internal body schemas owned by `internal/store/journalcodec`.

There is no separate attempt table. A running command's current attempt identity, lease, and start time live in `flow_command_queue`; a coordinator's equivalent fields live in `flow_coordinators`. `AttemptStarted` and `AttemptConcluded` entries in `flow_journal` are the complete immutable history after an attempt stops being current. Command and coordinator rows hold their monotonic invocation and consumed-budget counters.

`event_namespace = 'command_success'` is the implementation detail behind `Command.Done`; failure/cancellation/expiry/skip use runtime terminal names and are exposed to a coordinator through `OnOutcome`, not by synthesizing a second event.

There is deliberately no durable row per plan read. The plan scheduler reads a complete durable snapshot whenever `flow_executions.plan_dirty` is true, while the execution row retains only bounded waiting diagnostics from the latest reconciliation.

### 3.7 Coordinators

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

### 3.8 Migration metadata

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
| `EventRecorded` | canonical typed event payload; indexed columns hold its durable selector metadata. Explicit `Emit`/`Publish` events use the application-event size limit; command-success events use the command-result limit. |
| `PlanReconciled` | plan revision, highest prior journal position consumed, quiescence, waiting count and bounded summary, counts, and ordered new-command `(key, ID, declaration fingerprint)` tuples; no command payloads or topology. |
| `ExecutionBecameFailing` | triggering required command/event, fail-fast setting, and the survivor-set decision needed to explain cancellations. |
| `CoordinatorTransition` | handled activation/position, previous and new state revision, canonical resulting state, decision kind, and causation. |

Secrets such as lease tokens are never journaled. Worker/process identity is operational metadata and subject to configured redaction/size bounds.

`PlanReconciled` is the first entry in its reconciliation batch. Its causation points to `ExecutionStarted` or the latest previously committed journal position included in the snapshot, while its `consumed_through_position` body field records the complete prefix. Commands and immediate terminal transitions created by that decision point back to the `PlanReconciled` position. This records a decision once without creating one plan-delivery row per trigger.

## 5. Store transaction API

The engine and runtime call a narrow store API rather than composing SQL ad hoc:

```go
type Store interface {
    BeginSemantic(ctx context.Context, id ExecutionID, mode LockMode) (*SemanticTx, error)
    ProbeCommands(ctx context.Context, filter ClaimFilter, limit int) ([]Candidate, error)
    ProbePlans(ctx context.Context, filter PlanFilter, limit int) ([]PlanCandidate, error)
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

New direct/plan/coordinator start inserts `ExecutionStarted` at position 1 and sets `next_journal_position` after the complete initial batch. Direct start creates its root command in that batch. Plan start instead sets `plan_dirty = true`; the first plan scheduler claim records revision 1 and any initial declarations. Coordinator start stores initial state and `delivery_key = 'start'`/`delivery_state = 'ready'`.

### 6.3 Command candidate probe

Candidate discovery takes no lock and may return stale rows:

```sql
WITH t AS MATERIALIZED (
    SELECT clock_timestamp() AS db_now
),
registered(name, version) AS (
    SELECT * FROM unnest($1::text[], $2::integer[])
)
SELECT d.execution_id, d.command_id, d.queue, d.next_run_at
FROM registered r
CROSS JOIN LATERAL (
    SELECT execution_id, command_id, queue, next_run_at
    FROM public.flow_command_queue, t
    WHERE name = r.name
      AND version = r.version
      AND state IN ('ready', 'retry_wait')
      AND next_run_at <= t.db_now
    ORDER BY next_run_at, queue, command_id
    LIMIT $3
) d
ORDER BY d.next_run_at, d.queue, d.command_id
LIMIT $4;
```

Registering a command pair makes that handler capable of every stored queue. Queue affects scheduling and concurrency, not handler compatibility. This is necessary because changing a command definition's queue default affects only new commands, while existing commands retain and must remain claimable on their accepted queue.

The runtime groups probes by execution. For a chosen execution it begins a skip-locked semantic transaction, then locks proposed commands/queue rows in `command_id` order with `FOR UPDATE SKIP LOCKED`, revalidates all predicates, and may claim up to immediately free capacity. It appends one `AttemptStarted` per claim and commits before handlers receive work.

### 6.4 Dirty plan probe and reconciliation claim

Candidate discovery joins the runtime's exact registered plan pairs to the partial dirty index:

```sql
WITH registered(name, version) AS (
    SELECT * FROM unnest($1::text[], $2::integer[])
)
SELECT e.execution_id, e.definition_name, e.definition_version,
       e.plan_revision, e.plan_dirty_since
FROM registered r
CROSS JOIN LATERAL (
    SELECT execution_id, definition_name, definition_version,
           plan_revision, plan_dirty_since
    FROM public.flow_executions
    WHERE driver_mode = 'plan'
      AND definition_name = r.name
      AND definition_version = r.version
      AND status IN ('running', 'failing')
      AND plan_dirty
    ORDER BY plan_dirty_since, execution_id
    LIMIT $3
) e
ORDER BY e.plan_dirty_since, e.execution_id
LIMIT $4;
```

The candidate is stale by design. Reconciliation starts a semantic transaction with `FOR UPDATE SKIP LOCKED` on that execution and rechecks mode, status, plan pair, and `plan_dirty`. The pure plan runs while the row and connection are held; it must be CPU-bounded. The transaction appends `PlanReconciled` first, then any `CommandCreated` and immediate terminal entries caused by that revision, updates the plan fields, and clears dirty. Rollback leaves all prior triggers committed and dirty.

### 6.5 Fenced command mutation

Every running-command conclusion includes:

```sql
... WHERE d.command_id = $1
      AND d.state = 'running'
      AND d.active_attempt_id = $2
      AND d.lease_token = $3
      AND d.lease_expires_at > $db_now
```

and a matching `flow_commands.state = 'running'` check. Zero rows aborts the transaction. Success or terminal failure deletes the queue row; retry changes it to `retry_wait`, clears lease fields, and writes the already chosen `next_run_at`. A terminal command transition also sets `plan_dirty = true` when the owning execution is live and plan-driven; retry and lease-only transitions do not.

### 6.6 Command creation batch

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
5. bulk insert commands in key order, dependency groups/members, waits, queue rows, and `CommandCreated` journal rows.

All statements share the transaction, so a later failure rolls the counter back. Equivalent proposals neither increment nor append.

### 6.7 Plan snapshot

One narrow command query plus normalized dependency/wait queries load the structural plan state and capture the execution's journal high-water position. Provisional pure passes discover event selectors in memory; the store batch-loads exact matching slices through that fixed position with `flow_journal_event_lookup_idx`, including an explicit empty result for an absent selector. Event bodies are then decoded lazily by locator and memoized. Terminal results come from commands. No query loads or updates a persistent read/subscription set, and no pass scans unrelated event history.

The snapshot queries run after the dirty execution lock. Therefore the plan sees every event and terminal command outcome committed before that lock, including several triggers that coalesced behind one dirty bit.

### 6.8 Event insertion and idempotency

External `Publish` first looks up `(execution_id, namespace, name, key)` before rejecting a terminal execution. Equivalent canonical bytes and version return success; disagreement returns `ErrConflict`. A genuinely new event then requires a running execution and the execution lock. The transaction rechecks idempotency after acquiring that lock, so two concurrent first publications cannot turn a unique-index race into an ambiguous semantic error.

After the lock, the store loads matching unresolved wait rows. Wait satisfaction and readiness are generic topology changes. A wait with `Within` is updated only when the event transaction's captured `DBNow` is no later than `wait_deadline_at`; a later event is journaled but leaves the wait for expiry maintenance. Every newly accepted application event sets `plan_dirty = true` for a live plan-driven execution in the same transaction, regardless of what its last evaluation happened to read. No plan code or read-routing lookup is required.

Worker/coordinator `Emit` requires the same non-empty stable event key and uses the same cross-version uniqueness constraint as `Publish`. Equivalent repeated keys coalesce; disagreement rejects the whole staged decision. Derived command and execution terminal events use their dedicated partial unique indexes, not caller event idempotency. Publishing a derived `Command.Done` descriptor through the public `Publish` path is rejected by descriptor validation.

### 6.9 Coordinator selection and claim

When an active coordinator is idle, selection first chooses `start` if pending. Otherwise it queries the lowest journal event above `inbox_position` matching any immutable registered selector. Ordinary `On` selectors use journal event namespace/name/version. `OnOutcome` selectors scan command-terminal rows in execution-position order and join `flow_journal.command_id` to the immutable command name/version projection. Both paths return candidate positions, and the lowest wins. This keeps one terminal journal row per command instead of adding a second outcome stream or denormalizing more routing columns into the journal.

M1 does not preemptively add another command-terminal index. The sparse-`OnOutcome` query-plan benchmark determines whether repeated unmatched prefixes justify a partial `(execution_id, name, version, terminal_position)` index on terminal commands. Adding it is a physical optimization with measurable write cost, not a semantic requirement.

Selection is persisted as one delivery key before or during the skip-locked claim transaction. Retry leaves it selected. On success, a guarded update requires matching coordinator ID, delivery key, attempt, token, and lease; advances inbox to the event position, clears delivery fields, resets delivery counters, and updates state/revision. Start success clears `start_pending` without moving inbox.

### 6.10 Maintenance

Every sweep first probes a bounded index and then enters the ordinary execution-first semantic path:

- expired command leases from `flow_command_queue_lease_idx`;
- expired coordinator leases from `flow_coordinators_lease_idx`;
- `wait_deadline_at` on pending commands;
- `deadline_at` on running/failing executions.

No due-retry or delayed-command state sweep exists: `next_run_at` becomes claimable naturally. Duplicate sweepers are safe because probes are stale hints and semantic locks/state predicates choose one winner.

## 7. Constraints not expressible locally

The store validates these in code under the execution lock and tests them as invariants:

- command and all dependency members share one execution;
- command projection and queue state agree;
- `flow_executions.command_count` equals accepted command rows;
- `flow_executions.open_commands` equals non-terminal command rows after each transition;
- every command's `created_position` identifies its `CommandCreated` entry;
- every terminal command's `terminal_position` identifies its sole terminal event;
- every positive execution `plan_revision` has exactly one matching `PlanReconciled` entry, and revisions are contiguous within that execution;
- every running queue/coordinator `active_attempt_id` identifies exactly one earlier `AttemptStarted` entry for that same subject;
- every `AttemptConcluded` entry identifies exactly one earlier `AttemptStarted` in the same execution and for the same command or coordinator, with no other conclusion for that attempt;
- coordinator state position identifies `ExecutionStarted` or its latest `CoordinatorTransition`;
- journal causation remains inside the same execution;
- a parent marked child-membership-closed has a complete immutable child set.

Property and replay tests enforce these after arbitrary operation sequences.

## 8. Autovacuum, growth, and partitioning

Initial table settings prioritize the hot delivery rows:

```sql
ALTER TABLE public.flow_command_queue SET (
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

Suggested initial units: migration ledger; executions; commands/queue; topology/waits; coordinators; journal; indexes/storage settings. `New` calls compatibility check only. It never migrates implicitly.

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

`Trace` runs a small fixed query set in one `REPEATABLE READ, READ ONLY` transaction: execution including bounded plan diagnostics; commands with queue rows; dependencies/waits; the bounded journal range containing attempt history, plan revisions, and events; coordinator. It does not execute N queries per command. Current running-attempt detail comes from queue/coordinator rows; concluded attempts come from journal entries. Live state may advance immediately after the snapshot, but the returned view is internally consistent.

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
- application `Emit`/`Publish` keys are non-empty, equivalent content coalesces across retries, and a version or content change under the same natural key conflicts;
- exact-version waits are not satisfied by another version, including during rolling deployment;
- concurrent first publication rechecks idempotency under the execution lock, every plan-mode publication works without plan capability and sets dirty, and facts on opposite sides of a persisted wait deadline resolve deterministically regardless of sweep timing;
- concurrent plan triggers coalesce behind `plan_dirty`, a claimed reconciliation sees their complete committed snapshot, and rollback leaves the execution dirty;
- every `PlanReconciled` row has no command/coordinator subject and carries only the compact declaration-identity delta represented in full by same-batch `CommandCreated` entries;
- every maintenance runner is bounded and duplicate-safe;
- replay from journal equals settled projections after randomized operations.

### 12.3 Required query-plan benchmarks

Run `EXPLAIN (ANALYZE, BUFFERS, WAL)` at 10K, 1M, and 10M aggregate commands/journal rows with realistic ready/running/terminal distributions:

1. claim with 90% of the oldest lane backlog unregistered locally;
2. claim bursts from one 1,000-command execution and from many executions;
3. dirty-plan probe with many registered plan versions, coalescing under relevant and irrelevant event floods, plus whole-plan snapshot at 10, 100, and 1,000 commands with 100 dependencies per node at the adversarial edge;
4. repeated coordinator next-match lookup across mixed `On`/`OnOutcome` selectors with sparse subscriptions and long unmatched prefixes, comparing the base indexes with the candidate terminal-kind index from §6.9;
5. expired-wait maintenance with a large mostly-unexpired pending set;
6. history paging and full 1,000-command trace;
7. lease renewal at 500 active attempts;
8. journal insert/WAL growth at 1 KiB, 64 KiB, and maximum payload sizes;
9. HOT update and autovacuum behavior for queue/coordinator rows.

## 13. Acceptance conditions

- migration DDL implements every table and stable constraint above;
- no semantic or journal statement runs without the execution lock;
- claim uses only skip-locked execution-first locks and commits `AttemptStarted` before invocation;
- renewal writes no journal entry and changes no durable timing anchor;
- one complete journal source reconstructs command existence, topology, attempts, events, outcomes, and coordinator transitions;
- command ceiling and open count are maintained without table scans and verified after randomized transitions;
- materialization/queue invariants and exactly-one terminal constraints hold under concurrency and injected crashes;
- all integration tests and the required query-plan benchmarks pass with recorded evidence.
