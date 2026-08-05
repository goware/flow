CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE {{schema}}.flow_executions (
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

    next_journal_position bigint NOT NULL DEFAULT 1 CHECK (next_journal_position >= 1),
    root_command_id       uuid,
    failure               jsonb,

    created_at            timestamptz NOT NULL,
    updated_at            timestamptz NOT NULL,
    status_at             timestamptz NOT NULL,
    finished_at           timestamptz,

    CONSTRAINT flow_executions_driver_mode_ck CHECK
        (driver_mode IN ('direct', 'coordinator')),
    CONSTRAINT flow_executions_status_ck CHECK
        (status IN ('running', 'failing', 'succeeded', 'failed', 'cancelled', 'expired')),
    CONSTRAINT flow_executions_terminal_shape_ck CHECK (
        (status IN ('running', 'failing') AND finished_at IS NULL)
        OR
        (status IN ('succeeded', 'failed', 'cancelled', 'expired') AND finished_at IS NOT NULL)
    ),
    CONSTRAINT flow_executions_command_limit_ck CHECK
        (max_commands = 0 OR command_count <= max_commands)
);

CREATE UNIQUE INDEX flow_executions_idempotency_uq
    ON {{schema}}.flow_executions (driver_mode, definition_name, execution_key)
    WHERE execution_key <> '';

CREATE INDEX flow_executions_list_idx
    ON {{schema}}.flow_executions (definition_name, created_at DESC, execution_id DESC);

CREATE INDEX flow_executions_key_prefix_idx
    ON {{schema}}.flow_executions
       (definition_name, execution_key text_pattern_ops, created_at DESC, execution_id DESC);

CREATE INDEX flow_executions_status_idx
    ON {{schema}}.flow_executions (status, created_at DESC, execution_id DESC);

CREATE INDEX flow_executions_metadata_idx
    ON {{schema}}.flow_executions USING gin (metadata jsonb_path_ops);

CREATE INDEX flow_executions_deadline_idx
    ON {{schema}}.flow_executions (deadline_at, execution_id)
    WHERE status IN ('running', 'failing') AND deadline_at IS NOT NULL;

CREATE TABLE {{schema}}.flow_commands (
    command_id              uuid PRIMARY KEY,
    execution_id            uuid NOT NULL
        REFERENCES {{schema}}.flow_executions(execution_id) ON DELETE RESTRICT,
    command_key             text NOT NULL,

    name                    text NOT NULL,
    version                 integer NOT NULL CHECK (version > 0),
    origin                  text NOT NULL,
    parent_command_id       uuid REFERENCES {{schema}}.flow_commands(command_id) ON DELETE RESTRICT,
    required                boolean NOT NULL DEFAULT true,

    args                    bytea NOT NULL,
    args_hash               bytea NOT NULL CHECK (octet_length(args_hash) = 32),
    declaration_fingerprint bytea NOT NULL CHECK (octet_length(declaration_fingerprint) = 32),

    state                   text NOT NULL,
    unsatisfied_waits       integer NOT NULL DEFAULT 0 CHECK (unsatisfied_waits >= 0),

    queue                   text NOT NULL,
    attempt_timeout_ms      bigint CHECK (attempt_timeout_ms IS NULL OR attempt_timeout_ms > 0),
    retry_policy            jsonb NOT NULL,
    retry_policy_hash       bytea NOT NULL CHECK (octet_length(retry_policy_hash) = 32),

    initial_delay_ms        bigint CHECK (initial_delay_ms IS NULL OR initial_delay_ms > 0),
    budget_started_at       timestamptz,
    next_attempt_at         timestamptz,
    wait_started_at         timestamptz,
    wait_deadline_at        timestamptz,
    wait_timeout_ms         bigint CHECK (wait_timeout_ms IS NULL OR wait_timeout_ms > 0),

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
        (origin IN ('direct_root', 'worker_child', 'coordinator_command')),
    CONSTRAINT flow_commands_state_ck CHECK
        (state IN ('pending', 'ready', 'running', 'retry_wait',
                   'succeeded', 'failed', 'cancelled', 'expired')),
    CONSTRAINT flow_commands_parent_shape_ck CHECK (
        (origin = 'worker_child' AND parent_command_id IS NOT NULL)
        OR
        (origin <> 'worker_child' AND parent_command_id IS NULL)
    ),
    CONSTRAINT flow_commands_result_shape_ck CHECK (
        (state = 'succeeded' AND result IS NOT NULL AND result_hash IS NOT NULL)
        OR
        (state <> 'succeeded' AND result IS NULL AND result_hash IS NULL)
    ),
    CONSTRAINT flow_commands_pending_shape_ck CHECK (
        state <> 'pending' OR unsatisfied_waits > 0
    ),
    CONSTRAINT flow_commands_terminal_shape_ck CHECK (
        (state IN ('succeeded', 'failed', 'cancelled', 'expired')
            AND finished_at IS NOT NULL AND terminal_position IS NOT NULL)
        OR
        (state NOT IN ('succeeded', 'failed', 'cancelled', 'expired')
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
    ON {{schema}}.flow_commands (execution_id, command_key)
    INCLUDE (command_id, name, version, origin, parent_command_id, required,
             state, unsatisfied_waits, terminal_position);

CREATE INDEX flow_commands_parent_idx
    ON {{schema}}.flow_commands (parent_command_id, command_key)
    WHERE parent_command_id IS NOT NULL;

CREATE INDEX flow_commands_terminal_idx
    ON {{schema}}.flow_commands (execution_id, terminal_position)
    WHERE terminal_position IS NOT NULL;

CREATE INDEX flow_commands_wait_deadline_idx
    ON {{schema}}.flow_commands (wait_deadline_at, command_id)
    INCLUDE (execution_id)
    WHERE state = 'pending' AND wait_deadline_at IS NOT NULL;

CREATE TABLE {{schema}}.flow_command_queue (
    command_id        uuid PRIMARY KEY
        REFERENCES {{schema}}.flow_commands(command_id) ON DELETE CASCADE,
    execution_id      uuid NOT NULL
        REFERENCES {{schema}}.flow_executions(execution_id) ON DELETE CASCADE,

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
    ON {{schema}}.flow_command_queue
       (name, version, next_run_at, queue, command_id)
    INCLUDE (execution_id)
    WHERE state IN ('ready', 'retry_wait');

CREATE INDEX flow_command_queue_lease_idx
    ON {{schema}}.flow_command_queue (lease_expires_at, command_id)
    INCLUDE (execution_id, active_attempt_id, lease_token)
    WHERE state = 'running';

CREATE INDEX flow_command_queue_execution_idx
    ON {{schema}}.flow_command_queue (execution_id, command_id);

CREATE TABLE {{schema}}.flow_command_event_waits (
    command_id         uuid NOT NULL
        REFERENCES {{schema}}.flow_commands(command_id) ON DELETE CASCADE,
    execution_id       uuid NOT NULL
        REFERENCES {{schema}}.flow_executions(execution_id) ON DELETE CASCADE,
    event_name         text NOT NULL,
    event_key          text NOT NULL CHECK (event_key <> ''),
    satisfied_position bigint,

    PRIMARY KEY (command_id, event_name, event_key),
    CONSTRAINT flow_command_event_waits_position_ck CHECK
        (satisfied_position IS NULL OR satisfied_position >= 1)
);

CREATE INDEX flow_command_event_waits_reverse_idx
    ON {{schema}}.flow_command_event_waits
       (execution_id, event_name, event_key, command_id)
    WHERE satisfied_position IS NULL;

CREATE TABLE {{schema}}.flow_coordinators (
    coordinator_id        uuid PRIMARY KEY,
    execution_id          uuid NOT NULL UNIQUE
        REFERENCES {{schema}}.flow_executions(execution_id) ON DELETE RESTRICT,
    name                  text NOT NULL,
    version               integer NOT NULL CHECK (version > 0),
    status                text NOT NULL DEFAULT 'active',

    state                 bytea NOT NULL,
    state_hash            bytea NOT NULL CHECK (octet_length(state_hash) = 32),
    state_revision        bigint NOT NULL DEFAULT 0 CHECK (state_revision >= 0),
    state_position        bigint NOT NULL CHECK (state_position >= 1),

    start_pending         boolean NOT NULL DEFAULT true,
    inbox_position        bigint NOT NULL DEFAULT 0 CHECK (inbox_position >= 0),
    scan_position         bigint NOT NULL DEFAULT 0 CHECK (scan_position >= inbox_position),
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
    ON {{schema}}.flow_coordinators (name, version, next_attempt_at, coordinator_id)
    INCLUDE (execution_id, delivery_key, delivery_position)
    WHERE status = 'active' AND delivery_state IN ('ready', 'retry_wait');

CREATE INDEX flow_coordinators_idle_idx
    ON {{schema}}.flow_coordinators (name, version, coordinator_id)
    INCLUDE (execution_id, scan_position)
    WHERE status = 'active' AND delivery_state = 'idle';

CREATE INDEX flow_coordinators_lease_idx
    ON {{schema}}.flow_coordinators (lease_expires_at, coordinator_id)
    WHERE status = 'active' AND delivery_state = 'running';

ALTER TABLE {{schema}}.flow_executions
    ADD CONSTRAINT flow_executions_root_command_fk
    FOREIGN KEY (root_command_id) REFERENCES {{schema}}.flow_commands(command_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE {{schema}}.flow_journal (
    execution_id       uuid NOT NULL
        REFERENCES {{schema}}.flow_executions(execution_id) ON DELETE RESTRICT,
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
            AND event_name IS NOT NULL AND event_class IS NOT NULL)
        OR
        (entry_kind <> 'event_recorded'
            AND event_id IS NULL AND event_namespace IS NULL
            AND event_name IS NULL AND event_key IS NULL
            AND event_class IS NULL AND terminal_status IS NULL)
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
        (event_namespace IS NULL OR event_namespace IN ('application', 'runtime')),
    CONSTRAINT flow_journal_event_class_ck CHECK
        (event_class IS NULL OR event_class IN
            ('application', 'command_terminal', 'execution_terminal', 'coordinator_terminal')),
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
            ('succeeded', 'failed', 'cancelled', 'expired'))
);

CREATE UNIQUE INDEX flow_journal_event_id_uq
    ON {{schema}}.flow_journal (event_id)
    WHERE event_id IS NOT NULL;

CREATE UNIQUE INDEX flow_journal_application_event_key_uq
    ON {{schema}}.flow_journal (execution_id, event_namespace, event_name, event_key)
    WHERE entry_kind = 'event_recorded'
      AND event_class = 'application'
      AND event_key IS NOT NULL;

CREATE UNIQUE INDEX flow_journal_command_created_uq
    ON {{schema}}.flow_journal (command_id)
    WHERE entry_kind = 'command_created';

CREATE UNIQUE INDEX flow_journal_command_terminal_uq
    ON {{schema}}.flow_journal (command_id)
    WHERE entry_kind = 'event_recorded' AND event_class = 'command_terminal';

CREATE UNIQUE INDEX flow_journal_execution_terminal_uq
    ON {{schema}}.flow_journal (execution_id)
    WHERE entry_kind = 'event_recorded' AND event_class = 'execution_terminal';

CREATE UNIQUE INDEX flow_journal_execution_failing_uq
    ON {{schema}}.flow_journal (execution_id)
    WHERE entry_kind = 'execution_failing';

CREATE UNIQUE INDEX flow_journal_coordinator_terminal_uq
    ON {{schema}}.flow_journal (coordinator_id)
    WHERE entry_kind = 'event_recorded' AND event_class = 'coordinator_terminal';

CREATE UNIQUE INDEX flow_journal_attempt_started_uq
    ON {{schema}}.flow_journal (attempt_id)
    WHERE entry_kind = 'attempt_started';

CREATE UNIQUE INDEX flow_journal_attempt_concluded_uq
    ON {{schema}}.flow_journal (attempt_id)
    WHERE entry_kind = 'attempt_concluded';

CREATE INDEX flow_journal_event_lookup_idx
    ON {{schema}}.flow_journal
       (execution_id, event_namespace, event_name, event_key, position)
    WHERE entry_kind = 'event_recorded';

CREATE INDEX flow_journal_command_events_idx
    ON {{schema}}.flow_journal (command_id, position)
    WHERE command_id IS NOT NULL;

CREATE TABLE {{schema}}.flow_schema_migrations (
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
