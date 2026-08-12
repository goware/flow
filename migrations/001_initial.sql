CREATE SCHEMA IF NOT EXISTS {{schema}};

CREATE TABLE {{schema}}.flow_runs (
    run_id                uuid PRIMARY KEY,
    definition_name       text NOT NULL,
    definition_version    integer NOT NULL CHECK (definition_version > 0),
    run_key               text NOT NULL DEFAULT '',
    key_scope             text NOT NULL DEFAULT 'permanent',

    status                text NOT NULL DEFAULT 'running',

    start_fingerprint     bytea NOT NULL CHECK (octet_length(start_fingerprint) = 32),

    deadline_at           timestamptz,
    max_commands          integer NOT NULL CHECK (max_commands >= 0),
    command_count         integer NOT NULL DEFAULT 0 CHECK (command_count >= 0),
    open_commands         integer NOT NULL DEFAULT 0 CHECK (open_commands >= 0),

    next_journal_position bigint NOT NULL DEFAULT 1
        CONSTRAINT flow_runs_next_journal_position_ck CHECK (next_journal_position >= 1),
    root_command_id       uuid CONSTRAINT flow_runs_root_command_nn NOT NULL,
    failure               jsonb,

    created_at            timestamptz NOT NULL,
    updated_at            timestamptz NOT NULL,
    status_at             timestamptz NOT NULL,
    finished_at           timestamptz,

    CONSTRAINT flow_runs_status_ck CHECK
        (status IN ('running', 'failing', 'succeeded', 'failed', 'cancelled', 'expired')),
    CONSTRAINT flow_runs_terminal_shape_ck CHECK (
        (status IN ('running', 'failing') AND finished_at IS NULL)
        OR
        (status IN ('succeeded', 'failed', 'cancelled', 'expired') AND finished_at IS NOT NULL)
    ),
    CONSTRAINT flow_runs_command_limit_ck CHECK
        (max_commands = 0 OR command_count <= max_commands),
    CONSTRAINT flow_runs_open_commands_ck CHECK
        (open_commands <= command_count),
    CONSTRAINT flow_runs_key_scope_ck CHECK
        (key_scope IN ('permanent', 'live'))
);

CREATE UNIQUE INDEX flow_runs_idempotency_uq
    ON {{schema}}.flow_runs (definition_name, run_key)
    WHERE run_key <> '' AND key_scope = 'permanent';

CREATE UNIQUE INDEX flow_runs_live_key_uq
    ON {{schema}}.flow_runs (definition_name, run_key)
    WHERE run_key <> '' AND key_scope = 'live'
      AND status IN ('running', 'failing');

CREATE INDEX flow_runs_list_idx
    ON {{schema}}.flow_runs (definition_name, created_at DESC, run_id DESC);

CREATE INDEX flow_runs_key_prefix_idx
    ON {{schema}}.flow_runs
       (definition_name, run_key text_pattern_ops);

CREATE INDEX flow_runs_key_lookup_idx
    ON {{schema}}.flow_runs
       (run_key COLLATE "C", definition_name, created_at, run_id);

CREATE INDEX flow_runs_created_idx
    ON {{schema}}.flow_runs (created_at DESC, run_id DESC);

CREATE INDEX flow_runs_status_idx
    ON {{schema}}.flow_runs (status, created_at DESC, run_id DESC);

CREATE INDEX flow_runs_deadline_idx
    ON {{schema}}.flow_runs (deadline_at, run_id)
    WHERE status IN ('running', 'failing') AND deadline_at IS NOT NULL;

CREATE INDEX flow_runs_prune_idx
    ON {{schema}}.flow_runs (finished_at, run_id)
    WHERE finished_at IS NOT NULL
      AND (run_key = '' OR key_scope = 'live');

CREATE TABLE {{schema}}.flow_commands (
    command_id              uuid PRIMARY KEY,
    run_id                  uuid NOT NULL
        REFERENCES {{schema}}.flow_runs(run_id) ON DELETE RESTRICT,
    command_key             text NOT NULL,

    name                    text NOT NULL,
    version                 integer NOT NULL CHECK (version > 0),
    parent_command_id       uuid,

    args                    bytea NOT NULL,
    declaration_fingerprint bytea NOT NULL CHECK (octet_length(declaration_fingerprint) = 32),

    state                   text NOT NULL,
    unsatisfied_waits       integer NOT NULL DEFAULT 0 CHECK (unsatisfied_waits >= 0),

    queue                   text NOT NULL,
    attempt_timeout_ms      bigint CHECK (attempt_timeout_ms IS NULL OR attempt_timeout_ms > 0),
    recovery_lease_ms       bigint CHECK (recovery_lease_ms IS NULL OR recovery_lease_ms >= 30),
    retry_policy            bytea NOT NULL,

    initial_delay_ms        bigint CHECK (initial_delay_ms IS NULL OR initial_delay_ms > 0),
    budget_started_at       timestamptz,
    next_attempt_at         timestamptz,
    wait_started_at         timestamptz,
    wait_deadline_at        timestamptz,
    wait_timeout_ms         bigint CHECK (wait_timeout_ms IS NULL OR wait_timeout_ms > 0),

    attempt_ordinal         integer NOT NULL DEFAULT 0 CHECK (attempt_ordinal >= 0),
    consumed_attempts       integer NOT NULL DEFAULT 0 CHECK (consumed_attempts >= 0),

    result                  bytea,
    last_error              jsonb,
    terminal_failure       jsonb,
    terminal_position       bigint,

    created_position        bigint NOT NULL
        CONSTRAINT flow_commands_created_position_ck CHECK (created_position >= 1),
    created_at              timestamptz NOT NULL,
    updated_at              timestamptz NOT NULL,
    status_at               timestamptz NOT NULL,
    finished_at             timestamptz,

    CONSTRAINT flow_commands_run_key_uq UNIQUE (run_id, command_key),
    CONSTRAINT flow_commands_run_command_uq UNIQUE (run_id, command_id),
    CONSTRAINT flow_commands_parent_run_fk
        FOREIGN KEY (run_id, parent_command_id)
        REFERENCES {{schema}}.flow_commands(run_id, command_id) ON DELETE RESTRICT,
    CONSTRAINT flow_commands_state_ck CHECK
        (state IN ('pending', 'ready', 'running', 'retry_wait',
                   'succeeded', 'failed', 'cancelled', 'expired')),
    CONSTRAINT flow_commands_result_shape_ck CHECK (
        (state = 'succeeded' AND result IS NOT NULL)
        OR
        (state <> 'succeeded' AND result IS NULL)
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
    CONSTRAINT flow_commands_terminal_position_ck CHECK
        (terminal_position IS NULL OR terminal_position >= 1),
    CONSTRAINT flow_commands_attempt_counts_ck CHECK (consumed_attempts <= attempt_ordinal)
);

CREATE INDEX flow_commands_parent_idx
    ON {{schema}}.flow_commands (parent_command_id)
    WHERE parent_command_id IS NOT NULL;

CREATE INDEX flow_commands_wait_deadline_idx
    ON {{schema}}.flow_commands (wait_deadline_at, command_id)
    INCLUDE (run_id)
    WHERE state = 'pending' AND wait_deadline_at IS NOT NULL;

CREATE TABLE {{schema}}.flow_command_queue (
    command_id        uuid PRIMARY KEY,
    run_id            uuid NOT NULL
        REFERENCES {{schema}}.flow_runs(run_id) ON DELETE CASCADE,

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

    CONSTRAINT flow_command_queue_command_run_fk
        FOREIGN KEY (run_id, command_id)
        REFERENCES {{schema}}.flow_commands(run_id, command_id) ON DELETE CASCADE,
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
    INCLUDE (run_id)
    WHERE state IN ('ready', 'retry_wait');

CREATE INDEX flow_command_queue_lease_idx
    ON {{schema}}.flow_command_queue (lease_expires_at, command_id)
    INCLUDE (run_id, active_attempt_id, lease_token)
    WHERE state = 'running';

CREATE INDEX flow_command_queue_run_idx
    ON {{schema}}.flow_command_queue (run_id, command_id);

CREATE INDEX flow_command_queue_stats_idx
    ON {{schema}}.flow_command_queue (queue, state, next_run_at);

CREATE TABLE {{schema}}.flow_command_event_waits (
    command_id         uuid NOT NULL,
    run_id             uuid NOT NULL
        REFERENCES {{schema}}.flow_runs(run_id) ON DELETE CASCADE,
    event_name         text NOT NULL,
    event_key          text NOT NULL CHECK (event_key <> ''),
    satisfied_position bigint,

    PRIMARY KEY (command_id, event_name, event_key),
    CONSTRAINT flow_command_event_waits_command_run_fk
        FOREIGN KEY (run_id, command_id)
        REFERENCES {{schema}}.flow_commands(run_id, command_id) ON DELETE CASCADE,
    CONSTRAINT flow_command_event_waits_position_ck CHECK
        (satisfied_position IS NULL OR satisfied_position >= 1)
);

CREATE INDEX flow_command_event_waits_reverse_idx
    ON {{schema}}.flow_command_event_waits
       (run_id, event_name, event_key, command_id)
    WHERE satisfied_position IS NULL;

ALTER TABLE {{schema}}.flow_runs
    ADD CONSTRAINT flow_runs_root_command_fk
    FOREIGN KEY (run_id, root_command_id)
    REFERENCES {{schema}}.flow_commands(run_id, command_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE {{schema}}.flow_journal (
    run_id             uuid NOT NULL
        REFERENCES {{schema}}.flow_runs(run_id) ON DELETE RESTRICT,
    position           bigint NOT NULL
        CONSTRAINT flow_journal_position_ck CHECK (position >= 1),
    entry_id           uuid NOT NULL,
    entry_kind         text NOT NULL,
    recorded_at        timestamptz NOT NULL,
    causation_position bigint,

    command_id         uuid,
    attempt_id         uuid,
    event_id           uuid,
    event_namespace    text,
    event_name         text,
    event_key          text,
    event_class        text,
    terminal_status    text,

    body               bytea NOT NULL,
    body_hash          bytea NOT NULL CHECK (octet_length(body_hash) = 32),

    PRIMARY KEY (run_id, position),
    CONSTRAINT flow_journal_command_run_fk
        FOREIGN KEY (run_id, command_id)
        REFERENCES {{schema}}.flow_commands(run_id, command_id) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT flow_journal_position_causation_ck CHECK
        (causation_position IS NULL OR (causation_position >= 1 AND causation_position < position)),
    CONSTRAINT flow_journal_entry_kind_ck CHECK
        (entry_kind IN ('run_started', 'run_failing', 'command_created',
                        'attempt_started', 'attempt_concluded',
                        'event_recorded')),
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
                AND attempt_id IS NOT NULL AND command_id IS NOT NULL)
            OR
            (entry_kind NOT IN ('attempt_started', 'attempt_concluded')
                AND attempt_id IS NULL)
        )
    ),
    CONSTRAINT flow_journal_event_namespace_ck CHECK
        (event_namespace IS NULL OR event_namespace IN ('application', 'runtime')),
    CONSTRAINT flow_journal_event_class_ck CHECK
        (event_class IS NULL OR event_class IN
            ('application', 'command_terminal', 'run_terminal')),
    CONSTRAINT flow_journal_application_event_key_ck CHECK (
        event_class IS DISTINCT FROM 'application'
        OR (event_key IS NOT NULL AND event_key <> '')
    ),
    CONSTRAINT flow_journal_command_terminal_shape_ck CHECK (
        event_class <> 'command_terminal'
        OR (command_id IS NOT NULL AND terminal_status IS NOT NULL)
    ),
    CONSTRAINT flow_journal_run_terminal_shape_ck CHECK (
        event_class <> 'run_terminal'
        OR terminal_status IN ('succeeded', 'failed', 'cancelled', 'expired')
    ),
    CONSTRAINT flow_journal_terminal_status_ck CHECK
        (terminal_status IS NULL OR terminal_status IN
            ('succeeded', 'failed', 'cancelled', 'expired'))
);

CREATE UNIQUE INDEX flow_journal_application_event_key_uq
    ON {{schema}}.flow_journal (run_id, event_namespace, event_name, event_key)
    WHERE entry_kind = 'event_recorded'
      AND event_class = 'application'
      AND event_key IS NOT NULL;

CREATE UNIQUE INDEX flow_journal_command_created_uq
    ON {{schema}}.flow_journal (command_id)
    WHERE entry_kind = 'command_created';

CREATE UNIQUE INDEX flow_journal_command_terminal_uq
    ON {{schema}}.flow_journal (command_id)
    WHERE entry_kind = 'event_recorded' AND event_class = 'command_terminal';

CREATE UNIQUE INDEX flow_journal_run_terminal_uq
    ON {{schema}}.flow_journal (run_id)
    WHERE entry_kind = 'event_recorded' AND event_class = 'run_terminal';

CREATE UNIQUE INDEX flow_journal_run_failing_uq
    ON {{schema}}.flow_journal (run_id)
    WHERE entry_kind = 'run_failing';

CREATE UNIQUE INDEX flow_journal_attempt_kind_uq
    ON {{schema}}.flow_journal (attempt_id, entry_kind)
    WHERE entry_kind IN ('attempt_started', 'attempt_concluded');

CREATE INDEX flow_journal_command_events_idx
    ON {{schema}}.flow_journal (command_id)
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
