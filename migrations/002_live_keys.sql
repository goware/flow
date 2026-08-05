-- Live-scoped execution keys and delayed direct starts.
--
-- key_scope 'permanent' keeps the original identity semantics: one execution
-- ever per (driver_mode, definition_name, execution_key), idempotently
-- rediscovered by an equivalent start. key_scope 'live' scopes uniqueness to
-- non-terminal executions: the key is held while its execution runs and is
-- released when it settles, so a later start with the same key creates a new
-- execution. Queue-style producers use live keys as entity-scoped dedupe.

ALTER TABLE {{schema}}.flow_executions
    ADD COLUMN key_scope text NOT NULL DEFAULT 'permanent'
        CONSTRAINT flow_executions_key_scope_ck CHECK (key_scope IN ('permanent', 'live'));

DROP INDEX {{schema}}.flow_executions_idempotency_uq;

CREATE UNIQUE INDEX flow_executions_idempotency_uq
    ON {{schema}}.flow_executions (driver_mode, definition_name, execution_key)
    WHERE execution_key <> '' AND key_scope = 'permanent';

CREATE UNIQUE INDEX flow_executions_live_key_uq
    ON {{schema}}.flow_executions (driver_mode, definition_name, execution_key)
    WHERE execution_key <> '' AND key_scope = 'live' AND status IN ('running', 'failing');
