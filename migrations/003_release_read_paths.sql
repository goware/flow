-- Bounded release read paths and one defensive aggregate relationship.

CREATE INDEX flow_executions_key_lookup_idx
    ON {{schema}}.flow_executions
       (execution_key COLLATE "C", definition_name, created_at, execution_id);

CREATE INDEX flow_executions_created_idx
    ON {{schema}}.flow_executions (created_at DESC, execution_id DESC);

CREATE INDEX flow_command_queue_depth_idx
    ON {{schema}}.flow_command_queue (queue, state, next_run_at);

ALTER TABLE {{schema}}.flow_executions
    ADD CONSTRAINT flow_executions_open_commands_ck
    CHECK (open_commands <= command_count);
