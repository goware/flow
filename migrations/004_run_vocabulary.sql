-- Rename the live catalog from execution vocabulary to run vocabulary.
-- Historical journal entry kinds, event classes, and encoded bodies remain
-- byte-compatible and intentionally retain their released wire strings.

ALTER TABLE {{schema}}.flow_executions RENAME TO flow_runs;

ALTER TABLE {{schema}}.flow_runs RENAME COLUMN execution_id TO run_id;
ALTER TABLE {{schema}}.flow_runs RENAME COLUMN execution_key TO run_key;
ALTER TABLE {{schema}}.flow_commands RENAME COLUMN execution_id TO run_id;
ALTER TABLE {{schema}}.flow_command_queue RENAME COLUMN execution_id TO run_id;
ALTER TABLE {{schema}}.flow_command_event_waits RENAME COLUMN execution_id TO run_id;
ALTER TABLE {{schema}}.flow_journal RENAME COLUMN execution_id TO run_id;

DO $flow$
DECLARE
    item record;
BEGIN
    FOR item IN
        SELECT n.nspname AS schema_name, relation.relname AS relation_name,
               constraint_row.conname AS old_name,
               replace(constraint_row.conname, 'execution', 'run') AS new_name
        FROM pg_catalog.pg_constraint constraint_row
        JOIN pg_catalog.pg_class relation ON relation.oid = constraint_row.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = relation.relnamespace
        WHERE n.oid = (
            SELECT relnamespace
            FROM pg_catalog.pg_class
            WHERE oid = '{{schema}}.flow_runs'::regclass
        )
          AND relation.relname LIKE 'flow\_%' ESCAPE '\'
          AND constraint_row.conname LIKE '%execution%'
        ORDER BY relation.relname, constraint_row.conname
    LOOP
        EXECUTE format(
            'ALTER TABLE %I.%I RENAME CONSTRAINT %I TO %I',
            item.schema_name, item.relation_name, item.old_name, item.new_name
        );
    END LOOP;

    -- Constraint-backed indexes were renamed with their constraints. Rename
    -- every remaining standalone Flow index that still uses the old term.
    FOR item IN
        SELECT n.nspname AS schema_name, relation.relname AS old_name,
               replace(relation.relname, 'execution', 'run') AS new_name
        FROM pg_catalog.pg_class relation
        JOIN pg_catalog.pg_namespace n ON n.oid = relation.relnamespace
        WHERE n.oid = (
            SELECT relnamespace
            FROM pg_catalog.pg_class
            WHERE oid = '{{schema}}.flow_runs'::regclass
        )
          AND relation.relkind IN ('i', 'I')
          AND relation.relname LIKE 'flow\_%' ESCAPE '\'
          AND relation.relname LIKE '%execution%'
        ORDER BY relation.relname
    LOOP
        EXECUTE format(
            'ALTER INDEX %I.%I RENAME TO %I',
            item.schema_name, item.old_name, item.new_name
        );
    END LOOP;
END
$flow$;
