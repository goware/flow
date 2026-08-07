package flow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"testing"

	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMigrateAndCheckSchema(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	option := WithSchema(database.Schema)
	if err := Migrate(ctx, database.DB, option); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if err := Migrate(ctx, database.DB, option); err != nil {
		t.Fatalf("idempotent Migrate() error = %v", err)
	}
	status, err := CheckSchema(ctx, database.DB, option)
	if err != nil {
		t.Fatalf("CheckSchema() error = %v", err)
	}
	if !status.Compatible || status.Schema != database.Schema || status.CurrentVersion != currentSchemaVersion ||
		status.MinReaderVersion != 1 || status.MinWriterVersion != 1 || status.AppliedAt.IsZero() {
		t.Fatalf("CheckSchema() = %#v", status)
	}

	var tables, indexes, migrations int
	if err := database.DB.Conn.QueryRow(ctx,
		`SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname=$1 AND tablename LIKE 'flow_%'`,
		database.Schema,
	).Scan(&tables); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if tables != 6 {
		t.Fatalf("Flow table count = %d, want 6", tables)
	}
	if err := database.DB.Conn.QueryRow(ctx,
		`SELECT count(*) FROM pg_catalog.pg_indexes WHERE schemaname=$1 AND indexname LIKE 'flow_%'`,
		database.Schema,
	).Scan(&indexes); err != nil {
		t.Fatalf("count indexes: %v", err)
	}
	if indexes != 28 {
		t.Fatalf("Flow index count = %d, want 28", indexes)
	}
	if err := database.DB.Conn.QueryRow(ctx,
		`SELECT count(*) FROM `+quoteIdentifier(database.Schema)+`.flow_schema_migrations`,
	).Scan(&migrations); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if migrations != len(migrationFiles) {
		t.Fatalf("migration count = %d, want %d", migrations, len(migrationFiles))
	}

	migrationFS, err := MigrationFS(option)
	if err != nil {
		t.Fatalf("MigrationFS() error = %v", err)
	}
	rendered, err := fs.ReadFile(migrationFS, "migrations/001_initial.sql")
	if err != nil {
		t.Fatalf("read rendered migration: %v", err)
	}
	if bytes.Contains(rendered, []byte(migrationToken)) || !bytes.Contains(rendered, []byte(quoteIdentifier(database.Schema)+`.flow_executions`)) {
		t.Fatal("MigrationFS did not safely render the configured schema")
	}
}

func TestMigrationPrunesAndNarrowsIndexes(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	removed := []string{
		"flow_commands_execution_idx",
		"flow_commands_terminal_idx",
		"flow_journal_entry_id_uq",
		"flow_journal_event_id_uq",
		"flow_journal_attempt_started_uq",
		"flow_journal_attempt_concluded_uq",
		"flow_journal_event_lookup_idx",
	}
	var remaining int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_indexes
		WHERE schemaname=$1 AND indexname=ANY($2::text[])`, database.Schema, removed).Scan(&remaining); err != nil {
		t.Fatalf("inspect removed indexes: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("redundant indexes remaining = %d, want 0", remaining)
	}

	type indexShape struct {
		name       string
		keyColumns int
		allColumns int
		unique     bool
	}
	want := []indexShape{
		{name: "flow_executions_key_prefix_idx", keyColumns: 2, allColumns: 2},
		{name: "flow_commands_execution_key_uq", keyColumns: 2, allColumns: 2, unique: true},
		{name: "flow_commands_parent_idx", keyColumns: 1, allColumns: 1},
		{name: "flow_journal_attempt_kind_uq", keyColumns: 2, allColumns: 2, unique: true},
		{name: "flow_journal_command_events_idx", keyColumns: 1, allColumns: 1},
	}
	for _, expected := range want {
		actual := indexShape{name: expected.name}
		err := database.DB.Conn.QueryRow(ctx, `SELECT i.indnkeyatts,i.indnatts,i.indisunique
			FROM pg_catalog.pg_index i
			JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid
			JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
			WHERE n.nspname=$1 AND c.relname=$2`, database.Schema, expected.name).
			Scan(&actual.keyColumns, &actual.allColumns, &actual.unique)
		if err != nil {
			t.Fatalf("inspect index %s: %v", expected.name, err)
		}
		if actual != expected {
			t.Fatalf("index %s shape = %#v, want %#v", expected.name, actual, expected)
		}
	}

	var commandKeyDefinition string
	var commandKeyColumns []string
	if err := database.DB.Conn.QueryRow(ctx, `SELECT pg_get_indexdef(i.indexrelid),
		ARRAY(SELECT pg_get_indexdef(i.indexrelid, position, true)
			FROM generate_series(1, i.indnkeyatts) position ORDER BY position)
		FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_class c ON c.oid=i.indexrelid
		JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
		WHERE n.nspname=$1 AND c.relname='flow_commands_execution_key_uq'`, database.Schema).
		Scan(&commandKeyDefinition, &commandKeyColumns); err != nil {
		t.Fatalf("inspect command-key index definition: %v", err)
	}
	if !strings.EqualFold(strings.Join(commandKeyColumns, ","), "execution_id,command_key") {
		t.Fatalf("command-key index columns = %v, want [execution_id command_key]", commandKeyColumns)
	}
	if strings.Contains(strings.ToUpper(commandKeyDefinition), "INCLUDE") {
		t.Fatalf("command-key index contains INCLUDE: %s", commandKeyDefinition)
	}

	var duplicateCommandKeyIndexes int
	if err := database.DB.Conn.QueryRow(ctx, `WITH command_indexes AS MATERIALIZED (
		SELECT i.indexrelid,i.indnkeyatts
		FROM pg_catalog.pg_index i
		JOIN pg_catalog.pg_class indexed ON indexed.oid=i.indrelid
		JOIN pg_catalog.pg_namespace n ON n.oid=indexed.relnamespace
		WHERE n.nspname=$1 AND indexed.relname='flow_commands'
	)
	SELECT count(*) FROM command_indexes i
		WHERE ARRAY(SELECT pg_get_indexdef(i.indexrelid, position, true)
			FROM generate_series(1, i.indnkeyatts) position ORDER BY position)
			= ARRAY['execution_id','command_key']`, database.Schema).Scan(&duplicateCommandKeyIndexes); err != nil {
		t.Fatalf("count command-key indexes: %v", err)
	}
	if duplicateCommandKeyIndexes != 1 {
		t.Fatalf("command-key index count = %d, want 1", duplicateCommandKeyIndexes)
	}

	var ownershipDefinition string
	if err := database.DB.Conn.QueryRow(ctx, `SELECT pg_get_constraintdef(oid)
		FROM pg_catalog.pg_constraint
		WHERE connamespace=$1::regnamespace AND conname='flow_commands_execution_command_uq'
		AND contype='u'`, database.Schema).Scan(&ownershipDefinition); err != nil {
		t.Fatalf("inspect command ownership key: %v", err)
	}
	if !strings.EqualFold(ownershipDefinition, "UNIQUE (execution_id, command_id)") {
		t.Fatalf("command ownership key = %s", ownershipDefinition)
	}
}

func TestSchemaCommandKeyQueryPlans(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, execution, stop := startHundredCommandExecution(t, database, ctx, "schema-command-key-plans")
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	filler := DefineCommand[None, None]("schema.command-key-plan.filler", 1)
	for index := range 900 {
		if _, err := filler.With(runtime).Execute(ctx, fmt.Sprintf("schema-command-key-filler/%03d", index), None{}, WithoutExecutionDeadline()); err != nil {
			t.Fatalf("create unrelated command %d: %v", index, err)
		}
	}
	stop()
	stopped = true

	schema := quoteIdentifier(database.Schema)
	if _, err := database.DB.Conn.Exec(ctx, `ANALYZE `+schema+`.flow_commands, `+schema+`.flow_command_queue`); err != nil {
		t.Fatalf("analyze command fixture: %v", err)
	}
	plans := []struct {
		name  string
		query string
		args  []any
	}{
		{
			name: "execution_order",
			query: `SELECT command_id,command_key,name,version,parent_command_id,required,state,
				unsatisfied_waits,terminal_position FROM ` + schema + `.flow_commands
				WHERE execution_id=$1 ORDER BY command_key`,
			args: []any{execution.ID},
		},
		{
			name: "child_key_conflict",
			query: `SELECT count(*) FROM ` + schema + `.flow_commands
				WHERE execution_id=$1 AND command_key=ANY($2)`,
			args: []any{execution.ID, []string{"work/010", "work/050", "work/090"}},
		},
		{
			name: "trace_queue_join",
			query: `SELECT c.command_id,c.state,c.unsatisfied_waits,c.budget_started_at,c.next_attempt_at,
				c.wait_started_at,c.wait_deadline_at,c.attempt_ordinal,c.consumed_attempts,c.last_error,
				c.created_at,c.updated_at,c.status_at,c.finished_at,q.state,q.lease_owner,q.lease_started_at,q.lease_expires_at
				FROM ` + schema + `.flow_commands c
				LEFT JOIN ` + schema + `.flow_command_queue q USING(command_id)
				WHERE c.execution_id=$1 ORDER BY c.command_key`,
			args: []any{execution.ID},
		},
	}
	explainPlans := func(indexShape string) {
		t.Helper()
		for _, plan := range plans {
			rows, err := database.DB.Conn.Query(ctx, `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) `+plan.query, plan.args...)
			if err != nil {
				t.Fatalf("explain %s with %s index: %v", plan.name, indexShape, err)
			}
			lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
			if err != nil {
				t.Fatalf("collect %s plan with %s index: %v", plan.name, indexShape, err)
			}
			if len(lines) == 0 {
				t.Fatalf("%s plan with %s index is empty", plan.name, indexShape)
			}
			t.Logf("%s plan with %s index:\n%s", plan.name, indexShape, strings.Join(lines, "\n"))
		}
	}
	var narrowBytes int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT pg_relation_size($1::regclass)`,
		database.Schema+`.flow_commands_execution_key_uq`).Scan(&narrowBytes); err != nil {
		t.Fatalf("measure narrow command-key index: %v", err)
	}
	explainPlans("narrow")

	if _, err := database.DB.Conn.Exec(ctx, `ALTER TABLE `+schema+`.flow_commands
		DROP CONSTRAINT flow_commands_execution_key_uq,
		ADD CONSTRAINT flow_commands_execution_key_uq UNIQUE (execution_id,command_key)
		INCLUDE (command_id,name,version,parent_command_id,required,state,unsatisfied_waits,terminal_position)`); err != nil {
		t.Fatalf("install legacy command-key index shape: %v", err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `ANALYZE `+schema+`.flow_commands`); err != nil {
		t.Fatalf("analyze legacy command-key index shape: %v", err)
	}
	var legacyBytes int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT pg_relation_size($1::regclass)`,
		database.Schema+`.flow_commands_execution_key_uq`).Scan(&legacyBytes); err != nil {
		t.Fatalf("measure legacy command-key index: %v", err)
	}
	explainPlans("legacy INCLUDE")
	t.Logf("command-key index bytes: narrow=%d legacy_include=%d", narrowBytes, legacyBytes)
}

func TestMigrationPrunesOnlyUnusedProjectionColumns(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	var pruned int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*)
		FROM information_schema.columns
		WHERE table_schema=$1 AND (
			(table_name='flow_executions' AND column_name IN ('input_hash','metadata_hash'))
			OR (table_name='flow_commands' AND column_name IN ('args_hash','retry_policy_hash','result_hash'))
			OR (table_name='flow_command_queue' AND column_name='updated_at')
		)`, database.Schema).Scan(&pruned); err != nil {
		t.Fatalf("inspect pruned columns: %v", err)
	}
	if pruned != 0 {
		t.Fatalf("unused projection columns remaining = %d, want 0", pruned)
	}

	var retained int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*)
		FROM information_schema.columns
		WHERE table_schema=$1 AND (
			(table_name='flow_executions' AND column_name IN ('input','metadata_canonical'))
			OR (table_name='flow_commands' AND column_name IN
				('declaration_fingerprint','result','last_error','terminal_failure'))
		)`, database.Schema).Scan(&retained); err != nil {
		t.Fatalf("inspect retained columns: %v", err)
	}
	if retained != 6 {
		t.Fatalf("retained semantic projection columns = %d, want 6", retained)
	}
}

func TestMigrationDurableTypesAndStateVocabularies(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	var dataType string
	if err := database.DB.Conn.QueryRow(ctx, `SELECT data_type FROM information_schema.columns
		WHERE table_schema=$1 AND table_name='flow_commands' AND column_name='retry_policy'`, database.Schema).Scan(&dataType); err != nil {
		t.Fatal(err)
	}
	if dataType != "bytea" {
		t.Fatalf("retry_policy type = %q, want bytea", dataType)
	}

	constraints := map[string][]string{
		"flow_executions_status_ck":       {string(ExecutionStatusRunning), string(ExecutionStatusFailing), string(ExecutionStatusSucceeded), string(ExecutionStatusFailed), string(ExecutionStatusCancelled), string(ExecutionStatusExpired)},
		"flow_commands_state_ck":          {string(CommandStatusPending), string(CommandStatusReady), string(CommandStatusRunning), string(CommandStatusRetryWait), string(CommandStatusSucceeded), string(CommandStatusFailed), string(CommandStatusCancelled), string(CommandStatusExpired)},
		"flow_command_queue_state_ck":     {string(QueueStateReady), string(QueueStateRetryWait), string(QueueStateRunning)},
		"flow_executions_key_scope_ck":    {string(KeyScopePermanent), string(KeyScopeLive)},
		"flow_journal_terminal_status_ck": {string(TerminalStatusSucceeded), string(TerminalStatusFailed), string(TerminalStatusCancelled), string(TerminalStatusExpired)},
	}
	for name, values := range constraints {
		var definition string
		if err := database.DB.Conn.QueryRow(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint
			WHERE connamespace=$1::regnamespace AND conname=$2`, database.Schema, name).Scan(&definition); err != nil {
			t.Fatalf("read constraint %s: %v", name, err)
		}
		for _, value := range values {
			if !strings.Contains(definition, "'"+value+"'") {
				t.Fatalf("constraint %s = %s; missing %q", name, definition, value)
			}
		}
	}
}

func TestMigrationOwnershipAndPositionConstraints(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("constraint.root", 1)
	first, err := command.With(runtime).Execute(ctx, "first", None{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := command.With(runtime).Execute(ctx, "second", None{})
	if err != nil {
		t.Fatal(err)
	}
	schema := quoteIdentifier(database.Schema)
	assertConstraint := func(name string, operation func(pgx.Tx) error) {
		t.Helper()
		tx, err := database.DB.Conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		err = operation(tx)
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.ConstraintName != name {
			t.Fatalf("constraint error = %v (%q), want %s", err, postgresConstraintName(postgresError), name)
		}
	}
	assertInvalidPosition := func(name string, operation func(pgx.Tx, int64) error) {
		t.Helper()
		for _, value := range []int64{0, -1} {
			assertConstraint(name, func(tx pgx.Tx) error { return operation(tx, value) })
		}
	}

	var rootNotNull bool
	if err := database.DB.Conn.QueryRow(ctx, `SELECT attribute.attnotnull
		FROM pg_attribute attribute
		JOIN pg_class relation ON relation.oid=attribute.attrelid
		JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
		WHERE namespace.nspname=$1 AND relation.relname='flow_executions'
		AND attribute.attname='root_command_id' AND NOT attribute.attisdropped`, database.Schema).Scan(&rootNotNull); err != nil {
		t.Fatal(err)
	}
	if !rootNotNull {
		t.Fatal("root_command_id is nullable")
	}
	tx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `UPDATE `+schema+`.flow_executions SET root_command_id=NULL WHERE execution_id=$1`, first.ID)
	_ = tx.Rollback(ctx)
	var notNullError *pgconn.PgError
	if !errors.As(err, &notNullError) || notNullError.ColumnName != "root_command_id" {
		t.Fatalf("root NOT NULL error = %v", err)
	}
	assertConstraint("flow_executions_root_command_fk", func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_executions SET root_command_id=$2 WHERE execution_id=$1`, first.ID, second.RootCommandID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `SET CONSTRAINTS `+schema+`.flow_executions_root_command_fk IMMEDIATE`)
		return err
	})
	assertConstraint("flow_commands_parent_execution_fk", func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_commands SET parent_command_id=$2 WHERE command_id=$1`, first.RootCommandID, second.RootCommandID)
		return err
	})
	assertConstraint("flow_command_queue_command_execution_fk", func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_command_queue SET execution_id=$2 WHERE command_id=$1`, first.RootCommandID, second.ID)
		return err
	})
	if _, err := database.DB.Conn.Exec(ctx, `INSERT INTO `+schema+`.flow_command_event_waits
		(command_id,execution_id,event_name,event_key) VALUES ($1,$2,'constraint.event','key')`, first.RootCommandID, first.ID); err != nil {
		t.Fatal(err)
	}
	assertConstraint("flow_command_event_waits_command_execution_fk", func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_command_event_waits SET execution_id=$2 WHERE command_id=$1`, first.RootCommandID, second.ID)
		return err
	})
	assertConstraint("flow_journal_command_execution_fk", func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_journal SET command_id=$3 WHERE execution_id=$1 AND position=$2`, first.ID, 1, second.RootCommandID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `SET CONSTRAINTS `+schema+`.flow_journal_command_execution_fk IMMEDIATE`)
		return err
	})
	assertInvalidPosition("flow_executions_next_journal_position_ck", func(tx pgx.Tx, value int64) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_executions SET next_journal_position=$2 WHERE execution_id=$1`, first.ID, value)
		return err
	})
	assertInvalidPosition("flow_commands_created_position_ck", func(tx pgx.Tx, value int64) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_commands SET created_position=$2 WHERE command_id=$1`, first.RootCommandID, value)
		return err
	})
	assertInvalidPosition("flow_command_event_waits_position_ck", func(tx pgx.Tx, value int64) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_command_event_waits SET satisfied_position=$2 WHERE command_id=$1`, first.RootCommandID, value)
		return err
	})
	assertInvalidPosition("flow_journal_position_ck", func(tx pgx.Tx, value int64) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_journal SET position=$2 WHERE execution_id=$1 AND position=1`, first.ID, value)
		return err
	})
	assertInvalidPosition("flow_commands_terminal_position_ck", func(tx pgx.Tx, value int64) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_commands
			SET state='succeeded',result='{}',terminal_position=$2,finished_at=clock_timestamp()
			WHERE command_id=$1`, first.RootCommandID, value)
		return err
	})
	assertInvalidPosition("flow_journal_position_causation_ck", func(tx pgx.Tx, value int64) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_journal SET causation_position=$2 WHERE execution_id=$1 AND position=2`, first.ID, value)
		return err
	})
}

func postgresConstraintName(err *pgconn.PgError) string {
	if err == nil {
		return ""
	}
	return err.ConstraintName
}

func TestMigrationChecksumMismatch(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	option := WithSchema(database.Schema)
	if err := Migrate(ctx, database.DB, option); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if _, err := database.DB.Conn.Exec(ctx,
		`UPDATE `+quoteIdentifier(database.Schema)+`.flow_schema_migrations SET checksum=decode(repeat('00',32),'hex') WHERE version=1`,
	); err != nil {
		t.Fatalf("alter checksum: %v", err)
	}
	if _, err := CheckSchema(ctx, database.DB, option); !errors.Is(err, ErrSchema) {
		t.Fatalf("CheckSchema() error = %v, want ErrSchema", err)
	}
	if err := Migrate(ctx, database.DB, option); !errors.Is(err, ErrSchema) {
		t.Fatalf("Migrate() error = %v, want ErrSchema", err)
	}
}

func TestMigrationCompatibilityMismatch(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	option := WithSchema(database.Schema)
	if err := Migrate(ctx, database.DB, option); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if _, err := database.DB.Conn.Exec(ctx,
		`ALTER TABLE `+quoteIdentifier(database.Schema)+`.flow_schema_migrations DROP CONSTRAINT flow_schema_migrations_compatibility_ck`,
	); err != nil {
		t.Fatalf("drop compatibility constraint: %v", err)
	}
	if _, err := database.DB.Conn.Exec(ctx,
		`UPDATE `+quoteIdentifier(database.Schema)+`.flow_schema_migrations SET min_writer_version=2 WHERE version=1`,
	); err != nil {
		t.Fatalf("alter compatibility: %v", err)
	}
	if _, err := CheckSchema(ctx, database.DB, option); !errors.Is(err, ErrSchema) {
		t.Fatalf("CheckSchema() error = %v, want ErrSchema", err)
	}
}

func TestConcurrentMigrate(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	option := WithSchema(database.Schema)
	errorsCh := make(chan error, 2)
	for range 2 {
		go func() { errorsCh <- Migrate(ctx, database.DB, option) }()
	}
	for range 2 {
		if err := <-errorsCh; err != nil {
			t.Fatalf("concurrent Migrate() error = %v", err)
		}
	}
	var count int
	if err := database.DB.Conn.QueryRow(ctx,
		`SELECT count(*) FROM `+quoteIdentifier(database.Schema)+`.flow_schema_migrations`,
	).Scan(&count); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if count != len(migrationFiles) {
		t.Fatalf("migration count = %d, want %d", count, len(migrationFiles))
	}
}

func TestMigrationFSAppliesCompatibleSchema(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	option := WithSchema(database.Schema)
	migrationFS, err := MigrationFS(option)
	if err != nil {
		t.Fatalf("MigrationFS() error = %v", err)
	}
	for _, file := range migrationFiles {
		rendered, err := fs.ReadFile(migrationFS, file.path)
		if err != nil {
			t.Fatalf("read migration %s: %v", file.path, err)
		}
		if _, err := database.DB.Conn.Exec(ctx, string(rendered)); err != nil {
			t.Fatalf("apply external migration %s: %v", file.path, err)
		}
	}
	if _, err := CheckSchema(ctx, database.DB, option); err != nil {
		t.Fatalf("CheckSchema() after external migration error = %v", err)
	}
	if err := Migrate(ctx, database.DB, option); err != nil {
		t.Fatalf("Migrate() after external migration error = %v", err)
	}
}

func TestMigrationValidation(t *testing.T) {
	t.Parallel()

	if err := Migrate(context.Background(), nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Migrate(nil) error = %v", err)
	}
	if _, err := CheckSchema(context.Background(), nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("CheckSchema(nil) error = %v", err)
	}
	for _, schema := range []string{"", "bad-name", "public;DROP SCHEMA public", strings.Repeat("x", 64)} {
		if _, err := MigrationFS(WithSchema(schema)); !errors.Is(err, ErrInvalid) {
			t.Fatalf("MigrationFS(%q) error = %v", schema, err)
		}
	}
	var nilOption MigrateOption
	if _, err := MigrationFS(nilOption); !errors.Is(err, ErrInvalid) {
		t.Fatalf("MigrationFS(nil) error = %v", err)
	}
}

func TestCheckSchemaMissing(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	if _, err := CheckSchema(context.Background(), database.DB, WithSchema(database.Schema)); !errors.Is(err, ErrSchema) {
		t.Fatalf("CheckSchema(missing) error = %v", err)
	}
}

func TestCheckSchemaDetectsMissingTable(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	option := WithSchema(database.Schema)
	if err := Migrate(ctx, database.DB, option); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `DROP TABLE `+quoteIdentifier(database.Schema)+`.flow_command_event_waits`); err != nil {
		t.Fatalf("drop Flow table: %v", err)
	}
	if _, err := CheckSchema(ctx, database.DB, option); !errors.Is(err, ErrSchema) {
		t.Fatalf("CheckSchema() error = %v, want ErrSchema", err)
	}
}

func TestCheckSchemaDetectsUnexpectedFlowTable(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	option := WithSchema(database.Schema)
	if err := Migrate(ctx, database.DB, option); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `CREATE TABLE `+quoteIdentifier(database.Schema)+`.flow_unexpected (id integer)`); err != nil {
		t.Fatalf("create unexpected Flow table: %v", err)
	}
	if _, err := CheckSchema(ctx, database.DB, option); !errors.Is(err, ErrSchema) {
		t.Fatalf("CheckSchema() error = %v, want ErrSchema", err)
	}
}
