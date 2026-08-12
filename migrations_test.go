package flow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"time"

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
	if indexes != 31 {
		t.Fatalf("Flow index count = %d, want 31", indexes)
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
	initial, err := fs.ReadFile(migrationFS, "migrations/001_initial.sql")
	if err != nil {
		t.Fatalf("read rendered initial migration: %v", err)
	}
	if bytes.Contains(initial, []byte(migrationToken)) || !bytes.Contains(initial, []byte(quoteIdentifier(database.Schema)+`.flow_runs`)) ||
		bytes.Contains(initial, []byte("flow_executions")) || bytes.Contains(initial, []byte("execution_id")) {
		t.Fatal("MigrationFS did not render the clean Run-named baseline")
	}
	if _, err := fs.ReadFile(migrationFS, "migrations/002_live_keys.sql"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("retired migration remains: %v", err)
	}
}

func TestCleanBaselineRequiresVersionOneReaderAndWriter(t *testing.T) {
	t.Parallel()
	if !schemaVersionsCompatible(1, 1, 1, 1, 1, 1) {
		t.Fatal("version-one reader/writer rejected the clean baseline")
	}
	if schemaVersionsCompatible(2, 1, 1, 1, 1, 1) {
		t.Fatal("different schema version was accepted")
	}
}

func TestMigrationReleaseReadPaths(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}

	type indexShape struct {
		name      string
		columns   []string
		unique    bool
		predicate bool
		collation *string
		opclass   *string
		contains  string
	}
	want := []indexShape{
		{name: "flow_runs_key_lookup_idx", columns: []string{"run_key", "definition_name", "created_at", "run_id"}, collation: ptr("C"), opclass: ptr("text_ops")},
		{name: "flow_runs_created_idx", columns: []string{"created_at", "run_id"}, contains: "(created_at DESC, run_id DESC)"},
		{name: "flow_command_queue_stats_idx", columns: []string{"queue", "state", "next_run_at"}},
	}
	for _, expected := range want {
		var columns []string
		var unique, predicate bool
		var collation, opclass *string
		var definition string
		err := database.DB.Conn.QueryRow(ctx, `WITH target AS MATERIALIZED (
			SELECT i.indexrelid,i.indnkeyatts,i.indisunique,i.indpred,i.indcollation[0] AS first_collation,
				i.indclass[0] AS first_opclass
			FROM pg_catalog.pg_index i
			JOIN pg_catalog.pg_class indexed ON indexed.oid=i.indrelid
			JOIN pg_catalog.pg_class index_relation ON index_relation.oid=i.indexrelid
			JOIN pg_catalog.pg_namespace n ON n.oid=indexed.relnamespace
			WHERE n.nspname=$1 AND index_relation.relname=$2
		)
		SELECT ARRAY(SELECT pg_get_indexdef(target.indexrelid, position, true)
				FROM generate_series(1, target.indnkeyatts) position ORDER BY position),
			target.indisunique, target.indpred IS NOT NULL, col.collname, opc.opcname,
			pg_get_indexdef(target.indexrelid)
		FROM target
		LEFT JOIN pg_catalog.pg_collation col ON col.oid=target.first_collation
		LEFT JOIN pg_catalog.pg_opclass opc ON opc.oid=target.first_opclass`, database.Schema, expected.name).
			Scan(&columns, &unique, &predicate, &collation, &opclass, &definition)
		if err != nil {
			t.Fatalf("inspect index %s: %v", expected.name, err)
		}
		if !slices.Equal(columns, expected.columns) || unique != expected.unique || predicate != expected.predicate {
			t.Fatalf("index %s = columns %v unique=%v predicate=%v, want %#v", expected.name, columns, unique, predicate, expected)
		}
		if expected.collation != nil && (collation == nil || *collation != *expected.collation) {
			t.Fatalf("index %s collation = %v, want %q", expected.name, collation, *expected.collation)
		}
		if expected.opclass != nil && (opclass == nil || *opclass != *expected.opclass) {
			t.Fatalf("index %s operator class = %v, want %q", expected.name, opclass, *expected.opclass)
		}
		if expected.contains != "" && !strings.Contains(definition, expected.contains) {
			t.Fatalf("index %s definition = %s, want substring %q", expected.name, definition, expected.contains)
		}
	}

	var prefixDefinition string
	if err := database.DB.Conn.QueryRow(ctx, `SELECT indexdef FROM pg_catalog.pg_indexes
		WHERE schemaname=$1 AND indexname='flow_runs_key_prefix_idx'`, database.Schema).Scan(&prefixDefinition); err != nil {
		t.Fatalf("inspect retained prefix index: %v", err)
	}
	if !strings.Contains(prefixDefinition, "(definition_name, run_key text_pattern_ops)") {
		t.Fatalf("retained prefix index = %s", prefixDefinition)
	}

	var checkDefinition string
	if err := database.DB.Conn.QueryRow(ctx, `SELECT pg_get_constraintdef(oid)
		FROM pg_catalog.pg_constraint
		WHERE connamespace=$1::regnamespace AND conname='flow_runs_open_commands_ck'`, database.Schema).
		Scan(&checkDefinition); err != nil {
		t.Fatalf("inspect open-command check: %v", err)
	}
	if !strings.Contains(checkDefinition, "open_commands <= command_count") {
		t.Fatalf("open-command check = %s", checkDefinition)
	}
}

func ptr[T any](value T) *T { return &value }

func TestMigrationMixedCaseSchemaIsIdempotent(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	schema := "FlowReleaseMixedCase"
	t.Cleanup(func() {
		_, _ = database.DB.Conn.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+quoteIdentifier(schema)+` CASCADE`)
	})
	option := WithSchema(schema)
	if err := Migrate(ctx, database.DB, option); err != nil {
		t.Fatalf("first Migrate() error = %v", err)
	}
	if err := Migrate(ctx, database.DB, option); err != nil {
		t.Fatalf("second Migrate() error = %v", err)
	}
	if _, err := CheckSchema(ctx, database.DB, option); err != nil {
		t.Fatalf("CheckSchema() error = %v", err)
	}
}

func TestVerifyAppliedMigrationsRequiresKnownPrefix(t *testing.T) {
	_, units, err := prepareMigrations()
	if err != nil {
		t.Fatal(err)
	}
	row := func(unit migrationUnit) appliedMigration {
		return appliedMigration{
			version: unit.version, name: unit.name, checksum: unit.checksum,
			minReader: unit.minReader, minWriter: unit.minWriter, appliedAt: time.Now(),
		}
	}
	tests := []struct {
		name    string
		applied map[int]appliedMigration
		wantErr bool
	}{
		{name: "empty", applied: map[int]appliedMigration{}},
		{name: "first", applied: map[int]appliedMigration{1: row(units[0])}},
		{name: "unknown future", applied: map[int]appliedMigration{1: row(units[0]), 2: {version: 2}}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := verifyAppliedMigrations(test.applied, units)
			if test.wantErr && !errors.Is(err, ErrSchema) {
				t.Fatalf("verifyAppliedMigrations() error = %v, want ErrSchema", err)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("verifyAppliedMigrations() error = %v", err)
			}
		})
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
		"flow_commands_run_idx",
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
		{name: "flow_runs_key_prefix_idx", keyColumns: 2, allColumns: 2},
		{name: "flow_commands_run_key_uq", keyColumns: 2, allColumns: 2, unique: true},
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
		WHERE n.nspname=$1 AND c.relname='flow_commands_run_key_uq'`, database.Schema).
		Scan(&commandKeyDefinition, &commandKeyColumns); err != nil {
		t.Fatalf("inspect command-key index definition: %v", err)
	}
	if !strings.EqualFold(strings.Join(commandKeyColumns, ","), "run_id,command_key") {
		t.Fatalf("command-key index columns = %v, want [run_id command_key]", commandKeyColumns)
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
			= ARRAY['run_id','command_key']`, database.Schema).Scan(&duplicateCommandKeyIndexes); err != nil {
		t.Fatalf("count command-key indexes: %v", err)
	}
	if duplicateCommandKeyIndexes != 1 {
		t.Fatalf("command-key index count = %d, want 1", duplicateCommandKeyIndexes)
	}

	var ownershipDefinition string
	if err := database.DB.Conn.QueryRow(ctx, `SELECT pg_get_constraintdef(oid)
		FROM pg_catalog.pg_constraint
		WHERE connamespace=$1::regnamespace AND conname='flow_commands_run_command_uq'
		AND contype='u'`, database.Schema).Scan(&ownershipDefinition); err != nil {
		t.Fatalf("inspect command ownership key: %v", err)
	}
	if !strings.EqualFold(ownershipDefinition, "UNIQUE (run_id, command_id)") {
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
	runtime, run, stop := startHundredCommandRun(t, database, ctx, "schema-command-key-plans")
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	filler := DefineCommand[None, None]("schema.command-key-plan.filler", 1)
	for index := range 900 {
		if _, err := filler.Enqueue(ctx, runtime, fmt.Sprintf("schema-command-key-filler/%03d", index), None{}, WithoutRunDeadline()); err != nil {
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
			name: "run_order",
			query: `SELECT command_id,command_key,name,version,parent_command_id,state,
				unsatisfied_waits,terminal_position FROM ` + schema + `.flow_commands
				WHERE run_id=$1 ORDER BY command_key`,
			args: []any{run.ID},
		},
		{
			name: "child_key_conflict",
			query: `SELECT count(*) FROM ` + schema + `.flow_commands
				WHERE run_id=$1 AND command_key=ANY($2)`,
			args: []any{run.ID, []string{"work/010", "work/050", "work/090"}},
		},
		{
			name: "trace_queue_join",
			query: `SELECT c.command_id,c.state,c.unsatisfied_waits,c.budget_started_at,c.next_attempt_at,
				c.wait_started_at,c.wait_deadline_at,c.attempt_ordinal,c.consumed_attempts,c.last_error,
				c.created_at,c.updated_at,c.status_at,c.finished_at,q.state,q.lease_owner,q.lease_started_at,q.lease_expires_at
				FROM ` + schema + `.flow_commands c
				LEFT JOIN ` + schema + `.flow_command_queue q USING(command_id)
				WHERE c.run_id=$1 ORDER BY c.command_key`,
			args: []any{run.ID},
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
		database.Schema+`.flow_commands_run_key_uq`).Scan(&narrowBytes); err != nil {
		t.Fatalf("measure narrow command-key index: %v", err)
	}
	explainPlans("narrow")

	if _, err := database.DB.Conn.Exec(ctx, `ALTER TABLE `+schema+`.flow_commands
		DROP CONSTRAINT flow_commands_run_key_uq,
		ADD CONSTRAINT flow_commands_run_key_uq UNIQUE (run_id,command_key)
		INCLUDE (command_id,name,version,parent_command_id,state,unsatisfied_waits,terminal_position)`); err != nil {
		t.Fatalf("install legacy command-key index shape: %v", err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `ANALYZE `+schema+`.flow_commands`); err != nil {
		t.Fatalf("analyze legacy command-key index shape: %v", err)
	}
	var legacyBytes int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT pg_relation_size($1::regclass)`,
		database.Schema+`.flow_commands_run_key_uq`).Scan(&legacyBytes); err != nil {
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
			(table_name='flow_runs' AND column_name IN
				('input','input_hash','metadata','metadata_canonical','metadata_hash','fail_fast'))
			OR (table_name='flow_commands' AND column_name='required')
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
		WHERE table_schema=$1 AND table_name='flow_commands' AND column_name IN
				('declaration_fingerprint','result','last_error','terminal_failure','recovery_lease_ms')
		`, database.Schema).Scan(&retained); err != nil {
		t.Fatalf("inspect retained columns: %v", err)
	}
	if retained != 5 {
		t.Fatalf("retained semantic projection columns = %d, want 5", retained)
	}
	var nullable string
	var dataType string
	if err := database.DB.Conn.QueryRow(ctx, `SELECT is_nullable,data_type FROM information_schema.columns
		WHERE table_schema=$1 AND table_name='flow_commands' AND column_name='recovery_lease_ms'`, database.Schema).
		Scan(&nullable, &dataType); err != nil {
		t.Fatal(err)
	}
	if nullable != "YES" || dataType != "bigint" {
		t.Fatalf("recovery_lease_ms shape = nullable %s type %s", nullable, dataType)
	}
	var recoveryConstraint string
	if err := database.DB.Conn.QueryRow(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE connamespace=$1::regnamespace AND conrelid=$2::regclass
		  AND pg_get_constraintdef(oid) LIKE '%recovery_lease_ms%'`, database.Schema,
		database.Schema+".flow_commands").Scan(&recoveryConstraint); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(recoveryConstraint, ">= 30") {
		t.Fatalf("recovery lease constraint = %s", recoveryConstraint)
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
		"flow_runs_status_ck":             {string(RunStatusRunning), string(RunStatusFailing), string(RunStatusSucceeded), string(RunStatusFailed), string(RunStatusCancelled), string(RunStatusExpired)},
		"flow_commands_state_ck":          {string(CommandStatusPending), string(CommandStatusReady), string(CommandStatusRunning), string(CommandStatusRetryWait), string(CommandStatusSucceeded), string(CommandStatusFailed), string(CommandStatusCancelled), string(CommandStatusExpired)},
		"flow_command_queue_state_ck":     {string(QueueStateReady), string(QueueStateRetryWait), string(QueueStateRunning)},
		"flow_runs_key_scope_ck":          {string(KeyScopePermanent), string(KeyScopeLive)},
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
	first, err := command.Enqueue(ctx, runtime, "first", None{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := command.Enqueue(ctx, runtime, "second", None{})
	if err != nil {
		t.Fatal(err)
	}
	firstRun := mustGetRun(t, runtime, first.RunID)
	secondRun := mustGetRun(t, runtime, second.RunID)
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
		WHERE namespace.nspname=$1 AND relation.relname='flow_runs'
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
	_, err = tx.Exec(ctx, `UPDATE `+schema+`.flow_runs SET root_command_id=NULL WHERE run_id=$1`, first.RunID)
	_ = tx.Rollback(ctx)
	var notNullError *pgconn.PgError
	if !errors.As(err, &notNullError) || notNullError.ColumnName != "root_command_id" {
		t.Fatalf("root NOT NULL error = %v", err)
	}
	assertConstraint("flow_runs_root_command_fk", func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_runs SET root_command_id=$2 WHERE run_id=$1`, first.RunID, secondRun.RootCommandID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `SET CONSTRAINTS `+schema+`.flow_runs_root_command_fk IMMEDIATE`)
		return err
	})
	assertConstraint("flow_commands_parent_run_fk", func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_commands SET parent_command_id=$2 WHERE command_id=$1`, firstRun.RootCommandID, secondRun.RootCommandID)
		return err
	})
	assertConstraint("flow_command_queue_command_run_fk", func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_command_queue SET run_id=$2 WHERE command_id=$1`, firstRun.RootCommandID, second.RunID)
		return err
	})
	if _, err := database.DB.Conn.Exec(ctx, `INSERT INTO `+schema+`.flow_command_event_waits
		(command_id,run_id,event_name,event_key) VALUES ($1,$2,'constraint.event','key')`, firstRun.RootCommandID, first.RunID); err != nil {
		t.Fatal(err)
	}
	assertConstraint("flow_command_event_waits_command_run_fk", func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_command_event_waits SET run_id=$2 WHERE command_id=$1`, firstRun.RootCommandID, second.RunID)
		return err
	})
	assertConstraint("flow_journal_command_run_fk", func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_journal SET command_id=$3 WHERE run_id=$1 AND position=$2`, first.RunID, 1, secondRun.RootCommandID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `SET CONSTRAINTS `+schema+`.flow_journal_command_run_fk IMMEDIATE`)
		return err
	})
	assertInvalidPosition("flow_runs_next_journal_position_ck", func(tx pgx.Tx, value int64) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_runs SET next_journal_position=$2 WHERE run_id=$1`, first.RunID, value)
		return err
	})
	assertInvalidPosition("flow_commands_created_position_ck", func(tx pgx.Tx, value int64) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_commands SET created_position=$2 WHERE command_id=$1`, firstRun.RootCommandID, value)
		return err
	})
	assertInvalidPosition("flow_command_event_waits_position_ck", func(tx pgx.Tx, value int64) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_command_event_waits SET satisfied_position=$2 WHERE command_id=$1`, firstRun.RootCommandID, value)
		return err
	})
	assertInvalidPosition("flow_journal_position_ck", func(tx pgx.Tx, value int64) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_journal SET position=$2 WHERE run_id=$1 AND position=1`, first.RunID, value)
		return err
	})
	assertInvalidPosition("flow_commands_terminal_position_ck", func(tx pgx.Tx, value int64) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_commands
			SET state='succeeded',result='{}',terminal_position=$2,finished_at=clock_timestamp()
			WHERE command_id=$1`, firstRun.RootCommandID, value)
		return err
	})
	assertInvalidPosition("flow_journal_position_causation_ck", func(tx pgx.Tx, value int64) error {
		_, err := tx.Exec(ctx, `UPDATE `+schema+`.flow_journal SET causation_position=$2 WHERE run_id=$1 AND position=2`, first.RunID, value)
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
		`UPDATE `+quoteIdentifier(database.Schema)+`.flow_schema_migrations SET min_writer_version=3 WHERE version=1`,
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
