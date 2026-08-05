package flow

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"strings"
	"testing"

	"github.com/goware/flow/internal/testpg"
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
	if indexes < 25 {
		t.Fatalf("Flow index count = %d, want at least 25", indexes)
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
