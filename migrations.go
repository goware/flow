package flow

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"runtime/debug"
	"slices"
	"strings"
	"testing/fstest"
	"time"

	"github.com/goware/flow/internal/durable"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5"
)

const (
	defaultSchema         = "public"
	currentSchemaVersion  = 3
	currentReaderVersion  = 1
	currentWriterVersion  = 1
	migrationToken        = "{{schema}}"
	migrationAdvisorySalt = "goware/flow/migrate/v1"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// MigrateOption is a sealed migration and schema-inspection option.
type MigrateOption interface {
	applyMigration(*migrationOptions)
}

type schemaOption struct {
	schema string
}

func (o schemaOption) applyMigration(options *migrationOptions) { options.schema = o.schema }

// WithSchema places Flow's fixed flow_ tables in a validated PostgreSQL
// schema. The default is public. Runtime options adopt the same value in the
// runtime phase; table names always retain their flow_ prefix.
func WithSchema(schema string) schemaOption { return schemaOption{schema: schema} }

type migrationOptions struct {
	schema string
}

// SchemaStatus describes the verified migration and compatibility state.
type SchemaStatus struct {
	Schema           string
	CurrentVersion   int
	MinReaderVersion int
	MinWriterVersion int
	Compatible       bool
	AppliedAt        time.Time
}

type migrationUnit struct {
	version   int
	name      string
	path      string
	checksum  [sha256.Size]byte
	contents  []byte
	minReader int
	minWriter int
}

var migrationFiles = []struct {
	version   int
	name      string
	path      string
	minReader int
	minWriter int
}{
	{version: 1, name: "initial", path: "migrations/001_initial.sql", minReader: 1, minWriter: 1},
	{version: 2, name: "live_keys", path: "migrations/002_live_keys.sql", minReader: 1, minWriter: 1},
	{version: 3, name: "release_read_paths", path: "migrations/003_release_read_paths.sql", minReader: 1, minWriter: 1},
}

// Migrate applies every unapplied embedded Flow migration in its own
// execution-serialized transaction and verifies all previously recorded
// checksums before writing.
func Migrate(ctx context.Context, db *pgkit.DB, opts ...MigrateOption) error {
	options, units, err := prepareMigrations(opts...)
	if err != nil {
		return err
	}
	if db == nil || db.Conn == nil {
		return newError(ErrInvalid, "migrate", "database", "", "database is nil")
	}

	for _, unit := range units {
		if err := applyMigrationUnit(ctx, db, options.schema, unit, units); err != nil {
			return err
		}
	}
	return nil
}

func applyMigrationUnit(ctx context.Context, db *pgkit.DB, schema string, unit migrationUnit, units []migrationUnit) (resultErr error) {
	tx, err := db.Conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return store.MapError("begin migration", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()

	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended(current_database() || ':' || $1 || ':' || $2, 0))`,
		migrationAdvisorySalt, schema,
	); err != nil {
		return store.MapError("lock migration", err)
	}

	ledgerExists, err := migrationLedgerExists(ctx, tx, schema)
	if err != nil {
		return err
	}
	if ledgerExists {
		applied, err := loadAppliedMigrations(ctx, tx, schema)
		if err != nil {
			return err
		}
		if err := verifyAppliedMigrations(applied, units); err != nil {
			return err
		}
		if _, ok := applied[unit.version]; ok {
			if err := tx.Commit(ctx); err != nil {
				return store.MapError("commit verified migration", err)
			}
			return nil
		}
		if unit.version == 1 {
			return newError(ErrSchema, "migrate", "schema", schema, "migration ledger exists without the initial migration")
		}
	} else if unit.version != 1 {
		return newError(ErrSchema, "migrate", "schema", schema, "migration ledger is missing")
	}

	if _, err := tx.Exec(ctx, string(unit.contents)); err != nil {
		return mapMigrationError("apply", schema, err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s.flow_schema_migrations
		    (version, name, checksum, library_version, min_reader_version, min_writer_version, applied_at)
		VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp())`, quoteIdentifier(schema)),
		unit.version, unit.name, unit.checksum[:], libraryVersion(), unit.minReader, unit.minWriter,
	); err != nil {
		return mapMigrationError("record", schema, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return store.MapError("commit migration", err)
	}
	return nil
}

// CheckSchema verifies migration checksums, reader/writer compatibility, and
// the fixed Flow table inventory without changing the database.
func CheckSchema(ctx context.Context, db *pgkit.DB, opts ...MigrateOption) (SchemaStatus, error) {
	options, units, err := prepareMigrations(opts...)
	if err != nil {
		return SchemaStatus{}, err
	}
	if db == nil || db.Conn == nil {
		return SchemaStatus{}, newError(ErrInvalid, "check", "database", "", "database is nil")
	}
	ledgerExists, err := migrationLedgerExists(ctx, db.Conn, options.schema)
	if err != nil {
		return SchemaStatus{}, err
	}
	if !ledgerExists {
		return SchemaStatus{}, newError(ErrSchema, "check", "schema", options.schema, "migration ledger is missing")
	}
	applied, err := loadAppliedMigrations(ctx, db.Conn, options.schema)
	if err != nil {
		return SchemaStatus{}, err
	}
	if err := verifyAppliedMigrations(applied, units); err != nil {
		return SchemaStatus{}, err
	}
	latest, ok := applied[currentSchemaVersion]
	if !ok || len(applied) != len(units) {
		return SchemaStatus{}, newError(ErrSchema, "check", "schema", options.schema, "schema version is incomplete or unknown")
	}
	status := SchemaStatus{
		Schema: options.schema, CurrentVersion: latest.version,
		MinReaderVersion: latest.minReader, MinWriterVersion: latest.minWriter,
		AppliedAt: latest.appliedAt,
	}
	status.Compatible = latest.version == currentSchemaVersion &&
		latest.minReader <= currentReaderVersion && latest.minWriter <= currentWriterVersion
	if !status.Compatible {
		return status, newError(ErrSchema, "check", "schema", options.schema, "reader or writer compatibility does not include this library")
	}
	if err := verifyFlowTables(ctx, db.Conn, options.schema); err != nil {
		return status, err
	}
	return status, nil
}

// MigrationFS returns schema-rendered SQL files for an external transactional
// migration runner. Each file records the same checksum and compatibility row
// as Migrate, so CheckSchema accepts either application path.
func MigrationFS(opts ...MigrateOption) (fs.FS, error) {
	options, units, err := prepareMigrations(opts...)
	if err != nil {
		return nil, err
	}
	files := make(fstest.MapFS, len(units))
	for _, unit := range units {
		files[unit.path] = &fstest.MapFile{Data: externalMigration(options.schema, unit), Mode: 0o444}
	}
	return files, nil
}

func externalMigration(schema string, unit migrationUnit) []byte {
	contents := slices.Clone(unit.contents)
	contents = append(contents, []byte(fmt.Sprintf(`

INSERT INTO %s.flow_schema_migrations
    (version, name, checksum, library_version, min_reader_version, min_writer_version, applied_at)
VALUES (%d, '%s', decode('%s','hex'), '(external)', %d, %d, clock_timestamp());
`, quoteIdentifier(schema), unit.version, unit.name, hex.EncodeToString(unit.checksum[:]), unit.minReader, unit.minWriter))...)
	return contents
}

type appliedMigration struct {
	version   int
	name      string
	checksum  [sha256.Size]byte
	minReader int
	minWriter int
	appliedAt time.Time
}

type queryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func prepareMigrations(opts ...MigrateOption) (migrationOptions, []migrationUnit, error) {
	options := migrationOptions{schema: defaultSchema}
	for _, option := range opts {
		if option == nil {
			return migrationOptions{}, nil, newError(ErrInvalid, "configure", "migration", "", "nil option")
		}
		option.applyMigration(&options)
	}
	if err := validateSchemaName(options.schema); err != nil {
		return migrationOptions{}, nil, err
	}
	units := make([]migrationUnit, 0, len(migrationFiles))
	for _, file := range migrationFiles {
		for field, value := range map[string]int{
			"migration version":      file.version,
			"minimum reader version": file.minReader,
			"minimum writer version": file.minWriter,
		} {
			if err := durable.PostgresInteger(field, value, 1, durable.PostgresIntegerMax); err != nil {
				return migrationOptions{}, nil, newError(ErrInvalid, "configure", "migration", file.path, err.Error())
			}
		}
		source, err := embeddedMigrations.ReadFile(file.path)
		if err != nil {
			return migrationOptions{}, nil, fmt.Errorf("flow migration %s: %w", file.path, err)
		}
		rendered := []byte(strings.ReplaceAll(string(source), migrationToken, quoteIdentifier(options.schema)))
		units = append(units, migrationUnit{
			version: file.version, name: file.name, path: file.path,
			contents: rendered, checksum: sha256.Sum256(rendered),
			minReader: file.minReader, minWriter: file.minWriter,
		})
	}
	return options, units, nil
}

func validateSchemaName(schema string) error {
	if err := pgschema.Validate(schema); err != nil {
		return newError(ErrInvalid, "configure", "schema", schema, err.Error())
	}
	return nil
}

func quoteIdentifier(identifier string) string {
	return pgschema.Quote(identifier)
}

func migrationLedgerExists(ctx context.Context, db queryer, schema string) (bool, error) {
	var relation *string
	qualifiedLedger := quoteIdentifier(schema) + `.` + quoteIdentifier("flow_schema_migrations")
	if err := db.QueryRow(ctx, `SELECT to_regclass($1)`, qualifiedLedger).Scan(&relation); err != nil {
		return false, store.MapError("lookup migration schema", err)
	}
	return relation != nil, nil
}

func loadAppliedMigrations(ctx context.Context, db queryer, schema string) (map[int]appliedMigration, error) {
	rows, err := db.Query(ctx, fmt.Sprintf(`
		SELECT version, name, checksum, min_reader_version, min_writer_version, applied_at
		FROM %s.flow_schema_migrations ORDER BY version`, quoteIdentifier(schema)))
	if err != nil {
		return nil, store.MapError("read migration ledger", err)
	}
	defer rows.Close()
	applied := make(map[int]appliedMigration)
	for rows.Next() {
		var row appliedMigration
		var checksum []byte
		if err := rows.Scan(&row.version, &row.name, &checksum, &row.minReader, &row.minWriter, &row.appliedAt); err != nil {
			return nil, store.MapError("scan migration ledger", err)
		}
		if len(checksum) != sha256.Size {
			return nil, newError(ErrSchema, "check", "migration", fmt.Sprint(row.version), "invalid checksum length")
		}
		copy(row.checksum[:], checksum)
		applied[row.version] = row
	}
	if err := rows.Err(); err != nil {
		return nil, store.MapError("read migration ledger rows", err)
	}
	return applied, nil
}

func verifyAppliedMigrations(applied map[int]appliedMigration, units []migrationUnit) error {
	known := make(map[int]migrationUnit, len(units))
	for _, unit := range units {
		known[unit.version] = unit
	}
	for version, row := range applied {
		unit, ok := known[version]
		if !ok {
			return newError(ErrSchema, "check", "migration", fmt.Sprint(version), "database has a migration unknown to this library")
		}
		if row.name != unit.name || row.checksum != unit.checksum ||
			row.minReader != unit.minReader || row.minWriter != unit.minWriter {
			return newError(ErrSchema, "check", "migration", fmt.Sprint(version), "name, checksum, or compatibility differs from embedded migration")
		}
	}
	for version := 1; version <= len(applied); version++ {
		if _, ok := applied[version]; !ok {
			return newError(ErrSchema, "check", "migration", fmt.Sprint(version), "database migration ledger is not contiguous")
		}
	}
	return nil
}

func verifyFlowTables(ctx context.Context, db queryer, schema string) error {
	expected := []string{
		"flow_executions", "flow_commands", "flow_command_queue",
		"flow_command_event_waits", "flow_journal", "flow_schema_migrations",
	}
	rows, err := db.Query(ctx, `
		SELECT tablename FROM pg_catalog.pg_tables
		WHERE schemaname = $1 AND left(tablename, 5) = 'flow_'
		ORDER BY tablename`, schema)
	if err != nil {
		return store.MapError("check migration tables", err)
	}
	defer rows.Close()
	actual := make([]string, 0, len(expected))
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return store.MapError("scan migration tables", err)
		}
		actual = append(actual, table)
	}
	if err := rows.Err(); err != nil {
		return store.MapError("read migration tables", err)
	}
	slices.Sort(expected)
	if !slices.Equal(actual, expected) {
		return newError(ErrSchema, "check", "schema", schema,
			fmt.Sprintf("Flow table inventory is %v, want %v", actual, expected))
	}
	return nil
}

func mapMigrationError(op, schema string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return store.MapError("migrate "+op+" "+schema, err)
}

func libraryVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
		return info.Main.Version
	}
	return "(devel)"
}
