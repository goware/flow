// Command db manages the disposable PostgreSQL database used by Flow's tests.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/goware/flow"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	databaseURLEnv       = "FLOW_TEST_DATABASE_URL"
	databasePasswordEnv  = "FLOW_TEST_DATABASE_PASSWORD"
	adminDatabaseEnv     = "FLOW_TEST_ADMIN_DATABASE"
	defaultAdminDatabase = "postgres"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "flow db: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: go run ./tools/db reset|migrate|status")
	}

	databaseURL := os.Getenv(databaseURLEnv)
	if databaseURL == "" {
		return fmt.Errorf("%s is required", databaseURLEnv)
	}
	target, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("parse %s: %w", databaseURLEnv, err)
	}
	if password := os.Getenv(databasePasswordEnv); password != "" {
		target.ConnConfig.Password = password
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch args[0] {
	case "reset":
		if err := reset(ctx, target); err != nil {
			return err
		}
		return migrate(ctx, target)
	case "migrate":
		return migrate(ctx, target)
	case "status":
		return status(ctx, target)
	default:
		return fmt.Errorf("unknown command %q; expected reset, migrate, or status", args[0])
	}
}

func reset(ctx context.Context, target *pgxpool.Config) error {
	database := target.ConnConfig.Database
	if !strings.HasSuffix(database, "_test") {
		return fmt.Errorf("refusing to reset database %q: its name must end in _test", database)
	}

	adminDatabase := os.Getenv(adminDatabaseEnv)
	if adminDatabase == "" {
		adminDatabase = defaultAdminDatabase
	}
	if adminDatabase == database {
		return fmt.Errorf("admin database %q must differ from the reset target", adminDatabase)
	}

	admin := target.Copy()
	admin.ConnConfig.Database = adminDatabase
	admin.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, admin)
	if err != nil {
		return fmt.Errorf("open admin database %q: %w", adminDatabase, err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("connect to admin database %q: %w", adminDatabase, err)
	}

	if _, err := pool.Exec(ctx, `
		SELECT pg_terminate_backend(pid)
		FROM pg_stat_activity
		WHERE datname = $1 AND pid <> pg_backend_pid()`, database); err != nil {
		return fmt.Errorf("terminate connections to %q: %w", database, err)
	}
	identifier := pgx.Identifier{database}.Sanitize()
	if _, err := pool.Exec(ctx, "DROP DATABASE IF EXISTS "+identifier); err != nil {
		return fmt.Errorf("drop database %q: %w", database, err)
	}
	if _, err := pool.Exec(ctx, "CREATE DATABASE "+identifier); err != nil {
		return fmt.Errorf("create database %q: %w", database, err)
	}
	fmt.Printf("recreated PostgreSQL database %s\n", database)
	return nil
}

func migrate(ctx context.Context, config *pgxpool.Config) error {
	database, closeDatabase, err := connect(ctx, config)
	if err != nil {
		return err
	}
	defer closeDatabase()

	if err := flow.Migrate(ctx, database); err != nil {
		return fmt.Errorf("migrate database %q: %w", config.ConnConfig.Database, err)
	}
	return printStatus(ctx, database, config.ConnConfig.Database)
}

func status(ctx context.Context, config *pgxpool.Config) error {
	database, closeDatabase, err := connect(ctx, config)
	if err != nil {
		return err
	}
	defer closeDatabase()
	return printStatus(ctx, database, config.ConnConfig.Database)
}

func connect(ctx context.Context, config *pgxpool.Config) (*pgkit.DB, func(), error) {
	database, err := pgkit.ConnectWithPGX("flow-db", config.Copy())
	if err != nil {
		return nil, nil, fmt.Errorf("open database %q: %w", config.ConnConfig.Database, err)
	}
	if err := database.Conn.Ping(ctx); err != nil {
		database.Conn.Close()
		return nil, nil, fmt.Errorf("connect to database %q: %w", config.ConnConfig.Database, err)
	}
	return database, database.Conn.Close, nil
}

func printStatus(ctx context.Context, database *pgkit.DB, name string) error {
	status, err := flow.CheckSchema(ctx, database)
	if err != nil {
		return fmt.Errorf("check database %q: %w", name, err)
	}
	fmt.Printf("Flow schema %s is compatible at version %d\n", status.Schema, status.CurrentVersion)
	return nil
}
