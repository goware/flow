// Package testpg creates isolated PostgreSQL schemas for integration tests.
// It never substitutes an in-memory database for PostgreSQL behavior.
package testpg

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultURL = "postgres://postgres@127.0.0.1/postgres?sslmode=disable"

type Database struct {
	DB     *pgkit.DB
	Schema string
}

func Open(t testing.TB) Database {
	t.Helper()
	url := os.Getenv("FLOW_TEST_DATABASE_URL")
	if url == "" {
		url = defaultURL
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse FLOW_TEST_DATABASE_URL: %v", err)
	}
	if password := os.Getenv("FLOW_TEST_DATABASE_PASSWORD"); password != "" {
		config.ConnConfig.Password = password
	}
	config.MaxConns = 12
	db, err := pgkit.ConnectWithPGX("flow-test", config)
	if err != nil {
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.Conn.Ping(ctx); err != nil {
		db.Conn.Close()
		if os.Getenv("FLOW_TEST_DATABASE_URL") == "" {
			t.Skipf("PostgreSQL integration test unavailable at %s: %v", defaultURL, err)
		}
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	schema := "flow_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = db.Conn.Exec(cleanupCtx, fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, schema))
		db.Conn.Close()
	})
	return Database{DB: db, Schema: schema}
}
