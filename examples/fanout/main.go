package main

import (
	"context"
	"fmt"
	"os"

	"github.com/goware/flow"
	"github.com/goware/flow/internal/examples"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	databaseURL := os.Getenv("FLOW_EXAMPLE_DATABASE_URL")
	if databaseURL == "" {
		fmt.Fprintln(os.Stderr, "FLOW_EXAMPLE_DATABASE_URL is required")
		os.Exit(2)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		panic(err)
	}
	db, err := pgkit.ConnectWithPGX("flow-fanout-example", config)
	if err != nil {
		panic(err)
	}
	defer db.Conn.Close()
	if err := flow.Migrate(ctx, db); err != nil {
		panic(err)
	}
	result, err := examples.RunFanOut(ctx, db, "public", os.Stdout)
	if err != nil {
		panic(err)
	}
	fmt.Printf("execution %s completed with %d commands and %d journal entries\n",
		result.Handle.ID, len(result.Trace.Commands), len(result.Trace.History))
}
