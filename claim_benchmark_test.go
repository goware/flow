package flow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
)

func BenchmarkClaimProbeUnhandledHead10K(b *testing.B) {
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatalf("Migrate() error = %v", err)
	}
	rt, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(0))
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	plan := DefinePlan[runtimeArgs]("benchmark.claim.probe", 1, func(*Plan, runtimeArgs) {})
	handle, err := plan.With(rt).Execute(ctx, "probe", runtimeArgs{})
	if err != nil {
		b.Fatalf("Plan.Execute() error = %v", err)
	}
	executionID, err := uuid.Parse(string(handle.ID))
	if err != nil {
		b.Fatalf("parse execution ID: %v", err)
	}
	seedClaimPlanRows(b, database.Schema, database.DB.Conn, executionID, 10_000)
	if _, err := database.DB.Conn.Exec(ctx, `ANALYZE `+pgschema.Table(database.Schema, "flow_command_queue")); err != nil {
		b.Fatalf("ANALYZE queue: %v", err)
	}
	kinds := []store.CommandKind{{Name: "claim.plan.handled", Version: 1}}

	b.ResetTimer()
	for range b.N {
		candidates, err := rt.store.ProbeCommands(ctx, kinds, 32)
		if err != nil || len(candidates) != 32 {
			b.Fatalf("ProbeCommands() candidates=%d error=%v", len(candidates), err)
		}
	}
}

// Run with -benchtime=1x. Each measured iteration creates and claims a fresh
// 1,000-command burst from one execution in capacity-sized semantic batches;
// fixture construction is outside the timer.
func BenchmarkSameExecutionClaimBurst1000(b *testing.B) {
	database := testpg.Open(b)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		b.Fatalf("Migrate() error = %v", err)
	}
	rt, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(0))
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[runtimeArgs, runtimeResult]("benchmark.claim.burst", 1)
	plan := DefinePlan[runtimeArgs]("benchmark.claim.burst.plan", 1, func(*Plan, runtimeArgs) {})
	kinds := []store.CommandKind{{Name: command.Name(), Version: command.Version()}}

	for iteration := range b.N {
		b.StopTimer()
		handle, err := plan.With(rt).Execute(ctx, fmt.Sprintf("burst/%d", iteration), runtimeArgs{})
		if err != nil {
			b.Fatalf("Plan.Execute() error = %v", err)
		}
		tx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			b.Fatalf("begin fixture transaction: %v", err)
		}
		client := rt.InTx(tx)
		for index := range 1_000 {
			if _, err := Issue(ctx, client, handle.ID, fmt.Sprintf("work/%04d", index), command, runtimeArgs{}); err != nil {
				_ = tx.Rollback(ctx)
				b.Fatalf("Issue(%d) error = %v", index, err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			b.Fatalf("commit fixture: %v", err)
		}
		candidates, err := rt.store.ProbeCommands(ctx, kinds, 1_000)
		if err != nil || len(candidates) != 1_000 {
			b.Fatalf("ProbeCommands() candidates=%d error=%v", len(candidates), err)
		}

		b.StartTimer()
		claimed := 0
		for offset := 0; offset < len(candidates); offset += 32 {
			end := min(offset+32, len(candidates))
			result, claimErr := rt.store.ClaimCommands(ctx, candidates[offset:end], time.Minute, "benchmark", nil)
			if claimErr != nil {
				b.Fatalf("ClaimCommands() error = %v", claimErr)
			}
			claimed += len(result.Commands)
		}
		b.StopTimer()
		if claimed != 1_000 {
			b.Fatalf("claimed=%d want=1000", claimed)
		}
	}
}
