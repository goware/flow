package flow

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
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
