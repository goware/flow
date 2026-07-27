package flow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
)

// Run with -benchtime=1x. Each measured iteration performs one complete
// PostgreSQL reconciliation transaction; execution ingress is outside the
// timer so the result isolates plan snapshot/evaluation/materialization cost.
func BenchmarkPlanReconciliation(b *testing.B) {
	for _, size := range []int{10, 100, 1_000} {
		b.Run(fmt.Sprintf("commands_%d", size), func(b *testing.B) {
			database := testpg.Open(b)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				b.Fatal(err)
			}
			runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(0),
				WithPlanVerification(true))
			if err != nil {
				b.Fatal(err)
			}
			command := DefineCommand[None, None](fmt.Sprintf("benchmark.plan.command.%d", size), 1)
			plan := DefinePlan[None](fmt.Sprintf("benchmark.plan.%d", size), 1, func(plan *Plan, _ None) {
				for index := range size {
					Do(plan, fmt.Sprintf("work/%04d", index), command, None{})
				}
			})
			if err := runtime.Register(plan); err != nil {
				b.Fatal(err)
			}
			definition, ok := runtime.registry.plan(plan.def.Name, plan.def.Version)
			if !ok {
				b.Fatal("registered plan unavailable")
			}
			for iteration := range b.N {
				b.StopTimer()
				handle, err := plan.With(runtime).Execute(ctx, fmt.Sprintf("reconcile/%d", iteration), None{}, WithoutExecutionDeadline())
				if err != nil {
					b.Fatal(err)
				}
				executionID, err := parseExecutionID(handle.ID)
				if err != nil {
					b.Fatal(err)
				}
				candidate := store.PlanCandidate{ExecutionID: executionID, Name: plan.def.Name,
					Version: plan.def.Version, DirtySince: time.Now()}
				b.StartTimer()
				if !runtime.reconcilePlan(ctx, candidate, definition) {
					b.Fatal("plan reconciliation made no progress")
				}
				b.StopTimer()
				trace, err := Trace(ctx, runtime, handle.ID)
				if err != nil || len(trace.Commands) != size {
					b.Fatalf("trace commands=%d error=%v", len(trace.Commands), err)
				}
			}
		})
	}
}
