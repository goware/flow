package flow

import (
	"context"
	"testing"
	"time"

	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
)

func TestClaimSkipsLockedRowsAndUnhandledBacklog(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(0))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[runtimeArgs, runtimeResult]("claim.handled", 1)
	exec, err := command.With(runtime).Execute(ctx, "locked", runtimeArgs{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	kinds := []store.CommandKind{{Name: command.Name(), Version: command.Version()}}
	candidates, err := runtime.store.ProbeCommands(ctx, kinds, 10)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("ProbeCommands() = %#v, %v", candidates, err)
	}

	lockTx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	if _, err := lockTx.Exec(ctx, `SELECT execution_id FROM `+pgschema.Table(database.Schema, "flow_executions")+`
		WHERE execution_id=$1 FOR UPDATE`, exec.ID); err != nil {
		t.Fatalf("lock execution: %v", err)
	}
	started := time.Now()
	claim, err := runtime.store.ClaimCommand(ctx, candidates[0], time.Second, "claim-test", nil)
	if err != nil || claim.Command != nil || time.Since(started) > 100*time.Millisecond {
		t.Fatalf("execution-locked claim = %#v, %v in %s", claim, err, time.Since(started))
	}
	_ = lockTx.Rollback(ctx)

	lockTx, err = database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx(queue) error = %v", err)
	}
	if _, err := lockTx.Exec(ctx, `SELECT command_id FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		WHERE command_id=$1 FOR UPDATE`, exec.RootCommandID); err != nil {
		t.Fatalf("lock queue: %v", err)
	}
	started = time.Now()
	claim, err = runtime.store.ClaimCommand(ctx, candidates[0], time.Second, "claim-test", nil)
	if err != nil || claim.Command != nil || time.Since(started) > 100*time.Millisecond {
		t.Fatalf("queue-locked claim = %#v, %v in %s", claim, err, time.Since(started))
	}
	_ = lockTx.Rollback(ctx)
	if err := CancelExecution(ctx, runtime, exec.ID, "claim lock test complete"); err != nil {
		t.Fatalf("CancelExecution() error = %v", err)
	}

}
