package flow

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/failure"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/store/journalcodec"
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

func TestClaimBatchPersistsSixteenSiblingAttemptsTogether(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	child := DefineCommand[None, None]("claim.batch_sixteen_child", 1)
	runtime, execution := stageClaimFixture(t, database, "sixteen", 16, func(work *Work[None]) {
		for index := range 16 {
			Execute(work, fmt.Sprintf("child/%02d", index), child, None{})
		}
	})
	candidates := probeClaimCandidates(t, runtime, []store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 16)
	result, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "claim-batch-test", fault.None{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Progressed || len(result.Commands) != 16 {
		t.Fatalf("ClaimCommands() progressed=%t commands=%d", result.Progressed, len(result.Commands))
	}
	for index, command := range result.Commands {
		if command.CommandID != candidates[index].CommandID || command.Attempt != 1 {
			t.Fatalf("claim[%d]=%s attempt=%d, candidate=%s", index, command.CommandID, command.Attempt,
				candidates[index].CommandID)
		}
		if index > 0 && command.AttemptStartedPosition != result.Commands[index-1].AttemptStartedPosition+1 {
			t.Fatalf("claim positions are not contiguous at %d: %d after %d", index,
				command.AttemptStartedPosition, result.Commands[index-1].AttemptStartedPosition)
		}
	}
	var runningCommands, runningQueues, starts, caused int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+`
		 WHERE execution_id=$1 AND parent_command_id=$2 AND state='running' AND attempt_ordinal=1),
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		 WHERE execution_id=$1 AND state='running' AND active_attempt_id IS NOT NULL AND lease_token IS NOT NULL),
		count(*),count(*) FILTER (WHERE j.causation_position=c.created_position)
	FROM `+pgschema.Table(database.Schema, "flow_journal")+` j
	JOIN `+pgschema.Table(database.Schema, "flow_commands")+` c
	  ON c.execution_id=j.execution_id AND c.command_id=j.command_id
	WHERE j.execution_id=$1 AND j.entry_kind='attempt_started' AND c.parent_command_id=$2`,
		execution.ID, execution.RootCommandID).Scan(&runningCommands, &runningQueues, &starts, &caused); err != nil {
		t.Fatal(err)
	}
	if runningCommands != 16 || runningQueues != 16 || starts != 16 || caused != 16 {
		t.Fatalf("batch projections commands=%d queues=%d starts=%d caused=%d",
			runningCommands, runningQueues, starts, caused)
	}
}

func TestClaimBatchSkipsOneLockedSibling(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	child := DefineCommand[None, None]("claim.batch_locked_child", 1)
	runtime, _ := stageClaimFixture(t, database, "locked_sibling", 4, func(work *Work[None]) {
		for index := range 4 {
			Execute(work, fmt.Sprintf("child/%d", index), child, None{})
		}
	})
	candidates := probeClaimCandidates(t, runtime, []store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 4)
	lockTx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.Exec(ctx, `SELECT command_id FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		WHERE command_id=$1 FOR UPDATE`, candidates[1].CommandID); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "claim-batch-test", fault.None{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Commands) != 3 {
		t.Fatalf("ClaimCommands() commands=%d, want 3", len(result.Commands))
	}
	for _, command := range result.Commands {
		if command.CommandID == candidates[1].CommandID {
			t.Fatal("locked sibling was claimed")
		}
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	last, err := runtime.store.ClaimCommand(ctx, candidates[1], time.Minute, "claim-batch-test", fault.None{})
	if err != nil || last.Command == nil {
		t.Fatalf("ClaimCommand(locked sibling)=%#v, %v", last, err)
	}
}

func TestRenewCommandLeasesClassifiesLockedAndLostRows(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	child := DefineCommand[None, None]("claim.renew_classification_child", 1)
	runtime, execution := stageClaimFixture(t, database, "renew_classification", 2, func(work *Work[None]) {
		Execute(work, "child/0", child, None{})
		Execute(work, "child/1", child, None{})
	})
	candidates := probeClaimCandidates(t, runtime,
		[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 2)
	claimed, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "renew-test", fault.None{})
	if err != nil || len(claimed.Commands) != 2 {
		t.Fatalf("ClaimCommands() commands=%d, err=%v", len(claimed.Commands), err)
	}
	renewals := make([]store.LeaseRenewal, len(claimed.Commands))
	for index, command := range claimed.Commands {
		renewals[index] = store.LeaseRenewal{
			CommandID: command.CommandID, AttemptID: command.AttemptID, Token: command.LeaseToken,
		}
	}
	incomplete := renewals[0]
	incomplete.Token = uuid.Nil
	if _, err := runtime.store.RenewCommandLeases(ctx, []store.LeaseRenewal{incomplete}, time.Minute); err == nil {
		t.Fatal("RenewCommandLeases() accepted an incomplete fence")
	}
	if _, err := runtime.store.RenewCommandLeases(ctx,
		[]store.LeaseRenewal{renewals[0], renewals[0]}, time.Minute); err == nil {
		t.Fatal("RenewCommandLeases() accepted a duplicate command ID")
	}

	lockTx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.Exec(ctx, `SELECT command_id FROM `+
		pgschema.Table(database.Schema, "flow_command_queue")+` WHERE command_id=$1 FOR UPDATE`,
		claimed.Commands[0].CommandID); err != nil {
		_ = lockTx.Rollback(ctx)
		t.Fatal(err)
	}
	started := time.Now()
	results, err := runtime.store.RenewCommandLeases(ctx, renewals, time.Minute)
	if err != nil {
		_ = lockTx.Rollback(ctx)
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		_ = lockTx.Rollback(ctx)
		t.Fatalf("skip-locked renewal took %s", elapsed)
	}
	if len(results) != 2 || results[0].Outcome != store.LeaseUncertain ||
		results[1].Outcome != store.LeaseRenewed || results[1].LeaseExpiresAt == nil {
		_ = lockTx.Rollback(ctx)
		t.Fatalf("locked renewal results = %#v", results)
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	reader, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := CancelExecution(ctx, reader, execution.ID, "renewal classification complete"); err != nil {
		t.Fatal(err)
	}
	results, err = runtime.store.RenewCommandLeases(ctx, renewals, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for index, result := range results {
		if result.Outcome != store.LeaseLost || result.LeaseExpiresAt != nil {
			t.Fatalf("lost renewal[%d] = %#v", index, result)
		}
	}
}

func TestClaimLocalLeaseDeadlineAnchorsAtClaimStart(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	child := DefineCommand[None, None]("claim.local_lease_anchor_child", 1)
	runtime, execution := stageClaimFixture(t, database, "local_lease_anchor", 1, func(work *Work[None]) {
		Execute(work, "child", child, None{})
	})
	runtime.commandLease = 300 * time.Millisecond
	candidates := probeClaimCandidates(t, runtime,
		[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	runtime.faults = fault.Func(func(ctx context.Context, point fault.Point) error {
		if point != fault.ClaimBeforeJournal {
			return nil
		}
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	type claimResult struct {
		result store.ClaimBatchResult
		err    error
	}
	resultChannel := make(chan claimResult, 1)
	go func() {
		result, err := runtime.claimExecutionGroup(ctx, candidates)
		resultChannel <- claimResult{result: result, err: err}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("claim did not reach the pre-journal barrier")
	}
	time.Sleep(150 * time.Millisecond)
	close(release)
	claimed := <-resultChannel
	if claimed.err != nil || len(claimed.result.Commands) != 1 {
		t.Fatalf("claim commands=%d, err=%v", len(claimed.result.Commands), claimed.err)
	}
	remaining := time.Until(claimed.result.Commands[0].LocalLeaseExpiresAt)
	if remaining <= 0 || remaining >= 250*time.Millisecond {
		t.Fatalf("claim-local lease remaining=%s; want a positive duration anchored before the delayed claim", remaining)
	}
	reader, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := CancelExecution(ctx, reader, execution.ID, "claim-local deadline test complete"); err != nil {
		t.Fatal(err)
	}
}

func TestClaimBatchGroupsEventInputsByCommand(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[None]("claim.batch_input")
	child := DefineCommand[None, None]("claim.batch_input_child", 1)
	runtime, _ := stageClaimFixture(t, database, "event_inputs", 2, func(work *Work[None]) {
		for index := range 256 {
			key := fmt.Sprintf("input/%03d", index)
			if err := Emit(work, event, key, None{}); err != nil {
				return
			}
		}
		Execute(work, "child/no-inputs", child, None{})
		withInputs := Execute(work, "child/inputs", child, None{})
		for index := range 256 {
			withInputs.WaitFor(event, fmt.Sprintf("input/%03d", index))
		}
	})
	candidates := probeClaimCandidates(t, runtime, []store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 2)
	result, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "claim-batch-test", fault.None{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Commands) != 2 {
		t.Fatalf("ClaimCommands() commands=%d, want 2", len(result.Commands))
	}
	for _, command := range result.Commands {
		switch command.CommandKey {
		case "child/no-inputs":
			if len(command.EventInputs) != 0 {
				t.Fatalf("no-input command has %d inputs", len(command.EventInputs))
			}
		case "child/inputs":
			if len(command.EventInputs) != 256 {
				t.Fatalf("input command has %d inputs", len(command.EventInputs))
			}
			for index, input := range command.EventInputs {
				if input.Name != event.Name() || input.Key != fmt.Sprintf("input/%03d", index) || string(input.Payload) != `{}` {
					t.Fatalf("input[%d]=%+v", index, input)
				}
			}
		default:
			t.Fatalf("unexpected command key %q", command.CommandKey)
		}
	}
}

func TestClaimEventInputSnapshotIntegrity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		body             func() []byte
		retainStoredHash bool
		selectorMismatch bool
	}{
		{name: "body hash mismatch", body: func() []byte { return []byte(`{"payload":"changed","v":1}`) }, retainStoredHash: true},
		{name: "malformed body", body: func() []byte { return []byte(`{"payload":`) }},
		{name: "trailing data", body: func() []byte { return []byte(`{"payload":"stable","v":1}{}`) }},
		{name: "duplicate envelope key", body: func() []byte {
			return []byte(`{"payload":"first","payload":"second","v":1}`)
		}},
		{name: "nested duplicate payload key", body: func() []byte {
			return []byte(`{"payload":{"key":1,"key":2},"v":1}`)
		}},
		{name: "noncanonical nested payload", body: func() []byte {
			return []byte(`{"payload":{"z":1,"a":2},"v":1}`)
		}},
		{name: "missing version", body: func() []byte { return []byte(`{"payload":"stable"}`) }},
		{name: "zero version", body: func() []byte { return []byte(`{"payload":"stable","v":0}`) }},
		{name: "unknown version", body: func() []byte { return []byte(`{"payload":"stable","v":2}`) }},
		{name: "missing payload", body: func() []byte { return []byte(`{"v":1}`) }},
		{name: "oversized payload", body: func() []byte {
			return []byte(`{"payload":"` + strings.Repeat("x", journalcodec.MaxApplicationEventPayloadBytes) + `","v":1}`)
		}},
		{name: "selector mismatch", selectorMismatch: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database := testpg.Open(t)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				t.Fatal(err)
			}
			suffix := strings.ReplaceAll(tt.name, " ", "_")
			event := DefineEvent[string]("claim.snapshot_integrity_event_" + suffix)
			command := DefineCommand[None, None]("claim.snapshot_integrity_command_"+suffix, 1)
			runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
			if err != nil {
				t.Fatal(err)
			}
			execution, err := command.With(runtime).Execute(ctx, "claim/snapshot/integrity/"+suffix, None{}, WaitFor(event, "input"))
			if err != nil {
				t.Fatal(err)
			}
			if err := event.Emit(ctx, runtime, execution.ID, "input", "stable"); err != nil {
				t.Fatal(err)
			}
			candidates := probeClaimCandidates(t, runtime,
				[]store.CommandKind{{Name: command.Name(), Version: command.Version()}}, 1)
			if tt.selectorMismatch {
				if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_journal")+`
					SET event_key='different' WHERE execution_id=$1 AND event_class='application'`, execution.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				body := tt.body()
				if tt.retainStoredHash {
					if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_journal")+`
						SET body=$2 WHERE execution_id=$1 AND event_class='application'`, execution.ID, body); err != nil {
						t.Fatal(err)
					}
				} else {
					digest := sha256.Sum256(body)
					if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_journal")+`
						SET body=$2,body_hash=$3 WHERE execution_id=$1 AND event_class='application'`,
						execution.ID, body, digest[:]); err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err := runtime.store.ClaimCommand(ctx, candidates[0], time.Minute, "snapshot-integrity", fault.None{}); !errors.Is(err, ErrInvalidState) {
				t.Fatalf("ClaimCommand() error=%v, want ErrInvalidState", err)
			}
			var commandState, queueState string
			var activeFence bool
			var starts int
			if err := database.DB.Conn.QueryRow(ctx, `SELECT c.state,q.state,
				(q.active_attempt_id IS NOT NULL OR q.lease_token IS NOT NULL),
				(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
				 WHERE execution_id=$1 AND entry_kind='attempt_started')
			FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
			JOIN `+pgschema.Table(database.Schema, "flow_command_queue")+` q ON q.command_id=c.command_id
			WHERE c.command_id=$2`, execution.ID, execution.RootCommandID).
				Scan(&commandState, &queueState, &activeFence, &starts); err != nil {
				t.Fatal(err)
			}
			if commandState != "ready" || queueState != "ready" || activeFence || starts != 0 {
				t.Fatalf("failed claim changed projections: command=%s queue=%s fence=%t starts=%d",
					commandState, queueState, activeFence, starts)
			}
		})
	}
}

func TestClaimEventInputSnapshotStableAcrossRetryAndLeaseTakeover(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	event := DefineEvent[string]("claim.snapshot_stability_event")
	command := DefineCommand[None, None]("claim.snapshot_stability_command", 1, WithRetry(Attempts(3)))
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	execution, err := command.With(runtime).Execute(ctx, "claim/snapshot/stability", None{}, WaitFor(event, "input"))
	if err != nil {
		t.Fatal(err)
	}
	if err := event.Emit(ctx, runtime, execution.ID, "input", "stable"); err != nil {
		t.Fatal(err)
	}
	claimNext := func() store.ClaimedCommand {
		candidates := probeClaimCandidates(t, runtime,
			[]store.CommandKind{{Name: command.Name(), Version: command.Version()}}, 1)
		result, err := runtime.store.ClaimCommand(ctx, candidates[0], time.Minute, "snapshot-stability", fault.None{})
		if err != nil || result.Command == nil {
			t.Fatalf("ClaimCommand() = %+v, %v", result, err)
		}
		if len(result.Command.EventInputs) != 1 {
			t.Fatalf("ClaimCommand() inputs=%d", len(result.Command.EventInputs))
		}
		return *result.Command
	}

	first := claimNext()
	wantPayload := append([]byte(nil), first.EventInputs[0].Payload...)
	wantPosition := first.EventInputs[0].Position
	first.EventInputs[0].Payload[0] = '!'
	if _, err := runtime.store.SettleCommandConclusion(ctx, store.CommandConclusion{
		Claim: first, Classification: retrypolicy.ClassInterrupted,
		Failure: failure.Value{Code: "interrupted", Message: "retry snapshot"},
	}, fault.None{}); err != nil {
		t.Fatal(err)
	}

	retry := claimNext()
	if retry.EventInputs[0].Position != wantPosition || string(retry.EventInputs[0].Payload) != string(wantPayload) {
		t.Fatalf("retry snapshot position=%d payload=%s, want %d/%s", retry.EventInputs[0].Position,
			retry.EventInputs[0].Payload, wantPosition, wantPayload)
	}
	retry.EventInputs[0].Payload[0] = '!'
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_command_queue")+`
		SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE command_id=$1`, retry.CommandID); err != nil {
		t.Fatal(err)
	}
	changed, err := runtime.store.RecoverExpiredCommandLease(ctx, store.ExpiredLeaseCandidate{
		CommandID: retry.CommandID, ExecutionID: retry.ExecutionID,
	})
	if err != nil || !changed {
		t.Fatalf("RecoverExpiredCommandLease() = %t, %v", changed, err)
	}

	takeover := claimNext()
	if takeover.EventInputs[0].Position != wantPosition || string(takeover.EventInputs[0].Payload) != string(wantPayload) {
		t.Fatalf("takeover snapshot position=%d payload=%s, want %d/%s", takeover.EventInputs[0].Position,
			takeover.EventInputs[0].Payload, wantPosition, wantPayload)
	}
}

func TestClaimBatchSupportsMixedVersionsAndQueues(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	v1 := DefineCommand[None, None]("claim.batch_mixed", 1)
	v2 := DefineCommand[None, None]("claim.batch_mixed", 2, WithQueue("priority"))
	other := DefineCommand[None, None]("claim.batch_other", 1, WithQueue("bulk"))
	runtime, _ := stageClaimFixture(t, database, "mixed_kinds", 3, func(work *Work[None]) {
		Execute(work, "child/v1", v1, None{})
		Execute(work, "child/v2", v2, None{})
		Execute(work, "child/other", other, None{})
	})
	candidates := probeClaimCandidates(t, runtime, []store.CommandKind{
		{Name: v1.Name(), Version: v1.Version()},
		{Name: v2.Name(), Version: v2.Version()},
		{Name: other.Name(), Version: other.Version()},
	}, 3)
	result, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "claim-batch-test", fault.None{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Commands) != 3 {
		t.Fatalf("ClaimCommands() commands=%d, want 3", len(result.Commands))
	}
	got := make(map[string]string, len(result.Commands))
	for _, command := range result.Commands {
		got[fmt.Sprintf("%s/%d", command.Name, command.Version)] = command.Queue
	}
	if got[v1.Name()+"/1"] != "default" || got[v2.Name()+"/2"] != "priority" || got[other.Name()+"/1"] != "bulk" {
		t.Fatalf("claimed kinds and queues=%v", got)
	}
}

func TestClaimBatchTerminalizesElapsedRetryAlongsideEligibleSibling(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	eligible := DefineCommand[None, None]("claim.batch_elapsed_eligible", 1)
	expiring := DefineCommand[None, None]("claim.batch_elapsed_expired", 1, WithRetry(RetryFor(time.Millisecond)))
	runtime, _ := stageClaimFixture(t, database, "retry_elapsed", 2, func(work *Work[None]) {
		Execute(work, "child/eligible", eligible, None{})
		Execute(work, "child/expired", expiring, None{})
	})
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_commands")+`
		SET budget_started_at=clock_timestamp()-interval '1 second'
		WHERE name=$1`, expiring.Name()); err != nil {
		t.Fatal(err)
	}
	candidates := probeClaimCandidates(t, runtime, []store.CommandKind{
		{Name: eligible.Name(), Version: eligible.Version()},
		{Name: expiring.Name(), Version: expiring.Version()},
	}, 2)
	result, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "claim-batch-test", fault.None{})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Progressed || len(result.Commands) != 1 || result.Commands[0].Name != eligible.Name() {
		t.Fatalf("ClaimCommands()=%+v", result)
	}
	var state, code, expiredID string
	var queueRows int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT c.state,c.terminal_failure->>'code',c.command_id::text,
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` q WHERE q.command_id=c.command_id)
	FROM `+pgschema.Table(database.Schema, "flow_commands")+` c WHERE c.name=$1`, expiring.Name()).
		Scan(&state, &code, &expiredID, &queueRows); err != nil {
		t.Fatal(err)
	}
	if state != "failed" || code != "retry_elapsed" || queueRows != 0 {
		t.Fatalf("expired command state=%s code=%s queue=%d", state, code, queueRows)
	}
	var eligibleState, queueState, executionStatus, executionCode string
	var activeAttempt, leaseToken bool
	var commandCount, openCommands int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT c.state,q.state,
		q.active_attempt_id IS NOT NULL,q.lease_token IS NOT NULL,e.status,e.failure->>'code',
		e.command_count,e.open_commands
	FROM `+pgschema.Table(database.Schema, "flow_commands")+` c
	JOIN `+pgschema.Table(database.Schema, "flow_command_queue")+` q ON q.command_id=c.command_id
	JOIN `+pgschema.Table(database.Schema, "flow_executions")+` e ON e.execution_id=c.execution_id
	WHERE c.command_id=$1`, result.Commands[0].CommandID).
		Scan(&eligibleState, &queueState, &activeAttempt, &leaseToken, &executionStatus, &executionCode,
			&commandCount, &openCommands); err != nil {
		t.Fatal(err)
	}
	if eligibleState != "running" || queueState != "running" || !activeAttempt || !leaseToken ||
		executionStatus != "failing" || executionCode != "retry_elapsed" || commandCount != 3 || openCommands != 1 {
		t.Fatalf("survivor command=%s queue=%s active=%t token=%t execution=%s code=%s counters=%d/%d",
			eligibleState, queueState, activeAttempt, leaseToken, executionStatus, executionCode,
			commandCount, openCommands)
	}
	rows, err := database.DB.Conn.Query(ctx, `SELECT entry_kind,COALESCE(command_id::text,''),COALESCE(terminal_status,'')
	FROM (SELECT position,entry_kind,command_id,terminal_status
		FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=(SELECT execution_id FROM `+pgschema.Table(database.Schema, "flow_commands")+` WHERE command_id=$1)
		ORDER BY position DESC LIMIT 3) recent ORDER BY position`, result.Commands[0].CommandID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var journalOrder []string
	for rows.Next() {
		var kind, commandID, terminal string
		if err := rows.Scan(&kind, &commandID, &terminal); err != nil {
			t.Fatal(err)
		}
		journalOrder = append(journalOrder, kind+":"+commandID+":"+terminal)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{
		"attempt_started:" + result.Commands[0].CommandID.String() + ":",
		"event_recorded:" + expiredID + ":failed",
		"execution_failing::",
	}
	if fmt.Sprint(journalOrder) != fmt.Sprint(wantOrder) {
		t.Fatalf("journal order=%v, want %v", journalOrder, wantOrder)
	}
	var failingBody []byte
	if err := database.DB.Conn.QueryRow(ctx, `SELECT body FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=(SELECT execution_id FROM `+pgschema.Table(database.Schema, "flow_commands")+` WHERE command_id=$1)
		  AND entry_kind='execution_failing' ORDER BY position DESC LIMIT 1`, result.Commands[0].CommandID).
		Scan(&failingBody); err != nil {
		t.Fatal(err)
	}
	failing, err := journalcodec.Decode[struct {
		V         int      `json:"v"`
		Survivors []string `json:"survivors"`
	}](failingBody)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(failing.Survivors) != "[child/eligible]" {
		t.Fatalf("execution failing survivors=%v", failing.Survivors)
	}
}

func TestClaimBatchRollbackAndAmbiguousCommitFences(t *testing.T) {
	t.Parallel()

	t.Run("malformed durable policy", func(t *testing.T) {
		database := testpg.Open(t)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		child := DefineCommand[None, None]("claim.batch_malformed_child", 1)
		runtime, execution := stageClaimFixture(t, database, "malformed", 2, func(work *Work[None]) {
			Execute(work, "child/good", child, None{})
			Execute(work, "child/malformed", child, None{})
		})
		if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_commands")+`
			SET retry_policy=convert_to('{}','UTF8') WHERE execution_id=$1 AND command_key='child/malformed'`,
			execution.ID); err != nil {
			t.Fatal(err)
		}
		candidates := probeClaimCandidates(t, runtime,
			[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 2)
		if _, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "claim-batch-test", fault.None{}); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("ClaimCommands() error=%v", err)
		}
		var ready, starts int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+`
			 WHERE execution_id=$1 AND parent_command_id=$2 AND state='ready'),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
			 WHERE execution_id=$1 AND entry_kind='attempt_started')`, execution.ID, execution.RootCommandID).
			Scan(&ready, &starts); err != nil {
			t.Fatal(err)
		}
		if ready != 2 || starts != 1 {
			t.Fatalf("malformed batch ready=%d attempt starts=%d", ready, starts)
		}
	})

	t.Run("malformed event input", func(t *testing.T) {
		database := testpg.Open(t)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		event := DefineEvent[None]("claim.batch_malformed_event")
		child := DefineCommand[None, None]("claim.batch_malformed_event_child", 1)
		runtime, execution := stageClaimFixture(t, database, "malformed_event", 2, func(work *Work[None]) {
			if err := Emit(work, event, "input", None{}); err != nil {
				t.Fatalf("Emit() error = %v", err)
			}
			Execute(work, "child/good", child, None{})
			Execute(work, "child/corrupt", child, None{}).WaitFor(event, "input")
		})
		if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_journal")+`
			SET body=convert_to('{','UTF8')
			WHERE execution_id=$1 AND entry_kind='event_recorded' AND event_class='application'`, execution.ID); err != nil {
			t.Fatal(err)
		}
		candidates := probeClaimCandidates(t, runtime,
			[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 2)
		if _, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "claim-batch-test", fault.None{}); !errors.Is(err, ErrInvalidState) {
			t.Fatalf("ClaimCommands() error=%v", err)
		}
		var readyCommands, readyQueues, activeFences, starts int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+`
			 WHERE execution_id=$1 AND parent_command_id=$2 AND state='ready'),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
			 WHERE execution_id=$1 AND state='ready'),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
			 WHERE execution_id=$1 AND (active_attempt_id IS NOT NULL OR lease_token IS NOT NULL)),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
			 WHERE execution_id=$1 AND entry_kind='attempt_started')`, execution.ID, execution.RootCommandID).
			Scan(&readyCommands, &readyQueues, &activeFences, &starts); err != nil {
			t.Fatal(err)
		}
		if readyCommands != 2 || readyQueues != 2 || activeFences != 0 || starts != 1 {
			t.Fatalf("malformed event batch commands=%d queues=%d active fences=%d attempt starts=%d",
				readyCommands, readyQueues, activeFences, starts)
		}
	})

	t.Run("rollback", func(t *testing.T) {
		database := testpg.Open(t)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		child := DefineCommand[None, None]("claim.batch_rollback_child", 1)
		runtime, execution := stageClaimFixture(t, database, "rollback", 4, func(work *Work[None]) {
			for index := range 4 {
				Execute(work, fmt.Sprintf("child/%d", index), child, None{})
			}
		})
		candidates := probeClaimCandidates(t, runtime, []store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 4)
		_, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "claim-batch-test", fault.Func(
			func(_ context.Context, point fault.Point) error {
				if point == fault.ClaimBeforeCommit {
					return fault.Injected(point)
				}
				return nil
			}))
		if !errors.Is(err, fault.ErrInjected) {
			t.Fatalf("ClaimCommands() error=%v", err)
		}
		var ready, starts int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+`
			 WHERE execution_id=$1 AND parent_command_id=$2 AND state='ready'),
			(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
			 WHERE execution_id=$1 AND entry_kind='attempt_started')`, execution.ID, execution.RootCommandID).
			Scan(&ready, &starts); err != nil {
			t.Fatal(err)
		}
		if ready != 4 || starts != 1 {
			// The one retained attempt belongs to the already-settled fixture root.
			t.Fatalf("rollback ready=%d attempt starts=%d", ready, starts)
		}
	})

	t.Run("commit error preserves prepared fences", func(t *testing.T) {
		database := testpg.Open(t)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		child := DefineCommand[None, None]("claim.batch_commit_error_child", 1)
		runtime, _ := stageClaimFixture(t, database, "commit_error", 2, func(work *Work[None]) {
			Execute(work, "child/first", child, None{})
			Execute(work, "child/second", child, None{})
		})
		candidates := probeClaimCandidates(t, runtime,
			[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 2)
		claimCtx, cancelClaim := context.WithCancel(ctx)
		result, err := runtime.store.ClaimCommands(claimCtx, candidates, time.Minute, "claim-batch-test", fault.Func(
			func(_ context.Context, point fault.Point) error {
				if point == fault.ClaimBeforeCommit {
					cancelClaim()
				}
				return nil
			}))
		cancelClaim()
		if err == nil {
			t.Fatal("ClaimCommands() unexpectedly committed with a cancelled commit context")
		}
		if len(result.Commands) != 2 {
			t.Fatalf("ClaimCommands() commit-error commands=%d, want 2", len(result.Commands))
		}
		for _, command := range result.Commands {
			if command.AttemptID == uuid.Nil || command.LeaseToken == uuid.Nil {
				t.Fatalf("commit-error command omitted prepared fence metadata: %+v", command)
			}
		}
	})

	t.Run("ambiguous", func(t *testing.T) {
		database := testpg.Open(t)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		child := DefineCommand[None, None]("claim.batch_ambiguous_child", 1)
		runtime, _ := stageClaimFixture(t, database, "ambiguous", 4, func(work *Work[None]) {
			for index := range 4 {
				Execute(work, fmt.Sprintf("child/%d", index), child, None{})
			}
		})
		candidates := probeClaimCandidates(t, runtime, []store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 4)
		result, err := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "claim-batch-test", fault.Func(
			func(_ context.Context, point fault.Point) error {
				if point == fault.ClaimCommitAmbiguous {
					return fault.Injected(point)
				}
				return nil
			}))
		if !errors.Is(err, fault.ErrInjected) || len(result.Commands) != 4 {
			t.Fatalf("ClaimCommands() commands=%d error=%v", len(result.Commands), err)
		}
		for _, command := range result.Commands {
			ownership, resolveErr := runtime.store.ResolveCommandAttempt(ctx, command.CommandID,
				command.AttemptID, command.LeaseToken)
			if resolveErr != nil || ownership != store.AttemptOwnershipStillOwned {
				t.Fatalf("ResolveCommandAttempt(%s)=%s, %v", command.CommandID, ownership, resolveErr)
			}
		}
	})
}

func TestClaimBatchCompetingReplicasCreateOneFencePerCommand(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	child := DefineCommand[None, None]("claim.batch_replica_child", 1)
	first, execution := stageClaimFixture(t, database, "replicas", 8, func(work *Work[None]) {
		for index := range 8 {
			Execute(work, fmt.Sprintf("child/%d", index), child, None{})
		}
	})
	second, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	candidates := probeClaimCandidates(t, first, []store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 8)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	hook := fault.Func(func(ctx context.Context, point fault.Point) error {
		if point != fault.ClaimExecutionLock {
			return nil
		}
		select {
		case entered <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	type response struct {
		result store.ClaimBatchResult
		err    error
	}
	responses := make(chan response, 2)
	var group sync.WaitGroup
	group.Add(2)
	for _, runtime := range []*Runtime{first, second} {
		runtime := runtime
		go func() {
			defer group.Done()
			result, claimErr := runtime.store.ClaimCommands(ctx, candidates, time.Minute, "claim-replica", hook)
			responses <- response{result: result, err: claimErr}
		}()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("competing claim did not reach execution-lock barrier")
		}
	}
	close(release)
	group.Wait()
	close(responses)
	claimed := 0
	for response := range responses {
		if response.err != nil {
			t.Fatal(response.err)
		}
		claimed += len(response.result.Commands)
	}
	if claimed != 8 {
		t.Fatalf("competing replicas claimed %d commands, want 8", claimed)
	}
	var starts, distinctAttempts int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*),count(DISTINCT attempt_id)
	FROM `+pgschema.Table(database.Schema, "flow_journal")+`
	WHERE execution_id=$1 AND entry_kind='attempt_started' AND command_id<>$2`, execution.ID, execution.RootCommandID).
		Scan(&starts, &distinctAttempts); err != nil {
		t.Fatal(err)
	}
	if starts != 8 || distinctAttempts != 8 {
		t.Fatalf("attempt starts=%d distinct fences=%d", starts, distinctAttempts)
	}
}

func stageClaimFixture(
	t *testing.T,
	database testpg.Database,
	suffix string,
	children int,
	stage func(*Work[None]),
) (*Runtime, Execution) {
	t.Helper()
	ctx := context.Background()
	parent := DefineCommand[None, None]("claim.fixture_parent_"+suffix, 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(0),
		WithWorkerConcurrency(1), WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(parent, func(_ context.Context, work *Work[None]) (None, error) {
		stage(work)
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	execution, err := parent.With(runtime).Execute(ctx, "claim/fixture/"+suffix, None{})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var count int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT command_count FROM `+
			pgschema.Table(database.Schema, "flow_executions")+` WHERE execution_id=$1`, execution.ID).Scan(&count); err != nil {
			cancel()
			t.Fatal(err)
		}
		if count == children+1 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("fixture command count=%d, want %d", count, children+1)
		}
		time.Sleep(5 * time.Millisecond)
	}
	stopRuntime(t, cancel, runResult)
	return runtime, execution
}

func probeClaimCandidates(
	t *testing.T,
	runtime *Runtime,
	kinds []store.CommandKind,
	want int,
) []store.CommandCandidate {
	t.Helper()
	candidates, err := runtime.store.ProbeCommands(context.Background(), kinds, want+8)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != want {
		t.Fatalf("ProbeCommands() candidates=%d, want %d", len(candidates), want)
	}
	return candidates
}
