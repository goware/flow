package flow

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
	"github.com/goware/pgkit/v2"
)

func TestPruneTerminalRunsDeletesOnlyEligibleCompleteAggregates(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().UTC().Add(-time.Hour)
	old := cutoff.Add(-time.Hour)
	eligible := []uuid.UUID{
		seedRetentionAggregate(t, database.DB, database.Schema, old, "succeeded", "", "permanent", true),
		seedRetentionAggregate(t, database.DB, database.Schema, old.Add(time.Second), "failed", "", "permanent", true),
		seedRetentionAggregate(t, database.DB, database.Schema, old.Add(2*time.Second), "cancelled", "", "permanent", true),
		seedRetentionAggregate(t, database.DB, database.Schema, old.Add(3*time.Second), "expired", "", "permanent", true),
		seedRetentionAggregate(t, database.DB, database.Schema, old.Add(4*time.Second), "succeeded", "generation", "live", true),
		seedRetentionAggregate(t, database.DB, database.Schema, old.Add(5*time.Second), "succeeded", "generation", "live", true),
	}
	permanent := seedRetentionAggregate(t, database.DB, database.Schema, old, "succeeded", "permanent", "permanent", true)
	running := seedRetentionAggregate(t, database.DB, database.Schema, time.Time{}, "running", "", "permanent", true)
	atCutoff := seedRetentionAggregate(t, database.DB, database.Schema, cutoff, "succeeded", "", "permanent", true)
	if _, err := database.DB.Conn.Exec(ctx, `CREATE TABLE `+pgschema.Table(database.Schema, "retention_application_rows")+`
		(run_id uuid PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	for _, id := range eligible {
		if _, err := database.DB.Conn.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "retention_application_rows")+`
			(run_id) VALUES ($1)`, id); err != nil {
			t.Fatal(err)
		}
	}

	result, err := PruneTerminalRuns(ctx, runtime, cutoff, 100)
	if err != nil {
		t.Fatal(err)
	}
	if result != (PruneResult{Runs: 6, Commands: 12, JournalEntries: 12}) {
		t.Fatalf("prune result = %#v", result)
	}
	assertRetentionRows(t, database.DB, database.Schema, eligible, 0, 0, 0, 0, 0)
	assertRetentionRows(t, database.DB, database.Schema, []uuid.UUID{permanent, running, atCutoff}, 3, 6, 6, 3, 3)
	var applicationRows int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+
		pgschema.Table(database.Schema, "retention_application_rows")).Scan(&applicationRows); err != nil {
		t.Fatal(err)
	}
	if applicationRows != len(eligible) {
		t.Fatalf("application rows = %d, want %d", applicationRows, len(eligible))
	}
	empty, err := PruneTerminalRuns(ctx, runtime, cutoff, 100)
	if err != nil || empty != (PruneResult{}) {
		t.Fatalf("empty prune = %#v, %v", empty, err)
	}
}

func TestPruneTerminalRunsValidatesBoundsAndUsesDeterministicSkipLockedBatches(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().UTC()
	if _, err := PruneTerminalRuns(ctx, nil, cutoff, 1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil runtime error = %v", err)
	}
	for _, test := range []struct {
		cutoff time.Time
		limit  int
	}{
		{time.Time{}, 1}, {cutoff, 0}, {cutoff, -1}, {cutoff, 1001},
	} {
		if _, err := PruneTerminalRuns(ctx, runtime, test.cutoff, test.limit); !errors.Is(err, ErrInvalid) {
			t.Fatalf("PruneTerminalRuns(%v,%d) error = %v", test.cutoff, test.limit, err)
		}
	}

	oldest := seedRetentionAggregate(t, database.DB, database.Schema, cutoff.Add(-3*time.Hour), "succeeded", "", "permanent", false)
	middle := seedRetentionAggregate(t, database.DB, database.Schema, cutoff.Add(-2*time.Hour), "succeeded", "", "permanent", false)
	newest := seedRetentionAggregate(t, database.DB, database.Schema, cutoff.Add(-time.Hour), "succeeded", "", "permanent", false)
	first, err := PruneTerminalRuns(ctx, runtime, cutoff, 2)
	if err != nil || first.Runs != 2 {
		t.Fatalf("first bounded prune = %#v, %v", first, err)
	}
	assertRetentionRows(t, database.DB, database.Schema, []uuid.UUID{oldest, middle}, 0, 0, 0, 0, 0)
	assertRetentionRows(t, database.DB, database.Schema, []uuid.UUID{newest}, 1, 2, 2, 0, 0)

	locked := newest
	later := seedRetentionAggregate(t, database.DB, database.Schema, cutoff.Add(-30*time.Minute), "succeeded", "", "permanent", false)
	lockTx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback(ctx)
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM `+pgschema.Table(database.Schema, "flow_runs")+`
		WHERE run_id=$1 FOR UPDATE`, locked); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	skipped, err := PruneTerminalRuns(ctx, runtime, cutoff, 1)
	if err != nil || skipped.Runs != 1 || time.Since(started) > time.Second {
		t.Fatalf("skip-locked prune = %#v, %v in %s", skipped, err, time.Since(started))
	}
	assertRetentionRows(t, database.DB, database.Schema, []uuid.UUID{locked}, 1, 2, 2, 0, 0)
	assertRetentionRows(t, database.DB, database.Schema, []uuid.UUID{later}, 0, 0, 0, 0, 0)
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	last, err := PruneTerminalRuns(ctx, runtime, cutoff, 1)
	if err != nil || last.Runs != 1 {
		t.Fatalf("last prune = %#v, %v", last, err)
	}
}

func TestPruneTerminalRunsConcurrentWorkersDoNotDoubleCount(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().UTC()
	for index := range 40 {
		seedRetentionAggregate(t, database.DB, database.Schema, cutoff.Add(-time.Duration(index+1)*time.Second),
			"succeeded", "", "permanent", false)
	}
	type response struct {
		result PruneResult
		err    error
	}
	responses := make(chan response, 2)
	var start sync.WaitGroup
	start.Add(1)
	for range 2 {
		go func() {
			start.Wait()
			result, err := PruneTerminalRuns(ctx, runtime, cutoff, 20)
			responses <- response{result: result, err: err}
		}()
	}
	start.Done()
	var total PruneResult
	for range 2 {
		response := <-responses
		if response.err != nil {
			t.Fatal(response.err)
		}
		total.Runs += response.result.Runs
		total.Commands += response.result.Commands
		total.JournalEntries += response.result.JournalEntries
	}
	if total != (PruneResult{Runs: 40, Commands: 80, JournalEntries: 80}) {
		t.Fatalf("concurrent prune total = %#v", total)
	}
}

func TestPruneTerminalRunsKeepsTraceCoherentAndUnrelatedRuntimeProgressing(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("retention.integration", 1)
	runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(4),
		WithPollInterval(5*time.Millisecond), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(context.Context, *Work[None]) (None, error) {
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	cancel, runResult := startRuntime(t, runtime)
	defer stopRuntime(t, cancel, runResult)

	ids := make([]RunID, 12)
	for index := range ids {
		started, err := command.Enqueue(ctx, runtime, "", None{})
		if err != nil {
			t.Fatal(err)
		}
		ids[index] = started.RunID
	}
	for _, id := range ids {
		waitForRunStatus(t, database.Schema, database.DB.Conn, id, "succeeded", 5*time.Second)
	}
	progress, err := command.Enqueue(ctx, runtime, "retention/progress", None{})
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	errorsSeen := make(chan error, len(ids))
	for _, id := range ids {
		go func(id RunID) {
			<-start
			trace, err := Trace(ctx, runtime, id)
			if err == nil && (trace.Run.ID != id || len(trace.Commands) != 1 || len(trace.History) == 0) {
				err = fmt.Errorf("incomplete trace for %s: %#v", id, trace)
			}
			if err != nil && !errors.Is(err, ErrNotFound) {
				err = fmt.Errorf("trace %s: %w", id, err)
			} else if errors.Is(err, ErrNotFound) {
				err = nil
			}
			errorsSeen <- err
		}(id)
	}
	close(start)
	pruned, err := PruneTerminalRuns(ctx, runtime, time.Now().UTC().Add(time.Second), len(ids))
	if err != nil || pruned.Runs != int64(len(ids)) {
		t.Fatalf("concurrent trace prune = %#v, %v", pruned, err)
	}
	for range ids {
		if err := <-errorsSeen; err != nil {
			t.Fatal(err)
		}
	}
	awaitCtx, awaitCancel := context.WithTimeout(ctx, 5*time.Second)
	defer awaitCancel()
	progressRun, err := AwaitRun(awaitCtx, runtime, progress.RunID)
	if err != nil || progressRun.Status != RunStatusSucceeded {
		t.Fatalf("unrelated run = %#v, %v", progressRun, err)
	}
}

func seedRetentionAggregate(t *testing.T, db *pgkit.DB, schema string, finishedAt time.Time,
	status, key, keyScope string, withOwnedRows bool) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	runID, rootID, childID := uuid.New(), uuid.New(), uuid.New()
	createdAt := time.Now().UTC().Add(-24 * time.Hour)
	var finished any
	openCommands := 2
	if !finishedAt.IsZero() {
		finished = finishedAt
		openCommands = 0
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(schema, "flow_runs")+` (
		run_id,definition_name,definition_version,run_key,key_scope,status,start_fingerprint,
		max_commands,command_count,open_commands,next_journal_position,root_command_id,
		created_at,updated_at,status_at,finished_at
	) VALUES ($1,'retention.fixture',1,$2,$3,$4,$5,100,2,$6,3,$7,$8,$8,$8,$9)`,
		runID, key, keyScope, status, make([]byte, 32), openCommands, rootID, createdAt, finished); err != nil {
		t.Fatal(err)
	}
	commandState := status
	if status == "running" {
		commandState = "ready"
	}
	var result []byte
	var terminalPosition any
	var commandFinished any
	if status == "succeeded" {
		result = []byte(`{}`)
	}
	if !finishedAt.IsZero() {
		terminalPosition = int64(2)
		commandFinished = finishedAt
	}
	insertCommand := func(id uuid.UUID, key string, parent any, position int64) {
		if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(schema, "flow_commands")+` (
			command_id,run_id,command_key,name,version,parent_command_id,args,declaration_fingerprint,
			state,unsatisfied_waits,queue,retry_policy,budget_started_at,next_attempt_at,result,
			terminal_position,created_position,created_at,updated_at,status_at,finished_at
		) VALUES ($1,$2,$3,'retention.fixture',1,$4,$5,$6,$7,0,'default',$8,$9,$9,$10,$11,$12,$9,$9,$9,$13)`,
			id, runID, key, parent, []byte(`{}`), make([]byte, 32), commandState, []byte(`{}`), createdAt,
			result, terminalPosition, position, commandFinished); err != nil {
			t.Fatal(err)
		}
	}
	insertCommand(rootID, "root", nil, 1)
	insertCommand(childID, "child", rootID, 2)
	if withOwnedRows {
		if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(schema, "flow_command_queue")+`
			(command_id,run_id,queue,name,version,state,next_run_at)
			VALUES ($1,$2,'default','retention.fixture',1,'ready',$3)`, childID, runID, createdAt); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(schema, "flow_command_event_waits")+`
			(command_id,run_id,event_name,event_key) VALUES ($1,$2,'retention.event','key')`, childID, runID); err != nil {
			t.Fatal(err)
		}
	}
	body := []byte(`{}`)
	hash := sha256.Sum256(body)
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(schema, "flow_journal")+`
		(run_id,position,entry_id,entry_kind,recorded_at,body,body_hash)
		VALUES ($1,1,$2,'run_started',$4,$5,$6),($1,2,$3,'run_failing',$4,$5,$6)`,
		runID, uuid.New(), uuid.New(), createdAt, body, hash[:]); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return runID
}

func assertRetentionRows(t *testing.T, db *pgkit.DB, schema string, runIDs []uuid.UUID,
	runs, commands, journal, queue, waits int) {
	t.Helper()
	ctx := context.Background()
	tables := []struct {
		name string
		want int
	}{
		{"flow_runs", runs}, {"flow_commands", commands}, {"flow_journal", journal},
		{"flow_command_queue", queue}, {"flow_command_event_waits", waits},
	}
	for _, table := range tables {
		var got int
		if err := db.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(schema, table.name)+
			` WHERE run_id=ANY($1::uuid[])`, runIDs).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != table.want {
			t.Fatalf("%s rows for %s = %d, want %d", table.name, fmt.Sprint(runIDs), got, table.want)
		}
	}
}
