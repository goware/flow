package flow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
)

type replaceArgs struct {
	Generation int `json:"generation"`
}

func TestReplaceCurrentRunUsesExpectedIDBeforeEquivalence(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[replaceArgs, None]("replace.expected_first", 1)
	options := []RunOption{WithLiveKey(), WithStartDelay(time.Hour)}
	original, err := command.Enqueue(ctx, runtime, "intent/42", replaceArgs{Generation: 1}, options...)
	if err != nil {
		t.Fatal(err)
	}

	replaced, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/42",
		replaceArgs{Generation: 1}, "operator retry", options...)
	if err != nil {
		t.Fatal(err)
	}
	if !replaced.Replaced || replaced.RunID == original.RunID {
		t.Fatalf("identical replacement = %#v", replaced)
	}
	old, err := GetRun(ctx, runtime, original.RunID)
	if err != nil || old.Status != RunStatusCancelled {
		t.Fatalf("old run = %#v, %v", old, err)
	}
	current, found, err := GetCurrentRun(ctx, runtime, command.Name(), "intent/42")
	if err != nil || !found || current.ID != replaced.RunID {
		t.Fatalf("current replacement = %#v, %v, %v", current, found, err)
	}

	rediscovered, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/42",
		replaceArgs{Generation: 1}, "operator retry", options...)
	if err != nil || rediscovered.Replaced || rediscovered.RunID != replaced.RunID {
		t.Fatalf("equivalent retry = %#v, %v", rediscovered, err)
	}
	if _, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/42",
		replaceArgs{Generation: 2}, "stale retry", options...); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale different replacement error = %v, want ErrConflict", err)
	}
	current, found, err = GetCurrentRun(ctx, runtime, command.Name(), "intent/42")
	if err != nil || !found || current.ID != replaced.RunID {
		t.Fatalf("stale replacement changed current = %#v, %v, %v", current, found, err)
	}

	var runs, live int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE status IN ('running','failing'))
		FROM `+pgschema.Table(database.Schema, "flow_runs")+`
		WHERE definition_name=$1 AND run_key=$2`, command.Name(), "intent/42").Scan(&runs, &live); err != nil {
		t.Fatal(err)
	}
	if runs != 2 || live != 1 {
		t.Fatalf("replacement generations = %d total, %d live", runs, live)
	}
	assertReplayMatches(t, runtime, original.RunID)
	assertReplayMatches(t, runtime, replaced.RunID)
}

func TestReplaceCurrentRunValidatesLiveRootAndAbsence(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[replaceArgs, None]("replace.validation", 1)
	original, err := command.Enqueue(ctx, runtime, "intent/validation", replaceArgs{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/validation", replaceArgs{}, "missing live option"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("permanent replacement error = %v, want ErrInvalid", err)
	}
	if _, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "", replaceArgs{}, "empty key", WithLiveKey()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty-key replacement error = %v, want ErrInvalid", err)
	}
	if err := CancelRun(ctx, runtime, original.RunID, "validation complete"); err != nil {
		t.Fatal(err)
	}
	if _, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/validation", replaceArgs{}, "no current", WithLiveKey(), WithStartDelay(time.Hour)); !errors.Is(err, ErrConflict) {
		t.Fatalf("absent replacement error = %v, want ErrConflict", err)
	}
}

func TestReplaceCurrentRunCallerTransactionRollback(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `CREATE TABLE `+pgschema.Table(database.Schema, "replace_records")+` (id text PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[replaceArgs, None]("replace.rollback", 1)
	original, err := command.Enqueue(ctx, runtime, "intent/rollback", replaceArgs{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	flowTx := runtime.InTx(tx)
	replacement, err := command.ReplaceCurrentRun(ctx, flowTx, original.RunID, "intent/rollback",
		replaceArgs{}, "rollback replacement", WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil || !replacement.Replaced {
		_ = tx.Rollback(ctx)
		t.Fatalf("transaction replacement = %#v, %v", replacement, err)
	}
	current, found, err := GetCurrentRun(ctx, flowTx, command.Name(), "intent/rollback")
	if err != nil || !found || current.ID != replacement.RunID {
		_ = tx.Rollback(ctx)
		t.Fatalf("transaction current = %#v, %v, %v", current, found, err)
	}
	if err := flowTx.BeginApplicationWrites(); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "replace_records")+` (id) VALUES ('rollback')`); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	current, found, err = GetCurrentRun(ctx, runtime, command.Name(), "intent/rollback")
	if err != nil || !found || current.ID != original.RunID || current.Status != RunStatusRunning {
		t.Fatalf("current after rollback = %#v, %v, %v", current, found, err)
	}
	if _, err := GetRun(ctx, runtime, replacement.RunID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back replacement lookup error = %v, want ErrNotFound", err)
	}
	var records int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "replace_records")).Scan(&records); err != nil || records != 0 {
		t.Fatalf("rolled-back records = %d, %v", records, err)
	}
}

func TestReplaceCurrentRunCallerTransactionCommit(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := database.DB.Conn.Exec(ctx, `CREATE TABLE `+pgschema.Table(database.Schema, "replace_commit_records")+` (id text PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[replaceArgs, None]("replace.commit", 1)
	original, err := command.Enqueue(ctx, runtime, "intent/commit", replaceArgs{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	flowTx := runtime.InTx(tx)
	replacement, err := command.ReplaceCurrentRun(ctx, flowTx, original.RunID, "intent/commit",
		replaceArgs{Generation: 2}, "committed replacement", WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil || !replacement.Replaced {
		t.Fatalf("transaction replacement = %#v, %v", replacement, err)
	}
	if err := flowTx.BeginApplicationWrites(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "replace_commit_records")+` (id) VALUES ('commit')`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	current, found, err := GetCurrentRun(ctx, runtime, command.Name(), "intent/commit")
	if err != nil || !found || current.ID != replacement.RunID {
		t.Fatalf("committed current = %#v, %v, %v", current, found, err)
	}
	old, err := GetRun(ctx, runtime, original.RunID)
	if err != nil || old.Status != RunStatusCancelled {
		t.Fatalf("committed predecessor = %#v, %v", old, err)
	}
	var records int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "replace_commit_records")).Scan(&records); err != nil || records != 1 {
		t.Fatalf("committed records = %d, %v", records, err)
	}
	assertReplayMatches(t, runtime, original.RunID)
	assertReplayMatches(t, runtime, replacement.RunID)
}

func TestConcurrentEquivalentCurrentRunReplacementCreatesOneSuccessor(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[replaceArgs, None]("replace.concurrent", 1)
	original, err := command.Enqueue(ctx, runtime, "intent/concurrent", replaceArgs{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result ReplaceRunResult
		err    error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	var ready sync.WaitGroup
	ready.Add(2)
	for range 2 {
		go func() {
			ready.Done()
			<-start
			result, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/concurrent",
				replaceArgs{}, "concurrent retry", WithLiveKey(), WithStartDelay(time.Hour))
			outcomes <- outcome{result: result, err: err}
		}()
	}
	ready.Wait()
	close(start)
	first, second := <-outcomes, <-outcomes
	if first.err != nil || second.err != nil || first.result.RunID != second.result.RunID || first.result.Replaced == second.result.Replaced {
		t.Fatalf("concurrent outcomes = %#v / %#v", first, second)
	}
	var live int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_runs")+`
		WHERE definition_name=$1 AND run_key=$2 AND status IN ('running','failing')`, command.Name(), "intent/concurrent").Scan(&live); err != nil {
		t.Fatal(err)
	}
	if live != 1 {
		t.Fatalf("live successors = %d, want 1", live)
	}
}

func TestConcurrentDifferentCurrentRunReplacementRejectsLoser(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[replaceArgs, None]("replace.concurrent_different", 1)
	original, err := command.Enqueue(ctx, runtime, "intent/concurrent-different", replaceArgs{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		generation int
		result     ReplaceRunResult
		err        error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, 2)
	for generation := 1; generation <= 2; generation++ {
		go func(generation int) {
			<-start
			result, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/concurrent-different",
				replaceArgs{Generation: generation}, "concurrent different retry", WithLiveKey(), WithStartDelay(time.Hour))
			outcomes <- outcome{generation: generation, result: result, err: err}
		}(generation)
	}
	close(start)
	first, second := <-outcomes, <-outcomes
	values := []outcome{first, second}
	successes, conflicts := 0, 0
	var winner outcome
	for _, value := range values {
		switch {
		case value.err == nil && value.result.Replaced:
			successes++
			winner = value
		case errors.Is(value.err, ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent outcome = %#v", value)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent outcomes = %#v / %#v", first, second)
	}
	current, found, err := GetCurrentRun(ctx, runtime, command.Name(), "intent/concurrent-different")
	if err != nil || !found || current.ID != winner.result.RunID {
		t.Fatalf("current winner = %#v, found=%v err=%v generation=%d", current, found, err, winner.generation)
	}
}

func TestReplaceCurrentRunRollsBackBeforeCommitFault(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[replaceArgs, None]("replace.rollback_fault", 1)
	original, err := command.Enqueue(ctx, runtime, "intent/rollback-fault", replaceArgs{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	runtime.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.IngressBeforeCommit {
			return fault.Injected(point)
		}
		return nil
	})
	if _, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/rollback-fault",
		replaceArgs{Generation: 1}, "faulted retry", WithLiveKey(), WithStartDelay(time.Hour)); !errors.Is(err, fault.ErrInjected) {
		t.Fatalf("faulted replacement error = %v", err)
	}
	runtime.faults = fault.None{}
	current, found, err := GetCurrentRun(ctx, runtime, command.Name(), "intent/rollback-fault")
	if err != nil || !found || current.ID != original.RunID || current.Status != RunStatusRunning {
		t.Fatalf("current after fault = %#v, %v, %v", current, found, err)
	}
	var generations int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_runs")+`
		WHERE definition_name=$1 AND run_key=$2`, command.Name(), "intent/rollback-fault").Scan(&generations); err != nil {
		t.Fatal(err)
	}
	if generations != 1 {
		t.Fatalf("faulted replacement generations = %d, want 1", generations)
	}
}

func TestReplaceCurrentRunRollsBackWhenSuccessorInsertFails(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[replaceArgs, None]("replace.insert_fault", 1)
	original, err := command.Enqueue(ctx, runtime, "intent/insert-fault", replaceArgs{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	schema := pgschema.Quote(database.Schema)
	if _, err := database.DB.Conn.Exec(ctx, `CREATE FUNCTION `+schema+`.reject_replacement_insert()
		RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'replacement insert rejected'; END $$;
		CREATE TRIGGER reject_replacement_insert BEFORE INSERT ON `+schema+`.flow_runs
		FOR EACH ROW EXECUTE FUNCTION `+schema+`.reject_replacement_insert()`); err != nil {
		t.Fatal(err)
	}
	if _, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/insert-fault",
		replaceArgs{Generation: 2}, "insert failure", WithLiveKey(), WithStartDelay(time.Hour)); err == nil {
		t.Fatal("replacement unexpectedly survived successor insert failure")
	}

	current, found, err := GetCurrentRun(ctx, runtime, command.Name(), "intent/insert-fault")
	if err != nil || !found || current.ID != original.RunID || current.Status != RunStatusRunning {
		t.Fatalf("current after insert failure = %#v, %v, %v", current, found, err)
	}
	var generations int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+schema+`.flow_runs
		WHERE definition_name=$1 AND run_key=$2`, command.Name(), "intent/insert-fault").Scan(&generations); err != nil {
		t.Fatal(err)
	}
	if generations != 1 {
		t.Fatalf("generations after insert failure = %d, want 1", generations)
	}
	assertReplayMatches(t, runtime, original.RunID)
}

func TestReplaceCurrentRunRacingCancellationHasOneAtomicOutcome(t *testing.T) {
	t.Parallel()
	for iteration := range 12 {
		database := testpg.Open(t)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		runtime, err := New(database.DB, WithSchema(database.Schema))
		if err != nil {
			t.Fatal(err)
		}
		command := DefineCommand[replaceArgs, None]("replace.cancel_race", 1)
		key := fmt.Sprintf("intent/cancel-race/%d", iteration)
		original, err := command.Enqueue(ctx, runtime, key, replaceArgs{}, WithLiveKey(), WithStartDelay(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		replaceResult := make(chan struct {
			value ReplaceRunResult
			err   error
		}, 1)
		cancelResult := make(chan error, 1)
		go func() {
			<-start
			value, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, key,
				replaceArgs{Generation: 2}, "replacement won", WithLiveKey(), WithStartDelay(time.Hour))
			replaceResult <- struct {
				value ReplaceRunResult
				err   error
			}{value: value, err: err}
		}()
		go func() {
			<-start
			cancelResult <- CancelRun(ctx, runtime, original.RunID, "cancellation won")
		}()
		close(start)
		replaced, cancelled := <-replaceResult, <-cancelResult

		switch {
		case replaced.err == nil:
			if !replaced.value.Replaced || (!errors.Is(cancelled, ErrTerminal) && cancelled != nil) {
				t.Fatalf("iteration %d replacement winner = %#v, cancel error %v", iteration, replaced, cancelled)
			}
			current, found, err := GetCurrentRun(ctx, runtime, command.Name(), key)
			if err != nil || !found || current.ID != replaced.value.RunID {
				t.Fatalf("iteration %d current replacement = %#v, %v, %v", iteration, current, found, err)
			}
		case errors.Is(replaced.err, ErrConflict):
			if cancelled != nil {
				t.Fatalf("iteration %d cancellation winner error = %v", iteration, cancelled)
			}
			if _, found, err := GetCurrentRun(ctx, runtime, command.Name(), key); err != nil || found {
				t.Fatalf("iteration %d cancelled key current found=%v err=%v", iteration, found, err)
			}
		default:
			t.Fatalf("iteration %d replacement error = %v, cancel error = %v", iteration, replaced.err, cancelled)
		}
		var live int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_runs")+`
			WHERE definition_name=$1 AND run_key=$2 AND status IN ('running','failing')`, command.Name(), key).Scan(&live); err != nil {
			t.Fatal(err)
		}
		if live > 1 {
			t.Fatalf("iteration %d live holders = %d", iteration, live)
		}
		assertReplayMatches(t, runtime, original.RunID)
		if replaced.err == nil {
			assertReplayMatches(t, runtime, replaced.value.RunID)
		}
	}
}

func TestReplaceCurrentRunRacingOrdinaryEnqueueKeepsOneLiveHolder(t *testing.T) {
	t.Parallel()
	for iteration := range 12 {
		database := testpg.Open(t)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		runtime, err := New(database.DB, WithSchema(database.Schema))
		if err != nil {
			t.Fatal(err)
		}
		command := DefineCommand[replaceArgs, None]("replace.enqueue_race", 1)
		key := fmt.Sprintf("intent/enqueue-race/%d", iteration)
		original, err := command.Enqueue(ctx, runtime, key, replaceArgs{}, WithLiveKey(), WithStartDelay(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		replaceResult := make(chan struct {
			value ReplaceRunResult
			err   error
		}, 1)
		enqueueResult := make(chan struct {
			value EnqueueResult
			err   error
		}, 1)
		go func() {
			<-start
			value, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, key,
				replaceArgs{Generation: 2}, "replacement", WithLiveKey(), WithStartDelay(time.Hour))
			replaceResult <- struct {
				value ReplaceRunResult
				err   error
			}{value: value, err: err}
		}()
		go func() {
			<-start
			value, err := command.Enqueue(ctx, runtime, key, replaceArgs{Generation: 2}, WithLiveKey(), WithStartDelay(time.Hour))
			enqueueResult <- struct {
				value EnqueueResult
				err   error
			}{value: value, err: err}
		}()
		close(start)
		replaced, enqueued := <-replaceResult, <-enqueueResult
		if replaced.err != nil || !replaced.value.Replaced || enqueued.err != nil || enqueued.value.Created {
			t.Fatalf("iteration %d replace=%#v enqueue=%#v", iteration, replaced, enqueued)
		}
		if enqueued.value.RunID != original.RunID && enqueued.value.RunID != replaced.value.RunID {
			t.Fatalf("iteration %d enqueue returned unrelated run %s", iteration, enqueued.value.RunID)
		}
		current, found, err := GetCurrentRun(ctx, runtime, command.Name(), key)
		if err != nil || !found || current.ID != replaced.value.RunID {
			t.Fatalf("iteration %d current = %#v, %v, %v", iteration, current, found, err)
		}
		var live int
		if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_runs")+`
			WHERE definition_name=$1 AND run_key=$2 AND status IN ('running','failing')`, command.Name(), key).Scan(&live); err != nil {
			t.Fatal(err)
		}
		if live != 1 {
			t.Fatalf("iteration %d live holders = %d, want 1", iteration, live)
		}
		assertReplayMatches(t, runtime, original.RunID)
		assertReplayMatches(t, runtime, replaced.value.RunID)
	}
}

func TestReplaceCurrentRunRacingTerminalSettlementHasOneWinner(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[replaceArgs, None]("replace.settlement_race", 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	runtime, err := New(database.DB, WithSchema(database.Schema), WithPollInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(Handle(command, func(context.Context, *Work[replaceArgs]) (None, error) {
		enterOnce.Do(func() { close(entered) })
		<-release
		return None{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	startRuntime(t, runtime)
	original, err := command.Enqueue(ctx, runtime, "intent/settlement-race", replaceArgs{}, WithLiveKey())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("original handler did not start")
	}
	start := make(chan struct{})
	replaceResult := make(chan struct {
		value ReplaceRunResult
		err   error
	}, 1)
	go func() {
		<-start
		value, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/settlement-race",
			replaceArgs{Generation: 2}, "settlement race", WithLiveKey(), WithStartDelay(time.Hour))
		replaceResult <- struct {
			value ReplaceRunResult
			err   error
		}{value: value, err: err}
	}()
	close(start)
	close(release)
	replaced := <-replaceResult
	waitForRunStatusAny(t, database.Schema, database.DB.Conn, original.RunID,
		[]string{"succeeded", "cancelled"}, 5*time.Second)
	old, err := GetRun(ctx, runtime, original.RunID)
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case replaced.err == nil:
		if !replaced.value.Replaced || old.Status != RunStatusCancelled {
			t.Fatalf("replacement winner = %#v, predecessor = %#v", replaced, old)
		}
		current, found, err := GetCurrentRun(ctx, runtime, command.Name(), "intent/settlement-race")
		if err != nil || !found || current.ID != replaced.value.RunID {
			t.Fatalf("replacement current = %#v, %v, %v", current, found, err)
		}
		assertReplayMatches(t, runtime, replaced.value.RunID)
	case errors.Is(replaced.err, ErrConflict):
		if old.Status != RunStatusSucceeded {
			t.Fatalf("settlement winner predecessor = %#v", old)
		}
		if _, found, err := GetCurrentRun(ctx, runtime, command.Name(), "intent/settlement-race"); err != nil || found {
			t.Fatalf("settled key current found=%v err=%v", found, err)
		}
	default:
		t.Fatalf("replacement error = %v", replaced.err)
	}
	assertReplayMatches(t, runtime, original.RunID)
}

func TestReplaceCurrentRunRacingExpiryHasOneAtomicOutcome(t *testing.T) {
	t.Parallel()
	for iteration := range 8 {
		database := testpg.Open(t)
		ctx := context.Background()
		if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
			t.Fatal(err)
		}
		runtime, err := New(database.DB, WithSchema(database.Schema))
		if err != nil {
			t.Fatal(err)
		}
		command := DefineCommand[replaceArgs, None]("replace.expiry_race", 1)
		key := fmt.Sprintf("intent/expiry-race/%d", iteration)
		original, err := command.Enqueue(ctx, runtime, key, replaceArgs{}, WithLiveKey(),
			WithRunDeadline(time.Millisecond), WithStartDelay(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
		originalID, err := parseRunID(original.RunID)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		replaceResult := make(chan struct {
			value ReplaceRunResult
			err   error
		}, 1)
		expiryResult := make(chan struct {
			changed bool
			err     error
		}, 1)
		go func() {
			<-start
			value, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, key,
				replaceArgs{Generation: 2}, "expiry race replacement", WithLiveKey(), WithStartDelay(time.Hour))
			replaceResult <- struct {
				value ReplaceRunResult
				err   error
			}{value: value, err: err}
		}()
		go func() {
			<-start
			changed, err := runtime.store.ExpireRun(ctx, originalID, "run deadline reached")
			expiryResult <- struct {
				changed bool
				err     error
			}{changed: changed, err: err}
		}()
		close(start)
		replaced, expired := <-replaceResult, <-expiryResult
		if expired.err != nil {
			t.Fatalf("iteration %d expiry error = %v", iteration, expired.err)
		}
		old, err := GetRun(ctx, runtime, original.RunID)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case replaced.err == nil:
			if !replaced.value.Replaced || old.Status != RunStatusCancelled || expired.changed {
				t.Fatalf("iteration %d replacement winner=%#v expiry=%#v old=%#v", iteration, replaced, expired, old)
			}
			current, found, err := GetCurrentRun(ctx, runtime, command.Name(), key)
			if err != nil || !found || current.ID != replaced.value.RunID {
				t.Fatalf("iteration %d current replacement=%#v found=%v err=%v", iteration, current, found, err)
			}
			assertReplayMatches(t, runtime, replaced.value.RunID)
		case errors.Is(replaced.err, ErrConflict):
			if !expired.changed || old.Status != RunStatusExpired {
				t.Fatalf("iteration %d expiry winner=%#v replacement=%#v old=%#v", iteration, expired, replaced, old)
			}
			if _, found, err := GetCurrentRun(ctx, runtime, command.Name(), key); err != nil || found {
				t.Fatalf("iteration %d expired key current found=%v err=%v", iteration, found, err)
			}
		default:
			t.Fatalf("iteration %d replacement error=%v expiry=%#v", iteration, replaced.err, expired)
		}
		assertReplayMatches(t, runtime, original.RunID)
	}
}

func waitForRunStatusAny(t *testing.T, schema string, db queryRower, id RunID, wants []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		if err := db.QueryRow(context.Background(), `SELECT status FROM `+pgschema.Table(schema, "flow_runs")+` WHERE run_id=$1`, id).Scan(&status); err == nil {
			for _, want := range wants {
				if status == want {
					return
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach one of %v", id, wants)
}

func TestReplaceCurrentRunRecoversAmbiguousCommit(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[replaceArgs, None]("replace.ambiguous", 1)
	original, err := command.Enqueue(ctx, runtime, "intent/ambiguous", replaceArgs{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	runtime.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.IngressCommitAmbiguous {
			return fault.Injected(point)
		}
		return nil
	})
	if _, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/ambiguous",
		replaceArgs{}, "ambiguous retry", WithLiveKey(), WithStartDelay(time.Hour)); !errors.Is(err, fault.ErrInjected) {
		t.Fatalf("ambiguous replacement error = %v", err)
	}
	runtime.faults = fault.None{}
	recovered, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/ambiguous",
		replaceArgs{}, "ambiguous retry", WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil || recovered.Replaced || recovered.RunID == original.RunID {
		t.Fatalf("ambiguous retry = %#v, %v", recovered, err)
	}
}

func TestReplaceCurrentRunContextCancellationBeforeCommitIsAtomic(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[replaceArgs, None]("replace.cancelled_commit", 1)
	original, err := command.Enqueue(ctx, runtime, "intent/cancelled-commit", replaceArgs{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	commitCtx, cancel := context.WithCancel(context.Background())
	runtime.faults = fault.Func(func(_ context.Context, point fault.Point) error {
		if point == fault.IngressBeforeCommit {
			cancel()
		}
		return nil
	})
	_, replaceErr := command.ReplaceCurrentRun(commitCtx, runtime, original.RunID, "intent/cancelled-commit",
		replaceArgs{Generation: 2}, "cancelled commit", WithLiveKey(), WithStartDelay(time.Hour))
	if replaceErr == nil {
		t.Fatal("replacement unexpectedly reported success after its commit context was cancelled")
	}
	runtime.faults = fault.None{}

	current, found, err := GetCurrentRun(ctx, runtime, command.Name(), "intent/cancelled-commit")
	if err != nil || !found {
		t.Fatalf("current after cancelled commit = %#v, found=%v err=%v", current, found, err)
	}
	old, err := GetRun(ctx, runtime, original.RunID)
	if err != nil {
		t.Fatal(err)
	}
	var generations, live int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE status IN ('running','failing'))
		FROM `+pgschema.Table(database.Schema, "flow_runs")+` WHERE definition_name=$1 AND run_key=$2`,
		command.Name(), "intent/cancelled-commit").Scan(&generations, &live); err != nil {
		t.Fatal(err)
	}
	if live != 1 || generations < 1 || generations > 2 {
		t.Fatalf("cancelled commit generations=%d live=%d", generations, live)
	}
	if current.ID == original.RunID {
		if old.Status != RunStatusRunning || generations != 1 {
			t.Fatalf("rolled-back outcome current=%#v old=%#v generations=%d", current, old, generations)
		}
	} else {
		if old.Status != RunStatusCancelled || generations != 2 {
			t.Fatalf("ambiguous committed outcome current=%#v old=%#v generations=%d", current, old, generations)
		}
		assertReplayMatches(t, runtime, current.ID)
	}
	assertReplayMatches(t, runtime, original.RunID)
}

func TestReplaceCurrentRunPublishesObservationsOnlyAfterOwnedCommit(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	runtime.observations.run()
	defer runtime.observations.close()
	command := DefineCommand[replaceArgs, None]("replace.observations", 1)
	original, err := command.Enqueue(ctx, runtime, "intent/observations", replaceArgs{}, WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if observations := waitForObservations(t, observer, 1); len(observations) != 1 || observations[0].RunID != original.RunID || observations[0].Operation != "start" {
		t.Fatalf("original observations = %#v", observations)
	}
	replaced, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/observations",
		replaceArgs{Generation: 2}, "observed replacement", WithLiveKey(), WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	observations := waitForObservations(t, observer, 3)
	if len(observations) != 3 ||
		observations[1].Operation != "cancel" || observations[1].RunID != original.RunID ||
		observations[2].Operation != "start" || observations[2].RunID != replaced.RunID {
		t.Fatalf("replacement observations = %#v", observations)
	}
	if _, err := command.ReplaceCurrentRun(ctx, runtime, original.RunID, "intent/observations",
		replaceArgs{Generation: 2}, "observed replacement", WithLiveKey(), WithStartDelay(time.Hour)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	if observations := observer.snapshot(); len(observations) != 3 {
		t.Fatalf("rediscovery emitted observations = %#v", observations)
	}
}
