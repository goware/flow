package flow

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/replay"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
)

type ingressArgs struct {
	Value string `json:"value"`
}

type ingressResult struct {
	Value string `json:"value"`
}

func TestExecutionStartsAndEventEmit(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(3))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	command := DefineCommand[ingressArgs, ingressResult]("ingress.work", 1)
	direct, err := command.With(runtime).Execute(ctx, "direct/1", ingressArgs{Value: "a"}, WithMetadata(map[string]string{"tenant": "one"}))
	if err != nil {
		t.Fatalf("Command.Execute() error = %v", err)
	}
	if !direct.Created || direct.RootCommandID == "" || direct.Type != command.Name() {
		t.Fatalf("direct handle = %#v", direct)
	}
	assertExecutionShape(t, database.Schema, database.DB.Conn, direct, 1, 1)

	repeated, err := command.With(runtime).Execute(ctx, "direct/1", ingressArgs{Value: "a"}, WithMetadata(map[string]string{"tenant": "one"}))
	if err != nil || repeated.Created || repeated.ID != direct.ID || repeated.RootCommandID != direct.RootCommandID {
		t.Fatalf("repeated direct = %#v, %v", repeated, err)
	}
	if _, err := command.With(runtime).Execute(ctx, "direct/1", ingressArgs{Value: "different"}, WithMetadata(map[string]string{"tenant": "one"})); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting direct error = %v", err)
	}
	if _, err := command.With(runtime).Execute(ctx, "direct/1", ingressArgs{Value: "a"}, WithMetadata(map[string]string{"tenant": "one"}), WithFailFast(false)); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting direct options error = %v", err)
	}

	event := DefineEvent[ingressArgs]("ingress.fact")
	if err := event.Emit(ctx, runtime, direct.ID, "fact/1", ingressArgs{Value: "seen"}); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}
	if err := event.Emit(ctx, runtime, direct.ID, "fact/1", ingressArgs{Value: "seen"}); err != nil {
		t.Fatalf("repeated Emit() error = %v", err)
	}
	if err := event.Emit(ctx, runtime, direct.ID, "fact/1", ingressArgs{Value: "changed"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting Emit() error = %v", err)
	}
}

func TestCallerOwnedTransactionCommitAndRollback(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[ingressArgs, ingressResult]("tx.work", 1)
	if _, err := database.DB.Conn.Exec(ctx, `CREATE TABLE `+pgschema.Table(database.Schema, "app_records")+` (id text PRIMARY KEY)`); err != nil {
		t.Fatalf("create application table: %v", err)
	}

	tx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	rolledBack, err := command.With(runtime.InTx(tx)).Execute(ctx, "tx/rollback", ingressArgs{Value: "x"})
	if err != nil {
		t.Fatalf("transaction Execute() error = %v", err)
	}
	uncommittedHistory, err := History(ctx, runtime.InTx(tx), rolledBack.ID)
	if err != nil || len(uncommittedHistory) != 2 {
		t.Fatalf("History(uncommitted) = %d, %v", len(uncommittedHistory), err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "app_records")+` (id) VALUES ('rollback')`); err != nil {
		t.Fatalf("insert rolled-back application row: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	var count int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_executions")+` WHERE execution_id=$1`, rolledBack.ID).Scan(&count); err != nil {
		t.Fatalf("count rolled-back execution: %v", err)
	}
	if count != 0 {
		t.Fatalf("rolled-back execution count = %d", count)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "app_records")).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rolled-back application count = %d, %v", count, err)
	}

	tx, err = database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx(commit) error = %v", err)
	}
	committed, err := command.With(runtime.InTx(tx)).Execute(ctx, "tx/commit", ingressArgs{Value: "x"})
	if err != nil {
		t.Fatalf("transaction Execute(commit) error = %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "app_records")+` (id) VALUES ('commit')`); err != nil {
		t.Fatalf("insert committed application row: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	assertExecutionShape(t, database.Schema, database.DB.Conn, committed, 1, 1)
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "app_records")).Scan(&count); err != nil || count != 1 {
		t.Fatalf("committed application count = %d, %v", count, err)
	}
	if _, err := command.With(runtime.InTx(tx)).Execute(ctx, "tx/closed", ingressArgs{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Execute(closed tx) error = %v", err)
	}
}

func TestConcurrentStartDefaultsAndCommandCeiling(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(1))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[ingressArgs, ingressResult]("concurrent.work", 1,
		WithQueue("original"), WithRetry(Attempts(3)), WithTimeout(111*time.Millisecond))
	const callers = 16
	handles := make([]ExecutionHandle, callers)
	errs := make([]error, callers)
	var group sync.WaitGroup
	for index := range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			handles[index], errs[index] = command.With(runtime).Execute(ctx, "same", ingressArgs{Value: "stable"})
		}()
	}
	group.Wait()
	created := 0
	for index := range callers {
		if errs[index] != nil {
			t.Fatalf("concurrent Execute(%d) error = %v", index, errs[index])
		}
		if handles[index].ID != handles[0].ID || handles[index].RootCommandID != handles[0].RootCommandID {
			t.Fatalf("concurrent handle %d = %#v, first %#v", index, handles[index], handles[0])
		}
		if handles[index].Created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created handles = %d, want 1", created)
	}
	assertExecutionShape(t, database.Schema, database.DB.Conn, handles[0], 1, 1)

	changedRuntime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(99))
	if err != nil {
		t.Fatalf("New(changed defaults) error = %v", err)
	}
	changedCommand := DefineCommand[ingressArgs, ingressResult]("concurrent.work", 1,
		WithQueue("changed"), WithRetry(Attempts(9)), WithTimeout(999*time.Millisecond))
	repeated, err := changedCommand.With(changedRuntime).Execute(ctx, "same", ingressArgs{Value: "stable"})
	if err != nil || repeated.Created || repeated.ID != handles[0].ID {
		t.Fatalf("start under changed defaults = %#v, %v", repeated, err)
	}
	var maxCommands, acceptedAttempts int
	var queue string
	var timeoutMS int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT e.max_commands,c.queue,
		(c.retry_policy->>'max_attempts')::integer,c.attempt_timeout_ms FROM `+
		pgschema.Table(database.Schema, "flow_executions")+` e JOIN `+pgschema.Table(database.Schema, "flow_commands")+` c USING (execution_id)
		WHERE e.execution_id=$1`, handles[0].ID).Scan(&maxCommands, &queue, &acceptedAttempts, &timeoutMS); err != nil {
		t.Fatalf("read accepted defaults: %v", err)
	}
	if maxCommands != 1 || queue != "original" || acceptedAttempts != 3 || timeoutMS != 111 {
		t.Fatalf("accepted defaults = max %d queue %s attempts %d timeout %dms",
			maxCommands, queue, acceptedAttempts, timeoutMS)
	}

	fact := DefineEvent[ingressArgs]("ceiling.fact")
	publishErrors := make(chan error, callers)
	for range callers {
		go func() { publishErrors <- fact.Emit(ctx, runtime, handles[0].ID, "same", ingressArgs{Value: "fact"}) }()
	}
	for range callers {
		if err := <-publishErrors; err != nil {
			t.Fatalf("concurrent Emit() error = %v", err)
		}
	}
	var eventCount int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=$1 AND event_class='application'`, handles[0].ID).Scan(&eventCount); err != nil || eventCount != 1 {
		t.Fatalf("concurrent event count = %d, %v", eventCount, err)
	}
}

func TestTransactionExecutionOrdering(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[ingressArgs, ingressResult]("order.command", 1)
	first, _ := command.With(runtime).Execute(ctx, "one", ingressArgs{})
	second, _ := command.With(runtime).Execute(ctx, "two", ingressArgs{})
	ids := []ExecutionID{first.ID, second.ID}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	fact := DefineEvent[ingressArgs]("order.fact")

	tx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	client := runtime.InTx(tx)
	if err := fact.Emit(ctx, client, ids[1], "high", ingressArgs{}); err != nil {
		t.Fatalf("Emit(high) error = %v", err)
	}
	if err := fact.Emit(ctx, client, ids[0], "low", ingressArgs{}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Emit(reverse order) error = %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	tx, err = database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx(ordered) error = %v", err)
	}
	client = runtime.InTx(tx)
	if err := fact.Emit(ctx, client, ids[0], "low", ingressArgs{}); err != nil {
		t.Fatalf("Emit(low) error = %v", err)
	}
	if err := fact.Emit(ctx, client, ids[1], "high", ingressArgs{}); err != nil {
		t.Fatalf("Emit(high ordered) error = %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

func TestRuntimeAndIngressValidation(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if _, err := New(database.DB, WithSchema(database.Schema)); !errors.Is(err, ErrSchema) {
		t.Fatalf("New(before migration) error = %v", err)
	}
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if _, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerExecution(-1)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("New(negative max) error = %v", err)
	}
	if _, err := New(database.DB, WithSchema(database.Schema), WithObserver(nil)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("New(nil observer) error = %v", err)
	}
	var nilOption Option
	if _, err := New(database.DB, WithSchema(database.Schema), nilOption); !errors.Is(err, ErrInvalid) {
		t.Fatalf("New(nil option) error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[ingressArgs, ingressResult]("validation.work", 1)
	if _, err := command.Execute(ctx, "unbound", ingressArgs{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Execute(unbound) error = %v", err)
	}
	if _, err := (Command[ingressArgs, ingressResult]{}).With(runtime).Execute(ctx, "zero", ingressArgs{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Execute(zero command) error = %v", err)
	}
	if _, err := command.With(runtime).Execute(ctx, "bad/options", ingressArgs{}, WithExecutionDeadline(0)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Execute(invalid deadline) error = %v", err)
	}
	if _, err := command.With(runtime).Execute(ctx, "bad/metadata", ingressArgs{}, WithMetadata(map[string]string{"x": strings.Repeat("x", 1025)})); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Execute(invalid metadata) error = %v", err)
	}
	large := ingressArgs{Value: strings.Repeat("x", maxCommandArgumentBytes)}
	if _, err := command.With(runtime).Execute(ctx, "large", large); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("Execute(large args) error = %v", err)
	}
	if err := DefineEvent[ingressArgs]("event").Emit(ctx, runtime, ExecutionID("bad"), "key", ingressArgs{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Emit(invalid ID) error = %v", err)
	}
	handle, err := command.With(runtime).Execute(ctx, "validation/event-size", ingressArgs{})
	if err != nil {
		t.Fatalf("Command.Execute(event size) error = %v", err)
	}
	largeEvent := DefineEvent[ingressArgs]("validation.large_event")
	if err := largeEvent.Emit(ctx, runtime, handle.ID, "large", ingressArgs{Value: strings.Repeat("x", maxApplicationEventBytes)}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("Emit(large payload) error = %v", err)
	}
	if err := CancelExecution(ctx, runtime, ExecutionID("bad"), "reason"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("CancelExecution(invalid ID) error = %v", err)
	}
	if err := CancelCommand(ctx, runtime, CommandID("bad"), "reason"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("CancelCommand(invalid ID) error = %v", err)
	}
	if _, err := History(ctx, runtime, ExecutionID("bad")); !errors.Is(err, ErrInvalid) {
		t.Fatalf("History(invalid ID) error = %v", err)
	}
}

func TestIngressCancellationAndTerminalIdempotency(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[ingressArgs, ingressResult]("cancel.work", 1)
	direct, err := command.With(runtime).Execute(ctx, "cancel/direct", ingressArgs{})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if err := CancelCommand(ctx, runtime, direct.RootCommandID, "operator request"); err != nil {
		t.Fatalf("CancelCommand() error = %v", err)
	}
	if err := CancelCommand(ctx, runtime, direct.RootCommandID, "operator request"); err != nil {
		t.Fatalf("idempotent CancelCommand() error = %v", err)
	}
	if err := CancelCommand(ctx, runtime, direct.RootCommandID, "different"); !errors.Is(err, ErrTerminal) {
		t.Fatalf("changed CancelCommand() error = %v", err)
	}
	repeatedDirect, err := command.With(runtime).Execute(ctx, "cancel/direct", ingressArgs{})
	if err != nil || repeatedDirect.Created || repeatedDirect.ID != direct.ID {
		t.Fatalf("start retry after terminal = %#v, %v", repeatedDirect, err)
	}
	var executionStatus, commandStatus string
	var queueCount, terminalEvents int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT status FROM `+pgschema.Table(database.Schema, "flow_executions")+` WHERE execution_id=$1`, direct.ID).Scan(&executionStatus); err != nil {
		t.Fatalf("read execution status: %v", err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT state FROM `+pgschema.Table(database.Schema, "flow_commands")+` WHERE command_id=$1`, direct.RootCommandID).Scan(&commandStatus); err != nil {
		t.Fatalf("read command status: %v", err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` WHERE execution_id=$1`, direct.ID).Scan(&queueCount); err != nil {
		t.Fatalf("count queue: %v", err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE execution_id=$1 AND entry_kind='event_recorded'`, direct.ID).Scan(&terminalEvents); err != nil {
		t.Fatalf("count terminal events: %v", err)
	}
	if executionStatus != "failed" || commandStatus != "cancelled" || queueCount != 0 || terminalEvents != 2 {
		t.Fatalf("cancelled direct = execution=%s command=%s queue=%d events=%d", executionStatus, commandStatus, queueCount, terminalEvents)
	}

	history, err := History(ctx, runtime, direct.ID, HistoryLimit(2))
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(history) != 2 || history[0].Kind != HistoryExecutionStarted || history[1].Kind != HistoryCommandCreated {
		t.Fatalf("History(first page) = %#v", history)
	}
	if history[1].CausationPosition == nil || *history[1].CausationPosition != 1 {
		t.Fatalf("root command causation = %#v", history[1].CausationPosition)
	}
	history[0].Body[0] = '['
	next, err := History(ctx, runtime, direct.ID, HistoryAfter(history[1].Position), HistoryLimit(10))
	if err != nil {
		t.Fatalf("History(next page) error = %v", err)
	}
	if len(next) != 3 || next[0].TerminalStatus != "cancelled" || next[2].TerminalStatus != "failed" {
		t.Fatalf("History(next page) = %#v", next)
	}
	again, err := History(ctx, runtime, direct.ID)
	if err != nil || len(again) != 5 || again[0].Body[0] != '{' {
		t.Fatalf("History(immutable) = %d, %v, %q", len(again), err, again[0].Body)
	}
	if _, err := History(ctx, runtime, ExecutionID("00000000-0000-0000-0000-000000000001")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("History(missing) error = %v", err)
	}

	assertReplayMatches(t, runtime, direct.ID)
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func assertExecutionShape(t *testing.T, schema string, db queryRower, handle ExecutionHandle, commands, open int) {
	t.Helper()
	var gotCommands, gotOpen, journalCount, queueCount int
	if err := db.QueryRow(context.Background(), `SELECT command_count,open_commands FROM `+
		pgschema.Table(schema, "flow_executions")+` WHERE execution_id=$1`, handle.ID).
		Scan(&gotCommands, &gotOpen); err != nil {
		t.Fatalf("read execution: %v", err)
	}
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM `+pgschema.Table(schema, "flow_journal")+` WHERE execution_id=$1`, handle.ID).Scan(&journalCount); err != nil {
		t.Fatalf("count journal: %v", err)
	}
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM `+pgschema.Table(schema, "flow_command_queue")+` WHERE execution_id=$1`, handle.ID).Scan(&queueCount); err != nil {
		t.Fatalf("count queue: %v", err)
	}
	wantJournal := 1 + commands
	if gotCommands != commands || gotOpen != open || journalCount != wantJournal || queueCount != commands {
		t.Fatalf("execution shape = commands=%d open=%d journal=%d queue=%d; want %d/%d/%d/%d",
			gotCommands, gotOpen, journalCount, queueCount, commands, open, wantJournal, commands)
	}
}

func assertReplayMatches(t *testing.T, runtime *Runtime, id ExecutionID) {
	t.Helper()
	parsed, err := parseExecutionID(id)
	if err != nil {
		t.Fatalf("parseExecutionID() error = %v", err)
	}
	rows, err := runtime.store.History(context.Background(), parsed, 0, 1000)
	if err != nil {
		t.Fatalf("store.History() error = %v", err)
	}
	for end := 1; end <= len(rows); end++ {
		if _, err := replay.Fold(rows[:end]); err != nil {
			t.Fatalf("replay.Fold(prefix %d) error = %v", end, err)
		}
	}
	state, err := replay.Fold(rows)
	if err != nil {
		t.Fatalf("replay.Fold() error = %v", err)
	}
	var status string
	var count, open int
	if err := runtime.db.Conn.QueryRow(context.Background(), `SELECT status,command_count,open_commands FROM `+
		pgschema.Table(runtime.schema, "flow_executions")+` WHERE execution_id=$1`, parsed).
		Scan(&status, &count, &open); err != nil {
		t.Fatalf("read live projection: %v", err)
	}
	if state.Status != status || state.CommandCount != count || state.OpenCommands != open {
		t.Fatalf("replay/live differ: replay=%s/%d/%d live=%s/%d/%d",
			state.Status, state.CommandCount, state.OpenCommands, status, count, open)
	}
	for commandID, replayed := range state.Commands {
		var liveState string
		var created int64
		var terminal *int64
		if err := runtime.db.Conn.QueryRow(context.Background(), `SELECT state,created_position,terminal_position FROM `+
			pgschema.Table(runtime.schema, "flow_commands")+` WHERE command_id=$1`, commandID).
			Scan(&liveState, &created, &terminal); err != nil {
			t.Fatalf("read live command: %v", err)
		}
		if replayed.State != liveState || replayed.CreatedPosition != created || !equalInt64Pointer(replayed.TerminalPosition, terminal) {
			t.Fatalf("replayed command %s differs: %#v live=%s/%d/%v", commandID, replayed, liveState, created, terminal)
		}
	}
}

func equalInt64Pointer(a, b *int64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
