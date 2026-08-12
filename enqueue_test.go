package flow

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/replay"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
)

type ingressArgs struct {
	Value string `json:"value"`
}

type ingressResult struct {
	Value string `json:"value"`
}

func TestRunStartsAndEventDeliver(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerRun(3))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	command := DefineCommand[ingressArgs, ingressResult]("ingress.work", 1)
	direct, err := command.Enqueue(ctx, runtime, "direct/1", ingressArgs{Value: "a"}, WithMetadata(map[string]string{"tenant": "one"}))
	if err != nil {
		t.Fatalf("Command.Enqueue() error = %v", err)
	}
	if !direct.Created || direct.RootCommandID == "" || direct.Type != command.Name() {
		t.Fatalf("direct exec = %#v", direct)
	}
	assertRunShape(t, database.Schema, database.DB.Conn, direct, 1, 1)

	repeated, err := command.Enqueue(ctx, runtime, "direct/1", ingressArgs{Value: "a"}, WithMetadata(map[string]string{"tenant": "one"}))
	if err != nil || repeated.Created || repeated.ID != direct.ID || repeated.RootCommandID != direct.RootCommandID {
		t.Fatalf("repeated direct = %#v, %v", repeated, err)
	}
	if _, err := command.Enqueue(ctx, runtime, "direct/1", ingressArgs{Value: "different"}, WithMetadata(map[string]string{"tenant": "one"})); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting direct error = %v", err)
	}
	if _, err := command.Enqueue(ctx, runtime, "direct/1", ingressArgs{Value: "a"}, WithMetadata(map[string]string{"tenant": "one"}), WithFailFast(false)); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting direct options error = %v", err)
	}

	event := DefineEvent[ingressArgs]("ingress.fact")
	if err := event.Deliver(ctx, runtime, direct.ID, "fact/1", ingressArgs{Value: "seen"}); err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if err := event.Deliver(ctx, runtime, direct.ID, "fact/1", ingressArgs{Value: "seen"}); err != nil {
		t.Fatalf("repeated Deliver() error = %v", err)
	}
	if err := event.Deliver(ctx, runtime, direct.ID, "fact/1", ingressArgs{Value: "changed"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting Deliver() error = %v", err)
	}
}

func TestApplicationEventCannotReopenTerminalRun(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[None, None]("ingress.terminal_event", 1)
	event := DefineEvent[string]("ingress.after_terminal")
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false), WithPollInterval(5*time.Millisecond))
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
	exec, err := command.Enqueue(ctx, runtime, "terminal-event", None{})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunStatus(t, database.Schema, database.DB.Conn, exec.ID, "succeeded", 5*time.Second)
	before, err := History(ctx, runtime, exec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := event.Deliver(ctx, runtime, exec.ID, "late", "must not be recorded"); !errors.Is(err, ErrTerminal) {
		t.Fatalf("late Event.Deliver() error=%v, want ErrTerminal", err)
	}
	after, err := History(ctx, runtime, exec.ID)
	if err != nil {
		t.Fatal(err)
	}
	run, err := GetRun(ctx, runtime, exec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "succeeded" || len(after) != len(before) {
		t.Fatalf("terminal run status=%s history before=%d after=%d", run.Status, len(before), len(after))
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
	flowTx := runtime.InTx(tx)
	rolledBack, err := command.Enqueue(ctx, flowTx, "tx/rollback", ingressArgs{Value: "x"})
	if err != nil {
		t.Fatalf("transaction Enqueue() error = %v", err)
	}
	uncommittedHistory, err := History(ctx, flowTx, rolledBack.ID)
	if err != nil || len(uncommittedHistory) != 2 {
		t.Fatalf("History(uncommitted) = %d, %v", len(uncommittedHistory), err)
	}
	if err := flowTx.BeginApplicationWrites(); err != nil {
		t.Fatalf("BeginApplicationWrites(rollback) error: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "app_records")+` (id) VALUES ('rollback')`); err != nil {
		t.Fatalf("insert rolled-back application row: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}
	var count int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_runs")+` WHERE run_id=$1`, rolledBack.ID).Scan(&count); err != nil {
		t.Fatalf("count rolled-back run: %v", err)
	}
	if count != 0 {
		t.Fatalf("rolled-back run count = %d", count)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "app_records")).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rolled-back application count = %d, %v", count, err)
	}

	tx, err = database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx(commit) error = %v", err)
	}
	flowTx = runtime.InTx(tx)
	committed, err := command.Enqueue(ctx, flowTx, "tx/commit", ingressArgs{Value: "x"})
	if err != nil {
		t.Fatalf("transaction Enqueue(commit) error = %v", err)
	}
	if err := flowTx.BeginApplicationWrites(); err != nil {
		t.Fatalf("BeginApplicationWrites(commit) error: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(database.Schema, "app_records")+` (id) VALUES ('commit')`); err != nil {
		t.Fatalf("insert committed application row: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	assertRunShape(t, database.Schema, database.DB.Conn, committed, 1, 1)
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "app_records")).Scan(&count); err != nil || count != 1 {
		t.Fatalf("committed application count = %d, %v", count, err)
	}
	closedTx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	closedClient := runtime.InTx(closedTx)
	if err := closedTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := command.Enqueue(ctx, closedClient, "tx/closed", ingressArgs{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Enqueue(closed tx) error = %v", err)
	}
}

func TestConcurrentStartDefaultsAndCommandCeiling(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerRun(1))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	command := DefineCommand[ingressArgs, ingressResult]("concurrent.work", 1,
		WithQueue("original"), WithRetry(Attempts(3)), WithTimeout(111*time.Millisecond))
	const callers = 16
	execs := make([]Run, callers)
	errs := make([]error, callers)
	var group sync.WaitGroup
	for index := range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			execs[index], errs[index] = command.Enqueue(ctx, runtime, "same", ingressArgs{Value: "stable"})
		}()
	}
	group.Wait()
	created := 0
	for index := range callers {
		if errs[index] != nil {
			t.Fatalf("concurrent Enqueue(%d) error = %v", index, errs[index])
		}
		if execs[index].ID != execs[0].ID || execs[index].RootCommandID != execs[0].RootCommandID {
			t.Fatalf("concurrent exec %d = %#v, first %#v", index, execs[index], execs[0])
		}
		if execs[index].Created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created execs = %d, want 1", created)
	}
	assertRunShape(t, database.Schema, database.DB.Conn, execs[0], 1, 1)

	changedRuntime, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerRun(99))
	if err != nil {
		t.Fatalf("New(changed defaults) error = %v", err)
	}
	changedCommand := DefineCommand[ingressArgs, ingressResult]("concurrent.work", 1,
		WithQueue("changed"), WithRetry(Attempts(9)), WithTimeout(999*time.Millisecond))
	repeated, err := changedCommand.Enqueue(ctx, changedRuntime, "same", ingressArgs{Value: "stable"})
	if err != nil || repeated.Created || repeated.ID != execs[0].ID {
		t.Fatalf("start under changed defaults = %#v, %v", repeated, err)
	}
	var maxCommands int
	var queue string
	var timeoutMS int64
	var retryPolicy []byte
	if err := database.DB.Conn.QueryRow(ctx, `SELECT e.max_commands,c.queue,
		c.retry_policy,c.attempt_timeout_ms FROM `+
		pgschema.Table(database.Schema, "flow_runs")+` e JOIN `+pgschema.Table(database.Schema, "flow_commands")+` c USING (run_id)
		WHERE e.run_id=$1`, execs[0].ID).Scan(&maxCommands, &queue, &retryPolicy, &timeoutMS); err != nil {
		t.Fatalf("read accepted defaults: %v", err)
	}
	policy, err := retrypolicy.PublicFromCanonical(retryPolicy)
	if err != nil {
		t.Fatalf("decode accepted retry policy: %v", err)
	}
	acceptedAttempts := *retrypolicy.ValueOf(policy).MaxAttempts
	if maxCommands != 1 || queue != "original" || acceptedAttempts != 3 || timeoutMS != 111 {
		t.Fatalf("accepted defaults = max %d queue %s attempts %d timeout %dms",
			maxCommands, queue, acceptedAttempts, timeoutMS)
	}

	fact := DefineEvent[ingressArgs]("ceiling.fact")
	publishErrors := make(chan error, callers)
	for range callers {
		go func() { publishErrors <- fact.Deliver(ctx, runtime, execs[0].ID, "same", ingressArgs{Value: "fact"}) }()
	}
	for range callers {
		if err := <-publishErrors; err != nil {
			t.Fatalf("concurrent Deliver() error = %v", err)
		}
	}
	var eventCount int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE run_id=$1 AND event_class='application'`, execs[0].ID).Scan(&eventCount); err != nil || eventCount != 1 {
		t.Fatalf("concurrent event count = %d, %v", eventCount, err)
	}
}

func TestTransactionRunOrdering(t *testing.T) {
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
	first, _ := command.Enqueue(ctx, runtime, "one", ingressArgs{})
	second, _ := command.Enqueue(ctx, runtime, "two", ingressArgs{})
	ids := []RunID{first.ID, second.ID}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	keyByID := map[RunID]string{first.ID: "one", second.ID: "two"}
	fact := DefineEvent[ingressArgs]("order.fact")

	tx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	client := runtime.InTx(tx)
	if err := fact.Deliver(ctx, client, ids[1], "high", ingressArgs{}); err != nil {
		t.Fatalf("Emit(high) error = %v", err)
	}
	if _, err := command.Enqueue(ctx, client, keyByID[ids[0]], ingressArgs{}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Enqueue(rediscover reverse order) error = %v", err)
	}
	if err := fact.Deliver(ctx, client, ids[0], "low", ingressArgs{}); !errors.Is(err, ErrInvalidState) {
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
	if err := fact.Deliver(ctx, client, ids[0], "low", ingressArgs{}); err != nil {
		t.Fatalf("Emit(low) error = %v", err)
	}
	rediscovered, err := command.Enqueue(ctx, client, keyByID[ids[1]], ingressArgs{})
	if err != nil || rediscovered.ID != ids[1] || rediscovered.Created {
		t.Fatalf("Enqueue(rediscover ordered) = %#v, %v", rediscovered, err)
	}
	if err := fact.Deliver(ctx, client, ids[1], "high", ingressArgs{}); err != nil {
		t.Fatalf("Emit(high ordered) error = %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

func TestTransactionStartRejectsApplicationPhaseBeforeSQL(t *testing.T) {
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
	command := DefineCommand[ingressArgs, ingressResult]("order.application_phase", 1)
	event := DefineEvent[ingressArgs]("order.application_phase_event")
	target, err := command.Enqueue(ctx, runtime, "existing", ingressArgs{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	client := runtime.InTx(tx)
	if err := client.BeginApplicationWrites(); err != nil {
		t.Fatal(err)
	}
	if err := client.BeginApplicationWrites(); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("duplicate BeginApplicationWrites() error = %v, want ErrInvalidState", err)
	}
	if _, err := command.Enqueue(ctx, client, "must-not-start", ingressArgs{}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Enqueue(application phase) error = %v", err)
	}
	if err := event.Deliver(ctx, client, target.ID, "must-not-deliver", ingressArgs{}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Deliver(application phase) error = %v", err)
	}
	if err := CancelCommand(ctx, client, target.RootCommandID, "must not cancel"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("CancelCommand(application phase) error = %v", err)
	}
	if err := CancelRun(ctx, client, target.ID, "must not cancel"); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("CancelRun(application phase) error = %v", err)
	}
	if _, err := command.ReplaceCurrentRun(ctx, client, target.ID, "existing", ingressArgs{}, "must not replace", WithLiveKey()); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("ReplaceCurrentRun(application phase) error = %v", err)
	}
	var runs int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_runs")+`
		WHERE definition_name=$1 AND run_key=$2`, command.Name(), "must-not-start").Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if runs != 0 {
		t.Fatalf("application-phase start wrote %d runs", runs)
	}
	var events int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE run_id=$1 AND event_name=$2`, target.ID, event.Name()).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("application-phase delivery wrote %d events", events)
	}
	if err := (*TransactionClient)(nil).BeginApplicationWrites(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil BeginApplicationWrites() error = %v, want ErrInvalid", err)
	}
}

func TestTransactionOwnedRunDoesNotCreateUUIDLockOrderEdge(t *testing.T) {
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
	command := DefineCommand[ingressArgs, ingressResult]("order.owned_run", 1)
	input, err := encodeDefinitionValue(command.def.Args, ingressArgs{}, maxCommandArgumentBytes, "command arguments")
	if err != nil {
		t.Fatal(err)
	}
	high := uuid.MustParse("ffffffff-ffff-ffff-ffff-fffffffffff0")
	low := uuid.MustParse("00000000-0000-0000-0000-000000000010")
	highRequest, err := command.prepareStartRequest("order/high", input, runtime.maxCommands, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	highRequest.ID = high
	tx, err := database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.store.StartInTx(ctx, tx, highRequest, nil); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	lowRequest, err := command.prepareStartRequest("order/low", input, runtime.maxCommands, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	lowRequest.ID = low
	tx, err = database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var order store.LockOrder
	if err := order.BeforeRun(high); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.store.AttachSemantic(ctx, tx, high, store.LockBlocking); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if result, err := runtime.store.StartInTx(ctx, tx, lowRequest, &order); err != nil || !result.Created {
		_ = tx.Rollback(ctx)
		t.Fatalf("reverse-ID owned start = %#v, %v", result, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	conflictRequest, err := command.prepareStartRequest("order/low", input, runtime.maxCommands, WithStartDelay(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	tx, err = database.DB.Conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	order = store.LockOrder{}
	if err := order.BeforeRun(high); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.store.AttachSemantic(ctx, tx, high, store.LockBlocking); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.store.StartInTx(ctx, tx, conflictRequest, &order); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("reverse pre-existing rediscovery error = %v, want ErrInvalidState", err)
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
	if _, err := New(database.DB, WithSchema(database.Schema), WithMaxCommandsPerRun(-1)); !errors.Is(err, ErrInvalid) {
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
	if _, err := command.Enqueue(ctx, nil, "nil-client", ingressArgs{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Enqueue(nil client) error = %v", err)
	}
	if _, err := (Command[ingressArgs, ingressResult]{}).Enqueue(ctx, runtime, "zero", ingressArgs{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Enqueue(zero command) error = %v", err)
	}
	if _, err := command.Enqueue(ctx, runtime, "bad/options", ingressArgs{}, WithRunDeadline(0)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Enqueue(invalid deadline) error = %v", err)
	}
	if _, err := command.Enqueue(ctx, runtime, "bad/metadata", ingressArgs{}, WithMetadata(map[string]string{"x": strings.Repeat("x", 1025)})); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Enqueue(invalid metadata) error = %v", err)
	}
	large := ingressArgs{Value: strings.Repeat("x", maxCommandArgumentBytes)}
	if _, err := command.Enqueue(ctx, runtime, "large", large); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("Enqueue(large args) error = %v", err)
	}
	if err := DefineEvent[ingressArgs]("event").Deliver(ctx, runtime, RunID("bad"), "key", ingressArgs{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Emit(invalid ID) error = %v", err)
	}
	exec, err := command.Enqueue(ctx, runtime, "validation/event-size", ingressArgs{})
	if err != nil {
		t.Fatalf("Command.Enqueue(event size) error = %v", err)
	}
	largeEvent := DefineEvent[ingressArgs]("validation.large_event")
	if err := largeEvent.Deliver(ctx, runtime, exec.ID, "large", ingressArgs{Value: strings.Repeat("x", maxApplicationEventBytes)}); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("Emit(large payload) error = %v", err)
	}
	if err := CancelRun(ctx, runtime, RunID("bad"), "reason"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("CancelRun(invalid ID) error = %v", err)
	}
	if err := CancelCommand(ctx, runtime, CommandID("bad"), "reason"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("CancelCommand(invalid ID) error = %v", err)
	}
	if _, err := History(ctx, runtime, RunID("bad")); !errors.Is(err, ErrInvalid) {
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
	direct, err := command.Enqueue(ctx, runtime, "cancel/direct", ingressArgs{})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
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
	repeatedDirect, err := command.Enqueue(ctx, runtime, "cancel/direct", ingressArgs{})
	if err != nil || repeatedDirect.Created || repeatedDirect.ID != direct.ID {
		t.Fatalf("start retry after terminal = %#v, %v", repeatedDirect, err)
	}
	var runStatus, commandStatus string
	var queueCount, terminalEvents int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT status FROM `+pgschema.Table(database.Schema, "flow_runs")+` WHERE run_id=$1`, direct.ID).Scan(&runStatus); err != nil {
		t.Fatalf("read run status: %v", err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT state FROM `+pgschema.Table(database.Schema, "flow_commands")+` WHERE command_id=$1`, direct.RootCommandID).Scan(&commandStatus); err != nil {
		t.Fatalf("read command status: %v", err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+` WHERE run_id=$1`, direct.ID).Scan(&queueCount); err != nil {
		t.Fatalf("count queue: %v", err)
	}
	if err := database.DB.Conn.QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		WHERE run_id=$1 AND entry_kind='event_recorded'`, direct.ID).Scan(&terminalEvents); err != nil {
		t.Fatalf("count terminal events: %v", err)
	}
	if runStatus != "failed" || commandStatus != "cancelled" || queueCount != 0 || terminalEvents != 2 {
		t.Fatalf("cancelled direct = run=%s command=%s queue=%d events=%d", runStatus, commandStatus, queueCount, terminalEvents)
	}

	history, err := History(ctx, runtime, direct.ID, HistoryLimit(2))
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(history) != 2 || history[0].Kind != HistoryRunStarted || history[1].Kind != HistoryCommandCreated {
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
	if _, err := History(ctx, runtime, RunID("00000000-0000-0000-0000-000000000001")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("History(missing) error = %v", err)
	}

	assertReplayMatches(t, runtime, direct.ID)
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func assertRunShape(t *testing.T, schema string, db queryRower, exec Run, commands, open int) {
	t.Helper()
	var gotCommands, gotOpen, journalCount, queueCount int
	if err := db.QueryRow(context.Background(), `SELECT command_count,open_commands FROM `+
		pgschema.Table(schema, "flow_runs")+` WHERE run_id=$1`, exec.ID).
		Scan(&gotCommands, &gotOpen); err != nil {
		t.Fatalf("read run: %v", err)
	}
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM `+pgschema.Table(schema, "flow_journal")+` WHERE run_id=$1`, exec.ID).Scan(&journalCount); err != nil {
		t.Fatalf("count journal: %v", err)
	}
	if err := db.QueryRow(context.Background(), `SELECT count(*) FROM `+pgschema.Table(schema, "flow_command_queue")+` WHERE run_id=$1`, exec.ID).Scan(&queueCount); err != nil {
		t.Fatalf("count queue: %v", err)
	}
	wantJournal := 1 + commands
	if gotCommands != commands || gotOpen != open || journalCount != wantJournal || queueCount != commands {
		t.Fatalf("run shape = commands=%d open=%d journal=%d queue=%d; want %d/%d/%d/%d",
			gotCommands, gotOpen, journalCount, queueCount, commands, open, wantJournal, commands)
	}
}

func assertReplayMatches(t *testing.T, runtime *Runtime, id RunID) {
	t.Helper()
	parsed, err := parseRunID(id)
	if err != nil {
		t.Fatalf("parseRunID() error = %v", err)
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
		pgschema.Table(runtime.schema, "flow_runs")+` WHERE run_id=$1`, parsed).
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
