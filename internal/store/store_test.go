package store_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/testpg"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestBeginSemanticExecutionFirst(t *testing.T) {
	t.Parallel()

	db, schema, repository := setupStore(t)
	id := seedExecution(t, db, schema, "lock")
	ctx := context.Background()
	first, err := repository.BeginSemantic(ctx, id, store.LockBlocking)
	if err != nil {
		t.Fatalf("BeginSemantic(first) error = %v", err)
	}
	defer first.Rollback(ctx)
	if first.DBNow().IsZero() || first.ExecutionID() != id {
		t.Fatalf("first semantic metadata = %s/%s", first.ExecutionID(), first.DBNow())
	}

	started := time.Now()
	if _, err := repository.BeginSemantic(ctx, id, store.LockSkipLocked); !errors.Is(err, store.ErrLockUnavailable) {
		t.Fatalf("BeginSemantic(skip locked) error = %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("skip-locked execution acquisition waited")
	}

	type response struct {
		tx  *store.SemanticTx
		err error
	}
	result := make(chan response, 1)
	go func() {
		tx, err := repository.BeginSemantic(ctx, id, store.LockBlocking)
		result <- response{tx: tx, err: err}
	}()
	select {
	case got := <-result:
		if got.tx != nil {
			_ = got.tx.Rollback(ctx)
		}
		t.Fatalf("blocking lock returned before release: %v", got.err)
	case <-time.After(75 * time.Millisecond):
	}
	if err := first.Rollback(ctx); err != nil {
		t.Fatalf("Rollback(first) error = %v", err)
	}
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("BeginSemantic(second) error = %v", got.err)
		}
		if !got.tx.DBNow().After(first.DBNow()) {
			t.Fatalf("second DBNow %s was not captured after first %s", got.tx.DBNow(), first.DBNow())
		}
		if err := got.tx.Rollback(ctx); err != nil {
			t.Fatalf("Rollback(second) error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocking semantic acquisition did not resume")
	}
}

func TestJournalAllocationGapFreeAndHistory(t *testing.T) {
	t.Parallel()

	db, schema, repository := setupStore(t)
	id := seedExecution(t, db, schema, "journal")
	ctx := context.Background()
	firstEntry, err := store.NewJournalEntry(store.ExecutionStarted, map[string]any{"v": 1, "kind": "first"})
	if err != nil {
		t.Fatalf("NewJournalEntry(first) error = %v", err)
	}
	secondEntry, err := store.NewJournalEntry(store.ExecutionFailing, map[string]any{"v": 1, "kind": "second"})
	if err != nil {
		t.Fatalf("NewJournalEntry(second) error = %v", err)
	}
	zero := 0
	secondEntry.CausationBatchIndex = &zero
	eventEntry, err := store.NewJournalEntry(store.EventRecorded, map[string]any{"v": 1, "fact": "observed"})
	if err != nil {
		t.Fatalf("NewJournalEntry(event) error = %v", err)
	}
	one := 1
	eventID := uuid.New()
	eventNamespace, eventName, eventKey, eventClass := "application", "observed", "observation/1", "application"
	eventEntry.CausationBatchIndex = &one
	eventEntry.EventID = &eventID
	eventEntry.EventNamespace = &eventNamespace
	eventEntry.EventName = &eventName
	eventEntry.EventKey = &eventKey
	eventEntry.EventClass = &eventClass

	tx, err := repository.BeginSemantic(ctx, id, store.LockBlocking)
	if err != nil {
		t.Fatalf("BeginSemantic() error = %v", err)
	}
	rolledBack, err := tx.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{firstEntry, secondEntry, eventEntry}})
	if err != nil {
		t.Fatalf("Apply(rollback) error = %v", err)
	}
	if rolledBack.Journal[0].Position != 1 || rolledBack.Journal[1].Position != 2 ||
		rolledBack.Journal[1].CausationPosition == nil || *rolledBack.Journal[1].CausationPosition != 1 {
		t.Fatalf("rolled-back positions = %#v", rolledBack.Journal)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	tx, err = repository.BeginSemantic(ctx, id, store.LockBlocking)
	if err != nil {
		t.Fatalf("BeginSemantic(commit) error = %v", err)
	}
	committed, err := tx.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{firstEntry, secondEntry, eventEntry}})
	if err != nil {
		t.Fatalf("Apply(commit) error = %v", err)
	}
	if committed.Journal[0].Position != 1 || committed.Journal[1].Position != 2 {
		t.Fatalf("positions after rollback = %d,%d", committed.Journal[0].Position, committed.Journal[1].Position)
	}
	if !committed.Journal[0].RecordedAt.Equal(tx.DBNow()) || !committed.Journal[1].RecordedAt.Equal(tx.DBNow()) {
		t.Fatal("journal batch does not share the semantic database time")
	}
	if _, err := tx.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{firstEntry}}); !errors.Is(err, flow.ErrInvalidState) {
		t.Fatalf("second Apply() error = %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if err := tx.Commit(ctx); !errors.Is(err, flow.ErrClosed) {
		t.Fatalf("second Commit() error = %v", err)
	}

	history, err := repository.History(ctx, id, 0, 1)
	if err != nil {
		t.Fatalf("History(first page) error = %v", err)
	}
	if len(history) != 1 || history[0].Position != 1 || history[0].Kind != store.ExecutionStarted {
		t.Fatalf("History(first page) = %#v", history)
	}
	history[0].Body[0] = '['
	next, err := repository.History(ctx, id, 1, 10)
	if err != nil {
		t.Fatalf("History(next page) error = %v", err)
	}
	if len(next) != 2 || next[0].Position != 2 || next[0].CausationPosition == nil || *next[0].CausationPosition != 1 ||
		next[1].Position != 3 || next[1].EventID == nil || *next[1].EventID != eventID ||
		next[1].CausationPosition == nil || *next[1].CausationPosition != 2 {
		t.Fatalf("History(next page) = %#v", next)
	}
	again, err := repository.History(ctx, id, 0, 10)
	if err != nil {
		t.Fatalf("History(again) error = %v", err)
	}
	if len(again) != 3 || again[0].Body[0] != '{' || sha256.Sum256(again[0].Body) != again[0].BodyHash {
		t.Fatalf("History immutability/hash = %#v", again)
	}
	var nextPosition int64
	if err := db.Conn.QueryRow(ctx, `SELECT next_journal_position FROM `+pgschema.Table(schema, "flow_executions")+` WHERE execution_id=$1`, id).Scan(&nextPosition); err != nil {
		t.Fatalf("read allocator: %v", err)
	}
	if nextPosition != 4 {
		t.Fatalf("next_journal_position = %d, want 4", nextPosition)
	}
}

func TestStoreValidation(t *testing.T) {
	t.Parallel()

	db, schema, repository := setupStore(t)
	id := seedExecution(t, db, schema, "validation")
	ctx := context.Background()
	if _, err := store.New(nil, schema, false); !errors.Is(err, flow.ErrInvalid) {
		t.Fatalf("New(nil) error = %v", err)
	}
	if _, err := store.New(db, "bad-schema", false); !errors.Is(err, flow.ErrInvalid) {
		t.Fatalf("New(bad schema) error = %v", err)
	}
	if _, err := repository.BeginSemantic(ctx, uuid.Nil, store.LockBlocking); !errors.Is(err, flow.ErrInvalid) {
		t.Fatalf("BeginSemantic(nil ID) error = %v", err)
	}
	if _, err := repository.BeginSemantic(ctx, uuid.New(), store.LockBlocking); !errors.Is(err, flow.ErrNotFound) {
		t.Fatalf("BeginSemantic(missing) error = %v", err)
	}
	if _, err := repository.BeginSemantic(ctx, id, store.LockMode(99)); !errors.Is(err, flow.ErrInvalid) {
		t.Fatalf("BeginSemantic(mode) error = %v", err)
	}
	for _, test := range []struct {
		after uint64
		limit int
	}{
		{after: 0, limit: 0}, {after: 0, limit: store.MaxHistoryLimit + 1}, {after: math.MaxUint64, limit: 1},
	} {
		if _, err := repository.History(ctx, id, test.after, test.limit); !errors.Is(err, flow.ErrInvalid) {
			t.Fatalf("History(%d,%d) error = %v", test.after, test.limit, err)
		}
	}

	tx, err := repository.BeginSemantic(ctx, id, store.LockBlocking)
	if err != nil {
		t.Fatalf("BeginSemantic() error = %v", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Apply(ctx, store.PersistedChangeSet{}); !errors.Is(err, flow.ErrInvalid) {
		t.Fatalf("Apply(empty) error = %v", err)
	}
	entry, _ := store.NewJournalEntry(store.ExecutionStarted, map[string]int{"v": 1})
	entry.EntryID = uuid.Nil
	if _, err := tx.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{entry}}); !errors.Is(err, flow.ErrInvalid) {
		t.Fatalf("Apply(nil entry ID) error = %v", err)
	}
	validEntry, _ := store.NewJournalEntry(store.ExecutionStarted, map[string]int{"v": 1})
	duplicate := validEntry
	if _, err := tx.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{validEntry, duplicate}}); !errors.Is(err, flow.ErrConflict) {
		t.Fatalf("Apply(duplicate entry) error = %v", err)
	}
	badKind := validEntry
	badKind.Kind = "unknown"
	if _, err := tx.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{badKind}}); !errors.Is(err, flow.ErrInvalid) {
		t.Fatalf("Apply(invalid kind) error = %v", err)
	}
	badHash := validEntry
	badHash.Body.Digest[0]++
	if _, err := tx.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{badHash}}); !errors.Is(err, flow.ErrInvalid) {
		t.Fatalf("Apply(invalid hash) error = %v", err)
	}
	noncanonical := validEntry
	noncanonical.Body = canonical.Value{Bytes: []byte(`{ "v": 1 }`), Digest: sha256.Sum256([]byte(`{ "v": 1 }`))}
	if _, err := tx.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{noncanonical}}); !errors.Is(err, flow.ErrInvalid) {
		t.Fatalf("Apply(noncanonical matching hash) error = %v", err)
	}
	duplicateKey := validEntry
	duplicateKey.Body = canonical.Value{Bytes: []byte(`{"v":1,"v":1}`), Digest: sha256.Sum256([]byte(`{"v":1,"v":1}`))}
	if _, err := tx.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{duplicateKey}}); !errors.Is(err, flow.ErrInvalid) {
		t.Fatalf("Apply(duplicate-key matching hash) error = %v", err)
	}
	causePosition := int64(1)
	positionAsIndex := 0
	badCausation := validEntry
	badCausation.CausationPosition = &causePosition
	badCausation.CausationBatchIndex = &positionAsIndex
	if _, err := tx.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{badCausation}}); !errors.Is(err, flow.ErrInvalid) {
		t.Fatalf("Apply(two causation forms) error = %v", err)
	}
	if _, err := store.NewJournalEntry(store.ExecutionStarted, map[string]string{"missing": "version"}); err == nil {
		t.Fatal("NewJournalEntry() accepted an unversioned body")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("Rollback(validation transaction) error = %v", err)
	}

	tx, err = repository.BeginSemantic(ctx, id, store.LockBlocking)
	if err != nil {
		t.Fatalf("BeginSemantic(causation validation) error = %v", err)
	}
	futureCausation := int64(100)
	futureEntry, _ := store.NewJournalEntry(store.ExecutionStarted, map[string]int{"v": 1})
	futureEntry.CausationPosition = &futureCausation
	if _, err := tx.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{futureEntry}}); !errors.Is(err, flow.ErrInvalidState) {
		t.Fatalf("Apply(future causation) error = %v, want invalid state", err)
	}
	if err := tx.Commit(ctx); !errors.Is(err, flow.ErrInvalidState) {
		t.Fatalf("Commit(future causation) error = %v, want invalid state", err)
	}
	var allocatorAfterCausation int64
	if err := db.Conn.QueryRow(ctx, `SELECT next_journal_position FROM `+
		pgschema.Table(schema, "flow_executions")+` WHERE execution_id=$1`, id).Scan(&allocatorAfterCausation); err != nil {
		t.Fatalf("read allocator after causation rejection: %v", err)
	}
	if allocatorAfterCausation != 1 {
		t.Fatalf("allocator after causation rejection = %d, want 1", allocatorAfterCausation)
	}

	tx, err = repository.BeginSemantic(ctx, id, store.LockBlocking)
	if err != nil {
		t.Fatalf("BeginSemantic(database validation) error = %v", err)
	}
	invalidEntry, err := store.NewJournalEntry(store.EventRecorded, map[string]int{"v": 1})
	if err != nil {
		t.Fatalf("NewJournalEntry(removed kind) error = %v", err)
	}
	if _, err := tx.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{invalidEntry}}); !errors.Is(err, flow.ErrInvalid) {
		t.Fatalf("Apply(database-invalid entry) error = %v", err)
	}
	if err := tx.Commit(ctx); !errors.Is(err, flow.ErrInvalidState) {
		t.Fatalf("Commit(poisoned transaction) error = %v", err)
	}
	var position int64
	if err := db.Conn.QueryRow(ctx, `SELECT next_journal_position FROM `+pgschema.Table(schema, "flow_executions")+` WHERE execution_id=$1`, id).Scan(&position); err != nil {
		t.Fatalf("read allocator after failed apply: %v", err)
	}
	if position != 1 {
		t.Fatalf("allocator after failed apply = %d, want 1", position)
	}
}

func TestNotificationChannelAndPayload(t *testing.T) {
	t.Parallel()
	lower := store.NotificationChannel("tenant", "database")
	upper := store.NotificationChannel("Tenant", "database")
	if lower == upper || len(lower) != 29 || lower[:5] != "flow_" {
		t.Fatalf("notification channels lower=%q upper=%q", lower, upper)
	}
	id := uuid.New()
	parsed, ok := store.ParseNotificationHint(`{"v":1,"kind":"execution","key":"` + id.String() + `"}`)
	if !ok || parsed != id {
		t.Fatalf("parsed notification=%s/%t want %s", parsed, ok, id)
	}
	for _, invalid := range []string{"", `{}`, `{"v":2,"kind":"execution","key":"` + id.String() + `"}`,
		`{"v":1,"kind":"work","key":"` + id.String() + `"}`} {
		if _, ok := store.ParseNotificationHint(invalid); ok {
			t.Fatalf("accepted invalid notification %q", invalid)
		}
	}
}

func TestSchemaConstraints(t *testing.T) {
	t.Parallel()

	db, schema, _ := setupStore(t)
	ctx := context.Background()
	validID := seedExecution(t, db, schema, "duplicate")
	_, err := db.Conn.Exec(ctx, executionInsertSQL(schema), uuid.New(), "test", 1, "duplicate", "running", nil, uuid.New())
	assertConstraint(t, err, "flow_executions_idempotency_uq")
	_, err = db.Conn.Exec(ctx, executionInsertSQL(schema), uuid.New(), "test", 1, "invalid-status", "unknown", nil, uuid.New())
	assertConstraint(t, err, "flow_executions_status_ck")
	_, err = db.Conn.Exec(ctx, executionInsertSQL(schema), uuid.New(), "test", 1, "bad-terminal", "succeeded", nil, uuid.New())
	assertConstraint(t, err, "flow_executions_terminal_shape_ck")

	commandID := seedCommand(t, db, schema, validID, "command")
	_, err = db.Conn.Exec(ctx, `UPDATE `+pgschema.Table(schema, "flow_commands")+` SET consumed_attempts=2, attempt_ordinal=1 WHERE command_id=$1`, commandID)
	assertConstraint(t, err, "flow_commands_attempt_counts_ck")
	_, err = db.Conn.Exec(ctx, `UPDATE `+pgschema.Table(schema, "flow_commands")+` SET result='{}'::text::bytea WHERE command_id=$1`, commandID)
	assertConstraint(t, err, "flow_commands_result_shape_ck")
	_, err = db.Conn.Exec(ctx, `UPDATE `+pgschema.Table(schema, "flow_commands")+` SET state='succeeded' WHERE command_id=$1`, commandID)
	assertConstraint(t, err, "flow_commands_result_shape_ck")
	_, err = db.Conn.Exec(ctx, `INSERT INTO `+pgschema.Table(schema, "flow_command_queue")+`
		(command_id,execution_id,queue,name,version,state,next_run_at,lease_token)
		VALUES ($1,$2,'default','work',1,'ready',clock_timestamp(),$3)`, commandID, validID, uuid.New())
	assertConstraint(t, err, "flow_command_queue_lease_shape_ck")

	_, err = db.Conn.Exec(ctx, `INSERT INTO `+pgschema.Table(schema, "flow_command_event_waits")+`
		(command_id,execution_id,event_name,event_key,satisfied_position) VALUES ($1,$2,'event','key',0)`, commandID, validID)
	assertConstraint(t, err, "flow_command_event_waits_position_ck")

	_, err = db.Conn.Exec(ctx, `INSERT INTO `+pgschema.Table(schema, "flow_journal")+`
		(execution_id,position,entry_id,entry_kind,recorded_at,event_id,event_namespace,event_name,event_key,event_class,body,body_hash)
		VALUES ($1,1,$2,'execution_started',clock_timestamp(),$3,'application','event','key','application','{}'::text::bytea,decode(repeat('00',32),'hex'))`,
		validID, uuid.New(), uuid.New())
	assertConstraint(t, err, "flow_journal_event_shape_ck")
	if validID == uuid.Nil {
		t.Fatal("seed execution returned nil")
	}
}

func TestSparseEventWaitUpdateUsesProductionReverseIndexQuery(t *testing.T) {
	db, schema, repository := setupStore(t)
	executionID := seedExecution(t, db, schema, "sparse-waits")
	ctx := context.Background()

	semantic, err := repository.BeginSemantic(ctx, executionID, store.LockBlocking)
	if err != nil {
		t.Fatalf("BeginSemantic() error = %v", err)
	}
	eventName, eventKey := "store.sparse_target", "target"
	eventID := uuid.New()
	eventNamespace, eventClass := "application", "application"
	event, err := store.NewJournalEntry(store.EventRecorded, map[string]any{"v": 1, "payload": map[string]any{}})
	if err != nil {
		t.Fatalf("NewJournalEntry() error = %v", err)
	}
	event.EventID = &eventID
	event.EventNamespace = &eventNamespace
	event.EventName = &eventName
	event.EventKey = &eventKey
	event.EventClass = &eventClass
	applied, err := semantic.Apply(ctx, store.PersistedChangeSet{Journal: []store.JournalEntry{event}})
	if err != nil {
		t.Fatalf("Apply(event) error = %v", err)
	}
	if err := semantic.Commit(ctx); err != nil {
		t.Fatalf("Commit(event) error = %v", err)
	}
	position := applied.Journal[0].Position

	commands := pgschema.Table(schema, "flow_commands")
	waits := pgschema.Table(schema, "flow_command_event_waits")
	var rootID uuid.UUID
	if err := db.Conn.QueryRow(ctx, `SELECT root_command_id FROM `+pgschema.Table(schema, "flow_executions")+`
		WHERE execution_id=$1`, executionID).Scan(&rootID); err != nil {
		t.Fatalf("load root command: %v", err)
	}
	if _, err := db.Conn.Exec(ctx, `INSERT INTO `+commands+` (
		command_id,execution_id,command_key,name,version,parent_command_id,required,args,declaration_fingerprint,
		state,unsatisfied_waits,queue,retry_policy,wait_started_at,wait_timeout_ms,
		created_position,created_at,updated_at,status_at)
		SELECT md5($1::text||':'||g::text)::uuid,$1::uuid,'scale/'||g::text,'store.sparse.synthetic',1,$2::uuid,true,
		       convert_to('{}','UTF8'),decode(repeat('00',32),'hex'),'pending',1,'default',convert_to('{}','UTF8'),
		       clock_timestamp(),3600000,1,clock_timestamp(),clock_timestamp(),clock_timestamp()
		FROM generate_series(0,10000) AS g`, executionID, rootID); err != nil {
		t.Fatalf("seed sparse commands: %v", err)
	}
	if _, err := db.Conn.Exec(ctx, `INSERT INTO `+waits+` (command_id,execution_id,event_name,event_key)
		SELECT command_id,execution_id,
		       CASE WHEN command_key='scale/0' THEN $2 ELSE 'store.sparse_unrelated' END,
		       CASE WHEN command_key='scale/0' THEN $3 ELSE command_key END
		FROM `+commands+` WHERE execution_id=$1 AND command_key LIKE 'scale/%'`,
		executionID, eventName, eventKey); err != nil {
		t.Fatalf("seed sparse waits: %v", err)
	}
	if _, err := db.Conn.Exec(ctx, `ANALYZE `+waits); err != nil {
		t.Fatalf("analyze sparse waits: %v", err)
	}

	tx, err := db.Conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin sparse explain: %v", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `EXPLAIN (ANALYZE,BUFFERS,FORMAT TEXT) `+
		store.EventWaitUpdateQueryForTest(repository),
		executionID, []string{eventName}, []string{eventKey}, []int64{position})
	if err != nil {
		t.Fatalf("explain production event wait update: %v", err)
	}
	planLines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("collect production event wait update plan: %v", err)
	}
	var updated int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM `+waits+`
		WHERE execution_id=$1 AND satisfied_position=$2`, executionID, position).Scan(&updated); err != nil {
		t.Fatalf("count sparse updated waits: %v", err)
	}
	if updated != 1 {
		t.Fatalf("production sparse wait update changed %d rows, want 1", updated)
	}
	var updateNode, reverseIndexNode string
	for _, line := range planLines {
		switch {
		case strings.Contains(line, "Update on flow_command_event_waits"):
			updateNode = line
		case strings.Contains(line, "Index Scan using flow_command_event_waits_reverse_idx"):
			reverseIndexNode = line
		}
	}
	oneRow := regexp.MustCompile(`actual .* rows=1(?:\.0+)? loops=1`)
	if updateNode == "" || !oneRow.MatchString(updateNode) ||
		reverseIndexNode == "" || !oneRow.MatchString(reverseIndexNode) {
		t.Fatalf("production sparse wait update plan did not update/index-scan exactly one row:\n%s",
			strings.Join(planLines, "\n"))
	}
}

func seedCommand(t *testing.T, db *pgkit.DB, schema string, executionID uuid.UUID, key string) uuid.UUID {
	t.Helper()
	id := uuid.New()
	_, err := db.Conn.Exec(context.Background(), `INSERT INTO `+pgschema.Table(schema, "flow_commands")+` (
		command_id,execution_id,command_key,name,version,args,declaration_fingerprint,
		state,queue,retry_policy,budget_started_at,next_attempt_at,
		created_position,created_at,updated_at,status_at
	) VALUES ($1,$2,$3,'work',1,'{}'::text::bytea,decode(repeat('00',32),'hex'),
		'ready','default','{}'::text::bytea,clock_timestamp(),clock_timestamp(),
		1,clock_timestamp(),clock_timestamp(),clock_timestamp())`, id, executionID, key)
	if err != nil {
		t.Fatalf("seed command: %v", err)
	}
	return id
}

func setupStore(t *testing.T) (*pgkit.DB, string, *store.Store) {
	t.Helper()
	database := testpg.Open(t)
	if err := flow.Migrate(context.Background(), database.DB, flow.WithSchema(database.Schema)); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	repository, err := store.New(database.DB, database.Schema, false)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	return database.DB, database.Schema, repository
}

func seedExecution(t *testing.T, db *pgkit.DB, schema, key string) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	tx, err := db.Conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin seed execution: %v", err)
	}
	defer tx.Rollback(ctx)
	id, rootID := uuid.New(), uuid.New()
	if _, err := tx.Exec(ctx, executionInsertSQL(schema), id, "test", 1, key, "running", nil, rootID); err != nil {
		t.Fatalf("seed execution: %v", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO `+pgschema.Table(schema, "flow_commands")+` (
		command_id,execution_id,command_key,name,version,args,declaration_fingerprint,
		state,queue,retry_policy,budget_started_at,next_attempt_at,
		created_position,created_at,updated_at,status_at
	) VALUES ($1,$2,'root','work',1,'{}'::text::bytea,decode(repeat('00',32),'hex'),
		'ready','default','{}'::text::bytea,clock_timestamp(),clock_timestamp(),
		1,clock_timestamp(),clock_timestamp(),clock_timestamp())`, rootID, id); err != nil {
		t.Fatalf("seed root command: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit seed execution: %v", err)
	}
	return id
}

func executionInsertSQL(schema string) string {
	return `INSERT INTO ` + pgschema.Table(schema, "flow_executions") + ` (
		execution_id, definition_name, definition_version, execution_key, status,
		start_fingerprint, input, metadata, metadata_canonical,
		max_commands, command_count, open_commands, root_command_id,
		created_at, updated_at, status_at, finished_at
	) VALUES ($1,$2,$3,$4,$5,
		decode(repeat('00',32),'hex'), '{}'::text::bytea,
		'{}'::jsonb, '{}'::text::bytea,
		100, 1, 1, $7, clock_timestamp(), clock_timestamp(), clock_timestamp(), $6)`
}

func assertConstraint(t *testing.T, err error, constraint string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected constraint %s", constraint)
	}
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) || pgError.ConstraintName != constraint {
		t.Fatalf("constraint error = %v, want %s", err, constraint)
	}
}
