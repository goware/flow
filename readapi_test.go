package flow

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/goware/flow/internal/testpg"
	"github.com/jackc/pgx/v5"
)

// The bounded by-keys reads are the supported surface for consumers that
// decorate their own domain rows with flow state: a live-keyed execution's
// queued work is addressable by its key through ListLiveWork, its journal
// through ListHistoryByKeys, and settling the execution removes it from live
// work while its history remains.
func TestReadGettersExposeLiveWorkAndHistory(t *testing.T) {
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

	command := DefineCommand[ingressArgs, ingressResult]("readapi.work", 1)
	started, err := command.With(runtime).Execute(ctx, "txn:42", ingressArgs{Value: "a"}, WithLiveKey())
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !started.Created {
		t.Fatalf("execution not created: %#v", started)
	}

	workPage, err := ListLiveWork(ctx, runtime, LiveWorkFilter{Keys: []string{"txn:42", "txn:missing"}})
	if err != nil {
		t.Fatalf("ListLiveWork() error = %v", err)
	}
	work := workPage.Work
	if len(work) != 1 {
		t.Fatalf("live work rows = %d, want 1 (missing keys contribute none)", len(work))
	}
	row := work[0]
	if row.ExecutionKey != "txn:42" || row.KeyScope != "live" || row.ExecutionStatus != "running" ||
		row.DefinitionName != command.Name() || row.Queue == "" || row.QueueState == "" {
		t.Fatalf("live work row = %#v", row)
	}

	historyPage, err := ListHistoryByKeys(ctx, runtime, KeyedHistoryFilter{Keys: []string{"txn:42"}})
	if err != nil {
		t.Fatalf("ListHistoryByKeys() error = %v", err)
	}
	history := historyPage.Entries
	if len(history) == 0 {
		t.Fatal("history by keys is empty for the started execution")
	}
	if history[0].ExecutionKey != "txn:42" || history[0].DefinitionName != command.Name() ||
		history[0].Kind != HistoryExecutionStarted {
		t.Fatalf("first history entry = %#v", history[0])
	}

	if err := CancelExecution(ctx, runtime, started.ID, "test settle"); err != nil {
		t.Fatalf("CancelExecution() error = %v", err)
	}

	workPage, err = ListLiveWork(ctx, runtime, LiveWorkFilter{Keys: []string{"txn:42"}})
	if err != nil {
		t.Fatalf("ListLiveWork() after settle error = %v", err)
	}
	work = workPage.Work
	if len(work) != 0 {
		t.Fatalf("settled execution still has %d live-work rows", len(work))
	}
	historyPage, err = ListHistoryByKeys(ctx, runtime, KeyedHistoryFilter{Keys: []string{"txn:42"}})
	if err != nil {
		t.Fatalf("ListHistoryByKeys() after settle error = %v", err)
	}
	history = historyPage.Entries
	if len(history) == 0 {
		t.Fatal("history lost after settle")
	}

	if _, err := ListLiveWork(ctx, runtime, LiveWorkFilter{Keys: make([]string, MaxReadKeys+1)}); err == nil {
		t.Fatal("oversized key batch must error")
	}
	if _, err := ListHistoryByKeys(ctx, runtime, KeyedHistoryFilter{Keys: []string{""}}); err == nil {
		t.Fatal("empty key must error")
	}
}

func TestBoundedReadPaginationIsStableAndFilterBound(t *testing.T) {
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
	command := DefineCommand[ingressArgs, ingressResult]("readapi.pagination", 1)
	inputKeys := []string{"read/z", "read/A", "read/a", "read/é", "read/A"}
	originalKeys := slices.Clone(inputKeys)
	for index, key := range inputKeys[:4] {
		if _, err := command.With(runtime).Execute(ctx, key, ingressArgs{Value: fmt.Sprint(index)}); err != nil {
			t.Fatalf("Execute(%q) error = %v", key, err)
		}
	}

	liveFilter := LiveWorkFilter{Keys: inputKeys, PageSize: 2}
	var work []LiveWork
	for {
		page, err := ListLiveWork(ctx, runtime, liveFilter)
		if err != nil {
			t.Fatalf("ListLiveWork() error = %v", err)
		}
		if len(page.Work) > liveFilter.PageSize {
			t.Fatalf("live page length = %d, want at most %d", len(page.Work), liveFilter.PageSize)
		}
		work = append(work, page.Work...)
		if page.NextCursor == "" {
			break
		}
		if len(work) == len(page.Work) {
			if _, err := ListHistoryByKeys(ctx, runtime, KeyedHistoryFilter{Keys: inputKeys, Cursor: page.NextCursor}); !errors.Is(err, ErrInvalid) {
				t.Fatalf("cross-query cursor error = %v, want ErrInvalid", err)
			}
			if _, err := ListLiveWork(ctx, runtime, LiveWorkFilter{Keys: []string{"read/A"}, Cursor: page.NextCursor}); !errors.Is(err, ErrInvalid) {
				t.Fatalf("changed-filter cursor error = %v, want ErrInvalid", err)
			}
		}
		liveFilter.Cursor = page.NextCursor
	}
	if !reflect.DeepEqual(inputKeys, originalKeys) {
		t.Fatalf("caller keys mutated: got %v, want %v", inputKeys, originalKeys)
	}
	if len(work) != 4 {
		t.Fatalf("paged live work rows = %d, want 4", len(work))
	}
	liveSeen := make(map[CommandID]struct{}, len(work))
	for index, row := range work {
		if _, duplicate := liveSeen[row.CommandID]; duplicate {
			t.Fatalf("duplicate live command %s", row.CommandID)
		}
		liveSeen[row.CommandID] = struct{}{}
		if index > 0 && work[index-1].ExecutionKey > row.ExecutionKey {
			t.Fatalf("live keys are not bytewise sorted at %q > %q", work[index-1].ExecutionKey, row.ExecutionKey)
		}
	}

	historyFilter := KeyedHistoryFilter{Keys: inputKeys, PageSize: 3}
	var history []KeyedHistoryEntry
	for {
		page, err := ListHistoryByKeys(ctx, runtime, historyFilter)
		if err != nil {
			t.Fatalf("ListHistoryByKeys() error = %v", err)
		}
		if len(page.Entries) > historyFilter.PageSize {
			t.Fatalf("history page length = %d, want at most %d", len(page.Entries), historyFilter.PageSize)
		}
		history = append(history, page.Entries...)
		if page.NextCursor == "" {
			break
		}
		historyFilter.Cursor = page.NextCursor
	}
	if len(history) != 8 {
		t.Fatalf("paged history rows = %d, want 8", len(history))
	}
	historySeen := make(map[JournalEntryID]struct{}, len(history))
	for index, entry := range history {
		if _, duplicate := historySeen[entry.EntryID]; duplicate {
			t.Fatalf("duplicate history entry %s", entry.EntryID)
		}
		historySeen[entry.EntryID] = struct{}{}
		if index > 0 {
			previous := history[index-1]
			if previous.ExecutionKey > entry.ExecutionKey ||
				(previous.ExecutionID == entry.ExecutionID && previous.Position >= entry.Position) {
				t.Fatalf("history order regressed at %#v then %#v", previous, entry)
			}
		}
	}
}

func TestBoundedReadValidationAndCursorStrictness(t *testing.T) {
	t.Parallel()

	keys := []string{"z", "a", "a"}
	normalized, pageSize, _, err := prepareKeyedRead(keys, 0, "", readKindLiveWork)
	if err != nil {
		t.Fatal(err)
	}
	if pageSize != DefaultReadPageSize || !slices.Equal(normalized, []string{"a", "z"}) || !slices.Equal(keys, []string{"z", "a", "a"}) {
		t.Fatalf("prepare read = keys %v size %d; caller keys %v", normalized, pageSize, keys)
	}
	if _, pageSize, _, err := prepareKeyedRead(keys, MaxReadPageSize, "", readKindHistory); err != nil || pageSize != MaxReadPageSize {
		t.Fatalf("prepare read at maximum page size = %d, %v", pageSize, err)
	}

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema))
	if err != nil {
		t.Fatal(err)
	}
	command := DefineCommand[ingressArgs, ingressResult]("readapi.cursor", 1)
	if _, err := command.With(runtime).Execute(ctx, "cursor/key", ingressArgs{}); err != nil {
		t.Fatal(err)
	}
	other := DefineCommand[ingressArgs, ingressResult]("readapi.cursor.other", 1)
	if _, err := other.With(runtime).Execute(ctx, "cursor/key", ingressArgs{}); err != nil {
		t.Fatal(err)
	}
	first, err := ListLiveWork(ctx, runtime, LiveWorkFilter{Keys: []string{"cursor/key"}, PageSize: 1})
	if err != nil || first.NextCursor == "" {
		t.Fatalf("first ListLiveWork() = %#v, %v", first, err)
	}

	invalid := []LiveWorkFilter{
		{Keys: []string{"cursor/key"}, PageSize: -1},
		{Keys: []string{"cursor/key"}, PageSize: MaxReadPageSize + 1},
		{Keys: []string{""}},
		{Keys: []string{strings.Repeat("x", 1025)}},
		{Keys: []string{string([]byte{0xff})}},
		{Keys: make([]string, MaxReadKeys+1)},
		{Keys: []string{"cursor/key"}, Cursor: "not-base64"},
		{Keys: []string{"cursor/key"}, Cursor: strings.Repeat("x", maxReadCursorBytes+1)},
	}
	for index, filter := range invalid {
		if _, err := ListLiveWork(ctx, runtime, filter); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid filter %d error = %v, want ErrInvalid", index, err)
		}
	}

	encodedUnknown := base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"unknown":true}`))
	encodedTrailing := base64.RawURLEncoding.EncodeToString([]byte(`{} {}`))
	for _, cursor := range []string{encodedUnknown, encodedTrailing} {
		if _, err := ListLiveWork(ctx, runtime, LiveWorkFilter{Keys: []string{"cursor/key"}, Cursor: cursor}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("strict cursor %q error = %v, want ErrInvalid", cursor, err)
		}
	}

	decoded, err := decodeKeyedReadCursor(first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	decoded.Version++
	wrongVersion, err := encodeKeyedReadCursor(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ListLiveWork(ctx, runtime, LiveWorkFilter{Keys: []string{"cursor/key"}, Cursor: wrongVersion}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("wrong-version cursor error = %v, want ErrInvalid", err)
	}

	emptyLive, err := ListLiveWork(ctx, runtime, LiveWorkFilter{})
	if err != nil || emptyLive.NextCursor != "" || len(emptyLive.Work) != 0 {
		t.Fatalf("empty live filter = %#v, %v", emptyLive, err)
	}
	emptyHistory, err := ListHistoryByKeys(ctx, runtime, KeyedHistoryFilter{})
	if err != nil || emptyHistory.NextCursor != "" || len(emptyHistory.Entries) != 0 {
		t.Fatalf("empty history filter = %#v, %v", emptyHistory, err)
	}
}

func TestBoundedReadsObserveCallerTransaction(t *testing.T) {
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
	command := DefineCommand[ingressArgs, ingressResult]("readapi.transaction", 1)
	tx, err := database.DB.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	client := runtime.InTx(tx)
	if _, err := command.With(client).Execute(ctx, "transaction/read", ingressArgs{}); err != nil {
		t.Fatal(err)
	}
	live, err := ListLiveWork(ctx, client, LiveWorkFilter{Keys: []string{"transaction/read"}})
	if err != nil || len(live.Work) != 1 {
		t.Fatalf("transaction ListLiveWork() = %#v, %v", live, err)
	}
	history, err := ListHistoryByKeys(ctx, client, KeyedHistoryFilter{Keys: []string{"transaction/read"}})
	if err != nil || len(history.Entries) != 2 {
		t.Fatalf("transaction ListHistoryByKeys() = %#v, %v", history, err)
	}
	outside, err := ListLiveWork(ctx, runtime, LiveWorkFilter{Keys: []string{"transaction/read"}})
	if err != nil || len(outside.Work) != 0 {
		t.Fatalf("outside ListLiveWork() = %#v, %v", outside, err)
	}
}
