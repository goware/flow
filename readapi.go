package flow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/definition"
	"github.com/goware/flow/internal/store"
)

const (
	// MaxReadKeys bounds every by-keys batch read before duplicate removal.
	MaxReadKeys = store.MaxReadKeys

	// DefaultReadPageSize is used when a by-key read filter has PageSize zero.
	DefaultReadPageSize = 100
	// MaxReadPageSize is the largest public by-key read page.
	MaxReadPageSize = 1000

	keyedReadCursorVersion = 2
	maxReadCursorBytes     = 4096
	readKindLiveWork       = "live_work"
	readKindHistory        = "keyed_history"
)

// LiveWork is one queued (or leased) command of a non-terminal run,
// carrying the run's identity. It is the batch, key-addressed read for
// consumers that decorate their own domain rows with dispatch state, without
// touching Flow's tables.
type LiveWork struct {
	RunID            RunID
	DefinitionName   string
	RunKey           string
	KeyScope         KeyScope
	RunStatus        RunStatus
	CommandID        CommandID
	CommandKey       string
	CommandName      string
	Queue            string
	QueueState       QueueState
	NextRunAt        time.Time
	LeaseOwner       string
	LeaseExpiresAt   *time.Time
	AttemptOrdinal   int
	CommandCreatedAt time.Time
}

// LiveWorkFilter selects bounded queued work for exact run keys. Cursor
// values are opaque and may be reused only with the same Keys filter.
type LiveWorkFilter struct {
	Keys     []string
	PageSize int
	Cursor   string
}

// LiveWorkPage contains one bounded page and an opaque cursor for the next
// page. NextCursor is empty when no later row was observed.
type LiveWorkPage struct {
	Work       []LiveWork
	NextCursor string
}

// ListLiveWork returns one bounded page of queued commands for non-terminal
// runs whose run key is in filter.Keys. Rows are ordered by key,
// definition, run creation, run ID, and command ID. An ordinary
// client does not provide a cross-page snapshot; a transaction-scoped client
// uses the caller's transaction and observes its uncommitted writes.
func ListLiveWork(ctx context.Context, c Client, filter LiveWorkFilter) (LiveWorkPage, error) {
	client, err := resolveClient(c)
	if err != nil {
		return LiveWorkPage{}, err
	}
	keys, pageSize, cursor, err := prepareKeyedRead(filter.Keys, filter.PageSize, filter.Cursor, readKindLiveWork)
	if err != nil {
		return LiveWorkPage{}, err
	}
	if len(keys) == 0 {
		return LiveWorkPage{Work: []LiveWork{}}, nil
	}
	storeFilter := store.LiveWorkListFilter{Keys: keys, Limit: pageSize + 1}
	if cursor != nil {
		storeFilter.Cursor = &store.LiveWorkCursor{
			RunKey: cursor.RunKey, DefinitionName: cursor.DefinitionName,
			RunCreatedAt: cursor.RunCreatedAt,
			RunID:        uuid.MustParse(cursor.RunID),
			CommandID:    uuid.MustParse(cursor.CommandID),
		}
	}
	rows, err := client.runtime.store.ListLiveWorkInTx(ctx, client.tx, storeFilter)
	if err != nil {
		return LiveWorkPage{}, err
	}
	page := LiveWorkPage{Work: make([]LiveWork, min(len(rows), pageSize))}
	for index := range page.Work {
		page.Work[index], err = liveWorkFromStore(rows[index])
		if err != nil {
			return LiveWorkPage{}, err
		}
	}
	if len(rows) > pageSize {
		last := rows[pageSize-1]
		page.NextCursor, err = encodeKeyedReadCursor(keyedReadCursor{
			Version: keyedReadCursorVersion, Kind: readKindLiveWork,
			KeysHash: keyedReadKeysHash(keys), RunKey: last.RunKey,
			DefinitionName: last.DefinitionName, RunCreatedAt: last.RunCreatedAt.UTC(),
			RunID: last.RunID.String(), CommandID: last.CommandID.String(),
		})
		if err != nil {
			return LiveWorkPage{}, err
		}
	}
	return page, nil
}

// KeyedHistoryEntry is a history entry carrying its run's identity.
type KeyedHistoryEntry struct {
	DefinitionName string
	RunKey         string
	KeyScope       KeyScope
	HistoryEntry
}

// KeyedHistoryFilter selects bounded retained history for exact run
// keys. Cursor values are opaque and may be reused only with the same Keys
// filter.
type KeyedHistoryFilter struct {
	Keys     []string
	PageSize int
	Cursor   string
}

// KeyedHistoryPage contains one bounded page and an opaque cursor for the next
// page. NextCursor is empty when no later row was observed.
type KeyedHistoryPage struct {
	Entries    []KeyedHistoryEntry
	NextCursor string
}

// ListHistoryByKeys returns one bounded retained-history page for every
// run that ever held one of filter.Keys. Rows are ordered by key,
// definition, run creation, run ID, and journal position. Journal
// order is preserved within each run. Transaction-scoped clients use the
// caller's transaction and observe their uncommitted writes.
func ListHistoryByKeys(ctx context.Context, c Client, filter KeyedHistoryFilter) (KeyedHistoryPage, error) {
	client, err := resolveClient(c)
	if err != nil {
		return KeyedHistoryPage{}, err
	}
	keys, pageSize, cursor, err := prepareKeyedRead(filter.Keys, filter.PageSize, filter.Cursor, readKindHistory)
	if err != nil {
		return KeyedHistoryPage{}, err
	}
	if len(keys) == 0 {
		return KeyedHistoryPage{Entries: []KeyedHistoryEntry{}}, nil
	}
	storeFilter := store.KeyedHistoryListFilter{Keys: keys, Limit: pageSize + 1}
	if cursor != nil {
		storeFilter.Cursor = &store.KeyedHistoryCursor{
			RunKey: cursor.RunKey, DefinitionName: cursor.DefinitionName,
			RunCreatedAt: cursor.RunCreatedAt,
			RunID:        uuid.MustParse(cursor.RunID),
			Position:     cursor.Position,
		}
	}
	rows, err := client.runtime.store.ListJournalByKeysInTx(ctx, client.tx, storeFilter)
	if err != nil {
		return KeyedHistoryPage{}, err
	}
	page := KeyedHistoryPage{Entries: make([]KeyedHistoryEntry, min(len(rows), pageSize))}
	for index := range page.Entries {
		page.Entries[index], err = keyedHistoryFromStore(rows[index])
		if err != nil {
			return KeyedHistoryPage{}, err
		}
	}
	if len(rows) > pageSize {
		last := rows[pageSize-1]
		page.NextCursor, err = encodeKeyedReadCursor(keyedReadCursor{
			Version: keyedReadCursorVersion, Kind: readKindHistory,
			KeysHash: keyedReadKeysHash(keys), RunKey: last.RunKey,
			DefinitionName: last.DefinitionName, RunCreatedAt: last.RunCreatedAt.UTC(),
			RunID: last.Entry.RunID.String(), Position: last.Entry.Position,
		})
		if err != nil {
			return KeyedHistoryPage{}, err
		}
	}
	return page, nil
}

func liveWorkFromStore(row store.LiveWorkRow) (LiveWork, error) {
	keyScope, err := keyScopeFromString(row.KeyScope)
	if err != nil {
		return LiveWork{}, newError(ErrInvalidState, "decode", "key scope", row.KeyScope, "stored key scope is unknown")
	}
	runStatus, err := runStatusFromString(row.RunStatus)
	if err != nil {
		return LiveWork{}, newError(ErrInvalidState, "decode", "run status", row.RunStatus, "stored status is unknown")
	}
	queueState, err := queueStateFromString(row.QueueState)
	if err != nil {
		return LiveWork{}, newError(ErrInvalidState, "decode", "queue state", row.QueueState, "stored state is unknown")
	}
	work := LiveWork{
		RunID: RunID(row.RunID.String()), DefinitionName: row.DefinitionName,
		RunKey: row.RunKey, KeyScope: keyScope, RunStatus: runStatus,
		CommandID: CommandID(row.CommandID.String()), CommandKey: row.CommandKey,
		CommandName: row.CommandName, Queue: row.Queue, QueueState: queueState,
		NextRunAt: row.NextRunAt, AttemptOrdinal: row.AttemptOrdinal,
		CommandCreatedAt: row.CommandCreatedAt,
	}
	if row.LeaseOwner != nil {
		work.LeaseOwner = *row.LeaseOwner
	}
	if row.LeaseExpiresAt != nil {
		expires := *row.LeaseExpiresAt
		work.LeaseExpiresAt = &expires
	}
	return work, nil
}

func keyedHistoryFromStore(row store.KeyedJournalRow) (KeyedHistoryEntry, error) {
	keyScope, err := keyScopeFromString(row.KeyScope)
	if err != nil {
		return KeyedHistoryEntry{}, newError(ErrInvalidState, "decode", "key scope", row.KeyScope, "stored key scope is unknown")
	}
	history, err := historyEntries([]store.JournalRow{row.Entry})
	if err != nil {
		return KeyedHistoryEntry{}, err
	}
	return KeyedHistoryEntry{
		DefinitionName: row.DefinitionName, RunKey: row.RunKey,
		KeyScope: keyScope, HistoryEntry: history[0],
	}, nil
}

type keyedReadCursor struct {
	Version        int       `json:"v"`
	Kind           string    `json:"kind"`
	KeysHash       string    `json:"keys_hash"`
	RunKey         string    `json:"run_key"`
	DefinitionName string    `json:"definition_name"`
	RunCreatedAt   time.Time `json:"run_created_at"`
	RunID          string    `json:"run_id"`
	CommandID      string    `json:"command_id,omitempty"`
	Position       int64     `json:"position,omitempty"`
}

func prepareKeyedRead(keys []string, pageSize int, encodedCursor, kind string) ([]string, int, *keyedReadCursor, error) {
	if len(keys) > MaxReadKeys {
		return nil, 0, nil, newError(ErrInvalid, "list", "run keys", "", "too many keys")
	}
	normalized := append([]string(nil), keys...)
	for _, key := range normalized {
		if key == "" || len(key) > maxRunKeyBytes || !utf8.ValidString(key) {
			return nil, 0, nil, newError(ErrInvalid, "list", "run key", "", "key is empty, malformed, or too long")
		}
	}
	slices.Sort(normalized)
	normalized = slices.Compact(normalized)
	if pageSize == 0 {
		pageSize = DefaultReadPageSize
	}
	if pageSize < 1 || pageSize > MaxReadPageSize {
		return nil, 0, nil, newError(ErrInvalid, "list", "page size", "", "page size must be between 1 and 1000")
	}
	if len(normalized) == 0 {
		if encodedCursor != "" {
			return nil, 0, nil, newError(ErrInvalid, "list", "cursor", "", "cursor requires run keys")
		}
		return normalized, pageSize, nil, nil
	}
	if encodedCursor == "" {
		return normalized, pageSize, nil, nil
	}
	cursor, err := decodeKeyedReadCursor(encodedCursor)
	if err != nil {
		return nil, 0, nil, err
	}
	if cursor.Version != keyedReadCursorVersion || cursor.Kind != kind || cursor.KeysHash != keyedReadKeysHash(normalized) {
		return nil, 0, nil, newError(ErrInvalid, "list", "cursor", "", "cursor does not match this read filter")
	}
	if cursor.RunKey == "" || len(cursor.RunKey) > maxRunKeyBytes || !utf8.ValidString(cursor.RunKey) ||
		definition.ValidateName(cursor.DefinitionName) != nil || cursor.RunCreatedAt.IsZero() {
		return nil, 0, nil, newError(ErrInvalid, "list", "cursor", "", "cursor ordering values are invalid")
	}
	if _, err := uuid.Parse(cursor.RunID); err != nil {
		return nil, 0, nil, newError(ErrInvalid, "list", "cursor", "", "cursor run ID is invalid")
	}
	switch kind {
	case readKindLiveWork:
		if cursor.Position != 0 {
			return nil, 0, nil, newError(ErrInvalid, "list", "cursor", "", "live-work cursor has history state")
		}
		if _, err := uuid.Parse(cursor.CommandID); err != nil {
			return nil, 0, nil, newError(ErrInvalid, "list", "cursor", "", "cursor command ID is invalid")
		}
	case readKindHistory:
		if cursor.CommandID != "" || cursor.Position < 1 {
			return nil, 0, nil, newError(ErrInvalid, "list", "cursor", "", "history cursor ordering values are invalid")
		}
	default:
		return nil, 0, nil, newError(ErrInvalidState, "list", "cursor", "", "read kind is unknown")
	}
	return normalized, pageSize, &cursor, nil
}

func keyedReadKeysHash(keys []string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte{keyedReadCursorVersion})
	var length [4]byte
	for _, key := range keys {
		binary.BigEndian.PutUint32(length[:], uint32(len(key)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(key))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func encodeKeyedReadCursor(cursor keyedReadCursor) (string, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return "", newError(ErrInvalidState, "encode", "cursor", "", "cursor cannot be encoded")
	}
	value := base64.RawURLEncoding.EncodeToString(encoded)
	if len(value) > maxReadCursorBytes {
		return "", newError(ErrInvalidState, "encode", "cursor", "", "cursor exceeds its internal bound")
	}
	return value, nil
}

func decodeKeyedReadCursor(value string) (keyedReadCursor, error) {
	if len(value) > maxReadCursorBytes {
		return keyedReadCursor{}, newError(ErrInvalid, "list", "cursor", "", "cursor is too large")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || !utf8.Valid(decoded) {
		return keyedReadCursor{}, newError(ErrInvalid, "list", "cursor", "", "cursor is malformed")
	}
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()
	var cursor keyedReadCursor
	if err := decoder.Decode(&cursor); err != nil {
		return keyedReadCursor{}, newError(ErrInvalid, "list", "cursor", "", "cursor is malformed")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return keyedReadCursor{}, newError(ErrInvalid, "list", "cursor", "", "cursor has trailing data")
	}
	return cursor, nil
}
