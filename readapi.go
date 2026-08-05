package flow

import (
	"context"
	"time"

	"github.com/goware/flow/internal/store"
)

// MaxReadKeys bounds every by-keys batch read.
const MaxReadKeys = store.MaxReadKeys

// LiveWork is one queued (or leased) command of a non-terminal execution,
// carrying the execution's identity. It is the batch, key-addressed read for
// consumers that decorate their own domain rows with dispatch state, without
// touching flow's tables.
type LiveWork struct {
	ExecutionID      ExecutionID
	DefinitionName   string
	ExecutionKey     string
	KeyScope         string
	ExecutionStatus  string
	CommandID        CommandID
	CommandKey       string
	CommandName      string
	Queue            string
	QueueState       string
	NextRunAt        time.Time
	LeaseOwner       string
	LeaseExpiresAt   *time.Time
	AttemptOrdinal   int
	CommandCreatedAt time.Time
}

// LiveWorkByKeys returns the queued commands of every non-terminal execution
// whose execution key is in keys, ordered by key then next run time. Keys
// with no live execution contribute no rows. At most MaxReadKeys per call;
// transaction-scoped clients observe their own uncommitted writes.
func LiveWorkByKeys(ctx context.Context, c Client, keys []string) ([]LiveWork, error) {
	client, err := resolveClient(c)
	if err != nil {
		return nil, err
	}
	rows, err := client.runtime.store.LiveWorkByKeysInTx(ctx, client.tx, keys)
	if err != nil {
		return nil, err
	}
	work := make([]LiveWork, len(rows))
	for index, row := range rows {
		work[index] = LiveWork{
			ExecutionID:      ExecutionID(row.ExecutionID.String()),
			DefinitionName:   row.DefinitionName,
			ExecutionKey:     row.ExecutionKey,
			KeyScope:         row.KeyScope,
			ExecutionStatus:  row.ExecutionStatus,
			CommandID:        CommandID(row.CommandID.String()),
			CommandKey:       row.CommandKey,
			CommandName:      row.CommandName,
			Queue:            row.Queue,
			QueueState:       row.QueueState,
			NextRunAt:        row.NextRunAt,
			AttemptOrdinal:   row.AttemptOrdinal,
			CommandCreatedAt: row.CommandCreatedAt,
		}
		if row.LeaseOwner != nil {
			work[index].LeaseOwner = *row.LeaseOwner
		}
		if row.LeaseExpiresAt != nil {
			expires := *row.LeaseExpiresAt
			work[index].LeaseExpiresAt = &expires
		}
	}
	return work, nil
}

// KeyedHistoryEntry is a history entry carrying its execution's identity.
type KeyedHistoryEntry struct {
	DefinitionName string
	ExecutionKey   string
	KeyScope       string
	HistoryEntry
}

// HistoryByKeys returns the journal of every execution that ever held one of
// the keys, in write order across executions — the full trail for a set of
// related keys, spanning re-enqueues of live keys. At most MaxReadKeys per
// call; transaction-scoped clients observe their own uncommitted writes.
func HistoryByKeys(ctx context.Context, c Client, keys []string) ([]KeyedHistoryEntry, error) {
	client, err := resolveClient(c)
	if err != nil {
		return nil, err
	}
	rows, err := client.runtime.store.JournalByKeysInTx(ctx, client.tx, keys)
	if err != nil {
		return nil, err
	}
	entries := make([]KeyedHistoryEntry, len(rows))
	for index, row := range rows {
		entries[index] = KeyedHistoryEntry{
			DefinitionName: row.DefinitionName,
			ExecutionKey:   row.ExecutionKey,
			KeyScope:       row.KeyScope,
			HistoryEntry:   historyEntries([]store.JournalRow{row.Entry})[0],
		}
	}
	return entries, nil
}
