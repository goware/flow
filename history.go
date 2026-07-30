package flow

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/goware/flow/internal/store"
)

type HistoryKind string

const (
	HistoryExecutionStarted      HistoryKind = "execution_started"
	HistoryExecutionFailing      HistoryKind = "execution_failing"
	HistoryCommandCreated        HistoryKind = "command_created"
	HistoryAttemptStarted        HistoryKind = "attempt_started"
	HistoryAttemptConcluded      HistoryKind = "attempt_concluded"
	HistoryEventRecorded         HistoryKind = "event_recorded"
	HistoryPlanReconciled        HistoryKind = "plan_reconciled"
	HistoryCoordinatorTransition HistoryKind = "coordinator_transition"
)

type HistoryEntry struct {
	ExecutionID       ExecutionID
	Position          JournalPosition
	EntryID           JournalEntryID
	Kind              HistoryKind
	RecordedAt        time.Time
	CausationPosition *JournalPosition
	CommandID         CommandID
	AttemptID         AttemptID
	CoordinatorID     CoordinatorID
	PlanRevision      PlanRevision
	EventID           EventID
	EventNamespace    string
	EventName         string
	EventKey          string
	EventClass        string
	TerminalStatus    string
	Body              json.RawMessage
	BodyHash          string
}

type HistoryOption interface {
	applyHistory(*historyOptions)
}

type historyOptions struct {
	after JournalPosition
	limit int
	errs  []error
}

type historyOptionFunc func(*historyOptions)

func (f historyOptionFunc) applyHistory(options *historyOptions) { f(options) }

func HistoryAfter(position JournalPosition) HistoryOption {
	return historyOptionFunc(func(options *historyOptions) { options.after = position })
}

func HistoryLimit(limit int) HistoryOption {
	return historyOptionFunc(func(options *historyOptions) {
		if limit <= 0 || limit > store.MaxHistoryLimit {
			options.errs = append(options.errs, errors.New("history limit is out of range"))
			return
		}
		options.limit = limit
	})
}

func History(ctx context.Context, c Client, id ExecutionID, opts ...HistoryOption) ([]HistoryEntry, error) {
	executionID, err := parseExecutionID(id)
	if err != nil {
		return nil, err
	}
	client, err := resolveClient(c)
	if err != nil {
		return nil, err
	}
	options := historyOptions{limit: 100}
	for _, option := range opts {
		if option == nil {
			options.errs = append(options.errs, errors.New("nil history option"))
			continue
		}
		option.applyHistory(&options)
	}
	if err := errors.Join(options.errs...); err != nil {
		return nil, newError(ErrInvalid, "history", "options", "", err.Error())
	}
	rows, err := client.runtime.store.HistoryInTx(ctx, client.tx, executionID, uint64(options.after), options.limit)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 && options.after == 0 {
		// Every valid execution has ExecutionStarted at position one. Distinguish
		// a missing execution from an empty page without another query.
		return nil, newError(ErrNotFound, "history", "execution", string(id), "execution does not exist")
	}
	return historyEntries(rows), nil
}

func historyEntries(rows []store.JournalRow) []HistoryEntry {
	result := make([]HistoryEntry, len(rows))
	for index, row := range rows {
		entry := HistoryEntry{
			ExecutionID: ExecutionID(row.ExecutionID.String()), Position: JournalPosition(row.Position),
			EntryID: JournalEntryID(row.EntryID.String()), Kind: HistoryKind(row.Kind), RecordedAt: row.RecordedAt,
			Body: json.RawMessage(append([]byte(nil), row.Body...)), BodyHash: hex.EncodeToString(row.BodyHash[:]),
		}
		if row.CausationPosition != nil {
			position := JournalPosition(*row.CausationPosition)
			entry.CausationPosition = &position
		}
		if row.CommandID != nil {
			entry.CommandID = CommandID(row.CommandID.String())
		}
		if row.AttemptID != nil {
			entry.AttemptID = AttemptID(row.AttemptID.String())
		}
		if row.CoordinatorID != nil {
			entry.CoordinatorID = CoordinatorID(row.CoordinatorID.String())
		}
		if row.PlanRevision != nil {
			entry.PlanRevision = PlanRevision(*row.PlanRevision)
		}
		if row.EventID != nil {
			entry.EventID = EventID(row.EventID.String())
		}
		if row.EventNamespace != nil {
			entry.EventNamespace = *row.EventNamespace
		}
		if row.EventName != nil {
			entry.EventName = *row.EventName
		}
		if row.EventKey != nil {
			entry.EventKey = *row.EventKey
		}
		if row.EventClass != nil {
			entry.EventClass = *row.EventClass
		}
		if row.TerminalStatus != nil {
			entry.TerminalStatus = *row.TerminalStatus
		}
		result[index] = entry
	}
	return result
}
