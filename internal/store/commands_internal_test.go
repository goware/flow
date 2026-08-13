package store

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/goware/flow/internal/flowerr"
)

func TestPostgresSecondsDurationIsBounded(t *testing.T) {
	for _, seconds := range []float64{-1, 0, math.NaN()} {
		if duration, ok := postgresSecondsDuration(seconds); ok || duration != 0 {
			t.Fatalf("postgresSecondsDuration(%v) = %s, %t; want 0, false", seconds, duration, ok)
		}
	}
	if duration, ok := postgresSecondsDuration(1.5); !ok || duration != 1500*time.Millisecond {
		t.Fatalf("postgresSecondsDuration(1.5) = %s, %t", duration, ok)
	}
	for _, seconds := range []float64{math.Inf(1), math.MaxFloat64} {
		if duration, ok := postgresSecondsDuration(seconds); !ok || duration != time.Duration(math.MaxInt64) {
			t.Fatalf("postgresSecondsDuration(%v) = %s, %t; want MaxInt64, true", seconds, duration, ok)
		}
	}
}

func TestSuccessfulSettlementJournalLayoutMapsAcceptedPositions(t *testing.T) {
	layout := newSuccessfulSettlementJournalLayout(2, 2)
	application := "application"
	commandTerminal := "command_terminal"
	result := ApplyResult{Journal: []JournalRow{
		{Position: 10, Kind: AttemptConcluded},
		{Position: 11, Kind: EventRecorded, EventClass: &application},
		{Position: 12, Kind: EventRecorded, EventClass: &application},
		{Position: 13, Kind: CommandCreated},
		{Position: 14, Kind: CommandCreated},
		{Position: 15, Kind: EventRecorded, EventClass: &commandTerminal},
		{Position: 16, Kind: EventRecorded},
	}}

	if err := layout.validateAccepted(result); err != nil {
		t.Fatalf("validateAccepted() error = %v", err)
	}
	if got := layout.applicationEventPosition(result, 0); got != 11 {
		t.Fatalf("first application event position = %d, want 11", got)
	}
	if got := layout.applicationEventPosition(result, 1); got != 12 {
		t.Fatalf("second application event position = %d, want 12", got)
	}
	if got := layout.childCreatedPosition(result, 0); got != 13 {
		t.Fatalf("first child position = %d, want 13", got)
	}
	if got := layout.childCreatedPosition(result, 1); got != 14 {
		t.Fatalf("second child position = %d, want 14", got)
	}
	if got := layout.parentTerminalPosition(result); got != 15 {
		t.Fatalf("parent terminal position = %d, want 15", got)
	}

	invalid := result
	invalid.Journal = append([]JournalRow(nil), result.Journal...)
	invalid.Journal[2].EventClass = &commandTerminal
	if err := layout.validateAccepted(invalid); !errors.Is(err, flowerr.ErrInvalidState) {
		t.Fatalf("validateAccepted(invalid) error = %v, want invalid state", err)
	}
}
