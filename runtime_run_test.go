package flow

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWakeHubBroadcastsOneGenerationToEveryScheduler(t *testing.T) {
	hub := newWakeHub()
	seen := hub.snapshot()
	const waiters = 8
	done := make(chan struct{}, waiters)
	for range waiters {
		go func() {
			hub.wait(context.Background(), seen, time.Minute)
			done <- struct{}{}
		}()
	}
	hub.signal()
	for range waiters {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("one wake generation did not reach every scheduler")
		}
	}
}

func TestLeaseRenewalResultCannotCancelWorkOutsideItsSnapshot(t *testing.T) {
	active := newActiveCommands()
	oldID, newID, replacedID := uuid.New(), uuid.New(), uuid.New()
	oldAttempt, newAttempt, replacedAttempt, replacementAttempt := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	oldCancelled := make(chan struct{}, 1)
	newCancelled := make(chan struct{}, 1)
	replacementCancelled := make(chan struct{}, 1)
	active.register(activeCommand{commandID: oldID, attemptID: oldAttempt, cancel: func(error) { oldCancelled <- struct{}{} }})
	active.register(activeCommand{commandID: newID, attemptID: newAttempt, cancel: func(error) { newCancelled <- struct{}{} }})
	// A newer retry of the same logical command may replace the snapshotted
	// attempt while its renewal query is in flight.
	active.register(activeCommand{commandID: replacedID, attemptID: replacementAttempt, cancel: func(error) { replacementCancelled <- struct{}{} }})
	active.renewed(replacedID, replacedAttempt, time.Now().Add(time.Hour))
	foundReplacement := false
	for _, value := range active.snapshot() {
		if value.commandID == replacedID {
			foundReplacement = true
			if !value.localExpiry.IsZero() {
				t.Fatal("an older renewal changed a newer command attempt's local expiry")
			}
		}
	}
	if !foundReplacement {
		t.Fatal("replacement command attempt missing")
	}

	active.cancelUnrenewed(map[uuid.UUID]uuid.UUID{oldID: oldAttempt, replacedID: replacedAttempt}, map[uuid.UUID]struct{}{})
	select {
	case <-oldCancelled:
	default:
		t.Fatal("an attempted but unrenewed lease was not cancelled")
	}
	select {
	case <-newCancelled:
		t.Fatal("a command registered after the renewal snapshot was cancelled")
	default:
	}
	select {
	case <-replacementCancelled:
		t.Fatal("a newer attempt for a snapshotted command was cancelled")
	default:
	}

	coordinators := newActiveCoordinators()
	coordinatorID, coordinatorOldAttempt, coordinatorNewAttempt := uuid.New(), uuid.New(), uuid.New()
	coordinatorCancelled := make(chan struct{}, 1)
	coordinators.register(activeCoordinator{coordinatorID: coordinatorID, attemptID: coordinatorNewAttempt,
		cancel: func(error) { coordinatorCancelled <- struct{}{} }})
	coordinators.renewed(coordinatorID, coordinatorOldAttempt, time.Now().Add(time.Hour))
	if got := coordinators.snapshot(); len(got) != 1 || !got[0].localExpiry.IsZero() {
		t.Fatal("an older renewal changed a newer coordinator attempt's local expiry")
	}
	coordinators.cancelUnrenewed(map[uuid.UUID]uuid.UUID{coordinatorID: coordinatorOldAttempt}, map[uuid.UUID]struct{}{})
	select {
	case <-coordinatorCancelled:
		t.Fatal("a newer coordinator attempt was cancelled by an older renewal result")
	default:
	}
}
