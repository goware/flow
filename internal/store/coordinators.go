package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/canonical"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/flowerr"
	"github.com/goware/flow/internal/pgschema"
	retrypolicy "github.com/goware/flow/internal/retry"
	"github.com/goware/flow/internal/store/journalcodec"
	"github.com/jackc/pgx/v5"
)

type CoordinatorKind struct {
	Name    string
	Version int
}

type CoordinatorSelector struct {
	Namespace string
	Name      string
	Version   int
	Outcome   bool
}

type CoordinatorCandidate struct {
	CoordinatorID uuid.UUID
	ExecutionID   uuid.UUID
	Name          string
	Version       int
}

func (s *Store) ProbeCoordinators(ctx context.Context, kinds []CoordinatorKind, limit int) ([]CoordinatorCandidate, error) {
	if len(kinds) == 0 || limit <= 0 {
		return nil, nil
	}
	names := make([]string, len(kinds))
	versions := make([]int32, len(kinds))
	for i, kind := range kinds {
		if kind.Name == "" || kind.Version <= 0 {
			return nil, fmt.Errorf("%w: invalid coordinator probe kind", flowerr.ErrInvalid)
		}
		names[i], versions[i] = kind.Name, int32(kind.Version)
	}
	rows, err := s.db.Conn.Query(ctx, `WITH handled(name,version) AS (SELECT * FROM unnest($1::text[],$2::integer[]))
		SELECT candidate.coordinator_id,candidate.execution_id,candidate.name,candidate.version
		FROM handled h CROSS JOIN LATERAL (
			SELECT c.coordinator_id,c.execution_id,c.name,c.version
			FROM `+pgschema.Table(s.schema, "flow_coordinators")+` c
			JOIN `+pgschema.Table(s.schema, "flow_executions")+` e USING (execution_id)
			WHERE c.name=h.name AND c.version=h.version AND c.status='active'
			  AND e.status IN ('running','failing')
			  AND (c.delivery_state='idle' OR
			      (c.delivery_state IN ('ready','retry_wait') AND c.next_attempt_at<=clock_timestamp()))
			ORDER BY c.next_attempt_at NULLS FIRST,c.coordinator_id LIMIT $3
		) candidate ORDER BY candidate.coordinator_id LIMIT $3`, names, versions, limit)
	if err != nil {
		return nil, MapError("probe coordinators", err)
	}
	defer rows.Close()
	result := make([]CoordinatorCandidate, 0, limit)
	for rows.Next() {
		var value CoordinatorCandidate
		if err := rows.Scan(&value.CoordinatorID, &value.ExecutionID, &value.Name, &value.Version); err != nil {
			return nil, MapError("scan coordinator candidate", err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read coordinator candidates", err)
	}
	return result, nil
}

type CoordinatorDelivery struct {
	Start          bool
	Position       *int64
	EventID        *uuid.UUID
	Namespace      string
	Name           string
	Version        int
	Key            string
	RecordedAt     time.Time
	Body           []byte
	TerminalStatus string
	CommandName    string
	CommandVersion int
	CommandResult  []byte
	CommandFailure []byte
}

type ClaimedCoordinator struct {
	CoordinatorID          uuid.UUID
	ExecutionID            uuid.UUID
	Name                   string
	Version                int
	State                  []byte
	StateRevision          int64
	DeliveryKey            string
	Delivery               CoordinatorDelivery
	RetryPolicy            []byte
	BudgetStartedAt        time.Time
	ExecutionDeadline      *time.Time
	Attempt                int
	ConsumedAttempts       int
	AttemptID              uuid.UUID
	LeaseToken             uuid.UUID
	DBNow                  time.Time
	LeaseExpiresAt         time.Time
	AttemptStartedPosition int64
}

type CoordinatorClaimResult struct {
	Coordinator *ClaimedCoordinator
	Progressed  bool
}

func (s *Store) ClaimCoordinator(ctx context.Context, candidate CoordinatorCandidate, selectors []CoordinatorSelector,
	lease time.Duration, owner string, hook fault.Hook) (CoordinatorClaimResult, error) {
	if candidate.CoordinatorID == uuid.Nil || candidate.ExecutionID == uuid.Nil || lease <= 0 || owner == "" {
		return CoordinatorClaimResult{}, fmt.Errorf("%w: incomplete coordinator claim", flowerr.ErrInvalid)
	}
	if hook == nil {
		hook = fault.None{}
	}
	if err := hook.Hit(ctx, fault.ClaimExecutionLock); err != nil {
		return CoordinatorClaimResult{}, err
	}
	semantic, err := s.BeginSemantic(ctx, candidate.ExecutionID, LockSkipLocked)
	if errors.Is(err, ErrLockUnavailable) {
		return CoordinatorClaimResult{}, nil
	}
	if err != nil {
		return CoordinatorClaimResult{}, err
	}
	defer semantic.Rollback(context.WithoutCancel(ctx))

	var state, deliveryState string
	var deliveryKey *string
	var deliveryPosition *int64
	var stateBytes, policyBytes []byte
	var stateRevision int64
	var budgetStartedAt, nextAttemptAt *time.Time
	var ordinal, consumed int
	var executionStatus string
	var deadline *time.Time
	err = semantic.PGX().QueryRow(ctx, `SELECT c.state,c.state_revision,c.delivery_state,c.delivery_key,c.delivery_position,
		c.retry_policy::text::bytea,c.budget_started_at,c.next_attempt_at,c.attempt_ordinal,c.consumed_attempts,
		e.status,e.deadline_at
		FROM `+pgschema.Table(s.schema, "flow_coordinators")+` c
		JOIN `+pgschema.Table(s.schema, "flow_executions")+` e USING (execution_id)
		WHERE c.coordinator_id=$1 AND c.execution_id=$2 AND c.name=$3 AND c.version=$4
		FOR UPDATE OF c SKIP LOCKED`, candidate.CoordinatorID, candidate.ExecutionID, candidate.Name, candidate.Version).
		Scan(&stateBytes, &stateRevision, &deliveryState, &deliveryKey, &deliveryPosition, &policyBytes,
			&budgetStartedAt, &nextAttemptAt, &ordinal, &consumed, &executionStatus, &deadline)
	if errors.Is(err, pgx.ErrNoRows) {
		return CoordinatorClaimResult{}, nil
	}
	if err != nil {
		return CoordinatorClaimResult{}, MapError("lock coordinator claim", err)
	}
	state = executionStatus
	if state != "running" && state != "failing" {
		return CoordinatorClaimResult{}, nil
	}
	if deadline != nil && !semantic.DBNow().Before(*deadline) {
		if err := s.expireExecutionLocked(ctx, semantic, "execution deadline reached"); err != nil {
			return CoordinatorClaimResult{}, err
		}
		if err := semantic.Commit(ctx); err != nil {
			return CoordinatorClaimResult{}, err
		}
		return CoordinatorClaimResult{Progressed: true}, nil
	}
	if deliveryState == "idle" {
		selected, err := s.selectCoordinatorDeliveryLocked(ctx, semantic, selectors)
		if err != nil {
			return CoordinatorClaimResult{}, err
		}
		if selected == nil {
			return CoordinatorClaimResult{}, nil
		}
		deliveryKey, deliveryPosition = &selected.Key, selected.Position
		budget := semantic.DBNow()
		budgetStartedAt, nextAttemptAt = &budget, &budget
		deliveryState = "ready"
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_coordinators")+`
			SET delivery_key=$2,delivery_position=$3,delivery_state='ready',budget_started_at=$4,
			    next_attempt_at=$4,updated_at=$4 WHERE coordinator_id=$1`, candidate.CoordinatorID,
			selected.Key, selected.Position, semantic.DBNow()); err != nil {
			return CoordinatorClaimResult{}, MapError("select coordinator delivery", err)
		}
	}
	if deliveryKey == nil || (deliveryState != "ready" && deliveryState != "retry_wait") ||
		nextAttemptAt == nil || semantic.DBNow().Before(*nextAttemptAt) {
		return CoordinatorClaimResult{}, nil
	}
	if err := hook.Hit(ctx, fault.ClaimBeforeJournal); err != nil {
		return CoordinatorClaimResult{}, err
	}
	attemptID, token := uuid.New(), uuid.New()
	started, err := NewJournalEntry(AttemptStarted, journalcodec.AttemptStartedBody{
		V: 1, AttemptID: attemptID.String(), Attempt: ordinal + 1, StartedAt: semantic.DBNow(), Worker: owner,
		LeaseDurationMS: lease.Milliseconds(), ConsumedAttempts: consumed, BudgetStartedAt: *budgetStartedAt,
		CoordinatorID: candidate.CoordinatorID.String(), DeliveryKey: *deliveryKey,
	})
	if err != nil {
		return CoordinatorClaimResult{}, err
	}
	started.CoordinatorID = clonePointer(&candidate.CoordinatorID)
	started.AttemptID = clonePointer(&attemptID)
	if deliveryPosition != nil {
		started.CausationPosition = clonePointer(deliveryPosition)
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: []JournalEntry{started}})
	if err != nil {
		return CoordinatorClaimResult{}, err
	}
	expires := semantic.DBNow().Add(lease)
	if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_coordinators")+`
		SET delivery_state='running',attempt_ordinal=attempt_ordinal+1,active_attempt_id=$2,lease_token=$3,
		    lease_owner=$4,lease_started_at=$5,lease_expires_at=$6,updated_at=$5 WHERE coordinator_id=$1`,
		candidate.CoordinatorID, attemptID, token, owner, semantic.DBNow(), expires); err != nil {
		return CoordinatorClaimResult{}, MapError("claim coordinator", err)
	}
	delivery, err := s.loadCoordinatorDeliveryLocked(ctx, semantic, *deliveryKey, deliveryPosition)
	if err != nil {
		return CoordinatorClaimResult{}, err
	}
	if err := hook.Hit(ctx, fault.ClaimBeforeCommit); err != nil {
		return CoordinatorClaimResult{}, err
	}
	if err := semantic.Commit(ctx); err != nil {
		return CoordinatorClaimResult{}, err
	}
	claim := &ClaimedCoordinator{
		CoordinatorID: candidate.CoordinatorID, ExecutionID: candidate.ExecutionID, Name: candidate.Name,
		Version: candidate.Version, State: slices.Clone(stateBytes), StateRevision: stateRevision,
		DeliveryKey: *deliveryKey, Delivery: delivery, RetryPolicy: slices.Clone(policyBytes),
		BudgetStartedAt: *budgetStartedAt, ExecutionDeadline: clonePointer(deadline), Attempt: ordinal + 1,
		ConsumedAttempts: consumed, AttemptID: attemptID, LeaseToken: token, DBNow: semantic.DBNow(),
		LeaseExpiresAt: expires, AttemptStartedPosition: journal.Journal[0].Position,
	}
	return CoordinatorClaimResult{Coordinator: claim, Progressed: true}, nil
}

func (s *Store) selectCoordinatorDeliveryLocked(ctx context.Context, semantic *SemanticTx, selectors []CoordinatorSelector) (*CoordinatorDelivery, error) {
	var startPending bool
	var inbox int64
	if err := semantic.PGX().QueryRow(ctx, `SELECT start_pending,inbox_position FROM `+
		pgschema.Table(s.schema, "flow_coordinators")+` WHERE execution_id=$1`, semantic.ExecutionID()).Scan(&startPending, &inbox); err != nil {
		return nil, MapError("load coordinator inbox", err)
	}
	if startPending {
		return &CoordinatorDelivery{Start: true, Key: "start"}, nil
	}
	var eventNamespaces, eventNames, outcomeNames []string
	var eventVersions, outcomeVersions []int32
	for _, selector := range selectors {
		if selector.Name == "" || selector.Version <= 0 {
			return nil, fmt.Errorf("%w: invalid coordinator selector", flowerr.ErrInvalid)
		}
		if selector.Outcome {
			outcomeNames = append(outcomeNames, selector.Name)
			outcomeVersions = append(outcomeVersions, int32(selector.Version))
		} else {
			eventNamespaces = append(eventNamespaces, selector.Namespace)
			eventNames = append(eventNames, selector.Name)
			eventVersions = append(eventVersions, int32(selector.Version))
		}
	}
	var position int64
	err := semantic.PGX().QueryRow(ctx, `SELECT j.position FROM `+pgschema.Table(s.schema, "flow_journal")+` j
		WHERE j.execution_id=$1 AND j.position>$2 AND j.entry_kind='event_recorded' AND (
		  EXISTS (SELECT 1 FROM unnest($3::text[],$4::text[],$5::integer[]) s(namespace,name,version)
		          WHERE j.event_namespace=s.namespace AND j.event_name=s.name AND j.event_version=s.version)
		  OR (j.event_class='command_terminal' AND EXISTS (
		      SELECT 1 FROM `+pgschema.Table(s.schema, "flow_commands")+` c,
		          unnest($6::text[],$7::integer[]) s(name,version)
		      WHERE c.command_id=j.command_id AND c.name=s.name AND c.version=s.version)))
		ORDER BY j.position LIMIT 1`, semantic.ExecutionID(), inbox, eventNamespaces, eventNames, eventVersions,
		outcomeNames, outcomeVersions).Scan(&position)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, MapError("select coordinator event", err)
	}
	key := fmt.Sprintf("event/%d", position)
	return &CoordinatorDelivery{Position: &position, Key: key}, nil
}

func (s *Store) loadCoordinatorDeliveryLocked(ctx context.Context, semantic *SemanticTx, key string, position *int64) (CoordinatorDelivery, error) {
	if position == nil {
		return CoordinatorDelivery{Start: true, Key: key}, nil
	}
	var value CoordinatorDelivery
	value.Key = key
	value.Position = clonePointer(position)
	var eventID uuid.UUID
	var eventKey, terminal *string
	var commandName *string
	var commandVersion *int
	var result, failure []byte
	err := semantic.PGX().QueryRow(ctx, `SELECT j.event_id,j.event_namespace,j.event_name,j.event_version,j.event_key,
		j.recorded_at,j.body,j.terminal_status,c.name,c.version,c.result,c.terminal_failure
		FROM `+pgschema.Table(s.schema, "flow_journal")+` j
		LEFT JOIN `+pgschema.Table(s.schema, "flow_commands")+` c ON c.command_id=j.command_id
		WHERE j.execution_id=$1 AND j.position=$2 AND j.entry_kind='event_recorded'`, semantic.ExecutionID(), *position).
		Scan(&eventID, &value.Namespace, &value.Name, &value.Version, &eventKey, &value.RecordedAt,
			&value.Body, &terminal, &commandName, &commandVersion, &result, &failure)
	if err != nil {
		return CoordinatorDelivery{}, MapError("load coordinator delivery", err)
	}
	value.EventID = &eventID
	if eventKey != nil {
		value.Key = *eventKey
	}
	if terminal != nil {
		value.TerminalStatus = *terminal
	}
	if commandName != nil {
		value.CommandName = *commandName
	}
	if commandVersion != nil {
		value.CommandVersion = *commandVersion
	}
	value.CommandResult, value.CommandFailure = slices.Clone(result), slices.Clone(failure)
	return value, nil
}

type CoordinatorSuccess struct {
	Claim     ClaimedCoordinator
	State     canonical.Value
	Events    []ApplicationEvent
	Children  []CommandCreate
	Terminal  string
	ResultRef string
	Reason    string
}

func (s *Store) SettleCoordinatorSuccess(ctx context.Context, request CoordinatorSuccess, hook fault.Hook) (SettleResult, error) {
	if hook == nil {
		hook = fault.None{}
	}
	if len(request.State.Bytes) == 0 || (request.Terminal != "" && request.Terminal != "succeeded" && request.Terminal != "failed") {
		return SettleResult{}, fmt.Errorf("%w: invalid coordinator decision", flowerr.ErrInvalid)
	}
	if request.Terminal != "" && len(request.Children) != 0 {
		return SettleResult{}, fmt.Errorf("%w: terminal coordinator decision cannot spawn commands", flowerr.ErrInvalid)
	}
	semantic, err := s.BeginSemantic(ctx, request.Claim.ExecutionID, LockBlocking)
	if err != nil {
		return SettleResult{}, err
	}
	defer semantic.Rollback(context.WithoutCancel(ctx))
	fence, err := s.lockCoordinatorFence(ctx, semantic, request.Claim)
	if err != nil {
		return SettleResult{}, err
	}
	if err := hook.Hit(ctx, fault.SettleAfterFence); err != nil {
		return SettleResult{}, err
	}
	if fence.deadline != nil && !semantic.DBNow().Before(*fence.deadline) {
		if err := s.expireExecutionLocked(ctx, semantic, "execution deadline reached"); err != nil {
			return SettleResult{}, err
		}
		if err := semantic.Commit(ctx); err != nil {
			return SettleResult{}, err
		}
		return SettleResult{Terminal: true, Status: "expired"}, nil
	}
	if err := s.validateCoordinatorOutputs(ctx, semantic, fence.head, request); err != nil {
		return SettleResult{}, err
	}
	concluded, err := coordinatorAttemptConcluded(request.Claim, fence, "succeeded", false, fence.consumed, nil, "", "", semantic.DBNow())
	if err != nil {
		return SettleResult{}, err
	}
	entries := []JournalEntry{concluded}
	for _, event := range request.Events {
		entry := JournalEntry{EntryID: uuid.New(), Kind: EventRecorded, EventID: clonePointer(&event.ID),
			CoordinatorID: clonePointer(&request.Claim.CoordinatorID), EventNamespace: stringPointer("application"),
			EventName: clonePointer(&event.Name), EventVersion: clonePointer(&event.Version), EventKey: clonePointer(&event.Key),
			EventClass: stringPointer("application"), Body: event.Body}
		zero := 0
		entry.CausationBatchIndex = &zero
		entries = append(entries, entry)
	}
	childIndexes := make([]int, len(request.Children))
	for i, child := range request.Children {
		next := semantic.DBNow().Add(child.InitialDelay)
		entry, err := commandCreatedEntry(child, next, next)
		if err != nil {
			return SettleResult{}, err
		}
		zero := 0
		entry.CausationBatchIndex = &zero
		childIndexes[i] = len(entries)
		entries = append(entries, entry)
	}
	transitionIndex := len(entries)
	transition, err := NewJournalEntry(CoordinatorTransition, journalcodec.CoordinatorTransitionBody{
		V: 1, CoordinatorID: request.Claim.CoordinatorID.String(), DeliveryKey: request.Claim.DeliveryKey,
		HandledPosition: clonePointer(request.Claim.Delivery.Position), PriorStateRevision: fence.stateRevision,
		StateRevision: fence.stateRevision + 1, State: json.RawMessage(request.State.BytesCopy()),
		TerminalDecision: request.Terminal, ResultRef: request.ResultRef,
	})
	if err != nil {
		return SettleResult{}, err
	}
	transition.CoordinatorID = clonePointer(&request.Claim.CoordinatorID)
	zero := 0
	transition.CausationBatchIndex = &zero
	entries = append(entries, transition)

	first, err := semantic.nextJournalPosition(ctx)
	if err != nil {
		return SettleResult{}, err
	}
	var waitUpdates []graphWaitUpdate
	for i, event := range request.Events {
		matches, err := s.matchingWaitsLocked(ctx, semantic, "application", event.Name, event.Version, first+int64(1+i))
		if err != nil {
			return SettleResult{}, err
		}
		waitUpdates = append(waitUpdates, matches...)
	}
	resolution, err := s.resolveGraphLocked(ctx, semantic, nil, waitUpdates)
	if err != nil {
		return SettleResult{}, err
	}
	skippedOffset := len(entries)
	skipped, err := resolution.skippedEntries(transitionIndex)
	if err != nil {
		return SettleResult{}, err
	}
	entries = append(entries, skipped...)

	var cancelled []terminalizedCommand
	if request.Terminal != "" {
		cancelled, err = s.coordinatorTerminalCommandsLocked(ctx, semantic)
		if err != nil {
			return SettleResult{}, err
		}
		for i := range cancelled {
			causationIndex := transitionIndex
			if cancelled[i].AttemptID != nil && cancelled[i].AttemptPosition != nil {
				concluded, err := NewJournalEntry(AttemptConcluded, journalcodec.AttemptConcludedBody{
					V: 1, AttemptID: cancelled[i].AttemptID.String(), CommandID: cancelled[i].ID.String(),
					CommandKey: cancelled[i].Key, Attempt: cancelled[i].AttemptOrdinal, Classification: "cancelled",
					ConsumedAttempts: cancelled[i].ConsumedAttempts, FinishedAt: semantic.DBNow(),
					ErrorCode: "coordinator_completed", ErrorMessage: "coordinator completed the execution",
				})
				if err != nil {
					return SettleResult{}, err
				}
				concluded.CommandID, concluded.AttemptID = clonePointer(&cancelled[i].ID), clonePointer(cancelled[i].AttemptID)
				concluded.CausationPosition = clonePointer(cancelled[i].AttemptPosition)
				causationIndex = len(entries)
				entries = append(entries, concluded)
			}
			entry, err := terminalEventWithCode(cancelled[i].ID, cancelled[i].Key, "cancelled", "coordinator_completed",
				"coordinator completed the execution", "flow.command_cancelled", "command_terminal")
			if err != nil {
				return SettleResult{}, err
			}
			entry.CausationBatchIndex = &causationIndex
			cancelled[i].journalIndex = len(entries)
			entries = append(entries, entry)
		}
		name := "flow.execution_succeeded"
		if request.Terminal == "failed" {
			name = "flow.execution_failed"
		}
		terminal, err := executionTerminalEvent(request.Terminal, request.Reason, name)
		if err != nil {
			return SettleResult{}, err
		}
		terminal.CausationBatchIndex = &transitionIndex
		entries = append(entries, terminal)
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return SettleResult{}, err
	}
	if err := hook.Hit(ctx, fault.CoordinatorBeforeInboxAdvance); err != nil {
		return SettleResult{}, err
	}
	for i, child := range request.Children {
		next := semantic.DBNow().Add(child.InitialDelay)
		if err := s.insertCommand(ctx, semantic.PGX(), semantic.ExecutionID(), child, journal.Journal[childIndexes[i]].Position, next, next); err != nil {
			return SettleResult{}, err
		}
	}
	if err := s.applyGraphResolution(ctx, semantic, resolution, journal, skippedOffset); err != nil {
		return SettleResult{}, err
	}
	startPending := request.Claim.Delivery.Start
	inbox := int64(0)
	if request.Claim.Delivery.Position != nil {
		inbox = *request.Claim.Delivery.Position
	}
	if request.Terminal == "" {
		_, err = semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_coordinators")+`
			SET state=$2,state_hash=$3,state_revision=state_revision+1,state_position=$4,
			    start_pending=CASE WHEN $5 THEN false ELSE start_pending END,
			    inbox_position=GREATEST(inbox_position,$6),delivery_key=NULL,delivery_position=NULL,delivery_state='idle',
			    budget_started_at=NULL,next_attempt_at=NULL,consumed_attempts=0,active_attempt_id=NULL,lease_token=NULL,
			    lease_owner=NULL,lease_started_at=NULL,lease_expires_at=NULL,last_error=NULL,updated_at=$7
			WHERE coordinator_id=$1`, request.Claim.CoordinatorID, request.State.Bytes, request.State.Digest[:],
			journal.Journal[transitionIndex].Position, startPending, inbox, semantic.DBNow())
		if err != nil {
			return SettleResult{}, MapError("advance coordinator inbox", err)
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET command_count=command_count+$2,open_commands=open_commands+$2-$3,updated_at=$4 WHERE execution_id=$1`,
			semantic.ExecutionID(), len(request.Children), len(resolution.skipped), semantic.DBNow()); err != nil {
			return SettleResult{}, MapError("update execution after coordinator decision", err)
		}
	} else {
		failure := terminalFailure{Code: "coordinator_completed", Message: "coordinator completed the execution"}
		if request.Terminal == "failed" {
			failure = terminalFailure{Code: "coordinator_failed", Message: request.Reason}
		}
		for _, command := range cancelled {
			position := journal.Journal[command.journalIndex].Position
			if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
				SET state='cancelled',terminal_failure=$2::jsonb,terminal_position=$3,finished_at=$4,updated_at=$4,status_at=$4
				WHERE command_id=$1`, command.ID, jsonString(failure), position, semantic.DBNow()); err != nil {
				return SettleResult{}, MapError("cancel coordinator outstanding command", err)
			}
		}
		if _, err := semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE execution_id=$1`, semantic.ExecutionID()); err != nil {
			return SettleResult{}, MapError("remove coordinator execution queue", err)
		}
		coordinatorStatus := "completed"
		if request.Terminal == "failed" {
			coordinatorStatus = "failed"
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_coordinators")+`
			SET status=$2,state=$3,state_hash=$4,state_revision=state_revision+1,state_position=$5,start_pending=false,
			    inbox_position=GREATEST(inbox_position,$6),
			    delivery_key=NULL,delivery_position=NULL,delivery_state='idle',active_attempt_id=NULL,lease_token=NULL,
			    lease_owner=NULL,lease_started_at=NULL,lease_expires_at=NULL,finished_at=$7,updated_at=$7
			WHERE coordinator_id=$1`, request.Claim.CoordinatorID, coordinatorStatus, request.State.Bytes,
			request.State.Digest[:], journal.Journal[transitionIndex].Position, inbox, semantic.DBNow()); err != nil {
			return SettleResult{}, MapError("complete coordinator", err)
		}
		if _, err := semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET status=$2,open_commands=0,failure=CASE WHEN $2='failed' THEN $3::jsonb ELSE failure END,
			    outcome_ref=CASE WHEN $2='succeeded' THEN NULLIF($5,'') ELSE outcome_ref END,
			    finished_at=$4,updated_at=$4,status_at=$4 WHERE execution_id=$1`, semantic.ExecutionID(),
			request.Terminal, jsonString(failure), semantic.DBNow(), request.ResultRef); err != nil {
			return SettleResult{}, MapError("complete coordinator execution", err)
		}
	}
	if err := hook.Hit(ctx, fault.SettleBeforeCommit); err != nil {
		return SettleResult{}, err
	}
	if err := semantic.Commit(ctx); err != nil {
		return SettleResult{}, err
	}
	return SettleResult{Terminal: request.Terminal != "", Status: map[bool]string{true: request.Terminal, false: "idle"}[request.Terminal != ""]}, nil
}

type coordinatorFence struct {
	head            ExecutionHead
	stateRevision   int64
	consumed        int
	retryPolicy     []byte
	budgetStartedAt time.Time
	deadline        *time.Time
}

func (s *Store) lockCoordinatorFence(ctx context.Context, semantic *SemanticTx, claim ClaimedCoordinator) (coordinatorFence, error) {
	head, err := s.LoadExecutionHead(ctx, semantic)
	if err != nil {
		return coordinatorFence{}, err
	}
	if head.Mode != DriverCoordinator || (head.Status != "running" && head.Status != "failing") {
		return coordinatorFence{}, fmt.Errorf("%w: coordinator execution is terminal", flowerr.ErrTerminal)
	}
	var result coordinatorFence
	result.head = head
	var status, deliveryState, deliveryKey string
	var active, token uuid.UUID
	var leaseExpires time.Time
	err = semantic.PGX().QueryRow(ctx, `SELECT c.status,c.delivery_state,c.delivery_key,c.state_revision,c.consumed_attempts,
		c.retry_policy::text::bytea,c.budget_started_at,c.active_attempt_id,c.lease_token,c.lease_expires_at,e.deadline_at
		FROM `+pgschema.Table(s.schema, "flow_coordinators")+` c JOIN `+pgschema.Table(s.schema, "flow_executions")+` e USING(execution_id)
		WHERE c.coordinator_id=$1 AND c.execution_id=$2 FOR UPDATE OF c`, claim.CoordinatorID, claim.ExecutionID).
		Scan(&status, &deliveryState, &deliveryKey, &result.stateRevision, &result.consumed, &result.retryPolicy,
			&result.budgetStartedAt, &active, &token, &leaseExpires, &result.deadline)
	if err != nil {
		return coordinatorFence{}, MapError("lock coordinator fence", err)
	}
	if status != "active" || deliveryState != "running" || deliveryKey != claim.DeliveryKey ||
		active != claim.AttemptID || token != claim.LeaseToken || !semantic.DBNow().Before(leaseExpires) {
		return coordinatorFence{}, fmt.Errorf("%w: coordinator attempt no longer owns its lease", flowerr.ErrLeaseLost)
	}
	return result, nil
}

func coordinatorAttemptConcluded(claim ClaimedCoordinator, fence coordinatorFence, class string, consumedBudget bool,
	consumed int, next *time.Time, code, message string, now time.Time) (JournalEntry, error) {
	entry, err := NewJournalEntry(AttemptConcluded, journalcodec.AttemptConcludedBody{
		V: 1, AttemptID: claim.AttemptID.String(), Attempt: claim.Attempt, Classification: class,
		ConsumedBudget: consumedBudget, ConsumedAttempts: consumed, FinishedAt: now, NextAttemptAt: next,
		ErrorCode: code, ErrorMessage: message, CoordinatorID: claim.CoordinatorID.String(), DeliveryKey: claim.DeliveryKey,
	})
	if err != nil {
		return JournalEntry{}, err
	}
	entry.CoordinatorID = clonePointer(&claim.CoordinatorID)
	entry.AttemptID = clonePointer(&claim.AttemptID)
	entry.CausationPosition = clonePointer(&claim.AttemptStartedPosition)
	return entry, nil
}

func (s *Store) validateCoordinatorOutputs(ctx context.Context, semantic *SemanticTx, head ExecutionHead, request CoordinatorSuccess) error {
	if head.MaxCommands > 0 && head.CommandCount+len(request.Children) > head.MaxCommands {
		return fmt.Errorf("%w: execution command ceiling exceeded", flowerr.ErrInvalidState)
	}
	keys := make([]string, len(request.Children))
	for i, child := range request.Children {
		if child.Origin != "coordinator_spawn" || child.ParentCommandID != nil {
			return fmt.Errorf("%w: invalid coordinator-spawned command", flowerr.ErrInvalid)
		}
		keys[i] = child.Key
	}
	if len(keys) > 0 {
		var conflicts int
		if err := semantic.PGX().QueryRow(ctx, `SELECT count(*) FROM `+pgschema.Table(s.schema, "flow_commands")+`
			WHERE execution_id=$1 AND command_key=ANY($2)`, semantic.ExecutionID(), keys).Scan(&conflicts); err != nil {
			return MapError("validate coordinator command keys", err)
		}
		if conflicts != 0 {
			return fmt.Errorf("%w: coordinator command key already exists", flowerr.ErrConflict)
		}
	}
	for _, event := range request.Events {
		existing, err := s.LookupApplicationEvent(ctx, semantic.PGX(), semantic.ExecutionID(), event.Name, event.Key)
		if err != nil {
			return err
		}
		if existing.Found {
			if existing.Version == event.Version && bytes.Equal(existing.Body, event.Body.Bytes) {
				return fmt.Errorf("%w: coordinator event identity already exists", flowerr.ErrConflict)
			}
			return fmt.Errorf("%w: coordinator event identity differs", flowerr.ErrConflict)
		}
	}
	return nil
}

type terminalizedCommand struct {
	ID               uuid.UUID
	Key              string
	journalIndex     int
	AttemptID        *uuid.UUID
	AttemptOrdinal   int
	ConsumedAttempts int
	AttemptPosition  *int64
}

func (s *Store) coordinatorTerminalCommandsLocked(ctx context.Context, semantic *SemanticTx) ([]terminalizedCommand, error) {
	rows, err := semantic.PGX().Query(ctx, `SELECT c.command_id,c.command_key,c.attempt_ordinal,c.consumed_attempts,
		q.active_attempt_id,(SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+`
		 WHERE execution_id=c.execution_id AND attempt_id=q.active_attempt_id AND entry_kind='attempt_started')
		FROM `+pgschema.Table(s.schema, "flow_commands")+` c
		LEFT JOIN `+pgschema.Table(s.schema, "flow_command_queue")+` q ON q.command_id=c.command_id
		WHERE c.execution_id=$1 AND c.state NOT IN ('succeeded','failed','cancelled','expired','skipped')
		ORDER BY c.command_key FOR UPDATE OF c`, semantic.ExecutionID())
	if err != nil {
		return nil, MapError("lock coordinator terminal commands", err)
	}
	defer rows.Close()
	var result []terminalizedCommand
	for rows.Next() {
		var value terminalizedCommand
		if err := rows.Scan(&value.ID, &value.Key, &value.AttemptOrdinal, &value.ConsumedAttempts,
			&value.AttemptID, &value.AttemptPosition); err != nil {
			return nil, MapError("scan coordinator terminal command", err)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, MapError("read coordinator terminal commands", err)
	}
	return result, nil
}

type CoordinatorConclusion struct {
	Claim          ClaimedCoordinator
	Classification retrypolicy.ErrorClass
	ExplicitDelay  *time.Duration
	ErrorCode      string
	ErrorMessage   string
}

func (s *Store) SettleCoordinatorConclusion(ctx context.Context, request CoordinatorConclusion, hook fault.Hook) (SettleResult, error) {
	if hook == nil {
		hook = fault.None{}
	}
	semantic, err := s.BeginSemantic(ctx, request.Claim.ExecutionID, LockBlocking)
	if err != nil {
		return SettleResult{}, err
	}
	defer semantic.Rollback(context.WithoutCancel(ctx))
	fence, err := s.lockCoordinatorFence(ctx, semantic, request.Claim)
	if err != nil {
		return SettleResult{}, err
	}
	policy, err := retrypolicy.PublicFromCanonical(fence.retryPolicy)
	if err != nil {
		return SettleResult{}, fmt.Errorf("%w: invalid coordinator retry policy", flowerr.ErrInvalidState)
	}
	decision, err := retrypolicy.DecidePublic(policy, retrypolicy.Input{
		DBNow: semantic.DBNow(), BudgetStartedAt: fence.budgetStartedAt, ConsumedAttempts: fence.consumed,
		AttemptID: request.Claim.AttemptID.String(), Classification: request.Classification,
		ExplicitDelay: request.ExplicitDelay, ExecutionDeadline: fence.deadline,
	})
	if err != nil {
		return SettleResult{}, err
	}
	if decision.Retry && decision.NextAttemptAt.IsZero() {
		decision.NextAttemptAt = semantic.DBNow()
	}
	var next *time.Time
	if decision.Retry {
		next = clonePointer(&decision.NextAttemptAt)
	}
	concluded, err := coordinatorAttemptConcluded(request.Claim, fence, string(request.Classification), decision.ConsumesAttempt,
		decision.ConsumedAttempts, next, request.ErrorCode, request.ErrorMessage, semantic.DBNow())
	if err != nil {
		return SettleResult{}, err
	}
	entries := []JournalEntry{concluded}
	var commands []terminalizedCommand
	if !decision.Retry {
		commands, err = s.coordinatorTerminalCommandsLocked(ctx, semantic)
		if err != nil {
			return SettleResult{}, err
		}
		for i := range commands {
			causationIndex := 0
			if commands[i].AttemptID != nil && commands[i].AttemptPosition != nil {
				concludedCommand, err := NewJournalEntry(AttemptConcluded, journalcodec.AttemptConcludedBody{
					V: 1, AttemptID: commands[i].AttemptID.String(), CommandID: commands[i].ID.String(),
					CommandKey: commands[i].Key, Attempt: commands[i].AttemptOrdinal, Classification: "cancelled",
					ConsumedAttempts: commands[i].ConsumedAttempts, FinishedAt: semantic.DBNow(),
					ErrorCode: "coordinator_failed", ErrorMessage: "coordinator failed",
				})
				if err != nil {
					return SettleResult{}, err
				}
				concludedCommand.CommandID, concludedCommand.AttemptID = clonePointer(&commands[i].ID), clonePointer(commands[i].AttemptID)
				concludedCommand.CausationPosition = clonePointer(commands[i].AttemptPosition)
				causationIndex = len(entries)
				entries = append(entries, concludedCommand)
			}
			entry, err := terminalEventWithCode(commands[i].ID, commands[i].Key, "cancelled", "coordinator_failed",
				"coordinator failed", "flow.command_cancelled", "command_terminal")
			if err != nil {
				return SettleResult{}, err
			}
			entry.CausationBatchIndex = &causationIndex
			commands[i].journalIndex = len(entries)
			entries = append(entries, entry)
		}
		failed := JournalEntry{}
		failed, err = NewJournalEntry(EventRecorded, journalcodec.TerminalEventBody{V: 1, Status: "failed", Code: request.ErrorCode, Reason: request.ErrorMessage})
		if err != nil {
			return SettleResult{}, err
		}
		eventID, version := uuid.New(), 1
		failed.CoordinatorID = clonePointer(&request.Claim.CoordinatorID)
		failed.EventID, failed.EventNamespace, failed.EventName, failed.EventVersion = &eventID, stringPointer("runtime"), stringPointer("flow.coordinator_failed"), &version
		failed.EventClass, failed.TerminalStatus = stringPointer("coordinator_terminal"), stringPointer("failed")
		zero := 0
		failed.CausationBatchIndex = &zero
		entries = append(entries, failed)
		execution, err := executionTerminalEvent("failed", request.ErrorMessage, "flow.execution_failed")
		if err != nil {
			return SettleResult{}, err
		}
		execution.CausationBatchIndex = &zero
		entries = append(entries, execution)
	}
	journal, err := semantic.Apply(ctx, PersistedChangeSet{Journal: entries})
	if err != nil {
		return SettleResult{}, err
	}
	failure := terminalFailure{Code: request.ErrorCode, Message: request.ErrorMessage}
	if decision.Retry {
		_, err = semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_coordinators")+`
			SET delivery_state='retry_wait',consumed_attempts=$2,next_attempt_at=$3,last_error=$4::jsonb,
			active_attempt_id=NULL,lease_token=NULL,lease_owner=NULL,lease_started_at=NULL,lease_expires_at=NULL,updated_at=$5
			WHERE coordinator_id=$1`, request.Claim.CoordinatorID, decision.ConsumedAttempts, decision.NextAttemptAt,
			jsonString(failure), semantic.DBNow())
		if err != nil {
			return SettleResult{}, MapError("retry coordinator delivery", err)
		}
	} else {
		for _, command := range commands {
			_, err = semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_commands")+`
				SET state='cancelled',terminal_failure=$2::jsonb,terminal_position=$3,finished_at=$4,updated_at=$4,status_at=$4 WHERE command_id=$1`,
				command.ID, jsonString(failure), journal.Journal[command.journalIndex].Position, semantic.DBNow())
			if err != nil {
				return SettleResult{}, MapError("cancel command after coordinator failure", err)
			}
		}
		if _, err = semantic.PGX().Exec(ctx, `DELETE FROM `+pgschema.Table(s.schema, "flow_command_queue")+` WHERE execution_id=$1`, semantic.ExecutionID()); err != nil {
			return SettleResult{}, MapError("remove command queue after coordinator failure", err)
		}
		if _, err = semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_coordinators")+`
			SET status='failed',delivery_state='idle',delivery_key=NULL,delivery_position=NULL,consumed_attempts=$2,
			last_error=$3::jsonb,active_attempt_id=NULL,lease_token=NULL,lease_owner=NULL,lease_started_at=NULL,
			lease_expires_at=NULL,finished_at=$4,updated_at=$4 WHERE coordinator_id=$1`, request.Claim.CoordinatorID,
			decision.ConsumedAttempts, jsonString(failure), semantic.DBNow()); err != nil {
			return SettleResult{}, MapError("fail coordinator", err)
		}
		if _, err = semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_executions")+`
			SET status='failed',open_commands=0,failure=$2::jsonb,finished_at=$3,updated_at=$3,status_at=$3 WHERE execution_id=$1`,
			semantic.ExecutionID(), jsonString(failure), semantic.DBNow()); err != nil {
			return SettleResult{}, MapError("fail coordinator execution", err)
		}
	}
	if err := semantic.Commit(ctx); err != nil {
		return SettleResult{}, err
	}
	return SettleResult{Retry: decision.Retry, Terminal: !decision.Retry, NextAttemptAt: next,
		Status: map[bool]string{true: "retry_wait", false: "failed"}[decision.Retry]}, nil
}

type CoordinatorLeaseRenewal struct {
	CoordinatorID uuid.UUID
	AttemptID     uuid.UUID
	Token         uuid.UUID
}

func (s *Store) RenewCoordinatorLeases(ctx context.Context, owner string, lease time.Duration, values []CoordinatorLeaseRenewal) (map[uuid.UUID]time.Time, error) {
	if len(values) == 0 {
		return nil, nil
	}
	ids, attempts, tokens := make([]uuid.UUID, len(values)), make([]uuid.UUID, len(values)), make([]uuid.UUID, len(values))
	for i, v := range values {
		ids[i], attempts[i], tokens[i] = v.CoordinatorID, v.AttemptID, v.Token
	}
	rows, err := s.db.Conn.Query(ctx, `WITH wanted(coordinator_id,attempt_id,token) AS (SELECT * FROM unnest($1::uuid[],$2::uuid[],$3::uuid[]))
		UPDATE `+pgschema.Table(s.schema, "flow_coordinators")+` c SET lease_expires_at=clock_timestamp()+($5 * interval '1 millisecond'),updated_at=clock_timestamp()
		FROM wanted w WHERE c.coordinator_id=w.coordinator_id AND c.active_attempt_id=w.attempt_id AND c.lease_token=w.token
		AND c.lease_owner=$4 AND c.delivery_state='running' AND c.lease_expires_at>clock_timestamp()
		RETURNING c.coordinator_id,c.lease_expires_at`, ids, attempts, tokens, owner, lease.Milliseconds())
	if err != nil {
		return nil, MapError("renew coordinator leases", err)
	}
	defer rows.Close()
	result := make(map[uuid.UUID]time.Time)
	for rows.Next() {
		var id uuid.UUID
		var expires time.Time
		if err := rows.Scan(&id, &expires); err != nil {
			return nil, MapError("scan coordinator lease", err)
		}
		result[id] = expires
	}
	return result, rows.Err()
}

type ExpiredCoordinatorLeaseCandidate struct{ CoordinatorID, ExecutionID uuid.UUID }

func (s *Store) ProbeExpiredCoordinatorLeases(ctx context.Context, limit int) ([]ExpiredCoordinatorLeaseCandidate, error) {
	rows, err := s.db.Conn.Query(ctx, `SELECT coordinator_id,execution_id FROM `+pgschema.Table(s.schema, "flow_coordinators")+`
		WHERE status='active' AND delivery_state='running' AND lease_expires_at<=clock_timestamp() ORDER BY lease_expires_at LIMIT $1`, limit)
	if err != nil {
		return nil, MapError("probe expired coordinator leases", err)
	}
	defer rows.Close()
	var result []ExpiredCoordinatorLeaseCandidate
	for rows.Next() {
		var v ExpiredCoordinatorLeaseCandidate
		if err := rows.Scan(&v.CoordinatorID, &v.ExecutionID); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, rows.Err()
}

func (s *Store) RecoverExpiredCoordinatorLease(ctx context.Context, candidate ExpiredCoordinatorLeaseCandidate) (bool, error) {
	semantic, err := s.BeginSemantic(ctx, candidate.ExecutionID, LockSkipLocked)
	if errors.Is(err, ErrLockUnavailable) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer semantic.Rollback(context.WithoutCancel(ctx))
	var attempt uuid.UUID
	var ordinal, consumed int
	var key string
	var expires time.Time
	var startedPosition int64
	err = semantic.PGX().QueryRow(ctx, `SELECT active_attempt_id,attempt_ordinal,consumed_attempts,delivery_key,lease_expires_at,
		(SELECT position FROM `+pgschema.Table(s.schema, "flow_journal")+` WHERE execution_id=$2 AND attempt_id=active_attempt_id AND entry_kind='attempt_started')
		FROM `+pgschema.Table(s.schema, "flow_coordinators")+` WHERE coordinator_id=$1 AND status='active' AND delivery_state='running' FOR UPDATE SKIP LOCKED`, candidate.CoordinatorID, candidate.ExecutionID).
		Scan(&attempt, &ordinal, &consumed, &key, &expires, &startedPosition)
	if errors.Is(err, pgx.ErrNoRows) || semantic.DBNow().Before(expires) {
		return false, nil
	}
	if err != nil {
		return false, MapError("lock expired coordinator lease", err)
	}
	claim := ClaimedCoordinator{CoordinatorID: candidate.CoordinatorID, ExecutionID: candidate.ExecutionID, AttemptID: attempt, Attempt: ordinal, DeliveryKey: key, AttemptStartedPosition: startedPosition}
	entry, err := coordinatorAttemptConcluded(claim, coordinatorFence{}, string(retrypolicy.ClassLeaseLost), false, consumed, &semantic.dbNow, "lease_lost", "coordinator lease expired", semantic.DBNow())
	if err != nil {
		return false, err
	}
	if _, err = semantic.Apply(ctx, PersistedChangeSet{Journal: []JournalEntry{entry}}); err != nil {
		return false, err
	}
	if _, err = semantic.PGX().Exec(ctx, `UPDATE `+pgschema.Table(s.schema, "flow_coordinators")+` SET delivery_state='retry_wait',next_attempt_at=$2,
		active_attempt_id=NULL,lease_token=NULL,lease_owner=NULL,lease_started_at=NULL,lease_expires_at=NULL,last_error=$3::jsonb,updated_at=$2 WHERE coordinator_id=$1`,
		candidate.CoordinatorID, semantic.DBNow(), jsonString(terminalFailure{Code: "lease_lost", Message: "coordinator lease expired"})); err != nil {
		return false, err
	}
	if err = semantic.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
