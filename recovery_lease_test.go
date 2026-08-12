package flow

import (
	"context"
	"errors"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/pgschema"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/store/journalcodec"
	"github.com/goware/flow/internal/testpg"
	flowuuid "github.com/goware/flow/internal/uuid"
)

func TestWithRecoveryLeaseValidationAndDeclarationIdentity(t *testing.T) {
	t.Parallel()

	valid := DefineCommand[None, None]("lease.valid", 1, WithRecoveryLease(29*time.Millisecond+time.Nanosecond))
	if valid.err != nil || valid.defaults.recoveryLease != 30*time.Millisecond {
		t.Fatalf("normalized recovery lease = %s, err=%v", valid.defaults.recoveryLease, valid.err)
	}
	for name, option := range map[string]CommandOption{
		"zero":      WithRecoveryLease(0),
		"negative":  WithRecoveryLease(-time.Millisecond),
		"too_short": WithRecoveryLease(29 * time.Millisecond),
		"overflow":  WithRecoveryLease(time.Duration(math.MaxInt64)),
	} {
		command := DefineCommand[None, None]("lease.invalid."+name, 1, option)
		if command.err == nil {
			t.Fatalf("%s recovery lease was accepted", name)
		}
	}
	duplicate := DefineCommand[None, None]("lease.duplicate", 1,
		WithRecoveryLease(time.Second), WithRecoveryLease(2*time.Second))
	if duplicate.err == nil {
		t.Fatal("duplicate recovery lease was accepted")
	}

	base := DefineCommand[None, None]("lease.identity", 1, WithRecoveryLease(time.Second))
	same := DefineCommand[None, None]("lease.identity", 1, WithRecoveryLease(time.Second))
	different := DefineCommand[None, None]("lease.identity", 1, WithRecoveryLease(2*time.Second))
	if !equivalentCommandDefaults(base.defaults, same.defaults) || equivalentCommandDefaults(base.defaults, different.defaults) {
		t.Fatal("recovery lease was omitted from command declaration equivalence")
	}
	args, err := encodeDefinitionValue(base.def.Args, None{}, maxCommandArgumentBytes, "command arguments")
	if err != nil {
		t.Fatal(err)
	}
	baseCreate, err := prepareCommand(flowuuid.New(), "child", base.def, base.defaults, args)
	if err != nil {
		t.Fatal(err)
	}
	differentCreate, err := prepareCommand(flowuuid.New(), "child", different.def, different.defaults, args)
	if err != nil {
		t.Fatal(err)
	}
	baseFingerprint, err := commandDeclarationFingerprint(baseCreate)
	if err != nil {
		t.Fatal(err)
	}
	differentFingerprint, err := commandDeclarationFingerprint(differentCreate)
	if err != nil {
		t.Fatal(err)
	}
	if baseFingerprint == differentFingerprint {
		t.Fatal("recovery lease was omitted from command declaration fingerprint")
	}
}

func TestRecoveryLeasePersistsForStagedChild(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	child := DefineCommand[None, None]("lease.persist.child", 1, WithRecoveryLease(750*time.Millisecond))
	runtime, run := stageClaimFixture(t, database, "recovery_lease_persistence", 1, func(work *Work[None]) {
		Enqueue(work, "child", child, None{})
	})
	if runtime.commandLease != 60*time.Second {
		t.Fatalf("default runtime recovery lease = %s, want 60s", runtime.commandLease)
	}
	var recoveryMS *int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT recovery_lease_ms FROM `+
		pgschema.Table(database.Schema, "flow_commands")+` WHERE run_id=$1 AND command_key='child'`, run.ID).
		Scan(&recoveryMS); err != nil {
		t.Fatal(err)
	}
	if recoveryMS == nil || *recoveryMS != 750 {
		t.Fatalf("stored recovery lease = %v", recoveryMS)
	}
	var bodyBytes []byte
	if err := database.DB.Conn.QueryRow(ctx, `SELECT body FROM `+
		pgschema.Table(database.Schema, "flow_journal")+` WHERE run_id=$1 AND entry_kind='command_created' AND command_id=(
			SELECT command_id FROM `+pgschema.Table(database.Schema, "flow_commands")+` WHERE run_id=$1 AND command_key='child')`, run.ID).
		Scan(&bodyBytes); err != nil {
		t.Fatal(err)
	}
	body, err := journalcodec.Decode[journalcodec.CommandCreatedBody](bodyBytes)
	if err != nil || body.RecoveryLeaseMS == nil || *body.RecoveryLeaseMS != 750 {
		t.Fatalf("command-created recovery lease = %#v, err=%v", body.RecoveryLeaseMS, err)
	}
}

func TestRecoveryLeaseChangesPermanentRunIdentity(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(database.DB, WithSchema(database.Schema), WithNotifications(false))
	if err != nil {
		t.Fatal(err)
	}
	first := DefineCommand[None, None]("lease.permanent_identity", 1, WithRecoveryLease(time.Second))
	same := DefineCommand[None, None]("lease.permanent_identity", 1, WithRecoveryLease(time.Second))
	changed := DefineCommand[None, None]("lease.permanent_identity", 1, WithRecoveryLease(2*time.Second))
	created, err := first.Enqueue(ctx, runtime, "same", None{})
	if err != nil || !created.Created {
		t.Fatalf("first Enqueue() = %#v, %v", created, err)
	}
	var rootRecoveryMS *int64
	if err := database.DB.Conn.QueryRow(ctx, `SELECT recovery_lease_ms FROM `+
		pgschema.Table(database.Schema, "flow_commands")+` WHERE run_id=$1 AND command_key='root'`, created.RunID).
		Scan(&rootRecoveryMS); err != nil || rootRecoveryMS == nil || *rootRecoveryMS != 1000 {
		t.Fatalf("root recovery lease = %v, err=%v", rootRecoveryMS, err)
	}
	repeated, err := same.Enqueue(ctx, runtime, "same", None{})
	if err != nil || repeated.Created || repeated.RunID != created.RunID {
		t.Fatalf("same Enqueue() = %#v, %v", repeated, err)
	}
	if _, err := changed.Enqueue(ctx, runtime, "same", None{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed recovery lease error = %v, want ErrConflict", err)
	}
}

func TestMixedRecoveryLeasesClaimAndRenewTogether(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	short := DefineCommand[None, None]("lease.mixed.short", 1, WithRecoveryLease(120*time.Millisecond))
	standard := DefineCommand[None, None]("lease.mixed.standard", 1)
	runtime, run := stageClaimFixture(t, database, "mixed_recovery_leases", 2, func(work *Work[None]) {
		Enqueue(work, "short", short, None{})
		Enqueue(work, "standard", standard, None{})
	})
	candidates := probeClaimCandidates(t, runtime, []store.CommandKind{
		{Name: short.Name(), Version: short.Version()}, {Name: standard.Name(), Version: standard.Version()},
	}, 2)
	claimed, err := runtime.store.ClaimCommands(ctx, candidates, time.Second, "mixed-lease-test", fault.None{})
	if err != nil || len(claimed.Commands) != 2 {
		t.Fatalf("ClaimCommands() commands=%d, err=%v", len(claimed.Commands), err)
	}
	sort.Slice(claimed.Commands, func(i, j int) bool { return claimed.Commands[i].CommandKey < claimed.Commands[j].CommandKey })
	if claimed.Commands[1].CommandKey != "standard" || claimed.Commands[1].LeaseDuration != time.Second ||
		claimed.Commands[0].CommandKey != "short" || claimed.Commands[0].LeaseDuration != 120*time.Millisecond {
		t.Fatalf("mixed claimed leases = %s/%s, %s/%s",
			claimed.Commands[0].CommandKey, claimed.Commands[0].LeaseDuration,
			claimed.Commands[1].CommandKey, claimed.Commands[1].LeaseDuration)
	}
	for _, command := range claimed.Commands {
		if got := command.LeaseExpiresAt.Sub(command.DBNow); got != command.LeaseDuration {
			t.Fatalf("%s durable lease window = %s, want %s", command.CommandKey, got, command.LeaseDuration)
		}
	}
	rows, err := database.DB.Conn.Query(ctx, `SELECT j.body FROM `+
		pgschema.Table(database.Schema, "flow_journal")+` j JOIN `+
		pgschema.Table(database.Schema, "flow_commands")+` c USING (run_id,command_id)
		WHERE j.run_id=$1 AND j.entry_kind='attempt_started' AND c.command_key<>'root' ORDER BY j.position`, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var journalDurations []int64
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		body, err := journalcodec.Decode[journalcodec.AttemptStartedBody](encoded)
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		journalDurations = append(journalDurations, body.LeaseDurationMS)
	}
	rows.Close()
	sort.Slice(journalDurations, func(i, j int) bool { return journalDurations[i] < journalDurations[j] })
	if len(journalDurations) != 2 || journalDurations[0] != 120 || journalDurations[1] != 1000 {
		t.Fatalf("attempt-started lease durations = %v", journalDurations)
	}
	renewals := make([]store.LeaseRenewal, len(claimed.Commands))
	for index, command := range claimed.Commands {
		renewals[index] = store.LeaseRenewal{
			CommandID: command.CommandID, AttemptID: command.AttemptID,
			Token: command.LeaseToken, Duration: command.LeaseDuration,
		}
	}
	results, err := runtime.store.RenewCommandLeases(ctx, renewals)
	if err != nil || len(results) != 2 {
		t.Fatalf("RenewCommandLeases() results=%d, err=%v", len(results), err)
	}
	for index, result := range results {
		if result.Outcome != store.LeaseRenewed || result.LeaseExpiresAt == nil {
			t.Fatalf("renewal[%d] = %#v", index, result)
		}
	}
	if difference := results[1].LeaseExpiresAt.Sub(*results[0].LeaseExpiresAt); difference != 880*time.Millisecond {
		t.Fatalf("mixed renewal expiry difference = %s, want 880ms", difference)
	}
}

func TestShortRecoveryLeaseBecomesRecoverableBeforeDefault(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	short := DefineCommand[None, None]("lease.recovery.short", 1, WithRecoveryLease(90*time.Millisecond))
	standard := DefineCommand[None, None]("lease.recovery.standard", 1)
	runtime, _ := stageClaimFixture(t, database, "recovery_order", 2, func(work *Work[None]) {
		Enqueue(work, "short", short, None{})
		Enqueue(work, "standard", standard, None{})
	})
	candidates := probeClaimCandidates(t, runtime, []store.CommandKind{
		{Name: short.Name(), Version: short.Version()}, {Name: standard.Name(), Version: standard.Version()},
	}, 2)
	claimed, err := runtime.store.ClaimCommands(ctx, candidates, 900*time.Millisecond, "recovery-order", fault.None{})
	if err != nil || len(claimed.Commands) != 2 {
		t.Fatalf("ClaimCommands() commands=%d, err=%v", len(claimed.Commands), err)
	}
	time.Sleep(150 * time.Millisecond)
	expired, err := runtime.store.ProbeExpiredCommandLeases(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(expired) != 1 {
		t.Fatalf("expired leases=%d, want only the short command", len(expired))
	}
	var shortID, standardID flowuuid.UUID
	for _, command := range claimed.Commands {
		switch command.CommandKey {
		case "short":
			shortID = command.CommandID
		case "standard":
			standardID = command.CommandID
		}
	}
	if expired[0].CommandID != shortID {
		t.Fatalf("expired command=%s, want short %s", expired[0].CommandID, shortID)
	}
	if recovery, err := runtime.store.RecoverExpiredCommandLease(ctx, expired[0]); err != nil || !recovery.Changed {
		t.Fatalf("RecoverExpiredCommandLease()=%t, %v", recovery.Changed, err)
	}
	var shortState, standardState string
	if err := database.DB.Conn.QueryRow(ctx, `SELECT
		(SELECT state FROM `+pgschema.Table(database.Schema, "flow_commands")+` WHERE command_id=$1),
		(SELECT state FROM `+pgschema.Table(database.Schema, "flow_commands")+` WHERE command_id=$2)`,
		shortID, standardID).Scan(&shortState, &standardState); err != nil {
		t.Fatal(err)
	}
	if shortState != "retry_wait" || standardState != "running" {
		t.Fatalf("recovery states short=%s standard=%s", shortState, standardState)
	}
}

func TestMalformedDurableRecoveryLeaseRollsBackClaimBatch(t *testing.T) {
	t.Parallel()

	database := testpg.Open(t)
	ctx := context.Background()
	if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	child := DefineCommand[None, None]("lease.malformed.child", 1)
	runtime, run := stageClaimFixture(t, database, "malformed_recovery_lease", 2, func(work *Work[None]) {
		Enqueue(work, "good", child, None{})
		Enqueue(work, "malformed", child, None{})
	})
	if _, err := database.DB.Conn.Exec(ctx, `UPDATE `+pgschema.Table(database.Schema, "flow_commands")+`
		SET recovery_lease_ms=$2 WHERE run_id=$1 AND command_key='malformed'`, run.ID, int64(math.MaxInt64)); err != nil {
		t.Fatal(err)
	}
	candidates := probeClaimCandidates(t, runtime,
		[]store.CommandKind{{Name: child.Name(), Version: child.Version()}}, 2)
	if _, err := runtime.store.ClaimCommands(ctx, candidates, time.Second, "malformed-lease", fault.None{}); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("ClaimCommands() error=%v, want ErrInvalidState", err)
	}
	var readyCommands, readyQueues, childStarts int
	if err := database.DB.Conn.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_commands")+`
		 WHERE run_id=$1 AND parent_command_id=$2 AND state='ready'),
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_command_queue")+`
		 WHERE run_id=$1 AND state='ready'),
		(SELECT count(*) FROM `+pgschema.Table(database.Schema, "flow_journal")+`
		 WHERE run_id=$1 AND entry_kind='attempt_started' AND command_id<>$2)`, run.ID, run.RootCommandID).
		Scan(&readyCommands, &readyQueues, &childStarts); err != nil {
		t.Fatal(err)
	}
	if readyCommands != 2 || readyQueues != 2 || childStarts != 0 {
		t.Fatalf("malformed rollback commands=%d queues=%d child starts=%d",
			readyCommands, readyQueues, childStarts)
	}
}
