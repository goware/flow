package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/goware/flow"
	"github.com/goware/flow/flowtest/replaytest"
	"github.com/goware/flow/internal/testpg"
)

func TestPipelineExampleEndToEnd(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := flow.Migrate(ctx, database.DB, flow.WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	if err := ensureApprovalTable(ctx, database.DB, database.Schema); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	runtime, err := newPipelineRuntime(database.DB, database.Schema, &output)
	if err != nil {
		t.Fatal(err)
	}
	stop := runPipelineRuntime(runtime)
	defer stop()
	order := orderArgs{OrderID: "order-42", Generation: 7}
	publisher := approvalPublisher{db: database.DB, runtime: runtime, schema: database.Schema}
	run, trace, err := runOrder(ctx, runtime, publisher, order)
	if err != nil {
		t.Fatal(err)
	}
	if trace.Run.Status != flow.RunStatusSucceeded || len(trace.Commands) != 7 {
		t.Fatalf("trace status=%s commands=%d", trace.Run.Status, len(trace.Commands))
	}
	result, err := flow.ResultOf(trace, "finalize", finalizeOrder)
	if err != nil || !strings.Contains(result.ReceiptKey, "pay/order-42") {
		t.Fatalf("finalize result=%#v err=%v", result, err)
	}
	if !strings.Contains(output.String(), "order approved by operator@example.com") ||
		!strings.Contains(output.String(), "sent receipt") {
		t.Fatalf("output=%q", output.String())
	}
	var reviewer string
	if err := database.DB.Conn.QueryRow(ctx, `SELECT reviewer FROM `+approvalTable(database.Schema)+`
		WHERE order_id=$1 AND generation=$2`, order.OrderID, order.Generation).Scan(&reviewer); err != nil || reviewer != "operator@example.com" {
		t.Fatalf("approval reviewer=%q err=%v", reviewer, err)
	}
	queues := map[string]string{}
	for _, command := range trace.Commands {
		queues[command.Key] = command.Queue
	}
	if queues["payment"] != "payments" || queues["inventory"] != "inventory" || queues["receipt"] != "email" {
		t.Fatalf("command queues=%#v", queues)
	}
	replaytest.AssertMatchesLive(t, database.DB, database.Schema, run.ID)
}

func TestPipelineEventKeyFencesGenerations(t *testing.T) {
	t.Parallel()
	database := testpg.Open(t)
	ctx := context.Background()
	if err := flow.Migrate(ctx, database.DB, flow.WithSchema(database.Schema)); err != nil {
		t.Fatal(err)
	}
	runtime, err := newPipelineRuntime(database.DB, database.Schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	stop := runPipelineRuntime(runtime)
	defer stop()

	firstArgs := orderArgs{OrderID: "order-fenced", Generation: 1}
	first, err := startOrder.Enqueue(ctx, runtime, firstArgs.OrderID, firstArgs, flow.WithLiveKey())
	if err != nil {
		t.Fatal(err)
	}
	if err := flow.CancelRun(ctx, runtime, first.RunID, "advance generation"); err != nil {
		t.Fatal(err)
	}
	secondArgs := orderArgs{OrderID: firstArgs.OrderID, Generation: 2}
	second, err := startOrder.Enqueue(ctx, runtime, secondArgs.OrderID, secondArgs, flow.WithLiveKey())
	if err != nil {
		t.Fatal(err)
	}
	if err := approvalGranted.Deliver(ctx, runtime, second.RunID, orderFactKey(firstArgs.OrderID, firstArgs.Generation), approval{Reviewer: "stale"}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		trace, err := flow.Trace(ctx, runtime, second.RunID)
		if err != nil {
			t.Fatal(err)
		}
		for _, command := range trace.Commands {
			if command.Key == "approval/resumed" && command.Status == flow.CommandStatusPending {
				goto staleDidNotRelease
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("approval successor did not remain pending on the generation-2 fact")

staleDidNotRelease:
	if err := approvalGranted.Deliver(ctx, runtime, second.RunID, orderFactKey(secondArgs.OrderID, secondArgs.Generation), approval{Reviewer: "current"}); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	settled, err := flow.AwaitRun(waitCtx, runtime, second.RunID)
	if err != nil || settled.Status != flow.RunStatusSucceeded {
		t.Fatalf("generation-2 run=%#v err=%v", settled, err)
	}
	replaytest.AssertMatchesLive(t, database.DB, database.Schema, first.RunID)
	replaytest.AssertMatchesLive(t, database.DB, database.Schema, second.RunID)
}
