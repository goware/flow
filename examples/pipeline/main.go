// Command pipeline demonstrates when to split work into durable commands:
// independent queues, retryable side effects, an external wait, an all-of
// join, and a dynamic successor. It deliberately uses no workflow DSL.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/goware/flow"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type orderArgs struct {
	OrderID    string `json:"order_id"`
	Generation int    `json:"generation"`
}

type paymentResult struct {
	Authorization string `json:"authorization"`
}

type inventoryResult struct {
	Reservation string `json:"reservation"`
}

type approval struct {
	Reviewer string `json:"reviewer"`
}

type finalizeArgs struct {
	Order    orderArgs `json:"order"`
	Reviewer string    `json:"reviewer"`
}

type finalizedOrder struct {
	ReceiptKey string `json:"receipt_key"`
}

var (
	startOrder       = flow.DefineCommand[orderArgs, flow.None]("example.pipeline.start_order", 1)
	capturePayment   = flow.DefineCommand[orderArgs, paymentResult]("example.pipeline.capture_payment", 1, flow.WithQueue("payments"))
	reserveInventory = flow.DefineCommand[orderArgs, inventoryResult]("example.pipeline.reserve_inventory", 1, flow.WithQueue("inventory"))
	routeApproval    = flow.DefineCommand[orderArgs, flow.None]("example.pipeline.route_approval", 1)
	finalizeOrder    = flow.DefineCommand[finalizeArgs, finalizedOrder]("example.pipeline.finalize_order", 1)
	sendReceipt      = flow.DefineCommand[finalizedOrder, flow.None]("example.pipeline.send_receipt", 1, flow.WithQueue("email"))

	paymentCaptured   = flow.DefineEvent[paymentResult]("example.pipeline.payment_captured")
	inventoryReserved = flow.DefineEvent[inventoryResult]("example.pipeline.inventory_reserved")
	approvalGranted   = flow.DefineEvent[approval]("example.pipeline.approval_granted")
)

type pipeline struct {
	output io.Writer
}

type approvalPublisher struct {
	db      *pgkit.DB
	runtime *flow.Runtime
	schema  string
}

func main() {
	ctx := context.Background()
	databaseURL := os.Getenv("FLOW_EXAMPLE_DATABASE_URL")
	if databaseURL == "" {
		fmt.Fprintln(os.Stderr, "FLOW_EXAMPLE_DATABASE_URL is required")
		os.Exit(2)
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		panic(err)
	}
	db, err := pgkit.ConnectWithPGX("flow-pipeline-example", config)
	if err != nil {
		panic(err)
	}
	defer db.Conn.Close()
	if err := flow.Migrate(ctx, db); err != nil {
		panic(err)
	}
	if err := ensureApprovalTable(ctx, db, "public"); err != nil {
		panic(err)
	}
	runtime, err := newPipelineRuntime(db, "public", os.Stdout)
	if err != nil {
		panic(err)
	}
	stop := runPipelineRuntime(runtime)
	defer stop()

	publisher := approvalPublisher{db: db, runtime: runtime, schema: "public"}
	run, trace, err := runOrder(ctx, runtime, publisher, orderArgs{OrderID: "order-42", Generation: 1})
	if err != nil {
		panic(err)
	}
	fmt.Printf("order run %s completed with %d commands\n", run.ID, len(trace.Commands))
}

func newPipelineRuntime(db *pgkit.DB, schema string, output io.Writer) (*flow.Runtime, error) {
	engine := &pipeline{output: synchronized(output)}
	runtime, err := flow.New(db, flow.WithSchema(schema), flow.WithWorkerConcurrency(4), flow.WithPollInterval(10*time.Millisecond))
	if err != nil {
		return nil, err
	}
	if err := runtime.Register(
		flow.Handle(startOrder, engine.start),
		flow.Handle(capturePayment, engine.capturePayment),
		flow.Handle(reserveInventory, engine.reserveInventory),
		flow.Handle(routeApproval, engine.routeApproval),
		flow.Handle(finalizeOrder, engine.finalize),
		flow.Handle(sendReceipt, engine.sendReceipt),
	); err != nil {
		return nil, err
	}
	return runtime, nil
}

func (engine *pipeline) start(_ context.Context, work *flow.Work[orderArgs]) (flow.None, error) {
	// Payment and inventory are separate commands because they use independent
	// queues and retryable external side effects.
	flow.Enqueue(work, "payment", capturePayment, work.Args)
	flow.Enqueue(work, "inventory", reserveInventory, work.Args)

	// The first approval command requests external work. Its durable successor
	// resumes only after the exact generation-scoped approval exists.
	flow.Enqueue(work, "approval", routeApproval, work.Args)
	return flow.None{}, nil
}

func (engine *pipeline) capturePayment(_ context.Context, work *flow.Work[orderArgs]) (paymentResult, error) {
	result := paymentResult{Authorization: "pay/" + work.Args.OrderID}
	err := flow.Emit(work, paymentCaptured, orderFactKey(work.Args.OrderID, work.Args.Generation), result)
	return result, err
}

func (engine *pipeline) reserveInventory(_ context.Context, work *flow.Work[orderArgs]) (inventoryResult, error) {
	result := inventoryResult{Reservation: "stock/" + work.Args.OrderID}
	err := flow.Emit(work, inventoryReserved, orderFactKey(work.Args.OrderID, work.Args.Generation), result)
	return result, err
}

func (engine *pipeline) routeApproval(_ context.Context, work *flow.Work[orderArgs]) (flow.None, error) {
	key := orderFactKey(work.Args.OrderID, work.Args.Generation)
	approved, found, err := flow.GetEventValue(work, approvalGranted, key)
	if err != nil {
		return flow.None{}, err
	}
	if !found {
		// A selector absent from this immutable attempt snapshot is ordinary on
		// the first pass. The successor declares the durable rendezvous; retained
		// delivery also covers an approval that raced ahead of this declaration.
		flow.Enqueue(work, "approval/resumed", routeApproval, work.Args).
			WaitFor(approvalGranted, key).
			Within(10 * time.Minute)
		fmt.Fprintf(engine.output, "requested approval for %s\n", work.Args.OrderID)
		return flow.None{}, nil
	}

	// The join is created only after approval and consumes no worker or
	// connection until both independently produced facts exist.
	flow.Enqueue(work, "finalize", finalizeOrder, finalizeArgs{Order: work.Args, Reviewer: approved.Reviewer}).
		WaitFor(paymentCaptured, key).
		WaitFor(inventoryReserved, key).
		Within(10 * time.Minute)
	return flow.None{}, nil
}

func (engine *pipeline) finalize(_ context.Context, work *flow.Work[finalizeArgs]) (finalizedOrder, error) {
	key := orderFactKey(work.Args.Order.OrderID, work.Args.Order.Generation)
	payment, paymentFound, err := flow.GetEventValue(work, paymentCaptured, key)
	if err != nil {
		return finalizedOrder{}, err
	}
	inventory, inventoryFound, err := flow.GetEventValue(work, inventoryReserved, key)
	if err != nil {
		return finalizedOrder{}, err
	}
	if !paymentFound || !inventoryFound {
		return finalizedOrder{}, fmt.Errorf("required order facts are absent")
	}
	result := finalizedOrder{ReceiptKey: payment.Authorization + ":" + inventory.Reservation}
	fmt.Fprintf(engine.output, "order approved by %s; receipt %s\n", work.Args.Reviewer, result.ReceiptKey)

	// The receipt is dynamic work derived from accepted results. It is a command
	// because email has its own queue and retry boundary.
	flow.Enqueue(work, "receipt", sendReceipt, result)
	return result, nil
}

func (engine *pipeline) sendReceipt(_ context.Context, work *flow.Work[finalizedOrder]) (flow.None, error) {
	fmt.Fprintf(engine.output, "sent receipt %s\n", work.Args.ReceiptKey)
	return flow.None{}, nil
}

// approve demonstrates the explicit current-run race. GetCurrentRun resolves
// the current generation by domain key, but that run can settle before Deliver;
// callers must handle flow.ErrTerminal and retry/reconcile at the application
// boundary. The Flow write happens before application writes in the same short
// transaction.
func (publisher approvalPublisher) approve(ctx context.Context, order orderArgs, reviewer string) error {
	tx, err := publisher.db.Conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	flowTx := publisher.runtime.InTx(tx)
	current, found, err := startOrder.GetCurrentRun(ctx, flowTx, order.OrderID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("order %q has no current run", order.OrderID)
	}
	if err := approvalGranted.Deliver(ctx, flowTx, current.ID, orderFactKey(order.OrderID, order.Generation), approval{Reviewer: reviewer}); err != nil {
		return err
	}
	if err := flowTx.BeginApplicationWrites(); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO `+approvalTable(publisher.schema)+`
		(order_id,generation,reviewer) VALUES ($1,$2,$3)
		ON CONFLICT (order_id,generation) DO UPDATE SET reviewer=EXCLUDED.reviewer`,
		order.OrderID, order.Generation, reviewer)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func runOrder(ctx context.Context, runtime *flow.Runtime, publisher approvalPublisher, order orderArgs) (flow.Run, flow.RunTrace, error) {
	run, err := startOrder.Enqueue(ctx, runtime, order.OrderID, order, flow.WithLiveKey())
	if err != nil {
		return flow.Run{}, flow.RunTrace{}, err
	}
	if err := publisher.approve(ctx, order, "operator@example.com"); err != nil {
		return flow.Run{}, flow.RunTrace{}, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	settled, err := flow.AwaitRun(waitCtx, runtime, run.RunID)
	if err != nil {
		return flow.Run{}, flow.RunTrace{}, err
	}
	if settled.Status != flow.RunStatusSucceeded {
		return flow.Run{}, flow.RunTrace{}, fmt.Errorf("order run ended %s", settled.Status)
	}
	trace, err := flow.Trace(ctx, runtime, run.RunID)
	return settled, trace, err
}

func orderFactKey(orderID string, generation int) string {
	return fmt.Sprintf("order:%s:generation:%d", orderID, generation)
}

func ensureApprovalTable(ctx context.Context, db *pgkit.DB, schema string) error {
	_, err := db.Conn.Exec(ctx, `CREATE TABLE `+approvalTable(schema)+` (
		order_id text NOT NULL,
		generation integer NOT NULL,
		reviewer text NOT NULL,
		PRIMARY KEY (order_id,generation)
	)`)
	return err
}

func approvalTable(schema string) string {
	return pgx.Identifier{schema, "pipeline_approvals"}.Sanitize()
}

func runPipelineRuntime(runtime *flow.Runtime) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}

type synchronizedWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (writer *synchronizedWriter) Write(value []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.writer.Write(value)
}

func synchronized(writer io.Writer) io.Writer {
	if writer == nil {
		writer = io.Discard
	}
	return &synchronizedWriter{writer: writer}
}
