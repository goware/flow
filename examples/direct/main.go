package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/goware/flow"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

type receiptArgs struct {
	OrderID string `json:"order_id"`
	Email   string `json:"email"`
}

type receiptSent struct {
	ProviderMessageID string `json:"provider_message_id"`
}

var sendReceipt = flow.DefineCommand[receiptArgs, receiptSent]("example.send_receipt", 1)

type directExample struct {
	output io.Writer
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
	db, err := pgkit.ConnectWithPGX("flow-direct-example", config)
	if err != nil {
		panic(err)
	}
	defer db.Conn.Close()
	if err := flow.Migrate(ctx, db); err != nil {
		panic(err)
	}

	runtime, err := newFlowRuntime(db, "public", os.Stdout)
	if err != nil {
		panic(err)
	}
	stopFlowRuntime := runFlowRuntime(runtime)
	defer stopFlowRuntime()

	run, trace, err := runExampleCommand(ctx, runtime)
	if err != nil {
		panic(err)
	}
	fmt.Printf("run %s completed with %d journal entries\n", run.ID, len(trace.History))
}

func newFlowRuntime(db *pgkit.DB, schema string, output io.Writer) (*flow.Runtime, error) {
	if output == nil {
		output = io.Discard
	}
	example := &directExample{output: output}
	runtime, err := flow.New(db,
		flow.WithSchema(schema),
		flow.WithWorkerConcurrency(2),
		flow.WithPollInterval(20*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	if err := runtime.Register(flow.Handle(sendReceipt, example.sendReceipt)); err != nil {
		return nil, err
	}
	return runtime, nil
}

func runFlowRuntime(runtime *flow.Runtime) func() {
	runContext, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- runtime.Run(runContext) }()
	return func() {
		cancel()
		select {
		case <-result:
		case <-time.After(2 * time.Second):
		}
	}
}

// runExampleCommand submits one command and waits for its terminal trace.
func runExampleCommand(ctx context.Context, runtime *flow.Runtime) (flow.Run, flow.RunTrace, error) {
	run, err := sendReceipt.Enqueue(ctx, runtime, "receipt/example-order", receiptArgs{
		OrderID: "example-order",
		Email:   "person@example.com",
	})
	if err != nil {
		return flow.Run{}, flow.RunTrace{}, err
	}
	trace, err := waitForTerminal(ctx, runtime, run.RunID, 5*time.Second)
	if err != nil {
		return flow.Run{}, flow.RunTrace{}, err
	}
	result, found, err := sendReceipt.GetResult(ctx, runtime, run.RunID, "root")
	if err != nil || !found {
		if err == nil {
			err = fmt.Errorf("root receipt result is absent")
		}
		return flow.Run{}, flow.RunTrace{}, err
	}
	if result.ProviderMessageID == "" {
		return flow.Run{}, flow.RunTrace{}, fmt.Errorf("root receipt result has no provider message ID")
	}
	return trace.Run, trace, nil
}

// sendReceipt is the worker handler
func (example *directExample) sendReceipt(ctx context.Context, work *flow.Work[receiptArgs]) (receiptSent, error) {
	fmt.Fprintf(example.output, "sending receipt for %s to %s (command %s)\n",
		work.Args.OrderID, work.Args.Email, work.Info().CommandID)
	select {
	case <-ctx.Done():
		return receiptSent{}, ctx.Err()
	case <-time.After(25 * time.Millisecond):
	}
	result := receiptSent{ProviderMessageID: "stub-" + work.Args.OrderID}
	fmt.Fprintf(example.output, "receipt sent: %s\n", result.ProviderMessageID)
	return result, nil
}

func waitForTerminal(ctx context.Context, runtime *flow.Runtime, id flow.RunID, timeout time.Duration) (flow.RunTrace, error) {
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	settled, err := flow.AwaitRun(waitCtx, runtime, id)
	if err != nil {
		return flow.RunTrace{}, err
	}
	if settled.Status != flow.RunStatusSucceeded {
		return flow.RunTrace{}, fmt.Errorf("example run ended %s", settled.Status)
	}
	return flow.Trace(waitCtx, runtime, id)
}
