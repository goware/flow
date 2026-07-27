package examples

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/goware/flow"
	"github.com/goware/pgkit/v2"
)

type ReceiptArgs struct {
	OrderID string `json:"order_id"`
	Email   string `json:"email"`
}

type ReceiptSent struct {
	ProviderMessageID string `json:"provider_message_id"`
}

var SendReceipt = flow.DefineCommand[ReceiptArgs, ReceiptSent]("example.send_receipt", 1)

type DirectResult struct {
	Handle flow.ExecutionHandle
	Trace  flow.ExecutionTrace
}

// RunDirect runs the executable direct-command example. Stub application work
// prints and sleeps, while every Flow operation uses the real PostgreSQL store.
func RunDirect(ctx context.Context, db *pgkit.DB, schema string, output io.Writer) (DirectResult, error) {
	output = safeWriter(output)
	runtime, err := flow.New(db, flow.WithSchema(schema), flow.WithWorkerConcurrency(2),
		flow.WithPollInterval(20*time.Millisecond))
	if err != nil {
		return DirectResult{}, err
	}
	worker := func(ctx context.Context, work *flow.Work[ReceiptArgs]) (ReceiptSent, error) {
		fmt.Fprintf(output, "sending receipt for %s to %s (command %s)\n",
			work.Args.OrderID, work.Args.Email, work.Info().CommandID)
		select {
		case <-ctx.Done():
			return ReceiptSent{}, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
		result := ReceiptSent{ProviderMessageID: "stub-" + work.Args.OrderID}
		fmt.Fprintf(output, "receipt sent: %s\n", result.ProviderMessageID)
		return result, nil
	}
	if err := runtime.Register(flow.Handle(SendReceipt, worker)); err != nil {
		return DirectResult{}, err
	}
	cancelRun, runResult := startRuntime(runtime)
	defer stopRuntime(cancelRun, runResult)

	handle, err := SendReceipt.With(runtime).Execute(ctx, "receipt/example-order", ReceiptArgs{
		OrderID: "example-order", Email: "person@example.com",
	})
	if err != nil {
		return DirectResult{}, err
	}
	trace, err := waitForTerminal(ctx, runtime, handle.ID, 5*time.Second)
	if err != nil {
		return DirectResult{}, err
	}
	return DirectResult{Handle: handle, Trace: trace}, nil
}
