package examples

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/goware/flow"
	"github.com/goware/pgkit/v2"
)

type BridgeDelivery struct {
	TransactionHash string `json:"transaction_hash"`
}

type ConfirmBridgeResult struct {
	Confirmed bool `json:"confirmed"`
}

var (
	BridgeDelivered = flow.DefineEvent[BridgeDelivery](BridgeDeliveredName)
	ConfirmBridge   = flow.DefineCommand[flow.None, ConfirmBridgeResult]("example.confirm_bridge", 1)
	BridgePlan      = flow.DefinePlan[flow.None]("example.bridge", 1, func(plan *flow.Plan, _ flow.None) {
		flow.Execute(plan, "confirm", ConfirmBridge, flow.None{}).WaitFor(BridgeDelivered, "delivery/example").Within(2 * time.Second)
	})
)

const BridgeDeliveredName = "example.bridge_delivered"

type MonitorResult struct {
	Handle flow.ExecutionHandle
	Trace  flow.ExecutionTrace
}

// RunMonitor demonstrates a long external wait without a polling worker. The
// command consumes no worker, lease, or connection while a separate monitor
// sleeps and later publishes the durable fact that releases it.
func RunMonitor(ctx context.Context, db *pgkit.DB, schema string, output io.Writer) (MonitorResult, error) {
	output = safeWriter(output)
	runtime, err := flow.New(db, flow.WithSchema(schema), flow.WithWorkerConcurrency(2),
		flow.WithPollInterval(20*time.Millisecond))
	if err != nil {
		return MonitorResult{}, err
	}
	// The monitor has only the lightweight client surface. It deliberately
	// registers neither the plan nor its worker, proving publishers and plan
	// processors can be deployed independently.
	publisher, err := flow.New(db, flow.WithSchema(schema))
	if err != nil {
		return MonitorResult{}, err
	}
	if err := runtime.Register(
		BridgePlan,
		flow.Handle(ConfirmBridge, func(_ context.Context, _ *flow.Work[flow.None]) (ConfirmBridgeResult, error) {
			fmt.Fprintln(output, "bridge delivery confirmed")
			return ConfirmBridgeResult{Confirmed: true}, nil
		}),
	); err != nil {
		return MonitorResult{}, err
	}
	cancel, runResult := startRuntime(runtime)
	defer stopRuntime(cancel, runResult)
	handle, err := BridgePlan.With(runtime).Execute(ctx, "bridge/example", flow.None{})
	if err != nil {
		return MonitorResult{}, err
	}
	monitorResult := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			monitorResult <- ctx.Err()
			return
		case <-time.After(50 * time.Millisecond):
		}
		fmt.Fprintln(output, "external monitor observed bridge delivery")
		monitorResult <- BridgeDelivered.Emit(ctx, publisher, handle.ID, "delivery/example", BridgeDelivery{
			TransactionHash: "0xexample",
		})
	}()
	trace, err := waitForTerminal(ctx, runtime, handle.ID, 8*time.Second)
	if err != nil {
		return MonitorResult{}, err
	}
	if err := <-monitorResult; err != nil {
		return MonitorResult{}, err
	}
	return MonitorResult{Handle: handle, Trace: trace}, nil
}
