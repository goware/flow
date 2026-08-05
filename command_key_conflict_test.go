package flow

import (
	"context"
	"testing"
	"time"

	"github.com/goware/flow/internal/testpg"
)

type crossDecisionArgs struct {
	Value string `json:"value"`
}

func TestCrossDecisionCommandKeyReuseIsAConflict(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		values [2]string
	}{
		{name: "equivalent", values: [2]string{"same", "same"}},
		{name: "different", values: [2]string{"first", "second"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := testpg.Open(t)
			ctx := context.Background()
			if err := Migrate(ctx, database.DB, WithSchema(database.Schema)); err != nil {
				t.Fatal(err)
			}
			root := DefineCommand[None, None]("key_conflict.root."+test.name, 1)
			declarer := DefineCommand[crossDecisionArgs, None]("key_conflict.declarer."+test.name, 1)
			child := DefineCommand[crossDecisionArgs, None]("key_conflict.child."+test.name, 1)
			runtime, err := New(database.DB, WithSchema(database.Schema), WithWorkerConcurrency(1),
				WithNotifications(false), WithPollInterval(5*time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.Register(
				Handle(root, func(_ context.Context, work *Work[None]) (None, error) {
					Execute(work, "declarer/1", declarer, crossDecisionArgs{Value: test.values[0]}).Optional()
					Execute(work, "declarer/2", declarer, crossDecisionArgs{Value: test.values[1]}).Optional()
					return None{}, nil
				}),
				Handle(declarer, func(_ context.Context, work *Work[crossDecisionArgs]) (None, error) {
					Execute(work, "shared", child, work.Args)
					return None{}, nil
				}),
				Handle(child, func(context.Context, *Work[crossDecisionArgs]) (None, error) {
					return None{}, nil
				}),
			); err != nil {
				t.Fatal(err)
			}
			cancel, runResult := startRuntime(t, runtime)
			defer stopRuntime(t, cancel, runResult)
			handle, err := root.With(runtime).Execute(ctx, "key-conflict/"+test.name, None{})
			if err != nil {
				t.Fatal(err)
			}
			waitForExecutionStatus(t, database.Schema, database.DB.Conn, handle.ID, "succeeded", 5*time.Second)
			trace, err := Trace(ctx, runtime, handle.ID)
			if err != nil {
				t.Fatal(err)
			}
			var succeeded, conflicted int
			for _, command := range trace.Commands {
				if command.Name != declarer.Name() {
					continue
				}
				switch {
				case command.State == string(StatusSucceeded):
					succeeded++
				case command.State == string(StatusFailed) && command.FailureCode == "invalid_decision":
					conflicted++
				}
			}
			if len(trace.Commands) != 4 || succeeded != 1 || conflicted != 1 {
				t.Fatalf("cross-decision trace=%+v", trace.Commands)
			}
		})
	}
}
