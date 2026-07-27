package examples

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/goware/flow"
	"github.com/goware/pgkit/v2"
)

type AgentState struct {
	Turn         int                           `json:"turn"`
	PendingTools map[string]bool               `json:"pending_tools"`
	Outcomes     map[string]flow.CommandStatus `json:"outcomes"`
	UserMessage  string                        `json:"user_message"`
}
type ThinkArgs struct {
	Turn        int    `json:"turn"`
	UserMessage string `json:"user_message"`
}
type ThinkResult struct {
	FinalResultRef string   `json:"final_result_ref"`
	Tools          []string `json:"tools"`
}
type ToolArgs struct {
	Name string `json:"name"`
}
type ToolResult struct {
	OutputRef string `json:"output_ref"`
}
type AgentMessage struct {
	Text string `json:"text"`
}

var (
	AgentThink       = flow.DefineCommand[ThinkArgs, ThinkResult]("example.agent_think", 1)
	AgentTool        = flow.DefineCommand[ToolArgs, ToolResult]("example.agent_tool", 1, flow.WithMaxAttempts(1))
	AgentUserMessage = flow.DefineEvent[AgentMessage]("example.agent_user_message", 1)
	ResearchAgent    = flow.DefineCoordinator[AgentState]("example.research_agent", 1,
		flow.OnStart(startResearchAgent),
		flow.OnOutcome(AgentThink, handleAgentThought),
		flow.OnOutcome(AgentTool, handleAgentTool),
		flow.On(AgentUserMessage, handleAgentMessage),
	)
)

func startResearchAgent(_ context.Context, c *flow.Coordination[AgentState]) error {
	c.State.Turn = 1
	c.State.PendingTools = map[string]bool{}
	c.State.Outcomes = map[string]flow.CommandStatus{}
	return flow.Spawn(c, "turn/1/think", AgentThink, ThinkArgs{Turn: 1}, flow.Optional())
}

func handleAgentMessage(_ context.Context, c *flow.Coordination[AgentState], received flow.Received[AgentMessage]) error {
	c.State.UserMessage = received.Payload.Text
	return nil
}

func handleAgentThought(_ context.Context, c *flow.Coordination[AgentState], received flow.Received[flow.CommandOutcome[ThinkResult]]) error {
	if received.Payload.Status != flow.StatusSucceeded {
		return flow.FailExecution(c, errors.New("model command failed"))
	}
	if received.Payload.Result.FinalResultRef != "" {
		return flow.SucceedExecution(c, received.Payload.Result.FinalResultRef)
	}
	if len(received.Payload.Result.Tools) == 0 {
		return flow.FailExecution(c, errors.New("model returned no action"))
	}
	for _, name := range received.Payload.Result.Tools {
		key := "turn/1/tool/" + name
		if err := flow.Spawn(c, key, AgentTool, ToolArgs{Name: name}, flow.Optional()); err != nil {
			return err
		}
		c.State.PendingTools[key] = true
	}
	return nil
}

func handleAgentTool(_ context.Context, c *flow.Coordination[AgentState], received flow.Received[flow.CommandOutcome[ToolResult]]) error {
	if !c.State.PendingTools[received.Key] {
		return flow.Permanent(fmt.Errorf("unexpected tool outcome %q", received.Key))
	}
	delete(c.State.PendingTools, received.Key)
	c.State.Outcomes[received.Key] = received.Payload.Status
	if len(c.State.PendingTools) > 0 {
		return nil
	}
	c.State.Turn = 2
	return flow.Spawn(c, "turn/2/think", AgentThink, ThinkArgs{Turn: 2, UserMessage: c.State.UserMessage}, flow.Optional(), flow.StartAfter(20*time.Millisecond))
}

type AgentResult struct {
	Handle flow.ExecutionHandle
	Trace  flow.ExecutionTrace
}

func RunAgent(ctx context.Context, db *pgkit.DB, schema string, output io.Writer) (AgentResult, error) {
	output = safeWriter(output)
	runtime, err := flow.New(db, flow.WithSchema(schema), flow.WithWorkerConcurrency(4), flow.WithCoordinatorConcurrency(2), flow.WithPollInterval(10*time.Millisecond))
	if err != nil {
		return AgentResult{}, err
	}
	if err = runtime.Register(ResearchAgent,
		flow.Handle(AgentThink, func(ctx context.Context, w *flow.Work[ThinkArgs]) (ThinkResult, error) {
			fmt.Fprintf(output, "agent thinking on turn %d\n", w.Args.Turn)
			select {
			case <-ctx.Done():
				return ThinkResult{}, ctx.Err()
			case <-time.After(15 * time.Millisecond):
			}
			if w.Args.Turn == 1 {
				return ThinkResult{Tools: []string{"search", "broken"}}, nil
			}
			return ThinkResult{FinalResultRef: "result/final-report"}, nil
		}),
		flow.Handle(AgentTool, func(ctx context.Context, w *flow.Work[ToolArgs]) (ToolResult, error) {
			fmt.Fprintf(output, "running tool %s\n", w.Args.Name)
			select {
			case <-ctx.Done():
				return ToolResult{}, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			if w.Args.Name == "broken" {
				return ToolResult{}, flow.Permanent(errors.New("controlled tool failure"))
			}
			return ToolResult{OutputRef: "result/" + w.Args.Name}, nil
		}),
	); err != nil {
		return AgentResult{}, err
	}
	cancel, result := startRuntime(runtime)
	defer stopRuntime(cancel, result)
	handle, err := ResearchAgent.With(runtime).Execute(ctx, "agent/example", AgentState{})
	if err != nil {
		return AgentResult{}, err
	}
	if err = flow.Publish(ctx, runtime, handle.ID, AgentUserMessage, "message/1", AgentMessage{Text: "focus on durability"}); err != nil {
		return AgentResult{}, err
	}
	trace, err := waitForTerminal(ctx, runtime, handle.ID, 8*time.Second)
	if err != nil {
		return AgentResult{}, err
	}
	return AgentResult{Handle: handle, Trace: trace}, nil
}
