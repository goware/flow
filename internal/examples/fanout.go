package examples

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/goware/flow"
	"github.com/goware/pgkit/v2"
)

type ReportArgs struct {
	Parts int `json:"parts"`
}

type PrepareReportArgs struct {
	Parts int `json:"parts"`
}

type PrepareReportResult struct {
	Count int `json:"count"`
}

type AnalyzePartArgs struct {
	Part int `json:"part"`
}

type AnalyzePartResult struct {
	Score int `json:"score"`
}

type GenerateReportArgs struct {
	AnalysisKeys []string `json:"analysis_keys"`
}

type GenerateReportResult struct {
	Total int `json:"total"`
}

var (
	AnalyzePart    = flow.DefineCommand[AnalyzePartArgs, AnalyzePartResult]("example.analyze_part", 1)
	PrepareReport  = flow.DefineCommand[PrepareReportArgs, PrepareReportResult]("example.prepare_report", 1)
	GenerateReport = flow.DefineCommand[GenerateReportArgs, GenerateReportResult]("example.generate_report", 1)
	ReportPlan     = flow.DefinePlan[ReportArgs]("example.report", 1, func(plan *flow.Plan, args ReportArgs) {
		prepare := flow.Execute(plan, "prepare", PrepareReport, PrepareReportArgs{Parts: args.Parts})
		children, closed := prepare.Children()
		if !closed {
			return
		}
		flow.Execute(plan, "generate", GenerateReport, GenerateReportArgs{AnalysisKeys: children}).After(children...)
	})
)

type FanOutResult struct {
	Handle flow.ExecutionHandle
	Trace  flow.ExecutionTrace
}

// RunFanOut runs a dynamic plan: prepare spawns independent analyses, the plan
// joins the authoritative child set, and a final worker reads dependency
// results from durable command state.
func RunFanOut(ctx context.Context, db *pgkit.DB, schema string, output io.Writer) (FanOutResult, error) {
	output = safeWriter(output)
	runtime, err := flow.New(db, flow.WithSchema(schema), flow.WithWorkerConcurrency(8),
		flow.WithPollInterval(20*time.Millisecond), flow.WithPlanVerification(true))
	if err != nil {
		return FanOutResult{}, err
	}
	if err := runtime.Register(
		ReportPlan,
		flow.Handle(PrepareReport, func(ctx context.Context, work *flow.Work[PrepareReportArgs]) (PrepareReportResult, error) {
			fmt.Fprintf(output, "planning %d report analyses\n", work.Args.Parts)
			for part := range work.Args.Parts {
				key := fmt.Sprintf("analysis/%d", part)
				flow.Execute(work, key, AnalyzePart, AnalyzePartArgs{Part: part})
			}
			return PrepareReportResult{Count: work.Args.Parts}, nil
		}),
		flow.Handle(AnalyzePart, func(ctx context.Context, work *flow.Work[AnalyzePartArgs]) (AnalyzePartResult, error) {
			fmt.Fprintf(output, "analyzing part %d\n", work.Args.Part)
			select {
			case <-ctx.Done():
				return AnalyzePartResult{}, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
			return AnalyzePartResult{Score: work.Args.Part + 1}, nil
		}),
		flow.Handle(GenerateReport, func(_ context.Context, work *flow.Work[GenerateReportArgs]) (GenerateReportResult, error) {
			total := 0
			for _, key := range work.Args.AnalysisKeys {
				analysis, err := flow.ResultOf(work, key, AnalyzePart)
				if err != nil {
					return GenerateReportResult{}, err
				}
				total += analysis.Score
			}
			fmt.Fprintf(output, "generated report with score %d\n", total)
			return GenerateReportResult{Total: total}, nil
		}),
	); err != nil {
		return FanOutResult{}, err
	}
	cancel, runResult := startRuntime(runtime)
	defer stopRuntime(cancel, runResult)
	handle, err := ReportPlan.With(runtime).Execute(ctx, "report/example", ReportArgs{Parts: 3})
	if err != nil {
		return FanOutResult{}, err
	}
	trace, err := waitForTerminal(ctx, runtime, handle.ID, 8*time.Second)
	if err != nil {
		return FanOutResult{}, err
	}
	return FanOutResult{Handle: handle, Trace: trace}, nil
}
