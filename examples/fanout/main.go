package main

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/goware/flow"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5/pgxpool"
)

type prepareReportArgs struct {
	Parts []int `json:"parts"`
}

type prepareReportResult struct {
	Parts int `json:"parts"`
}

type analyzePartArgs struct {
	Part int `json:"part"`
}

type analyzedPart struct {
	Part  int `json:"part"`
	Score int `json:"score"`
}

type joinAnalysisArgs struct {
	Parts []int `json:"parts"`
}

type joinAnalysisResult struct {
	Parts int `json:"parts"`
}

type enrichPartArgs struct {
	Part  int `json:"part"`
	Score int `json:"score"`
}

type enrichedPart struct {
	Part  int `json:"part"`
	Score int `json:"score"`
}

type joinEnrichmentArgs struct {
	Parts []int `json:"parts"`
}

type joinEnrichmentResult struct {
	Total int `json:"total"`
}

type generateReportArgs struct {
	Total int `json:"total"`
}

type generateReportResult struct {
	Total int `json:"total"`
}

var (
	prepareReport  = flow.DefineCommand[prepareReportArgs, prepareReportResult]("example.prepare_report", 1)
	analyzePart    = flow.DefineCommand[analyzePartArgs, flow.None]("example.analyze_part", 1)
	joinAnalysis   = flow.DefineCommand[joinAnalysisArgs, joinAnalysisResult]("example.join_analysis", 1)
	enrichPart     = flow.DefineCommand[enrichPartArgs, flow.None]("example.enrich_part", 1)
	joinEnrichment = flow.DefineCommand[joinEnrichmentArgs, joinEnrichmentResult]("example.join_enrichment", 1)
	generateReport = flow.DefineCommand[generateReportArgs, generateReportResult]("example.generate_report", 1)
	partAnalyzed   = flow.DefineEvent[analyzedPart]("example.part_analyzed")
	partEnriched   = flow.DefineEvent[enrichedPart]("example.part_enriched")
)

type fanoutExample struct {
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
	db, err := pgkit.ConnectWithPGX("flow-fanout-example", config)
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
	fmt.Printf("run %s completed with %d commands and %d journal entries\n",
		run.ID, len(trace.Commands), len(trace.History))
}

func newFlowRuntime(db *pgkit.DB, schema string, output io.Writer) (*flow.Runtime, error) {
	example := &fanoutExample{output: synchronized(output)}
	runtime, err := flow.New(db,
		flow.WithSchema(schema),
		flow.WithWorkerConcurrency(8),
		flow.WithPollInterval(20*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	if err := runtime.Register(
		flow.Handle(prepareReport, example.prepareReport),
		flow.Handle(analyzePart, example.analyzePart),
		flow.Handle(joinAnalysis, example.joinAnalysis),
		flow.Handle(enrichPart, example.enrichPart),
		flow.Handle(joinEnrichment, example.joinEnrichment),
		flow.Handle(generateReport, example.generateReport),
	); err != nil {
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

// runExampleCommand executes two command-owned fan-out/join phases.
func runExampleCommand(ctx context.Context, runtime *flow.Runtime) (flow.Run, flow.RunTrace, error) {
	run, err := prepareReport.Enqueue(ctx, runtime, "report/example", prepareReportArgs{Parts: []int{0, 1, 2}})
	if err != nil {
		return flow.Run{}, flow.RunTrace{}, err
	}
	trace, err := waitForTerminal(ctx, runtime, run.RunID, 8*time.Second)
	return trace.Run, trace, err
}

// prepareReport discovers the stable part list and atomically stages every
// analysis plus their exact all-of join.
func (example *fanoutExample) prepareReport(_ context.Context, work *flow.Work[prepareReportArgs]) (prepareReportResult, error) {
	fmt.Fprintf(example.output, "preparing %d report analyses\n", len(work.Args.Parts))
	seen := make(map[int]struct{}, len(work.Args.Parts))
	join := flow.Enqueue(work, "analysis/join", joinAnalysis, joinAnalysisArgs{Parts: work.Args.Parts})
	for _, part := range work.Args.Parts {
		if _, duplicate := seen[part]; duplicate {
			return prepareReportResult{}, flow.Permanent(fmt.Errorf("duplicate report part %d", part))
		}
		seen[part] = struct{}{}
		key := fmt.Sprintf("analysis/%d", part)
		flow.Enqueue(work, key, analyzePart, analyzePartArgs{Part: part})
		join.WaitFor(partAnalyzed, key)
	}
	return prepareReportResult{Parts: len(work.Args.Parts)}, nil
}

func (example *fanoutExample) analyzePart(ctx context.Context, work *flow.Work[analyzePartArgs]) (flow.None, error) {
	delay := time.Duration(rand.IntN(4)+1) * time.Second // simulating random amount of work
	fmt.Fprintf(example.output, "analyzing part %d for %s\n", work.Args.Part, delay)
	select {
	case <-ctx.Done():
		return flow.None{}, ctx.Err()
	case <-time.After(delay):
	}
	key := fmt.Sprintf("analysis/%d", work.Args.Part)
	return flow.None{}, flow.Emit(work, partAnalyzed, key, analyzedPart{Part: work.Args.Part, Score: work.Args.Part + 1})
}

// joinAnalysis consumes only its declared event inputs, then stages the
// second fan-out and its next all-of join.
func (example *fanoutExample) joinAnalysis(_ context.Context, work *flow.Work[joinAnalysisArgs]) (joinAnalysisResult, error) {
	join := flow.Enqueue(work, "enrichment/join", joinEnrichment, joinEnrichmentArgs{Parts: work.Args.Parts})
	for _, part := range work.Args.Parts {
		analysisKey := fmt.Sprintf("analysis/%d", part)
		analyzed, found, err := flow.GetEventValue(work, partAnalyzed, analysisKey)
		if err != nil || !found {
			if err == nil {
				err = fmt.Errorf("required analysis %q is absent", analysisKey)
			}
			return joinAnalysisResult{}, err
		}
		enrichmentKey := fmt.Sprintf("enrichment/%d", part)
		flow.Enqueue(work, enrichmentKey, enrichPart, enrichPartArgs{Part: analyzed.Part, Score: analyzed.Score})
		join.WaitFor(partEnriched, enrichmentKey)
	}
	return joinAnalysisResult{Parts: len(work.Args.Parts)}, nil
}

func (example *fanoutExample) enrichPart(_ context.Context, work *flow.Work[enrichPartArgs]) (flow.None, error) {
	fmt.Fprintf(example.output, "enriching part %d\n", work.Args.Part)
	key := fmt.Sprintf("enrichment/%d", work.Args.Part)
	return flow.None{}, flow.Emit(work, partEnriched, key, enrichedPart{Part: work.Args.Part, Score: work.Args.Score})
}

func (example *fanoutExample) joinEnrichment(_ context.Context, work *flow.Work[joinEnrichmentArgs]) (joinEnrichmentResult, error) {
	total := 0
	for _, part := range work.Args.Parts {
		key := fmt.Sprintf("enrichment/%d", part)
		value, found, err := flow.GetEventValue(work, partEnriched, key)
		if err != nil || !found {
			if err == nil {
				err = fmt.Errorf("required enrichment %q is absent", key)
			}
			return joinEnrichmentResult{}, err
		}
		total += value.Score
	}
	flow.Enqueue(work, "generate", generateReport, generateReportArgs{Total: total})
	return joinEnrichmentResult{Total: total}, nil
}

func (example *fanoutExample) generateReport(_ context.Context, work *flow.Work[generateReportArgs]) (generateReportResult, error) {
	fmt.Fprintf(example.output, "generated report with score %d\n", work.Args.Total)
	return generateReportResult{Total: work.Args.Total}, nil
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
