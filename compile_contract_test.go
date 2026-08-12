package flow

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestRemovedPublicAPINamesStayRemoved(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate flow package")
	}
	denied := map[string]struct{}{
		"Plan": {}, "PlanDef": {}, "DefinePlan": {}, "Fact": {}, "Facts": {},
		"Action": {}, "DefineAction": {}, "HandleAction": {},
		"Task": {}, "Step": {}, "Checkpoint": {}, "Workflow": {},
		"Coordinator": {}, "Coordination": {}, "Handler": {}, "DefineCoordinator": {},
		"OnStart": {}, "OnEvent": {}, "OnOutcome": {}, "Received": {}, "CoordinatorID": {},
		"Outcome": {}, "OutcomeOf": {}, "ResultSource": {}, "Scope": {}, "WithCoordinatorConcurrency": {},
		"ReadEvent": {}, "LookupEventValue": {}, "DeliverToLive": {},
		"RunHandle": {}, "RunReceipt": {}, "Execute": {}, "Call": {},
		"Execution": {}, "ExecutionID": {}, "ExecutionStatus": {}, "ExecutionOption": {},
		"ExecutionFilter": {}, "ExecutionPage": {}, "ExecutionTrace": {},
		"GetExecution": {}, "AwaitExecution": {}, "ListExecutions": {}, "CancelExecution": {},
		"LookupLiveExecution": {}, "WithExecutionDeadline": {}, "WithoutExecutionDeadline": {},
		"WithMaxCommandsPerExecution": {}, "ObservationExecution": {},
		"HistoryExecutionStarted": {}, "HistoryExecutionFailing": {}, "BoundCommand": {},
		"Node": {}, "LiveWork": {}, "LiveWorkFilter": {}, "LiveWorkPage": {},
		"ListLiveWork": {}, "ListHistoryByKeys": {}, "QueueDepth": {}, "TraceOption": {},
		"CommandFailure": {}, "StatusSucceeded": {}, "StatusFailed": {}, "StatusCancelled": {},
		"StatusExpired": {}, "NopObserver": {}, "WithFailFast": {}, "WithMetadata": {},
	}
	removedFields := map[string]map[string]struct{}{
		"Run":          {"Type": {}, "Version": {}, "Key": {}, "Created": {}, "FailFast": {}, "Metadata": {}},
		"TraceCommand": {"State": {}, "Required": {}},
		"RunFilter":    {"Type": {}, "Metadata": {}},
	}
	entries, err := os.ReadDir(filepath.Dir(currentFile))
	if err != nil {
		t.Fatal(err)
	}
	files := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(files, filepath.Join(filepath.Dir(currentFile), name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		check := func(identifier *ast.Ident) {
			if identifier == nil || !ast.IsExported(identifier.Name) {
				return
			}
			if _, removed := denied[identifier.Name]; removed {
				t.Errorf("removed package declaration %s reappeared in %s", identifier.Name, name)
			}
		}
		for _, declaration := range file.Decls {
			switch value := declaration.(type) {
			case *ast.FuncDecl:
				check(value.Name)
				if value.Recv != nil && (value.Name.Name == "With" || value.Name.Name == "Execute" || value.Name.Name == "Emit" || value.Name.Name == "Optional") {
					t.Errorf("removed method %s reappeared in %s", value.Name.Name, name)
				}
			case *ast.GenDecl:
				for _, specification := range value.Specs {
					switch specification := specification.(type) {
					case *ast.TypeSpec:
						check(specification.Name)
						if structure, ok := specification.Type.(*ast.StructType); ok {
							for _, field := range structure.Fields.List {
								for _, name := range field.Names {
									if name.IsExported() && strings.HasPrefix(name.Name, "Execution") {
										t.Errorf("removed public field %s.%s reappeared in %s", specification.Name.Name, name.Name, entry.Name())
									}
									if fields := removedFields[specification.Name.Name]; fields != nil {
										if _, removed := fields[name.Name]; removed {
											t.Errorf("removed public field %s.%s reappeared in %s", specification.Name.Name, name.Name, entry.Name())
										}
									}
								}
							}
						}
					case *ast.ValueSpec:
						for _, identifier := range specification.Names {
							check(identifier)
						}
					}
				}
			}
		}
	}
}

func TestProductionLookupSymbolsStayRemoved(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate flow module")
	}
	root := filepath.Dir(currentFile)
	files := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if path != root && (entry.Name() == ".git" || entry.Name() == "specs") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(files, path, nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if ok && strings.HasPrefix(function.Name.Name, "Lookup") {
				t.Errorf("production Lookup symbol %s reappeared in %s", function.Name.Name, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestGenericAPIMisuseDoesNotCompile keeps the most important static-safety
// claims executable. Each fixture is a separate package so one expected type
// error cannot hide another.
func TestGenericAPIMisuseDoesNotCompile(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate flow module")
	}
	moduleRoot := filepath.Dir(currentFile)
	tests := map[string]string{
		"command_arguments": `package misuse
import (
	"context"
	flow "github.com/goware/flow"
)
var command = flow.DefineCommand[string, int]("compile.command", 1)
func invalid(rt *flow.Runtime) {
	_, _ = command.Enqueue(context.Background(), rt, "key", 42)
}
`,
		"worker_result": `package misuse
import (
	"context"
	flow "github.com/goware/flow"
)
var command = flow.DefineCommand[string, int]("compile.worker", 1)
var _ = flow.Handle(command, func(context.Context, *flow.Work[string]) (string, error) {
	return "wrong", nil
})
`,
		"event_payload": `package misuse
import (
	"context"
	flow "github.com/goware/flow"
)
var event = flow.DefineEvent[string]("compile.event")
func invalid(client flow.Client, id flow.RunID) {
	_ = event.Deliver(context.Background(), client, id, "key", 42)
}
`,
	}
	for name, source := range tests {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			goMod := "module compilemisuse\n\ngo 1.24\n\nrequire github.com/goware/flow v0.0.0\nreplace github.com/goware/flow => " + moduleRoot + "\n"
			if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte(goMod), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, "misuse.go"), []byte(source), 0o600); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("go", "test", "-mod=mod", ".")
			command.Dir = directory
			output, err := command.CombinedOutput()
			if err == nil {
				t.Fatalf("invalid generic API use compiled successfully:\n%s", output)
			}
			message := string(output)
			if !strings.Contains(message, "cannot use") && !strings.Contains(message, "does not match inferred type") {
				t.Fatalf("compile failure did not report a type mismatch:\n%s", output)
			}
		})
	}
}
