package flow

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

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
	_, _ = command.With(rt).Execute(context.Background(), "key", 42)
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
func invalid(client flow.Client, id flow.ExecutionID) {
	_ = event.Emit(context.Background(), client, id, "key", 42)
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
