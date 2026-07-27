// Package testengine is the private bridge between flow's production
// deterministic recorders and the public database-free flowtest package. It
// contains no implementation of orchestration semantics.
package testengine

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type Operation string

const (
	Worker      Operation = "worker"
	Commit      Operation = "commit"
	Plan        Operation = "plan"
	Coordinator Operation = "coordinator"
)

type Info struct {
	ExecutionID      string
	CommandID        string
	CommandKey       string
	Name             string
	Version          int
	CreatedAt        time.Time
	BudgetStartedAt  time.Time
	Attempt          int
	AttemptStartedAt time.Time
}

type Dependency struct {
	Key            string
	Name           string
	Version        int
	Status         string
	Result         json.RawMessage
	FailureCode    string
	FailureMessage string
}

type PlanCommand struct {
	ID             string
	Key            string
	Name           string
	Version        int
	Origin         string
	State          string
	Result         json.RawMessage
	FailureCode    string
	FailureMessage string
}

type PlanEvent struct {
	ID        string
	Position  int64
	Namespace string
	Name      string
	Version   int
	Key       string
	Payload   json.RawMessage
}

type EventSelector struct {
	Namespace string
	Name      string
	Version   int
}

type Request struct {
	Operation Operation
	Context   context.Context
	Args      json.RawMessage
	Result    json.RawMessage
	Info      Info
	Tx        any

	ExecutionID    string
	Status         string
	MaxCommands    int
	JournalThrough int64
	Dependencies   []Dependency
	Commands       []PlanCommand
	Events         []PlanEvent
	KnownEvents    []EventSelector

	State                  json.RawMessage
	DeliveryKind           string
	DeliveryNamespace      string
	DeliveryName           string
	DeliveryVersion        int
	DeliveryKey            string
	DeliveryPosition       int64
	DeliveryRecordedAt     time.Time
	DeliveryPayload        json.RawMessage
	DeliveryStatus         string
	DeliveryResult         json.RawMessage
	DeliveryFailureCode    string
	DeliveryFailureMessage string
}

type StagedEvent struct {
	Name    string
	Version int
	Key     string
	Payload json.RawMessage
}

type StagedCommand struct {
	Key        string
	Name       string
	Version    int
	Args       json.RawMessage
	Required   bool
	StartAfter time.Duration
}

type Declaration struct {
	Key          string
	Name         string
	Version      int
	Args         json.RawMessage
	Required     bool
	Dependencies [][]string
	Waits        []string
	Within       time.Duration
	Delay        time.Duration
}

type Read struct {
	Kind         string
	Identity     string
	Availability string
}

type Result struct {
	Value          json.RawMessage
	HandlerError   error
	Panicked       bool
	Events         []StagedEvent
	Commands       []StagedCommand
	Declarations   []Declaration
	Reads          []Read
	WaitingReads   int
	State          json.RawMessage
	Terminal       string
	ResultRef      string
	TerminalReason string
}

var Run func(any, Request) (Result, error)

func Invoke(registration any, request Request) (Result, error) {
	if Run == nil {
		return Result{}, errors.New("flow test engine is not initialized")
	}
	if request.Context == nil {
		request.Context = context.Background()
	}
	return Run(registration, request)
}
