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
	Worker Operation = "worker"
	Commit Operation = "commit"
)

type Info struct {
	RunID            string
	RunKey           string
	Definition       string
	CommandID        string
	CommandKey       string
	Name             string
	Version          int
	CreatedAt        time.Time
	BudgetStartedAt  time.Time
	Attempt          int
	AttemptStartedAt time.Time
}

type Request struct {
	Operation   Operation
	Context     context.Context
	Args        json.RawMessage
	Result      json.RawMessage
	Info        Info
	Tx          any
	EventInputs []EventInput
}

type EventInput struct {
	Name     string
	Key      string
	Position int64
	Payload  json.RawMessage
}

type StagedCommand struct {
	Key        string
	Name       string
	Version    int
	Args       json.RawMessage
	Required   bool
	StartAfter time.Duration
	Waits      []EventWait
	Within     time.Duration
}

type EventWait struct {
	Name string
	Key  string
}

type StagedEvent struct {
	Name    string
	Key     string
	Payload json.RawMessage
}

type Result struct {
	Value        json.RawMessage
	HandlerError error
	Panicked     bool
	Commands     []StagedCommand
	Events       []StagedEvent
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
