package flow

import (
	"slices"
	"time"

	"github.com/goware/flow/internal/durable"
)

// Node is an ephemeral builder for a command staged by a worker decision. It
// is valid only for the duration of that decision.
type Node struct {
	scope *scopeState
	key   string
}

func (node *Node) Key() string {
	if node == nil {
		return ""
	}
	return node.key
}

func (node *Node) WaitFor(event EventRef, key string) *Node {
	if node == nil {
		return node
	}
	wait, err := makeCommandEventWait(event, key)
	if err != nil {
		node.poison(err)
		return node
	}
	if command, ok := node.decisionCommand("wait for"); ok {
		if !slices.Contains(command.waits, wait) && len(command.waits) >= maxCommandEventWaits {
			node.scope.poison(newError(ErrInvalid, "enqueue", "wait", node.key, "command exceeds the 256 event-wait limit"))
			return node
		}
		command.waits = addCommandEventWait(command.waits, wait)
		node.scope.decision.commands[node.key] = command
	}
	return node
}

func (node *Node) Within(duration time.Duration) *Node {
	if node == nil {
		return node
	}
	if duration <= 0 {
		node.poison(newError(ErrInvalid, "enqueue", "within", node.key, "within must be positive"))
		return node
	}
	normalized, _, err := durable.CeilMilliseconds("within", duration)
	if err != nil {
		node.poison(newError(ErrInvalid, "enqueue", "within", node.key, err.Error()))
		return node
	}
	if command, ok := node.decisionCommand("within"); ok {
		if command.within == normalized {
			return node
		}
		if command.within > 0 {
			node.scope.poison(newError(ErrInvalid, "enqueue", "within", node.key, "within configured with different values"))
			return node
		}
		command.within = normalized
		node.scope.decision.commands[node.key] = command
	}
	return node
}

func (node *Node) Delay(duration time.Duration) *Node {
	if node == nil {
		return node
	}
	if duration <= 0 {
		node.poison(newError(ErrInvalid, "enqueue", "delay", node.key, "delay must be positive"))
		return node
	}
	normalized, _, err := durable.CeilMilliseconds("delay", duration)
	if err != nil {
		node.poison(newError(ErrInvalid, "enqueue", "delay", node.key, err.Error()))
		return node
	}
	if command, ok := node.decisionCommand("delay"); ok {
		if command.startAfter == normalized {
			return node
		}
		if command.startAfter > 0 {
			node.scope.poison(newError(ErrInvalid, "enqueue", "delay", node.key, "delay configured more than once"))
			return node
		}
		command.startAfter = normalized
		node.scope.decision.commands[node.key] = command
	}
	return node
}

func (node *Node) Optional() *Node {
	if node == nil {
		return node
	}
	if command, ok := node.decisionCommand("optional"); ok {
		if !command.required {
			return node
		}
		command.required = false
		node.scope.decision.commands[node.key] = command
	}
	return node
}

func (node *Node) decisionCommand(operation string) (stagedCommand, bool) {
	if node.scope == nil || node.scope.firstError != nil {
		return stagedCommand{}, false
	}
	command, ok := node.scope.decision.commands[node.key]
	if !ok {
		node.scope.poison(newError(ErrInvalidState, "enqueue", operation, node.key, "command is unavailable"))
	}
	return command, ok
}

func (node *Node) poison(err error) {
	if node != nil && node.scope != nil {
		node.scope.poison(err)
	}
}
