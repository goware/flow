package flow

import "time"

// Node is an ephemeral builder for a command staged by a worker or
// coordinator decision. It is valid only for the duration of that decision.
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
		command.waits = addCommandEventWait(command.waits, wait)
		node.scope.decision.commands[node.key] = command
	}
	return node
}

func (node *Node) Within(duration time.Duration) *Node {
	if node == nil {
		return node
	}
	if duration < time.Millisecond {
		node.poison(newError(ErrInvalid, "execute", "within", node.key, "within must be at least one millisecond"))
		return node
	}
	if command, ok := node.decisionCommand("within"); ok {
		if command.within == duration {
			return node
		}
		if command.within > 0 {
			node.scope.poison(newError(ErrInvalid, "execute", "within", node.key, "within configured with different values"))
			return node
		}
		command.within = duration
		node.scope.decision.commands[node.key] = command
	}
	return node
}

func (node *Node) Delay(duration time.Duration) *Node {
	if node == nil {
		return node
	}
	if duration < time.Millisecond {
		node.poison(newError(ErrInvalid, "execute", "delay", node.key, "delay must be at least one millisecond"))
		return node
	}
	if command, ok := node.decisionCommand("delay"); ok {
		if command.startAfter == duration {
			return node
		}
		if command.startAfter > 0 {
			node.scope.poison(newError(ErrInvalid, "execute", "delay", node.key, "delay configured more than once"))
			return node
		}
		command.startAfter = duration
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
	if node.scope.terminal != nil {
		node.scope.poison(newError(ErrInvalidState, "execute", operation, node.key, "coordinator is already terminal"))
		return stagedCommand{}, false
	}
	command, ok := node.scope.decision.commands[node.key]
	if !ok {
		node.scope.poison(newError(ErrInvalidState, "execute", operation, node.key, "command is unavailable"))
	}
	return command, ok
}

func (node *Node) poison(err error) {
	if node != nil && node.scope != nil {
		node.scope.poison(err)
	}
}
