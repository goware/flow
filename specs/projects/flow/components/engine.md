---
status: complete
completed_at: 2026-08-04
---

# Component: definitions and execution engine

## Purpose

The engine owns typed contracts and deterministic worker decisions. It transforms one invocation into a canonical change set without scheduling goroutines or issuing SQL while application code runs.

## Definitions and worker decisions

Commands retain name/version, argument/result codecs, retry policy, attempt timeout, and queue. Events retain a name and payload codec. `Event.Emit` is guarded external ingress; `Event.Deliver` is deliberately detached targeted ingress, including from a worker attempt. Definitions are immutable; invalid names/versions/options and nil workers fail validation.

`Work[A]` exposes typed `Args` and immutable `CommandInfo`. Its private state records the first defect, staged events, staged sub-commands, declared event snapshots, and stable insertion order. Worker-scoped `Execute` and `Emit` accept only `*Work`; `Event.Deliver` requires only a client and target execution ID because it is not part of the decision.

Durable arguments, results, event payloads, metadata, retry settings, and journal bodies use bounded canonical JSON and hashes. A command fingerprint covers definition, key, arguments, parent, required flag, exact waits, delay, wait budget, queue, timeout, and retry policy.

## Node grammar and event inputs

`Node` is a non-generic ephemeral staging builder:

- `Key` returns its durable key;
- `Optional` clears required status;
- `Delay` sets one initial delay;
- `WaitFor` adds one exact event selector;
- `Within` sets one wait budget.

Distinct waits on a repeated same-decision declaration merge as AND gates. Other disagreement poisons the decision. Nodes are valid only during their owning worker invocation.

Before a worker runs, the runtime supplies immutable canonical snapshots for every declared wait. `GetEventValue` checks the exact name/key declaration and returns the typed value from memory. Misuse becomes a decision defect. The function does not wait or query the database. At most 256 waits may exist on one command.

## Normalization and failure

Before settlement the engine returns the first defect, encodes the result, sorts events by name/key, sorts commands by key, validates modifiers, computes fingerprints, and enforces the execution command ceiling. No partially valid decision is accepted.

Required command failure may enter reduced fail-fast. Optional failure is observable but does not determine success. Running attempts remain fenced and settleable; sub-commands staged after failure begins are recorded cancelled.

`flowtest` uses the production recorder and codecs. It supports worker invocation, declared event fixtures, staged event/command inspection, modifier inspection, commit callbacks, and command-ceiling validation without PostgreSQL.
