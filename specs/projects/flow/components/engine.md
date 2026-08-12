---
status: complete
completed_at: 2026-08-04
---

# Component: definitions and run engine

## Purpose

The engine owns typed contracts and deterministic worker decisions. It transforms one invocation into a canonical change set without scheduling goroutines or issuing SQL while application code runs.

## Definitions and worker decisions

Commands retain name/version, argument/result codecs, retry policy, attempt timeout, and queue. Events retain a name and payload codec. `Event.Deliver` is deliberately detached targeted ingress to a known run, including from application code inside a worker attempt. Definitions are immutable; invalid names/versions/options and nil workers fail validation.

`Work[A]` is the attempt-local scope for one claimed command, not the whole
run or the immutable command definition. A fresh value is created for every
worker invocation. It exposes typed `Args` and immutable `CommandInfo`,
including the exact root `RunKey` already loaded with the claim. Its private
state records the first defect, staged events, staged sub-commands, event
snapshots, and stable insertion order. It is valid only during the worker call
and must not be retained or used concurrently. Worker-scoped `Enqueue` and
`Emit` accept only `*Work`;
`Event.Deliver` requires a client and target run ID because it is not part of
the decision.

Durable arguments, results, event payloads, retry settings, and journal bodies
use bounded canonical JSON and hashes. A command fingerprint covers definition,
key, arguments, parent, exact waits, delay, wait budget, queue, timeout, and
retry policy.

A useful command boundary introduces independent retry, side effects,
isolation, timeout, queue ownership, or parallelism. Small deterministic
transformations remain in one worker. Parent-produced data goes directly into
child arguments; exact events carry sibling, cross-branch, or external facts;
large or sensitive values use stable application-storage references.

## Staged-command grammar and event inputs

`StagedCommand` is a non-generic ephemeral staging builder:

- `Key` returns its durable key;
- `Delay` sets one initial delay;
- `WaitFor` adds one exact event selector;
- `Within` sets one wait budget.

Distinct waits on a repeated same-decision declaration merge as AND gates.
Other disagreement poisons the decision. Staged commands are valid only during
their owning worker invocation.

Before a worker runs, the runtime supplies immutable canonical snapshots for
accepted event inputs. `GetEventValue` performs an O(1) exact name/key lookup
and returns `(value, found, error)` from memory. Ordinary absence is
`found=false`; invalid selectors or corrupt typed payloads become decision
defects. The function does not wait or query the database. At most 256 waits
may exist on one command. One worker decision may stage at most 256 distinct
application events; exact duplicate emission remains idempotent and overflow
poisons the whole decision.

Positive public durations are rounded upward once to durable millisecond
precision before declaration fingerprints are computed. Internal store and
decode boundaries continue to require exact milliseconds.

## Normalization and failure

Before settlement the engine returns the first defect, encodes the result, sorts events by name/key, sorts commands by key, validates modifiers, computes fingerprints, and enforces the run command ceiling. No partially valid decision is accepted.

This complete normalized decision lets the store compare staged identities and
persist commands, waits, and queue rows in bounded sets while preserving stable
journal mapping. Related events and children should be staged together when
they form one atomic change. Very large fan-outs should be chunked through
bounded batch commands, and large all-of inputs reduced through hierarchical
joins.

Every unsuccessful terminal command makes the run fail. Queued/non-running
siblings are cancelled, running attempts remain fenced and settleable, and
sub-commands staged after failure begins are recorded cancelled.

`flowtest` uses the production recorder and codecs. It supports worker invocation, declared event fixtures, staged event/command inspection, modifier inspection, commit callbacks, and command-ceiling validation without PostgreSQL.
