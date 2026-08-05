---
status: complete
---

# Component: definitions and execution engine

## Purpose

The engine owns typed contracts and deterministic application decisions. It validates definitions and transforms one worker or coordinator invocation into a canonical change set. It does not schedule goroutines or issue PostgreSQL queries while application code runs.

## Definitions

Commands retain name/version, argument/result codecs, retry policy, attempt timeout, and queue. Events retain a name and payload codec. Coordinators retain name/version, state codec, and unique typed handler selectors.

All definitions are immutable values. Zero definitions, invalid names/versions, duplicate options, nil handlers, and duplicate coordinator selectors become registration/start errors rather than panics.

Registration erases generic functions behind codecs after validation. The runtime registry contains only worker and coordinator entries keyed by exact name/version.

## Canonical values

Durable arguments, results, state, event payloads, metadata, retry settings, and journal bodies use bounded canonical JSON. Canonical bytes and SHA-256 hashes support deterministic equality, idempotent retries, declaration fingerprints, and replay verification.

Command declaration fingerprints cover definition, key, arguments, origin, parent, required flag, exact waits, initial delay, wait budget, queue, timeout, and retry policy.

## Worker scope

`Work[A]` exposes typed `Args` and immutable `CommandInfo`. Its private scope contains:

- the first validation defect;
- staged application events;
- staged child commands;
- stable insertion order.

It does not expose results or outcomes from other commands.

`flow.Emit` validates the application event, non-empty stable key, payload type/size, duplicate identity, and scope. `flow.Execute` validates command definition, key, arguments, and scope, then returns an ephemeral `Node`.

## Node grammar

`Node` has no result type because it is a staging builder, not a query handle.

- `Key` returns its durable command key.
- `Optional` clears the required flag.
- `Delay` sets one initial delay of at least one millisecond.
- `WaitFor` adds one exact application event name/key selector.
- `Within` sets one wait budget of at least one millisecond.

Multiple waits are normalized, sorted, deduplicated, and interpreted as AND. Repeating the same staged command in a decision merges waits. Different arguments or singleton settings poison the complete decision. `Within` without waits is invalid.

Nodes are valid only during their owning worker/coordinator decision. They cannot be used to read outcomes or retained after return.

## Coordinator scope

`Coordination[S]` contains mutable typed state and the same decision recorder. Handlers receive exactly one typed retained input.

`OnOutcome` decodes every terminal command event into `Outcome[R]`. Success contains `R`; failure, cancellation, and expiry contain structured failure information.

`Succeed` and `Fail` encode the terminal state selection. A coordinator terminal choice is single-assignment; staging afterward poisons the decision. Handler error leaves the current delivery and state unaccepted so runtime retry can occur.

## Decision normalization

Before settlement, the engine:

1. returns the first scope defect, if any;
2. encodes the worker result or coordinator state;
3. sorts staged events by name/key;
4. sorts staged commands by key;
5. validates each modifier combination;
6. computes declaration fingerprints;
7. enforces the execution command ceiling;
8. emits store-ready canonical records.

No partially valid decision is accepted.

## Exact waits and readiness

Readiness is a two-input calculation:

```text
all exact waits satisfied AND initial delay elapsed
```

The wait deadline is derived at command creation. It runs concurrently with initial delay. Expiry terminalizes a still-waiting command. Once all waits resolve, delay eligibility remains authoritative.

Wait declarations contain only event name/key. Event payloads are consumed by coordinator handlers, not by command readiness.

## Failure calculation

The deterministic failure resolver distinguishes required and optional commands. Required failure can enter execution `failing`; optional failure is retained but does not determine direct completion.

Reduced fail-fast targets commands without running attempts. Running attempts keep their fences. If one later settles successfully, its result/events/commit are accepted and its staged children are recorded as cancelled when the execution is already failing.

## Event ordering

Within a semantic transaction, journal entries use deterministic groups:

1. attempt conclusion or coordinator transition context;
2. staged application events in canonical order;
3. staged command creation in key order;
4. command/coordinator terminal events;
5. derived cancellations or expiries in key order;
6. execution failing/terminal event where applicable.

Causation positions connect derived entries to the accepted input.

## Testing surface

`flowtest` crosses a private bridge to use the production recorder. It supports worker invocation, coordinator start/event/outcome delivery, staged event/command inspection, modifier inspection, commit-function execution, and command-ceiling validation without PostgreSQL.

Unit and integration tests cover duplicate declarations, scope poisoning, canonical equality, gate combinations, delay/expiry timing, outcomes, coordinator terminality, command ceilings, fail-fast settlement, and replay parity.
