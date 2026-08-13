# ADR-0065. An Event Cannot Name an Aggregate Other Than the One Emitting It

`aggregate_id` was the one auto-populated field a caller could override, and
the override survived to the store, where it decides which stream the event
lands in. Emitting an event that names a different aggregate now raises
`AggregateIdMismatchError`.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0046](0046-aggregate-type-single-source.md) | Extended. 0046 made the aggregate the single source for `aggregate_type` and left `aggregate_id` alone, because `aggregate_id` genuinely comes from the caller at construction. That stays true — this is not a second declaration site being collapsed. What is added is that the *event's* copy of it may not disagree with the aggregate's. |
| [0048](0048-failure-paths-report-and-retain.md) | Extended. 0048 established that a divergence invisible to a save/load round-trip is reported at the point it occurs rather than silently accepted, and applied it to a divergent `aggregate_type`. This is the same rule one field over. |
| [0022](0022-command-objects-and-decider-style.md) | Stands. The decider path gets the guard through `_stamp`, which is where 0022 already put stamping. |

## Context

An event's `aggregate_id` is its stream key. `create_event()` stamps it from
the aggregate, but an explicit keyword argument overrode that — and unlike
`aggregate_type`, the overridden value was never replaced downstream. It
reached the store intact.

The result is an event appended to a stream that disowns it. The aggregate that
emitted it never reads that stream. The aggregate the event names never
receives it, because it was appended under a different `expected_version`
lineage than that aggregate's own. Neither side can see the disagreement: load
the emitter and the event is absent; load the target and it is absent there
too. Every test passes, because a round-trip of either aggregate is internally
consistent. This is precisely the failure event sourcing is supposed to
preclude, and the library offered no guard against it.

The shape that produces it is ordinary: a command names a target
(`ShipOrder(order_id=...)`), and a handler copies that id onto the event it
builds. The named id is routing information — it selects *which* aggregate to
load — not a value to copy onto an event emitted from somewhere else.

## Decision

Reject it, on the aggregate, from `event.aggregate_id`.

Reading the event's own id rather than a per-aggregate declaration of what may
be targeted is what makes this work for every aggregate with no opt-in, no new
base class, and no field to remember to set. The guard sits at three points on
one funnel: `apply_event(is_new=True)` is the backstop and catches
hand-constructed events; `create_event()` and `DeciderAggregate._stamp()` run
ahead of it so the message can name the command being handled.

Replay (`is_new=False`) is not checked. Rehydration reads a stream whose events
agree with it by construction, and checking there would turn a historical
mistake into an unloadable aggregate.

### Rejected: a per-aggregate targeting declaration

A mixin (or class attribute) by which an aggregate declares which ids it may
target. It fails earlier and can name the command type, which is a better
message — but it requires every aggregate to opt in, and an aggregate that
forgets is exactly the one with the bug. A guard that needs a declaration
protects the codebases that did not need protecting.

### Rejected: restamping the foreign id silently

Symmetrical with what `aggregate_type` used to do, and rejected for the reason
0048 gives: a caller who passed an id meant something by it, and quietly
substituting another turns a visible mistake into an invisible one.

## Consequences

Code that relied on the override to emit an event about another aggregate now
raises. There is no shim, per the pre-1.0 no-shim policy. The correct form is
to load the named aggregate and emit from it — which is also the form that gets
the target's version lineage and optimistic-concurrency check right, both of
which the override skipped.

Cross-aggregate workflows are unaffected as long as they cross through the
store: emit from one aggregate, project or subscribe, then load and emit from
the other. The guard rejects one aggregate writing into another's stream, not
one aggregate causing another to act.
