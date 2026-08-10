# ADR-0060. Background Publishing Is Bounded, and Degrades to Inline

`publish(background=True)` spawned one tracked `asyncio.Task` per call with no
ceiling. A producer faster than its handlers therefore grew the in-flight task
set without limit, and `shutdown()` had to wait for — or cancel — all of it.
This ADR records the ceiling, and the more consequential decision of what
happens when a caller reaches it.

## Status

**Accepted.**

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0010](0010-uniform-event-bus-contract.md) | Amended. 0010 established `BaseEventBus` as the one home for background-task tracking and one drain path, and told adapter authors to "hand the operation to `self._track_background(...)` rather than awaiting it when `background=True`". That instruction is now literally wrong: `_track_background` is a coroutine and its three call sites await it. The decision 0010 made — one tracking mechanism, one drain path, `background` semantics owned by the base class — is untouched and is precisely what made this bound a change in one place rather than four. Only the call shape moved. |
| [0007](0007-event-bus-delivery-semantics.md) | Stands. 0007 promises at-least-once delivery with handler isolation. Inline degradation preserves it exactly: every event is still delivered to every handler, and the only observable difference is when the `publish()` call returns. Dropping at capacity would have contradicted 0007, which is the main reason it was rejected. |
| [0021](0021-snapshot-policy-scheduler-composition.md) | Stands, and its recorded negative is now half-closed. 0021 records that background snapshot scheduling has no bound on pending tasks. That gap is real and still open; this ADR closes the *same shape* in the event bus, the other consumer of `BackgroundTaskManager`. The manager now carries an optional `max_pending` and an `at_capacity` property, so closing 0021's half is a policy decision at that call site rather than new mechanism. |
| [0017](0017-snapshot-strategy-pattern.md) | Stands, for the same reason — its unbounded-background-task negative is about snapshot scheduling, not publishing. |

## Context

Three of the four bus adapters (`memory`, `redis`, `kafka`) implement
`publish(background=True)` by handing a coroutine to `_track_background`. The
task set was tracked, so nothing leaked, but nothing capped its size either.
Under sustained load where handlers are slower than producers — the condition a
background publish exists to paper over — the set grows until the process runs
out of memory, and every task is holding the events it has yet to deliver.

The ceiling is the easy half. The question that decides the design is what a
caller that reaches it should experience.

**Blocking on a slot deadlocks.** A handler invoked inside a background publish
task may itself publish; this is ordinary in a system where one event begets
another. If the inner call waited for a free slot, it would wait for one held by
the task it is running inside, and neither would ever proceed. Making that safe
requires a re-entrancy guard — a `contextvars` flag marking "already inside a
background publish" — which is real machinery whose failure mode is a hang.

**Dropping at capacity is not available to us.** ADR 0007 promises at-least-once
delivery. Silently discarding events under load contradicts it, and does so
invisibly: the symptom is a projection missing rows nobody will look for.

## Decision

`BaseEventBus` takes `max_background_tasks` (default `DEFAULT_MAX_BACKGROUND_TASKS`,
`None` to disable). At the ceiling, `_track_background` **awaits the coroutine
inline** and returns `None` instead of spawning another task.

`BackgroundTaskManager` gained `max_pending` and an `at_capacity` property, but
deliberately does **not** enforce anything: it reports capacity and the caller
decides. The right behavior at the ceiling differs per consumer, and a manager
that blocked or refused would impose the bus's answer on snapshot scheduling too.

## Consequences

**It cannot deadlock, and needs no re-entrancy guard.** A re-entrant publish at
capacity runs inline and completes. This is the property that made inline
degradation preferable to a semaphore, not a side benefit of it.

**No event is lost.** At-least-once holds unchanged.

**`publish(background=True)` is only non-blocking while there is headroom.**
Once saturated, it takes as long as delivery takes. This is the intended
backpressure — a producer is slowed to the rate its handlers can absorb — but it
is a real change in the shape of the call, and callers that assumed a constant
fast return will see latency appear under load rather than an error.

**The default is a real number, not an opt-in.** A bound nobody sets is not a
bound; that is the inert-configuration defect this library has now paid for
several times. `None` remains available for callers who genuinely want
unbounded fire-and-forget.

**Adapter authors await it.** Any backend implementing `background=True` must
`await self._track_background(...)`. A non-awaited call is now a bug that
surfaces as a never-executed coroutine.
