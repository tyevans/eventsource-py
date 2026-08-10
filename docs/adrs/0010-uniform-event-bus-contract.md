# 0010 - Uniform Event Bus Contract: `background` Semantics and `BaseEventBus`

## Status

Accepted (2026-07-29). Implemented by `src/eventsource/bus/base.py`
(`BaseEventBus`), `src/eventsource/bus/registry.py` (`SubscriptionRegistry`),
and the four backends -- `memory.py`, `redis.py`, `rabbitmq.py`, `kafka.py` --
which now all subclass `BaseEventBus` instead of `EventBus` directly.

**Amended by [ADR 0060](0060-bounded-background-publishing.md)** -- for the
call shape of `_track_background` only. It is now a coroutine that must be
awaited, and bounds in-flight background publishes rather than spawning
without limit. This ADR's guidance below to "hand the operation to
`self._track_background(...)` rather than awaiting it" is superseded by that;
the decision that background tracking and draining live once on
`BaseEventBus` stands unchanged.

**Amended by [ADR 0031](0031-bus-ring-split.md)** -- for module locations
only: `BaseEventBus` and `SubscriptionRegistry` live under
`eventsource.adapters._bus` now, not `eventsource.bus`. This ADR's
`background` semantics and contract Decision is unchanged.

This ADR amends [0007 - Event Bus Delivery Semantics and Tracing
Contract](0007-event-bus-delivery-semantics.md): D4 (thread-safety) is now
enforced by shared machinery rather than per-adapter locks, and the
`background` parameter mentioned in that ADR's worked examples now has a
uniform, documented meaning across all four adapters instead of being silently
ignored by one of them. See that ADR's Status section for the pointer.

## Context

Two gaps surfaced while writing a conformance test suite meant to run
identically against all four `EventBus` backends (`docs/superpowers/specs/2026-07-29-event-bus-contract-and-coverage-design.md`):

1. **`background=True` meant different things per backend.** `InMemoryEventBus`
   tracked the publish as a background `asyncio.Task`. `KafkaEventBus` did not
   await the broker ack. `RabbitMQEventBus` did not await the publisher
   confirm. `RedisEventBus` silently ignored the flag and always waited for the
   stream write -- its docstring said so ("Ignored for Redis"). A parameter
   that changes behavior on three backends and does nothing on the fourth is
   not a contract; it is an accident of implementation order.

2. **Subscription management, event-class resolution, and background-task
   tracking were reimplemented four times.** `bus/memory.py:215` copied two
   lists per dispatched event out of a `dict[type, list[HandlerAdapter]]`
   guarded by a `threading.RLock`; `redis.py`, `kafka.py`, and `rabbitmq.py`
   each carried their own near-identical `_get_event_class` lookup, and only
   `InMemoryEventBus` and `KafkaEventBus` tracked background tasks for orderly
   drain on shutdown -- Redis had no such tracking to honor `background=True`
   with. Four copies of the same logic drift; Kafka's copy had already drifted
   into a real bug (see below).

3. **Kafka's handler keying used the event *type name* as a string, not the
   event class.** Two identically-named event classes registered from
   different modules would resolve to whichever one happened to be looked up
   second, silently dropping delivery to the other's subscribers. This was
   found while unifying dispatch through the shared registry, which stores
   handlers keyed by `type[DomainEvent]`.

## Decision

### `background=True` uniformly means "do not wait for durability"

Across all four backends, `background=True` now means: return once the
event(s) are handed off, without waiting for delivery to be confirmed,
persisted, or handled.

- **InMemory**: publish runs as a tracked `asyncio.Task` (unchanged).
- **Redis**: the stream write (`XADD`) is deferred to a tracked background
  task instead of always being awaited. This is a behavior change -- see
  Consequences.
- **Kafka**: the producer send is registered with the shared background-task
  tracker instead of awaiting the broker ack.
- **RabbitMQ**: the publish does not await the publisher confirm.

In every case the operation still eventually happens (or fails and is logged);
`background=True` only removes the caller's wait, not the attempt.

### `BaseEventBus(EventBus)`: a concrete layer between the ABC and the backends

`EventBus` in `bus/interface.py` remains a pure ABC -- `publish` plus six
abstract subscription methods -- so third parties can still implement it
directly without inheriting anything. `BaseEventBus` sits between the ABC and
the four shipped backends and owns three things every backend needs:

- A `SubscriptionRegistry` (`bus/registry.py`): a thread-safe (`RLock`-guarded)
  store of `dict[type[DomainEvent], tuple[HandlerAdapter, ...]]` plus a
  wildcard tuple. Handlers are stored as immutable tuples, and
  `handlers_for(event_type)` returns a precomputed `(specific + wildcard)`
  tuple, so dispatch does zero allocation per event -- replacing the two
  per-event list copies `InMemoryEventBus._dispatch_event` used to do.
- Event-class resolution (`_resolve_event_class(name)`), replacing the three
  duplicated `_get_event_class` implementations, resolved via
  `type[DomainEvent]` identity rather than by name string. This is the fix for
  the Kafka collision described above: dispatch keys are the actual classes
  the `SubscriptionRegistry` was built around, not a name that two classes can
  share.
- Background-task tracking (`_track_background(coro)`,
  `async _drain_background(timeout)`), which every backend's `shutdown()`
  delegates to, so a backend that spawns background work (any of the four,
  once Redis gained tracking) has one drain path instead of reimplementing
  cleanup.

Resulting hierarchy:

```
interface.py:  EventBus(ABC)           # pure: publish + 6 abstract subscription methods
base.py:       BaseEventBus(EventBus)  # + SubscriptionRegistry, background tasks, event resolution
memory.py:     InMemoryEventBus(BaseEventBus)
redis.py:      RedisEventBus(BaseEventBus)
kafka.py:      KafkaEventBus(BaseEventBus)
rabbitmq.py:   RabbitMQEventBus(BaseEventBus)
```

Subscription methods (`subscribe`, `unsubscribe`, `subscribe_all`, etc.) are
now implemented once, concretely, on `BaseEventBus`; the four backends no
longer implement them at all.

## Consequences

### For users

`background=True` now has one meaning everywhere. Code that relied on Redis
always waiting for the stream write despite passing `background=True` will
observe a behavior change: the write is now deferred like the other three
backends. This is a changelog-worthy behavior change (minor version bump per
this branch's decisions table), not a bugfix, because the old behavior was
documented (if oddly) rather than accidental.

The Kafka handler-keying fix is a correctness fix, not a behavior change users
should have depended on: any application that happened to work around the
name-collision bug (e.g., by avoiding same-named event classes across modules)
is unaffected: the fix only restores delivery that was silently dropped
before.

### For contributors adding a fifth adapter

Subclass `BaseEventBus`, not `EventBus`, unless the new adapter genuinely needs
to reimplement subscription management (it should not). Call
`super().__init__()`. Implement `publish` to honor `background` per this ADR:
hand the operation to `await self._track_background(...)` when
`background=True` (see the amendment above — awaiting schedules the work and
returns immediately while there is headroom, and runs it inline at the
capacity bound), and drain via `self._drain_background(timeout)` from
`shutdown()`.

### For 0007's D4 (thread-safety)

D4 described `threading.RLock` as owned by `InMemoryEventBus` directly and
`asyncio.Lock` as owned by each broker adapter's connection lifecycle. The
`RLock` for subscription state now lives in `SubscriptionRegistry` inside
`BaseEventBus`, shared by all four backends rather than reimplemented by
`InMemoryEventBus` alone. The `asyncio.Lock`s over broker connection/channel
lifecycle in Redis and RabbitMQ are unchanged by this ADR.

## Alternatives Considered

### Deprecate `background` on the ABC instead of giving it uniform meaning

Rejected. The parameter already existed on all four `publish` signatures and
is part of the public contract exercised by the conformance suite; removing it
would be a larger breaking change than fixing its semantics, and callers who
use it on InMemory or Kafka today have a real, sensible use for "don't make me
wait."

### Split `background` into two separate parameters (e.g., `wait_for_ack` and `wait_for_handlers`)

Rejected. It would more precisely describe what "waiting" means per backend,
but at the cost of a wider, backend-leaking API surface, and no caller in this
codebase or its test suite needed the distinction. A single boolean with one
documented meaning ("do not wait for durability") is simpler to hold in your
head across four backends and matches what `InMemoryEventBus` and
`KafkaEventBus` already exposed.

### Document Redis's divergence instead of fixing it

Rejected, consistent with the same call made for handler-error isolation (see
[0011](0011-handler-error-isolation-with-no-ack.md)). A parameter that is
honored by three backends and silently ignored by the fourth is a trap for
anyone who swaps backends without re-reading every adapter's docstring, which
is exactly the failure mode `EventBus` as a shared abstraction exists to
prevent.

### Keep four copies of subscription management, event resolution, and background tracking

Rejected. The duplication had already produced one real bug (Kafka's
name-keyed dispatch) and was actively blocking a fifth copy (Redis background
tracking) from being written correctly. A shared concrete base class that the
ABC does not require third parties to use costs nothing for external
implementers and removes the duplication for the four shipped backends.

## References

- `src/eventsource/bus/base.py` -- `BaseEventBus`
- `src/eventsource/bus/registry.py` -- `SubscriptionRegistry`
- `src/eventsource/bus/interface.py` -- `EventBus` ABC, `background` docstring
- `src/eventsource/bus/memory.py`, `redis.py`, `kafka.py`, `rabbitmq.py`
- `docs/superpowers/specs/2026-07-29-event-bus-contract-and-coverage-design.md`
- `docs/adrs/0007-event-bus-delivery-semantics.md` -- D4, amended by this ADR
