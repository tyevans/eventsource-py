# 0047. Live Runner Checkpointing Is Feed-Driven, Not Bus-Driven

`LiveRunner` never checkpointed during the live phase. It read a `_position`
attribute off the bare `DomainEvent` the bus delivered, but nothing in the
tree ever set that attribute, so the read was always `None` and the runner
silently fell back to recording every live event at its inherited
catch-up position. The store, not the bus, now owns ordering: on a bus
notification the runner drains `GlobalEventFeed.read_all(from_position=...)`
forward from its checkpoint and delivers what the feed returns.

## Status

**Accepted.** **Amended by
[ADR 0059](0059-ordered-subscription-delivery.md)**, scoped to the
checkpoint-lockstep dependency: the deletion of duplicate suppression below rests
on a checkpointed position never being re-read, which holds only while delivery and
position advance stay in lockstep. 0059 states that dependency and pins it. The
Decision here is unchanged.

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0007](0007-event-bus-delivery-semantics.md) | Amended — for the live-subscription consumer only. `LiveRunner` no longer treats a bus-delivered `DomainEvent` as the thing to deliver; the payload is a wake-up signal, and the global feed is the delivery source. ADR 0007's at-least-once, handler-isolation, and no-ordering-guarantee decisions for `EventBus` itself are unchanged — every other consumer (projections, integration handlers) still receives bus payloads directly. |
| [0019](0019-clean-architecture-store-ports.md) | Stands — this decision is a new consumer of the existing `GlobalEventFeed.read_all(from_position=...)` port and its no-skip/exclusive-resumption feed guarantee; no port shape changes. |
| [0009](0009-multi-instance-subscription-coordination.md) | Stands — coordination and leader election are untouched; this ADR is scoped to how one runner turns a wake-up into delivery. |

ADR 0007's Status section carries an "Amended by ADR 0047" pointer, scoped
to the live-subscription consumer.

## Context

`src/eventsource/application/subscriptions/runners/live.py` subscribes to
the event bus for every event type the subscriber handles and, on each bus
delivery, calls `_get_event_position(event)`:

```python
def _get_event_position(self, event: DomainEvent) -> Position | None:
    position = getattr(event, "_position", None)
    if isinstance(position, Position):
        return position
    return None
```

No adapter, bus backend, or publisher anywhere in the tree ever sets
`event._position`. `DomainEvent` is a frozen pydantic model with no such
field. The lookup always returns `None`.

Because `_process_live_event` receives `position=None`, the `else` branch
records the event at the subscription's *unchanged*
`last_processed_position` — the position catch-up left it at — and never
calls `_maybe_checkpoint`. A subscription that runs live for a week
restarts from wherever catch-up ended and replays its entire live period.
Catch-up on restart resumes from `subscription.last_processed_position`
(`runners/catchup.py:204`), which is exactly that stale watermark.

The same root cause made the catch-up→live duplicate-suppression check in
`_process_live_event` inert: with `position` always `None`, the
`position <= last_processed` guard could never fire, so
`events_skipped_duplicate` — and `TransitionResult.buffer_events_skipped`,
which read it — were structurally pinned at zero. The metric read "no
duplicates" precisely when duplicates (or, after this fix, nothing to
deduplicate at all) were the honest story.

`_process_live_event(event, position=None)` also carried a `position`
parameter every one of its three call sites left at the default, dead code
kept alive only by the one thing that ever set it: test fixtures poking
`event._position` directly.

The underlying gap is architectural, not a typo. `ports/bus.py`'s
`EventBus`/`SubscribableEventBus` deliver a bare `DomainEvent` with no
concept of global position (ADR 0007's contract never promised one — the
four adapters have four different notions of broker-native offset, and
`Position` is deliberately store-owned per ADR 0019). `EventLookup`
(`ports/store.py:53`) offers only `event_exists(id) -> bool`, not an
`event_id` → `Position` mapping. There was no port through which the live
runner could ever have learned a bus event's feed position.

## Decision

**The store owns ordering; the bus is a wake-up signal.**

`LiveRunner` gains an `event_feed: GlobalEventFeed` dependency (the same
narrow port `CatchUpRunner` and `TransitionCoordinator` already depend on —
`TransitionCoordinator` passes its own `event_store` through to both
`LiveRunner` construction sites, so no new object graph wiring is needed
beyond the constructor parameter). On a bus notification,
`_handle_live_event` no longer inspects the delivered `DomainEvent` at all;
it drains the feed:

```python
async def _drain_feed(self) -> int:
    processed = 0
    from_position = self.subscription.last_processed_position
    async for envelope in self.event_feed.read_all(from_position=from_position):
        self._stats.events_received += 1
        await self.subscription.record_events_seen(1)
        await self._process_live_event(envelope)
        processed += 1
    return processed
```

`_process_live_event` now takes an `EventEnvelope` (event + position,
already paired by the store) instead of a `DomainEvent` and an optional
`Position`. Every envelope the feed returns carries a real position, so
`_maybe_checkpoint` runs on every delivered event according to the
configured `CheckpointStrategy`, closing the bug.

Because `read_all(from_position=...)` is exclusive and re-reads
`subscription.last_processed_position` on every drain, a position already
checkpointed is never read from the feed again. The catch-up→live
duplicate-suppression check is therefore unreachable by construction, not
merely untested, and is deleted along with `LiveRunnerStats.events_skipped_duplicate`
and `TransitionResult.buffer_events_skipped` — a metric that can only ever
report zero is not a metric, it is a decoration.

During the catch-up-to-live transition, `LiveRunner.start(buffer_events=True)`
still defers processing: wake-up notifications are queued (as sentinels,
not stored events — the feed remains the only source of truth for *what*
to process) until `process_buffer()` is called, at which point a single
feed drain from the checkpoint picks up everything that arrived during
buffering, however many wake-ups accumulated. `_get_event_position` and the
dead `position` parameter on `_process_live_event` are deleted.

### Rejected alternative: attach `Position` to bus-delivered events

Publishers could stamp a position onto the event before publishing to the
bus. Rejected: it would require every one of the four bus backends
(`InMemoryEventBus`, `RedisEventBus`, `RabbitMQEventBus`, `KafkaEventBus`)
to either carry a store-assigned position through wire serialization (none
do today, and their native offsets are not `Position` tokens per ADR 0019)
or have every append call site cooperate with every publish call site to
staple one on after the fact — coupling the bus port to a store concern
ADR 0007 deliberately keeps out of it. Reading the feed keeps `Position`
where ADR 0019 already put it: adapter-owned, opaque, and never derived
from anything but the store itself.

## Consequences

**Positive**

- Live subscriptions checkpoint correctly. A subscription live for an
  arbitrary duration resumes from where it actually left off, not from the
  catch-up watermark.
- The duplicate-suppression dead code and its permanently-zero metric are
  gone instead of merely documented as broken.
- `LiveRunner` depends on the same narrow `GlobalEventFeed` port
  `CatchUpRunner` already used — no new port, no widened dependency.

**Negative**

- **Breaking**: `LiveRunner.__init__` gains a required `event_feed`
  keyword argument. Any caller constructing `LiveRunner` directly (rather
  than through `TransitionCoordinator`, which now supplies it from its own
  `event_store`) must pass one.
- `TransitionResult.buffer_events_skipped` and
  `LiveRunnerStats.events_skipped_duplicate` are removed. Any caller
  reading either field breaks; per the pre-1.0 NO-SHIMS policy there is no
  deprecated alias.
- A drain now reads through the feed on every wake-up rather than acting on
  the bus payload directly, adding one store round-trip per notification
  batch. For the adapters in tree this is the same access pattern
  `CatchUpRunner` already uses at higher volume, so it is not expected to
  be a new bottleneck, but it was not benchmarked as part of this change.

**Neutral**

- The bus's own delivery contract (ADR 0007: at-least-once across brokers,
  per-handler isolation, no ordering guarantee) is unchanged for every
  other consumer. This ADR only changes what `LiveRunner` — one specific
  consumer — uses the bus *for*.
