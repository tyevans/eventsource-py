"""
Unit tests for the bound on background publish tasks.

Covers task #3. `BaseEventBus._track_background` created an `asyncio.Task` per
`publish(background=True)` call with no ceiling, so a producer faster than its
handlers grew the in-flight task set without limit -- the same negative ADR
0021 and ADR 0017 record for background snapshot scheduling, in a second place.
`_drain_background` then had to wait for (or cancel) all of it at shutdown.

The bound degrades to **inline** publishing at capacity rather than blocking on
a slot. That choice is load-bearing, not incidental:

- It cannot deadlock. A handler running inside a background publish task may
  itself publish; if that inner call had to wait for a slot, it would wait for
  one held by the very task it is running inside. Running inline completes
  instead of waiting, so no re-entrancy guard (contextvars or otherwise) is
  needed to make it safe.
- It loses nothing. Dropping at capacity would silently discard events, which
  is not a tradeoff an event bus gets to make.
- It is real backpressure: a producer outrunning its handlers is slowed to the
  rate handlers can absorb, which is the point of a bound.
"""

import asyncio

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event

_REGISTRY = EventRegistry()

BOUND = 5.0


@register_event(registry=_REGISTRY)
class BoundTestEvent(DomainEvent):
    """Simple test event for background-bound tests."""

    aggregate_type: str = "BoundAggregate"
    data: str = "test"


def make_event(data: str = "e") -> BoundTestEvent:
    from uuid import uuid4

    return BoundTestEvent(aggregate_id=uuid4(), data=data)


class TestBackgroundTaskBound:
    async def test_background_tasks_are_capped(self) -> None:
        """Publishing far more than the cap never grows the in-flight task set
        beyond it."""
        bus = InMemoryEventBus(max_background_tasks=4)
        release = asyncio.Event()
        peak = 0

        async def slow_handler(event: DomainEvent) -> None:
            await release.wait()

        bus.subscribe(BoundTestEvent, slow_handler)

        publishes = [
            asyncio.create_task(bus.publish([make_event()], background=True)) for _ in range(20)
        ]
        # Let the scheduler run so every publish that *can* start has.
        for _ in range(10):
            await asyncio.sleep(0)
            peak = max(peak, bus.get_background_task_count())

        assert peak <= 4, f"in-flight background tasks reached {peak}, cap was 4"

        release.set()
        await asyncio.wait_for(asyncio.gather(*publishes), timeout=BOUND)
        await bus.shutdown()

    async def test_no_events_are_lost_at_capacity(self) -> None:
        """Degrading to inline must deliver every event -- a bound that drops
        would pass a "bounded" assertion while silently losing data."""
        bus = InMemoryEventBus(max_background_tasks=2)
        seen: list[str] = []

        async def recording_handler(event: DomainEvent) -> None:
            assert isinstance(event, BoundTestEvent)
            seen.append(event.data)

        bus.subscribe(BoundTestEvent, recording_handler)

        for i in range(25):
            await bus.publish([make_event(f"e{i}")], background=True)

        await bus.shutdown()

        assert sorted(seen) == sorted(f"e{i}" for i in range(25))

    async def test_reentrant_publish_at_capacity_does_not_deadlock(self) -> None:
        """A handler that itself publishes, while the bus is at capacity, must
        complete. This is the case a blocking semaphore would deadlock on: the
        inner publish would wait for a slot held by the task it runs inside.
        """
        bus = InMemoryEventBus(max_background_tasks=1)
        inner_seen: list[str] = []

        async def inner_handler(event: DomainEvent) -> None:
            assert isinstance(event, BoundTestEvent)
            inner_seen.append(event.data)

        async def republishing_handler(event: DomainEvent) -> None:
            assert isinstance(event, BoundTestEvent)
            if event.data.startswith("outer"):
                await bus.publish([make_event("inner")], background=True)

        bus.subscribe(BoundTestEvent, republishing_handler)
        bus.subscribe(BoundTestEvent, inner_handler)

        async with asyncio.timeout(BOUND):
            for i in range(5):
                await bus.publish([make_event(f"outer{i}")], background=True)
            await bus.shutdown()

        assert inner_seen, "the re-entrant publish never reached its handler"

    async def test_unbounded_by_default_is_not_the_behavior(self) -> None:
        """The cap has a real default -- an opt-in bound nobody sets is the
        inert-config defect this whole wave was about."""
        bus = InMemoryEventBus()
        assert bus.max_background_tasks is not None
        assert bus.max_background_tasks > 0
        await bus.shutdown()


class TestExplicitlyUnbounded:
    async def test_none_disables_the_cap(self) -> None:
        """`None` is still available for callers who genuinely want the old
        fire-and-forget behavior."""
        bus = InMemoryEventBus(max_background_tasks=None)
        release = asyncio.Event()

        async def slow_handler(event: DomainEvent) -> None:
            await release.wait()

        bus.subscribe(BoundTestEvent, slow_handler)

        for _ in range(12):
            await bus.publish([make_event()], background=True)

        assert bus.get_background_task_count() == 12

        release.set()
        await bus.shutdown()
