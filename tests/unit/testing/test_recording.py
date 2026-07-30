"""Unit tests for RecordingEventBus."""

import warnings
from uuid import uuid4

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.testing.recording import RecordingEventBus


class RecordedEvent(DomainEvent):
    event_type: str = "RecordedEvent"
    aggregate_type: str = "Recorded"


async def test_records_published_events_and_forwards_them() -> None:
    inner = InMemoryEventBus()
    bus = RecordingEventBus(inner)

    received: list[DomainEvent] = []

    async def handler(event: DomainEvent) -> None:
        received.append(event)

    bus.subscribe(RecordedEvent, handler)
    event = RecordedEvent(aggregate_id=uuid4())
    await bus.publish([event])

    assert [e.event_id for e in bus.published_events] == [event.event_id]
    assert [e.event_id for e in received] == [event.event_id]


async def test_published_events_returns_a_copy() -> None:
    bus = RecordingEventBus(InMemoryEventBus())
    await bus.publish([RecordedEvent(aggregate_id=uuid4())])

    snapshot = bus.published_events
    snapshot.clear()

    assert len(bus.published_events) == 1


async def test_clear_published_events_empties_the_log() -> None:
    bus = RecordingEventBus(InMemoryEventBus())
    await bus.publish([RecordedEvent(aggregate_id=uuid4())])

    bus.clear_published_events()

    assert bus.published_events == []


async def test_max_events_bounds_memory() -> None:
    """The unbounded list on InMemoryEventBus leaked in long-lived processes."""
    bus = RecordingEventBus(InMemoryEventBus(), max_events=3)

    for _ in range(10):
        await bus.publish([RecordedEvent(aggregate_id=uuid4())])

    assert len(bus.published_events) == 3


async def test_max_events_none_means_unbounded() -> None:
    bus = RecordingEventBus(InMemoryEventBus(), max_events=None)

    for _ in range(50):
        await bus.publish([RecordedEvent(aggregate_id=uuid4())])

    assert len(bus.published_events) == 50


async def test_in_memory_published_events_warns_but_still_works() -> None:
    bus = InMemoryEventBus()
    await bus.publish([RecordedEvent(aggregate_id=uuid4())])

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        events = bus.published_events

    assert len(events) == 1
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)
