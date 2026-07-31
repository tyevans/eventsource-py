"""Unit tests for RecordingEventBus."""

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

    events = [RecordedEvent(aggregate_id=uuid4()) for _ in range(10)]
    for event in events:
        await bus.publish([event])

    retained_ids = [e.event_id for e in bus.published_events]
    assert retained_ids == [e.event_id for e in events[-3:]]


async def test_max_events_none_means_unbounded() -> None:
    bus = RecordingEventBus(InMemoryEventBus(), max_events=None)

    for _ in range(50):
        await bus.publish([RecordedEvent(aggregate_id=uuid4())])

    assert len(bus.published_events) == 50


async def test_clear_subscribers_delegates_to_wrapped_bus() -> None:
    inner = InMemoryEventBus()
    bus = RecordingEventBus(inner)

    async def handler(event: DomainEvent) -> None:
        pass

    bus.subscribe(RecordedEvent, handler)
    assert bus.get_subscriber_count(RecordedEvent) == 1

    bus.clear_subscribers()

    assert bus.get_subscriber_count(RecordedEvent) == 0


async def test_get_subscriber_count_delegates_to_wrapped_bus() -> None:
    inner = InMemoryEventBus()
    bus = RecordingEventBus(inner)

    async def handler(event: DomainEvent) -> None:
        pass

    assert bus.get_subscriber_count() == 0
    bus.subscribe(RecordedEvent, handler)
    assert bus.get_subscriber_count() == 1
    assert bus.get_subscriber_count(RecordedEvent) == 1


async def test_get_wildcard_subscriber_count_delegates_to_wrapped_bus() -> None:
    inner = InMemoryEventBus()
    bus = RecordingEventBus(inner)

    async def handler(event: DomainEvent) -> None:
        pass

    assert bus.get_wildcard_subscriber_count() == 0
    bus.subscribe_to_all_events(handler)
    assert bus.get_wildcard_subscriber_count() == 1
