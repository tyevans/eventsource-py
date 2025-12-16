"""
Unit tests for InMemoryEventBus published_events functionality.

Tests the published_events property and clear_published_events method
added to InMemoryEventBus for testing purposes.
"""

import threading
from uuid import uuid4

import pytest

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent

# =============================================================================
# Test Event Classes
# =============================================================================


class SampleEvent(DomainEvent):
    """Test event for published_events tests."""

    event_type: str = "SampleEvent"
    aggregate_type: str = "Sample"


class AnotherEvent(DomainEvent):
    """Another test event for order preservation tests."""

    event_type: str = "AnotherEvent"
    aggregate_type: str = "Another"


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def bus() -> InMemoryEventBus:
    """Create a fresh event bus for testing."""
    return InMemoryEventBus()


@pytest.fixture
def sample_event() -> SampleEvent:
    """Create a sample event for testing."""
    return SampleEvent(
        aggregate_id=uuid4(),
        aggregate_version=1,
    )


# =============================================================================
# Test Published Events Property
# =============================================================================


class TestPublishedEvents:
    """Tests for published_events property."""

    def test_empty_initially(self, bus: InMemoryEventBus) -> None:
        """published_events is empty before any publishing."""
        assert bus.published_events == []

    @pytest.mark.asyncio
    async def test_tracks_published_events(
        self, bus: InMemoryEventBus, sample_event: SampleEvent
    ) -> None:
        """published_events contains all published events."""
        await bus.publish([sample_event])
        assert len(bus.published_events) == 1
        assert bus.published_events[0] == sample_event

    @pytest.mark.asyncio
    async def test_tracks_multiple_events(self, bus: InMemoryEventBus) -> None:
        """published_events contains multiple published events."""
        events = [SampleEvent(aggregate_id=uuid4(), aggregate_version=i) for i in range(1, 4)]
        await bus.publish(events)
        assert len(bus.published_events) == 3
        for i, event in enumerate(events):
            assert bus.published_events[i] == event

    @pytest.mark.asyncio
    async def test_preserves_order(self, bus: InMemoryEventBus) -> None:
        """published_events maintains publication order."""
        events = [SampleEvent(aggregate_id=uuid4(), aggregate_version=i) for i in range(1, 6)]
        await bus.publish(events)
        assert bus.published_events == events

    @pytest.mark.asyncio
    async def test_preserves_order_across_publish_calls(self, bus: InMemoryEventBus) -> None:
        """published_events maintains order across multiple publish calls."""
        event1 = SampleEvent(aggregate_id=uuid4(), aggregate_version=1)
        event2 = AnotherEvent(aggregate_id=uuid4(), aggregate_version=1)
        event3 = SampleEvent(aggregate_id=uuid4(), aggregate_version=2)

        await bus.publish([event1])
        await bus.publish([event2])
        await bus.publish([event3])

        assert bus.published_events == [event1, event2, event3]

    @pytest.mark.asyncio
    async def test_returns_copy(self, bus: InMemoryEventBus, sample_event: SampleEvent) -> None:
        """published_events returns a copy, not the internal list."""
        await bus.publish([sample_event])
        events = bus.published_events
        events.clear()
        # Internal list should be unchanged
        assert len(bus.published_events) == 1

    @pytest.mark.asyncio
    async def test_returns_copy_mutation_does_not_affect_internal(
        self, bus: InMemoryEventBus, sample_event: SampleEvent
    ) -> None:
        """Modifying returned list does not affect internal state."""
        await bus.publish([sample_event])
        events = bus.published_events
        extra_event = SampleEvent(aggregate_id=uuid4(), aggregate_version=99)
        events.append(extra_event)
        # Internal list should not contain the appended event
        assert len(bus.published_events) == 1
        assert extra_event not in bus.published_events


# =============================================================================
# Test Clear Published Events
# =============================================================================


class TestClearPublishedEvents:
    """Tests for clear_published_events method."""

    @pytest.mark.asyncio
    async def test_clear_published_events(
        self, bus: InMemoryEventBus, sample_event: SampleEvent
    ) -> None:
        """clear_published_events empties the list."""
        await bus.publish([sample_event])
        assert len(bus.published_events) == 1

        bus.clear_published_events()
        assert bus.published_events == []

    def test_clear_on_empty_bus(self, bus: InMemoryEventBus) -> None:
        """clear_published_events works on empty bus."""
        bus.clear_published_events()
        assert bus.published_events == []

    @pytest.mark.asyncio
    async def test_clear_allows_reuse(self, bus: InMemoryEventBus) -> None:
        """Bus can publish events again after clearing."""
        event1 = SampleEvent(aggregate_id=uuid4(), aggregate_version=1)
        event2 = SampleEvent(aggregate_id=uuid4(), aggregate_version=2)

        await bus.publish([event1])
        bus.clear_published_events()
        await bus.publish([event2])

        assert len(bus.published_events) == 1
        assert bus.published_events[0] == event2


# =============================================================================
# Test Thread Safety
# =============================================================================


class TestPublishedEventsThreadSafety:
    """Tests for thread safety of published_events operations."""

    @pytest.mark.asyncio
    async def test_thread_safe_reading(self, bus: InMemoryEventBus) -> None:
        """Property access is thread-safe when reading."""
        results: list[int] = []

        def read_events() -> None:
            for _ in range(100):
                results.append(len(bus.published_events))

        async def publish_events() -> None:
            for i in range(100):
                event = SampleEvent(
                    aggregate_id=uuid4(),
                    aggregate_version=i + 1,
                )
                await bus.publish([event])

        reader = threading.Thread(target=read_events)
        reader.start()
        await publish_events()
        reader.join()

        # No exceptions = thread-safe
        assert len(results) == 100

    def test_thread_safe_clear(self, bus: InMemoryEventBus) -> None:
        """clear_published_events is thread-safe."""
        results: list[bool] = []

        def clear_events() -> None:
            for _ in range(50):
                bus.clear_published_events()
                results.append(True)

        threads = [threading.Thread(target=clear_events) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # No exceptions = thread-safe
        assert len(results) == 200


# =============================================================================
# Test Integration with Handler Execution
# =============================================================================


class TestPublishedEventsWithHandlers:
    """Tests for published_events with event handlers."""

    @pytest.mark.asyncio
    async def test_events_tracked_before_handler_execution(
        self, bus: InMemoryEventBus, sample_event: SampleEvent
    ) -> None:
        """Events are tracked even if no handlers are registered."""
        # No handlers registered
        await bus.publish([sample_event])
        assert len(bus.published_events) == 1
        assert bus.published_events[0] == sample_event

    @pytest.mark.asyncio
    async def test_events_tracked_when_handler_fails(
        self, bus: InMemoryEventBus, sample_event: SampleEvent
    ) -> None:
        """Events are tracked even if handler raises exception."""

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Handler failed")

        bus.subscribe(SampleEvent, failing_handler)
        await bus.publish([sample_event])

        # Event should still be tracked
        assert len(bus.published_events) == 1
        assert bus.published_events[0] == sample_event

    @pytest.mark.asyncio
    async def test_events_tracked_with_successful_handler(
        self, bus: InMemoryEventBus, sample_event: SampleEvent
    ) -> None:
        """Events are tracked when handler succeeds."""
        handled_events: list[DomainEvent] = []

        async def recording_handler(event: DomainEvent) -> None:
            handled_events.append(event)

        bus.subscribe(SampleEvent, recording_handler)
        await bus.publish([sample_event])

        # Both tracking mechanisms should work
        assert len(bus.published_events) == 1
        assert len(handled_events) == 1
        assert bus.published_events[0] == sample_event
        assert handled_events[0] == sample_event


# =============================================================================
# Test Background Publishing
# =============================================================================


class TestPublishedEventsWithBackgroundPublishing:
    """Tests for published_events with background publishing."""

    @pytest.mark.asyncio
    async def test_background_events_tracked(
        self, bus: InMemoryEventBus, sample_event: SampleEvent
    ) -> None:
        """Events are tracked when published in background."""
        import asyncio

        await bus.publish([sample_event], background=True)

        # Wait for background task to complete
        await asyncio.sleep(0.1)

        assert len(bus.published_events) == 1
        assert bus.published_events[0] == sample_event
