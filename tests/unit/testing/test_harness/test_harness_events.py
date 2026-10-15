"""Tests for test harness published events tracking, clearing, and filtering."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion
from eventsource.testing import InMemoryTestHarness
from tests.unit.testing.test_harness._fixtures import (
    HarnessOtherEvent,
    HarnessSampleEvent,
)


class TestHarnessPublishedEvents:
    """Tests for published_events functionality."""

    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        """Create a fresh harness for each test."""
        return InMemoryTestHarness()

    def test_published_events_initially_empty(self, harness: InMemoryTestHarness) -> None:
        """published_events is empty initially."""
        assert harness.published_events == []

    @pytest.mark.asyncio
    async def test_published_events_tracks_single_event(self, harness: InMemoryTestHarness) -> None:
        """published_events reflects a single event published to bus."""
        event = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event])

        assert len(harness.published_events) == 1
        assert harness.published_events[0] == event

    @pytest.mark.asyncio
    async def test_published_events_tracks_multiple_events(
        self, harness: InMemoryTestHarness
    ) -> None:
        """published_events reflects multiple events published to bus."""
        event1 = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        event2 = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event1, event2])

        assert len(harness.published_events) == 2
        assert harness.published_events[0] == event1
        assert harness.published_events[1] == event2

    @pytest.mark.asyncio
    async def test_published_events_tracks_events_across_multiple_publishes(
        self, harness: InMemoryTestHarness
    ) -> None:
        """published_events accumulates events from multiple publish calls."""
        event1 = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        event2 = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event1])
        await harness.event_bus.publish([event2])

        assert len(harness.published_events) == 2

    @pytest.mark.asyncio
    async def test_published_events_returns_copy(self, harness: InMemoryTestHarness) -> None:
        """published_events returns a copy to prevent external mutation."""
        event = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event])

        # Get the list and try to modify it
        events = harness.published_events
        events.clear()

        # Original should be unchanged
        assert len(harness.published_events) == 1


class TestHarnessClearPublishedEvents:
    """Tests for clear_published_events functionality."""

    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        """Create a fresh harness for each test."""
        return InMemoryTestHarness()

    @pytest.mark.asyncio
    async def test_clear_published_events_clears_list(self, harness: InMemoryTestHarness) -> None:
        """clear_published_events clears all published events."""
        event = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event])
        assert len(harness.published_events) == 1

        harness.clear_published_events()

        assert harness.published_events == []

    @pytest.mark.asyncio
    async def test_clear_published_events_preserves_subscriptions(
        self, harness: InMemoryTestHarness
    ) -> None:
        """clear_published_events preserves event bus subscriptions."""
        events_received: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            events_received.append(event)

        harness.event_bus.subscribe(HarnessSampleEvent, handler)

        # Publish and clear
        event1 = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event1])
        harness.clear_published_events()

        # Subscriptions should still work
        event2 = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event2])

        assert len(events_received) == 2

    @pytest.mark.asyncio
    async def test_clear_published_events_preserves_event_store_state(
        self, harness: InMemoryTestHarness
    ) -> None:
        """clear_published_events does not affect event store."""
        aggregate_id = uuid4()
        event = HarnessSampleEvent(
            aggregate_id=aggregate_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        # Store event
        await harness.event_store.append(
            StreamId(aggregate_id=aggregate_id, category="Sample"),
            [event],
            ExpectedVersion.no_stream(),
        )
        await harness.event_bus.publish([event])

        harness.clear_published_events()

        # Event store should still have the event
        envelopes = [
            e
            async for e in harness.event_store.read_stream(
                StreamId(aggregate_id=aggregate_id, category="Sample")
            )
        ]
        assert len(envelopes) == 1


class TestHarnessGetEventsOfType:
    """Tests for get_events_of_type functionality."""

    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        """Create a fresh harness for each test."""
        return InMemoryTestHarness()

    def test_get_events_of_type_empty_initially(self, harness: InMemoryTestHarness) -> None:
        """get_events_of_type returns empty list initially."""
        result = harness.get_events_of_type(HarnessSampleEvent)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_events_of_type_filters_correctly(self, harness: InMemoryTestHarness) -> None:
        """get_events_of_type filters events by type."""
        sample_event = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        other_event = HarnessOtherEvent(
            aggregate_id=uuid4(),
            aggregate_type="Other",
            aggregate_version=1,
        )
        await harness.event_bus.publish([sample_event, other_event])

        sample_events = harness.get_events_of_type(HarnessSampleEvent)
        other_events = harness.get_events_of_type(HarnessOtherEvent)

        assert len(sample_events) == 1
        assert sample_events[0] == sample_event
        assert len(other_events) == 1
        assert other_events[0] == other_event

    @pytest.mark.asyncio
    async def test_get_events_of_type_returns_all_matching(
        self, harness: InMemoryTestHarness
    ) -> None:
        """get_events_of_type returns all events of the requested type."""
        event1 = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        event2 = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        event3 = HarnessOtherEvent(
            aggregate_id=uuid4(),
            aggregate_type="Other",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event1, event2, event3])

        sample_events = harness.get_events_of_type(HarnessSampleEvent)

        assert len(sample_events) == 2
        assert sample_events[0] == event1
        assert sample_events[1] == event2

    @pytest.mark.asyncio
    async def test_get_events_of_type_with_no_matches(self, harness: InMemoryTestHarness) -> None:
        """get_events_of_type returns empty list when no matches."""
        event = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event])

        result = harness.get_events_of_type(HarnessOtherEvent)

        assert result == []

    @pytest.mark.asyncio
    async def test_published_of_type_delegates_to_bus(self, harness: InMemoryTestHarness) -> None:
        """published_of_type delegates to event_bus and matches get_events_of_type."""
        sample_event = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        other_event = HarnessOtherEvent(
            aggregate_id=uuid4(),
            aggregate_type="Other",
            aggregate_version=1,
        )
        await harness.event_bus.publish([sample_event, other_event])

        assert harness.published_of_type(HarnessSampleEvent) == [sample_event]
        assert harness.published_of_type(HarnessOtherEvent) == [other_event]
        assert harness.published_of_type(HarnessSampleEvent) == harness.get_events_of_type(
            HarnessSampleEvent
        )
        assert harness.event_bus.get_wildcard_subscriber_count() == 0
        harness.clear_published_events()
        assert harness.event_bus.published_events == []
