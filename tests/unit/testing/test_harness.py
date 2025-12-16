"""
Tests for the InMemoryTestHarness class.

These tests verify that the InMemoryTestHarness provides all necessary
infrastructure for testing event-sourced applications.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.repositories.dlq import InMemoryDLQRepository
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.testing import InMemoryTestHarness


class SampleEvent(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Sample"
    event_type: str = "SampleEvent"


class OtherEvent(DomainEvent):
    """Another event type for testing filtering."""

    aggregate_type: str = "Other"
    event_type: str = "OtherEvent"


class TestHarnessInstantiation:
    """Tests for harness instantiation."""

    def test_harness_can_be_created(self) -> None:
        """InMemoryTestHarness can be instantiated."""
        harness = InMemoryTestHarness()
        assert harness is not None

    def test_harness_creates_all_components(self) -> None:
        """Harness creates all required components on instantiation."""
        harness = InMemoryTestHarness()

        assert harness._event_store is not None
        assert harness._event_bus is not None
        assert harness._checkpoint_repo is not None
        assert harness._dlq_repo is not None


class TestHarnessProperties:
    """Tests for harness property accessors."""

    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        """Create a fresh harness for each test."""
        return InMemoryTestHarness()

    def test_event_store_type(self, harness: InMemoryTestHarness) -> None:
        """event_store returns InMemoryEventStore."""
        assert isinstance(harness.event_store, InMemoryEventStore)

    def test_event_bus_type(self, harness: InMemoryTestHarness) -> None:
        """event_bus returns InMemoryEventBus."""
        assert isinstance(harness.event_bus, InMemoryEventBus)

    def test_checkpoint_repo_type(self, harness: InMemoryTestHarness) -> None:
        """checkpoint_repo returns InMemoryCheckpointRepository."""
        assert isinstance(harness.checkpoint_repo, InMemoryCheckpointRepository)

    def test_dlq_repo_type(self, harness: InMemoryTestHarness) -> None:
        """dlq_repo returns InMemoryDLQRepository."""
        assert isinstance(harness.dlq_repo, InMemoryDLQRepository)

    def test_same_event_store_on_multiple_access(self, harness: InMemoryTestHarness) -> None:
        """event_store returns same instance on repeated access."""
        assert harness.event_store is harness.event_store

    def test_same_event_bus_on_multiple_access(self, harness: InMemoryTestHarness) -> None:
        """event_bus returns same instance on repeated access."""
        assert harness.event_bus is harness.event_bus

    def test_same_checkpoint_repo_on_multiple_access(self, harness: InMemoryTestHarness) -> None:
        """checkpoint_repo returns same instance on repeated access."""
        assert harness.checkpoint_repo is harness.checkpoint_repo

    def test_same_dlq_repo_on_multiple_access(self, harness: InMemoryTestHarness) -> None:
        """dlq_repo returns same instance on repeated access."""
        assert harness.dlq_repo is harness.dlq_repo


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
        event = SampleEvent(
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
        event1 = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        event2 = SampleEvent(
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
        event1 = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        event2 = SampleEvent(
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
        event = SampleEvent(
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
        event = SampleEvent(
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

        harness.event_bus.subscribe(SampleEvent, handler)

        # Publish and clear
        event1 = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event1])
        harness.clear_published_events()

        # Subscriptions should still work
        event2 = SampleEvent(
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
        event = SampleEvent(
            aggregate_id=aggregate_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        # Store event
        await harness.event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Sample",
            events=[event],
            expected_version=0,
        )
        await harness.event_bus.publish([event])

        harness.clear_published_events()

        # Event store should still have the event
        stream = await harness.event_store.get_events(aggregate_id, "Sample")
        assert len(stream.events) == 1


class TestHarnessReset:
    """Tests for reset functionality."""

    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        """Create a fresh harness for each test."""
        return InMemoryTestHarness()

    @pytest.mark.asyncio
    async def test_reset_creates_new_event_store(self, harness: InMemoryTestHarness) -> None:
        """reset() creates a new event store instance."""
        old_store = harness.event_store
        aggregate_id = uuid4()
        event = SampleEvent(
            aggregate_id=aggregate_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await old_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Sample",
            events=[event],
            expected_version=0,
        )

        harness.reset()

        assert harness.event_store is not old_store
        # New store should be empty
        event_count = await harness.event_store.get_event_count()
        assert event_count == 0

    @pytest.mark.asyncio
    async def test_reset_creates_new_event_bus(self, harness: InMemoryTestHarness) -> None:
        """reset() creates a new event bus instance."""
        old_bus = harness.event_bus
        event = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await old_bus.publish([event])

        harness.reset()

        assert harness.event_bus is not old_bus
        assert harness.published_events == []

    def test_reset_creates_new_checkpoint_repo(self, harness: InMemoryTestHarness) -> None:
        """reset() creates a new checkpoint repository."""
        old_repo = harness.checkpoint_repo
        harness.reset()
        assert harness.checkpoint_repo is not old_repo

    def test_reset_creates_new_dlq_repo(self, harness: InMemoryTestHarness) -> None:
        """reset() creates a new DLQ repository."""
        old_repo = harness.dlq_repo
        harness.reset()
        assert harness.dlq_repo is not old_repo

    @pytest.mark.asyncio
    async def test_reset_clears_checkpoint_data(self, harness: InMemoryTestHarness) -> None:
        """reset() clears checkpoint repository data."""
        event_id = uuid4()
        await harness.checkpoint_repo.save_position(
            subscription_id="test-projection",
            position=42,
            event_id=event_id,
            event_type="TestEvent",
        )

        harness.reset()

        position = await harness.checkpoint_repo.get_position("test-projection")
        assert position is None

    @pytest.mark.asyncio
    async def test_reset_clears_dlq_data(self, harness: InMemoryTestHarness) -> None:
        """reset() clears DLQ repository data."""
        await harness.dlq_repo.add_failed_event(
            event_id=uuid4(),
            projection_name="test-projection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Test error"),
        )

        harness.reset()

        entries = await harness.dlq_repo.get_failed_events()
        assert len(entries) == 0


class TestHarnessTracingDisabled:
    """Tests for tracing configuration."""

    def test_event_bus_tracing_disabled(self) -> None:
        """Event bus is created with tracing disabled."""
        harness = InMemoryTestHarness()
        assert harness.event_bus._enable_tracing is False

    def test_event_store_tracing_disabled(self) -> None:
        """Event store is created with tracing disabled."""
        harness = InMemoryTestHarness()
        assert harness.event_store._enable_tracing is False

    def test_checkpoint_repo_tracing_disabled(self) -> None:
        """Checkpoint repo is created with tracing disabled."""
        harness = InMemoryTestHarness()
        assert harness.checkpoint_repo._enable_tracing is False

    def test_dlq_repo_tracing_disabled(self) -> None:
        """DLQ repo is created with tracing disabled."""
        harness = InMemoryTestHarness()
        assert harness.dlq_repo._enable_tracing is False


class TestHarnessGetEventsOfType:
    """Tests for get_events_of_type functionality."""

    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        """Create a fresh harness for each test."""
        return InMemoryTestHarness()

    def test_get_events_of_type_empty_initially(self, harness: InMemoryTestHarness) -> None:
        """get_events_of_type returns empty list initially."""
        result = harness.get_events_of_type(SampleEvent)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_events_of_type_filters_correctly(self, harness: InMemoryTestHarness) -> None:
        """get_events_of_type filters events by type."""
        sample_event = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        other_event = OtherEvent(
            aggregate_id=uuid4(),
            aggregate_type="Other",
            aggregate_version=1,
        )
        await harness.event_bus.publish([sample_event, other_event])

        sample_events = harness.get_events_of_type(SampleEvent)
        other_events = harness.get_events_of_type(OtherEvent)

        assert len(sample_events) == 1
        assert sample_events[0] == sample_event
        assert len(other_events) == 1
        assert other_events[0] == other_event

    @pytest.mark.asyncio
    async def test_get_events_of_type_returns_all_matching(
        self, harness: InMemoryTestHarness
    ) -> None:
        """get_events_of_type returns all events of the requested type."""
        event1 = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        event2 = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        event3 = OtherEvent(
            aggregate_id=uuid4(),
            aggregate_type="Other",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event1, event2, event3])

        sample_events = harness.get_events_of_type(SampleEvent)

        assert len(sample_events) == 2
        assert sample_events[0] == event1
        assert sample_events[1] == event2

    @pytest.mark.asyncio
    async def test_get_events_of_type_with_no_matches(self, harness: InMemoryTestHarness) -> None:
        """get_events_of_type returns empty list when no matches."""
        event = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event])

        result = harness.get_events_of_type(OtherEvent)

        assert result == []


class TestHarnessRepr:
    """Tests for __repr__ method."""

    def test_repr_shows_counts_initially(self) -> None:
        """__repr__ shows event counts initially."""
        harness = InMemoryTestHarness()
        repr_str = repr(harness)

        assert "InMemoryTestHarness" in repr_str
        assert "published=" in repr_str
        assert "published=0" in repr_str

    @pytest.mark.asyncio
    async def test_repr_shows_published_count(self) -> None:
        """__repr__ shows correct published event count."""
        harness = InMemoryTestHarness()
        event = SampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await harness.event_bus.publish([event])

        repr_str = repr(harness)

        assert "published=1" in repr_str


class TestHarnessIntegration:
    """Integration tests verifying components work together."""

    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        """Create a fresh harness for each test."""
        return InMemoryTestHarness()

    @pytest.mark.asyncio
    async def test_event_store_and_bus_work_together(self, harness: InMemoryTestHarness) -> None:
        """Event store and bus can be used together."""
        aggregate_id = uuid4()
        event = SampleEvent(
            aggregate_id=aggregate_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        # Store event
        result = await harness.event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Sample",
            events=[event],
            expected_version=0,
        )
        assert result.success

        # Publish event
        await harness.event_bus.publish([event])

        # Both should reflect the event
        stream = await harness.event_store.get_events(aggregate_id, "Sample")
        assert len(stream.events) == 1
        assert len(harness.published_events) == 1

    @pytest.mark.asyncio
    async def test_checkpoint_repo_tracks_positions(self, harness: InMemoryTestHarness) -> None:
        """Checkpoint repository tracks positions correctly."""
        event_id = uuid4()

        await harness.checkpoint_repo.save_position(
            subscription_id="my-projection",
            position=100,
            event_id=event_id,
            event_type="SampleEvent",
        )

        position = await harness.checkpoint_repo.get_position("my-projection")
        assert position == 100

    @pytest.mark.asyncio
    async def test_dlq_repo_stores_failures(self, harness: InMemoryTestHarness) -> None:
        """DLQ repository stores failed events."""
        event_id = uuid4()

        await harness.dlq_repo.add_failed_event(
            event_id=event_id,
            projection_name="my-projection",
            event_type="SampleEvent",
            event_data={"key": "value"},
            error=ValueError("Test error"),
            retry_count=3,
        )

        entries = await harness.dlq_repo.get_failed_events("my-projection")
        assert len(entries) == 1
        assert entries[0].event_id == event_id
        assert entries[0].retry_count == 3

    @pytest.mark.asyncio
    async def test_multiple_aggregates_isolated(self, harness: InMemoryTestHarness) -> None:
        """Multiple aggregates are properly isolated in event store."""
        agg1_id = uuid4()
        agg2_id = uuid4()

        event1 = SampleEvent(
            aggregate_id=agg1_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )
        event2 = SampleEvent(
            aggregate_id=agg2_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )

        await harness.event_store.append_events(
            aggregate_id=agg1_id,
            aggregate_type="Sample",
            events=[event1],
            expected_version=0,
        )
        await harness.event_store.append_events(
            aggregate_id=agg2_id,
            aggregate_type="Sample",
            events=[event2],
            expected_version=0,
        )

        stream1 = await harness.event_store.get_events(agg1_id, "Sample")
        stream2 = await harness.event_store.get_events(agg2_id, "Sample")

        assert len(stream1.events) == 1
        assert len(stream2.events) == 1
        assert stream1.events[0].aggregate_id == agg1_id
        assert stream2.events[0].aggregate_id == agg2_id
