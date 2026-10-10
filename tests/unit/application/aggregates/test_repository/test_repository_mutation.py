"""Tests for saving aggregates, optimistic concurrency, and event publishing."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain import StreamId
from eventsource.domain.exceptions import OptimisticLockError
from eventsource.ports import collect
from tests.unit.application.aggregates.test_repository._models import (
    CounterAggregate,
    CounterIncremented,
    MockEventPublisher,
)


class TestSaveAggregate:
    """Tests for saving aggregates."""

    @pytest.mark.asyncio
    async def test_save_new_aggregate(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """save() should persist uncommitted events."""
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)
        aggregate.increment(3)

        await counter_repository.save(aggregate)

        # Verify events were persisted
        stream_id = StreamId(aggregate_id=agg_id, category="Counter")
        events = await collect(event_store.read_stream(stream_id))
        assert len(events) == 2
        assert await event_store.get_stream_version(stream_id) == 2

    @pytest.mark.asyncio
    async def test_save_clears_uncommitted_events(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """save() should mark events as committed."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)

        assert aggregate.has_uncommitted_events

        await counter_repository.save(aggregate)

        assert not aggregate.has_uncommitted_events
        assert len(aggregate.uncommitted_events) == 0

    @pytest.mark.asyncio
    async def test_save_preserves_aggregate_state(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """save() should not affect aggregate state."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(10)

        state_before = aggregate.state
        version_before = aggregate.version

        await counter_repository.save(aggregate)

        assert aggregate.state == state_before
        assert aggregate.version == version_before

    @pytest.mark.asyncio
    async def test_save_no_op_when_no_uncommitted_events(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """save() should be no-op when there are no uncommitted events."""
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)

        # Save without any changes
        await counter_repository.save(aggregate)

        # Event store should have no events
        stream_id = StreamId(aggregate_id=agg_id, category="Counter")
        events = await collect(event_store.read_stream(stream_id))
        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_save_existing_aggregate_with_new_events(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """save() should append new events to existing aggregate."""
        agg_id = uuid4()

        # Create and save initial aggregate
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)
        await counter_repository.save(aggregate)

        # Load, modify, and save again
        loaded = await counter_repository.load(agg_id)
        loaded.increment(3)
        await counter_repository.save(loaded)

        # Verify all events are persisted
        stream_id = StreamId(aggregate_id=agg_id, category="Counter")
        events = await collect(event_store.read_stream(stream_id))
        assert len(events) == 2
        assert await event_store.get_stream_version(stream_id) == 2


class TestOptimisticConcurrency:
    """Tests for optimistic concurrency control."""

    @pytest.mark.asyncio
    async def test_concurrent_modification_raises_error(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """Concurrent modification should raise OptimisticLockError."""
        agg_id = uuid4()

        # Create initial aggregate
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)
        await counter_repository.save(aggregate)

        # Load aggregate twice (simulating two concurrent users)
        loaded1 = await counter_repository.load(agg_id)
        loaded2 = await counter_repository.load(agg_id)

        # Both modify the aggregate
        loaded1.increment(10)
        loaded2.increment(20)

        # First save succeeds
        await counter_repository.save(loaded1)

        # Second save should fail
        with pytest.raises(OptimisticLockError) as exc_info:
            await counter_repository.save(loaded2)

        assert exc_info.value.aggregate_id == agg_id
        assert exc_info.value.expected_version == 1  # loaded2 expects version 1
        assert exc_info.value.actual_version == 2  # But actual version is 2

    @pytest.mark.asyncio
    async def test_expected_version_calculated_correctly(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        """Expected version should be current version minus uncommitted events."""
        agg_id = uuid4()

        # Create aggregate with some events
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(1)
        aggregate.increment(2)
        aggregate.increment(3)
        await counter_repository.save(aggregate)

        # Load and add more events
        loaded = await counter_repository.load(agg_id)
        loaded.increment(4)
        loaded.increment(5)

        # Before save:
        # - loaded.version = 5 (3 from history + 2 new)
        # - uncommitted_events = 2
        # - expected_version should be 5 - 2 = 3
        assert loaded.version == 5
        assert len(loaded.uncommitted_events) == 2

        # Save should succeed
        await counter_repository.save(loaded)

        # Verify final state
        final = await counter_repository.load(agg_id)
        assert final.version == 5


class TestEventPublishing:
    """Tests for event publishing after save."""

    @pytest.mark.asyncio
    async def test_events_published_after_save(
        self,
        counter_repository_with_publisher: AggregateRepository[CounterAggregate],
        mock_publisher: MockEventPublisher,
    ) -> None:
        """Events should be published after successful save."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)

        await counter_repository_with_publisher.save(aggregate)

        assert len(mock_publisher.published_events) == 2
        assert all(isinstance(e, CounterIncremented) for e in mock_publisher.published_events)

    @pytest.mark.asyncio
    async def test_no_publishing_when_no_events(
        self,
        counter_repository_with_publisher: AggregateRepository[CounterAggregate],
        mock_publisher: MockEventPublisher,
    ) -> None:
        """No events should be published when there are no uncommitted events."""
        aggregate = CounterAggregate(uuid4())

        await counter_repository_with_publisher.save(aggregate)

        assert len(mock_publisher.published_events) == 0

    @pytest.mark.asyncio
    async def test_no_publishing_without_publisher(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        """Save should work without publisher configured."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)

        # Should not raise
        await counter_repository.save(aggregate)

        assert not aggregate.has_uncommitted_events
