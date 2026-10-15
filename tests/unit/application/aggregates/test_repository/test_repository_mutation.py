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
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)
        aggregate.increment(3)

        await counter_repository.save(aggregate)

        stream_id = StreamId(aggregate_id=agg_id, category="Counter")
        events = await collect(event_store.read_stream(stream_id))
        assert len(events) == 2
        assert await event_store.get_stream_version(stream_id) == 2

    @pytest.mark.asyncio
    async def test_save_clears_uncommitted_events(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
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
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(10)
        state_before, version_before = aggregate.state, aggregate.version

        await counter_repository.save(aggregate)

        assert aggregate.state == state_before
        assert aggregate.version == version_before

    @pytest.mark.asyncio
    async def test_save_no_op_when_no_uncommitted_events(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        await counter_repository.save(aggregate)

        stream_id = StreamId(aggregate_id=agg_id, category="Counter")
        events = await collect(event_store.read_stream(stream_id))
        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_save_existing_aggregate_with_new_events(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)
        await counter_repository.save(aggregate)

        loaded = await counter_repository.load(agg_id)
        loaded.increment(3)
        await counter_repository.save(loaded)

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
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)
        await counter_repository.save(aggregate)

        loaded1 = await counter_repository.load(agg_id)
        loaded2 = await counter_repository.load(agg_id)
        loaded1.increment(10)
        loaded2.increment(20)

        await counter_repository.save(loaded1)
        with pytest.raises(OptimisticLockError) as exc_info:
            await counter_repository.save(loaded2)

        assert exc_info.value.aggregate_id == agg_id
        assert exc_info.value.expected_version == 1
        assert exc_info.value.actual_version == 2

    @pytest.mark.asyncio
    async def test_expected_version_calculated_correctly(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(1)
        aggregate.increment(2)
        aggregate.increment(3)
        await counter_repository.save(aggregate)

        loaded = await counter_repository.load(agg_id)
        loaded.increment(4)
        loaded.increment(5)

        assert loaded.version == 5
        assert len(loaded.uncommitted_events) == 2

        await counter_repository.save(loaded)
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
        await counter_repository_with_publisher.save(CounterAggregate(uuid4()))
        assert len(mock_publisher.published_events) == 0

    @pytest.mark.asyncio
    async def test_no_publishing_without_publisher(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        await counter_repository.save(aggregate)
        assert not aggregate.has_uncommitted_events
