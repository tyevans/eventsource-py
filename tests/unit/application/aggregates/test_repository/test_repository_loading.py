"""Tests for loading, querying, and creating aggregates with AggregateRepository."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain import StreamId
from eventsource.domain.exceptions import AggregateNotFoundError
from eventsource.ports import ExpectedVersion
from tests.unit.application.aggregates.test_repository._models import (
    CounterAggregate,
    CounterDecremented,
    CounterIncremented,
    MockEventPublisher,
)


class TestRepositoryInitialization:
    """Tests for repository initialization and properties."""

    def test_repository_stores_event_store(self, event_store: InMemoryEventStore) -> None:
        repo: AggregateRepository[CounterAggregate] = AggregateRepository(
            event_store=event_store, aggregate_factory=CounterAggregate
        )
        assert repo.event_store is event_store

    def test_repository_stores_aggregate_type(self, event_store: InMemoryEventStore) -> None:
        repo: AggregateRepository[CounterAggregate] = AggregateRepository(
            event_store=event_store, aggregate_factory=CounterAggregate
        )
        assert repo.aggregate_type == "Counter"

    def test_repository_stores_event_publisher(
        self, event_store: InMemoryEventStore, mock_publisher: MockEventPublisher
    ) -> None:
        repo: AggregateRepository[CounterAggregate] = AggregateRepository(
            event_store=event_store,
            aggregate_factory=CounterAggregate,
            event_publisher=mock_publisher,
        )
        assert repo.event_publisher is mock_publisher

    def test_repository_without_publisher(self, event_store: InMemoryEventStore) -> None:
        repo: AggregateRepository[CounterAggregate] = AggregateRepository(
            event_store=event_store, aggregate_factory=CounterAggregate
        )
        assert repo.event_publisher is None


class TestLoadAggregate:
    """Tests for loading aggregates from event store."""

    @pytest.mark.asyncio
    async def test_load_existing_aggregate(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        agg_id = uuid4()
        events = [
            CounterIncremented(
                aggregate_id=agg_id, aggregate_type="Counter", aggregate_version=1, increment=10
            ),
            CounterIncremented(
                aggregate_id=agg_id, aggregate_type="Counter", aggregate_version=2, increment=5
            ),
            CounterDecremented(
                aggregate_id=agg_id, aggregate_type="Counter", aggregate_version=3, decrement=3
            ),
        ]
        await event_store.append(
            StreamId(aggregate_id=agg_id, category="Counter"), events, ExpectedVersion.exact(0)
        )
        aggregate = await counter_repository.load(agg_id)
        assert aggregate.aggregate_id == agg_id
        assert aggregate.version == 3
        assert aggregate.state is not None and aggregate.state.value == 12

    @pytest.mark.asyncio
    async def test_load_nonexistent_aggregate_raises_error(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        nonexistent_id = uuid4()
        with pytest.raises(AggregateNotFoundError) as exc_info:
            await counter_repository.load(nonexistent_id)
        assert exc_info.value.aggregate_id == nonexistent_id
        assert exc_info.value.aggregate_type == "Counter"

    @pytest.mark.asyncio
    async def test_load_aggregate_has_no_uncommitted_events(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        agg_id = uuid4()
        event = CounterIncremented(
            aggregate_id=agg_id, aggregate_type="Counter", aggregate_version=1, increment=5
        )
        await event_store.append(
            StreamId(aggregate_id=agg_id, category="Counter"), [event], ExpectedVersion.exact(0)
        )
        aggregate = await counter_repository.load(agg_id)
        assert not aggregate.has_uncommitted_events
        assert len(aggregate.uncommitted_events) == 0


class TestLoadOrCreate:
    """Tests for load_or_create method."""

    @pytest.mark.asyncio
    async def test_load_or_create_loads_existing(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        agg_id = uuid4()
        event = CounterIncremented(
            aggregate_id=agg_id, aggregate_type="Counter", aggregate_version=1, increment=10
        )
        await event_store.append(
            StreamId(aggregate_id=agg_id, category="Counter"), [event], ExpectedVersion.exact(0)
        )
        aggregate = await counter_repository.load_or_create(agg_id)
        assert aggregate.aggregate_id == agg_id
        assert aggregate.version == 1
        assert aggregate.state is not None and aggregate.state.value == 10

    @pytest.mark.asyncio
    async def test_load_or_create_creates_new(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        new_id = uuid4()
        aggregate = await counter_repository.load_or_create(new_id)
        assert aggregate.aggregate_id == new_id
        assert aggregate.version == 0
        assert aggregate.state is None

    @pytest.mark.asyncio
    async def test_load_or_create_new_aggregate_can_be_modified(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        aggregate = await counter_repository.load_or_create(uuid4())
        aggregate.increment(5)
        assert aggregate.version == 1
        assert aggregate.state is not None and aggregate.state.value == 5
        assert aggregate.has_uncommitted_events


class TestGetOrRaise:
    """Tests for get_or_raise method."""

    @pytest.mark.asyncio
    async def test_get_or_raise_returns_existing(
        self,
        counter_repository: AggregateRepository[CounterAggregate],
        event_store: InMemoryEventStore,
    ) -> None:
        agg_id = uuid4()
        event = CounterIncremented(
            aggregate_id=agg_id, aggregate_type="Counter", aggregate_version=1, increment=7
        )
        await event_store.append(
            StreamId(aggregate_id=agg_id, category="Counter"), [event], ExpectedVersion.exact(0)
        )
        aggregate = await counter_repository.get_or_raise(agg_id)
        assert aggregate.aggregate_id == agg_id
        assert aggregate.version == 1

    @pytest.mark.asyncio
    async def test_get_or_raise_raises_for_nonexistent(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        with pytest.raises(AggregateNotFoundError):
            await counter_repository.get_or_raise(uuid4())


class TestExistsMethod:
    """Tests for exists method."""

    @pytest.mark.asyncio
    async def test_exists_returns_true_for_existing(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(5)
        await counter_repository.save(aggregate)
        assert await counter_repository.exists(agg_id) is True

    @pytest.mark.asyncio
    async def test_exists_returns_false_for_nonexistent(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        assert await counter_repository.exists(uuid4()) is False


class TestGetVersionMethod:
    """Tests for get_version method."""

    @pytest.mark.asyncio
    async def test_get_version_returns_correct_version(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.increment(1)
        aggregate.increment(2)
        aggregate.increment(3)
        await counter_repository.save(aggregate)
        assert await counter_repository.get_version(agg_id) == 3

    @pytest.mark.asyncio
    async def test_get_version_returns_zero_for_nonexistent(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        assert await counter_repository.get_version(uuid4()) == 0


class TestCreateNewMethod:
    """Tests for create_new method."""

    def test_create_new_returns_empty_aggregate(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        new_id = uuid4()
        aggregate = counter_repository.create_new(new_id)
        assert aggregate.aggregate_id == new_id
        assert aggregate.version == 0
        assert aggregate.state is None and not aggregate.has_uncommitted_events

    def test_create_new_can_be_modified_and_saved(
        self, counter_repository: AggregateRepository[CounterAggregate]
    ) -> None:
        aggregate = counter_repository.create_new(uuid4())
        aggregate.increment(5)
        assert aggregate.version == 1
        assert aggregate.state is not None and aggregate.state.value == 5
        assert aggregate.has_uncommitted_events
