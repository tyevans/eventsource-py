"""Shared fixtures for benchmark tests."""

import asyncio
from collections.abc import Callable, Generator
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.memory.outbox import InMemoryOutboxRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports.positions import ExpectedVersion
from tests.fixtures import (
    CounterIncremented,
    OrderCreated,
    OrderItemAdded,
    SampleEvent,
    create_event,
)


def run_async(coro: Any) -> Any:
    """Run an async coroutine synchronously for pytest-benchmark."""
    return asyncio.run(coro)


@pytest.fixture
def benchmark_aggregate_id() -> UUID:
    """Provide a consistent aggregate ID for benchmark tests."""
    return uuid4()


@pytest.fixture
def event_generator() -> Callable[..., DomainEvent]:
    """Provide a factory function for creating benchmark events."""
    return create_event


@pytest.fixture
def sample_events_10(benchmark_aggregate_id: UUID) -> list[DomainEvent]:
    """Generate a batch of 10 sample events for benchmarking."""
    return [
        create_event(
            SampleEvent,
            aggregate_id=benchmark_aggregate_id,
            aggregate_version=i + 1,
            data=f"test_data_{i}",
        )
        for i in range(10)
    ]


@pytest.fixture
def sample_events_100(benchmark_aggregate_id: UUID) -> list[DomainEvent]:
    """Generate a batch of 100 sample events for benchmarking."""
    return [
        create_event(
            SampleEvent,
            aggregate_id=benchmark_aggregate_id,
            aggregate_version=i + 1,
            data=f"test_data_{i}",
        )
        for i in range(100)
    ]


@pytest.fixture
def sample_events_1000(benchmark_aggregate_id: UUID) -> list[DomainEvent]:
    """Generate a batch of 1000 sample events for benchmarking."""
    return [
        create_event(
            SampleEvent,
            aggregate_id=benchmark_aggregate_id,
            aggregate_version=i + 1,
            data=f"test_data_{i}",
        )
        for i in range(1000)
    ]


@pytest.fixture
def counter_events_100(benchmark_aggregate_id: UUID) -> list[CounterIncremented]:
    """Generate 100 counter increment events for benchmarking."""
    return [
        CounterIncremented(
            aggregate_id=benchmark_aggregate_id,
            aggregate_version=i + 1,
            increment=1,
        )
        for i in range(100)
    ]


@pytest.fixture
def order_events_100(benchmark_aggregate_id: UUID) -> list[DomainEvent]:
    """Generate 100 alternating OrderCreated and OrderItemAdded events."""
    events: list[DomainEvent] = []
    customer_id = uuid4()

    for i in range(100):
        if i % 10 == 0:
            events.append(
                OrderCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    customer_id=customer_id,
                )
            )
        else:
            events.append(
                OrderItemAdded(
                    aggregate_id=events[-1].aggregate_id if events else uuid4(),
                    aggregate_version=(i % 10) + 1,
                    item_name=f"Item {i}",
                    price=Decimal(i * 10),
                )
            )

    return events


@pytest.fixture
def benchmark_store() -> InMemoryEventStore:
    """Provide a fresh InMemoryEventStore for benchmarking."""
    return InMemoryEventStore()


@pytest.fixture
def populated_benchmark_store_100(
    benchmark_store: InMemoryEventStore,
    sample_events_100: list[DomainEvent],
) -> Generator[InMemoryEventStore]:
    """Provide an InMemoryEventStore pre-populated with 100 events."""
    aggregate_id = sample_events_100[0].aggregate_id

    async def setup() -> None:
        stream_id = StreamId(aggregate_id, "TestAggregate")
        await benchmark_store.append(stream_id, sample_events_100, ExpectedVersion.exact(0))

    run_async(setup())
    yield benchmark_store


@pytest.fixture
def populated_benchmark_store_1000(
    benchmark_store: InMemoryEventStore,
    sample_events_1000: list[DomainEvent],
) -> Generator[InMemoryEventStore]:
    """Provide an InMemoryEventStore pre-populated with 1000 events."""
    aggregate_id = sample_events_1000[0].aggregate_id

    async def setup() -> None:
        stream_id = StreamId(aggregate_id, "TestAggregate")
        await benchmark_store.append(stream_id, sample_events_1000, ExpectedVersion.exact(0))

    run_async(setup())
    yield benchmark_store


@pytest.fixture
def benchmark_checkpoint_repo() -> InMemoryCheckpointRepository:
    """Provide a fresh InMemoryCheckpointRepository for benchmarking."""
    return InMemoryCheckpointRepository()


@pytest.fixture
def benchmark_dlq_repo() -> InMemoryDLQRepository:
    """Provide a fresh InMemoryDLQRepository for benchmarking."""
    return InMemoryDLQRepository()


@pytest.fixture
def benchmark_outbox_repo() -> InMemoryOutboxRepository:
    """Provide a fresh InMemoryOutboxRepository for benchmarking."""
    return InMemoryOutboxRepository()


@pytest.fixture
def populated_checkpoint_repo(
    benchmark_checkpoint_repo: InMemoryCheckpointRepository,
    sample_events_100: list[DomainEvent],
) -> Generator[InMemoryCheckpointRepository]:
    """Provide a checkpoint repository with 100 checkpoints pre-recorded."""

    async def setup() -> None:
        for i, event in enumerate(sample_events_100):
            await benchmark_checkpoint_repo.update_checkpoint(
                projection_name=f"Projection_{i % 10}",
                event_id=event.event_id,
                event_type=event.event_type,
            )

    run_async(setup())
    yield benchmark_checkpoint_repo
