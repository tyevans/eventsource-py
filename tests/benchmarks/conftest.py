"""
Shared fixtures for benchmark tests.

Provides fixtures specifically designed for performance benchmarking,
including pre-populated stores and event generators.
"""

import asyncio
from collections.abc import Callable, Generator
from typing import Any
from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.repositories.dlq import InMemoryDLQRepository
from eventsource.repositories.outbox import InMemoryOutboxRepository
from eventsource.stores.in_memory import InMemoryEventStore
from tests.fixtures import (
    CounterIncremented,
    OrderCreated,
    OrderItemAdded,
    SampleEvent,
    create_event,
)

# =============================================================================
# Helper Functions for Running Async Code in Benchmarks
# =============================================================================


def run_async(coro: Any) -> Any:
    """
    Run an async coroutine synchronously.

    This helper is needed because pytest-benchmark expects synchronous
    functions. We use asyncio.run() to execute async code within
    benchmark iterations.

    Args:
        coro: The coroutine to execute.

    Returns:
        The result of the coroutine.
    """
    return asyncio.run(coro)


# =============================================================================
# Event Generation Fixtures
# =============================================================================


@pytest.fixture
def benchmark_aggregate_id() -> UUID:
    """
    Provide a consistent aggregate ID for benchmark tests.

    Using a consistent ID allows for benchmarking operations on
    the same aggregate across iterations.
    """
    return uuid4()


@pytest.fixture
def event_generator() -> Callable[..., DomainEvent]:
    """
    Provide a factory function for creating benchmark events.

    Returns the create_event function with sensible defaults.
    """
    return create_event


@pytest.fixture
def sample_events_10(benchmark_aggregate_id: UUID) -> list[DomainEvent]:
    """
    Generate a batch of 10 sample events for benchmarking.
    """
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
    """
    Generate a batch of 100 sample events for benchmarking.
    """
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
    """
    Generate a batch of 1000 sample events for benchmarking.
    """
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
    """
    Generate 100 counter increment events for benchmarking.
    """
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
    """
    Generate 100 order-related events for benchmarking.

    Alternates between OrderCreated and OrderItemAdded events.
    """
    events: list[DomainEvent] = []
    customer_id = uuid4()

    for i in range(100):
        if i % 10 == 0:
            events.append(
                OrderCreated(
                    aggregate_id=uuid4(),  # New order for each creation
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
                    price=float(i * 10),
                )
            )

    return events


# =============================================================================
# Event Store Fixtures
# =============================================================================


@pytest.fixture
def benchmark_store() -> InMemoryEventStore:
    """
    Provide a fresh InMemoryEventStore for benchmarking.
    """
    return InMemoryEventStore()


@pytest.fixture
def populated_benchmark_store_100(
    benchmark_store: InMemoryEventStore,
    sample_events_100: list[DomainEvent],
) -> Generator[InMemoryEventStore, None, None]:
    """
    Provide an InMemoryEventStore pre-populated with 100 events.
    """
    aggregate_id = sample_events_100[0].aggregate_id

    async def setup() -> None:
        await benchmark_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=sample_events_100,
            expected_version=0,
        )

    run_async(setup())
    yield benchmark_store


@pytest.fixture
def populated_benchmark_store_1000(
    benchmark_store: InMemoryEventStore,
    sample_events_1000: list[DomainEvent],
) -> Generator[InMemoryEventStore, None, None]:
    """
    Provide an InMemoryEventStore pre-populated with 1000 events.
    """
    aggregate_id = sample_events_1000[0].aggregate_id

    async def setup() -> None:
        await benchmark_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            events=sample_events_1000,
            expected_version=0,
        )

    run_async(setup())
    yield benchmark_store


# =============================================================================
# Repository Fixtures
# =============================================================================


@pytest.fixture
def benchmark_checkpoint_repo() -> InMemoryCheckpointRepository:
    """
    Provide a fresh InMemoryCheckpointRepository for benchmarking.
    """
    return InMemoryCheckpointRepository()


@pytest.fixture
def benchmark_dlq_repo() -> InMemoryDLQRepository:
    """
    Provide a fresh InMemoryDLQRepository for benchmarking.
    """
    return InMemoryDLQRepository()


@pytest.fixture
def benchmark_outbox_repo() -> InMemoryOutboxRepository:
    """
    Provide a fresh InMemoryOutboxRepository for benchmarking.
    """
    return InMemoryOutboxRepository()


@pytest.fixture
def populated_checkpoint_repo(
    benchmark_checkpoint_repo: InMemoryCheckpointRepository,
    sample_events_100: list[DomainEvent],
) -> Generator[InMemoryCheckpointRepository, None, None]:
    """
    Provide a checkpoint repository with 100 checkpoints pre-recorded.
    """

    async def setup() -> None:
        for i, event in enumerate(sample_events_100):
            await benchmark_checkpoint_repo.update_checkpoint(
                projection_name=f"Projection_{i % 10}",
                event_id=event.event_id,
                event_type=event.event_type,
            )

    run_async(setup())
    yield benchmark_checkpoint_repo
