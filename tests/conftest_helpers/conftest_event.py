"""Event, stream, in-memory repository, and mock publisher fixtures for pytest."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.memory.outbox import InMemoryOutboxRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion
from tests.fixtures import (
    CounterDecremented,
    CounterIncremented,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
    SampleEvent,
    create_event,
)


@pytest.fixture
def tenant_id() -> UUID:
    """Provide a random tenant ID for multi-tenant tests."""
    return uuid4()


@pytest.fixture
def customer_id() -> UUID:
    """Provide a random customer ID for order tests."""
    return uuid4()


@pytest.fixture
def event_factory() -> Callable[..., DomainEvent]:
    """Factory fixture for creating test events with sensible defaults."""
    return create_event


@pytest.fixture
def sample_event(aggregate_id: UUID) -> SampleEvent:
    """Provide a pre-created sample event for simple tests."""
    return SampleEvent(
        aggregate_id=aggregate_id,
        aggregate_version=1,
        data="test_data",
    )


@pytest.fixture
def counter_event(aggregate_id: UUID) -> CounterIncremented:
    """Provide a counter increment event for counter tests."""
    return CounterIncremented(
        aggregate_id=aggregate_id,
        aggregate_version=1,
        increment=5,
    )


@pytest.fixture
def event_stream(aggregate_id: UUID) -> list[DomainEvent]:
    """Provide a list of events representing a typical counter lifecycle (value = 12)."""
    return [
        CounterIncremented(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            increment=10,
        ),
        CounterIncremented(
            aggregate_id=aggregate_id,
            aggregate_version=2,
            increment=5,
        ),
        CounterDecremented(
            aggregate_id=aggregate_id,
            aggregate_version=3,
            decrement=3,
        ),
    ]


@pytest.fixture
def order_event_stream(aggregate_id: UUID, customer_id: UUID) -> list[DomainEvent]:
    """Provide a list of events representing a complete order lifecycle."""
    return [
        OrderCreated(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            customer_id=customer_id,
        ),
        OrderItemAdded(
            aggregate_id=aggregate_id,
            aggregate_version=2,
            item_name="Widget A",
            price=Decimal("10.00"),
        ),
        OrderItemAdded(
            aggregate_id=aggregate_id,
            aggregate_version=3,
            item_name="Widget B",
            price=Decimal("15.00"),
        ),
        OrderShipped(
            aggregate_id=aggregate_id,
            aggregate_version=4,
            tracking_number="TRACK123",
        ),
    ]


@pytest.fixture
def in_memory_store() -> InMemoryEventStore:
    """Provide a fresh InMemoryEventStore instance."""
    return InMemoryEventStore()


@pytest_asyncio.fixture
async def populated_store(
    in_memory_store: InMemoryEventStore,
    event_stream: list[DomainEvent],
) -> AsyncGenerator[InMemoryEventStore]:
    """Provide an InMemoryEventStore pre-populated with 3 sample counter events."""
    aggregate_id = event_stream[0].aggregate_id
    await in_memory_store.append(
        StreamId(aggregate_id=aggregate_id, category="Counter"),
        event_stream,
        ExpectedVersion.no_stream(),
    )
    yield in_memory_store


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Provide a fresh InMemoryCheckpointRepository instance."""
    return InMemoryCheckpointRepository()


@pytest.fixture
def dlq_repo() -> InMemoryDLQRepository:
    """Provide a fresh InMemoryDLQRepository instance."""
    return InMemoryDLQRepository()


@pytest.fixture
def outbox_repo() -> InMemoryOutboxRepository:
    """Provide a fresh InMemoryOutboxRepository instance."""
    return InMemoryOutboxRepository()


class MockEventPublisher:
    """Mock event publisher that captures published events for verification."""

    def __init__(self) -> None:
        self.published_events: list[DomainEvent] = []

    async def publish(self, events: list[DomainEvent]) -> None:
        """Capture events for later verification."""
        self.published_events.extend(events)

    def clear(self) -> None:
        """Clear all captured events."""
        self.published_events.clear()


@pytest.fixture
def mock_event_publisher() -> MockEventPublisher:
    """Provide a mock event publisher for testing event publishing."""
    return MockEventPublisher()


__all__ = [
    "MockEventPublisher",
    "checkpoint_repo",
    "counter_event",
    "customer_id",
    "dlq_repo",
    "event_factory",
    "event_stream",
    "in_memory_store",
    "mock_event_publisher",
    "order_event_stream",
    "outbox_repo",
    "populated_store",
    "sample_event",
    "tenant_id",
]
