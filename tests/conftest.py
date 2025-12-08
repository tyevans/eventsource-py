"""
Shared pytest fixtures for the eventsource library tests.

This module provides comprehensive test fixtures including:
- Domain event fixtures (event_factory, sample_event, event_stream)
- Event store fixtures (in_memory_store, populated_store)
- Repository fixtures (checkpoint_repo, dlq_repo, outbox_repo)
- Aggregate fixtures (test_aggregate, populated_aggregate)
- Projection fixtures (test_projection)
- Sample data fixtures (aggregate_id, tenant_id)

All fixtures are properly scoped and documented for easy reuse.
"""

from collections.abc import AsyncGenerator, Callable
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from eventsource.events.base import DomainEvent
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.repositories.dlq import InMemoryDLQRepository
from eventsource.repositories.outbox import InMemoryOutboxRepository
from eventsource.stores.in_memory import InMemoryEventStore

# Import shared fixtures from fixtures module
from tests.fixtures import (
    CounterAggregate,
    CounterDecremented,
    CounterIncremented,
    CounterNamed,
    CounterReset,
    CounterState,
    DeclarativeCounterAggregate,
    OrderAggregate,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
    OrderState,
    SampleEvent,
    create_event,
)

# =============================================================================
# Sample Data Fixtures
# =============================================================================


@pytest.fixture
def aggregate_id() -> UUID:
    """
    Provide a random aggregate ID.

    Returns:
        A new random UUID for use as an aggregate identifier.
    """
    return uuid4()


@pytest.fixture
def tenant_id() -> UUID:
    """
    Provide a random tenant ID for multi-tenant tests.

    Returns:
        A new random UUID for use as a tenant identifier.
    """
    return uuid4()


@pytest.fixture
def customer_id() -> UUID:
    """
    Provide a random customer ID for order tests.

    Returns:
        A new random UUID for use as a customer identifier.
    """
    return uuid4()


# =============================================================================
# Event Factory Fixtures
# =============================================================================


@pytest.fixture
def event_factory() -> Callable[..., DomainEvent]:
    """
    Factory fixture for creating test events with sensible defaults.

    This fixture returns the create_event factory function from the fixtures
    module, allowing tests to easily create events with custom parameters.

    Usage:
        def test_something(event_factory):
            event = event_factory(CounterIncremented, increment=5)
            event = event_factory(aggregate_version=2)  # SampleEvent at v2

    Returns:
        The create_event factory function.
    """
    return create_event


@pytest.fixture
def sample_event(aggregate_id: UUID) -> SampleEvent:
    """
    Provide a pre-created sample event for simple tests.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A SampleEvent with default values.
    """
    return SampleEvent(
        aggregate_id=aggregate_id,
        aggregate_version=1,
        data="test_data",
    )


@pytest.fixture
def counter_event(aggregate_id: UUID) -> CounterIncremented:
    """
    Provide a counter increment event for counter tests.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A CounterIncremented event with increment=5.
    """
    return CounterIncremented(
        aggregate_id=aggregate_id,
        aggregate_version=1,
        increment=5,
    )


@pytest.fixture
def event_stream(aggregate_id: UUID) -> list[DomainEvent]:
    """
    Provide a list of events representing a typical counter lifecycle.

    Returns events: Incremented(10) -> Incremented(5) -> Decremented(3)
    Final counter value would be 12.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A list of 3 counter events with sequential versions.
    """
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
    """
    Provide a list of events representing a complete order lifecycle.

    Returns events: Created -> ItemAdded -> ItemAdded -> Shipped

    Args:
        aggregate_id: The aggregate ID fixture.
        customer_id: The customer ID fixture.

    Returns:
        A list of 4 order events with sequential versions.
    """
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
            price=10.0,
        ),
        OrderItemAdded(
            aggregate_id=aggregate_id,
            aggregate_version=3,
            item_name="Widget B",
            price=15.0,
        ),
        OrderShipped(
            aggregate_id=aggregate_id,
            aggregate_version=4,
            tracking_number="TRACK123",
        ),
    ]


# =============================================================================
# Event Store Fixtures
# =============================================================================


@pytest.fixture
def in_memory_store() -> InMemoryEventStore:
    """
    Provide a fresh InMemoryEventStore instance.

    Each test gets its own isolated event store.

    Returns:
        A new InMemoryEventStore instance.
    """
    return InMemoryEventStore()


@pytest_asyncio.fixture
async def populated_store(
    in_memory_store: InMemoryEventStore,
    event_stream: list[DomainEvent],
) -> AsyncGenerator[InMemoryEventStore, None]:
    """
    Provide an InMemoryEventStore pre-populated with sample events.

    Contains 3 counter events for a single aggregate (final value = 12).

    Args:
        in_memory_store: The event store fixture.
        event_stream: The event stream fixture.

    Yields:
        The event store populated with events.
    """
    aggregate_id = event_stream[0].aggregate_id
    await in_memory_store.append_events(
        aggregate_id=aggregate_id,
        aggregate_type="Counter",
        events=event_stream,
        expected_version=0,
    )
    yield in_memory_store


# =============================================================================
# Repository Fixtures
# =============================================================================


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """
    Provide a fresh InMemoryCheckpointRepository instance.

    Each test gets its own isolated checkpoint repository.

    Returns:
        A new InMemoryCheckpointRepository instance.
    """
    return InMemoryCheckpointRepository()


@pytest.fixture
def dlq_repo() -> InMemoryDLQRepository:
    """
    Provide a fresh InMemoryDLQRepository instance.

    Each test gets its own isolated dead letter queue repository.

    Returns:
        A new InMemoryDLQRepository instance.
    """
    return InMemoryDLQRepository()


@pytest.fixture
def outbox_repo() -> InMemoryOutboxRepository:
    """
    Provide a fresh InMemoryOutboxRepository instance.

    Each test gets its own isolated outbox repository.

    Returns:
        A new InMemoryOutboxRepository instance.
    """
    return InMemoryOutboxRepository()


# =============================================================================
# Aggregate Fixtures
# =============================================================================


@pytest.fixture
def counter_aggregate(aggregate_id: UUID) -> CounterAggregate:
    """
    Provide a fresh CounterAggregate instance.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A new CounterAggregate at version 0.
    """
    return CounterAggregate(aggregate_id)


@pytest.fixture
def declarative_counter_aggregate(aggregate_id: UUID) -> DeclarativeCounterAggregate:
    """
    Provide a fresh DeclarativeCounterAggregate instance.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A new DeclarativeCounterAggregate at version 0.
    """
    return DeclarativeCounterAggregate(aggregate_id)


@pytest.fixture
def populated_counter_aggregate(aggregate_id: UUID) -> CounterAggregate:
    """
    Provide a CounterAggregate with initial state.

    The counter is incremented by 10, giving it:
    - version = 1
    - state.value = 10

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A CounterAggregate with one event applied.
    """
    aggregate = CounterAggregate(aggregate_id)
    aggregate.increment(10)
    return aggregate


@pytest.fixture
def order_aggregate(aggregate_id: UUID) -> OrderAggregate:
    """
    Provide a fresh OrderAggregate instance.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A new OrderAggregate at version 0.
    """
    return OrderAggregate(aggregate_id)


@pytest.fixture
def populated_order_aggregate(
    aggregate_id: UUID,
    customer_id: UUID,
) -> OrderAggregate:
    """
    Provide an OrderAggregate with items.

    The order has:
    - customer_id set
    - 2 items: Widget A ($10) and Widget B ($15)
    - total = 25.0
    - version = 3

    Args:
        aggregate_id: The aggregate ID fixture.
        customer_id: The customer ID fixture.

    Returns:
        An OrderAggregate with creation and item events applied.
    """
    aggregate = OrderAggregate(aggregate_id)
    aggregate.create(customer_id)
    aggregate.add_item("Widget A", 10.0)
    aggregate.add_item("Widget B", 15.0)
    return aggregate


# =============================================================================
# Mock and Helper Fixtures
# =============================================================================


@pytest.fixture
def mock_event_publisher() -> "MockEventPublisher":
    """
    Provide a mock event publisher for testing event publishing.

    Returns:
        A MockEventPublisher that captures published events.
    """
    return MockEventPublisher()


class MockEventPublisher:
    """
    Mock event publisher for testing event publishing behavior.

    Captures all events that would be published, allowing tests to
    verify that the correct events are published at the right times.
    """

    def __init__(self) -> None:
        """Initialize with empty published events list."""
        self.published_events: list[DomainEvent] = []

    async def publish(self, events: list[DomainEvent]) -> None:
        """
        Capture events for later verification.

        Args:
            events: The events to "publish" (capture).
        """
        self.published_events.extend(events)

    def clear(self) -> None:
        """Clear all captured events."""
        self.published_events.clear()


# =============================================================================
# Re-export fixtures module items for convenience
# =============================================================================

# These are re-exported so tests can import them from conftest if needed
__all__ = [
    # Events
    "CounterIncremented",
    "CounterDecremented",
    "CounterNamed",
    "CounterReset",
    "OrderCreated",
    "OrderItemAdded",
    "OrderShipped",
    "SampleEvent",
    # States
    "CounterState",
    "OrderState",
    # Aggregates
    "CounterAggregate",
    "DeclarativeCounterAggregate",
    "OrderAggregate",
    # Factory
    "create_event",
    # Mock
    "MockEventPublisher",
]
