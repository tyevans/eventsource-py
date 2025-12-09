"""
Integration test fixtures for subscription management.

Provides reusable fixtures and test utilities for subscription integration tests.
"""

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.events.registry import register_event
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.subscriptions import SubscriptionManager

# =============================================================================
# Test Events
# These use unique names to avoid conflict with parent conftest events
# =============================================================================


@register_event
class SubTestOrderCreated(DomainEvent):
    """Test event for order creation in subscription integration tests."""

    event_type: str = "SubTestOrderCreated"
    aggregate_type: str = "SubTestOrder"
    order_number: str = ""
    amount: float = 0.0


@register_event
class SubTestOrderShipped(DomainEvent):
    """Test event for order shipment in subscription integration tests."""

    event_type: str = "SubTestOrderShipped"
    aggregate_type: str = "SubTestOrder"
    tracking_number: str = ""


@register_event
class SubTestOrderCancelled(DomainEvent):
    """Test event for order cancellation in subscription integration tests."""

    event_type: str = "SubTestOrderCancelled"
    aggregate_type: str = "SubTestOrder"
    reason: str = ""


# Aliases for cleaner imports in test files
TestOrderCreated = SubTestOrderCreated
TestOrderShipped = SubTestOrderShipped
TestOrderCancelled = SubTestOrderCancelled


# =============================================================================
# Test Projections
# =============================================================================


class CollectingProjection:
    """
    Projection that collects events for verification.

    Used in integration tests to verify event delivery, ordering,
    and completeness.
    """

    def __init__(self, name: str | None = None) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.event_ids: set[str] = set()  # Track event IDs for duplicate detection
        self._lock = asyncio.Lock()
        self._name = name

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated, SubTestOrderShipped, SubTestOrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        """Handle an event by collecting it."""
        async with self._lock:
            event_id_str = str(event.event_id)
            if event_id_str not in self.event_ids:
                self.events.append(event)
                self.event_ids.add(event_id_str)
                self.event_count += 1

    async def wait_for_events(self, count: int, timeout: float = 5.0) -> bool:
        """
        Wait until the specified number of events are collected.

        Args:
            count: Number of events to wait for
            timeout: Maximum seconds to wait

        Returns:
            True if the count was reached, False if timeout occurred
        """
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.05)
        return True

    def get_event_at_position(self, position: int) -> DomainEvent | None:
        """Get event at a specific position (0-indexed)."""
        if 0 <= position < len(self.events):
            return self.events[position]
        return None

    def has_duplicates(self) -> bool:
        """Check if any duplicate events were received."""
        # Since we track event_ids in a set, if events list is larger
        # than event_ids set, we have duplicates (though our handle prevents this)
        return len(self.events) != len(self.event_ids)


class FailingProjection:
    """
    Projection that fails on specified event types.

    Used for testing error handling scenarios.
    """

    def __init__(self, fail_on_event_types: set[str] | None = None) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.failures: list[tuple[DomainEvent, Exception]] = []
        self.fail_on_event_types = fail_on_event_types or set()
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated, SubTestOrderShipped, SubTestOrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        """Handle an event, potentially failing."""
        async with self._lock:
            if event.event_type in self.fail_on_event_types:
                error = ValueError(f"Intentional failure for {event.event_type}")
                self.failures.append((event, error))
                raise error
            self.events.append(event)
            self.event_count += 1


class SlowProjection:
    """
    Projection that processes events slowly.

    Used for testing concurrent scenarios and backpressure.
    """

    def __init__(self, delay: float = 0.1) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.delay = delay
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated, SubTestOrderShipped, SubTestOrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        """Handle an event with artificial delay."""
        await asyncio.sleep(self.delay)
        async with self._lock:
            self.events.append(event)
            self.event_count += 1

    async def wait_for_events(self, count: int, timeout: float = 10.0) -> bool:
        """Wait for events with longer timeout for slow processing."""
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.1)
        return True


# =============================================================================
# Fixtures
# =============================================================================


@pytest_asyncio.fixture
async def in_memory_event_store() -> AsyncGenerator[InMemoryEventStore, None]:
    """Create an in-memory event store for testing."""
    store = InMemoryEventStore(enable_tracing=False)
    yield store


@pytest_asyncio.fixture
async def in_memory_event_bus() -> AsyncGenerator[InMemoryEventBus, None]:
    """Create an in-memory event bus for testing."""
    bus = InMemoryEventBus(enable_tracing=False)
    yield bus
    await bus.shutdown()


@pytest_asyncio.fixture
async def in_memory_checkpoint_repo() -> AsyncGenerator[InMemoryCheckpointRepository, None]:
    """Create an in-memory checkpoint repository for testing."""
    repo = InMemoryCheckpointRepository(enable_tracing=False)
    yield repo


@pytest_asyncio.fixture
async def subscription_manager(
    in_memory_event_store: InMemoryEventStore,
    in_memory_event_bus: InMemoryEventBus,
    in_memory_checkpoint_repo: InMemoryCheckpointRepository,
) -> AsyncGenerator[SubscriptionManager, None]:
    """Create a subscription manager for testing."""
    manager = SubscriptionManager(
        event_store=in_memory_event_store,
        event_bus=in_memory_event_bus,
        checkpoint_repo=in_memory_checkpoint_repo,
    )
    yield manager
    if manager.is_running:
        await manager.stop()


@pytest.fixture
def collecting_projection() -> CollectingProjection:
    """Create a collecting projection for testing."""
    return CollectingProjection()


# =============================================================================
# Helper Functions
# =============================================================================


async def populate_event_store(
    store: InMemoryEventStore,
    count: int,
    start_index: int = 0,
) -> list[DomainEvent]:
    """
    Populate the event store with test events.

    Args:
        store: Event store to populate
        count: Number of events to create
        start_index: Starting index for order numbers

    Returns:
        List of created events in order
    """
    events = []
    for i in range(count):
        event = SubTestOrderCreated(
            aggregate_id=uuid4(),
            order_number=f"ORD-{start_index + i:05d}",
            amount=100.0 + i,
        )
        await store.append_events(
            aggregate_id=event.aggregate_id,
            aggregate_type="SubTestOrder",
            events=[event],
            expected_version=0,
        )
        events.append(event)
    return events


async def populate_event_store_with_types(
    store: InMemoryEventStore,
    created_count: int,
    shipped_count: int,
    cancelled_count: int = 0,
) -> list[DomainEvent]:
    """
    Populate the event store with mixed event types.

    Args:
        store: Event store to populate
        created_count: Number of OrderCreated events
        shipped_count: Number of OrderShipped events
        cancelled_count: Number of OrderCancelled events

    Returns:
        List of all created events
    """
    events: list[DomainEvent] = []
    total = 0

    for i in range(created_count):
        event = SubTestOrderCreated(
            aggregate_id=uuid4(),
            order_number=f"ORD-{total:05d}",
            amount=100.0 + i,
        )
        await store.append_events(
            aggregate_id=event.aggregate_id,
            aggregate_type="SubTestOrder",
            events=[event],
            expected_version=0,
        )
        events.append(event)
        total += 1

    for _i in range(shipped_count):
        event = SubTestOrderShipped(
            aggregate_id=uuid4(),
            tracking_number=f"TRK-{total:05d}",
        )
        await store.append_events(
            aggregate_id=event.aggregate_id,
            aggregate_type="SubTestOrder",
            events=[event],
            expected_version=0,
        )
        events.append(event)
        total += 1

    for _i in range(cancelled_count):
        event = SubTestOrderCancelled(
            aggregate_id=uuid4(),
            reason=f"Reason {total}",
        )
        await store.append_events(
            aggregate_id=event.aggregate_id,
            aggregate_type="SubTestOrder",
            events=[event],
            expected_version=0,
        )
        events.append(event)
        total += 1

    return events


async def publish_live_event(
    store: InMemoryEventStore,
    bus: InMemoryEventBus,
    order_number: str,
    amount: float = 100.0,
) -> SubTestOrderCreated:
    """
    Publish a live event through both store and bus.

    Args:
        store: Event store to persist the event
        bus: Event bus to publish the event
        order_number: Order number for the event
        amount: Order amount

    Returns:
        The created event
    """
    event = SubTestOrderCreated(
        aggregate_id=uuid4(),
        order_number=order_number,
        amount=amount,
    )
    await store.append_events(
        aggregate_id=event.aggregate_id,
        aggregate_type="SubTestOrder",
        events=[event],
        expected_version=0,
    )
    await bus.publish([event])
    return event
