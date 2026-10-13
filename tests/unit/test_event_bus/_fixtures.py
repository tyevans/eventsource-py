"""Fixtures and test doubles for event bus unit tests."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.domain.event import DomainEvent
from eventsource.ports.handlers import (
    AsyncEventHandler,
    FlexibleEventSubscriber,
)
from eventsource.ports.handlers import (
    FlexibleEventHandler as EventHandler,
)

# =============================================================================
# Test Event Classes
# =============================================================================


class OrderCreated(DomainEvent):
    """Test event for order creation."""

    aggregate_type: str = "Order"
    order_number: str
    customer_id: str


class OrderShipped(DomainEvent):
    """Test event for order shipping."""

    aggregate_type: str = "Order"
    order_number: str
    tracking_number: str


class OrderCancelled(DomainEvent):
    """Test event for order cancellation."""

    aggregate_type: str = "Order"
    order_number: str
    reason: str


# =============================================================================
# Test Handler Classes
# =============================================================================


class RecordingHandler:
    """Test handler that records all events it receives."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []
        self.handle_count: int = 0

    async def handle(self, event: DomainEvent) -> None:
        """Record the event."""
        self.handled_events.append(event)
        self.handle_count += 1


class SyncRecordingHandler:
    """Test handler with a synchronous handle method."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []
        self.handle_count: int = 0

    def handle(self, event: DomainEvent) -> None:
        """Record the event synchronously."""
        self.handled_events.append(event)
        self.handle_count += 1


class FailingHandler:
    """Test handler that always raises an exception."""

    def __init__(self, error_message: str = "Handler failed intentionally") -> None:
        self.error_message = error_message
        self.call_count: int = 0

    async def handle(self, event: DomainEvent) -> None:
        """Always raise an exception."""
        self.call_count += 1
        raise ValueError(self.error_message)


class SlowHandler:
    """Test handler that takes time to process."""

    def __init__(self, delay: float = 0.1) -> None:
        self.delay = delay
        self.handled_events: list[DomainEvent] = []

    async def handle(self, event: DomainEvent) -> None:
        """Handle event with delay."""
        await asyncio.sleep(self.delay)
        self.handled_events.append(event)


class SelectiveSubscriber:
    """Test subscriber that only handles specific event types."""

    def __init__(self, event_types: list[type[DomainEvent]]) -> None:
        self._event_types = event_types
        self.handled_events: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return subscribed event types."""
        return self._event_types

    async def handle(self, event: DomainEvent) -> None:
        """Handle the event."""
        self.handled_events.append(event)


class SampleAsyncEventHandler(AsyncEventHandler):
    """Sample implementation of AsyncEventHandler ABC for testing."""

    def __init__(self, event_types: list[type[DomainEvent]]) -> None:
        self._event_types = event_types
        self.handled_events: list[DomainEvent] = []

    def event_types(self) -> list[type[DomainEvent]]:
        """Return supported event types."""
        return self._event_types

    async def handle(self, event: DomainEvent) -> None:
        """Handle the event."""
        self.handled_events.append(event)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def event_bus() -> InMemoryEventBus:
    """Create a fresh event bus for testing."""
    return InMemoryEventBus()


@pytest.fixture
def sample_order_created() -> OrderCreated:
    """Create a sample OrderCreated event."""
    return OrderCreated(
        aggregate_id=uuid4(),
        order_number="ORD-001",
        customer_id="customer-123",
    )


@pytest.fixture
def sample_order_shipped() -> OrderShipped:
    """Create a sample OrderShipped event."""
    return OrderShipped(
        aggregate_id=uuid4(),
        order_number="ORD-001",
        tracking_number="TRK-123456",
    )


@pytest.fixture
def sample_order_cancelled() -> OrderCancelled:
    """Create a sample OrderCancelled event."""
    return OrderCancelled(
        aggregate_id=uuid4(),
        order_number="ORD-001",
        reason="Customer request",
    )


__all__ = [
    "EventHandler",
    "FailingHandler",
    "FlexibleEventSubscriber",
    "OrderCancelled",
    "OrderCreated",
    "OrderShipped",
    "RecordingHandler",
    "SampleAsyncEventHandler",
    "SelectiveSubscriber",
    "SlowHandler",
    "SyncRecordingHandler",
    "event_bus",
    "sample_order_cancelled",
    "sample_order_created",
    "sample_order_shipped",
]
