"""
Shared test event types and event factory.

This module provides reusable event types for testing:
- Counter events: CounterIncremented, CounterDecremented, CounterNamed, CounterReset
- Order events: OrderCreated, OrderItemAdded, OrderShipped, OrderCancelled
- Generic events: SampleEvent, UserRegistered

Also provides an event factory function for creating test events with sensible defaults.
"""

from typing import Any
from uuid import UUID, uuid4

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import register_event

# =============================================================================
# Counter Events - For testing aggregates with simple state
# =============================================================================


@register_event
class CounterIncremented(DomainEvent):
    """Event for incrementing a counter."""

    aggregate_type: str = "Counter"
    increment: int = 1


@register_event
class CounterDecremented(DomainEvent):
    """Event for decrementing a counter."""

    aggregate_type: str = "Counter"
    decrement: int = 1


@register_event
class CounterNamed(DomainEvent):
    """Event for naming a counter."""

    aggregate_type: str = "Counter"
    name: str


@register_event
class CounterReset(DomainEvent):
    """Event for resetting a counter to zero."""

    aggregate_type: str = "Counter"


# =============================================================================
# Order Events - For testing aggregates with complex state
# =============================================================================


@register_event
class OrderCreated(DomainEvent):
    """Event for order creation."""

    aggregate_type: str = "Order"
    customer_id: UUID


@register_event
class OrderItemAdded(DomainEvent):
    """Event for adding an item to an order."""

    aggregate_type: str = "Order"
    item_name: str
    price: float


@register_event
class OrderShipped(DomainEvent):
    """Event for shipping an order."""

    aggregate_type: str = "Order"
    tracking_number: str


@register_event
class OrderCancelled(DomainEvent):
    """Event for cancelling an order."""

    aggregate_type: str = "Order"
    reason: str = ""


# =============================================================================
# Generic Events - For simple test scenarios
# =============================================================================


@register_event
class SampleEvent(DomainEvent):
    """Simple test event for basic scenarios."""

    aggregate_type: str = "TestAggregate"
    data: str = "test"


@register_event
class UserRegistered(DomainEvent):
    """User registration event for multi-aggregate testing."""

    aggregate_type: str = "User"
    email: str


# =============================================================================
# Event Factory
# =============================================================================


def create_event(
    event_class: type[DomainEvent] = SampleEvent,
    aggregate_id: UUID | None = None,
    aggregate_type: str | None = None,
    aggregate_version: int = 1,
    tenant_id: UUID | None = None,
    **kwargs: Any,
) -> DomainEvent:
    """
    Factory function for creating test events with sensible defaults.

    This factory simplifies test setup by providing default values for
    common event fields while allowing customization through kwargs.

    Args:
        event_class: The event class to instantiate (default: SampleEvent)
        aggregate_id: The aggregate ID (default: random UUID)
        aggregate_type: The aggregate type (default: from event class)
        aggregate_version: The event version (default: 1)
        tenant_id: Optional tenant ID for multi-tenant tests
        **kwargs: Additional fields specific to the event type

    Returns:
        A new instance of the specified event class.

    Examples:
        >>> event = create_event(CounterIncremented, increment=5)
        >>> event = create_event(OrderCreated, customer_id=uuid4())
        >>> event = create_event(aggregate_version=2)  # SampleEvent at version 2
    """
    # Get default aggregate_type from event class if not provided
    if aggregate_type is None:
        # Access class attribute for default aggregate_type
        try:
            aggregate_type = event_class.model_fields["aggregate_type"].default
        except (KeyError, AttributeError):
            aggregate_type = "TestAggregate"

    return event_class(
        aggregate_id=aggregate_id or uuid4(),
        aggregate_type=aggregate_type,
        aggregate_version=aggregate_version,
        tenant_id=tenant_id,
        **kwargs,
    )
