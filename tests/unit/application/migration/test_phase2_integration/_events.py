"""Event models and registries for phase 2 migration integration tests."""

from __future__ import annotations

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class SampleTestEvent(DomainEvent):
    """Sample event for integration tests."""

    aggregate_type: str = "SampleAggregate"
    value: str = "test"


@register_event(registry=_REGISTRY)
class OrderCreated(DomainEvent):
    """Test order created event."""

    aggregate_type: str = "Order"
    customer_id: str = "customer-123"
    amount: float = 100.0


@register_event(registry=_REGISTRY)
class OrderConfirmed(DomainEvent):
    """Test order confirmed event."""

    aggregate_type: str = "Order"


__all__ = [
    "OrderConfirmed",
    "OrderCreated",
    "SampleTestEvent",
    "_REGISTRY",
]
