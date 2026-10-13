"""Test event types and position helpers for migration integration tests."""

from __future__ import annotations

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.positions import Position

SOURCE_STORE_ID = "source-store"
TARGET_STORE_ID = "target-store"


def src_pos(n: int) -> Position:
    """A source-store position token for the int the mapper is keyed on."""
    return Position(store_id=SOURCE_STORE_ID, key=(n,))


def tgt_pos(n: int) -> Position:
    """A target-store position token, as the migrator writes back."""
    return Position(store_id=TARGET_STORE_ID, key=(n,))


# =============================================================================
# Test Event Classes
# =============================================================================

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class SampleTestEvent(DomainEvent):
    """Generic test event for integration tests."""

    __test__ = False
    aggregate_type: str = "SampleAggregate"
    value: str = "test"


@register_event(registry=_REGISTRY)
class OrderCreated(DomainEvent):
    """Test order created event."""

    aggregate_type: str = "Order"
    customer_id: str = "customer-123"
    amount: float = 100.0


@register_event(registry=_REGISTRY)
class OrderUpdated(DomainEvent):
    """Test order updated event."""

    aggregate_type: str = "Order"
    new_amount: float = 150.0


__all__ = [  # pragma: allowlist secret
    "SOURCE_STORE_ID",
    "TARGET_STORE_ID",
    "_REGISTRY",
    "OrderCreated",
    "OrderUpdated",
    "SampleTestEvent",
    "src_pos",
    "tgt_pos",
]
