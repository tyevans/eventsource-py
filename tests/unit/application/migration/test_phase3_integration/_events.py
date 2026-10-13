"""Test event types and position helpers for Phase 3 integration tests."""

from __future__ import annotations

from eventsource.domain.event import DomainEvent
from eventsource.ports.positions import Position

SOURCE_STORE_ID = "source-store"
TARGET_STORE_ID = "target-store"


def source_pos(n: int) -> Position:
    """A source-store position token for the int the mapper is keyed on."""
    return Position(store_id=SOURCE_STORE_ID, key=(n,))


def target_pos(n: int) -> Position:
    """A target-store position token, as the migrator writes back."""
    return Position(store_id=TARGET_STORE_ID, key=(n,))


# Aliases for brevity / compatibility
src_pos = source_pos
tgt_pos = target_pos


# =============================================================================
# Test Event Classes
# =============================================================================


class SampleTestEvent(DomainEvent):
    """Generic test event."""

    __test__ = False
    aggregate_type: str = "SampleAggregate"
    value: str = "test"


class OrderCreated(DomainEvent):
    """Test order created event."""

    aggregate_type: str = "Order"
    customer_id: str = "customer-123"
    amount: float = 100.0


class OrderUpdated(DomainEvent):
    """Test order updated event."""

    aggregate_type: str = "Order"
    new_amount: float = 150.0


__all__ = [  # pragma: allowlist secret
    "SOURCE_STORE_ID",
    "TARGET_STORE_ID",
    "OrderCreated",
    "OrderUpdated",
    "SampleTestEvent",
    "source_pos",
    "src_pos",
    "target_pos",
    "tgt_pos",
]
