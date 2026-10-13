"""Test fixtures and sample models/events for ReadModelProjection tests."""

from decimal import Decimal

from pydantic import Field

from eventsource.domain.event import DomainEvent
from eventsource.ports.readmodels import ReadModel

# =============================================================================
# Test Events
# =============================================================================


class OrderCreated(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    """Sample event for testing."""

    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


class OrderCancelled(DomainEvent):
    """Sample event for testing - not handled by any projection."""

    aggregate_type: str = "Order"
    reason: str = Field(..., description="Cancellation reason")


# =============================================================================
# Test Models
# =============================================================================


class OrderSummary(ReadModel):
    """Test model for projection tests."""

    order_number: str
    status: str
    total_amount: Decimal = Decimal("0.00")


class CustomTableModel(ReadModel):
    """Test model with custom table name."""

    __table_name__ = "custom_orders"
    name: str
