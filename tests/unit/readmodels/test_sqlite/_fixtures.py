"""Test models and fixtures for SQLite read model tests."""

from decimal import Decimal

from eventsource.ports.readmodels.model import ReadModel


class OrderSummary(ReadModel):
    """Test model for repository tests."""

    order_number: str
    status: str
    total_amount: Decimal


class CustomTableModel(ReadModel):
    """Test model with custom table name."""

    __table_name__ = "custom_orders"
    name: str


__all__ = [
    "CustomTableModel",  # pragma: allowlist secret
    "OrderSummary",  # pragma: allowlist secret
]
