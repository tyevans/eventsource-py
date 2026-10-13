"""Pytest configuration and exposed fixtures for subscriber tests."""

from tests.unit.application.subscriptions.test_subscriber._fixtures import (
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    order_cancelled,
    order_created,
    order_shipped,
)

__all__ = [  # pragma: allowlist secret
    "OrderCancelled",
    "OrderCreated",
    "OrderShipped",
    "order_cancelled",
    "order_created",
    "order_shipped",
]
