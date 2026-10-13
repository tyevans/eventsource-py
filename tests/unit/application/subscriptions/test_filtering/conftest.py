"""Pytest configuration and shared fixtures for subscription filtering tests."""

from tests.unit.application.subscriptions.test_filtering._fixtures import (
    MockAllEventsSubscriber,
    MockOrderSubscriber,
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    PaymentReceived,
    UserRegistered,
    UserUpdatedEvent,
    order_cancelled,
    order_created,
    order_shipped,
    payment_received,
    user_registered,
)

__all__ = [  # pragma: allowlist secret
    "MockAllEventsSubscriber",
    "MockOrderSubscriber",
    "OrderCancelled",
    "OrderCreated",
    "OrderShipped",
    "PaymentReceived",
    "UserRegistered",
    "UserUpdatedEvent",
    "order_cancelled",
    "order_created",
    "order_shipped",
    "payment_received",
    "user_registered",
]
