"""Pytest configuration and shared fixtures for SubscriptionManager tests."""

from tests.unit.application.subscriptions.test_subscription_manager._fixtures import (
    AnotherManagerEvent,
    CustomerProjection,
    ManagerTestEvent,
    MockSubscriber,
    OrderProjection,
    add_events_to_store,
    checkpoint_repo,
    config,
    event_bus,
    event_store,
    manager,
    subscriber,
)

__all__ = [  # pragma: allowlist secret
    "AnotherManagerEvent",
    "CustomerProjection",
    "ManagerTestEvent",
    "MockSubscriber",
    "OrderProjection",
    "add_events_to_store",
    "checkpoint_repo",
    "config",
    "event_bus",
    "event_store",
    "manager",
    "subscriber",
]
