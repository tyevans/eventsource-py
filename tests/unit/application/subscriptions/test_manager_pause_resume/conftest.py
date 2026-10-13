"""Pytest configuration and shared fixtures for manager pause/resume tests."""

from tests.unit.application.subscriptions.test_manager_pause_resume._fixtures import (
    CustomerProjection,
    MockSubscriber,
    OrderProjection,
    PauseTestEvent,
    add_events_to_store,
    checkpoint_repo,
    event_bus,
    event_store,
    manager,
)

__all__ = [  # pragma: allowlist secret
    "CustomerProjection",
    "MockSubscriber",
    "OrderProjection",
    "PauseTestEvent",
    "add_events_to_store",
    "checkpoint_repo",
    "event_bus",
    "event_store",
    "manager",
]
