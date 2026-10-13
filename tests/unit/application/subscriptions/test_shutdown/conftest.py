"""Pytest configuration and exposed fixtures for shutdown tests."""

from tests.unit.application.subscriptions.test_shutdown._fixtures import (
    coordinator,
    dummy_async_hook,
    mock_subscriber,
    signal_mock,
)

__all__ = [  # pragma: allowlist secret
    "coordinator",
    "dummy_async_hook",
    "mock_subscriber",
    "signal_mock",
]
