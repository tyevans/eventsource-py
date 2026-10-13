"""Pytest configuration and shared fixtures for subscription metrics tests."""

from __future__ import annotations

import pytest

from ._fixtures import (
    mock_counter,
    mock_histogram,
    reset_meter,
    reset_metrics_registry,
)

__all__ = [  # pragma: allowlist secret
    "mock_counter",
    "mock_histogram",
    "reset_meter",
    "reset_metrics_registry",
]


@pytest.fixture(autouse=True)
def _auto_reset_metrics():
    """Automatically reset metrics registry before and after every test."""
    from eventsource.application.subscriptions.metrics import clear_metrics_registry

    clear_metrics_registry()
    yield
    clear_metrics_registry()
