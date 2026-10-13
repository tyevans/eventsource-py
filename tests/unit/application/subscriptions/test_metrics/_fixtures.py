"""Test fixtures and mock helpers for subscription metrics tests."""

from __future__ import annotations

from unittest.mock import Mock

import pytest


@pytest.fixture
def reset_metrics_registry():
    """Reset global meter state between tests."""
    from eventsource.application.subscriptions.metrics import clear_metrics_registry

    clear_metrics_registry()
    yield
    clear_metrics_registry()


reset_meter = reset_metrics_registry


@pytest.fixture
def mock_counter():
    """Create mock counter."""
    counter = Mock()
    counter.add = Mock()
    return counter


@pytest.fixture
def mock_histogram():
    """Create mock histogram."""
    histogram = Mock()
    histogram.record = Mock()
    return histogram
