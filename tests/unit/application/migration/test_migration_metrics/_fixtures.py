"""Shared fixtures and test helpers for migration metrics tests."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from eventsource.application.migration.metrics import clear_metrics_registry


@pytest.fixture(autouse=True)
def clean_metrics_registry():
    """Reset global metrics registry between tests."""
    clear_metrics_registry()
    yield
    clear_metrics_registry()


@pytest.fixture
def mock_counter() -> Mock:
    """Create mock counter."""
    counter = Mock()
    counter.add = Mock()
    return counter


@pytest.fixture
def mock_histogram() -> Mock:
    """Create mock histogram."""
    histogram = Mock()
    histogram.record = Mock()
    return histogram
