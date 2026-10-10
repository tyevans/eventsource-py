"""Fixtures for snapshot store tracing tests."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.observability import MockTracer
from eventsource.ports.snapshots import Snapshot


@pytest.fixture
def sample_snapshot() -> Snapshot:
    """Create a sample snapshot for testing."""
    return Snapshot(
        aggregate_id=uuid4(),
        aggregate_type="Order",
        version=100,
        state={"status": "shipped", "items": ["item1", "item2"]},
        schema_version=1,
        created_at=datetime.now(UTC),
    )


@pytest.fixture
def mock_tracer() -> MockTracer:
    """Create a MockTracer for testing span creation."""
    return MockTracer()
