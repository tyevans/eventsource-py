"""Shared fixtures and test helpers for status streamer tests."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.ports.migration.models import (
    MigrationPhase,
    MigrationStatus,
)


@pytest.fixture
def mock_coordinator() -> MagicMock:
    """Create a mock coordinator."""
    coordinator = MagicMock()
    coordinator.register_status_queue = MagicMock()
    coordinator.unregister_status_queue = MagicMock()
    return coordinator


@pytest.fixture
def base_status() -> MigrationStatus:
    """Create a base MigrationStatus."""
    return MigrationStatus(
        migration_id=uuid4(),
        tenant_id=uuid4(),
        phase=MigrationPhase.BULK_COPY,
        progress_percent=50.0,
        events_total=1000,
        events_copied=500,
        events_remaining=500,
        sync_lag_events=0,
        sync_lag_ms=0.0,
        error_count=0,
        started_at=datetime.now(UTC),
        phase_started_at=datetime.now(UTC),
        estimated_completion=None,
        current_rate_events_per_sec=100.0,
        is_paused=False,
    )


@pytest.fixture
def mock_deps() -> dict:
    """Create mock dependencies for MigrationCoordinator."""
    return {
        "source_store": MagicMock(),
        "migration_repo": AsyncMock(),
        "routing_repo": AsyncMock(),
        "router": MagicMock(),
    }
