"""Shared fixtures and test helpers for migration coordinator subscription tests."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from eventsource.application.migration.consistency import (
    VerificationLevel,
    VerificationReport,
)
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.subscription_migrator import (
    MigrationSummary,
)
from eventsource.ports.positions import Position


def source_pos(n: int) -> Position:
    """A source-store position token for test fixtures."""
    return Position(store_id="source-store", key=(n,))


def target_pos(n: int) -> Position:
    """A target-store position token for test fixtures."""
    return Position(store_id="target-store", key=(n,))


def make_verification_report(
    tenant_id: UUID,
    level: VerificationLevel = VerificationLevel.HASH,
    source_event_count: int = 1000,
    target_event_count: int = 1000,
    streams_verified: int = 10,
    is_consistent: bool = True,
    violations: list | None = None,
    stream_results: list | None = None,
    duration_seconds: float = 0.25,
) -> VerificationReport:
    """Helper to create VerificationReport with correct fields."""
    return VerificationReport(
        tenant_id=tenant_id,
        verification_level=level,
        is_consistent=is_consistent,
        source_event_count=source_event_count,
        target_event_count=target_event_count,
        streams_verified=streams_verified,
        streams_consistent=streams_verified if is_consistent else 0,
        streams_inconsistent=0 if is_consistent else streams_verified,
        sample_percentage=100.0,
        violations=violations or [],
        stream_results=stream_results or [],
        duration_seconds=duration_seconds,
        verified_at=datetime.now(UTC),
    )


def make_migration_summary(
    migration_id: UUID,
    tenant_id: UUID,
    results: list | None = None,
    successful_count: int = 0,
    failed_count: int = 0,
    skipped_count: int = 0,
) -> MigrationSummary:
    """Helper to create MigrationSummary with correct fields."""
    return MigrationSummary(
        migration_id=migration_id,
        tenant_id=tenant_id,
        results=results or [],
        successful_count=successful_count,
        failed_count=failed_count,
        skipped_count=skipped_count,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        duration_ms=100.0,
    )


def create_coordinator_deps() -> dict[str, Any]:
    """Create coordinator dependencies dict."""
    return {
        "source_store": AsyncMock(),
        "migration_repo": AsyncMock(),
        "routing_repo": AsyncMock(),
        "router": MagicMock(),
    }


@pytest.fixture
def coordinator_deps() -> dict[str, Any]:
    """Create coordinator dependencies."""
    return create_coordinator_deps()


def create_coordinator(**kwargs: Any) -> MigrationCoordinator:
    """Helper to instantiate MigrationCoordinator with default mocks."""
    deps = create_coordinator_deps()
    deps.update(kwargs)
    return MigrationCoordinator(**deps, enable_tracing=False)
