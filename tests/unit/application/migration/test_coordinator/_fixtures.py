"""Shared fixtures and test helpers for migration coordinator tests."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.domain.event import DomainEvent
from eventsource.ports.migration.models import (
    Migration,
    MigrationConfig,
    MigrationPhase,
    TenantRouting,
)


class TestEvent(DomainEvent):
    """Test event for unit tests."""

    __test__ = False
    aggregate_type: str = "TestAggregate"
    value: str = "test"


def create_coordinator_deps() -> dict[str, Any]:
    """Create fresh coordinator mock dependencies."""
    return {
        "source_store": AsyncMock(),
        "migration_repo": AsyncMock(),
        "routing_repo": AsyncMock(),
        "router": MagicMock(),
    }


@pytest.fixture
def coordinator_deps() -> dict[str, Any]:
    """Pytest fixture for coordinator dependencies."""
    return create_coordinator_deps()


def create_coordinator(
    source_store: Any = None,
    migration_repo: Any = None,
    routing_repo: Any = None,
    router: Any = None,
    source_store_id: str = "default",
    enable_tracing: bool = False,
) -> MigrationCoordinator:
    """Helper to instantiate MigrationCoordinator with default mocks."""
    return MigrationCoordinator(
        source_store=source_store or MagicMock(),
        migration_repo=migration_repo or AsyncMock(),
        routing_repo=routing_repo or AsyncMock(),
        router=router or MagicMock(),
        source_store_id=source_store_id,
        enable_tracing=enable_tracing,
    )


def create_test_migration(
    id: UUID | None = None,
    tenant_id: UUID | None = None,
    source_store_id: str = "default",
    target_store_id: str = "dedicated",
    phase: MigrationPhase = MigrationPhase.BULK_COPY,
    config: MigrationConfig | None = None,
    **kwargs: Any,
) -> Migration:
    """Helper to create a Migration instance with defaults."""
    return Migration(
        id=id or uuid4(),
        tenant_id=tenant_id or uuid4(),
        source_store_id=source_store_id,
        target_store_id=target_store_id,
        phase=phase,
        config=config or MigrationConfig(),
        **kwargs,
    )


def setup_start_migration(
    deps: dict[str, Any],
    tenant_id: UUID,
    migration: Migration | None,
) -> MigrationCoordinator:
    """Setup mock repository responses and return configured coordinator."""
    deps["migration_repo"].get_by_tenant = AsyncMock(return_value=None)
    deps["migration_repo"].create = AsyncMock()
    deps["migration_repo"].get = AsyncMock(return_value=migration)
    deps["migration_repo"].update_phase = AsyncMock()
    deps["routing_repo"].get_or_default = AsyncMock(
        return_value=TenantRouting(tenant_id=tenant_id, store_id="default")
    )
    deps["routing_repo"].set_migration_state = AsyncMock()
    return MigrationCoordinator(**deps, enable_tracing=False)
