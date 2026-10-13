"""Shared fixtures and test helpers for coordinator dual-write tests."""

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
)


class TestEvent(DomainEvent):
    """Test event for unit tests."""

    __test__ = False
    aggregate_type: str = "TestAggregate"
    value: str = "test"


def create_coordinator_deps(include_lock_manager: bool = False) -> dict[str, Any]:
    """Create fresh coordinator mock dependencies."""
    deps: dict[str, Any] = {
        "source_store": AsyncMock(),
        "migration_repo": AsyncMock(),
        "routing_repo": AsyncMock(),
        "router": MagicMock(),
    }
    if include_lock_manager:
        deps["lock_manager"] = AsyncMock()
    return deps


@pytest.fixture
def coordinator_deps() -> dict[str, Any]:
    """Pytest fixture for coordinator dependencies."""
    return create_coordinator_deps()


def create_coordinator(
    source_store: Any = None,
    migration_repo: Any = None,
    routing_repo: Any = None,
    router: Any = None,
    lock_manager: Any = None,
    source_store_id: str = "default",
    enable_tracing: bool = False,
) -> MigrationCoordinator:
    """Helper to instantiate MigrationCoordinator with default mocks."""
    kwargs: dict[str, Any] = {
        "source_store": source_store or MagicMock(),
        "migration_repo": migration_repo or AsyncMock(),
        "routing_repo": routing_repo or AsyncMock(),
        "router": router or MagicMock(),
        "source_store_id": source_store_id,
        "enable_tracing": enable_tracing,
    }
    if lock_manager is not None:
        kwargs["lock_manager"] = lock_manager
    return MigrationCoordinator(**kwargs)


def create_test_migration(
    id: UUID | None = None,
    tenant_id: UUID | None = None,
    source_store_id: str = "default",
    target_store_id: str = "dedicated",
    phase: MigrationPhase = MigrationPhase.DUAL_WRITE,
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
