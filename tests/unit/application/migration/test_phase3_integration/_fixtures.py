"""Pytest fixtures for Phase 3 integration tests."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.application.migration.router import TenantStoreRouter
from eventsource.application.migration.write_pause import WritePauseManager
from tests.unit.application.migration.test_phase3_integration._mocks import (
    InMemoryPositionMappingRepository,
    MockLockManager,
)
from tests.unit.application.migration.test_phase3_integration._repositories import (
    InMemoryCheckpointRepository,
    InMemoryMigrationRepository,
    InMemoryRoutingRepository,
)


@pytest.fixture
def source_store() -> InMemoryEventStore:
    """Create source (shared) event store."""
    return InMemoryEventStore("source")


@pytest.fixture
def target_store() -> InMemoryEventStore:
    """Create target (dedicated) event store."""
    return InMemoryEventStore("target")


@pytest.fixture
def migration_repo() -> InMemoryMigrationRepository:
    """Create migration repository."""
    return InMemoryMigrationRepository()


@pytest.fixture
def routing_repo() -> InMemoryRoutingRepository:
    """Create routing repository."""
    return InMemoryRoutingRepository()


@pytest.fixture
def position_mapping_repo() -> InMemoryPositionMappingRepository:
    """Create position mapping repository."""
    return InMemoryPositionMappingRepository()


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Create checkpoint repository."""
    return InMemoryCheckpointRepository()


@pytest.fixture
def lock_manager() -> MockLockManager:
    """Create mock lock manager."""
    return MockLockManager()


@pytest.fixture
def write_pause_manager() -> WritePauseManager:
    """Create write pause manager."""
    return WritePauseManager(default_timeout=5.0)


@pytest.fixture
def tenant_id() -> UUID:
    """Create a test tenant ID."""
    return uuid4()


@pytest.fixture
def migration_id() -> UUID:
    """Create a test migration ID."""
    return uuid4()


@pytest.fixture
def router(
    source_store: InMemoryEventStore,
    routing_repo: InMemoryRoutingRepository,
    write_pause_manager: WritePauseManager,
) -> TenantStoreRouter:
    """Create tenant store router."""
    return TenantStoreRouter(
        default_store=source_store,
        routing_repo=routing_repo,
        stores={"default": source_store},
        default_store_id="default",
        enable_tracing=False,
        write_pause_manager=write_pause_manager,
    )


@pytest.fixture
def position_mapper(
    position_mapping_repo: InMemoryPositionMappingRepository,
) -> PositionMapper:
    """Create position mapper."""
    return PositionMapper(position_mapping_repo, enable_tracing=False)


__all__ = [  # pragma: allowlist secret
    "checkpoint_repo",
    "lock_manager",
    "migration_id",
    "migration_repo",
    "position_mapper",
    "position_mapping_repo",
    "router",
    "routing_repo",
    "source_store",
    "target_store",
    "tenant_id",
    "write_pause_manager",
]
