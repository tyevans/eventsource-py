"""Pytest fixtures for phase 2 migration integration tests."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.router import TenantStoreRouter
from eventsource.application.migration.write_pause import WritePauseManager
from tests.unit.application.migration.test_phase2_integration._events import _REGISTRY
from tests.unit.application.migration.test_phase2_integration._mocks import MockLockManager
from tests.unit.application.migration.test_phase2_integration._repositories import (
    InMemoryMigrationRepository,
    InMemoryRoutingRepository,
)


@pytest.fixture
def source_store() -> InMemoryEventStore:
    """Create source (shared) event store."""
    return InMemoryEventStore("source", event_registry=_REGISTRY)


@pytest.fixture
def target_store() -> InMemoryEventStore:
    """Create target (dedicated) event store."""
    return InMemoryEventStore("target", event_registry=_REGISTRY)


@pytest.fixture
def migration_repo() -> InMemoryMigrationRepository:
    """Create migration repository."""
    return InMemoryMigrationRepository()


@pytest.fixture
def routing_repo() -> InMemoryRoutingRepository:
    """Create routing repository."""
    return InMemoryRoutingRepository()


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
def coordinator(
    source_store: InMemoryEventStore,
    migration_repo: InMemoryMigrationRepository,
    routing_repo: InMemoryRoutingRepository,
    router: TenantStoreRouter,
    lock_manager: MockLockManager,
) -> MigrationCoordinator:
    """Create migration coordinator."""
    return MigrationCoordinator(
        source_store=source_store,
        migration_repo=migration_repo,
        routing_repo=routing_repo,
        router=router,
        lock_manager=lock_manager,
        source_store_id="default",
        enable_tracing=False,
    )


__all__ = [
    "coordinator",
    "lock_manager",
    "migration_repo",
    "router",
    "routing_repo",
    "source_store",
    "target_store",
    "tenant_id",
    "write_pause_manager",
]
