"""Pytest fixtures and test re-exports for migration integration tests."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.metrics import clear_metrics_registry
from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.application.migration.router import TenantStoreRouter
from eventsource.application.migration.write_pause import WritePauseManager
from tests.unit.application.migration.test_final_integration._events import (
    _REGISTRY,
    SOURCE_STORE_ID,
    TARGET_STORE_ID,
    OrderCreated,
    OrderUpdated,
    SampleTestEvent,
    src_pos,
    tgt_pos,
)
from tests.unit.application.migration.test_final_integration._helpers import (
    copy_events_between_stores,
    create_test_events,
)
from tests.unit.application.migration.test_final_integration._mocks import (
    InMemoryAuditLogRepository,
    InMemoryPositionMappingRepository,
    MockLockInfo,
    MockLockManager,
)
from tests.unit.application.migration.test_final_integration._repositories import (
    InMemoryCheckpointRepository,
    InMemoryMigrationRepository,
    InMemoryRoutingRepository,
)

if TYPE_CHECKING:
    from collections.abc import Generator


# =============================================================================
# Test Fixtures
# =============================================================================


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
def audit_log_repo() -> InMemoryAuditLogRepository:
    """Create audit log repository."""
    return InMemoryAuditLogRepository()


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


@pytest.fixture(autouse=True)
def cleanup_metrics() -> Generator[None]:
    """Clean up metrics registry before and after each test."""
    clear_metrics_registry()
    yield
    clear_metrics_registry()


__all__ = [  # pragma: allowlist secret
    "InMemoryAuditLogRepository",
    "InMemoryCheckpointRepository",
    "InMemoryMigrationRepository",
    "InMemoryPositionMappingRepository",
    "InMemoryRoutingRepository",
    "MockLockInfo",
    "MockLockManager",
    "OrderCreated",
    "OrderUpdated",
    "SOURCE_STORE_ID",
    "SampleTestEvent",
    "TARGET_STORE_ID",
    "_REGISTRY",
    "audit_log_repo",
    "checkpoint_repo",
    "cleanup_metrics",
    "copy_events_between_stores",
    "create_test_events",
    "lock_manager",
    "migration_id",
    "migration_repo",
    "position_mapper",
    "position_mapping_repo",
    "router",
    "routing_repo",
    "source_store",
    "src_pos",
    "target_store",
    "tenant_id",
    "tgt_pos",
    "write_pause_manager",
]
