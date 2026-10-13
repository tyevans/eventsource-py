"""Shared fixtures and test helpers for migration chaos tests."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.router import TenantStoreRouter
from eventsource.application.migration.write_pause import WritePauseManager
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion

from ._infrastructure import (
    _REGISTRY,
    ChaosTestEvent,
    FailureConfig,
    FailureInjectableStore,
    FailureMode,
    InjectedFailureError,
)
from ._repositories import (
    InMemoryMigrationRepository,
    InMemoryRoutingRepository,
    MockLockInfo,
    MockLockManager,
)

# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def source_store() -> FailureInjectableStore:
    """Create failure-injectable source store."""
    return FailureInjectableStore()


@pytest.fixture
def target_store() -> FailureInjectableStore:
    """Create failure-injectable target store."""
    return FailureInjectableStore()


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
    source_store: FailureInjectableStore,
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


# =============================================================================
# Helper Functions
# =============================================================================


async def create_test_events(
    store: FailureInjectableStore | InMemoryEventStore,
    tenant_id: UUID,
    count: int = 10,
) -> list[UUID]:
    """Create test events in the store and return aggregate IDs."""
    aggregate_ids = []

    # If it's a FailureInjectableStore, use inner store
    actual_store = store.inner_store if isinstance(store, FailureInjectableStore) else store

    for i in range(count):
        aggregate_id = uuid4()
        aggregate_ids.append(aggregate_id)

        event = ChaosTestEvent(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
            value=f"event-{i}",
            sequence=i,
        )

        await actual_store.append(
            StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
            [event],
            ExpectedVersion.no_stream(),
        )

    return aggregate_ids


async def count_tenant_events(
    store: FailureInjectableStore | InMemoryEventStore,
    tenant_id: UUID,
) -> int:
    """Count events for a tenant in a store."""
    actual_store = store.inner_store if isinstance(store, FailureInjectableStore) else store

    count = 0
    async for envelope in actual_store.read_all():
        if envelope.event.tenant_id == tenant_id:
            count += 1
    return count


__all__ = [  # pragma: allowlist secret
    "ChaosTestEvent",
    "FailureConfig",
    "FailureInjectableStore",
    "FailureMode",
    "InMemoryMigrationRepository",
    "InMemoryRoutingRepository",
    "InjectedFailureError",
    "MockLockInfo",
    "MockLockManager",
    "_REGISTRY",
    "count_tenant_events",
    "create_test_events",
    "lock_manager",
    "migration_repo",
    "router",
    "routing_repo",
    "source_store",
    "target_store",
    "tenant_id",
    "write_pause_manager",
]
