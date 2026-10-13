"""Shared fixtures and test helpers for CutoverManager unit tests."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.cutover import CutoverManager
from eventsource.ports import Position
from eventsource.ports.locks import LockInfo
from eventsource.ports.migration.models import (
    MigrationConfig,
    SyncLag,
    TenantMigrationState,
    TenantRouting,
)


@pytest.fixture
def tenant_id():
    """Generate a test tenant ID."""
    return uuid4()


@pytest.fixture
def migration_id():
    """Generate a test migration ID."""
    return uuid4()


@pytest.fixture
def target_store_id():
    """Test target store ID."""
    return "dedicated-tenant-abc"


@pytest.fixture
def mock_lock_manager():
    """Create a mock PostgreSQL lock manager."""
    manager = MagicMock()
    manager.acquire = MagicMock()
    manager.try_acquire = AsyncMock()
    manager.release = AsyncMock()

    # Default: acquire succeeds
    lock_info = LockInfo(
        key="test-lock",
        lock_id=12345,
        acquired_at=datetime.now(UTC),
    )
    manager.try_acquire.return_value = lock_info

    return manager


@pytest.fixture
def mock_router():
    """Create a mock TenantStoreRouter."""
    router = MagicMock()
    router.pause_writes = AsyncMock()
    router.resume_writes = AsyncMock()
    router.clear_dual_write_interceptor = MagicMock()
    router.get_store = MagicMock()

    # Mock target store
    mock_store = MagicMock()
    mock_store.current_position = AsyncMock(return_value=Position(store_id="target", key=(100,)))
    router.get_store.return_value = mock_store

    return router


@pytest.fixture
def mock_routing_repo():
    """Create a mock TenantRoutingRepository."""
    r = TenantRouting(
        tenant_id=uuid4(), store_id="source-store", migration_state=TenantMigrationState.DUAL_WRITE
    )
    repo = MagicMock(
        get_routing=AsyncMock(return_value=r),
        set_migration_state=AsyncMock(),
        set_routing=AsyncMock(),
    )

    async def _switch(t_id, s_id, state=TenantMigrationState.MIGRATED, migration_id=None):
        await repo.set_routing(t_id, s_id)
        await repo.set_migration_state(t_id, state, migration_id=migration_id)

    repo.switch_routing = AsyncMock(side_effect=_switch)
    return repo


@pytest.fixture
def mock_lag_tracker():
    """Create a mock SyncLagTracker."""
    tracker = MagicMock()
    tracker.calculate_lag = AsyncMock()

    # Default: synced (zero lag)
    lag = SyncLag(
        events=0,
        source_position=Position(store_id="source", key=(100,)),
        target_position=Position(store_id="target", key=(100,)),
        timestamp=datetime.now(UTC),
    )
    tracker.current_lag = lag

    return tracker


@pytest.fixture
def config():
    """Create a test migration config."""
    return MigrationConfig(cutover_max_lag_events=100, cutover_timeout_ms=500)


@pytest.fixture
def strict_config():
    """Create a strict migration config (low lag tolerance)."""
    return MigrationConfig(cutover_max_lag_events=10, cutover_timeout_ms=100)


def create_lock_context_manager(lock_manager: MagicMock):
    """Create an async context manager for lock acquisition."""

    class LockContextManager:
        async def __aenter__(self):
            return LockInfo(
                key="test-lock",
                lock_id=12345,
                acquired_at=datetime.now(UTC),
            )

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False

    lock_manager.acquire.return_value = LockContextManager()
    return lock_manager


def create_failing_lock_context_manager(lock_manager: MagicMock, error: Exception):
    """Create a context manager that fails on acquisition."""

    class FailingLockContextManager:
        async def __aenter__(self):
            raise error

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            return False

    lock_manager.acquire.return_value = FailingLockContextManager()
    return lock_manager


@pytest.fixture
def cutover_manager(
    mock_lock_manager,
    mock_router,
    mock_routing_repo,
):
    """Create a CutoverManager with mock dependencies."""
    create_lock_context_manager(mock_lock_manager)
    return CutoverManager(
        lock_manager=mock_lock_manager,
        router=mock_router,
        routing_repo=mock_routing_repo,
        enable_tracing=False,
    )


__all__ = [  # pragma: allowlist secret
    "config",
    "create_failing_lock_context_manager",
    "create_lock_context_manager",
    "cutover_manager",
    "migration_id",
    "mock_lag_tracker",
    "mock_lock_manager",
    "mock_router",
    "mock_routing_repo",
    "strict_config",
    "target_store_id",
    "tenant_id",
]
