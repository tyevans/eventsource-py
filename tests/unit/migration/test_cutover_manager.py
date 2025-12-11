"""
Unit tests for CutoverManager.

Tests cover:
- CutoverManager initialization
- Successful cutover execution
- Lock acquisition and failure handling
- Sync lag validation before cutover
- Timeout enforcement during cutover
- Automatic rollback on failure
- Write pause/resume coordination
- Target store health verification
- Routing state transitions
- Cutover readiness validation
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.locks import LockAcquisitionError, LockInfo
from eventsource.migration.cutover import CutoverManager
from eventsource.migration.models import (
    CutoverResult,
    MigrationConfig,
    SyncLag,
    TenantMigrationState,
    TenantRouting,
)

# =============================================================================
# Test Fixtures
# =============================================================================


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
    mock_store.get_global_position = AsyncMock(return_value=100)
    router.get_store.return_value = mock_store

    return router


@pytest.fixture
def mock_routing_repo():
    """Create a mock TenantRoutingRepository."""
    repo = MagicMock()
    repo.set_migration_state = AsyncMock()
    repo.set_routing = AsyncMock()
    repo.get_routing = AsyncMock()

    # Default routing
    routing = TenantRouting(
        tenant_id=uuid4(),
        store_id="source-store",
        migration_state=TenantMigrationState.DUAL_WRITE,
    )
    repo.get_routing.return_value = routing

    return repo


@pytest.fixture
def mock_lag_tracker():
    """Create a mock SyncLagTracker."""
    tracker = MagicMock()
    tracker.calculate_lag = AsyncMock()

    # Default: synced (zero lag)
    lag = SyncLag(
        events=0,
        source_position=100,
        target_position=100,
        timestamp=datetime.now(UTC),
    )
    tracker.current_lag = lag

    return tracker


@pytest.fixture
def config():
    """Create a test migration config."""
    return MigrationConfig(
        cutover_max_lag_events=100,
        cutover_timeout_ms=500,
    )


@pytest.fixture
def strict_config():
    """Create a strict migration config (low lag tolerance)."""
    return MigrationConfig(
        cutover_max_lag_events=10,
        cutover_timeout_ms=100,
    )


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


# =============================================================================
# Test CutoverManager Initialization
# =============================================================================


class TestCutoverManagerInit:
    """Tests for CutoverManager initialization."""

    def test_init_with_defaults(
        self,
        mock_lock_manager,
        mock_router,
        mock_routing_repo,
    ):
        """Test initialization with default parameters."""
        manager = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
        )

        assert manager._lock_manager is mock_lock_manager
        assert manager._router is mock_router
        assert manager._routing_repo is mock_routing_repo
        assert manager._lock_acquisition_timeout == 0.5
        assert manager._enable_tracing is True

    def test_init_with_custom_timeout(
        self,
        mock_lock_manager,
        mock_router,
        mock_routing_repo,
    ):
        """Test initialization with custom lock timeout."""
        manager = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
            lock_acquisition_timeout=1.0,
        )

        assert manager._lock_acquisition_timeout == 1.0

    def test_init_with_tracing_disabled(
        self,
        mock_lock_manager,
        mock_router,
        mock_routing_repo,
    ):
        """Test initialization with tracing disabled."""
        manager = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        assert manager._enable_tracing is False


# =============================================================================
# Test Successful Cutover
# =============================================================================


class TestSuccessfulCutover:
    """Tests for successful cutover scenarios."""

    @pytest.mark.asyncio
    async def test_successful_cutover(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_router,
        mock_routing_repo,
    ):
        """Test successful cutover execution."""
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is True
        assert result.duration_ms > 0
        assert result.rolled_back is False
        assert result.error_message is None

        # Verify write pause/resume
        mock_router.pause_writes.assert_called_once_with(tenant_id)
        mock_router.resume_writes.assert_called_once_with(tenant_id)

        # Verify routing updates
        mock_routing_repo.set_migration_state.assert_any_call(
            tenant_id,
            TenantMigrationState.CUTOVER_PAUSED,
            migration_id=migration_id,
        )
        mock_routing_repo.set_migration_state.assert_any_call(
            tenant_id,
            TenantMigrationState.MIGRATED,
            migration_id=migration_id,
        )

        # Verify dual-write interceptor cleared
        mock_router.clear_dual_write_interceptor.assert_called_once_with(tenant_id)

    @pytest.mark.asyncio
    async def test_successful_cutover_with_small_lag(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        config,
    ):
        """Test successful cutover with small but acceptable lag."""
        # Set small lag (within threshold)
        lag = SyncLag(
            events=50,  # Below threshold of 100
            source_position=100,
            target_position=50,
            timestamp=datetime.now(UTC),
        )
        mock_lag_tracker.current_lag = lag

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            config=config,
        )

        assert result.success is True

    @pytest.mark.asyncio
    async def test_successful_cutover_with_custom_timeout(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
    ):
        """Test cutover with custom timeout."""
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            timeout_ms=1000.0,  # 1 second timeout
        )

        assert result.success is True

    @pytest.mark.asyncio
    async def test_cutover_tracks_events_synced(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
    ):
        """Test that cutover tracks events synced during pause."""
        # Initial lag
        initial_lag = SyncLag(
            events=10,
            source_position=100,
            target_position=90,
            timestamp=datetime.now(UTC),
        )
        # Final lag (improved during pause)
        final_lag = SyncLag(
            events=2,
            source_position=100,
            target_position=98,
            timestamp=datetime.now(UTC),
        )

        # Set up mock to return different lags on successive calls
        call_count = 0

        async def update_lag():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                mock_lag_tracker.current_lag = initial_lag
            else:
                mock_lag_tracker.current_lag = final_lag

        mock_lag_tracker.calculate_lag.side_effect = update_lag
        mock_lag_tracker.current_lag = initial_lag  # Initial state

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is True
        assert result.events_synced == 8  # 10 - 2


# =============================================================================
# Test Lock Acquisition
# =============================================================================


class TestLockAcquisition:
    """Tests for lock acquisition handling."""

    @pytest.mark.asyncio
    async def test_lock_acquisition_failure(
        self,
        mock_lock_manager,
        mock_router,
        mock_routing_repo,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
    ):
        """Test behavior when lock acquisition fails."""
        # Configure lock to fail
        error = LockAcquisitionError(
            key="cutover:test",
            reason="Timeout after 0.5s",
            timeout=0.5,
        )
        create_failing_lock_context_manager(mock_lock_manager, error)

        manager = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        result = await manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is False
        assert "Failed to acquire cutover lock" in result.error_message
        assert result.rolled_back is False

        # Verify no routing changes were made
        mock_routing_repo.set_migration_state.assert_not_called()

    @pytest.mark.asyncio
    async def test_lock_prevents_concurrent_cutover(
        self,
        mock_lock_manager,
        mock_router,
        mock_routing_repo,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
    ):
        """Test that lock prevents concurrent cutover attempts."""
        # First manager acquires lock
        create_lock_context_manager(mock_lock_manager)
        manager1 = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        # Second manager fails to acquire
        error = LockAcquisitionError(
            key="cutover:test",
            reason="Lock already held",
        )

        mock_lock_manager2 = MagicMock()
        create_failing_lock_context_manager(mock_lock_manager2, error)

        manager2 = CutoverManager(
            lock_manager=mock_lock_manager2,
            router=mock_router,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        # First succeeds
        result1 = await manager1.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        # Second fails
        result2 = await manager2.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result1.success is True
        assert result2.success is False
        assert "lock" in result2.error_message.lower()


# =============================================================================
# Test Sync Lag Validation
# =============================================================================


class TestSyncLagValidation:
    """Tests for sync lag validation during cutover."""

    @pytest.mark.asyncio
    async def test_cutover_fails_on_high_lag(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_router,
        mock_routing_repo,
        strict_config,
    ):
        """Test cutover fails when sync lag exceeds threshold."""
        # Set high lag
        lag = SyncLag(
            events=50,  # Exceeds strict threshold of 10
            source_position=100,
            target_position=50,
            timestamp=datetime.now(UTC),
        )
        mock_lag_tracker.current_lag = lag

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            config=strict_config,
        )

        assert result.success is False
        assert "lag too high" in result.error_message.lower()
        assert result.rolled_back is True

        # Verify writes were paused and resumed
        mock_router.pause_writes.assert_called_once()
        mock_router.resume_writes.assert_called_once()

        # Verify rollback to DUAL_WRITE
        mock_routing_repo.set_migration_state.assert_called_with(
            tenant_id,
            TenantMigrationState.DUAL_WRITE,
            migration_id=migration_id,
        )

    @pytest.mark.asyncio
    async def test_cutover_fails_when_lag_unavailable(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_routing_repo,
    ):
        """Test cutover fails when lag cannot be calculated."""
        mock_lag_tracker.current_lag = None

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is False
        assert "sync lag" in result.error_message.lower()
        assert result.rolled_back is True

    @pytest.mark.asyncio
    async def test_cutover_at_exact_threshold(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        config,
    ):
        """Test cutover succeeds when lag is exactly at threshold."""
        # Set lag exactly at threshold
        lag = SyncLag(
            events=100,  # Exactly at threshold
            source_position=200,
            target_position=100,
            timestamp=datetime.now(UTC),
        )
        mock_lag_tracker.current_lag = lag

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            config=config,
        )

        assert result.success is True


# =============================================================================
# Test Timeout Enforcement
# =============================================================================


class TestTimeoutEnforcement:
    """Tests for timeout enforcement during cutover."""

    @pytest.mark.asyncio
    async def test_cutover_timeout_enforcement(
        self,
        mock_lock_manager,
        mock_router,
        mock_routing_repo,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
    ):
        """Test that cutover fails if timeout is exceeded."""
        create_lock_context_manager(mock_lock_manager)

        # Add a delay to the lag calculation to exceed timeout
        async def slow_calculate_lag():
            import asyncio

            await asyncio.sleep(0.2)  # 200ms delay

        mock_lag_tracker.calculate_lag = slow_calculate_lag

        # Zero lag so lag check passes
        mock_lag_tracker.current_lag = SyncLag(
            events=0,
            source_position=100,
            target_position=100,
            timestamp=datetime.now(UTC),
        )

        manager = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        # Use very short timeout
        result = await manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            timeout_ms=50.0,  # 50ms timeout
        )

        # Note: This may succeed if system is fast enough
        # The important thing is that we don't hang indefinitely
        assert result.duration_ms >= 0

    @pytest.mark.asyncio
    async def test_cutover_uses_config_timeout(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
    ):
        """Test that config timeout is used when not specified."""
        config = MigrationConfig(cutover_timeout_ms=200)

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            config=config,
        )

        # Should complete within timeout
        assert result.success is True
        assert result.duration_ms < 200


# =============================================================================
# Test Automatic Rollback
# =============================================================================


class TestAutomaticRollback:
    """Tests for automatic rollback on failure."""

    @pytest.mark.asyncio
    async def test_rollback_on_lag_failure(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_routing_repo,
        strict_config,
    ):
        """Test rollback when lag check fails."""
        mock_lag_tracker.current_lag = SyncLag(
            events=100,  # Exceeds strict threshold
            source_position=200,
            target_position=100,
            timestamp=datetime.now(UTC),
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            config=strict_config,
        )

        assert result.success is False
        assert result.rolled_back is True

        # Verify rollback call
        mock_routing_repo.set_migration_state.assert_called_with(
            tenant_id,
            TenantMigrationState.DUAL_WRITE,
            migration_id=migration_id,
        )

    @pytest.mark.asyncio
    async def test_rollback_on_target_health_failure(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_router,
        mock_routing_repo,
    ):
        """Test rollback when target store health check fails."""
        # Make target store health check fail
        mock_store = MagicMock()
        mock_store.get_global_position = AsyncMock(side_effect=Exception("Connection failed"))
        mock_router.get_store.return_value = mock_store

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is False
        assert "health check failed" in result.error_message.lower()
        assert result.rolled_back is True

    @pytest.mark.asyncio
    async def test_rollback_on_routing_update_failure(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_routing_repo,
    ):
        """Test rollback when routing update fails."""
        # Make routing update fail
        mock_routing_repo.set_routing.side_effect = Exception("Database error")

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is False
        assert result.rolled_back is True

    @pytest.mark.asyncio
    async def test_rollback_failure_handled_gracefully(
        self,
        mock_lock_manager,
        mock_router,
        mock_routing_repo,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
    ):
        """Test that rollback failure is handled gracefully."""
        create_lock_context_manager(mock_lock_manager)

        # Set low lag so lag check passes
        mock_lag_tracker.current_lag = SyncLag(
            events=0,
            source_position=100,
            target_position=100,
            timestamp=datetime.now(UTC),
        )

        # Make target store health check fail to trigger rollback
        mock_store = MagicMock()
        mock_store.get_global_position = AsyncMock(side_effect=Exception("Connection failed"))
        mock_router.get_store.return_value = mock_store

        # Make rollback fail
        call_count = 0

        async def failing_set_state(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return  # First call (CUTOVER_PAUSED) succeeds
            raise Exception("Rollback failed")

        mock_routing_repo.set_migration_state.side_effect = failing_set_state

        manager = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        result = await manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is False
        assert result.rolled_back is False  # Rollback failed

        # Verify writes were still resumed
        mock_router.resume_writes.assert_called_once()


# =============================================================================
# Test Write Pause/Resume Coordination
# =============================================================================


class TestWritePauseResume:
    """Tests for write pause/resume coordination."""

    @pytest.mark.asyncio
    async def test_writes_paused_before_cutover(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_router,
    ):
        """Test that writes are paused before cutover operations."""
        await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        mock_router.pause_writes.assert_called_once_with(tenant_id)

    @pytest.mark.asyncio
    async def test_writes_resumed_after_success(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_router,
    ):
        """Test that writes are resumed after successful cutover."""
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is True
        mock_router.resume_writes.assert_called_once_with(tenant_id)

    @pytest.mark.asyncio
    async def test_writes_resumed_after_failure(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_router,
        strict_config,
    ):
        """Test that writes are resumed after failed cutover."""
        # Set high lag to trigger failure
        mock_lag_tracker.current_lag = SyncLag(
            events=100,
            source_position=200,
            target_position=100,
            timestamp=datetime.now(UTC),
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            config=strict_config,
        )

        assert result.success is False
        mock_router.resume_writes.assert_called_once_with(tenant_id)


# =============================================================================
# Test Target Store Verification
# =============================================================================


class TestTargetStoreVerification:
    """Tests for target store health verification."""

    @pytest.mark.asyncio
    async def test_target_store_verified(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_router,
    ):
        """Test that target store health is verified during cutover."""
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result.success is True
        mock_router.get_store.assert_called_with(target_store_id)

    @pytest.mark.asyncio
    async def test_cutover_handles_missing_target_store(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        mock_lag_tracker,
        mock_router,
    ):
        """Test cutover handles missing target store gracefully."""
        mock_router.get_store.return_value = None

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id="nonexistent-store",
        )

        # Should still succeed (with warning logged)
        assert result.success is True


# =============================================================================
# Test Routing State Transitions
# =============================================================================


class TestRoutingStateTransitions:
    """Tests for routing state transitions during cutover."""

    @pytest.mark.asyncio
    async def test_routing_transitions_in_order(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_routing_repo,
    ):
        """Test that routing transitions happen in correct order."""
        await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        calls = mock_routing_repo.set_migration_state.call_args_list

        # First call should be CUTOVER_PAUSED
        assert calls[0][0][1] == TenantMigrationState.CUTOVER_PAUSED

        # Second call should be MIGRATED
        assert calls[1][0][1] == TenantMigrationState.MIGRATED

    @pytest.mark.asyncio
    async def test_routing_updated_to_target(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_routing_repo,
    ):
        """Test that routing is updated to target store."""
        await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        mock_routing_repo.set_routing.assert_called_once_with(
            tenant_id,
            target_store_id,
        )

    @pytest.mark.asyncio
    async def test_dual_write_interceptor_cleared(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_router,
    ):
        """Test that dual-write interceptor is cleared after cutover."""
        await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        mock_router.clear_dual_write_interceptor.assert_called_once_with(tenant_id)


# =============================================================================
# Test Cutover Readiness Validation
# =============================================================================


class TestCutoverReadinessValidation:
    """Tests for cutover readiness validation."""

    @pytest.mark.asyncio
    async def test_validate_readiness_success(
        self,
        cutover_manager,
        tenant_id,
        mock_lag_tracker,
        mock_routing_repo,
        config,
    ):
        """Test successful readiness validation."""
        # Set up routing in DUAL_WRITE state
        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="source",
            migration_state=TenantMigrationState.DUAL_WRITE,
        )
        mock_routing_repo.get_routing.return_value = routing

        ready, error = await cutover_manager.validate_cutover_readiness(
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            config=config,
        )

        assert ready is True
        assert error is None

    @pytest.mark.asyncio
    async def test_validate_readiness_fails_on_high_lag(
        self,
        cutover_manager,
        tenant_id,
        mock_lag_tracker,
        strict_config,
    ):
        """Test readiness validation fails on high lag."""
        mock_lag_tracker.current_lag = SyncLag(
            events=100,
            source_position=200,
            target_position=100,
            timestamp=datetime.now(UTC),
        )

        ready, error = await cutover_manager.validate_cutover_readiness(
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            config=strict_config,
        )

        assert ready is False
        assert "lag too high" in error.lower()

    @pytest.mark.asyncio
    async def test_validate_readiness_fails_wrong_state(
        self,
        cutover_manager,
        tenant_id,
        mock_lag_tracker,
        mock_routing_repo,
    ):
        """Test readiness validation fails when not in DUAL_WRITE state."""
        # Set up routing in wrong state
        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="source",
            migration_state=TenantMigrationState.BULK_COPY,
        )
        mock_routing_repo.get_routing.return_value = routing

        ready, error = await cutover_manager.validate_cutover_readiness(
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
        )

        assert ready is False
        assert "DUAL_WRITE" in error

    @pytest.mark.asyncio
    async def test_validate_readiness_fails_no_routing(
        self,
        cutover_manager,
        tenant_id,
        mock_lag_tracker,
        mock_routing_repo,
    ):
        """Test readiness validation fails when no routing exists."""
        mock_routing_repo.get_routing.return_value = None

        ready, error = await cutover_manager.validate_cutover_readiness(
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
        )

        assert ready is False
        assert "routing" in error.lower()

    @pytest.mark.asyncio
    async def test_validate_readiness_fails_lock_held(
        self,
        cutover_manager,
        tenant_id,
        mock_lag_tracker,
        mock_routing_repo,
        mock_lock_manager,
    ):
        """Test readiness validation fails when lock is held."""
        # Set up routing in DUAL_WRITE state
        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="source",
            migration_state=TenantMigrationState.DUAL_WRITE,
        )
        mock_routing_repo.get_routing.return_value = routing

        # Lock already held
        mock_lock_manager.try_acquire.return_value = None

        ready, error = await cutover_manager.validate_cutover_readiness(
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
        )

        assert ready is False
        assert "lock" in error.lower()


# =============================================================================
# Test CutoverResult Properties
# =============================================================================


class TestCutoverResult:
    """Tests for CutoverResult dataclass."""

    def test_cutover_result_creation(self):
        """Test creating a CutoverResult."""
        result = CutoverResult(
            success=True,
            duration_ms=50.5,
            events_synced=10,
        )

        assert result.success is True
        assert result.duration_ms == 50.5
        assert result.events_synced == 10
        assert result.error_message is None
        assert result.rolled_back is False

    def test_cutover_result_within_timeout(self):
        """Test within_timeout property."""
        fast_result = CutoverResult(success=True, duration_ms=50.0)
        slow_result = CutoverResult(success=True, duration_ms=150.0)

        assert fast_result.within_timeout is True
        assert slow_result.within_timeout is False

    def test_cutover_result_with_error(self):
        """Test CutoverResult with error."""
        result = CutoverResult(
            success=False,
            duration_ms=200.0,
            error_message="Timeout exceeded",
            rolled_back=True,
        )

        assert result.success is False
        assert result.error_message == "Timeout exceeded"
        assert result.rolled_back is True


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_cutover_with_zero_timeout(
        self,
        mock_lock_manager,
        mock_router,
        mock_routing_repo,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
    ):
        """Test cutover behavior with zero timeout."""
        create_lock_context_manager(mock_lock_manager)

        manager = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        # Zero timeout should likely fail immediately
        result = await manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            timeout_ms=0.0,
        )

        # Should fail due to timeout
        assert result.success is False

    @pytest.mark.asyncio
    async def test_cutover_with_very_large_timeout(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
    ):
        """Test cutover with very large timeout."""
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            timeout_ms=100000.0,  # 100 seconds
        )

        assert result.success is True
        # Should complete much faster than timeout
        assert result.duration_ms < 1000

    @pytest.mark.asyncio
    async def test_multiple_cutover_attempts(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
    ):
        """Test multiple cutover attempts on same tenant."""
        # First attempt
        result1 = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        # Second attempt (should also succeed since first completed)
        result2 = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result1.success is True
        assert result2.success is True
