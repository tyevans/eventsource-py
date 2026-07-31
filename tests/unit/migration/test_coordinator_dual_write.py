"""
Unit tests for MigrationCoordinator dual-write and cutover functionality (P2-005).

Tests cover:
- Transition from BULK_COPY to DUAL_WRITE phase
- DualWriteInterceptor setup during transition
- SyncLagTracker setup and usage
- trigger_cutover() method
- Cutover success handling -> COMPLETED state
- Cutover failure handling -> rollback to DUAL_WRITE
- get_sync_lag() method
- is_cutover_ready() method
- Status including sync lag and cutover readiness
- Resource cleanup on abort/failure
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.migration.coordinator import MigrationCoordinator
from eventsource.migration.dual_write import DualWriteInterceptor
from eventsource.migration.exceptions import (
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
)
from eventsource.migration.models import (
    CutoverResult,
    Migration,
    MigrationConfig,
    MigrationPhase,
    MigrationStatus,
    SyncLag,
    TenantMigrationState,
    TenantRouting,
)
from eventsource.migration.sync_lag_tracker import SyncLagTracker


class TestTransitionToDualWrite:
    """Tests for MigrationCoordinator._transition_to_dual_write() method."""

    @pytest.fixture
    def coordinator_deps(self) -> dict:
        """Create coordinator dependencies."""
        return {
            "source_store": AsyncMock(),
            "migration_repo": AsyncMock(),
            "routing_repo": AsyncMock(),
            "router": MagicMock(),
        }

    @pytest.mark.asyncio
    async def test_transition_to_dual_write_creates_interceptor(
        self, coordinator_deps: dict
    ) -> None:
        """Test _transition_to_dual_write creates DualWriteInterceptor."""
        migration_id = uuid4()
        tenant_id = uuid4()
        target_store = AsyncMock()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        coordinator_deps["migration_repo"].update_phase = AsyncMock()
        coordinator_deps["routing_repo"].set_migration_state = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        await coordinator._transition_to_dual_write(migration, target_store)

        # Verify interceptor was set on router
        coordinator_deps["router"].set_dual_write_interceptor.assert_called_once()
        call_args = coordinator_deps["router"].set_dual_write_interceptor.call_args
        assert call_args[0][0] == tenant_id
        # Second arg should be DualWriteInterceptor
        interceptor = call_args[0][1]
        assert isinstance(interceptor, DualWriteInterceptor)

    @pytest.mark.asyncio
    async def test_transition_to_dual_write_creates_lag_tracker(
        self, coordinator_deps: dict
    ) -> None:
        """Test _transition_to_dual_write creates SyncLagTracker."""
        migration_id = uuid4()
        tenant_id = uuid4()
        target_store = AsyncMock()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        coordinator_deps["migration_repo"].update_phase = AsyncMock()
        coordinator_deps["routing_repo"].set_migration_state = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        await coordinator._transition_to_dual_write(migration, target_store)

        # Verify lag tracker was created
        assert migration_id in coordinator._lag_trackers
        lag_tracker = coordinator._lag_trackers[migration_id]
        assert isinstance(lag_tracker, SyncLagTracker)
        assert lag_tracker.tenant_id == tenant_id

    @pytest.mark.asyncio
    async def test_transition_to_dual_write_updates_phase(self, coordinator_deps: dict) -> None:
        """Test _transition_to_dual_write updates migration phase."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        coordinator_deps["migration_repo"].update_phase = AsyncMock()
        coordinator_deps["routing_repo"].set_migration_state = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        await coordinator._transition_to_dual_write(migration, AsyncMock())

        coordinator_deps["migration_repo"].update_phase.assert_called_once_with(
            migration_id, MigrationPhase.DUAL_WRITE
        )
        coordinator_deps["routing_repo"].set_migration_state.assert_called_once_with(
            tenant_id, TenantMigrationState.DUAL_WRITE, migration_id
        )


class TestTriggerCutover:
    """Tests for MigrationCoordinator.trigger_cutover() method."""

    @pytest.fixture
    def coordinator_deps(self) -> dict:
        """Create coordinator dependencies."""
        lock_manager = AsyncMock()
        return {
            "source_store": AsyncMock(),
            "migration_repo": AsyncMock(),
            "routing_repo": AsyncMock(),
            "router": MagicMock(),
            "lock_manager": lock_manager,
        }

    @pytest.mark.asyncio
    async def test_trigger_cutover_requires_lock_manager(self, coordinator_deps: dict) -> None:
        """Test trigger_cutover raises if lock_manager not provided."""
        migration_id = uuid4()

        # Remove lock_manager
        coordinator_deps.pop("lock_manager")

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        # Add lag tracker to pass validation
        coordinator._lag_trackers[migration_id] = MagicMock()

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.trigger_cutover(migration_id)

        assert "lock_manager not provided" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_trigger_cutover_raises_if_not_dual_write(self, coordinator_deps: dict) -> None:
        """Test trigger_cutover raises if not in DUAL_WRITE phase."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        with pytest.raises(MigrationStateError) as exc_info:
            await coordinator.trigger_cutover(migration_id)

        assert "bulk_copy" in str(exc_info.value).lower()
        assert exc_info.value.current_phase == MigrationPhase.BULK_COPY

    @pytest.mark.asyncio
    async def test_trigger_cutover_raises_if_no_lag_tracker(self, coordinator_deps: dict) -> None:
        """Test trigger_cutover raises if no lag tracker found."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        # No lag tracker added

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.trigger_cutover(migration_id)

        assert "No sync lag tracker found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_trigger_cutover_success(self, coordinator_deps: dict) -> None:
        """Test trigger_cutover completes migration on success."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
            config=MigrationConfig(),
        )

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator_deps["migration_repo"].update_phase = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        # Add lag tracker
        lag_tracker = MagicMock()
        lag_tracker.calculate_lag = AsyncMock()
        lag_tracker.current_lag = SyncLag(
            events=0, source_position=100, target_position=100, timestamp=datetime.now(UTC)
        )
        coordinator._lag_trackers[migration_id] = lag_tracker

        # Mock cutover manager execute_cutover to return success
        cutover_result = CutoverResult(success=True, duration_ms=50.0)
        with patch.object(coordinator, "_get_cutover_manager") as mock_get_cutover_manager:
            mock_cutover_manager = MagicMock()
            mock_cutover_manager.execute_cutover = AsyncMock(return_value=cutover_result)
            mock_get_cutover_manager.return_value = mock_cutover_manager

            result = await coordinator.trigger_cutover(migration_id)

        assert result.success is True
        assert result.duration_ms == 50.0

        # Verify phase was updated to CUTOVER then COMPLETED
        assert coordinator_deps["migration_repo"].update_phase.call_count >= 1

    @pytest.mark.asyncio
    async def test_trigger_cutover_failure_rollback(self, coordinator_deps: dict) -> None:
        """Test trigger_cutover rolls back on failure."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
            config=MigrationConfig(),
        )

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator_deps["migration_repo"].update_phase = AsyncMock()
        coordinator_deps["migration_repo"].record_error = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        # Add lag tracker
        lag_tracker = MagicMock()
        coordinator._lag_trackers[migration_id] = lag_tracker

        # Mock cutover manager execute_cutover to return failure
        cutover_result = CutoverResult(
            success=False,
            duration_ms=80.0,
            error_message="Timeout exceeded",
            rolled_back=True,
        )
        with patch.object(coordinator, "_get_cutover_manager") as mock_get_cutover_manager:
            mock_cutover_manager = MagicMock()
            mock_cutover_manager.execute_cutover = AsyncMock(return_value=cutover_result)
            mock_get_cutover_manager.return_value = mock_cutover_manager

            result = await coordinator.trigger_cutover(migration_id)

        assert result.success is False
        assert result.rolled_back is True

        # Verify phase was rolled back to DUAL_WRITE
        update_calls = coordinator_deps["migration_repo"].update_phase.call_args_list
        # Last call should be to DUAL_WRITE (rollback)
        last_call = update_calls[-1]
        assert last_call[0][1] == MigrationPhase.DUAL_WRITE

        # Verify error was recorded
        coordinator_deps["migration_repo"].record_error.assert_called()


class TestGetSyncLag:
    """Tests for MigrationCoordinator.get_sync_lag() method."""

    @pytest.mark.asyncio
    async def test_get_sync_lag_returns_lag(self) -> None:
        """Test get_sync_lag returns current sync lag."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        # Add lag tracker with specific lag
        expected_lag = SyncLag(
            events=50,
            source_position=1000,
            target_position=950,
            timestamp=datetime.now(UTC),
        )
        lag_tracker = MagicMock()
        lag_tracker.calculate_lag = AsyncMock(return_value=expected_lag)
        coordinator._lag_trackers[migration_id] = lag_tracker

        result = await coordinator.get_sync_lag(migration_id)

        assert result == expected_lag
        assert result.events == 50
        lag_tracker.calculate_lag.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_sync_lag_returns_none_if_no_tracker(self) -> None:
        """Test get_sync_lag returns None if no tracker found."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        result = await coordinator.get_sync_lag(migration_id)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_sync_lag_raises_if_not_found(self) -> None:
        """Test get_sync_lag raises if migration not found."""
        migration_id = uuid4()

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=None)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        with pytest.raises(MigrationNotFoundError):
            await coordinator.get_sync_lag(migration_id)


class TestIsCutoverReady:
    """Tests for MigrationCoordinator.is_cutover_ready() method."""

    @pytest.mark.asyncio
    async def test_is_cutover_ready_returns_true_when_ready(self) -> None:
        """Test is_cutover_ready returns True when sync lag is acceptable."""
        migration_id = uuid4()
        config = MigrationConfig(cutover_max_lag_events=100)

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
            config=config,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        # Add lag tracker with acceptable lag
        lag_tracker = MagicMock()
        lag_tracker.calculate_lag = AsyncMock()
        lag_tracker.is_sync_ready = MagicMock(return_value=True)
        coordinator._lag_trackers[migration_id] = lag_tracker

        ready, error = await coordinator.is_cutover_ready(migration_id)

        assert ready is True
        assert error is None

    @pytest.mark.asyncio
    async def test_is_cutover_ready_returns_false_when_lag_high(self) -> None:
        """Test is_cutover_ready returns False when sync lag is too high."""
        migration_id = uuid4()
        config = MigrationConfig(cutover_max_lag_events=100)

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
            config=config,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        # Add lag tracker with high lag
        lag = SyncLag(
            events=500,
            source_position=1000,
            target_position=500,
            timestamp=datetime.now(UTC),
        )
        lag_tracker = MagicMock()
        lag_tracker.calculate_lag = AsyncMock()
        lag_tracker.is_sync_ready = MagicMock(return_value=False)
        lag_tracker.current_lag = lag
        coordinator._lag_trackers[migration_id] = lag_tracker

        ready, error = await coordinator.is_cutover_ready(migration_id)

        assert ready is False
        assert "Sync lag too high" in error
        assert "500" in error

    @pytest.mark.asyncio
    async def test_is_cutover_ready_returns_false_if_wrong_phase(self) -> None:
        """Test is_cutover_ready returns False if not in DUAL_WRITE phase."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        ready, error = await coordinator.is_cutover_ready(migration_id)

        assert ready is False
        assert "bulk_copy" in error.lower()


class TestBuildStatusWithSyncLag:
    """Tests for MigrationCoordinator._build_status() with sync lag."""

    def test_build_status_includes_sync_lag(self) -> None:
        """Test _build_status includes sync lag from tracker."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
            events_total=1000,
            events_copied=1000,
            started_at=datetime.now(UTC),
        )

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        # Add lag tracker with current lag
        lag = SyncLag(
            events=25,
            source_position=1000,
            target_position=975,
            timestamp=datetime.now(UTC),
        )
        lag_tracker = MagicMock()
        lag_tracker.current_lag = lag
        coordinator._lag_trackers[migration_id] = lag_tracker

        status = coordinator._build_status(migration)

        assert isinstance(status, MigrationStatus)
        assert status.sync_lag_events == 25
        assert status.sync_lag_ms == 25.0  # lag_ms is same as events in SyncLag

    def test_build_status_zero_lag_without_tracker(self) -> None:
        """Test _build_status returns zero lag without tracker."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        # No lag tracker

        status = coordinator._build_status(migration)

        assert status.sync_lag_events == 0
        assert status.sync_lag_ms == 0.0


class TestCleanupMigrationResources:
    """Tests for MigrationCoordinator._cleanup_migration_resources() method."""

    def test_cleanup_removes_lag_tracker(self) -> None:
        """Test cleanup removes lag tracker."""
        migration_id = uuid4()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        coordinator._lag_trackers[migration_id] = MagicMock()
        coordinator._target_stores[migration_id] = MagicMock()

        coordinator._cleanup_migration_resources(migration_id)

        assert migration_id not in coordinator._lag_trackers
        assert migration_id not in coordinator._target_stores

    def test_cleanup_handles_missing_resources(self) -> None:
        """Test cleanup handles missing resources gracefully."""
        migration_id = uuid4()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        # No resources added
        # Should not raise
        coordinator._cleanup_migration_resources(migration_id)


class TestAbortMigrationCleansUpP2Resources:
    """Tests for abort_migration cleaning up P2 resources."""

    @pytest.mark.asyncio
    async def test_abort_cleans_up_lag_tracker(self) -> None:
        """Test abort_migration cleans up lag tracker."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
            events_copied=100,
            started_at=datetime.now(UTC),
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)
        migration_repo.update_phase = AsyncMock()
        migration_repo.record_error = AsyncMock()

        routing_repo = AsyncMock()
        routing_repo.clear_migration_state = AsyncMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=MagicMock(),
            enable_tracing=False,
        )

        # Add P2 resources
        coordinator._lag_trackers[migration_id] = MagicMock()
        coordinator._target_stores[migration_id] = MagicMock()

        await coordinator.abort_migration(migration_id, reason="Test abort")

        # Verify resources were cleaned up
        assert migration_id not in coordinator._lag_trackers
        assert migration_id not in coordinator._target_stores


class TestFailMigrationCleansUpP2Resources:
    """Tests for _fail_migration cleaning up P2 resources."""

    @pytest.mark.asyncio
    async def test_fail_cleans_up_lag_tracker(self) -> None:
        """Test _fail_migration cleans up lag tracker."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )

        migration_repo = AsyncMock()
        migration_repo.update_phase = AsyncMock()
        migration_repo.record_error = AsyncMock()

        routing_repo = AsyncMock()
        routing_repo.clear_migration_state = AsyncMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=MagicMock(),
            enable_tracing=False,
        )

        # Add P2 resources
        coordinator._lag_trackers[migration_id] = MagicMock()
        coordinator._target_stores[migration_id] = MagicMock()

        await coordinator._fail_migration(migration, "Test failure")

        # Verify resources were cleaned up
        assert migration_id not in coordinator._lag_trackers
        assert migration_id not in coordinator._target_stores


class TestGetCutoverManager:
    """Tests for MigrationCoordinator._get_cutover_manager() method."""

    def test_get_cutover_manager_creates_instance(self) -> None:
        """Test _get_cutover_manager creates CutoverManager instance."""
        lock_manager = MagicMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            lock_manager=lock_manager,
            enable_tracing=False,
        )

        cutover_manager = coordinator._get_cutover_manager()

        assert cutover_manager is not None
        assert coordinator._cutover_manager is cutover_manager

    def test_get_cutover_manager_returns_same_instance(self) -> None:
        """Test _get_cutover_manager returns same instance on repeated calls."""
        lock_manager = MagicMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            lock_manager=lock_manager,
            enable_tracing=False,
        )

        cutover_manager1 = coordinator._get_cutover_manager()
        cutover_manager2 = coordinator._get_cutover_manager()

        assert cutover_manager1 is cutover_manager2

    def test_get_cutover_manager_raises_without_lock_manager(self) -> None:
        """Test _get_cutover_manager raises if no lock_manager."""
        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        with pytest.raises(MigrationError) as exc_info:
            coordinator._get_cutover_manager()

        assert "lock_manager not provided" in str(exc_info.value)


class TestRollbackCutover:
    """Tests for MigrationCoordinator._rollback_cutover() method."""

    @pytest.mark.asyncio
    async def test_rollback_updates_phase_to_dual_write(self) -> None:
        """Test _rollback_cutover updates phase back to DUAL_WRITE."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.CUTOVER,
        )

        migration_repo = AsyncMock()
        migration_repo.update_phase = AsyncMock()
        migration_repo.record_error = AsyncMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        result = CutoverResult(
            success=False,
            duration_ms=100.0,
            error_message="Timeout exceeded",
            rolled_back=True,
        )

        await coordinator._rollback_cutover(migration, result)

        migration_repo.update_phase.assert_called_once_with(migration_id, MigrationPhase.DUAL_WRITE)

    @pytest.mark.asyncio
    async def test_rollback_records_error(self) -> None:
        """Test _rollback_cutover records error message."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.CUTOVER,
        )

        migration_repo = AsyncMock()
        migration_repo.update_phase = AsyncMock()
        migration_repo.record_error = AsyncMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        result = CutoverResult(
            success=False,
            duration_ms=100.0,
            error_message="Timeout exceeded",
            rolled_back=True,
        )

        await coordinator._rollback_cutover(migration, result)

        migration_repo.record_error.assert_called_once()
        error_arg = migration_repo.record_error.call_args[0][1]
        assert "Timeout exceeded" in error_arg


class TestCompleteCutover:
    """Tests for MigrationCoordinator._complete_cutover() method."""

    @pytest.mark.asyncio
    async def test_complete_cutover_updates_phase(self) -> None:
        """Test _complete_cutover updates phase to COMPLETED."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.CUTOVER,
        )

        migration_repo = AsyncMock()
        migration_repo.update_phase = AsyncMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        # Add resources to clean up
        coordinator._lag_trackers[migration_id] = MagicMock()
        coordinator._target_stores[migration_id] = MagicMock()

        await coordinator._complete_cutover(migration)

        migration_repo.update_phase.assert_called_once_with(migration_id, MigrationPhase.COMPLETED)

        # Verify resources were cleaned up
        assert migration_id not in coordinator._lag_trackers
        assert migration_id not in coordinator._target_stores


class TestStartMigrationStoresTargetStore:
    """Tests for start_migration storing target store reference."""

    @pytest.mark.asyncio
    async def test_start_migration_stores_target_store(self) -> None:
        """Test start_migration stores target store for later use."""
        tenant_id = uuid4()
        target_store = AsyncMock()
        target_store_id = "dedicated-tenant"

        migration_repo = AsyncMock()
        migration_repo.get_by_tenant = AsyncMock(return_value=None)
        migration_repo.create = AsyncMock()

        migration_id = uuid4()
        created_migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id=target_store_id,
            phase=MigrationPhase.BULK_COPY,
            started_at=datetime.now(UTC),
        )
        migration_repo.get = AsyncMock(return_value=created_migration)
        migration_repo.update_phase = AsyncMock()

        routing_repo = AsyncMock()
        routing_repo.get_or_default = AsyncMock(
            return_value=TenantRouting(tenant_id=tenant_id, store_id="default")
        )
        routing_repo.set_migration_state = AsyncMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=MagicMock(),
            enable_tracing=False,
        )

        with patch.object(coordinator, "_run_bulk_copy", new_callable=AsyncMock):
            migration = await coordinator.start_migration(
                tenant_id=tenant_id,
                target_store=target_store,
                target_store_id=target_store_id,
            )

        # Verify target store was stored
        assert migration.id in coordinator._target_stores
        assert coordinator._target_stores[migration.id] is target_store
