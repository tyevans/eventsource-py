"""Tests for coordinator cutover rollback, completion, and resource lifecycle."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.ports.migration.models import (
    CutoverResult,
    Migration,
    MigrationPhase,
    TenantRouting,
)


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

        coordinator._lag_trackers[migration_id] = MagicMock()
        coordinator._target_stores[migration_id] = MagicMock()

        await coordinator._complete_cutover(migration)

        migration_repo.update_phase.assert_called_once_with(migration_id, MigrationPhase.COMPLETED)
        assert migration_id not in coordinator._lag_trackers
        assert migration_id not in coordinator._target_stores

    @pytest.mark.asyncio
    async def test_complete_cutover_releases_migration_metrics(self) -> None:
        """_complete_cutover() releases this migration's MigrationMetrics."""
        from eventsource.application.migration.metrics import (
            ActiveMigrationsTracker,
            clear_metrics_registry,
            get_migration_metrics,
        )

        clear_metrics_registry()
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
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

        get_migration_metrics(str(migration_id), str(tenant_id))
        tracker = ActiveMigrationsTracker.get_instance()
        assert str(migration_id) in tracker.active_migrations

        await coordinator._complete_cutover(migration)

        assert str(migration_id) not in tracker.active_migrations
        clear_metrics_registry()


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

        coordinator._lag_trackers[migration_id] = MagicMock()
        coordinator._target_stores[migration_id] = MagicMock()

        await coordinator.abort_migration(migration_id, reason="Test abort")

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

        coordinator._lag_trackers[migration_id] = MagicMock()
        coordinator._target_stores[migration_id] = MagicMock()

        await coordinator._fail_migration(migration, "Test failure")

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

        assert migration.id in coordinator._target_stores
        assert coordinator._target_stores[migration.id] is target_store
