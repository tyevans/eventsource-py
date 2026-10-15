"""Tests for coordinator cutover rollback, completion, and resource lifecycle."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.ports.migration.models import (
    CutoverResult,
    Migration,
    MigrationPhase,
    TenantRouting,
)


def _make_coordinator(
    migration_repo: AsyncMock | MagicMock | None = None,
    routing_repo: AsyncMock | MagicMock | None = None,
) -> MigrationCoordinator:
    return MigrationCoordinator(
        source_store=MagicMock(),
        migration_repo=migration_repo or MagicMock(),
        routing_repo=routing_repo or MagicMock(),
        router=MagicMock(),
        enable_tracing=False,
    )


def _make_migration(
    migration_id: UUID | None = None,
    tenant_id: UUID | None = None,
    phase: MigrationPhase = MigrationPhase.CUTOVER,
) -> Migration:
    return Migration(
        id=migration_id or uuid4(),
        tenant_id=tenant_id or uuid4(),
        source_store_id="default",
        target_store_id="dedicated",
        phase=phase,
    )


class TestRollbackCutover:
    """Tests for MigrationCoordinator._rollback_cutover() method."""

    @pytest.mark.asyncio
    async def test_rollback_updates_phase_to_dual_write(self) -> None:
        migration = _make_migration()
        migration_repo = AsyncMock(update_phase=AsyncMock(), record_error=AsyncMock())
        coordinator = _make_coordinator(migration_repo=migration_repo)
        result = CutoverResult(
            success=False, duration_ms=100.0, error_message="Timeout exceeded", rolled_back=True
        )
        await coordinator._rollback_cutover(migration, result)
        migration_repo.update_phase.assert_called_once_with(migration.id, MigrationPhase.DUAL_WRITE)

    @pytest.mark.asyncio
    async def test_rollback_records_error(self) -> None:
        migration = _make_migration()
        migration_repo = AsyncMock(update_phase=AsyncMock(), record_error=AsyncMock())
        coordinator = _make_coordinator(migration_repo=migration_repo)
        result = CutoverResult(
            success=False, duration_ms=100.0, error_message="Timeout exceeded", rolled_back=True
        )
        await coordinator._rollback_cutover(migration, result)
        migration_repo.record_error.assert_called_once()
        assert "Timeout exceeded" in migration_repo.record_error.call_args[0][1]


class TestCompleteCutover:
    """Tests for MigrationCoordinator._complete_cutover() method."""

    @pytest.mark.asyncio
    async def test_complete_cutover_updates_phase(self) -> None:
        migration = _make_migration()
        migration_repo = AsyncMock(update_phase=AsyncMock())
        coordinator = _make_coordinator(migration_repo=migration_repo)
        coordinator._lag_trackers[migration.id] = MagicMock()
        coordinator._target_stores[migration.id] = MagicMock()
        await coordinator._complete_cutover(migration)
        migration_repo.update_phase.assert_called_once_with(migration.id, MigrationPhase.COMPLETED)
        assert migration.id not in coordinator._lag_trackers
        assert migration.id not in coordinator._target_stores

    @pytest.mark.asyncio
    async def test_complete_cutover_releases_migration_metrics(self) -> None:
        from eventsource.application.migration.metrics import (
            ActiveMigrationsTracker,
            clear_metrics_registry,
            get_migration_metrics,
        )

        clear_metrics_registry()
        migration = _make_migration()
        coordinator = _make_coordinator(migration_repo=AsyncMock(update_phase=AsyncMock()))
        get_migration_metrics(str(migration.id), str(migration.tenant_id))
        tracker = ActiveMigrationsTracker.get_instance()
        assert str(migration.id) in tracker.active_migrations
        await coordinator._complete_cutover(migration)
        assert str(migration.id) not in tracker.active_migrations
        clear_metrics_registry()


class TestCleanupMigrationResources:
    """Tests for MigrationCoordinator._cleanup_migration_resources() method."""

    def test_cleanup_removes_lag_tracker(self) -> None:
        migration_id = uuid4()
        coordinator = _make_coordinator()
        coordinator._lag_trackers[migration_id] = MagicMock()
        coordinator._target_stores[migration_id] = MagicMock()
        coordinator._cleanup_migration_resources(migration_id)
        assert migration_id not in coordinator._lag_trackers
        assert migration_id not in coordinator._target_stores

    def test_cleanup_handles_missing_resources(self) -> None:
        _make_coordinator()._cleanup_migration_resources(uuid4())


class TestAbortMigrationCleansUpP2Resources:
    """Tests for abort_migration cleaning up P2 resources."""

    @pytest.mark.asyncio
    async def test_abort_cleans_up_lag_tracker(self) -> None:
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
            events_copied=100,
            started_at=datetime.now(UTC),
        )
        migration_repo = AsyncMock(
            get=AsyncMock(return_value=migration),
            update_phase=AsyncMock(),
            record_error=AsyncMock(),
        )
        routing_repo = AsyncMock(clear_migration_state=AsyncMock())
        coordinator = _make_coordinator(migration_repo=migration_repo, routing_repo=routing_repo)
        coordinator._lag_trackers[migration_id] = MagicMock()
        coordinator._target_stores[migration_id] = MagicMock()
        await coordinator.abort_migration(migration_id, reason="Test abort")
        assert migration_id not in coordinator._lag_trackers
        assert migration_id not in coordinator._target_stores


class TestFailMigrationCleansUpP2Resources:
    """Tests for _fail_migration cleaning up P2 resources."""

    @pytest.mark.asyncio
    async def test_fail_cleans_up_lag_tracker(self) -> None:
        migration = _make_migration(phase=MigrationPhase.DUAL_WRITE)
        migration_repo = AsyncMock(update_phase=AsyncMock(), record_error=AsyncMock())
        routing_repo = AsyncMock(clear_migration_state=AsyncMock())
        coordinator = _make_coordinator(migration_repo=migration_repo, routing_repo=routing_repo)
        coordinator._lag_trackers[migration.id] = MagicMock()
        coordinator._target_stores[migration.id] = MagicMock()
        await coordinator._fail_migration(migration, "Test failure")
        assert migration.id not in coordinator._lag_trackers
        assert migration.id not in coordinator._target_stores


class TestStartMigrationStoresTargetStore:
    """Tests for start_migration storing target store reference."""

    @pytest.mark.asyncio
    async def test_start_migration_stores_target_store(self) -> None:
        tenant_id, migration_id = uuid4(), uuid4()
        target_store = AsyncMock()
        created_migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated-tenant",
            phase=MigrationPhase.BULK_COPY,
            started_at=datetime.now(UTC),
        )
        migration_repo = AsyncMock(
            get_by_tenant=AsyncMock(return_value=None),
            create=AsyncMock(),
            get=AsyncMock(return_value=created_migration),
            update_phase=AsyncMock(),
        )
        routing_repo = AsyncMock(
            get_or_default=AsyncMock(
                return_value=TenantRouting(tenant_id=tenant_id, store_id="default")
            ),
            set_migration_state=AsyncMock(),
        )
        coordinator = _make_coordinator(migration_repo=migration_repo, routing_repo=routing_repo)
        with patch.object(coordinator, "_run_bulk_copy", new_callable=AsyncMock):
            migration = await coordinator.start_migration(
                tenant_id=tenant_id, target_store=target_store, target_store_id="dedicated-tenant"
            )
        assert coordinator._target_stores.get(migration.id) is target_store
