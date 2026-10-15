"""Unit tests for MigrationCoordinator lifecycle management."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.exceptions import (
    MigrationAlreadyExistsError,
    MigrationError,
)
from eventsource.ports.migration.models import (
    MigrationConfig,
    MigrationPhase,
    MigrationResult,
    TenantMigrationState,
)
from tests.unit.application.migration.test_coordinator._fixtures import (
    create_coordinator,
    create_coordinator_deps,
    create_test_migration,
    setup_start_migration,
)


class TestMigrationCoordinatorInit:
    def test_init_with_required_args(self) -> None:
        src, mig_repo, rt_repo, router = MagicMock(), MagicMock(), MagicMock(), MagicMock()
        c = MigrationCoordinator(src, mig_repo, rt_repo, router, enable_tracing=False)
        assert (c._source_store, c._migration_repo) == (src, mig_repo)
        assert (c._routing_repo, c._router, c._source_store_id) == (rt_repo, router, "default")
        assert (c._active_copiers, c._active_tasks, c._status_queues) == ({}, {}, {})

    def test_init_with_custom_source_store_id(self) -> None:
        c = MigrationCoordinator(
            **create_coordinator_deps(), source_store_id="shared-postgres", enable_tracing=False
        )
        assert c._source_store_id == "shared-postgres"

    def test_init_with_tracing_disabled(self) -> None:
        c = MigrationCoordinator(**create_coordinator_deps(), enable_tracing=False)
        assert c._enable_tracing is False


class TestStartMigration:
    @pytest.fixture
    def coordinator_deps(self) -> dict:
        return create_coordinator_deps()

    async def test_start_migration_creates_migration(self, coordinator_deps: dict) -> None:
        tenant_id, target, target_id = uuid4(), AsyncMock(), "dedicated-tenant"
        created = create_test_migration(
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id=target_id,
            phase=MigrationPhase.BULK_COPY,
            started_at=datetime.now(UTC),
        )
        coordinator = setup_start_migration(coordinator_deps, tenant_id, created)
        with patch.object(coordinator, "_run_bulk_copy", new_callable=AsyncMock):
            migration = await coordinator.start_migration(tenant_id, target, target_id)
        assert (migration.tenant_id, migration.target_store_id) == (tenant_id, target_id)
        coordinator_deps["migration_repo"].create.assert_called_once()
        coordinator_deps["router"].register_store.assert_called_once_with(target_id, target)

    async def test_start_migration_with_config(self, coordinator_deps: dict) -> None:
        tenant_id = uuid4()
        cfg = MigrationConfig(batch_size=500, max_bulk_copy_rate=5000)
        created = create_test_migration(
            tenant_id=tenant_id, target_store_id="dedicated", config=cfg
        )
        coordinator = setup_start_migration(coordinator_deps, tenant_id, created)
        with patch.object(coordinator, "_run_bulk_copy", new_callable=AsyncMock):
            migration = await coordinator.start_migration(
                tenant_id, AsyncMock(), "dedicated", config=cfg
            )
        assert (migration.config.batch_size, migration.config.max_bulk_copy_rate) == (500, 5000)

    async def test_start_migration_raises_if_active_migration_exists(
        self, coordinator_deps: dict
    ) -> None:
        tenant_id = uuid4()
        existing = create_test_migration(tenant_id=tenant_id, target_store_id="existing")
        coordinator_deps["migration_repo"].get_by_tenant = AsyncMock(return_value=existing)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        with pytest.raises(MigrationAlreadyExistsError) as exc_info:
            await coordinator.start_migration(tenant_id, AsyncMock(), "new-target")
        assert exc_info.value.tenant_id == tenant_id
        assert exc_info.value.existing_migration_id == existing.id

    async def test_start_migration_updates_routing_state(self, coordinator_deps: dict) -> None:
        tenant_id = uuid4()
        created = create_test_migration(id=uuid4(), tenant_id=tenant_id)
        coordinator = setup_start_migration(coordinator_deps, tenant_id, created)
        with patch.object(coordinator, "_run_bulk_copy", new_callable=AsyncMock):
            await coordinator.start_migration(tenant_id, AsyncMock(), "dedicated")
        call_args = coordinator_deps["routing_repo"].set_migration_state.call_args
        assert (call_args[0][0], call_args[0][1]) == (tenant_id, TenantMigrationState.BULK_COPY)


class TestPauseMigration:
    async def test_pause_migration_pauses_copier(self) -> None:
        mid = uuid4()
        mig = create_test_migration(id=mid, is_paused=False)
        repo = AsyncMock(get=AsyncMock(return_value=mig), set_paused=AsyncMock())
        coordinator, mock_copier = create_coordinator(migration_repo=repo), MagicMock()
        coordinator._active_copiers[mid] = mock_copier
        await coordinator.pause_migration(mid)
        mock_copier.pause.assert_called_once()
        repo.set_paused.assert_called_once_with(mid, paused=True, reason="Operator requested")

    async def test_pause_migration_raises_if_terminal(self) -> None:
        mid = uuid4()
        mig = create_test_migration(id=mid, phase=MigrationPhase.COMPLETED)
        repo = AsyncMock(get=AsyncMock(return_value=mig))
        with pytest.raises(MigrationError, match="Cannot pause a completed migration"):
            await create_coordinator(migration_repo=repo).pause_migration(mid)

    async def test_pause_migration_noop_if_already_paused(self) -> None:
        mid = uuid4()
        mig = create_test_migration(id=mid, is_paused=True)
        repo = AsyncMock(get=AsyncMock(return_value=mig), set_paused=AsyncMock())
        await create_coordinator(migration_repo=repo).pause_migration(mid)
        repo.set_paused.assert_not_called()


class TestResumeMigration:
    async def test_resume_migration_resumes_copier(self) -> None:
        mid = uuid4()
        mig = create_test_migration(id=mid, is_paused=True)
        repo = AsyncMock(get=AsyncMock(return_value=mig), set_paused=AsyncMock())
        coordinator, mock_copier = create_coordinator(migration_repo=repo), MagicMock()
        coordinator._active_copiers[mid] = mock_copier
        await coordinator.resume_migration(mid)
        mock_copier.resume.assert_called_once()
        repo.set_paused.assert_called_once_with(mid, paused=False)

    async def test_resume_migration_raises_if_not_paused(self) -> None:
        mid = uuid4()
        mig = create_test_migration(id=mid, is_paused=False)
        repo = AsyncMock(get=AsyncMock(return_value=mig))
        with pytest.raises(MigrationError, match="Migration is not paused"):
            await create_coordinator(migration_repo=repo).resume_migration(mid)


class TestAbortMigration:
    async def test_abort_migration_cancels_copier_and_task(self) -> None:
        mid, tenant_id = uuid4(), uuid4()
        mig = create_test_migration(
            id=mid, tenant_id=tenant_id, events_copied=100, started_at=datetime.now(UTC)
        )
        repo = AsyncMock(
            get=AsyncMock(return_value=mig), update_phase=AsyncMock(), record_error=AsyncMock()
        )
        routing_repo, router = AsyncMock(clear_migration_state=AsyncMock()), MagicMock()
        coordinator = create_coordinator(
            migration_repo=repo, routing_repo=routing_repo, router=router
        )
        mock_copier, task = MagicMock(), asyncio.create_task(asyncio.sleep(100))
        coordinator._active_copiers[mid], coordinator._active_tasks[mid] = mock_copier, task
        result = await coordinator.abort_migration(mid, reason="Test abort")
        assert isinstance(result, MigrationResult) and result.success is False
        assert (result.final_phase, result.error_message) == (MigrationPhase.ABORTED, "Test abort")
        mock_copier.cancel.assert_called_once()
        assert task.cancelled()
        routing_repo.clear_migration_state.assert_called_once_with(tenant_id)
        router.clear_dual_write_interceptor.assert_called_once_with(tenant_id)
        repo.update_phase.assert_called_once_with(mid, MigrationPhase.ABORTED)

    async def test_abort_migration_raises_if_terminal(self) -> None:
        mid = uuid4()
        mig = create_test_migration(id=mid, phase=MigrationPhase.COMPLETED)
        repo = AsyncMock(get=AsyncMock(return_value=mig))
        with pytest.raises(MigrationError, match="Cannot abort a completed migration"):
            await create_coordinator(migration_repo=repo).abort_migration(mid)


class TestFailMigration:
    async def test_fail_migration_updates_phase_and_clears_routing(self) -> None:
        mid, tenant_id = uuid4(), uuid4()
        mig = create_test_migration(id=mid, tenant_id=tenant_id)
        repo = AsyncMock(update_phase=AsyncMock(), record_error=AsyncMock())
        routing_repo = AsyncMock(clear_migration_state=AsyncMock())
        coordinator = create_coordinator(migration_repo=repo, routing_repo=routing_repo)
        await coordinator._fail_migration(mig, "Test error")
        repo.update_phase.assert_called_once_with(mid, MigrationPhase.FAILED)
        repo.record_error.assert_called_once_with(mid, "Test error")
        routing_repo.clear_migration_state.assert_called_once_with(tenant_id)
