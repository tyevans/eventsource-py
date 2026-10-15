"""Tests for transition to dual-write and cutover trigger/readiness."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.application.migration.exceptions import MigrationError, MigrationStateError
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.ports.migration.models import (
    CutoverResult,
    MigrationConfig,
    MigrationPhase,
    SyncLag,
    TenantMigrationState,
)
from tests.unit.application.migration.test_coordinator_dual_write._fixtures import (
    create_test_migration,
)


class TestTransitionToDualWrite:
    @pytest.fixture
    def coordinator_deps(self) -> dict:
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
        migration_id, tenant_id = uuid4(), uuid4()
        migration = create_test_migration(
            id=migration_id, tenant_id=tenant_id, phase=MigrationPhase.BULK_COPY
        )
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        await coordinator._transition_to_dual_write(migration, AsyncMock())
        coordinator_deps["router"].set_dual_write_interceptor.assert_called_once()
        call_args = coordinator_deps["router"].set_dual_write_interceptor.call_args
        assert call_args[0][0] == tenant_id
        assert isinstance(call_args[0][1], DualWriteInterceptor)

    @pytest.mark.asyncio
    async def test_transition_to_dual_write_creates_lag_tracker(
        self, coordinator_deps: dict
    ) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        migration = create_test_migration(
            id=migration_id, tenant_id=tenant_id, phase=MigrationPhase.BULK_COPY
        )
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        await coordinator._transition_to_dual_write(migration, AsyncMock())
        assert migration_id in coordinator._lag_trackers
        lag_tracker = coordinator._lag_trackers[migration_id]
        assert isinstance(lag_tracker, SyncLagTracker)
        assert lag_tracker.tenant_id == tenant_id

    @pytest.mark.asyncio
    async def test_transition_to_dual_write_updates_phase(self, coordinator_deps: dict) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        migration = create_test_migration(
            id=migration_id, tenant_id=tenant_id, phase=MigrationPhase.BULK_COPY
        )
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        await coordinator._transition_to_dual_write(migration, AsyncMock())
        coordinator_deps["migration_repo"].update_phase.assert_called_once_with(
            migration_id, MigrationPhase.DUAL_WRITE
        )
        coordinator_deps["routing_repo"].set_migration_state.assert_called_once_with(
            tenant_id, TenantMigrationState.DUAL_WRITE, migration_id
        )


class TestTriggerCutover:
    @pytest.fixture
    def coordinator_deps(self) -> dict:
        return {
            "source_store": AsyncMock(),
            "migration_repo": AsyncMock(),
            "routing_repo": AsyncMock(),
            "router": MagicMock(),
            "lock_manager": AsyncMock(),
        }

    @pytest.mark.asyncio
    async def test_trigger_cutover_requires_lock_manager(self, coordinator_deps: dict) -> None:
        migration_id = uuid4()
        coordinator_deps.pop("lock_manager")
        migration = create_test_migration(
            id=migration_id, tenant_id=uuid4(), phase=MigrationPhase.DUAL_WRITE
        )
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        coordinator._lag_trackers[migration_id] = MagicMock()
        with pytest.raises(MigrationError, match="lock_manager not provided"):
            await coordinator.trigger_cutover(migration_id)

    @pytest.mark.asyncio
    async def test_trigger_cutover_raises_if_not_dual_write(self, coordinator_deps: dict) -> None:
        migration_id = uuid4()
        migration = create_test_migration(
            id=migration_id, tenant_id=uuid4(), phase=MigrationPhase.BULK_COPY
        )
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        with pytest.raises(MigrationStateError) as exc_info:
            await coordinator.trigger_cutover(migration_id)
        assert "bulk_copy" in str(exc_info.value).lower()
        assert exc_info.value.current_phase == MigrationPhase.BULK_COPY

    @pytest.mark.asyncio
    async def test_trigger_cutover_raises_if_no_lag_tracker(self, coordinator_deps: dict) -> None:
        migration_id = uuid4()
        migration = create_test_migration(
            id=migration_id, tenant_id=uuid4(), phase=MigrationPhase.DUAL_WRITE
        )
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        with pytest.raises(MigrationError, match="No sync lag tracker found"):
            await coordinator.trigger_cutover(migration_id)

    @pytest.mark.asyncio
    async def test_trigger_cutover_success(self, coordinator_deps: dict) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        migration = create_test_migration(
            id=migration_id, tenant_id=tenant_id, phase=MigrationPhase.DUAL_WRITE
        )
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        lag = SyncLag(
            events=0, source_position=100, target_position=100, timestamp=datetime.now(UTC)
        )
        coordinator._lag_trackers[migration_id] = MagicMock(
            calculate_lag=AsyncMock(), current_lag=lag
        )
        cutover_result = CutoverResult(success=True, duration_ms=50.0)
        with patch.object(coordinator, "_get_cutover_manager") as mock_get_cm:
            mock_get_cm.return_value = MagicMock(
                execute_cutover=AsyncMock(return_value=cutover_result)
            )
            result = await coordinator.trigger_cutover(migration_id)
        assert result.success is True
        assert result.duration_ms == 50.0
        assert coordinator_deps["migration_repo"].update_phase.call_count >= 1

    @pytest.mark.asyncio
    async def test_trigger_cutover_failure_rollback(self, coordinator_deps: dict) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        migration = create_test_migration(
            id=migration_id, tenant_id=tenant_id, phase=MigrationPhase.DUAL_WRITE
        )
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        coordinator._lag_trackers[migration_id] = MagicMock()
        cutover_result = CutoverResult(
            success=False, duration_ms=80.0, error_message="Timeout exceeded", rolled_back=True
        )
        with patch.object(coordinator, "_get_cutover_manager") as mock_get_cm:
            mock_get_cm.return_value = MagicMock(
                execute_cutover=AsyncMock(return_value=cutover_result)
            )
            result = await coordinator.trigger_cutover(migration_id)
        assert result.success is False
        assert result.rolled_back is True
        update_calls = coordinator_deps["migration_repo"].update_phase.call_args_list
        assert update_calls[-1][0][1] == MigrationPhase.DUAL_WRITE
        coordinator_deps["migration_repo"].record_error.assert_called()


class TestIsCutoverReady:
    def _make_coordinator(self, migration) -> MigrationCoordinator:
        return MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=AsyncMock(get=AsyncMock(return_value=migration)),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

    @pytest.mark.asyncio
    async def test_is_cutover_ready_returns_true_when_ready(self) -> None:
        migration_id = uuid4()
        cfg = MigrationConfig(cutover_max_lag_events=100)
        migration = create_test_migration(
            id=migration_id, tenant_id=uuid4(), phase=MigrationPhase.DUAL_WRITE, config=cfg
        )
        coordinator = self._make_coordinator(migration)
        coordinator._lag_trackers[migration_id] = MagicMock(
            calculate_lag=AsyncMock(), is_sync_ready=MagicMock(return_value=True)
        )
        ready, error = await coordinator.is_cutover_ready(migration_id)
        assert ready is True
        assert error is None

    @pytest.mark.asyncio
    async def test_is_cutover_ready_returns_false_when_lag_high(self) -> None:
        migration_id = uuid4()
        cfg = MigrationConfig(cutover_max_lag_events=100)
        migration = create_test_migration(
            id=migration_id, tenant_id=uuid4(), phase=MigrationPhase.DUAL_WRITE, config=cfg
        )
        coordinator = self._make_coordinator(migration)
        lag = SyncLag(
            events=500, source_position=1000, target_position=500, timestamp=datetime.now(UTC)
        )
        coordinator._lag_trackers[migration_id] = MagicMock(
            calculate_lag=AsyncMock(), is_sync_ready=MagicMock(return_value=False), current_lag=lag
        )
        ready, error = await coordinator.is_cutover_ready(migration_id)
        assert ready is False
        assert "Sync lag too high" in error and "500" in error

    @pytest.mark.asyncio
    async def test_is_cutover_ready_returns_false_if_wrong_phase(self) -> None:
        migration_id = uuid4()
        migration = create_test_migration(
            id=migration_id, tenant_id=uuid4(), phase=MigrationPhase.BULK_COPY
        )
        coordinator = self._make_coordinator(migration)
        ready, error = await coordinator.is_cutover_ready(migration_id)
        assert ready is False
        assert "bulk_copy" in error.lower()
