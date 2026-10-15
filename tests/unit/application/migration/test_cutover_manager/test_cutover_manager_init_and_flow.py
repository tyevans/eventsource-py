"""Unit tests for CutoverManager initialization, successful cutover, and zero-lag defaults."""

from datetime import UTC, datetime

import pytest

from eventsource.application.migration.cutover import CutoverManager
from eventsource.ports import Position
from eventsource.ports.migration.models import (
    SyncLag,
    TenantMigrationState,
    TenantRouting,
)


def _lag(events: int, src: int, tgt: int) -> SyncLag:
    return SyncLag(
        events=events,
        source_position=Position(store_id="source", key=(src,)),
        target_position=Position(store_id="target", key=(tgt,)),
        timestamp=datetime.now(UTC),
    )


class TestCutoverManagerInit:
    def test_init_with_defaults(self, mock_lock_manager, mock_router, mock_routing_repo):
        m = CutoverManager(
            lock_manager=mock_lock_manager, router=mock_router, routing_repo=mock_routing_repo
        )
        assert (m._lock_manager, m._router, m._routing_repo) == (
            mock_lock_manager,
            mock_router,
            mock_routing_repo,
        )
        assert (m._lock_acquisition_timeout, m._enable_tracing) == (0.5, True)

    def test_init_with_custom_timeout(self, mock_lock_manager, mock_router, mock_routing_repo):
        m = CutoverManager(
            mock_lock_manager, mock_router, mock_routing_repo, lock_acquisition_timeout=1.0
        )
        assert m._lock_acquisition_timeout == 1.0

    def test_init_with_tracing_disabled(self, mock_lock_manager, mock_router, mock_routing_repo):
        m = CutoverManager(mock_lock_manager, mock_router, mock_routing_repo, enable_tracing=False)
        assert m._enable_tracing is False


class TestSuccessfulCutover:
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
        result = await cutover_manager.execute_cutover(
            migration_id, tenant_id, mock_lag_tracker, target_store_id
        )
        assert (result.success, result.rolled_back, result.error_message) == (True, False, None)
        assert result.duration_ms > 0
        mock_router.pause_writes.assert_called_once_with(tenant_id)
        mock_router.resume_writes.assert_called_once_with(tenant_id)
        mock_routing_repo.set_migration_state.assert_any_call(
            tenant_id, TenantMigrationState.CUTOVER_PAUSED, migration_id=migration_id
        )
        mock_routing_repo.set_migration_state.assert_any_call(
            tenant_id, TenantMigrationState.MIGRATED, migration_id=migration_id
        )
        mock_router.clear_dual_write_interceptor.assert_called_once_with(tenant_id)

    @pytest.mark.asyncio
    async def test_successful_cutover_records_duration_metric(
        self, cutover_manager, tenant_id, migration_id, target_store_id, mock_lag_tracker
    ):
        from eventsource.application.migration.metrics import (
            clear_metrics_registry,
            get_migration_metrics,
        )

        clear_metrics_registry()
        result = await cutover_manager.execute_cutover(
            migration_id, tenant_id, mock_lag_tracker, target_store_id
        )
        assert result.success is True
        snapshot = get_migration_metrics(str(migration_id), str(tenant_id)).get_snapshot()
        assert snapshot.cutover_durations == [result.duration_ms]
        clear_metrics_registry()

    @pytest.mark.asyncio
    async def test_successful_cutover_with_small_lag(
        self, cutover_manager, tenant_id, migration_id, target_store_id, mock_lag_tracker, config
    ):
        mock_lag_tracker.current_lag = _lag(50, 100, 50)
        result = await cutover_manager.execute_cutover(
            migration_id, tenant_id, mock_lag_tracker, target_store_id, config=config
        )
        assert result.success is True

    @pytest.mark.asyncio
    async def test_successful_cutover_with_custom_timeout(
        self, cutover_manager, tenant_id, migration_id, target_store_id, mock_lag_tracker
    ):
        result = await cutover_manager.execute_cutover(
            migration_id, tenant_id, mock_lag_tracker, target_store_id, timeout_ms=1000.0
        )
        assert result.success is True

    @pytest.mark.asyncio
    async def test_cutover_tracks_events_synced(
        self, cutover_manager, tenant_id, migration_id, target_store_id, mock_lag_tracker, config
    ):
        initial_lag, final_lag = _lag(10, 100, 90), _lag(2, 100, 98)
        call_count = 0

        async def update_lag(*, since=None):
            nonlocal call_count
            call_count += 1
            mock_lag_tracker.current_lag = initial_lag if call_count == 1 else final_lag

        mock_lag_tracker.calculate_lag.side_effect = update_lag
        mock_lag_tracker.current_lag = initial_lag
        result = await cutover_manager.execute_cutover(
            migration_id, tenant_id, mock_lag_tracker, target_store_id, config=config
        )
        assert (result.success, result.events_synced) == (True, 8)


class TestStrictZeroLagDefault:
    @pytest.mark.asyncio
    async def test_default_config_refuses_cutover_on_a_single_missing_event(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_router,
        mock_routing_repo,
    ):
        mock_lag_tracker.current_lag = _lag(1, 101, 100)
        result = await cutover_manager.execute_cutover(
            migration_id, tenant_id, mock_lag_tracker, target_store_id
        )
        assert (result.success, result.rolled_back) == (False, True)
        mock_routing_repo.set_routing.assert_not_called()
        mock_router.resume_writes.assert_called_once_with(tenant_id)

    @pytest.mark.asyncio
    async def test_rollback_restores_the_route_not_just_the_state(
        self,
        cutover_manager,
        tenant_id,
        migration_id,
        target_store_id,
        mock_lag_tracker,
        mock_routing_repo,
    ):
        async def current_routing(_tenant_id):
            writes = mock_routing_repo.set_routing.call_args_list
            store_id = writes[-1].args[1] if writes else "source-store"
            return TenantRouting(
                tenant_id=tenant_id,
                store_id=store_id,
                migration_state=TenantMigrationState.DUAL_WRITE,
            )

        mock_routing_repo.get_routing.side_effect = current_routing
        mock_routing_repo.set_migration_state.side_effect = [
            None,
            Exception("state write failed"),
            None,
        ]
        result = await cutover_manager.execute_cutover(
            migration_id, tenant_id, mock_lag_tracker, target_store_id
        )
        assert (result.success, result.rolled_back) == (False, True)
        restored_to = [call.args[1] for call in mock_routing_repo.set_routing.call_args_list]
        assert restored_to[-1] == "source-store"

    @pytest.mark.asyncio
    async def test_an_explicit_threshold_still_tolerates_lag(
        self, cutover_manager, tenant_id, migration_id, target_store_id, mock_lag_tracker, config
    ):
        mock_lag_tracker.current_lag = _lag(1, 101, 100)
        result = await cutover_manager.execute_cutover(
            migration_id, tenant_id, mock_lag_tracker, target_store_id, config=config
        )
        assert result.success is True
