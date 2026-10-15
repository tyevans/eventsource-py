"""Unit tests for CutoverManager routing state transitions, readiness validation, result object, and edge cases."""

from datetime import UTC, datetime

import pytest

from eventsource.application.migration.cutover import CutoverManager
from eventsource.ports import Position
from eventsource.ports.migration.models import (
    CutoverResult,
    SyncLag,
    TenantRouting,
)
from eventsource.ports.migration.models import (
    TenantMigrationState as State,
)
from tests.unit.application.migration.test_cutover_manager._fixtures import (
    create_lock_context_manager,
)


async def _run_cutover(mgr, mid, tid, tracker, store_id, **kw):
    return await mgr.execute_cutover(
        migration_id=mid, tenant_id=tid, lag_tracker=tracker, target_store_id=store_id, **kw
    )


class TestRoutingStateTransitions:
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
        await _run_cutover(
            cutover_manager, migration_id, tenant_id, mock_lag_tracker, target_store_id
        )
        calls = mock_routing_repo.set_migration_state.call_args_list
        assert (calls[0][0][1], calls[1][0][1]) == (State.CUTOVER_PAUSED, State.MIGRATED)

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
        await _run_cutover(
            cutover_manager, migration_id, tenant_id, mock_lag_tracker, target_store_id
        )
        mock_routing_repo.switch_routing.assert_called_once_with(
            tenant_id, target_store_id, state=State.MIGRATED, migration_id=migration_id
        )
        mock_routing_repo.set_routing.assert_called_once_with(tenant_id, target_store_id)

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
        await _run_cutover(
            cutover_manager, migration_id, tenant_id, mock_lag_tracker, target_store_id
        )
        mock_router.clear_dual_write_interceptor.assert_called_once_with(tenant_id)


class TestCutoverReadinessValidation:
    @pytest.mark.asyncio
    async def test_validate_readiness_success(
        self, cutover_manager, tenant_id, mock_lag_tracker, mock_routing_repo, config
    ):
        mock_routing_repo.get_routing.return_value = TenantRouting(
            tenant_id=tenant_id, store_id="source", migration_state=State.DUAL_WRITE
        )
        ready, err = await cutover_manager.validate_cutover_readiness(
            tenant_id=tenant_id, lag_tracker=mock_lag_tracker, config=config
        )
        assert (ready, err) == (True, None)

    @pytest.mark.asyncio
    async def test_validate_readiness_fails_on_high_lag(
        self, cutover_manager, tenant_id, mock_lag_tracker, strict_config
    ):
        mock_lag_tracker.current_lag = SyncLag(
            events=100,
            source_position=Position(store_id="source", key=(200,)),
            target_position=Position(store_id="target", key=(100,)),
            timestamp=datetime.now(UTC),
        )
        ready, err = await cutover_manager.validate_cutover_readiness(
            tenant_id=tenant_id, lag_tracker=mock_lag_tracker, config=strict_config
        )
        assert ready is False and "lag too high" in err.lower()

    @pytest.mark.asyncio
    async def test_validate_readiness_fails_wrong_state(
        self, cutover_manager, tenant_id, mock_lag_tracker, mock_routing_repo
    ):
        mock_routing_repo.get_routing.return_value = TenantRouting(
            tenant_id=tenant_id, store_id="source", migration_state=State.BULK_COPY
        )
        ready, err = await cutover_manager.validate_cutover_readiness(
            tenant_id=tenant_id, lag_tracker=mock_lag_tracker
        )
        assert ready is False and "DUAL_WRITE" in err

    @pytest.mark.asyncio
    async def test_validate_readiness_fails_no_routing(
        self, cutover_manager, tenant_id, mock_lag_tracker, mock_routing_repo
    ):
        mock_routing_repo.get_routing.return_value = None
        ready, err = await cutover_manager.validate_cutover_readiness(
            tenant_id=tenant_id, lag_tracker=mock_lag_tracker
        )
        assert ready is False and "routing" in err.lower()

    @pytest.mark.asyncio
    async def test_validate_readiness_fails_lock_held(
        self, cutover_manager, tenant_id, mock_lag_tracker, mock_routing_repo, mock_lock_manager
    ):
        mock_routing_repo.get_routing.return_value = TenantRouting(
            tenant_id=tenant_id, store_id="source", migration_state=State.DUAL_WRITE
        )
        mock_lock_manager.try_acquire.return_value = None
        ready, err = await cutover_manager.validate_cutover_readiness(
            tenant_id=tenant_id, lag_tracker=mock_lag_tracker
        )
        assert ready is False and "lock" in err.lower()


class TestCutoverResult:
    def test_cutover_result_creation(self):
        r = CutoverResult(success=True, duration_ms=50.5, events_synced=10)
        assert (r.success, r.duration_ms, r.events_synced, r.error_message, r.rolled_back) == (
            True,
            50.5,
            10,
            None,
            False,
        )

    def test_cutover_result_within_timeout(self):
        assert CutoverResult(success=True, duration_ms=50.0).within_timeout is True
        assert CutoverResult(success=True, duration_ms=150.0).within_timeout is False

    def test_cutover_result_with_error(self):
        r = CutoverResult(
            success=False, duration_ms=200.0, error_message="Timeout exceeded", rolled_back=True
        )
        assert (r.success, r.error_message, r.rolled_back) == (False, "Timeout exceeded", True)


class TestEdgeCases:
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
        create_lock_context_manager(mock_lock_manager)
        mgr = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )
        res = await _run_cutover(
            mgr, migration_id, tenant_id, mock_lag_tracker, target_store_id, timeout_ms=0.0
        )
        assert res.success is False

    @pytest.mark.asyncio
    async def test_cutover_with_very_large_timeout(
        self, cutover_manager, tenant_id, migration_id, target_store_id, mock_lag_tracker
    ):
        res = await _run_cutover(
            cutover_manager,
            migration_id,
            tenant_id,
            mock_lag_tracker,
            target_store_id,
            timeout_ms=100000.0,
        )
        assert res.success is True and res.duration_ms < 1000

    @pytest.mark.asyncio
    async def test_multiple_cutover_attempts(
        self, cutover_manager, tenant_id, migration_id, target_store_id, mock_lag_tracker
    ):
        r1 = await _run_cutover(
            cutover_manager, migration_id, tenant_id, mock_lag_tracker, target_store_id
        )
        r2 = await _run_cutover(
            cutover_manager, migration_id, tenant_id, mock_lag_tracker, target_store_id
        )
        assert r1.success is True and r2.success is True
