"""Unit tests for CutoverManager routing state transitions, readiness validation, result object, and edge cases."""

from datetime import UTC, datetime

import pytest

from eventsource.application.migration.cutover import CutoverManager
from eventsource.ports import Position
from eventsource.ports.migration.models import (
    CutoverResult,
    SyncLag,
    TenantMigrationState,
    TenantRouting,
)
from tests.unit.application.migration.test_cutover_manager._fixtures import (
    create_lock_context_manager,
)


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

        mock_routing_repo.switch_routing.assert_called_once_with(
            tenant_id,
            target_store_id,
            state=TenantMigrationState.MIGRATED,
            migration_id=migration_id,
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
            source_position=Position(store_id="source", key=(200,)),
            target_position=Position(store_id="target", key=(100,)),
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
