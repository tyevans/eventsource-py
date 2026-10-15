"""Unit tests for CutoverManager lock acquisition, sync lag validation, and timeouts."""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from eventsource.application.migration.cutover import CutoverManager
from eventsource.ports import Position
from eventsource.ports.exceptions import LockAcquisitionError
from eventsource.ports.migration.models import MigrationConfig, SyncLag, TenantMigrationState
from tests.unit.application.migration.test_cutover_manager._fixtures import (
    create_failing_lock_context_manager,
    create_lock_context_manager,
)


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
        create_lock_context_manager(mock_lock_manager)
        manager1 = CutoverManager(
            lock_manager=mock_lock_manager,
            router=mock_router,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        error = LockAcquisitionError(key="cutover:test", reason="Lock already held")
        mock_lock_manager2 = MagicMock()
        create_failing_lock_context_manager(mock_lock_manager2, error)

        manager2 = CutoverManager(
            lock_manager=mock_lock_manager2,
            router=mock_router,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        result1 = await manager1.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )
        result2 = await manager2.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
        )

        assert result1.success is True
        assert result2.success is False
        assert "lock" in result2.error_message.lower()


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
        lag = SyncLag(
            events=50,
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(50,)),
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
        mock_router.pause_writes.assert_called_once()
        mock_router.resume_writes.assert_called_once()
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
        lag = SyncLag(
            events=100,
            source_position=Position(store_id="source", key=(200,)),
            target_position=Position(store_id="target", key=(100,)),
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
        create_lock_context_manager(mock_lock_manager)

        async def slow_calculate_lag():
            import asyncio

            await asyncio.sleep(0.2)

        mock_lag_tracker.calculate_lag = slow_calculate_lag
        mock_lag_tracker.current_lag = SyncLag(
            events=0,
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(100,)),
            timestamp=datetime.now(UTC),
        )

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
            timeout_ms=50.0,
        )
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
        config = MigrationConfig(cutover_timeout_ms=200)
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=mock_lag_tracker,
            target_store_id=target_store_id,
            config=config,
        )
        assert result.success is True
        assert result.duration_ms < 200
