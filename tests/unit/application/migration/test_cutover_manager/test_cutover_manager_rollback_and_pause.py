"""Unit tests for CutoverManager automatic rollback, write pause/resume, and target store verification."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from eventsource.application.migration.cutover import CutoverManager
from eventsource.ports import Position
from eventsource.ports.migration.models import SyncLag, TenantMigrationState
from tests.unit.application.migration.test_cutover_manager._fixtures import (
    create_lock_context_manager,
)


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
            source_position=Position(store_id="source", key=(200,)),
            target_position=Position(store_id="target", key=(100,)),
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
        mock_store.current_position = AsyncMock(side_effect=Exception("Connection failed"))
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
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(100,)),
            timestamp=datetime.now(UTC),
        )

        # Make target store health check fail to trigger rollback
        mock_store = MagicMock()
        mock_store.current_position = AsyncMock(side_effect=Exception("Connection failed"))
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
            source_position=Position(store_id="source", key=(200,)),
            target_position=Position(store_id="target", key=(100,)),
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
