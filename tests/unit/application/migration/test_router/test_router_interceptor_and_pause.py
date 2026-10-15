"""Tests for TenantStoreRouter dual-write interceptor, write pause, and state routing."""

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.router import TenantStoreRouter, WritePausedError
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.migration.models import TenantMigrationState, TenantRouting

from ._fixtures import TestEvent


class TestWritePausedError:
    """Tests for WritePausedError."""

    def test_error_attributes(self) -> None:
        """Test error stores tenant_id and timeout."""
        tenant_id = uuid4()
        error = WritePausedError(tenant_id=tenant_id, timeout=5.0)
        assert error.tenant_id == tenant_id
        assert error.timeout == 5.0

    def test_error_message(self) -> None:
        """Test error message formatting."""
        tenant_id = uuid4()
        error = WritePausedError(tenant_id=tenant_id, timeout=5.0)
        assert str(tenant_id) in str(error)
        assert "5.0" in str(error)


class TestDualWriteInterceptorManagement:
    """Tests for dual-write interceptor management."""

    def test_set_dual_write_interceptor(
        self, router: TenantStoreRouter, mock_dedicated_store: MagicMock
    ) -> None:
        """Test setting a dual-write interceptor."""
        tenant_id = uuid4()
        router.set_dual_write_interceptor(tenant_id, mock_dedicated_store)
        assert tenant_id in router._dual_write_interceptors
        assert router._dual_write_interceptors[tenant_id] == mock_dedicated_store

    def test_clear_dual_write_interceptor(
        self, router: TenantStoreRouter, mock_dedicated_store: MagicMock
    ) -> None:
        """Test clearing a dual-write interceptor."""
        tenant_id = uuid4()
        router.set_dual_write_interceptor(tenant_id, mock_dedicated_store)
        router.clear_dual_write_interceptor(tenant_id)
        assert tenant_id not in router._dual_write_interceptors

    def test_clear_nonexistent_interceptor(self, router: TenantStoreRouter) -> None:
        """Test clearing a non-existent interceptor doesn't raise."""
        tenant_id = uuid4()
        # Should not raise
        router.clear_dual_write_interceptor(tenant_id)

    def test_has_dual_write_interceptor(
        self, router: TenantStoreRouter, mock_dedicated_store: MagicMock
    ) -> None:
        """Test checking if tenant has interceptor."""
        tenant_id = uuid4()
        assert router.has_dual_write_interceptor(tenant_id) is False
        router.set_dual_write_interceptor(tenant_id, mock_dedicated_store)
        assert router.has_dual_write_interceptor(tenant_id) is True


class TestWritePauseManagement:
    """Tests for write pause management."""

    async def test_pause_writes(self, router: TenantStoreRouter) -> None:
        """Test pausing writes for a tenant."""
        tenant_id = uuid4()
        result = await router.pause_writes(tenant_id)
        assert result is True
        assert router.is_paused(tenant_id) is True

    async def test_pause_writes_idempotent(self, router: TenantStoreRouter) -> None:
        """Test pausing writes is idempotent."""
        tenant_id = uuid4()
        result1 = await router.pause_writes(tenant_id)
        result2 = await router.pause_writes(tenant_id)
        # Second call returns False (already paused)
        assert result1 is True
        assert result2 is False
        assert router.is_paused(tenant_id) is True

    async def test_resume_writes(self, router: TenantStoreRouter) -> None:
        """Test resuming writes for a tenant."""
        tenant_id = uuid4()
        await router.pause_writes(tenant_id)
        metrics = await router.resume_writes(tenant_id)
        assert metrics is not None
        assert router.is_paused(tenant_id) is False

    async def test_resume_writes_signals_waiters(self, router: TenantStoreRouter) -> None:
        """Test resuming writes signals waiting tasks."""
        tenant_id = uuid4()
        waiter_completed = False

        async def waiter():
            nonlocal waiter_completed
            await router._wait_if_paused(tenant_id)
            waiter_completed = True

        await router.pause_writes(tenant_id)
        # Start waiter
        waiter_task = asyncio.create_task(waiter())
        # Give waiter time to start waiting
        await asyncio.sleep(0.01)
        # Resume should unblock waiter
        await router.resume_writes(tenant_id)
        # Wait for waiter to complete
        await asyncio.wait_for(waiter_task, timeout=1.0)
        assert waiter_completed is True

    async def test_resume_nonexistent_pause(self, router: TenantStoreRouter) -> None:
        """Test resuming a non-paused tenant doesn't raise."""
        tenant_id = uuid4()
        # Should not raise
        await router.resume_writes(tenant_id)

    def test_is_paused(self, router: TenantStoreRouter) -> None:
        """Test checking if tenant is paused."""
        tenant_id = uuid4()
        assert router.is_paused(tenant_id) is False


class TestWaitIfPaused:
    """Tests for _wait_if_paused method."""

    async def test_wait_if_paused_returns_immediately_when_not_paused(
        self, router: TenantStoreRouter
    ) -> None:
        """Test returns immediately when tenant is not paused."""
        tenant_id = uuid4()
        # Should return immediately without blocking
        await asyncio.wait_for(router._wait_if_paused(tenant_id), timeout=0.1)

    async def test_wait_if_paused_returns_immediately_for_none_tenant(
        self, router: TenantStoreRouter
    ) -> None:
        """Test returns immediately when tenant_id is None."""
        await asyncio.wait_for(router._wait_if_paused(None), timeout=0.1)

    async def test_wait_if_paused_raises_on_timeout(
        self, mock_default_store: MagicMock, mock_routing_repo: AsyncMock
    ) -> None:
        """Test raises WritePausedError on timeout."""
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            write_pause_timeout=0.01,  # Very short timeout
            enable_tracing=False,
        )
        tenant_id = uuid4()
        await router.pause_writes(tenant_id)

        with pytest.raises(WritePausedError) as exc_info:
            await router._wait_if_paused(tenant_id)

        assert exc_info.value.tenant_id == tenant_id


class TestMigrationStateRouting:
    """Tests for routing behavior across all migration states."""

    @pytest.mark.parametrize(
        "state,expected_store_key",
        [
            (TenantMigrationState.NORMAL, "source"),
            (TenantMigrationState.BULK_COPY, "source"),
            (TenantMigrationState.MIGRATED, "source"),  # source is updated to target
        ],
    )
    async def test_write_routing_by_state(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
        state: TenantMigrationState,
        expected_store_key: str,
    ) -> None:
        """Test write routing for different migration states."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
        stores = {"source": mock_default_store, "target": mock_dedicated_store}

        mock_routing_repo.get_routing.return_value = TenantRouting(
            tenant_id=tenant_id,
            store_id="source",
            migration_state=state,
            target_store_id="target",
        )
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores=stores,
            default_store_id="source",
            enable_tracing=False,
        )

        await router.append(stream, [event], ExpectedVersion.no_stream())
        stores[expected_store_key].append.assert_called_once()

    async def test_cutover_paused_state_raises(
        self, mock_default_store: MagicMock, mock_routing_repo: AsyncMock
    ) -> None:
        """Test CUTOVER_PAUSED state blocks writes."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        mock_routing_repo.get_routing.return_value = TenantRouting(
            tenant_id=tenant_id,
            store_id="default",
            migration_state=TenantMigrationState.CUTOVER_PAUSED,
        )
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            write_pause_timeout=0.01,
            enable_tracing=False,
        )

        # Pause writes to simulate cutover
        await router.pause_writes(tenant_id)

        with pytest.raises(WritePausedError):
            await router.append(stream, [event], ExpectedVersion.no_stream())


class TestDualWriteStateWithoutInterceptor:
    """Tests for DUAL_WRITE state when interceptor is not set."""

    async def test_dual_write_falls_back_to_source_without_interceptor(
        self, mock_default_store: MagicMock, mock_routing_repo: AsyncMock
    ) -> None:
        """Test falls back to source store if interceptor not set."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        mock_routing_repo.get_routing.return_value = TenantRouting(
            tenant_id=tenant_id,
            store_id="default",
            migration_state=TenantMigrationState.DUAL_WRITE,
        )
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        # No interceptor set - should fall back to source
        await router.append(stream, [event], ExpectedVersion.no_stream())
        mock_default_store.append.assert_called_once()
