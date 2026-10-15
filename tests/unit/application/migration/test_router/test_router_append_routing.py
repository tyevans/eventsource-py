"""Tests for TenantStoreRouter append routing logic."""

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.router import TenantStoreRouter
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.migration.models import TenantMigrationState, TenantRouting

from ._fixtures import TestEvent, create_mock_store, sid


def _make_tenant_stream(tenant_id: UUID | None = None) -> tuple[UUID, StreamId, TestEvent]:
    aggregate_id = uuid4()
    event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
    stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
    return aggregate_id, stream, event


class TestAppendRouting:
    """Tests for append() routing logic."""

    async def test_append_empty_list_raises(self, router: TenantStoreRouter) -> None:
        with pytest.raises(ValueError, match="Cannot append empty event list"):
            await router.append(sid(), [], ExpectedVersion.no_stream())

    async def test_append_routes_to_default_without_tenant(
        self,
        router: TenantStoreRouter,
        mock_default_store: MagicMock,
    ) -> None:
        _, stream, event = _make_tenant_stream()
        await router.append(stream, [event], ExpectedVersion.no_stream())
        mock_default_store.append.assert_called_once_with(
            stream, [event], ExpectedVersion.no_stream()
        )

    async def test_append_routes_to_tenant_store_normal_state(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        tenant_id = uuid4()
        _, stream, event = _make_tenant_stream(tenant_id)
        mock_routing_repo.get_routing.return_value = TenantRouting(
            tenant_id=tenant_id, store_id="dedicated", migration_state=TenantMigrationState.NORMAL
        )
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": mock_dedicated_store},
            enable_tracing=False,
        )
        await router.append(stream, [event], ExpectedVersion.no_stream())
        mock_dedicated_store.append.assert_called_once()
        mock_default_store.append.assert_not_called()

    async def test_append_routes_to_source_in_bulk_copy(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        tenant_id = uuid4()
        _, stream, event = _make_tenant_stream(tenant_id)
        mock_routing_repo.get_routing.return_value = TenantRouting(
            tenant_id=tenant_id,
            store_id="default",
            migration_state=TenantMigrationState.BULK_COPY,
            target_store_id="dedicated",
        )
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": mock_dedicated_store},
            enable_tracing=False,
        )
        await router.append(stream, [event], ExpectedVersion.no_stream())
        mock_default_store.append.assert_called_once()

    async def test_append_uses_interceptor_in_dual_write(
        self,
        mock_default_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        tenant_id = uuid4()
        _, stream, event = _make_tenant_stream(tenant_id)
        mock_routing_repo.get_routing.return_value = TenantRouting(
            tenant_id=tenant_id, store_id="default", migration_state=TenantMigrationState.DUAL_WRITE
        )
        mock_interceptor = create_mock_store(store_id="interceptor")
        router = TenantStoreRouter(
            default_store=mock_default_store, routing_repo=mock_routing_repo, enable_tracing=False
        )
        router.set_dual_write_interceptor(tenant_id, mock_interceptor)
        await router.append(stream, [event], ExpectedVersion.no_stream())
        mock_interceptor.append.assert_called_once()
        mock_default_store.append.assert_not_called()

    async def test_append_routes_to_target_after_migration(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        tenant_id = uuid4()
        _, stream, event = _make_tenant_stream(tenant_id)
        mock_routing_repo.get_routing.return_value = TenantRouting(
            tenant_id=tenant_id,
            store_id="dedicated",
            migration_state=TenantMigrationState.MIGRATED,
        )
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": mock_dedicated_store},
            enable_tracing=False,
        )
        await router.append(stream, [event], ExpectedVersion.no_stream())
        mock_dedicated_store.append.assert_called_once()
        mock_default_store.append.assert_not_called()

    async def test_append_waits_when_paused(
        self,
        mock_default_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        tenant_id = uuid4()
        _, stream, event = _make_tenant_stream(tenant_id)
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            write_pause_timeout=0.1,
            enable_tracing=False,
        )
        await router.pause_writes(tenant_id)
        append_started, append_completed = False, False

        async def do_append():
            nonlocal append_started, append_completed
            append_started = True
            await router.append(stream, [event], ExpectedVersion.no_stream())
            append_completed = True

        append_task = asyncio.create_task(do_append())
        await asyncio.sleep(0.02)
        assert append_started is True and append_completed is False
        await router.resume_writes(tenant_id)
        await asyncio.wait_for(append_task, timeout=1.0)
        assert append_completed is True
