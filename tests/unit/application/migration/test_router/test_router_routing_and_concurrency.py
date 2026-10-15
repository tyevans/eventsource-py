"""Tests for TenantStoreRouter read/stream routing and concurrent operations."""

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from eventsource.application.migration.router import TenantStoreRouter
from eventsource.ports import CategoryReadOptions, FeedReadOptions, Position
from eventsource.ports.migration.models import TenantMigrationState, TenantRouting

from ._fixtures import async_generator_mock, create_mock_store, sid


def _make_tracked_gen():
    called = [False]

    def gen(*args, **kwargs):
        called[0] = True
        return async_generator_mock([])

    return called, gen


class TestReadOperationsRouting:
    """Tests for read operation routing."""

    async def test_read_category_with_tenant_id(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        tenant_id = uuid4()
        mock_routing_repo.get_routing.return_value = TenantRouting(
            tenant_id=tenant_id, store_id="dedicated", migration_state=TenantMigrationState.NORMAL
        )
        default_called, mock_default_store.read_category = _make_tracked_gen()
        dedicated_called, mock_dedicated_store.read_category = _make_tracked_gen()
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": mock_dedicated_store},
            enable_tracing=False,
        )
        async for _ in router.read_category(
            "TestAggregate", CategoryReadOptions(tenant_id=tenant_id)
        ):
            pass
        assert dedicated_called[0] is True and default_called[0] is False

    async def test_read_category_without_tenant_id_routes_to_default(
        self, router: TenantStoreRouter, mock_default_store: MagicMock
    ) -> None:
        called, mock_default_store.read_category = _make_tracked_gen()
        async for _ in router.read_category("TestAggregate"):
            pass
        assert called[0] is True

    async def test_event_exists_checks_all_stores(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        mock_default_store.event_exists.return_value = False
        mock_dedicated_store.event_exists.return_value = True
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": mock_dedicated_store},
            enable_tracing=False,
        )
        assert await router.event_exists(uuid4()) is True
        mock_default_store.event_exists.assert_called_once()
        mock_dedicated_store.event_exists.assert_called_once()

    async def test_event_exists_returns_false_if_not_found(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        mock_default_store.event_exists.return_value = False
        mock_dedicated_store.event_exists.return_value = False
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": mock_dedicated_store},
            enable_tracing=False,
        )
        assert await router.event_exists(uuid4()) is False

    async def test_get_stream_version(
        self, router: TenantStoreRouter, mock_default_store: MagicMock
    ) -> None:
        stream = sid()
        await router.get_stream_version(stream)
        mock_default_store.get_stream_version.assert_called_once_with(stream)

    async def test_current_position(
        self, router: TenantStoreRouter, mock_default_store: MagicMock
    ) -> None:
        result = await router.current_position()
        assert result == Position(store_id="default", key=(100,))
        mock_default_store.current_position.assert_called_once()


class TestStreamOperationsRouting:
    """Tests for read_all and read_stream routing."""

    async def test_read_all_routes_to_default_without_tenant(
        self, mock_routing_repo: AsyncMock
    ) -> None:
        default_store = create_mock_store(store_id="default")
        called, default_store.read_all = _make_tracked_gen()
        router = TenantStoreRouter(
            default_store=default_store, routing_repo=mock_routing_repo, enable_tracing=False
        )
        async for _ in router.read_all():
            pass
        assert called[0] is True

    async def test_read_all_routes_to_tenant_store_with_tenant_id(
        self, mock_routing_repo: AsyncMock
    ) -> None:
        tenant_id = uuid4()
        mock_routing_repo.get_routing.return_value = TenantRouting(
            tenant_id=tenant_id, store_id="dedicated", migration_state=TenantMigrationState.NORMAL
        )
        default_store = create_mock_store(store_id="default")
        dedicated_store = create_mock_store(store_id="dedicated")
        default_called, default_store.read_all = _make_tracked_gen()
        dedicated_called, dedicated_store.read_all = _make_tracked_gen()
        router = TenantStoreRouter(
            default_store=default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": dedicated_store},
            enable_tracing=False,
        )
        async for _ in router.read_all(options=FeedReadOptions(tenant_id=tenant_id)):
            pass
        assert dedicated_called[0] is True and default_called[0] is False

    async def test_read_stream_routes_to_default_without_tenant(
        self, mock_routing_repo: AsyncMock
    ) -> None:
        default_store = create_mock_store(store_id="default")
        called, default_store.read_stream = _make_tracked_gen()
        router = TenantStoreRouter(
            default_store=default_store, routing_repo=mock_routing_repo, enable_tracing=False
        )
        async for _ in router.read_stream(sid()):
            pass
        assert called[0] is True


class TestConcurrentOperations:
    """Tests for concurrent operation handling."""

    async def test_concurrent_pause_resume(self, router: TenantStoreRouter) -> None:
        tenant_id = uuid4()

        async def pause_resume():
            await router.pause_writes(tenant_id)
            await asyncio.sleep(0.001)
            await router.resume_writes(tenant_id)

        await asyncio.gather(*[asyncio.create_task(pause_resume()) for _ in range(10)])
        assert router.is_paused(tenant_id) is False

    async def test_concurrent_store_registration(self, router: TenantStoreRouter) -> None:
        for i in range(100):
            router.register_store(f"store-{i}", create_mock_store(store_id=f"store-{i}"))
        assert len(router.list_stores()) >= 100
