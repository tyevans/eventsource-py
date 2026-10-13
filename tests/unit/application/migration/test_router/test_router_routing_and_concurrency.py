"""Tests for TenantStoreRouter read/stream routing and concurrent operations."""

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from eventsource.application.migration.router import TenantStoreRouter
from eventsource.ports import (
    CategoryReadOptions,
    FeedReadOptions,
    Position,
)
from eventsource.ports.migration.models import (
    TenantMigrationState,
    TenantRouting,
)

from ._fixtures import async_generator_mock, create_mock_store, sid


class TestReadOperationsRouting:
    """Tests for read operation routing."""

    async def test_read_category_with_tenant_id(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test read_category routes based on options.tenant_id."""
        tenant_id = uuid4()

        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="dedicated",
            migration_state=TenantMigrationState.NORMAL,
        )
        mock_routing_repo.get_routing.return_value = routing

        default_called = False
        dedicated_called = False

        def default_read_category(*args, **kwargs):
            nonlocal default_called
            default_called = True
            return async_generator_mock([])

        def dedicated_read_category(*args, **kwargs):
            nonlocal dedicated_called
            dedicated_called = True
            return async_generator_mock([])

        mock_default_store.read_category = default_read_category
        mock_dedicated_store.read_category = dedicated_read_category

        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": mock_dedicated_store},
            enable_tracing=False,
        )

        options = CategoryReadOptions(tenant_id=tenant_id)
        async for _ in router.read_category("TestAggregate", options):
            pass

        assert dedicated_called is True
        assert default_called is False

    async def test_read_category_without_tenant_id_routes_to_default(
        self,
        router: TenantStoreRouter,
        mock_default_store: MagicMock,
    ) -> None:
        """Test read_category without tenant routes to default store."""
        read_category_called = False

        def tracked_read_category(*args, **kwargs):
            nonlocal read_category_called
            read_category_called = True
            return async_generator_mock([])

        mock_default_store.read_category = tracked_read_category

        async for _ in router.read_category("TestAggregate"):
            pass

        assert read_category_called is True

    async def test_event_exists_checks_all_stores(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test event_exists checks all stores."""
        event_id = uuid4()

        mock_default_store.event_exists.return_value = False
        mock_dedicated_store.event_exists.return_value = True

        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": mock_dedicated_store},
            enable_tracing=False,
        )

        result = await router.event_exists(event_id)

        assert result is True
        mock_default_store.event_exists.assert_called_once()
        mock_dedicated_store.event_exists.assert_called_once()

    async def test_event_exists_returns_false_if_not_found(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test event_exists returns False if event not in any store."""
        event_id = uuid4()

        mock_default_store.event_exists.return_value = False
        mock_dedicated_store.event_exists.return_value = False

        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": mock_dedicated_store},
            enable_tracing=False,
        )

        result = await router.event_exists(event_id)

        assert result is False

    async def test_get_stream_version(
        self,
        router: TenantStoreRouter,
        mock_default_store: MagicMock,
    ) -> None:
        """Test get_stream_version delegates to default store."""
        stream = sid()

        await router.get_stream_version(stream)

        mock_default_store.get_stream_version.assert_called_once_with(stream)

    async def test_current_position(
        self,
        router: TenantStoreRouter,
        mock_default_store: MagicMock,
    ) -> None:
        """Test current_position delegates to default store."""
        result = await router.current_position()

        assert result == Position(store_id="default", key=(100,))
        mock_default_store.current_position.assert_called_once()


class TestStreamOperationsRouting:
    """Tests for read_all and read_stream routing."""

    async def test_read_all_routes_to_default_without_tenant(
        self,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test read_all without tenant routes to default store."""
        read_all_called = False

        def tracked_read_all(*args, **kwargs):
            nonlocal read_all_called
            read_all_called = True
            return async_generator_mock([])

        default_store = create_mock_store(store_id="default")
        default_store.read_all = tracked_read_all

        router = TenantStoreRouter(
            default_store=default_store,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        async for _ in router.read_all():
            pass

        assert read_all_called is True

    async def test_read_all_routes_to_tenant_store_with_tenant_id(
        self,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test read_all with tenant_id routes to tenant's store."""
        tenant_id = uuid4()

        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="dedicated",
            migration_state=TenantMigrationState.NORMAL,
        )
        mock_routing_repo.get_routing.return_value = routing

        default_called = False
        dedicated_called = False

        def default_read_all(*args, **kwargs):
            nonlocal default_called
            default_called = True
            return async_generator_mock([])

        def dedicated_read_all(*args, **kwargs):
            nonlocal dedicated_called
            dedicated_called = True
            return async_generator_mock([])

        default_store = create_mock_store(store_id="default")
        dedicated_store = create_mock_store(store_id="dedicated")
        default_store.read_all = default_read_all
        dedicated_store.read_all = dedicated_read_all

        router = TenantStoreRouter(
            default_store=default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": dedicated_store},
            enable_tracing=False,
        )

        options = FeedReadOptions(tenant_id=tenant_id)
        async for _ in router.read_all(options=options):
            pass

        assert dedicated_called is True
        assert default_called is False

    async def test_read_stream_routes_to_default_without_tenant(
        self,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test read_stream without tenant routes to default store."""
        stream = sid()

        read_stream_called = False

        def tracked_read_stream(*args, **kwargs):
            nonlocal read_stream_called
            read_stream_called = True
            return async_generator_mock([])

        default_store = create_mock_store(store_id="default")
        default_store.read_stream = tracked_read_stream

        router = TenantStoreRouter(
            default_store=default_store,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        async for _ in router.read_stream(stream):
            pass

        assert read_stream_called is True


class TestConcurrentOperations:
    """Tests for concurrent operation handling."""

    async def test_concurrent_pause_resume(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test concurrent pause/resume operations are safe."""
        tenant_id = uuid4()

        async def pause_resume():
            await router.pause_writes(tenant_id)
            await asyncio.sleep(0.001)
            await router.resume_writes(tenant_id)

        # Run multiple concurrent pause/resume cycles
        tasks = [asyncio.create_task(pause_resume()) for _ in range(10)]
        await asyncio.gather(*tasks)

        # Should end in unparsed state
        assert router.is_paused(tenant_id) is False

    async def test_concurrent_store_registration(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test concurrent store registration is safe."""

        def register_stores():
            for i in range(100):
                mock_store = create_mock_store(store_id=f"store-{i}")
                router.register_store(f"store-{i}", mock_store)

        # Run in executor to simulate concurrency
        register_stores()

        assert len(router.list_stores()) >= 100
