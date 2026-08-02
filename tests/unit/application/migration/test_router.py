"""
Unit tests for TenantStoreRouter.

Tests cover:
- TenantStoreRouter initialization
- Store registry management (register, unregister, list)
- Dual-write interceptor management
- Write pause mechanism
- Routing based on migration state
- FullEventStore port (structural) implementation
- Error conditions
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.router import (
    StoreNotFoundError,
    TenantStoreRouter,
    WritePausedError,
)
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    ExpectedVersion,
    FeedReadOptions,
    FullEventStore,
    Position,
)
from eventsource.ports.migration.models import (
    TenantMigrationState,
    TenantRouting,
)

# =============================================================================
# Test Events
# =============================================================================


class TestEvent(DomainEvent):
    """Test event for unit tests."""

    aggregate_type: str = "TestAggregate"
    data: str = "test"


def sid(aggregate_id=None, category: str = "TestAggregate") -> StreamId:
    """Build a StreamId for tests."""
    return StreamId(aggregate_id=aggregate_id or uuid4(), category=category)


# =============================================================================
# Test Fixtures
# =============================================================================


def create_mock_store(store_id: str = "store", position_key: tuple = (100,)) -> MagicMock:
    """Create a mock FullEventStore with proper async iterator support."""
    store = MagicMock()
    store.max_append_batch = None
    store.append = AsyncMock(
        side_effect=lambda stream, events, expected: AppendResult(
            stream=stream,
            new_version=1,
            position=Position(store_id=store_id, key=(1,)),
        )
    )
    store.event_exists = AsyncMock(return_value=False)
    store.get_stream_version = AsyncMock(return_value=0)
    store.current_position = AsyncMock(return_value=Position(store_id=store_id, key=position_key))

    # For async generators, we need to return the generator itself, not a coroutine
    def mock_read_stream(*args, **kwargs):
        return async_generator_mock([])

    def mock_read_all(*args, **kwargs):
        return async_generator_mock([])

    def mock_read_category(*args, **kwargs):
        return async_generator_mock([])

    store.read_stream = mock_read_stream
    store.read_all = mock_read_all
    store.read_category = mock_read_category
    return store


@pytest.fixture
def mock_default_store() -> MagicMock:
    """Create a mock default event store."""
    return create_mock_store(store_id="default", position_key=(100,))


@pytest.fixture
def mock_dedicated_store() -> MagicMock:
    """Create a mock dedicated event store."""
    return create_mock_store(store_id="dedicated", position_key=(50,))


@pytest.fixture
def mock_routing_repo() -> AsyncMock:
    """Create a mock routing repository."""
    repo = AsyncMock()
    repo.get_routing = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def router(
    mock_default_store: MagicMock,
    mock_routing_repo: AsyncMock,
) -> TenantStoreRouter:
    """Create a router with mock dependencies."""
    return TenantStoreRouter(
        default_store=mock_default_store,
        routing_repo=mock_routing_repo,
        enable_tracing=False,
    )


class AsyncIteratorMock:
    """Helper to create async iterators for mocking."""

    def __init__(self, items: list):
        self.items = items
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.items):
            raise StopAsyncIteration
        item = self.items[self.index]
        self.index += 1
        return item


async def async_generator_mock(items: list):
    """Helper to create async generators for mocking."""
    for item in items:
        yield item


# =============================================================================
# Test Exceptions
# =============================================================================


class TestWritePausedError:
    """Tests for WritePausedError."""

    def test_error_attributes(self) -> None:
        """Test error stores tenant_id and timeout."""
        tenant_id = uuid4()
        error = WritePausedError(tenant_id, 5.0)

        assert error.tenant_id == tenant_id
        assert error.timeout == 5.0

    def test_error_message(self) -> None:
        """Test error message formatting."""
        tenant_id = uuid4()
        error = WritePausedError(tenant_id, 5.0)

        assert str(tenant_id) in str(error)
        assert "5.0" in str(error)


class TestStoreNotFoundError:
    """Tests for StoreNotFoundError."""

    def test_error_attributes(self) -> None:
        """Test error stores store_id."""
        error = StoreNotFoundError("missing-store")

        assert error.store_id == "missing-store"

    def test_error_message(self) -> None:
        """Test error message formatting."""
        error = StoreNotFoundError("missing-store")

        assert "missing-store" in str(error)


# =============================================================================
# Test Initialization
# =============================================================================


class TestTenantStoreRouterInit:
    """Tests for TenantStoreRouter initialization."""

    def test_init_with_defaults(
        self,
        mock_default_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test initialization with default parameters."""
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
        )

        assert router._default_store == mock_default_store
        assert router._routing_repo == mock_routing_repo
        assert router._default_store_id == "default"
        assert router._write_pause_timeout == 5.0

    def test_init_with_custom_store_id(
        self,
        mock_default_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test initialization with custom default store ID."""
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            default_store_id="shared",
        )

        assert router._default_store_id == "shared"
        assert "shared" in router._stores

    def test_init_with_stores_dict(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test initialization with pre-registered stores."""
        stores = {"dedicated": mock_dedicated_store}

        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores=stores,
        )

        assert "dedicated" in router._stores
        assert "default" in router._stores

    def test_init_with_custom_timeout(
        self,
        mock_default_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test initialization with custom write pause timeout."""
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            write_pause_timeout=10.0,
        )

        assert router._write_pause_timeout == 10.0

    def test_init_with_tracing_disabled(
        self,
        mock_default_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test initialization with tracing disabled."""
        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        # Should not raise and should disable tracing
        assert router._enable_tracing is False


# =============================================================================
# Test Store Registry Management
# =============================================================================


class TestStoreRegistryManagement:
    """Tests for store registry management."""

    def test_register_store(
        self,
        router: TenantStoreRouter,
        mock_dedicated_store: MagicMock,
    ) -> None:
        """Test registering a new store."""
        router.register_store("dedicated", mock_dedicated_store)

        assert "dedicated" in router._stores
        assert router._stores["dedicated"] == mock_dedicated_store

    def test_register_store_overwrites(
        self,
        router: TenantStoreRouter,
        mock_dedicated_store: MagicMock,
    ) -> None:
        """Test registering a store with existing ID overwrites."""
        other_store = create_mock_store(store_id="other")
        router.register_store("dedicated", mock_dedicated_store)
        router.register_store("dedicated", other_store)

        assert router._stores["dedicated"] == other_store

    def test_unregister_store(
        self,
        router: TenantStoreRouter,
        mock_dedicated_store: MagicMock,
    ) -> None:
        """Test unregistering a store."""
        router.register_store("dedicated", mock_dedicated_store)
        router.unregister_store("dedicated")

        assert "dedicated" not in router._stores

    def test_unregister_nonexistent_store(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test unregistering a non-existent store doesn't raise."""
        # Should not raise
        router.unregister_store("nonexistent")

    def test_cannot_unregister_default_store(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test cannot unregister the default store."""
        with pytest.raises(ValueError, match="Cannot unregister default store"):
            router.unregister_store("default")

    def test_get_store(
        self,
        router: TenantStoreRouter,
        mock_dedicated_store: MagicMock,
    ) -> None:
        """Test getting a registered store."""
        router.register_store("dedicated", mock_dedicated_store)

        store = router.get_store("dedicated")

        assert store == mock_dedicated_store

    def test_get_store_returns_none_for_unknown(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test getting an unknown store returns None."""
        store = router.get_store("unknown")

        assert store is None

    def test_list_stores(
        self,
        router: TenantStoreRouter,
        mock_dedicated_store: MagicMock,
    ) -> None:
        """Test listing all registered stores."""
        router.register_store("dedicated", mock_dedicated_store)
        router.register_store("tenant-a", mock_dedicated_store)

        stores = router.list_stores()

        assert "default" in stores
        assert "dedicated" in stores
        assert "tenant-a" in stores


# =============================================================================
# Test Dual-Write Interceptor Management
# =============================================================================


class TestDualWriteInterceptorManagement:
    """Tests for dual-write interceptor management."""

    def test_set_dual_write_interceptor(
        self,
        router: TenantStoreRouter,
        mock_dedicated_store: MagicMock,
    ) -> None:
        """Test setting a dual-write interceptor."""
        tenant_id = uuid4()

        router.set_dual_write_interceptor(tenant_id, mock_dedicated_store)

        assert tenant_id in router._dual_write_interceptors
        assert router._dual_write_interceptors[tenant_id] == mock_dedicated_store

    def test_clear_dual_write_interceptor(
        self,
        router: TenantStoreRouter,
        mock_dedicated_store: MagicMock,
    ) -> None:
        """Test clearing a dual-write interceptor."""
        tenant_id = uuid4()
        router.set_dual_write_interceptor(tenant_id, mock_dedicated_store)

        router.clear_dual_write_interceptor(tenant_id)

        assert tenant_id not in router._dual_write_interceptors

    def test_clear_nonexistent_interceptor(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test clearing a non-existent interceptor doesn't raise."""
        tenant_id = uuid4()

        # Should not raise
        router.clear_dual_write_interceptor(tenant_id)

    def test_has_dual_write_interceptor(
        self,
        router: TenantStoreRouter,
        mock_dedicated_store: MagicMock,
    ) -> None:
        """Test checking if tenant has interceptor."""
        tenant_id = uuid4()

        assert router.has_dual_write_interceptor(tenant_id) is False

        router.set_dual_write_interceptor(tenant_id, mock_dedicated_store)

        assert router.has_dual_write_interceptor(tenant_id) is True


# =============================================================================
# Test Write Pause Management
# =============================================================================


class TestWritePauseManagement:
    """Tests for write pause management."""

    async def test_pause_writes(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test pausing writes for a tenant."""
        tenant_id = uuid4()

        result = await router.pause_writes(tenant_id)

        assert result is True
        assert router.is_paused(tenant_id) is True

    async def test_pause_writes_idempotent(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test pausing writes is idempotent."""
        tenant_id = uuid4()

        result1 = await router.pause_writes(tenant_id)
        result2 = await router.pause_writes(tenant_id)

        # Second call returns False (already paused)
        assert result1 is True
        assert result2 is False
        assert router.is_paused(tenant_id) is True

    async def test_resume_writes(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test resuming writes for a tenant."""
        tenant_id = uuid4()

        await router.pause_writes(tenant_id)
        metrics = await router.resume_writes(tenant_id)

        assert metrics is not None
        assert router.is_paused(tenant_id) is False

    async def test_resume_writes_signals_waiters(
        self,
        router: TenantStoreRouter,
    ) -> None:
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

    async def test_resume_nonexistent_pause(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test resuming a non-paused tenant doesn't raise."""
        tenant_id = uuid4()

        # Should not raise
        await router.resume_writes(tenant_id)

    def test_is_paused(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test checking if tenant is paused."""
        tenant_id = uuid4()

        assert router.is_paused(tenant_id) is False


# =============================================================================
# Test Wait If Paused
# =============================================================================


class TestWaitIfPaused:
    """Tests for _wait_if_paused method."""

    async def test_wait_if_paused_returns_immediately_when_not_paused(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test returns immediately when tenant is not paused."""
        tenant_id = uuid4()

        # Should return immediately without blocking
        await asyncio.wait_for(
            router._wait_if_paused(tenant_id),
            timeout=0.1,
        )

    async def test_wait_if_paused_returns_immediately_for_none_tenant(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test returns immediately when tenant_id is None."""
        await asyncio.wait_for(
            router._wait_if_paused(None),
            timeout=0.1,
        )

    async def test_wait_if_paused_raises_on_timeout(
        self,
        mock_default_store: MagicMock,
        mock_routing_repo: AsyncMock,
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


# =============================================================================
# Test Append Routing
# =============================================================================


class TestAppendRouting:
    """Tests for append() routing logic."""

    async def test_append_empty_list_raises(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test appending empty event list raises ValueError."""
        with pytest.raises(ValueError, match="Cannot append empty event list"):
            await router.append(sid(), [], ExpectedVersion.no_stream())

    async def test_append_routes_to_default_without_tenant(
        self,
        router: TenantStoreRouter,
        mock_default_store: MagicMock,
    ) -> None:
        """Test events without tenant_id route to default store."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

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
        """Test events route to tenant's configured store in NORMAL state."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        # Configure routing
        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="dedicated",
            migration_state=TenantMigrationState.NORMAL,
        )
        mock_routing_repo.get_routing.return_value = routing

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
        """Test events route to source store during BULK_COPY."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="default",
            migration_state=TenantMigrationState.BULK_COPY,
            target_store_id="dedicated",
        )
        mock_routing_repo.get_routing.return_value = routing

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
        """Test events use dual-write interceptor when set."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="default",
            migration_state=TenantMigrationState.DUAL_WRITE,
        )
        mock_routing_repo.get_routing.return_value = routing

        mock_interceptor = create_mock_store(store_id="interceptor")

        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
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
        """Test events route to target store after MIGRATED state."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        # After migration, store_id is updated to target
        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="dedicated",  # Now points to target
            migration_state=TenantMigrationState.MIGRATED,
        )
        mock_routing_repo.get_routing.return_value = routing

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
        """Test append waits when writes are paused."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            write_pause_timeout=0.1,
            enable_tracing=False,
        )

        await router.pause_writes(tenant_id)

        append_started = False
        append_completed = False

        async def do_append():
            nonlocal append_started, append_completed
            append_started = True
            await router.append(stream, [event], ExpectedVersion.no_stream())
            append_completed = True

        append_task = asyncio.create_task(do_append())

        await asyncio.sleep(0.02)
        assert append_started is True
        assert append_completed is False

        await router.resume_writes(tenant_id)
        await asyncio.wait_for(append_task, timeout=1.0)

        assert append_completed is True


# =============================================================================
# Test Read Operations Routing
# =============================================================================


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


# =============================================================================
# Test Read All and Read Stream Routing
# =============================================================================


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

        # Track which store's read_all was called
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


# =============================================================================
# Test Tenant Store Resolution
# =============================================================================


class TestTenantStoreResolution:
    """Tests for get_store_for_tenant and get_write_stores_for_tenant."""

    async def test_get_store_for_tenant_returns_default_if_no_routing(
        self,
        router: TenantStoreRouter,
        mock_default_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test returns default store if no routing configured."""
        tenant_id = uuid4()
        mock_routing_repo.get_routing.return_value = None

        store = await router.get_store_for_tenant(tenant_id)

        assert store == mock_default_store

    async def test_get_store_for_tenant_returns_configured_store(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test returns configured store for tenant."""
        tenant_id = uuid4()

        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="dedicated",
            migration_state=TenantMigrationState.NORMAL,
        )
        mock_routing_repo.get_routing.return_value = routing

        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": mock_dedicated_store},
            enable_tracing=False,
        )

        store = await router.get_store_for_tenant(tenant_id)

        assert store == mock_dedicated_store

    async def test_get_write_stores_for_tenant_returns_default_if_no_routing(
        self,
        router: TenantStoreRouter,
        mock_default_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test returns [default_store] if no routing configured."""
        tenant_id = uuid4()
        mock_routing_repo.get_routing.return_value = None

        stores = await router.get_write_stores_for_tenant(tenant_id)

        assert stores == [mock_default_store]

    async def test_get_write_stores_for_tenant_returns_both_in_dual_write(
        self,
        mock_default_store: MagicMock,
        mock_dedicated_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test returns both stores during DUAL_WRITE phase."""
        tenant_id = uuid4()

        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="default",
            migration_state=TenantMigrationState.DUAL_WRITE,
            target_store_id="dedicated",
        )
        mock_routing_repo.get_routing.return_value = routing

        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores={"dedicated": mock_dedicated_store},
            enable_tracing=False,
        )

        stores = await router.get_write_stores_for_tenant(tenant_id)

        assert len(stores) == 2
        assert mock_default_store in stores
        assert mock_dedicated_store in stores


# =============================================================================
# Test Helper Methods
# =============================================================================


class TestHelperMethods:
    """Tests for helper methods."""

    def test_extract_tenant_id_returns_none_for_empty_list(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test returns None for empty event list."""
        tenant_id = router._extract_tenant_id([])

        assert tenant_id is None

    def test_extract_tenant_id_returns_none_if_no_tenant(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test returns None if event has no tenant_id."""
        event = TestEvent(aggregate_id=uuid4())

        tenant_id = router._extract_tenant_id([event])

        assert tenant_id is None

    def test_extract_tenant_id_returns_tenant_id(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """Test returns tenant_id from first event."""
        expected_tenant_id = uuid4()
        event = TestEvent(aggregate_id=uuid4(), tenant_id=expected_tenant_id)

        tenant_id = router._extract_tenant_id([event])

        assert tenant_id == expected_tenant_id


# =============================================================================
# Test Migration State Routing
# =============================================================================


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

        stores = {
            "source": mock_default_store,
            "target": mock_dedicated_store,
        }

        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="source",
            migration_state=state,
            target_store_id="target",
        )
        mock_routing_repo.get_routing.return_value = routing

        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            stores={"source": mock_default_store, "target": mock_dedicated_store},
            default_store_id="source",
            enable_tracing=False,
        )

        await router.append(stream, [event], ExpectedVersion.no_stream())

        expected_store = stores[expected_store_key]
        expected_store.append.assert_called_once()

    async def test_cutover_paused_state_raises(
        self,
        mock_default_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test CUTOVER_PAUSED state blocks writes."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="default",
            migration_state=TenantMigrationState.CUTOVER_PAUSED,
        )
        mock_routing_repo.get_routing.return_value = routing

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


# =============================================================================
# Test Dual-Write State Without Interceptor
# =============================================================================


class TestDualWriteStateWithoutInterceptor:
    """Tests for DUAL_WRITE state when interceptor is not set."""

    async def test_dual_write_falls_back_to_source_without_interceptor(
        self,
        mock_default_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """Test falls back to source store if interceptor not set."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="default",
            migration_state=TenantMigrationState.DUAL_WRITE,
        )
        mock_routing_repo.get_routing.return_value = routing

        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        # No interceptor set - should fall back to source
        await router.append(stream, [event], ExpectedVersion.no_stream())

        mock_default_store.append.assert_called_once()


# =============================================================================
# Test Structural Conformance to FullEventStore
# =============================================================================


class TestRouterStructuralConformance:
    """The router satisfies `FullEventStore` structurally, not via inheritance."""

    async def test_conforms_to_full_event_store(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """mypy-checked structural conformance, plus a call through each member."""
        _: FullEventStore = router

        assert router.max_append_batch is None

        stream = StreamId(aggregate_id=uuid4(), category="TestAggregate")
        event = TestEvent(aggregate_id=stream.aggregate_id)

        await router.append(stream, [event], ExpectedVersion.no_stream())
        async for _ in router.read_stream(stream):
            pass
        await router.get_stream_version(stream)
        await router.event_exists(event.event_id)
        async for _ in router.read_all():
            pass
        await router.current_position()
        async for _ in router.read_category("TestAggregate"):
            pass


# =============================================================================
# Test Concurrent Operations
# =============================================================================


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
