"""Tests for TenantStoreRouter initialization, registry management, and structural conformance."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.router import (
    StoreNotFoundError,
    TenantStoreRouter,
)
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion, FullEventStore

from ._fixtures import TestEvent, create_mock_store


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


class TestRouterStructuralConformance:
    """The router satisfies `FullEventStore` structurally, not via inheritance."""

    async def test_conforms_to_full_event_store(
        self,
        router: TenantStoreRouter,
    ) -> None:
        """mypy-checked structural conformance, plus a call through each member."""
        _: FullEventStore = router

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
