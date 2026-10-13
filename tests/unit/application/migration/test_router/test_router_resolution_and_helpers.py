"""Tests for tenant store resolution and helper methods."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.router import (
    StoreNotFoundError,
    TenantStoreRouter,
)
from eventsource.ports.migration.models import (
    TenantMigrationState,
    TenantRouting,
)

from ._fixtures import TestEvent


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

    async def test_get_write_stores_for_tenant_raises_if_target_not_registered(
        self,
        mock_default_store: MagicMock,
        mock_routing_repo: AsyncMock,
    ) -> None:
        """DUAL_WRITE with a target_store_id that was never registered must
        raise StoreNotFoundError, not silently drop the target and return
        only the source store."""
        tenant_id = uuid4()

        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id="default",
            migration_state=TenantMigrationState.DUAL_WRITE,
            target_store_id="never-registered",
        )
        mock_routing_repo.get_routing.return_value = routing

        router = TenantStoreRouter(
            default_store=mock_default_store,
            routing_repo=mock_routing_repo,
            enable_tracing=False,
        )

        with pytest.raises(StoreNotFoundError) as exc_info:
            await router.get_write_stores_for_tenant(tenant_id)

        assert exc_info.value.store_id == "never-registered"


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
