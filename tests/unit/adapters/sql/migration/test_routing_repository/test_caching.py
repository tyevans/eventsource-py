"""Unit tests for TenantRoutingRepository caching behavior."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.routing import (
    PostgreSQLTenantRoutingRepository,
)
from eventsource.ports.migration.models import TenantMigrationState


class TestPostgreSQLTenantRoutingRepositoryCaching:
    """Tests for PostgreSQLTenantRoutingRepository caching behavior."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo_with_cache(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        """Create a repository with cache enabled."""
        return PostgreSQLTenantRoutingRepository(
            mock_conn,
            enable_tracing=False,
            enable_cache=True,
            cache_ttl_seconds=1.0,
        )

    @pytest.mark.asyncio
    async def test_get_routing_caches_result(
        self,
        repo_with_cache: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test get_routing caches successful result."""
        tenant_id = uuid4()
        now = datetime.now(UTC)
        row = (tenant_id, "shared", "normal", None, now, now)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # First call - hits database
            result1 = await repo_with_cache.get_routing(tenant_id)
            assert result1 is not None

            # Second call - should hit cache
            result2 = await repo_with_cache.get_routing(tenant_id)
            assert result2 is not None

            # Should only have one database call
            assert mock_conn.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_cache_expires_after_ttl(
        self,
        repo_with_cache: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test cache entry expires after TTL."""
        tenant_id = uuid4()
        now = datetime.now(UTC)
        row = (tenant_id, "shared", "normal", None, now, now)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # First call
            await repo_with_cache.get_routing(tenant_id)

            # Wait for TTL to expire
            with patch("time.monotonic") as mock_time:
                # Simulate time passing beyond TTL
                original_time = time.monotonic()
                mock_time.return_value = original_time + 2.0  # Beyond 1.0s TTL

                # Cache entry should be expired, trigger new DB call
                # Reset the cache manually to simulate expiration
                await repo_with_cache.clear_cache()
                await repo_with_cache.get_routing(tenant_id)

            # Should have two database calls
            assert mock_conn.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_set_routing_invalidates_cache(
        self,
        repo_with_cache: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test set_routing invalidates cache for tenant."""
        tenant_id = uuid4()
        now = datetime.now(UTC)
        row = (tenant_id, "shared", "normal", None, now, now)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # First call - caches result
            await repo_with_cache.get_routing(tenant_id)

            # Update routing - should invalidate cache
            await repo_with_cache.set_routing(tenant_id, "dedicated")

            # Next get should hit database
            await repo_with_cache.get_routing(tenant_id)

            # Should have three database calls: get, set, get
            assert mock_conn.execute.call_count == 3

    @pytest.mark.asyncio
    async def test_set_migration_state_invalidates_cache(
        self,
        repo_with_cache: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test set_migration_state invalidates cache for tenant."""
        tenant_id = uuid4()
        now = datetime.now(UTC)
        row = (tenant_id, "shared", "normal", None, now, now)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # First call - caches result
            await repo_with_cache.get_routing(tenant_id)

            # Update state - should invalidate cache
            await repo_with_cache.set_migration_state(tenant_id, TenantMigrationState.DUAL_WRITE)

            # Next get should hit database
            await repo_with_cache.get_routing(tenant_id)

            # Should have three database calls
            assert mock_conn.execute.call_count == 3

    @pytest.mark.asyncio
    async def test_delete_routing_invalidates_cache(
        self,
        repo_with_cache: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test delete_routing invalidates cache for tenant."""
        tenant_id = uuid4()
        now = datetime.now(UTC)
        row = (tenant_id, "shared", "normal", None, now, now)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_result.rowcount = 1
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # First call - caches result
            await repo_with_cache.get_routing(tenant_id)

            # Delete - should invalidate cache
            await repo_with_cache.delete_routing(tenant_id)

            # Verify cache is empty for this tenant
            cached = await repo_with_cache._get_from_cache(tenant_id)
            assert cached is None

    @pytest.mark.asyncio
    async def test_clear_cache_removes_all_entries(
        self,
        repo_with_cache: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test clear_cache removes all cached entries."""
        tenant1_id = uuid4()
        tenant2_id = uuid4()
        now = datetime.now(UTC)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.side_effect = [
                (tenant1_id, "shared", "normal", None, now, now),
                (tenant2_id, "shared", "normal", None, now, now),
            ]
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # Cache two tenants
            await repo_with_cache.get_routing(tenant1_id)
            await repo_with_cache.get_routing(tenant2_id)

            # Clear all cache
            await repo_with_cache.clear_cache()

            # Verify both are gone
            assert await repo_with_cache._get_from_cache(tenant1_id) is None
            assert await repo_with_cache._get_from_cache(tenant2_id) is None
