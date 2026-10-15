"""Unit tests for TenantRoutingRepository caching behavior."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.routing import PostgreSQLTenantRoutingRepository
from eventsource.ports.migration.models import TenantMigrationState


class TestPostgreSQLTenantRoutingRepositoryCaching:
    """Tests for PostgreSQLTenantRoutingRepository caching behavior."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo_with_cache(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=True, cache_ttl_seconds=1.0
        )

    @pytest.mark.asyncio
    async def test_get_routing_caches_result(
        self, repo_with_cache: PostgreSQLTenantRoutingRepository
    ) -> None:
        tenant_id = uuid4()
        now = datetime.now(UTC)
        row = (tenant_id, "shared", "normal", None, now, now)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_conn.execute.return_value = MagicMock(**{"fetchone.return_value": row})
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            assert await repo_with_cache.get_routing(tenant_id) is not None
            assert await repo_with_cache.get_routing(tenant_id) is not None
            assert mock_conn.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_cache_expires_after_ttl(
        self, repo_with_cache: PostgreSQLTenantRoutingRepository
    ) -> None:
        tenant_id = uuid4()
        now = datetime.now(UTC)
        row = (tenant_id, "shared", "normal", None, now, now)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_conn.execute.return_value = MagicMock(**{"fetchone.return_value": row})
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo_with_cache.get_routing(tenant_id)

            with patch("time.monotonic") as mock_time:
                mock_time.return_value = time.monotonic() + 2.0
                await repo_with_cache.clear_cache()
                await repo_with_cache.get_routing(tenant_id)

            assert mock_conn.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_set_routing_invalidates_cache(
        self, repo_with_cache: PostgreSQLTenantRoutingRepository
    ) -> None:
        tenant_id = uuid4()
        now = datetime.now(UTC)
        row = (tenant_id, "shared", "normal", None, now, now)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_conn.execute.return_value = MagicMock(**{"fetchone.return_value": row})
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo_with_cache.get_routing(tenant_id)
            await repo_with_cache.set_routing(tenant_id, "dedicated")
            await repo_with_cache.get_routing(tenant_id)
            assert mock_conn.execute.call_count == 3

    @pytest.mark.asyncio
    async def test_set_migration_state_invalidates_cache(
        self, repo_with_cache: PostgreSQLTenantRoutingRepository
    ) -> None:
        tenant_id = uuid4()
        now = datetime.now(UTC)
        row = (tenant_id, "shared", "normal", None, now, now)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_conn.execute.return_value = MagicMock(**{"fetchone.return_value": row})
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo_with_cache.get_routing(tenant_id)
            await repo_with_cache.set_migration_state(tenant_id, TenantMigrationState.DUAL_WRITE)
            await repo_with_cache.get_routing(tenant_id)
            assert mock_conn.execute.call_count == 3

    @pytest.mark.asyncio
    async def test_delete_routing_invalidates_cache(
        self, repo_with_cache: PostgreSQLTenantRoutingRepository
    ) -> None:
        tenant_id = uuid4()
        now = datetime.now(UTC)
        row = (tenant_id, "shared", "normal", None, now, now)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_conn.execute.return_value = MagicMock(rowcount=1, **{"fetchone.return_value": row})
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo_with_cache.get_routing(tenant_id)
            await repo_with_cache.delete_routing(tenant_id)
            assert await repo_with_cache._get_from_cache(tenant_id) is None

    @pytest.mark.asyncio
    async def test_clear_cache_removes_all_entries(
        self, repo_with_cache: PostgreSQLTenantRoutingRepository
    ) -> None:
        tenant1_id, tenant2_id = uuid4(), uuid4()
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

            await repo_with_cache.get_routing(tenant1_id)
            await repo_with_cache.get_routing(tenant2_id)
            await repo_with_cache.clear_cache()

            assert await repo_with_cache._get_from_cache(tenant1_id) is None
            assert await repo_with_cache._get_from_cache(tenant2_id) is None
