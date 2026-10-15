"""Unit tests for TenantRoutingRepository migration state and listing operations."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.routing import PostgreSQLTenantRoutingRepository
from eventsource.ports.migration.models import TenantMigrationState


@pytest.fixture
def mock_conn() -> MagicMock:
    return MagicMock()


@pytest.fixture
def repo(mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
    return PostgreSQLTenantRoutingRepository(mock_conn, enable_tracing=False, enable_cache=False)


class TestPostgreSQLTenantRoutingRepositorySetMigrationState:
    """Tests for PostgreSQLTenantRoutingRepository.set_migration_state method."""

    @pytest.mark.asyncio
    async def test_set_migration_state_with_migration_id(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        tenant_id = uuid4()
        migration_id = uuid4()
        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.set_migration_state(tenant_id, TenantMigrationState.DUAL_WRITE, migration_id)
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert params["tenant_id"] == tenant_id
            assert params["state"] == "dual_write"
            assert params["migration_id"] == migration_id

    @pytest.mark.asyncio
    async def test_set_migration_state_without_migration_id(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        tenant_id = uuid4()
        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.set_migration_state(tenant_id, TenantMigrationState.NORMAL)
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert params["migration_id"] is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "state",
        [
            TenantMigrationState.NORMAL,
            TenantMigrationState.BULK_COPY,
            TenantMigrationState.DUAL_WRITE,
            TenantMigrationState.CUTOVER_PAUSED,
            TenantMigrationState.MIGRATED,
        ],
    )
    async def test_set_migration_state_all_states(
        self, repo: PostgreSQLTenantRoutingRepository, state: TenantMigrationState
    ) -> None:
        tenant_id = uuid4()
        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.set_migration_state(tenant_id, state)
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert params["state"] == state.value


class TestPostgreSQLTenantRoutingRepositoryClearMigrationState:
    """Tests for PostgreSQLTenantRoutingRepository.clear_migration_state method."""

    @pytest.mark.asyncio
    async def test_clear_migration_state_resets_to_normal(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        tenant_id = uuid4()
        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.clear_migration_state(tenant_id)
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert params["state"] == "normal"
            assert params["migration_id"] is None


class TestPostgreSQLTenantRoutingRepositoryListByState:
    """Tests for PostgreSQLTenantRoutingRepository.list_by_state method."""

    @pytest.mark.asyncio
    async def test_list_by_state_returns_empty_list(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            result = await repo.list_by_state(TenantMigrationState.DUAL_WRITE)
            assert result == []

    @pytest.mark.asyncio
    async def test_list_by_state_returns_matching_tenants(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        now = datetime.now(UTC)
        tenant1_id, tenant2_id, migration_id = uuid4(), uuid4(), uuid4()
        rows = [
            (tenant1_id, "shared", "dual_write", migration_id, now, now),
            (tenant2_id, "shared", "dual_write", migration_id, now, now),
        ]
        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = rows
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            result = await repo.list_by_state(TenantMigrationState.DUAL_WRITE)
            assert len(result) == 2
            assert result[0].tenant_id == tenant1_id
            assert result[1].tenant_id == tenant2_id
            assert all(r.migration_state == TenantMigrationState.DUAL_WRITE for r in result)


class TestPostgreSQLTenantRoutingRepositoryListByStore:
    """Tests for PostgreSQLTenantRoutingRepository.list_by_store method."""

    @pytest.mark.asyncio
    async def test_list_by_store_returns_empty_list(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            result = await repo.list_by_store("nonexistent")
            assert result == []

    @pytest.mark.asyncio
    async def test_list_by_store_returns_matching_tenants(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        now = datetime.now(UTC)
        tenant1_id, tenant2_id = uuid4(), uuid4()
        rows = [
            (tenant1_id, "shared", "normal", None, now, now),
            (tenant2_id, "shared", "bulk_copy", uuid4(), now, now),
        ]
        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = rows
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            result = await repo.list_by_store("shared")
            assert len(result) == 2
            assert result[0].tenant_id == tenant1_id
            assert result[1].tenant_id == tenant2_id
            assert all(r.store_id == "shared" for r in result)
