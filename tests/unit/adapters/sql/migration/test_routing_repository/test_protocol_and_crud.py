"""Unit tests for TenantRoutingRepository protocol and CRUD operations."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.routing import PostgreSQLTenantRoutingRepository
from eventsource.ports.migration.models import TenantMigrationState
from eventsource.ports.migration.repositories import TenantRoutingRepository


class TestTenantRoutingRepositoryProtocol:
    """Tests for TenantRoutingRepository protocol."""

    def test_postgresql_repository_implements_protocol(self) -> None:
        """Test PostgreSQLTenantRoutingRepository implements the protocol."""
        repo = PostgreSQLTenantRoutingRepository(MagicMock())
        assert isinstance(repo, TenantRoutingRepository)

    def test_protocol_has_required_methods(self) -> None:
        """Test protocol defines all required methods."""
        assert hasattr(TenantRoutingRepository, "get_routing")
        assert hasattr(TenantRoutingRepository, "get_or_default")
        assert hasattr(TenantRoutingRepository, "set_routing")
        assert hasattr(TenantRoutingRepository, "set_migration_state")
        assert hasattr(TenantRoutingRepository, "clear_migration_state")
        assert hasattr(TenantRoutingRepository, "list_by_state")
        assert hasattr(TenantRoutingRepository, "list_by_store")


class TestPostgreSQLTenantRoutingRepositoryInit:
    """Tests for PostgreSQLTenantRoutingRepository initialization."""

    def test_init_with_connection(self) -> None:
        """Test initialization with a connection."""
        mock_conn = MagicMock()
        repo = PostgreSQLTenantRoutingRepository(mock_conn)
        assert repo._conn == mock_conn

    def test_init_with_tracing_enabled(self) -> None:
        """Test initialization with tracing enabled."""
        mock_conn = MagicMock()
        repo = PostgreSQLTenantRoutingRepository(mock_conn, enable_tracing=True)
        assert repo._conn == mock_conn

    def test_init_with_tracing_disabled(self) -> None:
        """Test initialization with tracing disabled."""
        mock_conn = MagicMock()
        repo = PostgreSQLTenantRoutingRepository(mock_conn, enable_tracing=False)
        assert repo._conn == mock_conn

    def test_init_with_cache_enabled(self) -> None:
        """Test initialization with cache enabled (default)."""
        repo = PostgreSQLTenantRoutingRepository(MagicMock())
        assert repo._enable_cache is True
        assert repo._cache_ttl == 5.0

    def test_init_with_cache_disabled(self) -> None:
        """Test initialization with cache disabled."""
        repo = PostgreSQLTenantRoutingRepository(MagicMock(), enable_cache=False)
        assert repo._enable_cache is False

    def test_init_with_custom_cache_ttl(self) -> None:
        """Test initialization with custom cache TTL."""
        repo = PostgreSQLTenantRoutingRepository(MagicMock(), cache_ttl_seconds=10.0)
        assert repo._cache_ttl == 10.0


class TestPostgreSQLTenantRoutingRepositoryGetRouting:
    """Tests for PostgreSQLTenantRoutingRepository.get_routing method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_get_routing_returns_none_when_not_found(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        """Test get_routing returns None when tenant not found."""
        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_routing(uuid4())
            assert result is None

    @pytest.mark.asyncio
    async def test_get_routing_returns_routing_when_found(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        """Test get_routing returns routing when found."""
        tenant_id = uuid4()
        migration_id = uuid4()
        now = datetime.now(UTC)
        row = (tenant_id, "shared", "dual_write", migration_id, now, now)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_routing(tenant_id)
            assert result is not None
            assert result.tenant_id == tenant_id
            assert result.store_id == "shared"
            assert result.migration_state == TenantMigrationState.DUAL_WRITE
            assert result.active_migration_id == migration_id


class TestPostgreSQLTenantRoutingRepositoryGetOrDefault:
    """Tests for PostgreSQLTenantRoutingRepository.get_or_default method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_get_or_default_returns_existing(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        """Test get_or_default returns existing routing."""
        tenant_id = uuid4()
        now = datetime.now(UTC)
        row = (tenant_id, "shared", "normal", None, now, now)

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_or_default(tenant_id, "default")
            assert result.store_id == "shared"

    @pytest.mark.asyncio
    async def test_get_or_default_creates_new(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        """Test get_or_default creates new routing when not exists."""
        tenant_id = uuid4()
        now = datetime.now(UTC)
        inserted_row = (tenant_id, "default", "normal", None, now, now)
        call_count = 0

        def mock_fetchone():
            nonlocal call_count
            call_count += 1
            return None if call_count == 1 else inserted_row

        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.side_effect = mock_fetchone
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_or_default(tenant_id, "default")
            assert result.tenant_id == tenant_id
            assert result.store_id == "default"
            assert result.migration_state == TenantMigrationState.NORMAL


class TestPostgreSQLTenantRoutingRepositorySetRouting:
    """Tests for PostgreSQLTenantRoutingRepository.set_routing method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_set_routing_executes_upsert(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        """Test set_routing executes UPSERT query."""
        tenant_id = uuid4()
        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.set_routing(tenant_id, "dedicated")
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert params["tenant_id"] == tenant_id
            assert params["store_id"] == "dedicated"
            assert params["state"] == "normal"


class TestPostgreSQLTenantRoutingRepositoryDeleteRouting:
    """Tests for PostgreSQLTenantRoutingRepository.delete_routing method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_delete_routing_returns_true_when_deleted(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        """Test delete_routing returns True when a row is deleted."""
        tenant_id = uuid4()
        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 1
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.delete_routing(tenant_id)
            assert result is True

    @pytest.mark.asyncio
    async def test_delete_routing_returns_false_when_not_found(
        self, repo: PostgreSQLTenantRoutingRepository
    ) -> None:
        """Test delete_routing returns False when no row exists."""
        tenant_id = uuid4()
        with patch("eventsource.adapters.sql.migration.routing.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 0
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.delete_routing(tenant_id)
            assert result is False
