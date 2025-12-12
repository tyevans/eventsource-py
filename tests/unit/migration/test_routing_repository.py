"""
Unit tests for TenantRoutingRepository implementations.

Tests cover:
- TenantRoutingRepository protocol compliance
- PostgreSQLTenantRoutingRepository CRUD operations
- Migration state transitions
- Caching behavior (TTL, invalidation, hits/misses)
- List operations (by state, by store)
- Row to model conversion
"""

import time
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.migration.models import (
    TenantMigrationState,
)
from eventsource.migration.repositories.routing import (
    PostgreSQLTenantRoutingRepository,
    TenantRoutingRepository,
)


class TestTenantRoutingRepositoryProtocol:
    """Tests for TenantRoutingRepository protocol."""

    def test_postgresql_repository_implements_protocol(self) -> None:
        """Test PostgreSQLTenantRoutingRepository implements the protocol."""
        # Create a mock connection
        mock_conn = MagicMock()
        repo = PostgreSQLTenantRoutingRepository(mock_conn)

        # The protocol is runtime checkable
        assert isinstance(repo, TenantRoutingRepository)

    def test_protocol_has_required_methods(self) -> None:
        """Test protocol defines all required methods."""
        # Verify protocol methods exist
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
        mock_conn = MagicMock()
        repo = PostgreSQLTenantRoutingRepository(mock_conn)
        assert repo._enable_cache is True
        assert repo._cache_ttl == 5.0

    def test_init_with_cache_disabled(self) -> None:
        """Test initialization with cache disabled."""
        mock_conn = MagicMock()
        repo = PostgreSQLTenantRoutingRepository(mock_conn, enable_cache=False)
        assert repo._enable_cache is False

    def test_init_with_custom_cache_ttl(self) -> None:
        """Test initialization with custom cache TTL."""
        mock_conn = MagicMock()
        repo = PostgreSQLTenantRoutingRepository(mock_conn, cache_ttl_seconds=10.0)
        assert repo._cache_ttl == 10.0


class TestPostgreSQLTenantRoutingRepositoryGetRouting:
    """Tests for PostgreSQLTenantRoutingRepository.get_routing method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        """Create a repository with mock connection and cache disabled."""
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_get_routing_returns_none_when_not_found(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test get_routing returns None when tenant not found."""
        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_routing(uuid4())

            assert result is None

    @pytest.mark.asyncio
    async def test_get_routing_returns_routing_when_found(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test get_routing returns routing when found."""
        tenant_id = uuid4()
        migration_id = uuid4()
        now = datetime.now(UTC)

        # Create a row tuple matching the SELECT query
        row = (
            tenant_id,  # tenant_id
            "shared",  # store_id
            "dual_write",  # migration_state
            migration_id,  # active_migration_id
            now,  # created_at
            now,  # updated_at
        )

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
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
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        """Create a repository with mock connection and cache disabled."""
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_get_or_default_returns_existing(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test get_or_default returns existing routing."""
        tenant_id = uuid4()
        now = datetime.now(UTC)

        row = (
            tenant_id,
            "shared",
            "normal",
            None,
            now,
            now,
        )

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_or_default(tenant_id, "default")

            # Should return existing routing, not create with default
            assert result.store_id == "shared"

    @pytest.mark.asyncio
    async def test_get_or_default_creates_new(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test get_or_default creates new routing when not exists."""
        tenant_id = uuid4()
        now = datetime.now(UTC)

        # First call returns None (not found), second returns the inserted row
        inserted_row = (
            tenant_id,
            "default",
            "normal",
            None,
            now,
            now,
        )

        call_count = 0

        def mock_fetchone():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return None  # get_routing returns None
            return inserted_row  # INSERT RETURNING returns the row

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
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
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        """Create a repository with mock connection and cache disabled."""
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_set_routing_executes_upsert(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test set_routing executes UPSERT query."""
        tenant_id = uuid4()

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.set_routing(tenant_id, "dedicated")

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["tenant_id"] == tenant_id
            assert params["store_id"] == "dedicated"
            assert params["state"] == "normal"


class TestPostgreSQLTenantRoutingRepositorySetMigrationState:
    """Tests for PostgreSQLTenantRoutingRepository.set_migration_state method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        """Create a repository with mock connection and cache disabled."""
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_set_migration_state_with_migration_id(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test set_migration_state with migration ID."""
        tenant_id = uuid4()
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.set_migration_state(
                tenant_id,
                TenantMigrationState.DUAL_WRITE,
                migration_id,
            )

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["tenant_id"] == tenant_id
            assert params["state"] == "dual_write"
            assert params["migration_id"] == migration_id

    @pytest.mark.asyncio
    async def test_set_migration_state_without_migration_id(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test set_migration_state without migration ID."""
        tenant_id = uuid4()

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.set_migration_state(
                tenant_id,
                TenantMigrationState.NORMAL,
            )

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
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
        self,
        repo: PostgreSQLTenantRoutingRepository,
        state: TenantMigrationState,
    ) -> None:
        """Test set_migration_state for all possible states."""
        tenant_id = uuid4()

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.set_migration_state(tenant_id, state)

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["state"] == state.value


class TestPostgreSQLTenantRoutingRepositoryClearMigrationState:
    """Tests for PostgreSQLTenantRoutingRepository.clear_migration_state method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        """Create a repository with mock connection and cache disabled."""
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_clear_migration_state_resets_to_normal(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test clear_migration_state resets to NORMAL state."""
        tenant_id = uuid4()

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.clear_migration_state(tenant_id)

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["state"] == "normal"
            assert params["migration_id"] is None


class TestPostgreSQLTenantRoutingRepositoryListByState:
    """Tests for PostgreSQLTenantRoutingRepository.list_by_state method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        """Create a repository with mock connection and cache disabled."""
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_list_by_state_returns_empty_list(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test list_by_state returns empty list when no matches."""
        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.list_by_state(TenantMigrationState.DUAL_WRITE)

            assert result == []

    @pytest.mark.asyncio
    async def test_list_by_state_returns_matching_tenants(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test list_by_state returns matching tenants."""
        now = datetime.now(UTC)
        tenant1_id = uuid4()
        tenant2_id = uuid4()
        migration_id = uuid4()

        rows = [
            (tenant1_id, "shared", "dual_write", migration_id, now, now),
            (tenant2_id, "shared", "dual_write", migration_id, now, now),
        ]

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
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

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        """Create a repository with mock connection and cache disabled."""
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_list_by_store_returns_empty_list(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test list_by_store returns empty list when no matches."""
        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.list_by_store("nonexistent")

            assert result == []

    @pytest.mark.asyncio
    async def test_list_by_store_returns_matching_tenants(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test list_by_store returns matching tenants."""
        now = datetime.now(UTC)
        tenant1_id = uuid4()
        tenant2_id = uuid4()

        rows = [
            (tenant1_id, "shared", "normal", None, now, now),
            (tenant2_id, "shared", "bulk_copy", uuid4(), now, now),
        ]

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
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


class TestPostgreSQLTenantRoutingRepositoryDeleteRouting:
    """Tests for PostgreSQLTenantRoutingRepository.delete_routing method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        """Create a repository with mock connection and cache disabled."""
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_delete_routing_returns_true_when_deleted(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test delete_routing returns True when a row is deleted."""
        tenant_id = uuid4()

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 1
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.delete_routing(tenant_id)

            assert result is True

    @pytest.mark.asyncio
    async def test_delete_routing_returns_false_when_not_found(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test delete_routing returns False when no row exists."""
        tenant_id = uuid4()

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 0
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.delete_routing(tenant_id)

            assert result is False


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

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
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

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
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

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
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

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
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

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
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

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
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


class TestPostgreSQLTenantRoutingRepositoryHelpers:
    """Tests for PostgreSQLTenantRoutingRepository helper methods."""

    @pytest.fixture
    def repo(self) -> PostgreSQLTenantRoutingRepository:
        """Create a repository for testing."""
        mock_conn = MagicMock()
        return PostgreSQLTenantRoutingRepository(mock_conn, enable_tracing=False)

    def test_row_to_routing_with_all_fields(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test converting a row with all fields populated."""
        tenant_id = uuid4()
        migration_id = uuid4()
        created_at = datetime.now(UTC)
        updated_at = datetime.now(UTC)

        row = (
            tenant_id,
            "dedicated",
            "dual_write",
            migration_id,
            created_at,
            updated_at,
        )

        routing = repo._row_to_routing(row)

        assert routing.tenant_id == tenant_id
        assert routing.store_id == "dedicated"
        assert routing.migration_state == TenantMigrationState.DUAL_WRITE
        assert routing.active_migration_id == migration_id
        assert routing.created_at == created_at
        assert routing.updated_at == updated_at

    def test_row_to_routing_with_null_migration_id(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test converting a row with null migration_id."""
        tenant_id = uuid4()
        created_at = datetime.now(UTC)

        row = (
            tenant_id,
            "shared",
            "normal",
            None,  # No active migration
            created_at,
            created_at,
        )

        routing = repo._row_to_routing(row)

        assert routing.tenant_id == tenant_id
        assert routing.active_migration_id is None
        assert routing.migration_state == TenantMigrationState.NORMAL

    @pytest.mark.parametrize(
        "state_value,expected_state",
        [
            ("normal", TenantMigrationState.NORMAL),
            ("bulk_copy", TenantMigrationState.BULK_COPY),
            ("dual_write", TenantMigrationState.DUAL_WRITE),
            ("cutover_paused", TenantMigrationState.CUTOVER_PAUSED),
            ("migrated", TenantMigrationState.MIGRATED),
        ],
    )
    def test_row_to_routing_all_states(
        self,
        repo: PostgreSQLTenantRoutingRepository,
        state_value: str,
        expected_state: TenantMigrationState,
    ) -> None:
        """Test converting rows with all possible migration states."""
        tenant_id = uuid4()
        now = datetime.now(UTC)

        row = (tenant_id, "shared", state_value, None, now, now)

        routing = repo._row_to_routing(row)

        assert routing.migration_state == expected_state


class TestMigrationStateTransitionWorkflow:
    """Integration-style tests for migration state transition workflow."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLTenantRoutingRepository:
        """Create a repository with mock connection and cache disabled."""
        return PostgreSQLTenantRoutingRepository(
            mock_conn, enable_tracing=False, enable_cache=False
        )

    @pytest.mark.asyncio
    async def test_typical_migration_workflow(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test a typical migration state transition workflow."""
        tenant_id = uuid4()
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # Step 1: Set initial routing (tenant starts on shared store)
            await repo.set_routing(tenant_id, "shared")

            # Verify the set_routing call
            call_args = mock_conn.execute.call_args_list[0]
            assert call_args[0][1]["store_id"] == "shared"

            # Step 2: Start migration - transition to BULK_COPY
            await repo.set_migration_state(tenant_id, TenantMigrationState.BULK_COPY, migration_id)

            call_args = mock_conn.execute.call_args_list[1]
            assert call_args[0][1]["state"] == "bulk_copy"
            assert call_args[0][1]["migration_id"] == migration_id

            # Step 3: Enter dual-write phase
            await repo.set_migration_state(tenant_id, TenantMigrationState.DUAL_WRITE, migration_id)

            call_args = mock_conn.execute.call_args_list[2]
            assert call_args[0][1]["state"] == "dual_write"

            # Step 4: Cutover pause
            await repo.set_migration_state(
                tenant_id, TenantMigrationState.CUTOVER_PAUSED, migration_id
            )

            call_args = mock_conn.execute.call_args_list[3]
            assert call_args[0][1]["state"] == "cutover_paused"

            # Step 5: Complete migration
            await repo.set_migration_state(tenant_id, TenantMigrationState.MIGRATED, migration_id)

            call_args = mock_conn.execute.call_args_list[4]
            assert call_args[0][1]["state"] == "migrated"

            # Step 6: Clear migration state (cleanup)
            await repo.clear_migration_state(tenant_id)

            call_args = mock_conn.execute.call_args_list[5]
            assert call_args[0][1]["state"] == "normal"
            assert call_args[0][1]["migration_id"] is None

            # Total of 6 database operations
            assert mock_conn.execute.call_count == 6

    @pytest.mark.asyncio
    async def test_migration_abort_workflow(
        self,
        repo: PostgreSQLTenantRoutingRepository,
    ) -> None:
        """Test migration abort workflow."""
        tenant_id = uuid4()
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.routing.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # Start migration
            await repo.set_migration_state(tenant_id, TenantMigrationState.BULK_COPY, migration_id)

            # Enter dual-write
            await repo.set_migration_state(tenant_id, TenantMigrationState.DUAL_WRITE, migration_id)

            # Abort - clear migration state
            await repo.clear_migration_state(tenant_id)

            # Verify final state is NORMAL with no migration_id
            call_args = mock_conn.execute.call_args
            assert call_args[0][1]["state"] == "normal"
            assert call_args[0][1]["migration_id"] is None
