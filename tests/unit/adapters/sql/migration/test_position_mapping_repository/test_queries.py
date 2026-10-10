"""Unit tests for PositionMappingRepository listing, range queries, counts, bounds and deletions."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.position_mapping import (
    PostgreSQLPositionMappingRepository,
)
from eventsource.ports.migration.models import PositionMapping

from ._helpers import pos


class TestPostgreSQLPositionMappingRepositoryListByMigration:
    """Tests for PostgreSQLPositionMappingRepository.list_by_migration."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_list_by_migration_returns_empty_list(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test list_by_migration returns empty list when no mappings."""
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.list_by_migration(uuid4())

            assert result == []

    @pytest.mark.asyncio
    async def test_list_by_migration_returns_mappings_ordered_by_id(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test list_by_migration returns mappings ordered by id (source-position order)."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        rows = [
            (1, migration_id, pos(100, "src").to_str(), pos(50, "tgt").to_str(), uuid4(), now),
            (2, migration_id, pos(200, "src").to_str(), pos(100, "tgt").to_str(), uuid4(), now),
            (3, migration_id, pos(300, "src").to_str(), pos(150, "tgt").to_str(), uuid4(), now),
        ]

        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = rows
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.list_by_migration(migration_id)

            assert len(result) == 3
            assert result[0].source_position == pos(100, "src")
            assert result[1].source_position == pos(200, "src")
            assert result[2].source_position == pos(300, "src")

            call_args = mock_conn.execute.call_args
            query_text = str(call_args[0][0])
            assert "ORDER BY id ASC" in query_text

    @pytest.mark.asyncio
    async def test_list_by_migration_uses_pagination(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test list_by_migration uses limit and offset."""
        migration_id = uuid4()

        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.list_by_migration(migration_id, limit=50, offset=100)

            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["limit"] == 50
            assert params["offset"] == 100


class TestPostgreSQLPositionMappingRepositoryListInSourceRange:
    """Tests for PostgreSQLPositionMappingRepository.list_in_source_range."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_list_in_source_range_returns_empty_when_no_mappings(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test list_in_source_range returns empty list when no mappings exist."""
        repo.count_by_migration = AsyncMock(return_value=0)  # type: ignore[method-assign]

        result = await repo.list_in_source_range(uuid4(), pos(0), pos(100))

        assert result == []

    @pytest.mark.asyncio
    async def test_list_in_source_range_returns_matching_mappings(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test list_in_source_range resolves ordinal bounds and delegates to list_by_migration."""
        migration_id = uuid4()
        mappings = [
            PositionMapping(
                migration_id=migration_id,
                source_position=pos(n, "src"),
                target_position=pos(n // 2, "tgt"),
                event_id=uuid4(),
                mapped_at=datetime.now(UTC),
            )
            for n in (100, 150, 200)
        ]

        repo.count_by_migration = AsyncMock(return_value=3)  # type: ignore[method-assign]
        repo._find_first_ordinal_gte = AsyncMock(return_value=0)  # type: ignore[method-assign]
        repo._find_last_ordinal_lte = AsyncMock(return_value=2)  # type: ignore[method-assign]
        repo.list_by_migration = AsyncMock(return_value=mappings)  # type: ignore[method-assign]

        result = await repo.list_in_source_range(migration_id, pos(100), pos(200))

        assert len(result) == 3
        repo.list_by_migration.assert_called_once_with(migration_id, limit=3, offset=0)

    @pytest.mark.asyncio
    async def test_list_in_source_range_empty_when_bounds_invert(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test list_in_source_range returns [] when the lower ordinal exceeds the upper."""
        migration_id = uuid4()
        repo.count_by_migration = AsyncMock(return_value=5)  # type: ignore[method-assign]
        repo._find_first_ordinal_gte = AsyncMock(return_value=4)  # type: ignore[method-assign]
        repo._find_last_ordinal_lte = AsyncMock(return_value=1)  # type: ignore[method-assign]

        result = await repo.list_in_source_range(migration_id, pos(900), pos(100))

        assert result == []


class TestPostgreSQLPositionMappingRepositoryCountByMigration:
    """Tests for PostgreSQLPositionMappingRepository.count_by_migration."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_count_returns_zero_when_no_mappings(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test count_by_migration returns 0 when no mappings."""
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (0,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.count_by_migration(uuid4())

            assert result == 0

    @pytest.mark.asyncio
    async def test_count_returns_correct_count(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test count_by_migration returns correct count."""
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (1500,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.count_by_migration(uuid4())

            assert result == 1500


class TestPostgreSQLPositionMappingRepositoryGetPositionBounds:
    """Tests for PostgreSQLPositionMappingRepository.get_position_bounds."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_bounds_returns_none_when_no_mappings(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test get_position_bounds returns None when no mappings."""
        repo.count_by_migration = AsyncMock(return_value=0)  # type: ignore[method-assign]

        result = await repo.get_position_bounds(uuid4())

        assert result is None

    @pytest.mark.asyncio
    async def test_get_bounds_returns_first_and_last_by_id(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test get_position_bounds returns the first and last mapping by id."""
        migration_id = uuid4()
        first = PositionMapping(
            migration_id=migration_id,
            source_position=pos(100, "src"),
            target_position=pos(50, "tgt"),
            event_id=uuid4(),
            mapped_at=datetime.now(UTC),
        )
        last = PositionMapping(
            migration_id=migration_id,
            source_position=pos(10000, "src"),
            target_position=pos(5000, "tgt"),
            event_id=uuid4(),
            mapped_at=datetime.now(UTC),
        )

        repo.count_by_migration = AsyncMock(return_value=2)  # type: ignore[method-assign]

        async def get_by_ordinal(mig_id: object, ordinal: int) -> PositionMapping | None:
            return first if ordinal == 0 else last

        repo._get_by_ordinal = get_by_ordinal  # type: ignore[method-assign]

        result = await repo.get_position_bounds(migration_id)

        assert result == (pos(100, "src"), pos(10000, "src"))


class TestPostgreSQLPositionMappingRepositoryDeleteByMigration:
    """Tests for PostgreSQLPositionMappingRepository.delete_by_migration."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_delete_returns_zero_when_no_mappings(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test delete_by_migration returns 0 when no mappings to delete."""
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 0
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.delete_by_migration(uuid4())

            assert result == 0

    @pytest.mark.asyncio
    async def test_delete_returns_count_of_deleted(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test delete_by_migration returns count of deleted mappings."""
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 500
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.delete_by_migration(uuid4())

            assert result == 500
