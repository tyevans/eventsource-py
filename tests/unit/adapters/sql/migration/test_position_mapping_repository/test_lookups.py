"""Unit tests for PositionMappingRepository lookup operations."""

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


class TestPostgreSQLPositionMappingRepositoryFindBySourcePosition:
    """Tests for PostgreSQLPositionMappingRepository.find_by_source_position."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_find_by_source_position_returns_none_when_not_found(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_by_source_position returns None when not found."""
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_by_source_position(uuid4(), pos(1000))

            assert result is None

    @pytest.mark.asyncio
    async def test_find_by_source_position_returns_mapping_on_canonical_token(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_by_source_position matches on the canonical token."""
        migration_id = uuid4()
        event_id = uuid4()
        now = datetime.now(UTC)
        source_position = pos(1000, "src")
        target_position = pos(500, "tgt")

        row = (1, migration_id, source_position.to_str(), target_position.to_str(), event_id, now)

        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_by_source_position(migration_id, source_position)

            assert result is not None
            assert result.source_position == source_position
            assert result.target_position == target_position

            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["source_position_token"] == source_position.to_str()

    @pytest.mark.asyncio
    async def test_find_by_source_position_misses_on_token_from_different_store(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test a token from a different store does not match (simulated via query params)."""
        migration_id = uuid4()

        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            # The DB has no row matching the other-store token -- exact equality
            # on the canonical token string is what enforces the store boundary.
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            other_store_position = pos(1000, "other-store")
            result = await repo.find_by_source_position(migration_id, other_store_position)

            assert result is None
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["source_position_token"] == other_store_position.to_str()


class TestPostgreSQLPositionMappingRepositoryFindByTargetPosition:
    """Tests for PostgreSQLPositionMappingRepository.find_by_target_position."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_find_by_target_position_returns_none_when_not_found(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_by_target_position returns None when not found."""
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_by_target_position(uuid4(), pos(500, "tgt"))

            assert result is None

    @pytest.mark.asyncio
    async def test_find_by_target_position_returns_mapping(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_by_target_position returns matching mapping."""
        migration_id = uuid4()
        event_id = uuid4()
        now = datetime.now(UTC)
        source_position = pos(1000, "src")
        target_position = pos(500, "tgt")

        row = (1, migration_id, source_position.to_str(), target_position.to_str(), event_id, now)

        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_by_target_position(migration_id, target_position)

            assert result is not None
            assert result.target_position == target_position


class TestPostgreSQLPositionMappingRepositoryFindNearestSourcePosition:
    """Tests for PostgreSQLPositionMappingRepository.find_nearest_source_position.

    The nearest-match lookup is a binary search over the row ordinal
    (`ORDER BY id`), fetching one row per step via `_get_by_ordinal`. These
    tests mock `count_by_migration` and `_get_by_ordinal` directly rather
    than the raw SQL, since the binary search issues multiple queries.
    """

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    def _mapping_at(
        self,
        migration_id: object,
        source_n: int,
        target_n: int,
    ) -> PositionMapping:
        return PositionMapping(
            migration_id=migration_id,  # type: ignore[arg-type]
            source_position=pos(source_n, "src"),
            target_position=pos(target_n, "tgt"),
            event_id=uuid4(),
            mapped_at=datetime.now(UTC),
        )

    @pytest.mark.asyncio
    async def test_find_nearest_returns_none_when_no_mappings(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_nearest_source_position returns None when no mappings exist."""
        migration_id = uuid4()
        repo.count_by_migration = AsyncMock(return_value=0)  # type: ignore[method-assign]

        result = await repo.find_nearest_source_position(migration_id, pos(1000))

        assert result is None

    @pytest.mark.asyncio
    async def test_find_nearest_returns_exact_match(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_nearest_source_position returns exact match when available."""
        migration_id = uuid4()
        rows = [self._mapping_at(migration_id, 900, 450), self._mapping_at(migration_id, 1000, 500)]

        repo.count_by_migration = AsyncMock(return_value=len(rows))  # type: ignore[method-assign]

        async def get_by_ordinal(mig_id: object, ordinal: int) -> PositionMapping | None:
            return rows[ordinal]

        repo._get_by_ordinal = get_by_ordinal  # type: ignore[method-assign]

        result = await repo.find_nearest_source_position(migration_id, pos(1000))

        assert result is not None
        assert result.source_position == pos(1000, "src")

    @pytest.mark.asyncio
    async def test_find_nearest_returns_lower_position(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_nearest_source_position returns the greatest mapping <= the query."""
        migration_id = uuid4()
        rows = [
            self._mapping_at(migration_id, 100, 50),
            self._mapping_at(migration_id, 900, 450),
            self._mapping_at(migration_id, 1000, 500),
        ]

        repo.count_by_migration = AsyncMock(return_value=len(rows))  # type: ignore[method-assign]

        async def get_by_ordinal(mig_id: object, ordinal: int) -> PositionMapping | None:
            return rows[ordinal]

        repo._get_by_ordinal = get_by_ordinal  # type: ignore[method-assign]

        # Query for position 950 (< 1000, > 900): nearest is 900.
        result = await repo.find_nearest_source_position(migration_id, pos(950))

        assert result is not None
        assert result.source_position == pos(900, "src")

    @pytest.mark.asyncio
    async def test_find_nearest_returns_none_when_all_greater(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test nearest lookup returns None when every mapping is greater than the query."""
        migration_id = uuid4()
        rows = [
            self._mapping_at(migration_id, 500, 250),
            self._mapping_at(migration_id, 900, 450),
        ]

        repo.count_by_migration = AsyncMock(return_value=len(rows))  # type: ignore[method-assign]

        async def get_by_ordinal(mig_id: object, ordinal: int) -> PositionMapping | None:
            return rows[ordinal]

        repo._get_by_ordinal = get_by_ordinal  # type: ignore[method-assign]

        result = await repo.find_nearest_source_position(migration_id, pos(100))

        assert result is None

    @pytest.mark.asyncio
    async def test_find_nearest_single_row_table_matches(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test the binary search's degenerate single-row case: query >= the row."""
        migration_id = uuid4()
        rows = [self._mapping_at(migration_id, 500, 250)]

        repo.count_by_migration = AsyncMock(return_value=1)  # type: ignore[method-assign]

        async def get_by_ordinal(mig_id: object, ordinal: int) -> PositionMapping | None:
            return rows[ordinal]

        repo._get_by_ordinal = get_by_ordinal  # type: ignore[method-assign]

        result = await repo.find_nearest_source_position(migration_id, pos(600))

        assert result is not None
        assert result.source_position == pos(500, "src")

    @pytest.mark.asyncio
    async def test_find_nearest_single_row_table_none(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test the binary search's degenerate single-row case: query < the row."""
        migration_id = uuid4()
        rows = [self._mapping_at(migration_id, 500, 250)]

        repo.count_by_migration = AsyncMock(return_value=1)  # type: ignore[method-assign]

        async def get_by_ordinal(mig_id: object, ordinal: int) -> PositionMapping | None:
            return rows[ordinal]

        repo._get_by_ordinal = get_by_ordinal  # type: ignore[method-assign]

        result = await repo.find_nearest_source_position(migration_id, pos(100))

        assert result is None


class TestPostgreSQLPositionMappingRepositoryFindByEventId:
    """Tests for PostgreSQLPositionMappingRepository.find_by_event_id."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_find_by_event_id_returns_none_when_not_found(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_by_event_id returns None when not found."""
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_by_event_id(uuid4(), uuid4())

            assert result is None

    @pytest.mark.asyncio
    async def test_find_by_event_id_returns_mapping(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_by_event_id returns matching mapping."""
        migration_id = uuid4()
        event_id = uuid4()
        now = datetime.now(UTC)

        row = (1, migration_id, pos(1000, "src").to_str(), pos(500, "tgt").to_str(), event_id, now)

        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_by_event_id(migration_id, event_id)

            assert result is not None
            assert result.event_id == event_id
