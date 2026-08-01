"""
Unit tests for PositionMappingRepository implementations.

Tests cover:
- PositionMappingRepository protocol compliance
- PostgreSQLPositionMappingRepository CRUD operations
- Batch insert operations
- Position lookup operations (exact and nearest), keyed by opaque tokens
- Range queries
- Count and bounds operations
- Delete operations
- Row to model conversion
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.domain.exceptions import PositionDecodeError
from eventsource.migration.models import PositionMapping
from eventsource.migration.repositories.position_mapping import (
    PositionMappingRepository,
    PostgreSQLPositionMappingRepository,
)
from eventsource.ports.positions import Position


def pos(n: int, store: str = "src") -> Position:
    """Build a Position for a given store, keyed by a single int."""
    return Position(store_id=store, key=(n,))


class TestPositionMappingRepositoryProtocol:
    """Tests for PositionMappingRepository protocol."""

    def test_postgresql_repository_implements_protocol(self) -> None:
        """Test PostgreSQLPositionMappingRepository implements the protocol."""
        mock_conn = MagicMock()
        repo = PostgreSQLPositionMappingRepository(mock_conn)

        # The protocol is runtime checkable
        assert isinstance(repo, PositionMappingRepository)

    def test_protocol_has_required_methods(self) -> None:
        """Test protocol defines all required methods."""
        # Verify protocol methods exist
        assert hasattr(PositionMappingRepository, "create")
        assert hasattr(PositionMappingRepository, "create_batch")
        assert hasattr(PositionMappingRepository, "get")
        assert hasattr(PositionMappingRepository, "find_by_source_position")
        assert hasattr(PositionMappingRepository, "find_by_target_position")
        assert hasattr(PositionMappingRepository, "find_nearest_source_position")
        assert hasattr(PositionMappingRepository, "find_by_event_id")
        assert hasattr(PositionMappingRepository, "list_by_migration")
        assert hasattr(PositionMappingRepository, "list_in_source_range")
        assert hasattr(PositionMappingRepository, "count_by_migration")
        assert hasattr(PositionMappingRepository, "get_position_bounds")
        assert hasattr(PositionMappingRepository, "delete_by_migration")


class TestPostgreSQLPositionMappingRepositoryInit:
    """Tests for PostgreSQLPositionMappingRepository initialization."""

    def test_init_with_connection(self) -> None:
        """Test initialization with a connection."""
        mock_conn = MagicMock()
        repo = PostgreSQLPositionMappingRepository(mock_conn)
        assert repo._conn == mock_conn

    def test_init_with_tracing_enabled(self) -> None:
        """Test initialization with tracing enabled."""
        mock_conn = MagicMock()
        repo = PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=True)
        assert repo._conn == mock_conn

    def test_init_with_tracing_disabled(self) -> None:
        """Test initialization with tracing disabled."""
        mock_conn = MagicMock()
        repo = PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)
        assert repo._conn == mock_conn


class TestPostgreSQLPositionMappingRepositoryCreate:
    """Tests for PostgreSQLPositionMappingRepository.create method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.fixture
    def sample_mapping(self) -> PositionMapping:
        """Create a sample position mapping."""
        return PositionMapping(
            migration_id=uuid4(),
            source_position=pos(1000, "src"),
            target_position=pos(500, "tgt"),
            event_id=uuid4(),
            mapped_at=datetime.now(UTC),
        )

    @pytest.mark.asyncio
    async def test_create_returns_id(
        self,
        repo: PostgreSQLPositionMappingRepository,
        sample_mapping: PositionMapping,
    ) -> None:
        """Test create returns the database ID."""
        with patch(
            "eventsource.migration.repositories.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (42,)  # Database ID
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.create(sample_mapping)

            assert result == 42

    @pytest.mark.asyncio
    async def test_create_executes_insert_with_tokens(
        self,
        repo: PostgreSQLPositionMappingRepository,
        sample_mapping: PositionMapping,
    ) -> None:
        """Test create executes INSERT query with token params, not legacy int columns."""
        with patch(
            "eventsource.migration.repositories.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (1,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.create(sample_mapping)

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["migration_id"] == sample_mapping.migration_id
            assert params["source_position_token"] == sample_mapping.source_position.to_str()
            assert params["target_position_token"] == sample_mapping.target_position.to_str()
            assert params["event_id"] == sample_mapping.event_id
            assert "source_position" not in params
            assert "target_position" not in params

            query_text = str(call_args[0][0])
            assert "source_position_token" in query_text
            assert "target_position_token" in query_text


class TestPostgreSQLPositionMappingRepositoryCreateBatch:
    """Tests for PostgreSQLPositionMappingRepository.create_batch method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_create_batch_with_empty_list(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test create_batch returns 0 for empty list."""
        result = await repo.create_batch([])
        assert result == 0

    @pytest.mark.asyncio
    async def test_create_batch_returns_count(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test create_batch returns number of created mappings."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        mappings = [
            PositionMapping(
                migration_id=migration_id,
                source_position=pos(i * 100, "src"),
                target_position=pos(i * 50, "tgt"),
                event_id=uuid4(),
                mapped_at=now,
            )
            for i in range(5)
        ]

        with patch(
            "eventsource.migration.repositories.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 5
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.create_batch(mappings)

            assert result == 5

    @pytest.mark.asyncio
    async def test_create_batch_uses_multi_row_insert_with_tokens(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test create_batch builds multi-row INSERT query keyed by tokens."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        mappings = [
            PositionMapping(
                migration_id=migration_id,
                source_position=pos(i * 100, "src"),
                target_position=pos(i * 50, "tgt"),
                event_id=uuid4(),
                mapped_at=now,
            )
            for i in range(3)
        ]

        with patch(
            "eventsource.migration.repositories.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 3
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.create_batch(mappings)

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]

            # Should have token params for each mapping
            assert "migration_id_0" in params
            assert "migration_id_1" in params
            assert "migration_id_2" in params
            assert params["source_position_token_0"] == mappings[0].source_position.to_str()
            assert params["source_position_token_1"] == mappings[1].source_position.to_str()
            assert params["source_position_token_2"] == mappings[2].source_position.to_str()


class TestPostgreSQLPositionMappingRepositoryGet:
    """Tests for PostgreSQLPositionMappingRepository.get method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_returns_none_when_not_found(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test get returns None when mapping not found."""
        with patch(
            "eventsource.migration.repositories.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get(999)

            assert result is None

    @pytest.mark.asyncio
    async def test_get_returns_mapping_when_found(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test get returns mapping when found."""
        migration_id = uuid4()
        event_id = uuid4()
        now = datetime.now(UTC)
        source_position = pos(1000, "src")
        target_position = pos(500, "tgt")

        row = (
            1,  # id
            migration_id,  # migration_id
            source_position.to_str(),  # source_position_token
            target_position.to_str(),  # target_position_token
            event_id,  # event_id
            now,  # mapped_at
        )

        with patch(
            "eventsource.migration.repositories.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get(1)

            assert result is not None
            assert result.migration_id == migration_id
            assert result.source_position == source_position
            assert result.target_position == target_position
            assert result.event_id == event_id


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
            "eventsource.migration.repositories.position_mapping.sql_connection"
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
            "eventsource.migration.repositories.position_mapping.sql_connection"
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
            "eventsource.migration.repositories.position_mapping.sql_connection"
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
            "eventsource.migration.repositories.position_mapping.sql_connection"
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
            "eventsource.migration.repositories.position_mapping.sql_connection"
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
            "eventsource.migration.repositories.position_mapping.sql_connection"
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
            "eventsource.migration.repositories.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_by_event_id(migration_id, event_id)

            assert result is not None
            assert result.event_id == event_id


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
            "eventsource.migration.repositories.position_mapping.sql_connection"
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
            "eventsource.migration.repositories.position_mapping.sql_connection"
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
            "eventsource.migration.repositories.position_mapping.sql_connection"
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
            "eventsource.migration.repositories.position_mapping.sql_connection"
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
            "eventsource.migration.repositories.position_mapping.sql_connection"
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
            "eventsource.migration.repositories.position_mapping.sql_connection"
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
            "eventsource.migration.repositories.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 500
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.delete_by_migration(uuid4())

            assert result == 500


class TestPostgreSQLPositionMappingRepositoryHelpers:
    """Tests for PostgreSQLPositionMappingRepository helper methods."""

    @pytest.fixture
    def repo(self) -> PostgreSQLPositionMappingRepository:
        """Create a repository for testing."""
        mock_conn = MagicMock()
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    def test_row_to_mapping_with_all_fields(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test converting a row with all fields populated."""
        migration_id = uuid4()
        event_id = uuid4()
        mapped_at = datetime.now(UTC)
        source_position = pos(1000, "src")
        target_position = pos(500, "tgt")

        row = (
            1,  # id (not in model)
            migration_id,
            source_position.to_str(),
            target_position.to_str(),
            event_id,
            mapped_at,
        )

        mapping = repo._row_to_mapping(row)

        assert mapping.migration_id == migration_id
        assert mapping.source_position == source_position
        assert mapping.target_position == target_position
        assert mapping.event_id == event_id
        assert mapping.mapped_at == mapped_at

    def test_row_to_mapping_preserves_types(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test row conversion preserves correct types."""
        migration_id = uuid4()
        event_id = uuid4()
        mapped_at = datetime.now(UTC)

        row = (
            1,
            migration_id,
            pos(999999999, "src").to_str(),
            pos(888888888, "tgt").to_str(),
            event_id,
            mapped_at,
        )

        mapping = repo._row_to_mapping(row)

        from uuid import UUID

        assert isinstance(mapping.migration_id, UUID)
        assert isinstance(mapping.source_position, Position)
        assert isinstance(mapping.target_position, Position)
        assert isinstance(mapping.event_id, UUID)
        assert isinstance(mapping.mapped_at, datetime)

    def test_row_to_mapping_raises_on_missing_source_token(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test a row with no source token (legacy int-only row) raises PositionDecodeError."""
        row = (1, uuid4(), None, pos(500, "tgt").to_str(), uuid4(), datetime.now(UTC))

        with pytest.raises(PositionDecodeError):
            repo._row_to_mapping(row)

    def test_row_to_mapping_raises_on_missing_target_token(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test a row with no target token (legacy int-only row) raises PositionDecodeError."""
        row = (1, uuid4(), pos(1000, "src").to_str(), None, uuid4(), datetime.now(UTC))

        with pytest.raises(PositionDecodeError):
            repo._row_to_mapping(row)


class TestPositionMappingWorkflow:
    """Integration-style tests for position mapping workflows."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_bulk_copy_workflow(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test typical bulk copy workflow."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        # Create batch of mappings
        mappings = [
            PositionMapping(
                migration_id=migration_id,
                source_position=pos(i * 100, "src"),
                target_position=pos(i * 50, "tgt"),
                event_id=uuid4(),
                mapped_at=now,
            )
            for i in range(100)
        ]

        with patch(
            "eventsource.migration.repositories.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 100
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # Batch insert
            count = await repo.create_batch(mappings)

            assert count == 100
            mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_checkpoint_translation_workflow(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test checkpoint translation using nearest position lookup."""
        migration_id = uuid4()
        mapping = PositionMapping(
            migration_id=migration_id,
            source_position=pos(900, "src"),
            target_position=pos(450, "tgt"),
            event_id=uuid4(),
            mapped_at=datetime.now(UTC),
        )

        repo.count_by_migration = AsyncMock(return_value=1)  # type: ignore[method-assign]

        async def get_by_ordinal(mig_id: object, ordinal: int) -> PositionMapping | None:
            return mapping

        repo._get_by_ordinal = get_by_ordinal  # type: ignore[method-assign]

        # Subscriber has checkpoint at source position 950
        # Need to find nearest target position
        result = await repo.find_nearest_source_position(migration_id, pos(950))

        assert result is not None
        assert result.source_position == pos(900, "src")
        assert result.target_position == pos(450, "tgt")

        # Subscriber would continue from target position 450

    @pytest.mark.asyncio
    async def test_migration_cleanup_workflow(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test migration cleanup deletes all mappings."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 10000
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            deleted = await repo.delete_by_migration(migration_id)

            assert deleted == 10000
            # Verify DELETE query was executed
            call_args = mock_conn.execute.call_args
            query_text = str(call_args[0][0])
            assert "DELETE FROM migration_position_mappings" in query_text
