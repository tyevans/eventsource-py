"""
Unit tests for PositionMappingRepository implementations.

Tests cover:
- PositionMappingRepository protocol compliance
- PostgreSQLPositionMappingRepository CRUD operations
- Batch insert operations
- Position lookup operations (exact and nearest)
- Range queries
- Count and bounds operations
- Delete operations
- Row to model conversion
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.migration.models import PositionMapping
from eventsource.migration.repositories.position_mapping import (
    PositionMappingRepository,
    PostgreSQLPositionMappingRepository,
)


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
            source_position=1000,
            target_position=500,
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
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (42,)  # Database ID
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.create(sample_mapping)

            assert result == 42

    @pytest.mark.asyncio
    async def test_create_executes_insert(
        self,
        repo: PostgreSQLPositionMappingRepository,
        sample_mapping: PositionMapping,
    ) -> None:
        """Test create executes INSERT query with correct params."""
        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
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
            assert params["source_position"] == sample_mapping.source_position
            assert params["target_position"] == sample_mapping.target_position
            assert params["event_id"] == sample_mapping.event_id


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
                source_position=i * 100,
                target_position=i * 50,
                event_id=uuid4(),
                mapped_at=now,
            )
            for i in range(5)
        ]

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 5
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.create_batch(mappings)

            assert result == 5

    @pytest.mark.asyncio
    async def test_create_batch_uses_multi_row_insert(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test create_batch builds multi-row INSERT query."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        mappings = [
            PositionMapping(
                migration_id=migration_id,
                source_position=i * 100,
                target_position=i * 50,
                event_id=uuid4(),
                mapped_at=now,
            )
            for i in range(3)
        ]

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
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

            # Should have params for each mapping
            assert "migration_id_0" in params
            assert "migration_id_1" in params
            assert "migration_id_2" in params
            assert "source_position_0" in params
            assert "source_position_1" in params
            assert "source_position_2" in params


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
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
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

        row = (
            1,  # id
            migration_id,  # migration_id
            1000,  # source_position
            500,  # target_position
            event_id,  # event_id
            now,  # mapped_at
        )

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get(1)

            assert result is not None
            assert result.migration_id == migration_id
            assert result.source_position == 1000
            assert result.target_position == 500
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
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_by_source_position(uuid4(), 1000)

            assert result is None

    @pytest.mark.asyncio
    async def test_find_by_source_position_returns_mapping(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_by_source_position returns matching mapping."""
        migration_id = uuid4()
        event_id = uuid4()
        now = datetime.now(UTC)

        row = (1, migration_id, 1000, 500, event_id, now)

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_by_source_position(migration_id, 1000)

            assert result is not None
            assert result.source_position == 1000
            assert result.target_position == 500


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
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_by_target_position(uuid4(), 500)

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

        row = (1, migration_id, 1000, 500, event_id, now)

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_by_target_position(migration_id, 500)

            assert result is not None
            assert result.target_position == 500


class TestPostgreSQLPositionMappingRepositoryFindNearestSourcePosition:
    """Tests for PostgreSQLPositionMappingRepository.find_nearest_source_position."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_find_nearest_returns_none_when_no_mappings(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_nearest_source_position returns None when no mappings exist."""
        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_nearest_source_position(uuid4(), 1000)

            assert result is None

    @pytest.mark.asyncio
    async def test_find_nearest_returns_exact_match(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_nearest_source_position returns exact match when available."""
        migration_id = uuid4()
        event_id = uuid4()
        now = datetime.now(UTC)

        row = (1, migration_id, 1000, 500, event_id, now)

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.find_nearest_source_position(migration_id, 1000)

            assert result is not None
            assert result.source_position == 1000

    @pytest.mark.asyncio
    async def test_find_nearest_returns_lower_position(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_nearest_source_position returns nearest lower position."""
        migration_id = uuid4()
        event_id = uuid4()
        now = datetime.now(UTC)

        # Source position 900 is the nearest to 950
        row = (1, migration_id, 900, 450, event_id, now)

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # Query for position 950, should get 900
            result = await repo.find_nearest_source_position(migration_id, 950)

            assert result is not None
            assert result.source_position == 900

    @pytest.mark.asyncio
    async def test_find_nearest_query_uses_correct_order(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test find_nearest_source_position uses DESC ordering."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.find_nearest_source_position(migration_id, 1000)

            # Verify query uses DESC ordering
            call_args = mock_conn.execute.call_args
            query_text = str(call_args[0][0])
            assert "ORDER BY source_position DESC" in query_text
            assert "LIMIT 1" in query_text


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
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
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

        row = (1, migration_id, 1000, 500, event_id, now)

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
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
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.list_by_migration(uuid4())

            assert result == []

    @pytest.mark.asyncio
    async def test_list_by_migration_returns_mappings(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test list_by_migration returns mappings ordered by source_position."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        rows = [
            (1, migration_id, 100, 50, uuid4(), now),
            (2, migration_id, 200, 100, uuid4(), now),
            (3, migration_id, 300, 150, uuid4(), now),
        ]

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = rows
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.list_by_migration(migration_id)

            assert len(result) == 3
            assert result[0].source_position == 100
            assert result[1].source_position == 200
            assert result[2].source_position == 300

    @pytest.mark.asyncio
    async def test_list_by_migration_uses_pagination(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test list_by_migration uses limit and offset."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
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
    async def test_list_in_source_range_returns_empty_list(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test list_in_source_range returns empty list when no matches."""
        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.list_in_source_range(uuid4(), 0, 100)

            assert result == []

    @pytest.mark.asyncio
    async def test_list_in_source_range_returns_matching_mappings(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test list_in_source_range returns mappings within range."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        rows = [
            (1, migration_id, 100, 50, uuid4(), now),
            (2, migration_id, 150, 75, uuid4(), now),
            (3, migration_id, 200, 100, uuid4(), now),
        ]

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = rows
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.list_in_source_range(migration_id, 100, 200)

            assert len(result) == 3
            # All positions should be within range
            for mapping in result:
                assert 100 <= mapping.source_position <= 200

    @pytest.mark.asyncio
    async def test_list_in_source_range_uses_correct_params(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test list_in_source_range uses correct query params."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.list_in_source_range(migration_id, 500, 1000)

            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["migration_id"] == migration_id
            assert params["start_position"] == 500
            assert params["end_position"] == 1000


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
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
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
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
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
        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (None, None)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_position_bounds(uuid4())

            assert result is None

    @pytest.mark.asyncio
    async def test_get_bounds_returns_min_max(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test get_position_bounds returns (min, max) tuple."""
        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (100, 10000)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_position_bounds(uuid4())

            assert result == (100, 10000)


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
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
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
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
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

        row = (
            1,  # id (not in model)
            migration_id,
            1000,  # source_position
            500,  # target_position
            event_id,
            mapped_at,
        )

        mapping = repo._row_to_mapping(row)

        assert mapping.migration_id == migration_id
        assert mapping.source_position == 1000
        assert mapping.target_position == 500
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

        row = (1, migration_id, 999999999, 888888888, event_id, mapped_at)

        mapping = repo._row_to_mapping(row)

        # Verify types
        from uuid import UUID

        assert isinstance(mapping.migration_id, UUID)
        assert isinstance(mapping.source_position, int)
        assert isinstance(mapping.target_position, int)
        assert isinstance(mapping.event_id, UUID)
        assert isinstance(mapping.mapped_at, datetime)


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
                source_position=i * 100,
                target_position=i * 50,
                event_id=uuid4(),
                mapped_at=now,
            )
            for i in range(100)
        ]

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
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
        event_id = uuid4()
        now = datetime.now(UTC)

        # Existing mapping at position 900
        row = (1, migration_id, 900, 450, event_id, now)

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # Subscriber has checkpoint at source position 950
            # Need to find nearest target position
            mapping = await repo.find_nearest_source_position(migration_id, 950)

            assert mapping is not None
            assert mapping.source_position == 900
            assert mapping.target_position == 450

            # Subscriber would continue from target position 450

    @pytest.mark.asyncio
    async def test_migration_cleanup_workflow(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test migration cleanup deletes all mappings."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.position_mapping.execute_with_connection"
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
