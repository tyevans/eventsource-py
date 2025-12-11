"""
Unit tests for MigrationAuditLogRepository implementations.

Tests cover:
- MigrationAuditLogRepository protocol compliance
- PostgreSQLMigrationAuditLogRepository CRUD operations
- Recording audit entries
- Querying by migration ID with filters
- Getting latest entry
- Counting entries
- Row to entry conversion
"""

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.migration.models import (
    AuditEventType,
    MigrationAuditEntry,
    MigrationPhase,
)
from eventsource.migration.repositories.audit_log import (
    MigrationAuditLogRepository,
    PostgreSQLMigrationAuditLogRepository,
)


class TestMigrationAuditLogRepositoryProtocol:
    """Tests for MigrationAuditLogRepository protocol."""

    def test_postgresql_repository_implements_protocol(self) -> None:
        """Test PostgreSQLMigrationAuditLogRepository implements the protocol."""
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationAuditLogRepository(mock_conn)

        # The protocol is runtime checkable
        assert isinstance(repo, MigrationAuditLogRepository)

    def test_protocol_has_required_methods(self) -> None:
        """Test protocol defines all required methods."""
        assert hasattr(MigrationAuditLogRepository, "record")
        assert hasattr(MigrationAuditLogRepository, "get_by_migration")
        assert hasattr(MigrationAuditLogRepository, "get_by_id")
        assert hasattr(MigrationAuditLogRepository, "get_latest")
        assert hasattr(MigrationAuditLogRepository, "count_by_migration")


class TestPostgreSQLMigrationAuditLogRepositoryInit:
    """Tests for PostgreSQLMigrationAuditLogRepository initialization."""

    def test_init_with_connection(self) -> None:
        """Test initialization with a connection."""
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationAuditLogRepository(mock_conn)
        assert repo._conn == mock_conn

    def test_init_with_tracing_enabled(self) -> None:
        """Test initialization with tracing enabled."""
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=True)
        assert repo._conn == mock_conn

    def test_init_with_tracing_disabled(self) -> None:
        """Test initialization with tracing disabled."""
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)
        assert repo._conn == mock_conn


class TestPostgreSQLMigrationAuditLogRepositoryRecord:
    """Tests for PostgreSQLMigrationAuditLogRepository.record method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationAuditLogRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)

    @pytest.fixture
    def sample_entry(self) -> MigrationAuditEntry:
        """Create a sample audit entry for testing."""
        return MigrationAuditEntry.phase_change(
            migration_id=uuid4(),
            old_phase=MigrationPhase.BULK_COPY,
            new_phase=MigrationPhase.DUAL_WRITE,
            occurred_at=datetime.now(UTC),
            operator="admin@example.com",
            details={"reason": "Bulk copy complete"},
        )

    @pytest.mark.asyncio
    async def test_record_success(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
        sample_entry: MigrationAuditEntry,
    ) -> None:
        """Test successful audit entry recording."""
        expected_id = 42

        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (expected_id,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.record(sample_entry)

            assert result == expected_id
            mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_record_with_null_details(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test recording entry with null details."""
        entry = MigrationAuditEntry(
            id=None,
            migration_id=uuid4(),
            event_type=AuditEventType.MIGRATION_STARTED,
            old_phase=None,
            new_phase=MigrationPhase.PENDING,
            details=None,
            operator="system",
            occurred_at=datetime.now(UTC),
        )

        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (1,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.record(entry)

            assert result == 1
            # Verify params include null details
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["details"] is None

    @pytest.mark.asyncio
    async def test_record_with_null_phases(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test recording entry with null phases (e.g., error event)."""
        entry = MigrationAuditEntry.error_occurred(
            migration_id=uuid4(),
            occurred_at=datetime.now(UTC),
            error_message="Connection timeout",
            error_type="NetworkError",
        )

        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (1,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.record(entry)

            assert result == 1
            # Verify params include null phases
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["old_phase"] is None
            assert params["new_phase"] is None


class TestPostgreSQLMigrationAuditLogRepositoryGetByMigration:
    """Tests for PostgreSQLMigrationAuditLogRepository.get_by_migration method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationAuditLogRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_by_migration_returns_empty_list(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test get_by_migration returns empty list when no entries."""
        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_by_migration(uuid4())

            assert result == []

    @pytest.mark.asyncio
    async def test_get_by_migration_returns_entries(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test get_by_migration returns audit entries."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        rows = [
            (
                1,  # id
                migration_id,  # migration_id
                "migration_started",  # event_type
                None,  # old_phase
                "pending",  # new_phase
                json.dumps({"config": {}}),  # details
                "admin@example.com",  # operator
                now,  # occurred_at
            ),
            (
                2,  # id
                migration_id,  # migration_id
                "phase_changed",  # event_type
                "pending",  # old_phase
                "bulk_copy",  # new_phase
                None,  # details
                "system",  # operator
                now + timedelta(minutes=1),  # occurred_at
            ),
        ]

        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = rows
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_by_migration(migration_id)

            assert len(result) == 2
            assert result[0].id == 1
            assert result[0].event_type == AuditEventType.MIGRATION_STARTED
            assert result[0].new_phase == MigrationPhase.PENDING
            assert result[1].id == 2
            assert result[1].event_type == AuditEventType.PHASE_CHANGED
            assert result[1].old_phase == MigrationPhase.PENDING
            assert result[1].new_phase == MigrationPhase.BULK_COPY

    @pytest.mark.asyncio
    async def test_get_by_migration_with_event_type_filter(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test get_by_migration with event type filter."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.get_by_migration(
                migration_id,
                event_types=[AuditEventType.PHASE_CHANGED, AuditEventType.ERROR_OCCURRED],
            )

            # Verify filter was applied
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert "event_types" in params
            assert params["event_types"] == ["phase_changed", "error_occurred"]

    @pytest.mark.asyncio
    async def test_get_by_migration_with_time_filters(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test get_by_migration with time range filters."""
        migration_id = uuid4()
        since = datetime.now(UTC) - timedelta(hours=1)
        until = datetime.now(UTC)

        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.get_by_migration(
                migration_id,
                since=since,
                until=until,
            )

            # Verify filters were applied
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["since"] == since
            assert params["until"] == until

    @pytest.mark.asyncio
    async def test_get_by_migration_with_limit(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test get_by_migration with limit."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.get_by_migration(migration_id, limit=10)

            # Verify limit is in query
            call_args = mock_conn.execute.call_args
            query = str(call_args[0][0])
            assert "LIMIT 10" in query


class TestPostgreSQLMigrationAuditLogRepositoryGetById:
    """Tests for PostgreSQLMigrationAuditLogRepository.get_by_id method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationAuditLogRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_by_id_returns_none_when_not_found(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test get_by_id returns None when entry not found."""
        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_by_id(999)

            assert result is None

    @pytest.mark.asyncio
    async def test_get_by_id_returns_entry_when_found(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test get_by_id returns entry when found."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        row = (
            42,  # id
            migration_id,  # migration_id
            "phase_changed",  # event_type
            "bulk_copy",  # old_phase
            "dual_write",  # new_phase
            {"reason": "test"},  # details (dict)
            "admin@example.com",  # operator
            now,  # occurred_at
        )

        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_by_id(42)

            assert result is not None
            assert result.id == 42
            assert result.migration_id == migration_id
            assert result.event_type == AuditEventType.PHASE_CHANGED
            assert result.old_phase == MigrationPhase.BULK_COPY
            assert result.new_phase == MigrationPhase.DUAL_WRITE
            assert result.details == {"reason": "test"}


class TestPostgreSQLMigrationAuditLogRepositoryGetLatest:
    """Tests for PostgreSQLMigrationAuditLogRepository.get_latest method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationAuditLogRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_latest_returns_none_when_no_entries(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test get_latest returns None when no entries exist."""
        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_latest(uuid4())

            assert result is None

    @pytest.mark.asyncio
    async def test_get_latest_returns_most_recent(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test get_latest returns the most recent entry."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        row = (
            10,  # id
            migration_id,  # migration_id
            "migration_completed",  # event_type
            "cutover",  # old_phase
            "completed",  # new_phase
            None,  # details
            "system",  # operator
            now,  # occurred_at
        )

        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_latest(migration_id)

            assert result is not None
            assert result.id == 10
            assert result.event_type == AuditEventType.MIGRATION_COMPLETED

    @pytest.mark.asyncio
    async def test_get_latest_with_event_type_filter(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test get_latest with event type filter."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.get_latest(
                migration_id,
                event_type=AuditEventType.ERROR_OCCURRED,
            )

            # Verify filter was applied
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["event_type"] == "error_occurred"


class TestPostgreSQLMigrationAuditLogRepositoryCountByMigration:
    """Tests for PostgreSQLMigrationAuditLogRepository.count_by_migration method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationAuditLogRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_count_by_migration_returns_zero_when_empty(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test count_by_migration returns 0 when no entries."""
        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (0,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.count_by_migration(uuid4())

            assert result == 0

    @pytest.mark.asyncio
    async def test_count_by_migration_returns_count(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test count_by_migration returns correct count."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (15,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.count_by_migration(migration_id)

            assert result == 15

    @pytest.mark.asyncio
    async def test_count_by_migration_with_event_type_filter(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test count_by_migration with event type filter."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.audit_log.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (3,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.count_by_migration(
                migration_id,
                event_type=AuditEventType.ERROR_OCCURRED,
            )

            assert result == 3
            # Verify filter was applied
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["event_type"] == "error_occurred"


class TestPostgreSQLMigrationAuditLogRepositoryHelpers:
    """Tests for PostgreSQLMigrationAuditLogRepository helper methods."""

    @pytest.fixture
    def repo(self) -> PostgreSQLMigrationAuditLogRepository:
        """Create a repository for testing."""
        mock_conn = MagicMock()
        return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)

    def test_row_to_entry_with_all_fields(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test converting a complete row to entry."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        row = (
            1,  # id
            migration_id,  # migration_id
            "phase_changed",  # event_type
            "pending",  # old_phase
            "bulk_copy",  # new_phase
            json.dumps({"key": "value"}),  # details (JSON string)
            "admin@example.com",  # operator
            now,  # occurred_at
        )

        entry = repo._row_to_entry(row)

        assert entry.id == 1
        assert entry.migration_id == migration_id
        assert entry.event_type == AuditEventType.PHASE_CHANGED
        assert entry.old_phase == MigrationPhase.PENDING
        assert entry.new_phase == MigrationPhase.BULK_COPY
        assert entry.details == {"key": "value"}
        assert entry.operator == "admin@example.com"
        assert entry.occurred_at == now

    def test_row_to_entry_with_dict_details(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test converting row with details already as dict."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        row = (
            1,
            migration_id,
            "error_occurred",
            None,
            None,
            {"error": "test"},  # dict instead of JSON string
            "system",
            now,
        )

        entry = repo._row_to_entry(row)

        assert entry.details == {"error": "test"}

    def test_row_to_entry_with_null_phases(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test converting row with null phases."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        row = (
            1,
            migration_id,
            "error_occurred",
            None,  # old_phase
            None,  # new_phase
            None,  # details
            "system",
            now,
        )

        entry = repo._row_to_entry(row)

        assert entry.old_phase is None
        assert entry.new_phase is None
        assert entry.details is None

    def test_row_to_entry_with_all_event_types(
        self,
        repo: PostgreSQLMigrationAuditLogRepository,
    ) -> None:
        """Test converting rows with different event types."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        event_types = [
            "migration_started",
            "phase_changed",
            "migration_paused",
            "migration_resumed",
            "migration_aborted",
            "migration_completed",
            "migration_failed",
            "error_occurred",
            "cutover_initiated",
            "cutover_completed",
            "cutover_rolled_back",
            "verification_started",
            "verification_completed",
            "verification_failed",
            "progress_checkpoint",
        ]

        for event_type in event_types:
            row = (1, migration_id, event_type, None, None, None, "system", now)
            entry = repo._row_to_entry(row)
            assert entry.event_type.value == event_type


class TestMigrationAuditEntryFactoryMethods:
    """Tests for MigrationAuditEntry factory methods."""

    def test_phase_change_factory(self) -> None:
        """Test phase_change factory method."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        entry = MigrationAuditEntry.phase_change(
            migration_id=migration_id,
            old_phase=MigrationPhase.PENDING,
            new_phase=MigrationPhase.BULK_COPY,
            occurred_at=now,
            operator="admin",
            details={"reason": "Manual start"},
        )

        assert entry.migration_id == migration_id
        assert entry.event_type == AuditEventType.PHASE_CHANGED
        assert entry.old_phase == MigrationPhase.PENDING
        assert entry.new_phase == MigrationPhase.BULK_COPY
        assert entry.operator == "admin"
        assert entry.details == {"reason": "Manual start"}
        assert entry.id is None  # ID not set until persisted

    def test_migration_started_factory(self) -> None:
        """Test migration_started factory method."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        entry = MigrationAuditEntry.migration_started(
            migration_id=migration_id,
            occurred_at=now,
            operator="admin@example.com",
            details={"config": {"batch_size": 1000}},
        )

        assert entry.migration_id == migration_id
        assert entry.event_type == AuditEventType.MIGRATION_STARTED
        assert entry.old_phase is None
        assert entry.new_phase == MigrationPhase.PENDING
        assert entry.operator == "admin@example.com"
        assert entry.details == {"config": {"batch_size": 1000}}

    def test_error_occurred_factory(self) -> None:
        """Test error_occurred factory method."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        entry = MigrationAuditEntry.error_occurred(
            migration_id=migration_id,
            occurred_at=now,
            error_message="Connection timeout",
            error_type="NetworkError",
            operator="system",
        )

        assert entry.migration_id == migration_id
        assert entry.event_type == AuditEventType.ERROR_OCCURRED
        assert entry.old_phase is None
        assert entry.new_phase is None
        assert entry.details == {
            "error_message": "Connection timeout",
            "error_type": "NetworkError",
        }

    def test_to_dict(self) -> None:
        """Test to_dict serialization."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        entry = MigrationAuditEntry(
            id=42,
            migration_id=migration_id,
            event_type=AuditEventType.PHASE_CHANGED,
            old_phase=MigrationPhase.BULK_COPY,
            new_phase=MigrationPhase.DUAL_WRITE,
            details={"key": "value"},
            operator="admin",
            occurred_at=now,
        )

        data = entry.to_dict()

        assert data["id"] == 42
        assert data["migration_id"] == str(migration_id)
        assert data["event_type"] == "phase_changed"
        assert data["old_phase"] == "bulk_copy"
        assert data["new_phase"] == "dual_write"
        assert data["details"] == {"key": "value"}
        assert data["operator"] == "admin"
        assert data["occurred_at"] == now.isoformat()

    def test_to_dict_with_null_phases(self) -> None:
        """Test to_dict with null phases."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        entry = MigrationAuditEntry(
            id=1,
            migration_id=migration_id,
            event_type=AuditEventType.ERROR_OCCURRED,
            old_phase=None,
            new_phase=None,
            details=None,
            operator=None,
            occurred_at=now,
        )

        data = entry.to_dict()

        assert data["old_phase"] is None
        assert data["new_phase"] is None
        assert data["details"] is None
        assert data["operator"] is None


class TestAuditEventTypeEnum:
    """Tests for AuditEventType enum."""

    def test_all_event_types_match_db_constraint(self) -> None:
        """Test all enum values match the database CHECK constraint."""
        # These are the values from the migration_audit_log table CHECK constraint
        db_event_types = {
            "migration_started",
            "phase_changed",
            "migration_paused",
            "migration_resumed",
            "migration_aborted",
            "migration_completed",
            "migration_failed",
            "error_occurred",
            "cutover_initiated",
            "cutover_completed",
            "cutover_rolled_back",
            "verification_started",
            "verification_completed",
            "verification_failed",
            "progress_checkpoint",
        }

        enum_values = {et.value for et in AuditEventType}

        assert enum_values == db_event_types

    def test_event_type_values(self) -> None:
        """Test specific event type values."""
        assert AuditEventType.MIGRATION_STARTED.value == "migration_started"
        assert AuditEventType.PHASE_CHANGED.value == "phase_changed"
        assert AuditEventType.MIGRATION_PAUSED.value == "migration_paused"
        assert AuditEventType.MIGRATION_RESUMED.value == "migration_resumed"
        assert AuditEventType.MIGRATION_ABORTED.value == "migration_aborted"
        assert AuditEventType.MIGRATION_COMPLETED.value == "migration_completed"
        assert AuditEventType.MIGRATION_FAILED.value == "migration_failed"
        assert AuditEventType.ERROR_OCCURRED.value == "error_occurred"
        assert AuditEventType.CUTOVER_INITIATED.value == "cutover_initiated"
        assert AuditEventType.CUTOVER_COMPLETED.value == "cutover_completed"
        assert AuditEventType.CUTOVER_ROLLED_BACK.value == "cutover_rolled_back"
        assert AuditEventType.VERIFICATION_STARTED.value == "verification_started"
        assert AuditEventType.VERIFICATION_COMPLETED.value == "verification_completed"
        assert AuditEventType.VERIFICATION_FAILED.value == "verification_failed"
        assert AuditEventType.PROGRESS_CHECKPOINT.value == "progress_checkpoint"
