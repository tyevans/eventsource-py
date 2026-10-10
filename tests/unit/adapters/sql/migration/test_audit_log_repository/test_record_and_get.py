"""Tests for recording and retrieving audit log entries by ID."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.audit_log import (
    PostgreSQLMigrationAuditLogRepository,
)
from eventsource.ports.migration.models import (
    AuditEventType,
    MigrationAuditEntry,
    MigrationPhase,
)
from eventsource.ports.migration.repositories import MigrationAuditLogRepository


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

        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
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

        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
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

        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
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
        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
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

        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
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
