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
        repo = PostgreSQLMigrationAuditLogRepository(MagicMock())
        assert isinstance(repo, MigrationAuditLogRepository)

    def test_protocol_has_required_methods(self) -> None:
        for method in (
            "record",
            "get_by_migration",
            "get_by_id",
            "get_latest",
            "count_by_migration",
        ):
            assert hasattr(MigrationAuditLogRepository, method)


class TestPostgreSQLMigrationAuditLogRepositoryInit:
    """Tests for PostgreSQLMigrationAuditLogRepository initialization."""

    def test_init_with_connection(self) -> None:
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationAuditLogRepository(mock_conn)
        assert repo._conn == mock_conn

    def test_init_with_tracing_enabled(self) -> None:
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=True)
        assert repo._conn == mock_conn

    def test_init_with_tracing_disabled(self) -> None:
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)
        assert repo._conn == mock_conn


class TestPostgreSQLMigrationAuditLogRepositoryRecord:
    """Tests for PostgreSQLMigrationAuditLogRepository.record method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationAuditLogRepository:
        return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)

    @pytest.fixture
    def sample_entry(self) -> MigrationAuditEntry:
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
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
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
            assert mock_conn.execute.call_args[0][1]["details"] is None

    @pytest.mark.asyncio
    async def test_record_with_null_phases(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
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
            params = mock_conn.execute.call_args[0][1]
            assert params["old_phase"] is None and params["new_phase"] is None


class TestPostgreSQLMigrationAuditLogRepositoryGetById:
    """Tests for PostgreSQLMigrationAuditLogRepository.get_by_id method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationAuditLogRepository:
        return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_by_id_returns_none_when_not_found(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            assert await repo.get_by_id(999) is None

    @pytest.mark.asyncio
    async def test_get_by_id_returns_entry_when_found(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        migration_id, now = uuid4(), datetime.now(UTC)
        row = (
            42,
            migration_id,
            "phase_changed",
            "bulk_copy",
            "dual_write",
            {"reason": "test"},
            "admin@example.com",
            now,
        )
        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_by_id(42)
            assert result is not None and result.id == 42
            assert result.migration_id == migration_id
            assert result.event_type == AuditEventType.PHASE_CHANGED
            assert result.old_phase == MigrationPhase.BULK_COPY
            assert result.new_phase == MigrationPhase.DUAL_WRITE
            assert result.details == {"reason": "test"}
