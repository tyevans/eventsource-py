"""Tests for querying audit log entries by migration and counting entries."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.serialization import json_dumps
from eventsource.adapters.sql.migration.audit_log import (
    PostgreSQLMigrationAuditLogRepository,
)
from eventsource.ports.migration.models import AuditEventType, MigrationPhase


class TestPostgreSQLMigrationAuditLogRepositoryGetByMigration:
    """Tests for PostgreSQLMigrationAuditLogRepository.get_by_migration method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationAuditLogRepository:
        return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_by_migration_returns_empty_list(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_by_migration(uuid4())
            assert result == []

    @pytest.mark.asyncio
    async def test_get_by_migration_returns_entries(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        migration_id = uuid4()
        now = datetime.now(UTC)
        rows = [
            (
                1,
                migration_id,
                "migration_started",
                None,
                "pending",
                json_dumps({"config": {}}),
                "admin@example.com",
                now,
            ),
            (
                2,
                migration_id,
                "phase_changed",
                "pending",
                "bulk_copy",
                None,
                "system",
                now + timedelta(minutes=1),
            ),
        ]

        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
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
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        migration_id = uuid4()
        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.get_by_migration(
                migration_id,
                event_types=[AuditEventType.PHASE_CHANGED, AuditEventType.ERROR_OCCURRED],
            )
            params = mock_conn.execute.call_args[0][1]
            assert "event_types" in params
            assert params["event_types"] == ["phase_changed", "error_occurred"]

    @pytest.mark.asyncio
    async def test_get_by_migration_with_time_filters(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        migration_id = uuid4()
        since = datetime.now(UTC) - timedelta(hours=1)
        until = datetime.now(UTC)

        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.get_by_migration(migration_id, since=since, until=until)
            params = mock_conn.execute.call_args[0][1]
            assert params["since"] == since
            assert params["until"] == until

    @pytest.mark.asyncio
    async def test_get_by_migration_with_limit(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        migration_id = uuid4()
        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.get_by_migration(migration_id, limit=10)
            query = str(mock_conn.execute.call_args[0][0])
            assert "LIMIT 10" in query


class TestPostgreSQLMigrationAuditLogRepositoryGetLatest:
    """Tests for PostgreSQLMigrationAuditLogRepository.get_latest method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationAuditLogRepository:
        return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_latest_returns_none_when_no_entries(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_latest(uuid4())
            assert result is None

    @pytest.mark.asyncio
    async def test_get_latest_returns_most_recent(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        migration_id = uuid4()
        now = datetime.now(UTC)
        row = (
            10,
            migration_id,
            "migration_completed",
            "cutover",
            "completed",
            None,
            "system",
            now,
        )

        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
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
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        migration_id = uuid4()
        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.get_latest(migration_id, event_type=AuditEventType.ERROR_OCCURRED)
            params = mock_conn.execute.call_args[0][1]
            assert params["event_type"] == "error_occurred"


class TestPostgreSQLMigrationAuditLogRepositoryCountByMigration:
    """Tests for PostgreSQLMigrationAuditLogRepository.count_by_migration method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationAuditLogRepository:
        return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_count_by_migration_returns_zero_when_empty(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (0,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.count_by_migration(uuid4())
            assert result == 0

    @pytest.mark.asyncio
    async def test_count_by_migration_returns_count(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        migration_id = uuid4()
        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (15,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.count_by_migration(migration_id)
            assert result == 15

    @pytest.mark.asyncio
    async def test_count_by_migration_with_event_type_filter(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        migration_id = uuid4()
        with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (3,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.count_by_migration(
                migration_id, event_type=AuditEventType.ERROR_OCCURRED
            )
            assert result == 3
            params = mock_conn.execute.call_args[0][1]
            assert params["event_type"] == "error_occurred"
