"""Tests for querying audit log entries by migration and counting entries."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.serialization import json_dumps
from eventsource.adapters.sql.migration.audit_log import (
    PostgreSQLMigrationAuditLogRepository,
)
from eventsource.ports.migration.models import AuditEventType, MigrationPhase


@contextmanager
def _patched_sql_conn(*, fetchall=None, fetchone=None):
    with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
        mock_conn = AsyncMock()
        mock_result = MagicMock()
        if fetchall is not None:
            mock_result.fetchall.return_value = fetchall
        mock_result.fetchone.return_value = fetchone
        mock_conn.execute.return_value = mock_result
        mock_ctx.return_value.__aenter__.return_value = mock_conn
        yield mock_conn


@pytest.fixture
def mock_conn() -> MagicMock:
    return MagicMock()


@pytest.fixture
def repo(mock_conn: MagicMock) -> PostgreSQLMigrationAuditLogRepository:
    return PostgreSQLMigrationAuditLogRepository(mock_conn, enable_tracing=False)


@pytest.mark.asyncio
class TestPostgreSQLMigrationAuditLogRepositoryGetByMigration:
    """Tests for PostgreSQLMigrationAuditLogRepository.get_by_migration method."""

    async def test_get_by_migration_returns_empty_list(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with _patched_sql_conn(fetchall=[]):
            assert await repo.get_by_migration(uuid4()) == []

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
        with _patched_sql_conn(fetchall=rows):
            result = await repo.get_by_migration(migration_id)
            assert len(result) == 2
            assert result[0].id == 1 and result[0].event_type == AuditEventType.MIGRATION_STARTED
            assert result[0].new_phase == MigrationPhase.PENDING
            assert result[1].id == 2 and result[1].event_type == AuditEventType.PHASE_CHANGED
            assert result[1].old_phase == MigrationPhase.PENDING
            assert result[1].new_phase == MigrationPhase.BULK_COPY

    async def test_get_by_migration_with_event_type_filter(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with _patched_sql_conn(fetchall=[]) as mock_conn:
            await repo.get_by_migration(
                uuid4(),
                event_types=[AuditEventType.PHASE_CHANGED, AuditEventType.ERROR_OCCURRED],
            )
            params = mock_conn.execute.call_args[0][1]
            assert params["event_types"] == ["phase_changed", "error_occurred"]

    async def test_get_by_migration_with_time_filters(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        since = datetime.now(UTC) - timedelta(hours=1)
        until = datetime.now(UTC)
        with _patched_sql_conn(fetchall=[]) as mock_conn:
            await repo.get_by_migration(uuid4(), since=since, until=until)
            params = mock_conn.execute.call_args[0][1]
            assert params["since"] == since and params["until"] == until

    async def test_get_by_migration_with_limit(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with _patched_sql_conn(fetchall=[]) as mock_conn:
            await repo.get_by_migration(uuid4(), limit=10)
            assert "LIMIT 10" in str(mock_conn.execute.call_args[0][0])


@pytest.mark.asyncio
class TestPostgreSQLMigrationAuditLogRepositoryGetLatest:
    """Tests for PostgreSQLMigrationAuditLogRepository.get_latest method."""

    async def test_get_latest_returns_none_when_no_entries(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with _patched_sql_conn(fetchone=None):
            assert await repo.get_latest(uuid4()) is None

    async def test_get_latest_returns_most_recent(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        migration_id = uuid4()
        row = (
            10,
            migration_id,
            "migration_completed",
            "cutover",
            "completed",
            None,
            "system",
            datetime.now(UTC),
        )
        with _patched_sql_conn(fetchone=row):
            result = await repo.get_latest(migration_id)
            assert result is not None and result.id == 10
            assert result.event_type == AuditEventType.MIGRATION_COMPLETED

    async def test_get_latest_with_event_type_filter(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with _patched_sql_conn(fetchone=None) as mock_conn:
            await repo.get_latest(uuid4(), event_type=AuditEventType.ERROR_OCCURRED)
            assert mock_conn.execute.call_args[0][1]["event_type"] == "error_occurred"


@pytest.mark.asyncio
class TestPostgreSQLMigrationAuditLogRepositoryCountByMigration:
    """Tests for PostgreSQLMigrationAuditLogRepository.count_by_migration method."""

    async def test_count_by_migration_returns_zero_when_empty(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with _patched_sql_conn(fetchone=(0,)):
            assert await repo.count_by_migration(uuid4()) == 0

    async def test_count_by_migration_returns_count(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with _patched_sql_conn(fetchone=(15,)):
            assert await repo.count_by_migration(uuid4()) == 15

    async def test_count_by_migration_with_event_type_filter(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with _patched_sql_conn(fetchone=(3,)) as mock_conn:
            result = await repo.count_by_migration(
                uuid4(), event_type=AuditEventType.ERROR_OCCURRED
            )
            assert result == 3
            assert mock_conn.execute.call_args[0][1]["event_type"] == "error_occurred"
