"""get_by_migration accepts only None or an int >= 1 as `limit`, and rejects others before the driver.

Blackbox through `PostgreSQLMigrationAuditLogRepository.get_by_migration`. A bad
limit must raise `ValueError` without opening a connection.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.audit_log import PostgreSQLMigrationAuditLogRepository


@contextmanager
def _sql_connection_spy():
    """Patch the connection context manager and yield the mock that opens it."""
    with patch("eventsource.adapters.sql.migration.audit_log.sql_connection") as mock_ctx:
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = MagicMock(fetchall=MagicMock(return_value=[]))
        mock_ctx.return_value.__aenter__.return_value = mock_conn
        yield mock_ctx


@pytest.fixture
def repo() -> PostgreSQLMigrationAuditLogRepository:
    return PostgreSQLMigrationAuditLogRepository(MagicMock(), enable_tracing=False)


class TestInvalidLimitIsRejected:
    @pytest.mark.parametrize(
        "limit",
        [0, -1, -100, True, False],
        ids=["zero", "negative-one", "negative-hundred", "true", "false"],
    )
    async def test_rejects_out_of_range_or_bool_limit(
        self, repo: PostgreSQLMigrationAuditLogRepository, limit: Any
    ) -> None:
        with _sql_connection_spy() as opened, pytest.raises(ValueError, match="limit"):
            await repo.get_by_migration(uuid4(), limit=limit)
        opened.assert_not_called()

    @pytest.mark.parametrize("limit", [1.0, "10", 2.5], ids=["float", "string", "fraction"])
    async def test_rejects_non_int_limit(
        self, repo: PostgreSQLMigrationAuditLogRepository, limit: Any
    ) -> None:
        with _sql_connection_spy() as opened, pytest.raises(ValueError, match="limit"):
            await repo.get_by_migration(uuid4(), limit=limit)
        opened.assert_not_called()


class TestValidLimitIsAccepted:
    async def test_none_applies_no_limit_clause(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with _sql_connection_spy() as opened:
            assert await repo.get_by_migration(uuid4(), limit=None) == []
        sql = str(opened.return_value.__aenter__.return_value.execute.call_args[0][0])
        assert "LIMIT" not in sql

    async def test_one_is_the_smallest_accepted_limit(
        self, repo: PostgreSQLMigrationAuditLogRepository
    ) -> None:
        with _sql_connection_spy() as opened:
            await repo.get_by_migration(uuid4(), limit=1)
        conn = opened.return_value.__aenter__.return_value
        assert conn.execute.call_args[0][1]["limit"] == 1
