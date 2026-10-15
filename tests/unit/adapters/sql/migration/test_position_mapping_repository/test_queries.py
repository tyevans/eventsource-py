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

type Repo = PostgreSQLPositionMappingRepository

_SQL_CONN_PATCH = "eventsource.adapters.sql.migration.position_mapping.sql_connection"


def _setup_sql_conn(
    mock_ctx: MagicMock,
    *,
    fetchall: list | None = None,
    fetchone: tuple | None = None,
    rowcount: int | None = None,
) -> AsyncMock:
    mock_conn = AsyncMock()
    mock_result = MagicMock()
    if fetchall is not None:
        mock_result.fetchall.return_value = fetchall
    if fetchone is not None:
        mock_result.fetchone.return_value = fetchone
    if rowcount is not None:
        mock_result.rowcount = rowcount
    mock_conn.execute.return_value = mock_result
    mock_ctx.return_value.__aenter__.return_value = mock_conn
    return mock_conn


class _BaseRepoTest:
    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> Repo:
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)


class TestPostgreSQLPositionMappingRepositoryListByMigration(_BaseRepoTest):
    @pytest.mark.asyncio
    async def test_list_by_migration_returns_empty_list(self, repo: Repo) -> None:
        with patch(_SQL_CONN_PATCH) as mock_ctx:
            _setup_sql_conn(mock_ctx, fetchall=[])
            assert await repo.list_by_migration(uuid4()) == []

    @pytest.mark.asyncio
    async def test_list_by_migration_returns_mappings_ordered_by_id(self, repo: Repo) -> None:
        migration_id = uuid4()
        now = datetime.now(UTC)
        rows = [
            (1, migration_id, pos(100, "src").to_str(), pos(50, "tgt").to_str(), uuid4(), now),
            (2, migration_id, pos(200, "src").to_str(), pos(100, "tgt").to_str(), uuid4(), now),
            (3, migration_id, pos(300, "src").to_str(), pos(150, "tgt").to_str(), uuid4(), now),
        ]
        with patch(_SQL_CONN_PATCH) as mock_ctx:
            mock_conn = _setup_sql_conn(mock_ctx, fetchall=rows)
            result = await repo.list_by_migration(migration_id)
            assert [m.source_position for m in result] == [
                pos(100, "src"),
                pos(200, "src"),
                pos(300, "src"),
            ]
            assert "ORDER BY id ASC" in str(mock_conn.execute.call_args[0][0])

    @pytest.mark.asyncio
    async def test_list_by_migration_uses_pagination(self, repo: Repo) -> None:
        migration_id = uuid4()
        with patch(_SQL_CONN_PATCH) as mock_ctx:
            mock_conn = _setup_sql_conn(mock_ctx, fetchall=[])
            await repo.list_by_migration(migration_id, limit=50, offset=100)
            params = mock_conn.execute.call_args[0][1]
            assert params["limit"] == 50
            assert params["offset"] == 100


class TestPostgreSQLPositionMappingRepositoryListInSourceRange(_BaseRepoTest):
    @pytest.mark.asyncio
    async def test_list_in_source_range_returns_empty_when_no_mappings(self, repo: Repo) -> None:
        repo.count_by_migration = AsyncMock(return_value=0)  # type: ignore[method-assign]
        assert await repo.list_in_source_range(uuid4(), pos(0), pos(100)) == []

    @pytest.mark.asyncio
    async def test_list_in_source_range_returns_matching_mappings(self, repo: Repo) -> None:
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
    async def test_list_in_source_range_empty_when_bounds_invert(self, repo: Repo) -> None:
        migration_id = uuid4()
        repo.count_by_migration = AsyncMock(return_value=5)  # type: ignore[method-assign]
        repo._find_first_ordinal_gte = AsyncMock(return_value=4)  # type: ignore[method-assign]
        repo._find_last_ordinal_lte = AsyncMock(return_value=1)  # type: ignore[method-assign]
        assert await repo.list_in_source_range(migration_id, pos(900), pos(100)) == []


class TestPostgreSQLPositionMappingRepositoryCountByMigration(_BaseRepoTest):
    @pytest.mark.asyncio
    async def test_count_returns_zero_when_no_mappings(self, repo: Repo) -> None:
        with patch(_SQL_CONN_PATCH) as mock_ctx:
            _setup_sql_conn(mock_ctx, fetchone=(0,))
            assert await repo.count_by_migration(uuid4()) == 0

    @pytest.mark.asyncio
    async def test_count_returns_correct_count(self, repo: Repo) -> None:
        with patch(_SQL_CONN_PATCH) as mock_ctx:
            _setup_sql_conn(mock_ctx, fetchone=(1500,))
            assert await repo.count_by_migration(uuid4()) == 1500


class TestPostgreSQLPositionMappingRepositoryGetPositionBounds(_BaseRepoTest):
    @pytest.mark.asyncio
    async def test_get_bounds_returns_none_when_no_mappings(self, repo: Repo) -> None:
        repo.count_by_migration = AsyncMock(return_value=0)  # type: ignore[method-assign]
        assert await repo.get_position_bounds(uuid4()) is None

    @pytest.mark.asyncio
    async def test_get_bounds_returns_first_and_last_by_id(self, repo: Repo) -> None:
        migration_id = uuid4()
        first, last = (
            PositionMapping(
                migration_id=migration_id,
                source_position=pos(n, "src"),
                target_position=pos(n // 2, "tgt"),
                event_id=uuid4(),
                mapped_at=datetime.now(UTC),
            )
            for n in (100, 10000)
        )
        repo.count_by_migration = AsyncMock(return_value=2)  # type: ignore[method-assign]

        async def get_by_ordinal(mig_id: object, ordinal: int) -> PositionMapping | None:
            return first if ordinal == 0 else last

        repo._get_by_ordinal = get_by_ordinal  # type: ignore[method-assign]
        assert await repo.get_position_bounds(migration_id) == (
            pos(100, "src"),
            pos(10000, "src"),
        )


class TestPostgreSQLPositionMappingRepositoryDeleteByMigration(_BaseRepoTest):
    @pytest.mark.asyncio
    async def test_delete_returns_zero_when_no_mappings(self, repo: Repo) -> None:
        with patch(_SQL_CONN_PATCH) as mock_ctx:
            _setup_sql_conn(mock_ctx, rowcount=0)
            assert await repo.delete_by_migration(uuid4()) == 0

    @pytest.mark.asyncio
    async def test_delete_returns_count_of_deleted(self, repo: Repo) -> None:
        with patch(_SQL_CONN_PATCH) as mock_ctx:
            _setup_sql_conn(mock_ctx, rowcount=500)
            assert await repo.delete_by_migration(uuid4()) == 500
