"""Unit tests for PositionMappingRepository lookup operations."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.position_mapping import (
    PostgreSQLPositionMappingRepository,
)
from eventsource.ports.migration.models import PositionMapping

from ._helpers import pos


@pytest.fixture
def mock_conn() -> MagicMock:
    return MagicMock()


@pytest.fixture
def repo(mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
    return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)


@contextmanager
def _patch_fetchone(row: object) -> Iterator[AsyncMock]:
    with patch("eventsource.adapters.sql.migration.position_mapping.sql_connection") as mock_ctx:
        conn = AsyncMock()
        conn.execute.return_value = MagicMock(fetchone=MagicMock(return_value=row))
        mock_ctx.return_value.__aenter__.return_value = conn
        yield conn


class TestPostgreSQLPositionMappingRepositoryFindBySourcePosition:
    @pytest.mark.asyncio
    async def test_find_by_source_position_returns_none_when_not_found(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        with _patch_fetchone(None):
            assert await repo.find_by_source_position(uuid4(), pos(1000)) is None

    @pytest.mark.asyncio
    async def test_find_by_source_position_returns_mapping_on_canonical_token(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        migration_id, event_id, now = uuid4(), uuid4(), datetime.now(UTC)
        source_position, target_position = pos(1000, "src"), pos(500, "tgt")
        row = (1, migration_id, source_position.to_str(), target_position.to_str(), event_id, now)

        with _patch_fetchone(row) as conn:
            result = await repo.find_by_source_position(migration_id, source_position)
            assert result is not None
            assert result.source_position == source_position
            assert result.target_position == target_position
            assert conn.execute.call_args[0][1]["source_position_token"] == source_position.to_str()

    @pytest.mark.asyncio
    async def test_find_by_source_position_misses_on_token_from_different_store(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        migration_id, other_pos = uuid4(), pos(1000, "other-store")
        with _patch_fetchone(None) as conn:
            assert await repo.find_by_source_position(migration_id, other_pos) is None
            assert conn.execute.call_args[0][1]["source_position_token"] == other_pos.to_str()


class TestPostgreSQLPositionMappingRepositoryFindByTargetPosition:
    @pytest.mark.asyncio
    async def test_find_by_target_position_returns_none_when_not_found(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        with _patch_fetchone(None):
            assert await repo.find_by_target_position(uuid4(), pos(500, "tgt")) is None

    @pytest.mark.asyncio
    async def test_find_by_target_position_returns_mapping(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        migration_id, event_id, now = uuid4(), uuid4(), datetime.now(UTC)
        source_position, target_position = pos(1000, "src"), pos(500, "tgt")
        row = (1, migration_id, source_position.to_str(), target_position.to_str(), event_id, now)

        with _patch_fetchone(row):
            result = await repo.find_by_target_position(migration_id, target_position)
            assert result is not None and result.target_position == target_position


class TestPostgreSQLPositionMappingRepositoryFindNearestSourcePosition:
    def _mapping_at(self, migration_id: object, source_n: int, target_n: int) -> PositionMapping:
        return PositionMapping(
            migration_id=migration_id,  # type: ignore[arg-type]
            source_position=pos(source_n, "src"),
            target_position=pos(target_n, "tgt"),
            event_id=uuid4(),
            mapped_at=datetime.now(UTC),
        )

    def _stub_rows(
        self, repo: PostgreSQLPositionMappingRepository, rows: list[PositionMapping]
    ) -> None:
        repo.count_by_migration = AsyncMock(return_value=len(rows))  # type: ignore[method-assign]

        async def get_by_ordinal(mig_id: object, ordinal: int) -> PositionMapping | None:
            return rows[ordinal]

        repo._get_by_ordinal = get_by_ordinal  # type: ignore[method-assign]

    @pytest.mark.asyncio
    async def test_find_nearest_returns_none_when_no_mappings(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        repo.count_by_migration = AsyncMock(return_value=0)  # type: ignore[method-assign]
        assert await repo.find_nearest_source_position(uuid4(), pos(1000)) is None

    @pytest.mark.asyncio
    async def test_find_nearest_returns_exact_match(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        m_id = uuid4()
        self._stub_rows(repo, [self._mapping_at(m_id, 900, 450), self._mapping_at(m_id, 1000, 500)])
        result = await repo.find_nearest_source_position(m_id, pos(1000))
        assert result is not None and result.source_position == pos(1000, "src")

    @pytest.mark.asyncio
    async def test_find_nearest_returns_lower_position(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        m_id = uuid4()
        rows = [self._mapping_at(m_id, s, t) for s, t in ((100, 50), (900, 450), (1000, 500))]
        self._stub_rows(repo, rows)
        result = await repo.find_nearest_source_position(m_id, pos(950))
        assert result is not None and result.source_position == pos(900, "src")

    @pytest.mark.asyncio
    async def test_find_nearest_returns_none_when_all_greater(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        m_id = uuid4()
        self._stub_rows(repo, [self._mapping_at(m_id, 500, 250), self._mapping_at(m_id, 900, 450)])
        assert await repo.find_nearest_source_position(m_id, pos(100)) is None

    @pytest.mark.asyncio
    async def test_find_nearest_single_row_table_matches(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        m_id = uuid4()
        self._stub_rows(repo, [self._mapping_at(m_id, 500, 250)])
        result = await repo.find_nearest_source_position(m_id, pos(600))
        assert result is not None and result.source_position == pos(500, "src")

    @pytest.mark.asyncio
    async def test_find_nearest_single_row_table_none(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        m_id = uuid4()
        self._stub_rows(repo, [self._mapping_at(m_id, 500, 250)])
        assert await repo.find_nearest_source_position(m_id, pos(100)) is None


class TestPostgreSQLPositionMappingRepositoryFindByEventId:
    @pytest.mark.asyncio
    async def test_find_by_event_id_returns_none_when_not_found(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        with _patch_fetchone(None):
            assert await repo.find_by_event_id(uuid4(), uuid4()) is None

    @pytest.mark.asyncio
    async def test_find_by_event_id_returns_mapping(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        migration_id, event_id, now = uuid4(), uuid4(), datetime.now(UTC)
        row = (1, migration_id, pos(1000, "src").to_str(), pos(500, "tgt").to_str(), event_id, now)
        with _patch_fetchone(row):
            result = await repo.find_by_event_id(migration_id, event_id)
            assert result is not None and result.event_id == event_id
