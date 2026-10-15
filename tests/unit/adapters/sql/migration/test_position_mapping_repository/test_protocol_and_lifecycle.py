"""Unit tests for PositionMappingRepository protocol and lifecycle/CRUD operations."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.position_mapping import (
    PostgreSQLPositionMappingRepository,
)
from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.migration.repositories import PositionMappingRepository

from ._helpers import pos


class TestPositionMappingRepositoryProtocol:
    def test_postgresql_repository_implements_protocol(self) -> None:
        mock_conn = MagicMock()
        repo = PostgreSQLPositionMappingRepository(mock_conn)
        assert isinstance(repo, PositionMappingRepository)

    def test_protocol_has_required_methods(self) -> None:
        for method in (
            "create",
            "create_batch",
            "get",
            "find_by_source_position",
            "find_by_target_position",
            "find_nearest_source_position",
            "find_by_event_id",
            "list_by_migration",
            "list_in_source_range",
            "count_by_migration",
            "get_position_bounds",
            "delete_by_migration",
        ):
            assert hasattr(PositionMappingRepository, method)


class TestPostgreSQLPositionMappingRepositoryInit:
    def test_init_with_connection(self) -> None:
        mock_conn = MagicMock()
        repo = PostgreSQLPositionMappingRepository(mock_conn)
        assert repo._conn == mock_conn

    def test_init_with_tracing_enabled(self) -> None:
        mock_conn = MagicMock()
        repo = PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=True)
        assert repo._conn == mock_conn

    def test_init_with_tracing_disabled(self) -> None:
        mock_conn = MagicMock()
        repo = PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)
        assert repo._conn == mock_conn


class TestPostgreSQLPositionMappingRepositoryCreate:
    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.fixture
    def sample_mapping(self) -> PositionMapping:
        return PositionMapping(
            migration_id=uuid4(),
            source_position=pos(1000, "src"),
            target_position=pos(500, "tgt"),
            event_id=uuid4(),
            mapped_at=datetime.now(UTC),
        )

    @pytest.mark.asyncio
    async def test_create_returns_id(
        self, repo: PostgreSQLPositionMappingRepository, sample_mapping: PositionMapping
    ) -> None:
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = (42,)
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            assert await repo.create(sample_mapping) == 42

    @pytest.mark.asyncio
    async def test_create_executes_insert_with_tokens(
        self, repo: PostgreSQLPositionMappingRepository, sample_mapping: PositionMapping
    ) -> None:
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
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
    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_create_batch_with_empty_list(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        assert await repo.create_batch([]) == 0

    @pytest.mark.asyncio
    async def test_create_batch_returns_count(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        migration_id, now = uuid4(), datetime.now(UTC)
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
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 5
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            assert await repo.create_batch(mappings) == 5

    @pytest.mark.asyncio
    async def test_create_batch_uses_multi_row_insert_with_tokens(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        migration_id, now = uuid4(), datetime.now(UTC)
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
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 3
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.create_batch(mappings)
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert "migration_id_0" in params
            assert "migration_id_1" in params
            assert "migration_id_2" in params
            assert params["source_position_token_0"] == mappings[0].source_position.to_str()
            assert params["source_position_token_1"] == mappings[1].source_position.to_str()
            assert params["source_position_token_2"] == mappings[2].source_position.to_str()


class TestPostgreSQLPositionMappingRepositoryGet:
    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_returns_none_when_not_found(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            assert await repo.get(999) is None

    @pytest.mark.asyncio
    async def test_get_returns_mapping_when_found(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        migration_id, event_id, now = uuid4(), uuid4(), datetime.now(UTC)
        source_position, target_position = pos(1000, "src"), pos(500, "tgt")
        row = (
            1,
            migration_id,
            source_position.to_str(),
            target_position.to_str(),
            event_id,
            now,
        )
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
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
