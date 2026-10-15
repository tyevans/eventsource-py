"""Unit tests for PositionMappingRepository row helpers and workflows."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.sql.migration.position_mapping import (
    PostgreSQLPositionMappingRepository,
)
from eventsource.ports.exceptions import PositionDecodeError
from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position

from ._helpers import pos


class TestPostgreSQLPositionMappingRepositoryHelpers:
    @pytest.fixture
    def repo(self) -> PostgreSQLPositionMappingRepository:
        return PostgreSQLPositionMappingRepository(MagicMock(), enable_tracing=False)

    def test_row_to_mapping_with_all_fields(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        migration_id, event_id, mapped_at = uuid4(), uuid4(), datetime.now(UTC)
        source_position, target_position = pos(1000, "src"), pos(500, "tgt")
        row = (
            1,
            migration_id,
            source_position.to_str(),
            target_position.to_str(),
            event_id,
            mapped_at,
        )

        mapping = repo._row_to_mapping(row)
        assert mapping.migration_id == migration_id
        assert mapping.source_position == source_position
        assert mapping.target_position == target_position
        assert mapping.event_id == event_id
        assert mapping.mapped_at == mapped_at

    def test_row_to_mapping_preserves_types(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        migration_id, event_id, mapped_at = uuid4(), uuid4(), datetime.now(UTC)
        row = (
            1,
            migration_id,
            pos(999999999, "src").to_str(),
            pos(888888888, "tgt").to_str(),
            event_id,
            mapped_at,
        )

        mapping = repo._row_to_mapping(row)
        assert isinstance(mapping.migration_id, UUID)
        assert isinstance(mapping.source_position, Position)
        assert isinstance(mapping.target_position, Position)
        assert isinstance(mapping.event_id, UUID)
        assert isinstance(mapping.mapped_at, datetime)

    def test_row_to_mapping_raises_on_missing_source_token(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        row = (1, uuid4(), None, pos(500, "tgt").to_str(), uuid4(), datetime.now(UTC))
        with pytest.raises(PositionDecodeError):
            repo._row_to_mapping(row)

    def test_row_to_mapping_raises_on_missing_target_token(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        row = (1, uuid4(), pos(1000, "src").to_str(), None, uuid4(), datetime.now(UTC))
        with pytest.raises(PositionDecodeError):
            repo._row_to_mapping(row)


class TestPositionMappingWorkflow:
    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_bulk_copy_workflow(self, repo: PostgreSQLPositionMappingRepository) -> None:
        migration_id, now = uuid4(), datetime.now(UTC)
        mappings = [
            PositionMapping(
                migration_id=migration_id,
                source_position=pos(i * 100, "src"),
                target_position=pos(i * 50, "tgt"),
                event_id=uuid4(),
                mapped_at=now,
            )
            for i in range(100)
        ]

        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock(execute=AsyncMock(return_value=MagicMock(rowcount=100)))
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            count = await repo.create_batch(mappings)
            assert count == 100
            mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_checkpoint_translation_workflow(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        migration_id = uuid4()
        mapping = PositionMapping(
            migration_id=migration_id,
            source_position=pos(900, "src"),
            target_position=pos(450, "tgt"),
            event_id=uuid4(),
            mapped_at=datetime.now(UTC),
        )
        repo.count_by_migration = AsyncMock(return_value=1)  # type: ignore[method-assign]

        async def get_by_ordinal(mig_id: object, ordinal: int) -> PositionMapping | None:
            return mapping

        repo._get_by_ordinal = get_by_ordinal  # type: ignore[method-assign]

        result = await repo.find_nearest_source_position(migration_id, pos(950))
        assert result is not None
        assert result.source_position == pos(900, "src")
        assert result.target_position == pos(450, "tgt")

    @pytest.mark.asyncio
    async def test_migration_cleanup_workflow(
        self, repo: PostgreSQLPositionMappingRepository
    ) -> None:
        migration_id = uuid4()
        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock(execute=AsyncMock(return_value=MagicMock(rowcount=10000)))
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            deleted = await repo.delete_by_migration(migration_id)
            assert deleted == 10000
            query_text = str(mock_conn.execute.call_args[0][0])
            assert "DELETE FROM migration_position_mappings" in query_text
