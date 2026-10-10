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
    """Tests for PostgreSQLPositionMappingRepository helper methods."""

    @pytest.fixture
    def repo(self) -> PostgreSQLPositionMappingRepository:
        """Create a repository for testing."""
        mock_conn = MagicMock()
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    def test_row_to_mapping_with_all_fields(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test converting a row with all fields populated."""
        migration_id = uuid4()
        event_id = uuid4()
        mapped_at = datetime.now(UTC)
        source_position = pos(1000, "src")
        target_position = pos(500, "tgt")

        row = (
            1,  # id (not in model)
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
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test row conversion preserves correct types."""
        migration_id = uuid4()
        event_id = uuid4()
        mapped_at = datetime.now(UTC)

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
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test a row with no source token (legacy int-only row) raises PositionDecodeError."""
        row = (1, uuid4(), None, pos(500, "tgt").to_str(), uuid4(), datetime.now(UTC))

        with pytest.raises(PositionDecodeError):
            repo._row_to_mapping(row)

    def test_row_to_mapping_raises_on_missing_target_token(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test a row with no target token (legacy int-only row) raises PositionDecodeError."""
        row = (1, uuid4(), pos(1000, "src").to_str(), None, uuid4(), datetime.now(UTC))

        with pytest.raises(PositionDecodeError):
            repo._row_to_mapping(row)


class TestPositionMappingWorkflow:
    """Integration-style tests for position mapping workflows."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLPositionMappingRepository:
        """Create a repository with mock connection."""
        return PostgreSQLPositionMappingRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_bulk_copy_workflow(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test typical bulk copy workflow."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        # Create batch of mappings
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
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 100
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            # Batch insert
            count = await repo.create_batch(mappings)

            assert count == 100
            mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_checkpoint_translation_workflow(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test checkpoint translation using nearest position lookup."""
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

        # Subscriber has checkpoint at source position 950
        # Need to find nearest target position
        result = await repo.find_nearest_source_position(migration_id, pos(950))

        assert result is not None
        assert result.source_position == pos(900, "src")
        assert result.target_position == pos(450, "tgt")

        # Subscriber would continue from target position 450

    @pytest.mark.asyncio
    async def test_migration_cleanup_workflow(
        self,
        repo: PostgreSQLPositionMappingRepository,
    ) -> None:
        """Test migration cleanup deletes all mappings."""
        migration_id = uuid4()

        with patch(
            "eventsource.adapters.sql.migration.position_mapping.sql_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.rowcount = 10000
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            deleted = await repo.delete_by_migration(migration_id)

            assert deleted == 10000
            # Verify DELETE query was executed
            call_args = mock_conn.execute.call_args
            query_text = str(call_args[0][0])
            assert "DELETE FROM migration_position_mappings" in query_text
