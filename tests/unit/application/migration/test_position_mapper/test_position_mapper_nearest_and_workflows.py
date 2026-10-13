"""Unit tests for PositionMapper._find_nearest and end-to-end migration workflows."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.ports.migration.models import PositionMapping

from ._fixtures import PositionMapperTestBase, pos


class TestPositionMapperFindNearest:
    """Tests for PositionMapper._find_nearest method."""

    @pytest.fixture
    def mapper(self) -> PositionMapper:
        """Create a mapper for testing."""
        return PositionMapper(MagicMock(), enable_tracing=False)

    def _create_mapping(self, source_n: int, target_n: int) -> PositionMapping:
        """Helper to create a position mapping."""
        return PositionMapping(
            migration_id=uuid4(),
            source_position=pos(source_n, "src"),
            target_position=pos(target_n, "tgt"),
            event_id=uuid4(),
            mapped_at=datetime.now(UTC),
        )

    def test_find_nearest_empty_list(self, mapper: PositionMapper) -> None:
        """Test _find_nearest returns None for empty list."""
        result = mapper._find_nearest([], pos(100, "src"))
        assert result is None

    def test_find_nearest_exact_match(self, mapper: PositionMapper) -> None:
        """Test _find_nearest finds exact match."""
        mappings = [
            self._create_mapping(100, 50),
            self._create_mapping(200, 100),
            self._create_mapping(300, 150),
        ]

        result = mapper._find_nearest(mappings, pos(200, "src"))

        assert result is not None
        assert result.source_position == pos(200, "src")
        assert result.target_position == pos(100, "tgt")

    def test_find_nearest_returns_lower(self, mapper: PositionMapper) -> None:
        """Test _find_nearest returns nearest lower position."""
        mappings = [
            self._create_mapping(100, 50),
            self._create_mapping(200, 100),
            self._create_mapping(300, 150),
        ]

        # Query for 250, should get mapping at 200
        result = mapper._find_nearest(mappings, pos(250, "src"))

        assert result is not None
        assert result.source_position == pos(200, "src")

    def test_find_nearest_returns_none_when_all_higher(
        self,
        mapper: PositionMapper,
    ) -> None:
        """Test _find_nearest returns None when all mappings are higher."""
        mappings = [
            self._create_mapping(100, 50),
            self._create_mapping(200, 100),
            self._create_mapping(300, 150),
        ]

        # Query for 50, no mapping <= 50 exists
        result = mapper._find_nearest(mappings, pos(50, "src"))

        assert result is None

    def test_find_nearest_single_element_match(
        self,
        mapper: PositionMapper,
    ) -> None:
        """Test _find_nearest with single element that matches."""
        mappings = [self._create_mapping(100, 50)]

        result = mapper._find_nearest(mappings, pos(100, "src"))

        assert result is not None
        assert result.source_position == pos(100, "src")

    def test_find_nearest_single_element_higher(
        self,
        mapper: PositionMapper,
    ) -> None:
        """Test _find_nearest with single element higher than query."""
        mappings = [self._create_mapping(100, 50)]

        result = mapper._find_nearest(mappings, pos(150, "src"))

        assert result is not None
        assert result.source_position == pos(100, "src")

    def test_find_nearest_single_element_lower(
        self,
        mapper: PositionMapper,
    ) -> None:
        """Test _find_nearest with single element lower than query."""
        mappings = [self._create_mapping(100, 50)]

        result = mapper._find_nearest(mappings, pos(50, "src"))

        assert result is None

    def test_find_nearest_at_boundary(self, mapper: PositionMapper) -> None:
        """Test _find_nearest at exact boundary positions."""
        mappings = [
            self._create_mapping(100, 50),
            self._create_mapping(200, 100),
        ]

        # Query at exact first position
        result = mapper._find_nearest(mappings, pos(100, "src"))
        assert result is not None
        assert result.source_position == pos(100, "src")

        # Query at exact last position
        result = mapper._find_nearest(mappings, pos(200, "src"))
        assert result is not None
        assert result.source_position == pos(200, "src")

    def test_find_nearest_large_gap(self, mapper: PositionMapper) -> None:
        """Test _find_nearest with large gaps between positions."""
        mappings = [
            self._create_mapping(100, 50),
            self._create_mapping(10000, 5000),
        ]

        # Query in the middle of large gap
        result = mapper._find_nearest(mappings, pos(5000, "src"))

        assert result is not None
        assert result.source_position == pos(100, "src")


class TestPositionMapperWorkflows(PositionMapperTestBase):
    """Integration-style tests for position mapping workflows."""

    @pytest.mark.asyncio
    async def test_bulk_copy_and_translate_workflow(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test typical bulk copy then translate workflow."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        # Simulate bulk copy recording
        mock_repo.create_batch = AsyncMock(return_value=3)
        mappings = [
            (pos(100, "src"), pos(50, "tgt"), uuid4()),
            (pos(200, "src"), pos(100, "tgt"), uuid4()),
            (pos(300, "src"), pos(150, "tgt"), uuid4()),
        ]
        await mapper.record_mappings_batch(migration_id, mappings)

        # Simulate checkpoint translation after bulk copy
        mock_repo.find_by_source_position = AsyncMock(
            return_value=PositionMapping(
                migration_id=migration_id,
                source_position=pos(200, "src"),
                target_position=pos(100, "tgt"),
                event_id=uuid4(),
                mapped_at=now,
            )
        )

        result = await mapper.translate_position(migration_id, pos(200, "src"))

        assert result.target_position == pos(100, "tgt")
        assert result.is_exact is True

    @pytest.mark.asyncio
    async def test_subscription_migration_workflow(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test subscription checkpoint translation workflow."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        # Subscriber has checkpoint at source position 950
        # No exact mapping exists, but position 900 was mapped to 450
        mock_repo.find_by_source_position = AsyncMock(return_value=None)
        mock_repo.find_nearest_source_position = AsyncMock(
            return_value=PositionMapping(
                migration_id=migration_id,
                source_position=pos(900, "src"),
                target_position=pos(450, "tgt"),
                event_id=uuid4(),
                mapped_at=now,
            )
        )

        # Translate checkpoint for subscription migration
        result = await mapper.translate_position(migration_id, pos(950, "src"))

        # Subscriber will resume at target position 450
        # This is conservative - they may replay a few events
        assert result.target_position == pos(450, "tgt")
        assert result.is_exact is False
        assert result.nearest_source_position == pos(900, "src")

    @pytest.mark.asyncio
    async def test_migration_cleanup_workflow(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test migration cleanup deletes all mappings."""
        migration_id = uuid4()

        mock_repo.count_by_migration = AsyncMock(return_value=10000)
        mock_repo.delete_by_migration = AsyncMock(return_value=10000)

        # Check count before cleanup
        count = await mapper.get_mapping_count(migration_id)
        assert count == 10000

        # Cleanup
        deleted = await mapper.clear_mappings(migration_id)
        assert deleted == 10000
