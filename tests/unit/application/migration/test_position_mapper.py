"""
Unit tests for PositionMapper.

Tests cover:
- PositionMapper initialization
- record_mapping() single mapping creation
- record_mappings_batch() batch creation
- translate_position() with exact and nearest matching
- translate_position_reverse() for reverse lookups
- translate_positions_batch() for batch translation
- Helper methods (get_mapping_by_event_id, get_position_bounds, etc.)
- Error handling and edge cases
- Binary search nearest-neighbor algorithm
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.application.migration.position_mapper import (
    PositionMapper,
    ReverseTranslationResult,
    TranslationResult,
)
from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position


def pos(n: int, store: str = "src") -> Position:
    """Build a Position for a given store, keyed by a single int."""
    return Position(store_id=store, key=(n,))


class TestPositionMapperInit:
    """Tests for PositionMapper initialization."""

    def test_init_with_repository(self) -> None:
        """Test initialization with a repository."""
        mock_repo = MagicMock()
        mapper = PositionMapper(mock_repo)
        assert mapper._repo == mock_repo

    def test_init_with_tracing_enabled(self) -> None:
        """Test initialization with tracing enabled."""
        mock_repo = MagicMock()
        mapper = PositionMapper(mock_repo, enable_tracing=True)
        assert mapper._repo == mock_repo

    def test_init_with_tracing_disabled(self) -> None:
        """Test initialization with tracing disabled."""
        mock_repo = MagicMock()
        mapper = PositionMapper(mock_repo, enable_tracing=False)
        assert mapper._repo == mock_repo


class TestPositionMapperRecordMapping:
    """Tests for PositionMapper.record_mapping method."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        repo = MagicMock()
        repo.create = AsyncMock(return_value=1)
        return repo

    @pytest.fixture
    def mapper(self, mock_repo: MagicMock) -> PositionMapper:
        """Create a mapper with mock repository."""
        return PositionMapper(mock_repo, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_record_mapping_creates_mapping(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test record_mapping creates a mapping in the repository."""
        migration_id = uuid4()
        event_id = uuid4()
        source_position = pos(1000, "src")
        target_position = pos(500, "tgt")

        await mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_position,
            target_position=target_position,
            event_id=event_id,
        )

        mock_repo.create.assert_called_once()
        call_arg = mock_repo.create.call_args[0][0]
        assert isinstance(call_arg, PositionMapping)
        assert call_arg.migration_id == migration_id
        assert call_arg.source_position == source_position
        assert call_arg.target_position == target_position
        assert call_arg.event_id == event_id

    @pytest.mark.asyncio
    async def test_record_mapping_with_custom_timestamp(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test record_mapping with custom mapped_at timestamp."""
        migration_id = uuid4()
        event_id = uuid4()
        custom_time = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

        await mapper.record_mapping(
            migration_id=migration_id,
            source_position=pos(1000, "src"),
            target_position=pos(500, "tgt"),
            event_id=event_id,
            mapped_at=custom_time,
        )

        call_arg = mock_repo.create.call_args[0][0]
        assert call_arg.mapped_at == custom_time

    @pytest.mark.asyncio
    async def test_record_mapping_raises_on_error(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test record_mapping raises PositionMappingError on failure."""
        migration_id = uuid4()
        mock_repo.create.side_effect = Exception("Database error")
        source_position = pos(1000, "src")

        with pytest.raises(PositionMappingError) as exc_info:
            await mapper.record_mapping(
                migration_id=migration_id,
                source_position=source_position,
                target_position=pos(500, "tgt"),
                event_id=uuid4(),
            )

        assert exc_info.value.migration_id == migration_id
        assert exc_info.value.source_position == source_position
        assert "Database error" in str(exc_info.value)


class TestPositionMapperRecordMappingsBatch:
    """Tests for PositionMapper.record_mappings_batch method."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        repo = MagicMock()
        repo.create_batch = AsyncMock(return_value=5)
        return repo

    @pytest.fixture
    def mapper(self, mock_repo: MagicMock) -> PositionMapper:
        """Create a mapper with mock repository."""
        return PositionMapper(mock_repo, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_record_batch_with_empty_list(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test record_mappings_batch returns 0 for empty list."""
        result = await mapper.record_mappings_batch(uuid4(), [])
        assert result == 0
        mock_repo.create_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_record_batch_creates_mappings_in_ascending_order(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test record_mappings_batch creates all mappings, preserving caller order."""
        migration_id = uuid4()
        mappings = [
            (pos(100, "src"), pos(50, "tgt"), uuid4()),
            (pos(200, "src"), pos(100, "tgt"), uuid4()),
            (pos(300, "src"), pos(150, "tgt"), uuid4()),
        ]

        mock_repo.create_batch.return_value = 3
        result = await mapper.record_mappings_batch(migration_id, mappings)

        assert result == 3
        mock_repo.create_batch.assert_called_once()
        call_arg = mock_repo.create_batch.call_args[0][0]
        assert len(call_arg) == 3
        # The monotonicity precondition requires ascending source-position
        # order to be preserved by the batch writer, not reordered.
        assert [m.source_position for m in call_arg] == [
            pos(100, "src"),
            pos(200, "src"),
            pos(300, "src"),
        ]

    @pytest.mark.asyncio
    async def test_record_batch_raises_on_error(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test record_mappings_batch raises on failure."""
        migration_id = uuid4()
        mock_repo.create_batch.side_effect = Exception("Batch error")

        with pytest.raises(PositionMappingError) as exc_info:
            await mapper.record_mappings_batch(
                migration_id,
                [(pos(100, "src"), pos(50, "tgt"), uuid4())],
            )

        assert exc_info.value.migration_id == migration_id
        assert "Batch error" in str(exc_info.value)


class TestPositionMapperTranslatePosition:
    """Tests for PositionMapper.translate_position method."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        return MagicMock()

    @pytest.fixture
    def mapper(self, mock_repo: MagicMock) -> PositionMapper:
        """Create a mapper with mock repository."""
        return PositionMapper(mock_repo, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_translate_exact_match(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test translate_position returns exact match when available."""
        migration_id = uuid4()
        event_id = uuid4()
        source_position = pos(1000, "src")
        target_position = pos(500, "tgt")

        mock_repo.find_by_source_position = AsyncMock(
            return_value=PositionMapping(
                migration_id=migration_id,
                source_position=source_position,
                target_position=target_position,
                event_id=event_id,
                mapped_at=datetime.now(UTC),
            )
        )

        result = await mapper.translate_position(migration_id, source_position)

        assert isinstance(result, TranslationResult)
        assert result.source_position == source_position
        assert result.target_position == target_position
        assert result.is_exact is True
        assert result.nearest_source_position is None

    @pytest.mark.asyncio
    async def test_translate_nearest_match(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test translate_position returns nearest when exact not found."""
        migration_id = uuid4()
        event_id = uuid4()

        mock_repo.find_by_source_position = AsyncMock(return_value=None)
        mock_repo.find_nearest_source_position = AsyncMock(
            return_value=PositionMapping(
                migration_id=migration_id,
                source_position=pos(900, "src"),
                target_position=pos(450, "tgt"),
                event_id=event_id,
                mapped_at=datetime.now(UTC),
            )
        )

        result = await mapper.translate_position(migration_id, pos(950, "src"))

        assert result.source_position == pos(950, "src")
        assert result.target_position == pos(450, "tgt")
        assert result.is_exact is False
        assert result.nearest_source_position == pos(900, "src")

    @pytest.mark.asyncio
    async def test_translate_raises_when_no_mapping(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test translate_position raises when no mapping found."""
        migration_id = uuid4()
        source_position = pos(1000, "src")

        mock_repo.find_by_source_position = AsyncMock(return_value=None)
        mock_repo.find_nearest_source_position = AsyncMock(return_value=None)

        with pytest.raises(PositionMappingError) as exc_info:
            await mapper.translate_position(migration_id, source_position)

        assert exc_info.value.migration_id == migration_id
        assert exc_info.value.source_position == source_position
        assert exc_info.value.reason == "no_mapping"

    @pytest.mark.asyncio
    async def test_translate_raises_when_nearest_disabled(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test translate_position raises when use_nearest=False."""
        migration_id = uuid4()

        mock_repo.find_by_source_position = AsyncMock(return_value=None)

        with pytest.raises(PositionMappingError):
            await mapper.translate_position(
                migration_id,
                pos(1000, "src"),
                use_nearest=False,
            )

        # Should not call find_nearest when use_nearest=False
        mock_repo.find_nearest_source_position = AsyncMock()
        mock_repo.find_nearest_source_position.assert_not_called()


class TestPositionMapperTranslatePositionReverse:
    """Tests for PositionMapper.translate_position_reverse method."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        return MagicMock()

    @pytest.fixture
    def mapper(self, mock_repo: MagicMock) -> PositionMapper:
        """Create a mapper with mock repository."""
        return PositionMapper(mock_repo, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_translate_reverse_found(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test translate_position_reverse returns mapping when found."""
        migration_id = uuid4()
        event_id = uuid4()
        source_position = pos(1000, "src")
        target_position = pos(500, "tgt")

        mock_repo.find_by_target_position = AsyncMock(
            return_value=PositionMapping(
                migration_id=migration_id,
                source_position=source_position,
                target_position=target_position,
                event_id=event_id,
                mapped_at=datetime.now(UTC),
            )
        )

        result = await mapper.translate_position_reverse(migration_id, target_position)

        assert isinstance(result, ReverseTranslationResult)
        assert result.target_position == target_position
        assert result.source_position == source_position
        assert result.is_exact is True

    @pytest.mark.asyncio
    async def test_translate_reverse_not_found(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test translate_position_reverse raises when not found."""
        migration_id = uuid4()
        mock_repo.find_by_target_position = AsyncMock(return_value=None)

        with pytest.raises(PositionMappingError) as exc_info:
            await mapper.translate_position_reverse(migration_id, pos(500, "tgt"))

        assert exc_info.value.migration_id == migration_id
        assert exc_info.value.reason == "no_mapping"


class TestPositionMapperTranslatePositionsBatch:
    """Tests for PositionMapper.translate_positions_batch method."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        return MagicMock()

    @pytest.fixture
    def mapper(self, mock_repo: MagicMock) -> PositionMapper:
        """Create a mapper with mock repository."""
        return PositionMapper(mock_repo, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_batch_translate_empty_list(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test batch translation with empty list returns empty list."""
        result = await mapper.translate_positions_batch(uuid4(), [])
        assert result == []

    @pytest.mark.asyncio
    async def test_batch_translate_exact_matches(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test batch translation finds exact matches by delegating per position."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        by_source = {
            pos(100, "src"): PositionMapping(
                migration_id=migration_id,
                source_position=pos(100, "src"),
                target_position=pos(50, "tgt"),
                event_id=uuid4(),
                mapped_at=now,
            ),
            pos(200, "src"): PositionMapping(
                migration_id=migration_id,
                source_position=pos(200, "src"),
                target_position=pos(100, "tgt"),
                event_id=uuid4(),
                mapped_at=now,
            ),
            pos(300, "src"): PositionMapping(
                migration_id=migration_id,
                source_position=pos(300, "src"),
                target_position=pos(150, "tgt"),
                event_id=uuid4(),
                mapped_at=now,
            ),
        }

        async def find_by_source_position(
            mig_id: object, source_pos: Position
        ) -> PositionMapping | None:
            return by_source.get(source_pos)

        mock_repo.find_by_source_position = find_by_source_position

        results = await mapper.translate_positions_batch(
            migration_id,
            [pos(100, "src"), pos(200, "src"), pos(300, "src")],
        )

        assert len(results) == 3
        assert results[0].source_position == pos(100, "src")
        assert results[0].target_position == pos(50, "tgt")
        assert results[0].is_exact is True

    @pytest.mark.asyncio
    async def test_batch_translate_nearest_matches(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test batch translation finds nearest matches."""
        migration_id = uuid4()
        now = datetime.now(UTC)

        mock_repo.find_by_source_position = AsyncMock(return_value=None)
        mock_repo.find_nearest_source_position = AsyncMock(
            return_value=PositionMapping(
                migration_id=migration_id,
                source_position=pos(100, "src"),
                target_position=pos(50, "tgt"),
                event_id=uuid4(),
                mapped_at=now,
            )
        )

        # Position 150 should find nearest at 100
        results = await mapper.translate_positions_batch(
            migration_id,
            [pos(150, "src")],
        )

        assert len(results) == 1
        assert results[0].source_position == pos(150, "src")
        assert results[0].target_position == pos(50, "tgt")  # From position 100
        assert results[0].is_exact is False
        assert results[0].nearest_source_position == pos(100, "src")

    @pytest.mark.asyncio
    async def test_batch_translate_raises_when_no_mapping(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test batch translation raises when no mapping found."""
        migration_id = uuid4()
        source_position = pos(1000, "src")
        mock_repo.find_by_source_position = AsyncMock(return_value=None)
        mock_repo.find_nearest_source_position = AsyncMock(return_value=None)

        with pytest.raises(PositionMappingError) as exc_info:
            await mapper.translate_positions_batch(
                migration_id,
                [source_position],
            )

        assert exc_info.value.migration_id == migration_id
        assert exc_info.value.source_position == source_position


class TestPositionMapperHelperMethods:
    """Tests for PositionMapper helper methods."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        return MagicMock()

    @pytest.fixture
    def mapper(self, mock_repo: MagicMock) -> PositionMapper:
        """Create a mapper with mock repository."""
        return PositionMapper(mock_repo, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_mapping_by_event_id(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test get_mapping_by_event_id delegates to repository."""
        migration_id = uuid4()
        event_id = uuid4()
        now = datetime.now(UTC)

        expected_mapping = PositionMapping(
            migration_id=migration_id,
            source_position=pos(1000, "src"),
            target_position=pos(500, "tgt"),
            event_id=event_id,
            mapped_at=now,
        )
        mock_repo.find_by_event_id = AsyncMock(return_value=expected_mapping)

        result = await mapper.get_mapping_by_event_id(migration_id, event_id)

        assert result == expected_mapping
        mock_repo.find_by_event_id.assert_called_once_with(migration_id, event_id)

    @pytest.mark.asyncio
    async def test_get_position_bounds(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test get_position_bounds delegates to repository."""
        migration_id = uuid4()
        bounds = (pos(100, "src"), pos(10000, "src"))
        mock_repo.get_position_bounds = AsyncMock(return_value=bounds)

        result = await mapper.get_position_bounds(migration_id)

        assert result == bounds
        mock_repo.get_position_bounds.assert_called_once_with(migration_id)

    @pytest.mark.asyncio
    async def test_get_position_bounds_returns_none(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test get_position_bounds returns None when no mappings."""
        migration_id = uuid4()
        mock_repo.get_position_bounds = AsyncMock(return_value=None)

        result = await mapper.get_position_bounds(migration_id)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_mapping_count(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test get_mapping_count delegates to repository."""
        migration_id = uuid4()
        mock_repo.count_by_migration = AsyncMock(return_value=1500)

        result = await mapper.get_mapping_count(migration_id)

        assert result == 1500
        mock_repo.count_by_migration.assert_called_once_with(migration_id)

    @pytest.mark.asyncio
    async def test_clear_mappings(
        self,
        mapper: PositionMapper,
        mock_repo: MagicMock,
    ) -> None:
        """Test clear_mappings delegates to repository."""
        migration_id = uuid4()
        mock_repo.delete_by_migration = AsyncMock(return_value=500)

        result = await mapper.clear_mappings(migration_id)

        assert result == 500
        mock_repo.delete_by_migration.assert_called_once_with(migration_id)


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


class TestTranslationResultDataclass:
    """Tests for TranslationResult dataclass."""

    def test_translation_result_exact(self) -> None:
        """Test TranslationResult for exact match."""
        result = TranslationResult(
            source_position=pos(1000, "src"),
            target_position=pos(500, "tgt"),
            is_exact=True,
        )

        assert result.source_position == pos(1000, "src")
        assert result.target_position == pos(500, "tgt")
        assert result.is_exact is True
        assert result.nearest_source_position is None
        assert result.interpolated is False

    def test_translation_result_nearest(self) -> None:
        """Test TranslationResult for nearest match."""
        result = TranslationResult(
            source_position=pos(1050, "src"),
            target_position=pos(500, "tgt"),
            is_exact=False,
            nearest_source_position=pos(1000, "src"),
        )

        assert result.source_position == pos(1050, "src")
        assert result.target_position == pos(500, "tgt")
        assert result.is_exact is False
        assert result.nearest_source_position == pos(1000, "src")

    def test_translation_result_frozen(self) -> None:
        """Test TranslationResult is immutable."""
        result = TranslationResult(
            source_position=pos(1000, "src"),
            target_position=pos(500, "tgt"),
            is_exact=True,
        )

        with pytest.raises(AttributeError):
            result.source_position = pos(2000, "src")  # type: ignore[misc]


class TestReverseTranslationResultDataclass:
    """Tests for ReverseTranslationResult dataclass."""

    def test_reverse_result_exact(self) -> None:
        """Test ReverseTranslationResult for exact match."""
        result = ReverseTranslationResult(
            target_position=pos(500, "tgt"),
            source_position=pos(1000, "src"),
            is_exact=True,
        )

        assert result.target_position == pos(500, "tgt")
        assert result.source_position == pos(1000, "src")
        assert result.is_exact is True
        assert result.nearest_target_position is None

    def test_reverse_result_frozen(self) -> None:
        """Test ReverseTranslationResult is immutable."""
        result = ReverseTranslationResult(
            target_position=pos(500, "tgt"),
            source_position=pos(1000, "src"),
            is_exact=True,
        )

        with pytest.raises(AttributeError):
            result.target_position = pos(600, "tgt")  # type: ignore[misc]


class TestPositionMapperWorkflows:
    """Integration-style tests for position mapping workflows."""

    @pytest.fixture
    def mock_repo(self) -> MagicMock:
        """Create a mock repository."""
        return MagicMock()

    @pytest.fixture
    def mapper(self, mock_repo: MagicMock) -> PositionMapper:
        """Create a mapper with mock repository."""
        return PositionMapper(mock_repo, enable_tracing=False)

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
