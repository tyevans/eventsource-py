"""Unit tests for PositionMapper translation methods and translation dataclasses."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.application.migration.position_mapper import (
    ReverseTranslationResult,
    TranslationResult,
)
from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position

from ._fixtures import PositionMapperTestBase, pos


def _mapping(mig_id: UUID, src: int, tgt: int, event_id: UUID | None = None) -> PositionMapping:
    return PositionMapping(
        migration_id=mig_id,
        source_position=pos(src, "src"),
        target_position=pos(tgt, "tgt"),
        event_id=event_id or uuid4(),
        mapped_at=datetime.now(UTC),
    )


class TestPositionMapperTranslatePosition(PositionMapperTestBase):
    """Tests for PositionMapper.translate_position method."""

    @pytest.mark.asyncio
    async def test_translate_exact_match(
        self, mapper: PositionMapperTestBase, mock_repo: MagicMock
    ) -> None:
        migration_id = uuid4()
        source_position = pos(1000, "src")
        target_position = pos(500, "tgt")
        mock_repo.find_by_source_position = AsyncMock(
            return_value=_mapping(migration_id, 1000, 500)
        )
        result = await mapper.translate_position(migration_id, source_position)
        assert isinstance(result, TranslationResult)
        assert result.source_position == source_position
        assert result.target_position == target_position
        assert result.is_exact is True
        assert result.nearest_source_position is None

    @pytest.mark.asyncio
    async def test_translate_nearest_match(
        self, mapper: PositionMapperTestBase, mock_repo: MagicMock
    ) -> None:
        migration_id = uuid4()
        mock_repo.find_by_source_position = AsyncMock(return_value=None)
        mock_repo.find_nearest_source_position = AsyncMock(
            return_value=_mapping(migration_id, 900, 450)
        )
        result = await mapper.translate_position(migration_id, pos(950, "src"))
        assert result.source_position == pos(950, "src")
        assert result.target_position == pos(450, "tgt")
        assert result.is_exact is False
        assert result.nearest_source_position == pos(900, "src")

    @pytest.mark.asyncio
    async def test_translate_raises_when_no_mapping(
        self, mapper: PositionMapperTestBase, mock_repo: MagicMock
    ) -> None:
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
        self, mapper: PositionMapperTestBase, mock_repo: MagicMock
    ) -> None:
        migration_id = uuid4()
        mock_repo.find_by_source_position = AsyncMock(return_value=None)
        with pytest.raises(PositionMappingError):
            await mapper.translate_position(migration_id, pos(1000, "src"), use_nearest=False)
        mock_repo.find_nearest_source_position = AsyncMock()
        mock_repo.find_nearest_source_position.assert_not_called()


class TestPositionMapperTranslatePositionReverse(PositionMapperTestBase):
    """Tests for PositionMapper.translate_position_reverse method."""

    @pytest.mark.asyncio
    async def test_translate_reverse_found(
        self, mapper: PositionMapperTestBase, mock_repo: MagicMock
    ) -> None:
        migration_id = uuid4()
        source_position = pos(1000, "src")
        target_position = pos(500, "tgt")
        mock_repo.find_by_target_position = AsyncMock(
            return_value=_mapping(migration_id, 1000, 500)
        )
        result = await mapper.translate_position_reverse(migration_id, target_position)
        assert isinstance(result, ReverseTranslationResult)
        assert result.target_position == target_position
        assert result.source_position == source_position
        assert result.is_exact is True

    @pytest.mark.asyncio
    async def test_translate_reverse_not_found(
        self, mapper: PositionMapperTestBase, mock_repo: MagicMock
    ) -> None:
        migration_id = uuid4()
        mock_repo.find_by_target_position = AsyncMock(return_value=None)
        with pytest.raises(PositionMappingError) as exc_info:
            await mapper.translate_position_reverse(migration_id, pos(500, "tgt"))
        assert exc_info.value.migration_id == migration_id
        assert exc_info.value.reason == "no_mapping"


class TestPositionMapperTranslatePositionsBatch(PositionMapperTestBase):
    """Tests for PositionMapper.translate_positions_batch method."""

    @pytest.mark.asyncio
    async def test_batch_translate_empty_list(
        self, mapper: PositionMapperTestBase, mock_repo: MagicMock
    ) -> None:
        result = await mapper.translate_positions_batch(uuid4(), [])
        assert result == []

    @pytest.mark.asyncio
    async def test_batch_translate_exact_matches(
        self, mapper: PositionMapperTestBase, mock_repo: MagicMock
    ) -> None:
        migration_id = uuid4()
        by_source = {
            pos(100, "src"): _mapping(migration_id, 100, 50),
            pos(200, "src"): _mapping(migration_id, 200, 100),
            pos(300, "src"): _mapping(migration_id, 300, 150),
        }

        async def find_by_source_position(
            mig_id: object, source_pos: Position
        ) -> PositionMapping | None:
            return by_source.get(source_pos)

        mock_repo.find_by_source_position = find_by_source_position
        results = await mapper.translate_positions_batch(
            migration_id, [pos(100, "src"), pos(200, "src"), pos(300, "src")]
        )
        assert len(results) == 3
        assert results[0].source_position == pos(100, "src")
        assert results[0].target_position == pos(50, "tgt")
        assert results[0].is_exact is True

    @pytest.mark.asyncio
    async def test_batch_translate_nearest_matches(
        self, mapper: PositionMapperTestBase, mock_repo: MagicMock
    ) -> None:
        migration_id = uuid4()
        mock_repo.find_by_source_position = AsyncMock(return_value=None)
        mock_repo.find_nearest_source_position = AsyncMock(
            return_value=_mapping(migration_id, 100, 50)
        )
        results = await mapper.translate_positions_batch(migration_id, [pos(150, "src")])
        assert len(results) == 1
        assert results[0].source_position == pos(150, "src")
        assert results[0].target_position == pos(50, "tgt")
        assert results[0].is_exact is False
        assert results[0].nearest_source_position == pos(100, "src")

    @pytest.mark.asyncio
    async def test_batch_translate_raises_when_no_mapping(
        self, mapper: PositionMapperTestBase, mock_repo: MagicMock
    ) -> None:
        migration_id = uuid4()
        source_position = pos(1000, "src")
        mock_repo.find_by_source_position = AsyncMock(return_value=None)
        mock_repo.find_nearest_source_position = AsyncMock(return_value=None)
        with pytest.raises(PositionMappingError) as exc_info:
            await mapper.translate_positions_batch(migration_id, [source_position])
        assert exc_info.value.migration_id == migration_id
        assert exc_info.value.source_position == source_position


class TestTranslationResultDataclass:
    """Tests for TranslationResult dataclass."""

    def test_translation_result_exact(self) -> None:
        result = TranslationResult(
            source_position=pos(1000, "src"), target_position=pos(500, "tgt"), is_exact=True
        )
        assert result.source_position == pos(1000, "src")
        assert result.target_position == pos(500, "tgt")
        assert result.is_exact is True
        assert result.nearest_source_position is None
        assert result.interpolated is False

    def test_translation_result_nearest(self) -> None:
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
        result = TranslationResult(
            source_position=pos(1000, "src"), target_position=pos(500, "tgt"), is_exact=True
        )
        with pytest.raises(AttributeError):
            result.source_position = pos(2000, "src")  # type: ignore[misc]


class TestReverseTranslationResultDataclass:
    """Tests for ReverseTranslationResult dataclass."""

    def test_reverse_result_exact(self) -> None:
        result = ReverseTranslationResult(
            target_position=pos(500, "tgt"), source_position=pos(1000, "src"), is_exact=True
        )
        assert result.target_position == pos(500, "tgt")
        assert result.source_position == pos(1000, "src")
        assert result.is_exact is True
        assert result.nearest_target_position is None

    def test_reverse_result_frozen(self) -> None:
        result = ReverseTranslationResult(
            target_position=pos(500, "tgt"), source_position=pos(1000, "src"), is_exact=True
        )
        with pytest.raises(AttributeError):
            result.target_position = pos(600, "tgt")  # type: ignore[misc]
