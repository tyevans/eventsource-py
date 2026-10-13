"""Position translation tests during checkpoint migration for Phase 3 integration."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.application.migration.position_mapper import PositionMapper
from tests.unit.application.migration.test_phase3_integration._events import (
    source_pos,
    target_pos,
)


class TestPositionTranslation:
    """Tests for position translation during checkpoint migration."""

    @pytest.mark.asyncio
    async def test_exact_position_translation(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test exact position translation."""
        # Record mappings
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )

        result = await position_mapper.translate_position(
            migration_id=migration_id,
            source_position=source_pos(100),
        )

        assert result.target_position == target_pos(50)
        assert result.is_exact is True
        assert result.nearest_source_position is None

    @pytest.mark.asyncio
    async def test_nearest_position_translation(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test nearest position translation when exact match not found."""
        # Record mappings
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(200),
            target_position=target_pos(100),
            event_id=uuid4(),
        )

        # Query position between recorded mappings
        result = await position_mapper.translate_position(
            migration_id=migration_id,
            source_position=source_pos(150),
        )

        assert result.target_position == target_pos(50)  # Nearest lower position
        assert result.is_exact is False
        assert result.nearest_source_position == source_pos(100)

    @pytest.mark.asyncio
    async def test_translation_without_nearest_fails(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test that translation fails when no mapping exists and use_nearest=False."""
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )

        # Query different position without nearest fallback
        with pytest.raises(PositionMappingError):
            await position_mapper.translate_position(
                migration_id=migration_id,
                source_position=source_pos(150),
                use_nearest=False,
            )

    @pytest.mark.asyncio
    async def test_batch_position_translation(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test batch position translation."""
        # Record mappings
        mappings = [
            (source_pos(100), target_pos(50), uuid4()),
            (source_pos(200), target_pos(100), uuid4()),
            (source_pos(300), target_pos(150), uuid4()),
        ]
        await position_mapper.record_mappings_batch(migration_id, mappings)

        # Batch translate
        results = await position_mapper.translate_positions_batch(
            migration_id=migration_id,
            source_positions=[source_pos(100), source_pos(200), source_pos(300)],
        )

        assert len(results) == 3
        assert results[0].target_position == target_pos(50)
        assert results[1].target_position == target_pos(100)
        assert results[2].target_position == target_pos(150)
        assert all(r.is_exact for r in results)
