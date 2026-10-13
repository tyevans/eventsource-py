"""Position mapping recording tests during bulk copy for Phase 3 integration."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.position_mapper import PositionMapper
from tests.unit.application.migration.test_phase3_integration._events import (
    source_pos,
    target_pos,
)
from tests.unit.application.migration.test_phase3_integration._helpers import (
    copy_events_with_position_mapping,
    create_test_events,
)


class TestPositionMappingRecording:
    """Tests for position mapping recording during bulk copy."""

    @pytest.mark.asyncio
    async def test_record_single_mapping(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test recording a single position mapping."""
        event_id = uuid4()

        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=event_id,
        )

        # Verify mapping was recorded
        result = await position_mapper.translate_position(
            migration_id=migration_id,
            source_position=source_pos(100),
        )

        assert result.target_position == target_pos(50)
        assert result.is_exact is True
        assert result.source_position == source_pos(100)

    @pytest.mark.asyncio
    async def test_record_batch_mappings(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test recording multiple position mappings in batch."""
        mappings = [
            (source_pos(100), target_pos(50), uuid4()),
            (source_pos(200), target_pos(100), uuid4()),
            (source_pos(300), target_pos(150), uuid4()),
        ]

        count = await position_mapper.record_mappings_batch(
            migration_id=migration_id,
            mappings=mappings,
        )

        assert count == 3

        # Verify all mappings were recorded
        for sp, tp, _ in mappings:
            result = await position_mapper.translate_position(
                migration_id=migration_id,
                source_position=sp,
            )
            assert result.target_position == tp
            assert result.is_exact is True

    @pytest.mark.asyncio
    async def test_position_mapping_during_bulk_copy(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        position_mapper: PositionMapper,
        tenant_id: UUID,
        migration_id: UUID,
    ) -> None:
        """Test position mapping during actual bulk copy operation."""
        # Create events in source
        await create_test_events(source_store, tenant_id, count=5)

        # Copy events with position mapping
        copied_count = await copy_events_with_position_mapping(
            source_store,
            target_store,
            tenant_id,
            migration_id,
            position_mapper,
        )

        assert copied_count == 5

        # Verify mappings were recorded
        count = await position_mapper.get_mapping_count(migration_id)
        assert count == 5
