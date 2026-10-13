"""Tests for PositionMapping dataclass."""

from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position


class TestPositionMapping:
    """Tests for PositionMapping dataclass."""

    def test_creation(self) -> None:
        """Test creating a PositionMapping."""
        migration_id = uuid4()
        event_id = uuid4()
        mapped_at = datetime.now()
        source_position = Position(store_id="src", key=(100,))
        target_position = Position(store_id="tgt", key=(50,))
        mapping = PositionMapping(
            migration_id=migration_id,
            source_position=source_position,
            target_position=target_position,
            event_id=event_id,
            mapped_at=mapped_at,
        )
        assert mapping.migration_id == migration_id
        assert mapping.source_position == source_position
        assert mapping.target_position == target_position
        assert mapping.event_id == event_id
        assert mapping.mapped_at == mapped_at

    def test_is_frozen(self) -> None:
        """Test that PositionMapping is frozen (immutable)."""
        mapping = PositionMapping(
            migration_id=uuid4(),
            source_position=Position(store_id="src", key=(100,)),
            target_position=Position(store_id="tgt", key=(50,)),
            event_id=uuid4(),
            mapped_at=datetime.now(),
        )
        with pytest.raises(AttributeError):
            mapping.source_position = Position(store_id="src", key=(200,))  # type: ignore[misc]
