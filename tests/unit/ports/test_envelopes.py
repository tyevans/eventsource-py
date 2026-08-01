"""Tests for envelope and read-option value objects."""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    FeedReadOptions,
    Position,
    ReadDirection,
    StreamReadOptions,
)


class ThingHappened(DomainEvent):
    """Test event."""

    aggregate_type: str = "Thing"


def make_envelope(position: Position | None) -> EventEnvelope:
    """Create an EventEnvelope with optional position."""
    return EventEnvelope(
        event=ThingHappened(aggregate_id=uuid4()),
        stream_id=StreamId(aggregate_id=uuid4(), category="Thing"),
        stream_version=1,
        position=position,
        stored_at=datetime.now(UTC),
    )


class TestEnvelopes:
    """Tests for EventEnvelope value object."""

    def test_position_may_be_none_for_feedless_stores(self) -> None:
        """EventEnvelope can have None position for stores without global feed."""
        assert make_envelope(None).position is None

    def test_envelope_frozen(self) -> None:
        """EventEnvelope is frozen and cannot be mutated."""
        env = make_envelope(None)
        with pytest.raises(FrozenInstanceError):
            env.stream_version = 2  # type: ignore[misc]

    def test_append_result_has_no_conflict_flags(self) -> None:
        """AppendResult does not have success or conflict attributes."""
        result = AppendResult(
            stream=StreamId(aggregate_id=uuid4(), category="Thing"),
            new_version=1,
            position=None,
        )
        assert not hasattr(result, "success")
        assert not hasattr(result, "conflict")

    def test_option_defaults(self) -> None:
        """Read option classes have correct default values."""
        assert StreamReadOptions().direction is ReadDirection.FORWARD
        assert FeedReadOptions().tenant_id is None
        assert CategoryReadOptions().tenant_id is None
