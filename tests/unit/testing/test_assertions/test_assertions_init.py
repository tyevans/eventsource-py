"""Tests for EventAssertions initialization and properties."""

from __future__ import annotations

from uuid import uuid4

from eventsource.testing.assertions import EventAssertions
from tests.unit.testing.test_assertions._fixtures import SampleCreated


class TestEventAssertionsInit:
    """Tests for EventAssertions initialization."""

    def test_initializes_with_empty_list(self) -> None:
        """Test initialization with empty event list."""
        assertions = EventAssertions([])
        assert assertions.events == []

    def test_initializes_with_events(self) -> None:
        """Test initialization with events."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="test",
        )
        assertions = EventAssertions([event])
        assert len(assertions.events) == 1
        assert assertions.events[0] == event

    def test_events_property_returns_copy(self) -> None:
        """Test that events property returns a copy, not the original list."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="test",
        )
        assertions = EventAssertions([event])

        # Get events and modify the returned list
        events = assertions.events
        events.clear()

        # Original should be unchanged
        assert len(assertions.events) == 1

    def test_repr(self) -> None:
        """Test string representation."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="test",
        )
        assertions = EventAssertions([event])

        repr_str = repr(assertions)
        assert "EventAssertions" in repr_str
        assert "SampleCreated" in repr_str
