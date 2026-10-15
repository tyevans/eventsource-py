"""Tests for EventAssertions initialization, properties, and container dunders."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.domain.event import DomainEvent
from eventsource.testing.assertions import EventAssertions
from tests.unit.testing.test_assertions._fixtures import (
    SampleCreated,
    SampleDeleted,
    SampleUpdated,
)


class TestEventAssertionsInit:
    """Tests for EventAssertions initialization and container dunders."""

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
        events = assertions.events
        events.clear()
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

    def test_iter_and_getitem(self) -> None:
        """Test iterating and indexing EventAssertions."""
        agg_id = uuid4()
        ev1 = SampleCreated(
            aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1, name="a"
        )
        ev2 = SampleUpdated(
            aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=2, new_value=2
        )
        assertions = EventAssertions([ev1, ev2])
        assert list(iter(assertions)) == [ev1, ev2]
        assert list(assertions) == [ev1, ev2]
        assert assertions[0] == ev1
        assert assertions[1] == ev2
        assert assertions[-1] == ev2
        with pytest.raises(IndexError):
            _ = assertions[2]

    def test_contains_event_type_instance_and_other(self) -> None:
        """Test membership checks by DomainEvent subclass, event instance, and non-event object."""
        agg_id = uuid4()
        ev1 = SampleCreated(
            aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1, name="a"
        )
        ev2 = SampleUpdated(
            aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=2, new_value=2
        )
        other_ev = SampleCreated(
            aggregate_id=uuid4(), aggregate_type="Sample", aggregate_version=1, name="x"
        )
        assertions = EventAssertions([ev1, ev2])
        assert SampleCreated in assertions
        assert SampleUpdated in assertions
        assert DomainEvent in assertions
        assert SampleDeleted not in assertions
        assert ev1 in assertions
        assert ev2 in assertions
        assert other_ev not in assertions
        assert "not-an-event" not in assertions
        assert int not in assertions
