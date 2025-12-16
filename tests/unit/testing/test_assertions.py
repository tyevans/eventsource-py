"""
Unit tests for EventAssertions class.

Tests the EventAssertions class and all its assertion methods.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.testing.assertions import EventAssertions

# =============================================================================
# Test Domain Events
# =============================================================================


class SampleCreated(DomainEvent):
    """Test event for sample creation."""

    aggregate_type: str = "Sample"
    event_type: str = "SampleCreated"
    name: str


class SampleUpdated(DomainEvent):
    """Test event for sample update."""

    aggregate_type: str = "Sample"
    event_type: str = "SampleUpdated"
    new_value: int


class SampleDeleted(DomainEvent):
    """Test event for sample deletion."""

    aggregate_type: str = "Sample"
    event_type: str = "SampleDeleted"


class OtherEvent(DomainEvent):
    """Another test event type."""

    aggregate_type: str = "Other"
    event_type: str = "OtherEvent"
    data: str


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def empty_assertions() -> EventAssertions:
    """Create assertions with no events."""
    return EventAssertions([])


@pytest.fixture
def single_event_assertions() -> EventAssertions:
    """Create assertions with a single event."""
    event = SampleCreated(
        aggregate_id=uuid4(),
        aggregate_type="Sample",
        aggregate_version=1,
        name="test-sample",
    )
    return EventAssertions([event])


@pytest.fixture
def multi_event_assertions() -> EventAssertions:
    """Create assertions with multiple events."""
    agg_id = uuid4()
    events = [
        SampleCreated(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
            name="test-sample",
        ),
        SampleUpdated(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=2,
            new_value=42,
        ),
        SampleDeleted(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=3,
        ),
    ]
    return EventAssertions(events)


# =============================================================================
# Tests for __init__ and properties
# =============================================================================


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


# =============================================================================
# Tests for assert_event_published
# =============================================================================


class TestAssertEventPublished:
    """Tests for assert_event_published method."""

    def test_finds_event_by_type(self, single_event_assertions: EventAssertions) -> None:
        """Test finding event by type."""
        event = single_event_assertions.assert_event_published(SampleCreated)
        assert isinstance(event, SampleCreated)

    def test_returns_first_matching_event(self) -> None:
        """Test returns first event when multiple match."""
        event1 = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="first",
        )
        event2 = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="second",
        )
        assertions = EventAssertions([event1, event2])

        found = assertions.assert_event_published(SampleCreated)
        assert found.name == "first"

    def test_raises_when_no_matching_type(self, single_event_assertions: EventAssertions) -> None:
        """Test raises when no event of type found."""
        with pytest.raises(AssertionError) as exc_info:
            single_event_assertions.assert_event_published(SampleUpdated)

        assert "SampleUpdated" in str(exc_info.value)
        assert "no events of that type were found" in str(exc_info.value)

    def test_raises_when_no_events(self, empty_assertions: EventAssertions) -> None:
        """Test raises when no events at all."""
        with pytest.raises(AssertionError) as exc_info:
            empty_assertions.assert_event_published(SampleCreated)

        assert "none" in str(exc_info.value)


# =============================================================================
# Tests for assert_no_event_published
# =============================================================================


class TestAssertNoEventPublished:
    """Tests for assert_no_event_published method."""

    def test_passes_when_no_matching_type(self, single_event_assertions: EventAssertions) -> None:
        """Test passes when no events of specified type."""
        # single_event_assertions has SampleCreated, not SampleUpdated
        single_event_assertions.assert_no_event_published(SampleUpdated)
        # Should not raise

    def test_passes_when_no_events(self, empty_assertions: EventAssertions) -> None:
        """Test passes when no events at all."""
        empty_assertions.assert_no_event_published(SampleCreated)
        # Should not raise

    def test_raises_when_matching_type_exists(
        self, single_event_assertions: EventAssertions
    ) -> None:
        """Test raises when events of specified type exist."""
        with pytest.raises(AssertionError) as exc_info:
            single_event_assertions.assert_no_event_published(SampleCreated)

        assert "Expected no SampleCreated" in str(exc_info.value)


# =============================================================================
# Tests for assert_event_count
# =============================================================================


class TestAssertEventCount:
    """Tests for assert_event_count method."""

    def test_passes_for_correct_count(self, multi_event_assertions: EventAssertions) -> None:
        """Test passes when count matches."""
        multi_event_assertions.assert_event_count(3)
        # Should not raise

    def test_raises_for_wrong_count(self, multi_event_assertions: EventAssertions) -> None:
        """Test raises when count doesn't match."""
        with pytest.raises(AssertionError) as exc_info:
            multi_event_assertions.assert_event_count(5)

        assert "expected 5 events" in str(exc_info.value)
        assert "got 3" in str(exc_info.value)

    def test_zero_count_passes_when_empty(self, empty_assertions: EventAssertions) -> None:
        """Test count 0 passes when no events."""
        empty_assertions.assert_event_count(0)
        # Should not raise

    def test_error_includes_event_types(self, multi_event_assertions: EventAssertions) -> None:
        """Test error message includes event types."""
        with pytest.raises(AssertionError) as exc_info:
            multi_event_assertions.assert_event_count(10)

        error_msg = str(exc_info.value)
        assert "SampleCreated" in error_msg
        assert "SampleUpdated" in error_msg
        assert "SampleDeleted" in error_msg


# =============================================================================
# Tests for assert_event_sequence
# =============================================================================


class TestAssertEventSequence:
    """Tests for assert_event_sequence method."""

    def test_passes_for_correct_sequence(self, multi_event_assertions: EventAssertions) -> None:
        """Test passes when sequence matches."""
        multi_event_assertions.assert_event_sequence(
            [
                SampleCreated,
                SampleUpdated,
                SampleDeleted,
            ]
        )
        # Should not raise

    def test_raises_for_wrong_count(self, multi_event_assertions: EventAssertions) -> None:
        """Test raises when count differs."""
        with pytest.raises(AssertionError) as exc_info:
            multi_event_assertions.assert_event_sequence(
                [
                    SampleCreated,
                    SampleUpdated,
                ]
            )

        assert "expected 2 events" in str(exc_info.value)
        assert "got 3" in str(exc_info.value)

    def test_raises_for_wrong_order(self, multi_event_assertions: EventAssertions) -> None:
        """Test raises when order differs."""
        with pytest.raises(AssertionError) as exc_info:
            multi_event_assertions.assert_event_sequence(
                [
                    SampleUpdated,  # Wrong - should be SampleCreated
                    SampleCreated,
                    SampleDeleted,
                ]
            )

        assert "position 0" in str(exc_info.value)
        assert "expected SampleUpdated" in str(exc_info.value)
        assert "got SampleCreated" in str(exc_info.value)

    def test_empty_sequence_passes_when_empty(self, empty_assertions: EventAssertions) -> None:
        """Test empty sequence passes when no events."""
        empty_assertions.assert_event_sequence([])
        # Should not raise

    def test_includes_full_sequence_in_error(self, multi_event_assertions: EventAssertions) -> None:
        """Test error includes full expected and actual sequences."""
        with pytest.raises(AssertionError) as exc_info:
            multi_event_assertions.assert_event_sequence(
                [
                    SampleCreated,
                    SampleDeleted,  # Wrong order
                    SampleUpdated,
                ]
            )

        error_msg = str(exc_info.value)
        assert "Full sequence:" in error_msg
        assert "Expected:" in error_msg
        assert "Actual:" in error_msg


# =============================================================================
# Tests for assert_event_with_fields
# =============================================================================


class TestAssertEventWithFields:
    """Tests for assert_event_with_fields method."""

    def test_finds_event_without_field_constraints(
        self, single_event_assertions: EventAssertions
    ) -> None:
        """Test finds event when no fields specified."""
        event = single_event_assertions.assert_event_with_fields(SampleCreated)
        assert isinstance(event, SampleCreated)

    def test_finds_event_with_matching_fields(self) -> None:
        """Test finds event with matching field values."""
        agg_id = uuid4()
        event = SampleCreated(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
            name="specific-name",
        )
        assertions = EventAssertions([event])

        found = assertions.assert_event_with_fields(
            SampleCreated,
            name="specific-name",
            aggregate_id=agg_id,
        )
        assert found == event

    def test_finds_correct_event_among_multiple(self) -> None:
        """Test finds correct event when multiple exist."""
        event1 = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="wrong-name",
        )
        event2 = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="correct-name",
        )
        assertions = EventAssertions([event1, event2])

        found = assertions.assert_event_with_fields(SampleCreated, name="correct-name")
        assert found == event2

    def test_raises_when_no_matching_type(self, single_event_assertions: EventAssertions) -> None:
        """Test raises when no event of type found."""
        with pytest.raises(AssertionError) as exc_info:
            single_event_assertions.assert_event_with_fields(
                SampleUpdated,
                new_value=42,
            )

        assert "SampleUpdated" in str(exc_info.value)

    def test_raises_when_fields_dont_match(self, single_event_assertions: EventAssertions) -> None:
        """Test raises when field values don't match."""
        with pytest.raises(AssertionError) as exc_info:
            single_event_assertions.assert_event_with_fields(
                SampleCreated,
                name="wrong-name",
            )

        assert "wrong-name" in str(exc_info.value)
        assert "SampleCreated" in str(exc_info.value)

    def test_raises_for_missing_attribute(self) -> None:
        """Test raises when specified attribute doesn't exist on event."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="test",
        )
        assertions = EventAssertions([event])

        with pytest.raises(AssertionError) as exc_info:
            assertions.assert_event_with_fields(
                SampleCreated,
                nonexistent_field="value",
            )

        # Should raise because field doesn't exist
        assert "SampleCreated" in str(exc_info.value)


# =============================================================================
# Tests for assert_no_events_published
# =============================================================================


class TestAssertNoEventsPublished:
    """Tests for assert_no_events_published method."""

    def test_passes_when_no_events(self, empty_assertions: EventAssertions) -> None:
        """Test passes when no events."""
        empty_assertions.assert_no_events_published()
        # Should not raise

    def test_raises_when_events_exist(self, single_event_assertions: EventAssertions) -> None:
        """Test raises when events exist."""
        with pytest.raises(AssertionError) as exc_info:
            single_event_assertions.assert_no_events_published()

        assert "Expected no events" in str(exc_info.value)
        assert "SampleCreated" in str(exc_info.value)


# =============================================================================
# Tests for assert_event_for_aggregate
# =============================================================================


class TestAssertEventForAggregate:
    """Tests for assert_event_for_aggregate method."""

    def test_finds_event_for_aggregate(self) -> None:
        """Test finds event for specific aggregate."""
        agg_id = uuid4()
        event = SampleCreated(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
            name="test",
        )
        assertions = EventAssertions([event])

        found = assertions.assert_event_for_aggregate(SampleCreated, agg_id)
        assert found == event

    def test_finds_correct_aggregate_among_multiple(self) -> None:
        """Test finds event for correct aggregate when multiple exist."""
        agg_id_1 = uuid4()
        agg_id_2 = uuid4()
        event1 = SampleCreated(
            aggregate_id=agg_id_1,
            aggregate_type="Sample",
            aggregate_version=1,
            name="first",
        )
        event2 = SampleCreated(
            aggregate_id=agg_id_2,
            aggregate_type="Sample",
            aggregate_version=1,
            name="second",
        )
        assertions = EventAssertions([event1, event2])

        found = assertions.assert_event_for_aggregate(SampleCreated, agg_id_2)
        assert found == event2
        assert found.name == "second"

    def test_raises_when_no_event_type(self, single_event_assertions: EventAssertions) -> None:
        """Test raises when no events of type exist."""
        with pytest.raises(AssertionError) as exc_info:
            single_event_assertions.assert_event_for_aggregate(
                SampleUpdated,
                uuid4(),
            )

        assert "SampleUpdated" in str(exc_info.value)
        assert "no events of that type were found" in str(exc_info.value)

    def test_raises_when_wrong_aggregate(self) -> None:
        """Test raises when event exists but for different aggregate."""
        agg_id_1 = uuid4()
        agg_id_2 = uuid4()
        event = SampleCreated(
            aggregate_id=agg_id_1,
            aggregate_type="Sample",
            aggregate_version=1,
            name="test",
        )
        assertions = EventAssertions([event])

        with pytest.raises(AssertionError) as exc_info:
            assertions.assert_event_for_aggregate(SampleCreated, agg_id_2)

        error_msg = str(exc_info.value)
        assert str(agg_id_2) in error_msg
        assert "only for aggregates" in error_msg


# =============================================================================
# Tests for get_events_of_type
# =============================================================================


class TestGetEventsOfType:
    """Tests for get_events_of_type method."""

    def test_returns_all_matching_events(self) -> None:
        """Test returns all events of specified type."""
        event1 = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="first",
        )
        event2 = SampleUpdated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=2,
            new_value=42,
        )
        event3 = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="third",
        )
        assertions = EventAssertions([event1, event2, event3])

        created_events = assertions.get_events_of_type(SampleCreated)

        assert len(created_events) == 2
        assert all(isinstance(e, SampleCreated) for e in created_events)
        assert created_events[0].name == "first"
        assert created_events[1].name == "third"

    def test_returns_empty_list_when_no_match(
        self, single_event_assertions: EventAssertions
    ) -> None:
        """Test returns empty list when no events of type."""
        events = single_event_assertions.get_events_of_type(SampleUpdated)
        assert events == []

    def test_returns_empty_list_when_no_events(self, empty_assertions: EventAssertions) -> None:
        """Test returns empty list when no events at all."""
        events = empty_assertions.get_events_of_type(SampleCreated)
        assert events == []


# =============================================================================
# Integration Tests
# =============================================================================


class TestEventAssertionsIntegration:
    """Integration tests for EventAssertions."""

    def test_chained_assertions(self) -> None:
        """Test using multiple assertions in sequence."""
        agg_id = uuid4()
        events: list[DomainEvent] = [
            SampleCreated(
                aggregate_id=agg_id,
                aggregate_type="Sample",
                aggregate_version=1,
                name="test-sample",
            ),
            SampleUpdated(
                aggregate_id=agg_id,
                aggregate_type="Sample",
                aggregate_version=2,
                new_value=42,
            ),
        ]
        assertions = EventAssertions(events)

        # Multiple assertions on same data
        assertions.assert_event_count(2)
        assertions.assert_event_sequence([SampleCreated, SampleUpdated])
        created = assertions.assert_event_published(SampleCreated)
        assert created.name == "test-sample"
        assertions.assert_no_event_published(SampleDeleted)

    def test_assertions_from_harness_published_events(self) -> None:
        """Test creating assertions from harness-like data structure."""
        # Simulate what would come from harness.published_events
        published_events: list[DomainEvent] = [
            SampleCreated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=1,
                name="from-harness",
            ),
        ]

        assertions = EventAssertions(published_events)
        event = assertions.assert_event_with_fields(SampleCreated, name="from-harness")
        assert event is not None
