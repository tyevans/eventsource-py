"""Tests for event filtering, aggregate matching, and field assertions."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.testing.assertions import EventAssertions
from tests.unit.testing.test_assertions._fixtures import (
    SampleCreated,
    SampleUpdated,
)


class TestAssertEventWithFields:
    """Tests for assert_event_with_fields method."""

    def test_finds_event_without_field_constraints(
        self, single_event_assertions: EventAssertions
    ) -> None:
        event = single_event_assertions.assert_event_with_fields(SampleCreated)
        assert isinstance(event, SampleCreated)

    def test_finds_event_with_matching_fields(self) -> None:
        agg_id = uuid4()
        event = SampleCreated(
            aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1, name="specific-name"
        )
        assertions = EventAssertions([event])
        found = assertions.assert_event_with_fields(
            SampleCreated, name="specific-name", aggregate_id=agg_id
        )
        assert found == event

    def test_finds_correct_event_among_multiple(self) -> None:
        event1 = SampleCreated(
            aggregate_id=uuid4(), aggregate_type="Sample", aggregate_version=1, name="wrong-name"
        )
        event2 = SampleCreated(
            aggregate_id=uuid4(), aggregate_type="Sample", aggregate_version=1, name="correct-name"
        )
        assertions = EventAssertions([event1, event2])
        assert assertions.assert_event_with_fields(SampleCreated, name="correct-name") == event2

    def test_raises_when_no_matching_type(self, single_event_assertions: EventAssertions) -> None:
        with pytest.raises(AssertionError) as exc_info:
            single_event_assertions.assert_event_with_fields(SampleUpdated, new_value=42)
        assert "SampleUpdated" in str(exc_info.value)

    def test_raises_when_fields_dont_match(self, single_event_assertions: EventAssertions) -> None:
        with pytest.raises(AssertionError) as exc_info:
            single_event_assertions.assert_event_with_fields(SampleCreated, name="wrong-name")
        assert "wrong-name" in str(exc_info.value) and "SampleCreated" in str(exc_info.value)

    def test_raises_for_missing_attribute(self) -> None:
        event = SampleCreated(
            aggregate_id=uuid4(), aggregate_type="Sample", aggregate_version=1, name="test"
        )
        assertions = EventAssertions([event])
        with pytest.raises(AssertionError) as exc_info:
            assertions.assert_event_with_fields(SampleCreated, nonexistent_field="value")
        assert "SampleCreated" in str(exc_info.value)


class TestAssertEventForAggregate:
    """Tests for assert_event_for_aggregate method."""

    def test_finds_event_for_aggregate(self) -> None:
        agg_id = uuid4()
        event = SampleCreated(
            aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1, name="test"
        )
        assertions = EventAssertions([event])
        assert assertions.assert_event_for_aggregate(SampleCreated, agg_id) == event

    def test_finds_correct_aggregate_among_multiple(self) -> None:
        agg_id_1, agg_id_2 = uuid4(), uuid4()
        event1 = SampleCreated(
            aggregate_id=agg_id_1, aggregate_type="Sample", aggregate_version=1, name="first"
        )
        event2 = SampleCreated(
            aggregate_id=agg_id_2, aggregate_type="Sample", aggregate_version=1, name="second"
        )
        assertions = EventAssertions([event1, event2])
        found = assertions.assert_event_for_aggregate(SampleCreated, agg_id_2)
        assert found == event2 and found.name == "second"

    def test_raises_when_no_event_type(self, single_event_assertions: EventAssertions) -> None:
        with pytest.raises(AssertionError) as exc_info:
            single_event_assertions.assert_event_for_aggregate(SampleUpdated, uuid4())
        assert "SampleUpdated" in str(exc_info.value)
        assert "no events of that type were found" in str(exc_info.value)

    def test_raises_when_wrong_aggregate(self) -> None:
        agg_id_1, agg_id_2 = uuid4(), uuid4()
        event = SampleCreated(
            aggregate_id=agg_id_1, aggregate_type="Sample", aggregate_version=1, name="test"
        )
        assertions = EventAssertions([event])
        with pytest.raises(AssertionError) as exc_info:
            assertions.assert_event_for_aggregate(SampleCreated, agg_id_2)
        error_msg = str(exc_info.value)
        assert str(agg_id_2) in error_msg and "only for aggregates" in error_msg


class TestGetEventsOfType:
    """Tests for get_events_of_type method."""

    def test_returns_all_matching_events(self) -> None:
        event1 = SampleCreated(
            aggregate_id=uuid4(), aggregate_type="Sample", aggregate_version=1, name="first"
        )
        event2 = SampleUpdated(
            aggregate_id=uuid4(), aggregate_type="Sample", aggregate_version=2, new_value=42
        )
        event3 = SampleCreated(
            aggregate_id=uuid4(), aggregate_type="Sample", aggregate_version=1, name="third"
        )
        assertions = EventAssertions([event1, event2, event3])
        created_events = assertions.get_events_of_type(SampleCreated)

        assert len(created_events) == 2
        assert all(isinstance(e, SampleCreated) for e in created_events)
        assert (created_events[0].name, created_events[1].name) == ("first", "third")

    def test_returns_empty_list_when_no_match(
        self, single_event_assertions: EventAssertions
    ) -> None:
        assert single_event_assertions.get_events_of_type(SampleUpdated) == []

    def test_returns_empty_list_when_no_events(self, empty_assertions: EventAssertions) -> None:
        assert empty_assertions.get_events_of_type(SampleCreated) == []
