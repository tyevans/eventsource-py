"""Tests for for_aggregate, assert_events_for_aggregate, and __bool__ on EventAssertions."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.testing.assertions import EventAssertions
from tests.unit.testing.test_assertions._fixtures import (
    SampleCreated,
    SampleDeleted,
    SampleUpdated,
)


class TestForAggregate:
    """Tests for EventAssertions.for_aggregate method."""

    def test_returns_scoped_assertions_for_matching_aggregate(self) -> None:
        """for_aggregate returns an EventAssertions containing only events for that aggregate."""
        agg_id_1 = uuid4()
        agg_id_2 = uuid4()
        e1 = SampleCreated(
            aggregate_id=agg_id_1,
            aggregate_type="Sample",
            aggregate_version=1,
            name="first",
        )
        e2 = SampleCreated(
            aggregate_id=agg_id_2,
            aggregate_type="Sample",
            aggregate_version=1,
            name="second",
        )
        e3 = SampleUpdated(
            aggregate_id=agg_id_1,
            aggregate_type="Sample",
            aggregate_version=2,
            new_value=42,
        )
        assertions = EventAssertions([e1, e2, e3])

        scoped = assertions.for_aggregate(agg_id_1)

        assert isinstance(scoped, EventAssertions)
        assert scoped.events == [e1, e3]
        scoped.assert_event_sequence([SampleCreated, SampleUpdated])

    def test_returns_empty_assertions_when_no_events_for_aggregate(self) -> None:
        """for_aggregate returns an empty EventAssertions when no events match."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="first",
        )
        assertions = EventAssertions([event])

        scoped = assertions.for_aggregate(uuid4())

        assert scoped.events == []
        scoped.assert_no_events_published()


class TestAssertEventsForAggregate:
    """Tests for EventAssertions.assert_events_for_aggregate method."""

    def test_returns_matching_events_without_expected_count(self) -> None:
        """assert_events_for_aggregate returns matching events when expected_count is None."""
        agg_id = uuid4()
        other_id = uuid4()
        e1 = SampleCreated(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
            name="first",
        )
        e2 = SampleUpdated(
            aggregate_id=other_id,
            aggregate_type="Sample",
            aggregate_version=1,
            new_value=10,
        )
        e3 = SampleDeleted(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=2,
        )
        assertions = EventAssertions([e1, e2, e3])

        matching = assertions.assert_events_for_aggregate(agg_id)

        assert matching == [e1, e3]

    def test_returns_matching_events_with_matching_expected_count(self) -> None:
        """assert_events_for_aggregate succeeds when matching count equals expected_count."""
        agg_id = uuid4()
        e1 = SampleCreated(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
            name="first",
        )
        e2 = SampleUpdated(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=2,
            new_value=99,
        )
        assertions = EventAssertions([e1, e2])

        matching = assertions.assert_events_for_aggregate(agg_id, expected_count=2)

        assert matching == [e1, e2]

    def test_allows_expected_count_zero_when_none_found(self) -> None:
        """assert_events_for_aggregate(agg_id, expected_count=0) succeeds when 0 match."""
        assertions = EventAssertions([])
        matching = assertions.assert_events_for_aggregate(uuid4(), expected_count=0)
        assert matching == []

    def test_raises_when_no_events_for_aggregate_and_no_expected_count(self) -> None:
        """assert_events_for_aggregate raises when no events match and expected_count is None."""
        agg_id = uuid4()
        other_event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="other",
        )
        assertions = EventAssertions([other_event])

        with pytest.raises(AssertionError) as exc_info:
            assertions.assert_events_for_aggregate(agg_id)

        assert str(exc_info.value) == (
            f"Expected events for aggregate {agg_id}, but none were found."
        )

    def test_raises_when_count_differs_from_expected_count(self) -> None:
        """assert_events_for_aggregate raises when matching count != expected_count."""
        agg_id = uuid4()
        event = SampleCreated(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
            name="only-one",
        )
        assertions = EventAssertions([event])

        with pytest.raises(AssertionError) as exc_info:
            assertions.assert_events_for_aggregate(agg_id, expected_count=2)

        assert str(exc_info.value) == (f"Expected 2 event(s) for aggregate {agg_id}, got 1.")


class TestEventAssertionsBool:
    """Tests for EventAssertions.__bool__ method."""

    def test_empty_assertions_is_truthy(self) -> None:
        """bool(EventAssertions([])) is True even when len(assertions) == 0."""
        assertions = EventAssertions([])
        assert len(assertions) == 0
        assert bool(assertions) is True

    def test_non_empty_assertions_is_truthy(self) -> None:
        """bool(EventAssertions([...])) is True when events are present."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="item",
        )
        assertions = EventAssertions([event])
        assert bool(assertions) is True

    def test_or_fallback_preserves_empty_assertions_instance(self) -> None:
        """assertions or default returns assertions rather than falling back on empty."""
        empty = EventAssertions([])
        fallback = EventAssertions(
            [
                SampleCreated(
                    aggregate_id=uuid4(),
                    aggregate_type="Sample",
                    aggregate_version=1,
                    name="fallback",
                )
            ]
        )
        chosen = empty or fallback
        assert chosen is empty
