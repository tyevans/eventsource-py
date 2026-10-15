"""Tests for for_aggregate, assert_events_for_aggregate, positional assertions, and __bool__."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.testing.assertions import EventAssertions
from tests.unit.testing.test_assertions._fixtures import (
    SampleCreated,
    SampleDeleted,
    SampleUpdated,
)


def _created(agg_id: UUID | None = None, name: str = "first", version: int = 1) -> SampleCreated:
    return SampleCreated(
        aggregate_id=agg_id or uuid4(),
        aggregate_type="Sample",
        aggregate_version=version,
        name=name,
    )


def _updated(agg_id: UUID | None = None, new_value: int = 42, version: int = 2) -> SampleUpdated:
    return SampleUpdated(
        aggregate_id=agg_id or uuid4(),
        aggregate_type="Sample",
        aggregate_version=version,
        new_value=new_value,
    )


class TestForAggregate:
    """Tests for EventAssertions.for_aggregate method."""

    def test_returns_scoped_assertions_for_matching_aggregate(self) -> None:
        """for_aggregate returns an EventAssertions containing only events for that aggregate."""
        agg_id_1, agg_id_2 = uuid4(), uuid4()
        e1 = _created(agg_id_1, name="first")
        e2 = _created(agg_id_2, name="second")
        e3 = _updated(agg_id_1, new_value=42)
        assertions = EventAssertions([e1, e2, e3])

        scoped = assertions.for_aggregate(agg_id_1)

        assert isinstance(scoped, EventAssertions)
        assert scoped.events == [e1, e3]
        scoped.assert_event_sequence([SampleCreated, SampleUpdated])

    def test_returns_empty_assertions_when_no_events_for_aggregate(self) -> None:
        """for_aggregate returns an empty EventAssertions when no events match."""
        assertions = EventAssertions([_created()])

        scoped = assertions.for_aggregate(uuid4())

        assert scoped.events == []
        scoped.assert_no_events_published()


class TestAssertEventsForAggregate:
    """Tests for EventAssertions.assert_events_for_aggregate method."""

    def test_returns_matching_events_without_expected_count(self) -> None:
        """assert_events_for_aggregate returns matching events when expected_count is None."""
        agg_id = uuid4()
        e1 = _created(agg_id, name="first")
        e2 = _updated(uuid4(), new_value=10, version=1)
        e3 = SampleDeleted(aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=2)
        assertions = EventAssertions([e1, e2, e3])

        assert assertions.assert_events_for_aggregate(agg_id) == [e1, e3]

    def test_returns_matching_events_with_matching_expected_count(self) -> None:
        """assert_events_for_aggregate succeeds when matching count equals expected_count."""
        agg_id = uuid4()
        e1, e2 = _created(agg_id, name="first"), _updated(agg_id, new_value=99)
        assertions = EventAssertions([e1, e2])

        assert assertions.assert_events_for_aggregate(agg_id, expected_count=2) == [e1, e2]

    def test_allows_expected_count_zero_when_none_found(self) -> None:
        """assert_events_for_aggregate(agg_id, expected_count=0) succeeds when 0 match."""
        assertions = EventAssertions([])
        assert assertions.assert_events_for_aggregate(uuid4(), expected_count=0) == []

    def test_raises_when_no_events_for_aggregate_and_no_expected_count(self) -> None:
        """assert_events_for_aggregate raises when no events match and expected_count is None."""
        agg_id = uuid4()
        assertions = EventAssertions([_created(name="other")])

        with pytest.raises(AssertionError) as exc_info:
            assertions.assert_events_for_aggregate(agg_id)

        assert (
            str(exc_info.value) == f"Expected events for aggregate {agg_id}, but none were found."
        )

    def test_raises_when_count_differs_from_expected_count(self) -> None:
        """assert_events_for_aggregate raises when matching count != expected_count."""
        agg_id = uuid4()
        assertions = EventAssertions([_created(agg_id, name="only-one")])

        with pytest.raises(AssertionError) as exc_info:
            assertions.assert_events_for_aggregate(agg_id, expected_count=2)

        assert str(exc_info.value) == f"Expected 2 event(s) for aggregate {agg_id}, got 1."


class TestEventAssertionsBool:
    """Tests for EventAssertions.__bool__ method."""

    def test_empty_assertions_is_truthy(self) -> None:
        """bool(EventAssertions([])) is True even when len(assertions) == 0."""
        assertions = EventAssertions([])
        assert len(assertions) == 0
        assert bool(assertions) is True

    def test_non_empty_assertions_is_truthy(self) -> None:
        """bool(EventAssertions([...])) is True when events are present."""
        assert bool(EventAssertions([_created(name="item")])) is True

    def test_or_fallback_preserves_empty_assertions_instance(self) -> None:
        """assertions or default returns assertions rather than falling back on empty."""
        empty = EventAssertions([])
        fallback = EventAssertions([_created(name="fallback")])
        assert (empty or fallback) is empty


class TestAssertFirstAndLastEvent:
    """Tests for EventAssertions.assert_first_event and assert_last_event."""

    def test_assert_first_event_success_variants(self) -> None:
        """assert_first_event succeeds with/without event_type and expected_fields."""
        e1, e2 = _created(name="alpha"), _updated(new_value=7)
        assertions = EventAssertions([e1, e2])

        assert assertions.assert_first_event() is e1
        assert assertions.assert_first_event(SampleCreated) is e1
        assert assertions.assert_first_event(SampleCreated, name="alpha") is e1
        assert assertions.assert_first_event(name="alpha") is e1

    def test_assert_last_event_success_variants(self) -> None:
        """assert_last_event succeeds with/without event_type and expected_fields."""
        e1, e2 = _created(name="alpha"), _updated(new_value=7)
        assertions = EventAssertions([e1, e2])

        assert assertions.assert_last_event() is e2
        assert assertions.assert_last_event(SampleUpdated) is e2
        assert assertions.assert_last_event(SampleUpdated, new_value=7) is e2
        assert assertions.assert_last_event(new_value=7) is e2

    def test_assert_first_and_last_event_empty_raises(self) -> None:
        """Both methods raise when no events were recorded."""
        empty = EventAssertions([])
        for method in (empty.assert_first_event, empty.assert_last_event):
            with pytest.raises(AssertionError) as exc_info:
                method()
            assert (
                str(exc_info.value) == "Expected at least one event, but no events were recorded."
            )

    def test_assert_first_and_last_event_wrong_type_raises(self) -> None:
        """Both methods raise with position-specific message when event_type mismatches."""
        assertions = EventAssertions([_created(name="alpha"), _updated(new_value=7)])

        with pytest.raises(AssertionError) as first_exc:
            assertions.assert_first_event(SampleUpdated)
        assert (
            str(first_exc.value) == "Expected first event to be SampleUpdated, got SampleCreated."
        )

        with pytest.raises(AssertionError) as last_exc:
            assertions.assert_last_event(SampleCreated)
        assert str(last_exc.value) == "Expected last event to be SampleCreated, got SampleUpdated."

    def test_assert_first_and_last_event_wrong_field_raises(self) -> None:
        """Both methods raise with position-specific message when expected_fields mismatch."""
        assertions = EventAssertions([_created(name="alpha"), _updated(new_value=7)])

        with pytest.raises(AssertionError) as first_exc:
            assertions.assert_first_event(SampleCreated, name="beta")
        assert str(first_exc.value) == "Expected first event field name='beta', got 'alpha'."

        with pytest.raises(AssertionError) as last_exc:
            assertions.assert_last_event(SampleUpdated, new_value=99)
        assert str(last_exc.value) == "Expected last event field new_value=99, got 7."
