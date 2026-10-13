"""Tests for event publishing assertions."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.testing.assertions import EventAssertions
from tests.unit.testing.test_assertions._fixtures import (
    SampleCreated,
    SampleUpdated,
)


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
