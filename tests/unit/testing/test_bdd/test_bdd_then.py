"""Tests for then_event_published and then_no_events_published BDD helpers."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.testing import InMemoryTestHarness
from eventsource.testing.bdd import (
    then_event_published,
    then_no_events_published,
)
from tests.unit.testing.test_bdd._fixtures import (
    SampleCreated,
    SampleUpdated,
)


class TestThenEventPublished:
    """Tests for the then_event_published BDD helper."""

    async def test_finds_matching_event_by_type(self, harness: InMemoryTestHarness) -> None:
        """Test finding event by type without field constraints."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="test",
        )
        await harness.event_bus.publish([event])

        found = then_event_published(harness, SampleCreated)

        assert found == event

    async def test_finds_matching_event_with_field_matching(
        self, harness: InMemoryTestHarness
    ) -> None:
        """Test finding event with specific field values."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="specific-name",
        )
        await harness.event_bus.publish([event])

        found = then_event_published(harness, SampleCreated, name="specific-name")

        assert found == event
        assert found.name == "specific-name"

    async def test_finds_correct_event_among_multiple(self, harness: InMemoryTestHarness) -> None:
        """Test finding correct event when multiple events exist."""
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
        await harness.event_bus.publish([event1, event2])

        found = then_event_published(harness, SampleCreated, name="second")

        assert found == event2

    async def test_raises_when_no_matching_type(self, harness: InMemoryTestHarness) -> None:
        """Test raises AssertionError when no event of type found."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="test",
        )
        await harness.event_bus.publish([event])

        with pytest.raises(AssertionError) as exc_info:
            then_event_published(harness, SampleUpdated)

        assert "SampleUpdated" in str(exc_info.value)
        assert "SampleCreated" in str(exc_info.value)

    async def test_raises_when_no_events_published(self, harness: InMemoryTestHarness) -> None:
        """Test raises AssertionError when no events at all."""
        with pytest.raises(AssertionError) as exc_info:
            then_event_published(harness, SampleCreated)

        assert "no events of that type were found" in str(exc_info.value)
        assert "none" in str(exc_info.value)

    async def test_raises_when_attributes_dont_match(self, harness: InMemoryTestHarness) -> None:
        """Test raises AssertionError when field values don't match."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="actual-name",
        )
        await harness.event_bus.publish([event])

        with pytest.raises(AssertionError) as exc_info:
            then_event_published(harness, SampleCreated, name="expected-name")

        assert "expected-name" in str(exc_info.value)
        assert "SampleCreated" in str(exc_info.value)

    async def test_returns_event_for_chaining(self, harness: InMemoryTestHarness) -> None:
        """Test that returned event can be used for further assertions."""
        agg_id = uuid4()
        event = SampleUpdated(
            aggregate_id=agg_id,
            aggregate_type="Sample",
            aggregate_version=1,
            new_value=42,
        )
        await harness.event_bus.publish([event])

        found = then_event_published(harness, SampleUpdated)

        # Further assertions on the returned event
        assert found.aggregate_id == agg_id
        assert found.new_value == 42


class TestThenNoEventsPublished:
    """Tests for the then_no_events_published BDD helper."""

    async def test_passes_when_no_events(self, harness: InMemoryTestHarness) -> None:
        """Test passes when no events have been published."""
        then_no_events_published(harness)
        # Should not raise

    async def test_raises_when_events_exist(self, harness: InMemoryTestHarness) -> None:
        """Test raises when events have been published."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="test",
        )
        await harness.event_bus.publish([event])

        with pytest.raises(AssertionError) as exc_info:
            then_no_events_published(harness)

        assert "Expected no events" in str(exc_info.value)
        assert "SampleCreated" in str(exc_info.value)

    async def test_type_filter_passes_when_no_matching_type(
        self, harness: InMemoryTestHarness
    ) -> None:
        """Test passes for type filter when no events of that type exist."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="test",
        )
        await harness.event_bus.publish([event])

        # Should pass - no SampleUpdated events
        then_no_events_published(harness, SampleUpdated)

    async def test_type_filter_raises_when_matching_type_exists(
        self, harness: InMemoryTestHarness
    ) -> None:
        """Test raises for type filter when events of that type exist."""
        event = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="test",
        )
        await harness.event_bus.publish([event])

        with pytest.raises(AssertionError) as exc_info:
            then_no_events_published(harness, SampleCreated)

        assert "Expected no SampleCreated" in str(exc_info.value)
