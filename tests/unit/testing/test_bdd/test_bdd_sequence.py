"""Tests for then_event_sequence and then_event_count BDD helpers."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.testing import InMemoryTestHarness
from eventsource.testing.bdd import (
    then_event_count,
    then_event_sequence,
)
from tests.unit.testing.test_bdd._fixtures import (
    SampleCreated,
    SampleDeleted,
    SampleUpdated,
)


class TestThenEventSequence:
    """Tests for the then_event_sequence BDD helper."""

    async def test_passes_for_correct_sequence(self, harness: InMemoryTestHarness) -> None:
        """Test passes when event sequence matches."""
        events = [
            SampleCreated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=1,
                name="test",
            ),
            SampleUpdated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=2,
                new_value=42,
            ),
            SampleDeleted(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=3,
            ),
        ]
        await harness.event_bus.publish(events)

        result = then_event_sequence(
            harness,
            [SampleCreated, SampleUpdated, SampleDeleted],
        )

        assert result == events

    async def test_raises_for_wrong_count(self, harness: InMemoryTestHarness) -> None:
        """Test raises when event count doesn't match."""
        events = [
            SampleCreated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=1,
                name="test",
            ),
        ]
        await harness.event_bus.publish(events)

        with pytest.raises(AssertionError) as exc_info:
            then_event_sequence(harness, [SampleCreated, SampleUpdated])

        assert "expected 2 events" in str(exc_info.value)
        assert "got 1" in str(exc_info.value)

    async def test_raises_for_wrong_order(self, harness: InMemoryTestHarness) -> None:
        """Test raises when event order is wrong."""
        events = [
            SampleUpdated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=1,
                new_value=42,
            ),
            SampleCreated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=2,
                name="test",
            ),
        ]
        await harness.event_bus.publish(events)

        with pytest.raises(AssertionError) as exc_info:
            then_event_sequence(harness, [SampleCreated, SampleUpdated])

        assert "position 0" in str(exc_info.value)
        assert "expected SampleCreated" in str(exc_info.value)
        assert "got SampleUpdated" in str(exc_info.value)

    async def test_raises_for_wrong_type_in_middle(self, harness: InMemoryTestHarness) -> None:
        """Test raises when event type is wrong in the middle of sequence."""
        events = [
            SampleCreated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=1,
                name="test",
            ),
            SampleDeleted(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=2,
            ),
            SampleUpdated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=3,
                new_value=42,
            ),
        ]
        await harness.event_bus.publish(events)

        with pytest.raises(AssertionError) as exc_info:
            then_event_sequence(
                harness,
                [SampleCreated, SampleUpdated, SampleDeleted],
            )

        assert "position 1" in str(exc_info.value)

    async def test_empty_sequence_passes_when_no_events(self, harness: InMemoryTestHarness) -> None:
        """Test empty sequence passes when no events published."""
        result = then_event_sequence(harness, [])

        assert result == []

    async def test_returns_events_for_further_assertions(
        self, harness: InMemoryTestHarness
    ) -> None:
        """Test that returned events can be used for further assertions."""
        agg_id = uuid4()
        events = [
            SampleCreated(
                aggregate_id=agg_id,
                aggregate_type="Sample",
                aggregate_version=1,
                name="test-name",
            ),
        ]
        await harness.event_bus.publish(events)

        result = then_event_sequence(harness, [SampleCreated])

        assert len(result) == 1
        assert result[0].name == "test-name"


class TestThenEventCount:
    """Tests for the then_event_count BDD helper."""

    async def test_passes_for_correct_count(self, harness: InMemoryTestHarness) -> None:
        """Test passes when event count matches."""
        events = [
            SampleCreated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=1,
                name="test1",
            ),
            SampleCreated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=1,
                name="test2",
            ),
        ]
        await harness.event_bus.publish(events)

        then_event_count(harness, 2)
        # Should not raise

    async def test_raises_for_wrong_count(self, harness: InMemoryTestHarness) -> None:
        """Test raises when event count doesn't match."""
        events = [
            SampleCreated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=1,
                name="test",
            ),
        ]
        await harness.event_bus.publish(events)

        with pytest.raises(AssertionError) as exc_info:
            then_event_count(harness, 3)

        assert "expected 3 events" in str(exc_info.value)
        assert "got 1" in str(exc_info.value)

    async def test_zero_count_passes_when_no_events(self, harness: InMemoryTestHarness) -> None:
        """Test count of 0 passes when no events published."""
        then_event_count(harness, 0)
        # Should not raise

    async def test_error_message_includes_event_types(self, harness: InMemoryTestHarness) -> None:
        """Test error message includes types of published events."""
        events = [
            SampleCreated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=1,
                name="test",
            ),
            SampleUpdated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=2,
                new_value=42,
            ),
        ]
        await harness.event_bus.publish(events)

        with pytest.raises(AssertionError) as exc_info:
            then_event_count(harness, 5)

        assert "SampleCreated" in str(exc_info.value)
        assert "SampleUpdated" in str(exc_info.value)
