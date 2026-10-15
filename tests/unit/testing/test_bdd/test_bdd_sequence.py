"""Tests for then_event_sequence and then_event_count BDD helpers."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.testing import InMemoryTestHarness
from eventsource.testing.bdd import then_event_count, then_event_sequence
from tests.unit.testing.test_bdd._fixtures import SampleCreated, SampleDeleted, SampleUpdated


def _created(version: int = 1, name: str = "test", agg_id=None) -> SampleCreated:
    return SampleCreated(
        aggregate_id=agg_id or uuid4(),
        aggregate_type="Sample",
        aggregate_version=version,
        name=name,
    )


def _updated(version: int = 2, new_value: int = 42) -> SampleUpdated:
    return SampleUpdated(
        aggregate_id=uuid4(),
        aggregate_type="Sample",
        aggregate_version=version,
        new_value=new_value,
    )


def _deleted(version: int = 3) -> SampleDeleted:
    return SampleDeleted(
        aggregate_id=uuid4(),
        aggregate_type="Sample",
        aggregate_version=version,
    )


class TestThenEventSequence:
    async def test_passes_for_correct_sequence(self, harness: InMemoryTestHarness) -> None:
        events = [_created(1), _updated(2), _deleted(3)]
        await harness.event_bus.publish(events)
        result = then_event_sequence(harness, [SampleCreated, SampleUpdated, SampleDeleted])
        assert result == events

    async def test_raises_for_wrong_count(self, harness: InMemoryTestHarness) -> None:
        await harness.event_bus.publish([_created(1)])
        with pytest.raises(AssertionError) as exc_info:
            then_event_sequence(harness, [SampleCreated, SampleUpdated])
        assert "expected 2 events" in str(exc_info.value)
        assert "got 1" in str(exc_info.value)

    async def test_raises_for_wrong_order(self, harness: InMemoryTestHarness) -> None:
        await harness.event_bus.publish([_updated(1), _created(2)])
        with pytest.raises(AssertionError) as exc_info:
            then_event_sequence(harness, [SampleCreated, SampleUpdated])
        assert "position 0" in str(exc_info.value)
        assert "expected SampleCreated" in str(exc_info.value)
        assert "got SampleUpdated" in str(exc_info.value)

    async def test_raises_for_wrong_type_in_middle(self, harness: InMemoryTestHarness) -> None:
        await harness.event_bus.publish([_created(1), _deleted(2), _updated(3)])
        with pytest.raises(AssertionError) as exc_info:
            then_event_sequence(harness, [SampleCreated, SampleUpdated, SampleDeleted])
        assert "position 1" in str(exc_info.value)

    async def test_empty_sequence_passes_when_no_events(self, harness: InMemoryTestHarness) -> None:
        assert then_event_sequence(harness, []) == []

    async def test_returns_events_for_further_assertions(
        self, harness: InMemoryTestHarness
    ) -> None:
        await harness.event_bus.publish([_created(1, name="test-name")])
        result = then_event_sequence(harness, [SampleCreated])
        assert len(result) == 1
        assert result[0].name == "test-name"

    async def test_passes_for_variadic_event_types(self, harness: InMemoryTestHarness) -> None:
        events = [_created(1), _updated(2), _deleted(3)]
        await harness.event_bus.publish(events)
        result = then_event_sequence(harness, SampleCreated, SampleUpdated, SampleDeleted)
        assert result == events


class TestThenEventCount:
    async def test_passes_for_correct_count(self, harness: InMemoryTestHarness) -> None:
        await harness.event_bus.publish([_created(1, "test1"), _created(1, "test2")])
        then_event_count(harness, 2)

    async def test_raises_for_wrong_count(self, harness: InMemoryTestHarness) -> None:
        await harness.event_bus.publish([_created(1)])
        with pytest.raises(AssertionError) as exc_info:
            then_event_count(harness, 3)
        assert "expected 3 events" in str(exc_info.value)
        assert "got 1" in str(exc_info.value)

    async def test_zero_count_passes_when_no_events(self, harness: InMemoryTestHarness) -> None:
        then_event_count(harness, 0)

    async def test_error_message_includes_event_types(self, harness: InMemoryTestHarness) -> None:
        await harness.event_bus.publish([_created(1), _updated(2)])
        with pytest.raises(AssertionError) as exc_info:
            then_event_count(harness, 5)
        assert "SampleCreated" in str(exc_info.value)
        assert "SampleUpdated" in str(exc_info.value)

    async def test_event_type_count_passes_and_fails(self, harness: InMemoryTestHarness) -> None:
        events = [_created(1, "t"), _updated(2, 42), _updated(3, 99)]
        await harness.event_bus.publish(events)

        then_event_count(harness, 1, event_type=SampleCreated)
        then_event_count(harness, 2, event_type=SampleUpdated)
        then_event_count(harness, 0, event_type=SampleDeleted)

        with pytest.raises(AssertionError, match=r"Expected 3 SampleUpdated event\(s\), got 2"):
            then_event_count(harness, 3, event_type=SampleUpdated)
