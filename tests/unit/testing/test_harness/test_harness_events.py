"""Tests for test harness published events tracking, clearing, filtering, and container dunders."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion
from eventsource.testing import InMemoryTestHarness
from tests.unit.testing.test_harness._fixtures import (
    HarnessOtherEvent,
    HarnessSampleEvent,
)


def _sample_event(aggregate_id: UUID | None = None) -> HarnessSampleEvent:
    return HarnessSampleEvent(
        aggregate_id=aggregate_id or uuid4(),
        aggregate_type="Sample",
        aggregate_version=1,
    )


def _other_event(aggregate_id: UUID | None = None) -> HarnessOtherEvent:
    return HarnessOtherEvent(
        aggregate_id=aggregate_id or uuid4(),
        aggregate_type="Other",
        aggregate_version=1,
    )


class TestHarnessPublishedEvents:
    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        return InMemoryTestHarness()

    def test_published_events_initially_empty(self, harness: InMemoryTestHarness) -> None:
        assert harness.published_events == []

    @pytest.mark.asyncio
    async def test_published_events_tracks_single_event(self, harness: InMemoryTestHarness) -> None:
        event = _sample_event()
        await harness.event_bus.publish([event])
        assert len(harness.published_events) == 1
        assert harness.published_events[0] == event

    @pytest.mark.asyncio
    async def test_published_events_tracks_multiple_events(
        self, harness: InMemoryTestHarness
    ) -> None:
        event1, event2 = _sample_event(), _sample_event()
        await harness.event_bus.publish([event1, event2])
        assert len(harness.published_events) == 2
        assert harness.published_events[0] == event1
        assert harness.published_events[1] == event2

    @pytest.mark.asyncio
    async def test_published_events_tracks_events_across_multiple_publishes(
        self, harness: InMemoryTestHarness
    ) -> None:
        event1, event2 = _sample_event(), _sample_event()
        await harness.event_bus.publish([event1])
        await harness.event_bus.publish([event2])
        assert len(harness.published_events) == 2

    @pytest.mark.asyncio
    async def test_published_events_returns_copy(self, harness: InMemoryTestHarness) -> None:
        await harness.event_bus.publish([_sample_event()])
        events = harness.published_events
        events.clear()
        assert len(harness.published_events) == 1

    @pytest.mark.asyncio
    async def test_harness_iter_and_contains(self, harness: InMemoryTestHarness) -> None:
        assert list(iter(harness)) == []
        assert HarnessSampleEvent not in harness
        ev1, ev2 = _sample_event(), _other_event()
        unpub = _sample_event()
        await harness.publish_events(ev1)
        assert list(iter(harness)) == [ev1]
        assert HarnessSampleEvent in harness
        assert HarnessOtherEvent not in harness
        assert ev1 in harness
        assert unpub not in harness
        assert "invalid" not in harness
        await harness.publish_events(ev2)
        assert list(harness) == [ev1, ev2]
        assert HarnessOtherEvent in harness


class TestHarnessClearPublishedEvents:
    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        return InMemoryTestHarness()

    @pytest.mark.asyncio
    async def test_clear_published_events_clears_list(self, harness: InMemoryTestHarness) -> None:
        await harness.event_bus.publish([_sample_event()])
        assert len(harness.published_events) == 1
        harness.clear_published_events()
        assert harness.published_events == []

    @pytest.mark.asyncio
    async def test_clear_published_events_preserves_subscriptions(
        self, harness: InMemoryTestHarness
    ) -> None:
        events_received: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            events_received.append(event)

        harness.event_bus.subscribe(HarnessSampleEvent, handler)
        await harness.event_bus.publish([_sample_event()])
        harness.clear_published_events()
        await harness.event_bus.publish([_sample_event()])
        assert len(events_received) == 2

    @pytest.mark.asyncio
    async def test_clear_published_events_preserves_event_store_state(
        self, harness: InMemoryTestHarness
    ) -> None:
        aggregate_id = uuid4()
        event = _sample_event(aggregate_id)
        stream_id = StreamId(aggregate_id=aggregate_id, category="Sample")

        await harness.event_store.append(stream_id, [event], ExpectedVersion.no_stream())
        await harness.event_bus.publish([event])
        harness.clear_published_events()

        envelopes = [e async for e in harness.event_store.read_stream(stream_id)]
        assert len(envelopes) == 1


class TestHarnessGetEventsOfType:
    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        return InMemoryTestHarness()

    def test_get_events_of_type_empty_initially(self, harness: InMemoryTestHarness) -> None:
        assert harness.get_events_of_type(HarnessSampleEvent) == []

    @pytest.mark.asyncio
    async def test_get_events_of_type_filters_correctly(self, harness: InMemoryTestHarness) -> None:
        sample_event, other_event = _sample_event(), _other_event()
        await harness.event_bus.publish([sample_event, other_event])
        assert harness.get_events_of_type(HarnessSampleEvent) == [sample_event]
        assert harness.get_events_of_type(HarnessOtherEvent) == [other_event]

    @pytest.mark.asyncio
    async def test_get_events_of_type_returns_all_matching(
        self, harness: InMemoryTestHarness
    ) -> None:
        event1, event2, event3 = _sample_event(), _sample_event(), _other_event()
        await harness.event_bus.publish([event1, event2, event3])
        assert harness.get_events_of_type(HarnessSampleEvent) == [event1, event2]

    @pytest.mark.asyncio
    async def test_get_events_of_type_with_no_matches(self, harness: InMemoryTestHarness) -> None:
        await harness.event_bus.publish([_sample_event()])
        assert harness.get_events_of_type(HarnessOtherEvent) == []

    @pytest.mark.asyncio
    async def test_published_of_type_delegates_to_bus(self, harness: InMemoryTestHarness) -> None:
        sample_event, other_event = _sample_event(), _other_event()
        await harness.event_bus.publish([sample_event, other_event])
        assert harness.published_of_type(HarnessSampleEvent) == [sample_event]
        assert harness.published_of_type(HarnessOtherEvent) == [other_event]
        assert harness.published_of_type(HarnessSampleEvent) == harness.get_events_of_type(
            HarnessSampleEvent
        )
        assert harness.event_bus.get_wildcard_subscriber_count() == 0
        harness.clear_published_events()
        assert harness.event_bus.published_events == []
