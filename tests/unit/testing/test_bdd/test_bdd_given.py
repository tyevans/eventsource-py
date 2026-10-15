"""Tests for the given_events BDD helper."""

from __future__ import annotations

from uuid import uuid4

from eventsource.domain import StreamId
from eventsource.testing import InMemoryTestHarness
from eventsource.testing.bdd import given_events
from tests.unit.testing.test_bdd._fixtures import (
    OtherAggregateCreated,
    SampleCreated,
    SampleDeleted,
    SampleUpdated,
)


class TestGivenEvents:
    """Tests for the given_events BDD helper."""

    async def test_loads_single_event_into_store(self, harness: InMemoryTestHarness) -> None:
        """Test loading a single event into the event store."""
        agg_id = uuid4()
        events = [
            SampleCreated(
                aggregate_id=agg_id,
                aggregate_type="Sample",
                aggregate_version=1,
                name="test-sample",
            ),
        ]

        await given_events(harness, events)

        envelopes = [
            e
            async for e in harness.event_store.read_stream(
                StreamId(aggregate_id=agg_id, category="Sample")
            )
        ]
        assert len(envelopes) == 1
        assert isinstance(envelopes[0].event, SampleCreated)
        assert envelopes[0].event.name == "test-sample"

    async def test_loads_multiple_events_for_same_aggregate(
        self, harness: InMemoryTestHarness
    ) -> None:
        """Test loading multiple events for the same aggregate."""
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

        await given_events(harness, events)

        envelopes = [
            e
            async for e in harness.event_store.read_stream(
                StreamId(aggregate_id=agg_id, category="Sample")
            )
        ]
        assert len(envelopes) == 3
        assert isinstance(envelopes[0].event, SampleCreated)
        assert isinstance(envelopes[1].event, SampleUpdated)
        assert isinstance(envelopes[2].event, SampleDeleted)

    async def test_loads_events_for_multiple_aggregates(self, harness: InMemoryTestHarness) -> None:
        """Test loading events for multiple different aggregates."""
        agg_id_1 = uuid4()
        agg_id_2 = uuid4()
        events = [
            SampleCreated(
                aggregate_id=agg_id_1,
                aggregate_type="Sample",
                aggregate_version=1,
                name="sample-1",
            ),
            SampleCreated(
                aggregate_id=agg_id_2,
                aggregate_type="Sample",
                aggregate_version=1,
                name="sample-2",
            ),
        ]

        await given_events(harness, events)

        envelopes1 = [
            e
            async for e in harness.event_store.read_stream(
                StreamId(aggregate_id=agg_id_1, category="Sample")
            )
        ]
        envelopes2 = [
            e
            async for e in harness.event_store.read_stream(
                StreamId(aggregate_id=agg_id_2, category="Sample")
            )
        ]

        assert len(envelopes1) == 1
        assert envelopes1[0].event.name == "sample-1"
        assert len(envelopes2) == 1
        assert envelopes2[0].event.name == "sample-2"

    async def test_empty_events_is_noop(self, harness: InMemoryTestHarness) -> None:
        """Test that empty events list doesn't cause errors."""
        await given_events(harness, [])
        # Should not raise - just verifying it completes successfully

    async def test_loads_events_for_different_aggregate_types(
        self, harness: InMemoryTestHarness
    ) -> None:
        """Test loading events for different aggregate types."""
        agg_id_1 = uuid4()
        agg_id_2 = uuid4()
        events = [
            SampleCreated(
                aggregate_id=agg_id_1,
                aggregate_type="Sample",
                aggregate_version=1,
                name="sample",
            ),
            OtherAggregateCreated(
                aggregate_id=agg_id_2,
                aggregate_type="OtherAggregate",
                aggregate_version=1,
                description="other",
            ),
        ]

        await given_events(harness, events)

        envelopes1 = [
            e
            async for e in harness.event_store.read_stream(
                StreamId(aggregate_id=agg_id_1, category="Sample")
            )
        ]
        envelopes2 = [
            e
            async for e in harness.event_store.read_stream(
                StreamId(aggregate_id=agg_id_2, category="OtherAggregate")
            )
        ]

        assert len(envelopes1) == 1
        assert len(envelopes2) == 1

    async def test_loads_variadic_events_and_returns_list(
        self, harness: InMemoryTestHarness
    ) -> None:
        """Test loading variadic events and returning the flattened event list."""
        agg_id = uuid4()
        e1 = SampleCreated(
            aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=1, name="v-sample"
        )
        e2 = SampleUpdated(
            aggregate_id=agg_id, aggregate_type="Sample", aggregate_version=2, new_value=99
        )
        returned = await given_events(harness, e1, e2)
        assert returned == [e1, e2]
        envelopes = [
            e
            async for e in harness.event_store.read_stream(
                StreamId(aggregate_id=agg_id, category="Sample")
            )
        ]
        assert [e.event for e in envelopes] == [e1, e2]
