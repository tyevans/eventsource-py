"""Integration tests using BDD helpers together."""

from __future__ import annotations

from uuid import uuid4

from eventsource.domain import StreamId
from eventsource.testing import InMemoryTestHarness
from eventsource.testing.bdd import (
    given_events,
    then_event_count,
    then_event_published,
    then_no_events_published,
    when_command,
)
from tests.unit.testing.test_bdd._fixtures import (
    SampleAggregate,
    SampleCreated,
    SampleUpdated,
)


class TestBDDIntegration:
    """Integration tests using BDD helpers together."""

    async def test_full_bdd_scenario(self, harness: InMemoryTestHarness) -> None:
        """Test a complete BDD scenario using all helpers."""
        # Given: Events representing initial state
        agg_id = uuid4()
        await given_events(
            harness,
            [
                SampleCreated(
                    aggregate_id=agg_id,
                    aggregate_type="Sample",
                    aggregate_version=1,
                    name="my-sample",
                ),
            ],
        )

        # Create an aggregate and load from history
        aggregate = SampleAggregate(agg_id)
        envelopes = [
            e
            async for e in harness.event_store.read_stream(
                StreamId(aggregate_id=agg_id, category="Sample")
            )
        ]
        events = [e.event for e in envelopes]
        aggregate.load_from_history(events)

        # When: Execute a command
        new_events = when_command(aggregate, lambda a: a.update(99))

        # Publish the new events
        await harness.event_bus.publish(new_events)

        # Then: Verify the event was published
        event = then_event_published(harness, SampleUpdated, new_value=99)
        assert event.aggregate_id == agg_id

    async def test_bdd_scenario_with_no_op_command(self, harness: InMemoryTestHarness) -> None:
        """Test BDD scenario where command produces no events."""
        # Given: Some initial events
        await given_events(harness, [])

        # When: A no-op command is executed
        aggregate = SampleAggregate(uuid4())
        new_events = when_command(aggregate, lambda a: None)

        # Then: No events should have been produced
        assert len(new_events) == 0

        # And: No events should be published
        then_no_events_published(harness)

    async def test_clearing_events_between_scenarios(self, harness: InMemoryTestHarness) -> None:
        """Test that clear_published_events allows fresh assertions."""
        # First scenario
        event1 = SampleCreated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            name="first",
        )
        await harness.event_bus.publish([event1])
        then_event_count(harness, 1)

        # Clear for second scenario
        harness.clear_published_events()

        # Second scenario
        event2 = SampleUpdated(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
            new_value=42,
        )
        await harness.event_bus.publish([event2])

        # Should only see the second event
        then_event_count(harness, 1)
        then_event_published(harness, SampleUpdated)
        then_no_events_published(harness, SampleCreated)
