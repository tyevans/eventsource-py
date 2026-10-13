"""Integration tests for EventAssertions."""

from __future__ import annotations

from uuid import uuid4

from eventsource.domain.event import DomainEvent
from eventsource.testing.assertions import EventAssertions
from tests.unit.testing.test_assertions._fixtures import (
    SampleCreated,
    SampleDeleted,
    SampleUpdated,
)


class TestEventAssertionsIntegration:
    """Integration tests for EventAssertions."""

    def test_chained_assertions(self) -> None:
        """Test using multiple assertions in sequence."""
        agg_id = uuid4()
        events: list[DomainEvent] = [
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
        ]
        assertions = EventAssertions(events)

        # Multiple assertions on same data
        assertions.assert_event_count(2)
        assertions.assert_event_sequence([SampleCreated, SampleUpdated])
        created = assertions.assert_event_published(SampleCreated)
        assert created.name == "test-sample"
        assertions.assert_no_event_published(SampleDeleted)

    def test_assertions_from_harness_published_events(self) -> None:
        """Test creating assertions from harness-like data structure."""
        # Simulate what would come from harness.published_events
        published_events: list[DomainEvent] = [
            SampleCreated(
                aggregate_id=uuid4(),
                aggregate_type="Sample",
                aggregate_version=1,
                name="from-harness",
            ),
        ]

        assertions = EventAssertions(published_events)
        event = assertions.assert_event_with_fields(SampleCreated, name="from-harness")
        assert event is not None
