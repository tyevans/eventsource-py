"""
Unit tests for BDD test helpers.

Tests the given_events, when_command, then_event_published,
then_no_events_published, then_event_sequence, and then_event_count helpers.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from eventsource.aggregates.base import AggregateRoot
from eventsource.events.base import DomainEvent
from eventsource.testing import InMemoryTestHarness
from eventsource.testing.bdd import (
    given_events,
    then_event_count,
    then_event_published,
    then_event_sequence,
    then_no_events_published,
    when_command,
)

# =============================================================================
# Test Domain Events
# =============================================================================


class SampleCreated(DomainEvent):
    """Test event for sample creation."""

    aggregate_type: str = "Sample"
    event_type: str = "SampleCreated"
    name: str


class SampleUpdated(DomainEvent):
    """Test event for sample update."""

    aggregate_type: str = "Sample"
    event_type: str = "SampleUpdated"
    new_value: int


class SampleDeleted(DomainEvent):
    """Test event for sample deletion."""

    aggregate_type: str = "Sample"
    event_type: str = "SampleDeleted"


class OtherAggregateCreated(DomainEvent):
    """Test event for a different aggregate type."""

    aggregate_type: str = "OtherAggregate"
    event_type: str = "OtherAggregateCreated"
    description: str


# =============================================================================
# Test Aggregate
# =============================================================================


class SampleState(BaseModel):
    """State model for test aggregate."""

    name: str = ""
    value: int = 0


class SampleAggregate(AggregateRoot[SampleState]):
    """Test aggregate for BDD helper tests."""

    aggregate_type: str = "Sample"

    def _get_initial_state(self) -> SampleState:
        """Return initial state."""
        return SampleState()

    def _apply(self, event: DomainEvent) -> None:
        """Apply event to state."""
        if isinstance(event, SampleCreated):
            self._state = SampleState(name=event.name, value=0)
        elif isinstance(event, SampleUpdated) and self._state:
            self._state = self._state.model_copy(update={"value": event.new_value})

    def create(self, name: str) -> None:
        """Create the sample."""
        event = SampleCreated(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            name=name,
        )
        self._raise_event(event)

    def update(self, new_value: int) -> None:
        """Update the sample."""
        event = SampleUpdated(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            new_value=new_value,
        )
        self._raise_event(event)

    def delete(self) -> None:
        """Delete the sample."""
        event = SampleDeleted(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
        )
        self._raise_event(event)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def harness() -> InMemoryTestHarness:
    """Create a fresh test harness."""
    return InMemoryTestHarness()


@pytest.fixture
def aggregate_id() -> uuid4:
    """Create a test aggregate ID."""
    return uuid4()


@pytest.fixture
def sample_aggregate(aggregate_id: uuid4) -> SampleAggregate:
    """Create a test aggregate."""
    return SampleAggregate(aggregate_id)


# =============================================================================
# Tests for given_events
# =============================================================================


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

        stream = await harness.event_store.get_events(agg_id, "Sample")
        assert len(stream.events) == 1
        assert isinstance(stream.events[0], SampleCreated)
        assert stream.events[0].name == "test-sample"

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

        stream = await harness.event_store.get_events(agg_id, "Sample")
        assert len(stream.events) == 3
        assert isinstance(stream.events[0], SampleCreated)
        assert isinstance(stream.events[1], SampleUpdated)
        assert isinstance(stream.events[2], SampleDeleted)

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

        stream1 = await harness.event_store.get_events(agg_id_1, "Sample")
        stream2 = await harness.event_store.get_events(agg_id_2, "Sample")

        assert len(stream1.events) == 1
        assert stream1.events[0].name == "sample-1"
        assert len(stream2.events) == 1
        assert stream2.events[0].name == "sample-2"

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

        stream1 = await harness.event_store.get_events(agg_id_1, "Sample")
        stream2 = await harness.event_store.get_events(agg_id_2, "OtherAggregate")

        assert len(stream1.events) == 1
        assert len(stream2.events) == 1


# =============================================================================
# Tests for when_command
# =============================================================================


class TestWhenCommand:
    """Tests for the when_command BDD helper."""

    def test_executes_command_and_returns_new_events(
        self, sample_aggregate: SampleAggregate
    ) -> None:
        """Test that when_command executes command and returns resulting events."""
        new_events = when_command(sample_aggregate, lambda a: a.create("test"))

        assert len(new_events) == 1
        assert isinstance(new_events[0], SampleCreated)
        assert new_events[0].name == "test"

    def test_returns_only_new_events(self, sample_aggregate: SampleAggregate) -> None:
        """Test that only events from the command are returned, not pre-existing ones."""
        # First create some initial events
        sample_aggregate.create("initial")
        initial_count = len(sample_aggregate.uncommitted_events)
        assert initial_count == 1

        # Now execute another command
        new_events = when_command(sample_aggregate, lambda a: a.update(42))

        # Should only return the update event, not the create event
        assert len(new_events) == 1
        assert isinstance(new_events[0], SampleUpdated)
        assert new_events[0].new_value == 42

    def test_command_producing_multiple_events(self, sample_aggregate: SampleAggregate) -> None:
        """Test command that produces multiple events."""

        # Create a command that produces multiple events
        def multi_command(agg: SampleAggregate) -> None:
            agg.create("test")
            agg.update(100)

        new_events = when_command(sample_aggregate, multi_command)

        assert len(new_events) == 2
        assert isinstance(new_events[0], SampleCreated)
        assert isinstance(new_events[1], SampleUpdated)

    def test_command_producing_no_events(self, sample_aggregate: SampleAggregate) -> None:
        """Test command that produces no events (no-op)."""

        # Define a no-op command
        def noop_command(agg: SampleAggregate) -> None:
            pass

        new_events = when_command(sample_aggregate, noop_command)

        assert len(new_events) == 0


# =============================================================================
# Tests for then_event_published
# =============================================================================


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


# =============================================================================
# Tests for then_no_events_published
# =============================================================================


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


# =============================================================================
# Tests for then_event_sequence
# =============================================================================


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


# =============================================================================
# Tests for then_event_count
# =============================================================================


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


# =============================================================================
# Integration Tests (BDD Helpers Together)
# =============================================================================


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
        stream = await harness.event_store.get_events(agg_id, "Sample")
        aggregate.load_from_history(stream.events)

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
