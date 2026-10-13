"""Tests for the when_command BDD helper."""

from __future__ import annotations

from eventsource.testing.bdd import when_command
from tests.unit.testing.test_bdd._fixtures import (
    SampleAggregate,
    SampleCreated,
    SampleUpdated,
)


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
