"""Unit tests for event application, version tracking, and uncommitted event management."""

from __future__ import annotations

from uuid import uuid4

from tests.unit.domain.test_aggregate_root._fixtures import (
    CounterAggregate,
    CounterIncremented,
    DeclarativeCounterAggregate,
)


class TestEventApplication:
    """Tests for applying events to aggregates."""

    def test_apply_event_updates_version(self) -> None:
        """Applying event should update version to match event's aggregate_version."""
        aggregate = CounterAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=5,
        )
        aggregate.apply_event(event)
        assert aggregate.version == 1

    def test_apply_event_updates_state(self) -> None:
        """Applying event should update aggregate state."""
        aggregate = CounterAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=5,
        )
        aggregate.apply_event(event)
        assert aggregate.state is not None
        assert aggregate.state.value == 5

    def test_apply_event_tracks_uncommitted_by_default(self) -> None:
        """Applying event with is_new=True should track it as uncommitted."""
        aggregate = CounterAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=5,
        )
        aggregate.apply_event(event, is_new=True)
        assert len(aggregate.uncommitted_events) == 1
        assert aggregate.uncommitted_events[0] == event
        assert aggregate.has_uncommitted_events

    def test_apply_event_does_not_track_historical(self) -> None:
        """Applying event with is_new=False should not track it as uncommitted."""
        aggregate = CounterAggregate(uuid4())
        event = CounterIncremented(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Counter",
            aggregate_version=1,
            increment=5,
        )
        aggregate.apply_event(event, is_new=False)
        assert len(aggregate.uncommitted_events) == 0
        assert not aggregate.has_uncommitted_events

    def test_apply_multiple_events(self) -> None:
        """Applying multiple events should update state correctly."""
        aggregate = CounterAggregate(uuid4())

        for i in range(1, 4):
            event = CounterIncremented(
                aggregate_id=aggregate.aggregate_id,
                aggregate_type="Counter",
                aggregate_version=i,
                increment=10,
            )
            aggregate.apply_event(event)

        assert aggregate.version == 3
        assert aggregate.state is not None
        assert aggregate.state.value == 30
        assert len(aggregate.uncommitted_events) == 3

    def test_uncommitted_events_returns_copy(self) -> None:
        """uncommitted_events should return a copy to prevent external modification."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)

        events = aggregate.uncommitted_events
        events.clear()  # Modify the returned list

        # Original should be unchanged
        assert len(aggregate.uncommitted_events) == 1


class TestVersionTracking:
    """Tests for version tracking and optimistic concurrency support."""

    def test_get_next_version_for_new_aggregate(self) -> None:
        """get_next_version should return 1 for new aggregate."""
        aggregate = CounterAggregate(uuid4())
        assert aggregate.get_next_version() == 1

    def test_get_next_version_increments(self) -> None:
        """get_next_version should return version + 1."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        assert aggregate.version == 1
        assert aggregate.get_next_version() == 2

        aggregate.increment(3)
        assert aggregate.version == 2
        assert aggregate.get_next_version() == 3

    def test_version_matches_event_count(self) -> None:
        """Version should match the number of events applied."""
        aggregate = CounterAggregate(uuid4())

        for _i in range(5):
            aggregate.increment(1)

        assert aggregate.version == 5


class TestUncommittedEventManagement:
    """Tests for uncommitted event tracking and clearing."""

    def test_mark_events_as_committed_clears_uncommitted(self) -> None:
        """mark_events_as_committed should clear all uncommitted events."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)
        assert len(aggregate.uncommitted_events) == 2

        aggregate.mark_events_as_committed()

        assert len(aggregate.uncommitted_events) == 0
        assert not aggregate.has_uncommitted_events

    def test_mark_events_as_committed_preserves_state(self) -> None:
        """mark_events_as_committed should not affect aggregate state or version."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)

        state_before = aggregate.state
        version_before = aggregate.version

        aggregate.mark_events_as_committed()

        assert aggregate.state == state_before
        assert aggregate.version == version_before

    def test_clear_uncommitted_events_returns_events(self) -> None:
        """clear_uncommitted_events should return the events before clearing."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)

        events = aggregate.clear_uncommitted_events()

        assert len(events) == 2
        assert len(aggregate.uncommitted_events) == 0

    def test_can_add_new_events_after_commit(self) -> None:
        """After committing, new events should be tracked normally."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.mark_events_as_committed()

        aggregate.increment(3)

        assert len(aggregate.uncommitted_events) == 1
        assert aggregate.version == 2


class TestRaiseEventMethod:
    """Tests for the _raise_event convenience method."""

    def test_raise_event_applies_and_tracks(self) -> None:
        """_raise_event should apply event and track as uncommitted."""
        aggregate = DeclarativeCounterAggregate(uuid4())
        aggregate.increment(5)

        assert aggregate.version == 1
        assert aggregate.state is not None
        assert aggregate.state.value == 5
        assert len(aggregate.uncommitted_events) == 1


class TestIntegrationWithDomainEvent:
    """Tests for integration with DomainEvent features."""

    def test_events_have_correct_metadata(self) -> None:
        """Events created by commands should have correct aggregate metadata."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)

        event = aggregate.uncommitted_events[0]
        assert event.aggregate_id == aggregate.aggregate_id
        assert event.aggregate_type == "Counter"
        assert event.aggregate_version == 1

    def test_events_have_unique_event_ids(self) -> None:
        """Each event should have a unique event_id."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        aggregate.increment(3)

        events = aggregate.uncommitted_events
        assert events[0].event_id != events[1].event_id

    def test_events_have_occurred_at_timestamps(self) -> None:
        """Events should have occurred_at timestamps."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)

        event = aggregate.uncommitted_events[0]
        assert event.occurred_at is not None
