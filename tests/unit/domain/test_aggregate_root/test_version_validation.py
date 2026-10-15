"""Unit tests for event version validation feature (TD-004)."""

from __future__ import annotations

import logging
from uuid import UUID, uuid4

import pytest

from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import EventVersionError
from tests.unit.domain.test_aggregate_root._fixtures import (
    CounterAggregate,
    CounterIncremented,
    CounterNamed,
    DeclarativeCounterAggregate,
    LenientCounterAggregate,
)


def _inc(agg_id: UUID, version: int, increment: int = 5) -> CounterIncremented:
    return CounterIncremented(
        aggregate_id=agg_id,
        aggregate_type="Counter",
        aggregate_version=version,
        increment=increment,
    )


class TestEventVersionValidation:
    """Tests for event version validation feature (TD-004)."""

    def test_version_gap_raises_error(self) -> None:
        """AC1: Skipping versions raises EventVersionError."""
        aggregate = CounterAggregate(uuid4())
        event = _inc(aggregate.aggregate_id, version=3)
        with pytest.raises(EventVersionError) as exc_info:
            aggregate.apply_event(event)

        assert exc_info.value.expected_version == 1
        assert exc_info.value.actual_version == 3
        assert exc_info.value.aggregate_id == aggregate.aggregate_id
        assert exc_info.value.event_id == event.event_id

    def test_version_regression_raises_error(self) -> None:
        """AC2: Going backward in versions raises EventVersionError."""
        aggregate = CounterAggregate(uuid4())
        aggregate.apply_event(_inc(aggregate.aggregate_id, version=1))
        assert aggregate.version == 1

        with pytest.raises(EventVersionError) as exc_info:
            aggregate.apply_event(_inc(aggregate.aggregate_id, version=1, increment=3))

        assert exc_info.value.expected_version == 2
        assert exc_info.value.actual_version == 1

    def test_version_zero_event_raises_error(self) -> None:
        """Version 0 event should raise EventVersionError (version must be >= 1)."""
        aggregate = CounterAggregate(uuid4())
        with pytest.raises(EventVersionError) as exc_info:
            aggregate.apply_event(_inc(aggregate.aggregate_id, version=2))

        assert exc_info.value.expected_version == 1
        assert exc_info.value.actual_version == 2

    def test_historical_replay_skips_validation(self) -> None:
        """AC3: Events applied with is_new=False skip validation."""
        aggregate = CounterAggregate(uuid4())
        events: list[DomainEvent] = [
            _inc(aggregate.aggregate_id, version=1, increment=10),
            _inc(aggregate.aggregate_id, version=5, increment=5),
            _inc(aggregate.aggregate_id, version=3, increment=3),
        ]
        aggregate.load_from_history(events)

        assert aggregate.version == 3
        assert aggregate.state is not None
        assert aggregate.state.value == 18

    def test_validation_disabled_accepts_any_version(self) -> None:
        """AC4: validate_versions=False allows any version."""
        aggregate = LenientCounterAggregate(uuid4())
        aggregate.apply_event(_inc(aggregate.aggregate_id, version=99))

        assert aggregate.version == 99
        assert aggregate.state is not None
        assert aggregate.state.value == 5

    def test_error_message_includes_version_details(self) -> None:
        """AC5: Error messages include expected vs actual version."""
        aggregate = CounterAggregate(uuid4())
        event = _inc(aggregate.aggregate_id, version=5)
        with pytest.raises(EventVersionError) as exc_info:
            aggregate.apply_event(event)

        error_message = str(exc_info.value)
        assert "expected version 1" in error_message
        assert "got 5" in error_message
        assert str(aggregate.aggregate_id) in error_message
        assert str(event.event_id) in error_message

    def test_validation_disabled_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """AC6: Mismatched versions log warning when validation disabled."""
        aggregate = LenientCounterAggregate(uuid4())
        with caplog.at_level(logging.WARNING):
            aggregate.apply_event(_inc(aggregate.aggregate_id, version=99))

        assert "Version mismatch" in caplog.text
        assert "validation disabled" in caplog.text
        assert "expected 1" in caplog.text
        assert "got 99" in caplog.text

    def test_correct_version_does_not_log_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Correct version should not log any warning even with validation disabled."""
        aggregate = LenientCounterAggregate(uuid4())
        with caplog.at_level(logging.WARNING):
            aggregate.apply_event(_inc(aggregate.aggregate_id, version=1))

        assert "Version mismatch" not in caplog.text

    def test_validation_enabled_by_default(self) -> None:
        """Version validation should be enabled by default."""
        assert CounterAggregate(uuid4()).validate_versions is True

    def test_command_methods_work_with_validation(self) -> None:
        """Command methods should work correctly with version validation enabled."""
        aggregate = CounterAggregate(uuid4())
        aggregate.increment(5)
        assert aggregate.version == 1
        aggregate.increment(3)
        assert aggregate.version == 2
        aggregate.decrement(2)
        assert aggregate.version == 3
        assert aggregate.state is not None
        assert aggregate.state.value == 6

    def test_declarative_aggregate_validation(self) -> None:
        """DeclarativeAggregate should also support version validation."""
        aggregate = DeclarativeCounterAggregate(uuid4())
        with pytest.raises(EventVersionError):
            aggregate.apply_event(_inc(aggregate.aggregate_id, version=5))

    def test_version_validation_after_history_load(self) -> None:
        """Version validation should work correctly after loading history."""
        agg_id = uuid4()
        events: list[DomainEvent] = [
            _inc(agg_id, version=1, increment=10),
            _inc(agg_id, version=2, increment=5),
        ]
        aggregate = CounterAggregate(agg_id)
        aggregate.load_from_history(events)
        assert aggregate.version == 2

        with pytest.raises(EventVersionError) as exc_info:
            aggregate.apply_event(_inc(agg_id, version=5, increment=1))

        assert exc_info.value.expected_version == 3
        assert exc_info.value.actual_version == 5

    def test_version_validation_with_multiple_event_types(self) -> None:
        """Version validation should work with different event types."""
        agg_id = uuid4()
        aggregate = CounterAggregate(agg_id)
        aggregate.apply_event(_inc(agg_id, version=1, increment=10))

        event2 = CounterNamed(
            aggregate_id=agg_id,
            aggregate_type="Counter",
            aggregate_version=5,
            name="TestCounter",
        )
        with pytest.raises(EventVersionError) as exc_info:
            aggregate.apply_event(event2)

        assert exc_info.value.expected_version == 2
        assert exc_info.value.actual_version == 5
