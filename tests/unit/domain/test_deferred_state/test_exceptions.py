"""Tests for AggregateNotCreatedError exception."""

from uuid import uuid4

import pytest

from eventsource.domain.exceptions import AggregateNotCreatedError, EventSourceError
from tests.unit.domain.test_deferred_state._fixtures import ExtractionProcess


class TestAggregateNotCreatedError:
    """Tests for AggregateNotCreatedError exception."""

    def test_error_message_includes_class_name(self) -> None:
        """Error message includes the aggregate class name."""
        error = AggregateNotCreatedError("MyAggregate")
        assert "MyAggregate" in str(error)
        assert "has not been created" in str(error)

    def test_error_message_includes_suggestion(self) -> None:
        """Error message includes suggestion when provided."""
        error = AggregateNotCreatedError("MyAggregate", suggestion="Call create() first")
        assert "Call create() first" in str(error)
        assert "Hint:" in str(error)

    def test_error_attributes(self) -> None:
        """Error has correct attributes."""
        error = AggregateNotCreatedError("MyAggregate", suggestion="hint")
        assert error.aggregate_class == "MyAggregate"
        assert error.suggestion == "hint"

    def test_error_without_suggestion(self) -> None:
        """Error without suggestion has no hint in message."""
        error = AggregateNotCreatedError("MyAggregate")
        assert error.suggestion is None
        assert "Hint:" not in str(error)

    def test_error_is_event_source_error(self) -> None:
        """AggregateNotCreatedError is an EventSourceError."""
        error = AggregateNotCreatedError("MyAggregate")
        assert isinstance(error, EventSourceError)


class TestAggregateNotCreatedErrorFromAggregate:
    """Tests for AggregateNotCreatedError raised from aggregates."""

    def test_error_includes_class_name_from_aggregate(self) -> None:
        """Error includes the actual aggregate class name."""
        process = ExtractionProcess(uuid4())

        with pytest.raises(AggregateNotCreatedError) as exc_info:
            _ = process.state

        assert exc_info.value.aggregate_class == "ExtractionProcess"

    def test_error_includes_helpful_suggestion(self) -> None:
        """Error includes a helpful suggestion."""
        process = ExtractionProcess(uuid4())

        with pytest.raises(AggregateNotCreatedError) as exc_info:
            _ = process.state

        assert "Call a creation method on ExtractionProcess first" in str(exc_info.value)
