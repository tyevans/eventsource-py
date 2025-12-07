"""
Unit tests for exceptions module.

Tests all exception types and their error messages.
"""

from uuid import uuid4

import pytest

from eventsource.exceptions import (
    AggregateNotFoundError,
    CheckpointError,
    EventBusError,
    EventNotFoundError,
    EventSourceError,
    EventStoreError,
    OptimisticLockError,
    ProjectionError,
    SerializationError,
)


class TestEventSourceError:
    """Tests for the base EventSourceError."""

    def test_base_exception(self):
        """Test that EventSourceError can be raised with message."""
        with pytest.raises(EventSourceError) as exc_info:
            raise EventSourceError("Test error")
        assert str(exc_info.value) == "Test error"

    def test_is_exception_subclass(self):
        """Test that EventSourceError is a subclass of Exception."""
        assert issubclass(EventSourceError, Exception)


class TestOptimisticLockError:
    """Tests for OptimisticLockError."""

    def test_error_with_version_mismatch(self):
        """Test error message contains version information."""
        aggregate_id = uuid4()
        with pytest.raises(OptimisticLockError) as exc_info:
            raise OptimisticLockError(aggregate_id, expected_version=5, actual_version=7)

        error = exc_info.value
        assert error.aggregate_id == aggregate_id
        assert error.expected_version == 5
        assert error.actual_version == 7
        assert "expected version 5" in str(error)
        assert "current version is 7" in str(error)
        assert str(aggregate_id) in str(error)

    def test_is_eventsource_error_subclass(self):
        """Test that OptimisticLockError is a subclass of EventSourceError."""
        assert issubclass(OptimisticLockError, EventSourceError)


class TestEventNotFoundError:
    """Tests for EventNotFoundError."""

    def test_error_message_contains_event_id(self):
        """Test error message contains event ID."""
        event_id = uuid4()
        with pytest.raises(EventNotFoundError) as exc_info:
            raise EventNotFoundError(event_id)

        error = exc_info.value
        assert error.event_id == event_id
        assert str(event_id) in str(error)
        assert "Event not found" in str(error)

    def test_is_eventsource_error_subclass(self):
        """Test that EventNotFoundError is a subclass of EventSourceError."""
        assert issubclass(EventNotFoundError, EventSourceError)


class TestProjectionError:
    """Tests for ProjectionError."""

    def test_error_with_projection_and_event_info(self):
        """Test error message contains projection and event information."""
        projection_name = "OrderProjection"
        event_id = uuid4()
        message = "Failed to update read model"

        with pytest.raises(ProjectionError) as exc_info:
            raise ProjectionError(projection_name, event_id, message)

        error = exc_info.value
        assert error.projection_name == projection_name
        assert error.event_id == event_id
        assert projection_name in str(error)
        assert str(event_id) in str(error)
        assert message in str(error)

    def test_is_eventsource_error_subclass(self):
        """Test that ProjectionError is a subclass of EventSourceError."""
        assert issubclass(ProjectionError, EventSourceError)


class TestAggregateNotFoundError:
    """Tests for AggregateNotFoundError."""

    def test_error_without_aggregate_type(self):
        """Test error message without aggregate type."""
        aggregate_id = uuid4()
        with pytest.raises(AggregateNotFoundError) as exc_info:
            raise AggregateNotFoundError(aggregate_id)

        error = exc_info.value
        assert error.aggregate_id == aggregate_id
        assert error.aggregate_type is None
        assert str(aggregate_id) in str(error)
        assert "not found" in str(error)

    def test_error_with_aggregate_type(self):
        """Test error message with aggregate type."""
        aggregate_id = uuid4()
        aggregate_type = "Order"

        with pytest.raises(AggregateNotFoundError) as exc_info:
            raise AggregateNotFoundError(aggregate_id, aggregate_type)

        error = exc_info.value
        assert error.aggregate_id == aggregate_id
        assert error.aggregate_type == aggregate_type
        assert str(aggregate_id) in str(error)
        assert aggregate_type in str(error)
        assert "of type Order" in str(error)

    def test_is_eventsource_error_subclass(self):
        """Test that AggregateNotFoundError is a subclass of EventSourceError."""
        assert issubclass(AggregateNotFoundError, EventSourceError)


class TestEventStoreError:
    """Tests for EventStoreError."""

    def test_error_message(self):
        """Test EventStoreError with message."""
        with pytest.raises(EventStoreError) as exc_info:
            raise EventStoreError("Database connection failed")
        assert "Database connection failed" in str(exc_info.value)

    def test_is_eventsource_error_subclass(self):
        """Test that EventStoreError is a subclass of EventSourceError."""
        assert issubclass(EventStoreError, EventSourceError)


class TestEventBusError:
    """Tests for EventBusError."""

    def test_error_message(self):
        """Test EventBusError with message."""
        with pytest.raises(EventBusError) as exc_info:
            raise EventBusError("Message queue unavailable")
        assert "Message queue unavailable" in str(exc_info.value)

    def test_is_eventsource_error_subclass(self):
        """Test that EventBusError is a subclass of EventSourceError."""
        assert issubclass(EventBusError, EventSourceError)


class TestCheckpointError:
    """Tests for CheckpointError."""

    def test_error_message(self):
        """Test CheckpointError with message."""
        with pytest.raises(CheckpointError) as exc_info:
            raise CheckpointError("Checkpoint storage failed")
        assert "Checkpoint storage failed" in str(exc_info.value)

    def test_is_eventsource_error_subclass(self):
        """Test that CheckpointError is a subclass of EventSourceError."""
        assert issubclass(CheckpointError, EventSourceError)


class TestSerializationError:
    """Tests for SerializationError."""

    def test_error_with_event_type(self):
        """Test error message contains event type information."""
        event_type = "OrderCreated"
        message = "Invalid JSON structure"

        with pytest.raises(SerializationError) as exc_info:
            raise SerializationError(event_type, message)

        error = exc_info.value
        assert error.event_type == event_type
        assert event_type in str(error)
        assert message in str(error)
        assert "Serialization error" in str(error)

    def test_is_eventsource_error_subclass(self):
        """Test that SerializationError is a subclass of EventSourceError."""
        assert issubclass(SerializationError, EventSourceError)


class TestExceptionHierarchy:
    """Tests for exception hierarchy and catching."""

    def test_catch_all_with_base_class(self):
        """Test that all exceptions can be caught with EventSourceError."""
        exceptions = [
            OptimisticLockError(uuid4(), 1, 2),
            EventNotFoundError(uuid4()),
            ProjectionError("Test", uuid4(), "message"),
            AggregateNotFoundError(uuid4()),
            EventStoreError("test"),
            EventBusError("test"),
            CheckpointError("test"),
            SerializationError("Test", "message"),
        ]

        for exc in exceptions:
            try:
                raise exc
            except EventSourceError as caught:
                assert caught is exc
            except Exception:
                pytest.fail(f"{type(exc).__name__} was not caught by EventSourceError")

    def test_each_exception_is_distinct(self):
        """Test that each exception type can be caught separately."""
        aggregate_id = uuid4()

        # OptimisticLockError
        with pytest.raises(OptimisticLockError):
            raise OptimisticLockError(aggregate_id, 1, 2)

        # EventNotFoundError
        with pytest.raises(EventNotFoundError):
            raise EventNotFoundError(uuid4())

        # ProjectionError
        with pytest.raises(ProjectionError):
            raise ProjectionError("Test", uuid4(), "msg")

        # AggregateNotFoundError
        with pytest.raises(AggregateNotFoundError):
            raise AggregateNotFoundError(aggregate_id)

        # EventStoreError
        with pytest.raises(EventStoreError):
            raise EventStoreError("test")

        # EventBusError
        with pytest.raises(EventBusError):
            raise EventBusError("test")

        # CheckpointError
        with pytest.raises(CheckpointError):
            raise CheckpointError("test")

        # SerializationError
        with pytest.raises(SerializationError):
            raise SerializationError("Test", "msg")
