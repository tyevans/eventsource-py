"""Unit tests for exceptions module."""

from uuid import uuid4

import pytest

from eventsource.domain.exceptions import (
    AggregateNotFoundError,
    EventBusError,
    EventNotFoundError,
    EventSourceError,
    EventStoreError,
    OptimisticLockError,
    ProjectionError,
    SerializationError,
)
from eventsource.ports.exceptions import CheckpointError


class TestEventSourceError:
    """Tests for the base EventSourceError."""

    def test_base_exception(self):
        with pytest.raises(EventSourceError) as exc_info:
            raise EventSourceError("Test error")
        assert str(exc_info.value) == "Test error"

    def test_is_exception_subclass(self):
        assert issubclass(EventSourceError, Exception)


class TestOptimisticLockError:
    """Tests for OptimisticLockError."""

    def test_error_with_version_mismatch(self):
        aggregate_id = uuid4()
        with pytest.raises(OptimisticLockError) as exc_info:
            raise OptimisticLockError(aggregate_id, expected_version=5, actual_version=7)
        error = exc_info.value
        assert error.aggregate_id == aggregate_id
        assert (error.expected_version, error.actual_version) == (5, 7)
        assert "expected version 5" in str(error) and "current version is 7" in str(error)
        assert str(aggregate_id) in str(error)

    def test_is_eventsource_error_subclass(self):
        assert issubclass(OptimisticLockError, EventSourceError)


class TestEventNotFoundError:
    """Tests for EventNotFoundError."""

    def test_error_message_contains_event_id(self):
        event_id = uuid4()
        with pytest.raises(EventNotFoundError) as exc_info:
            raise EventNotFoundError(event_id)
        error = exc_info.value
        assert error.event_id == event_id and str(event_id) in str(error)
        assert "Event not found" in str(error)

    def test_is_eventsource_error_subclass(self):
        assert issubclass(EventNotFoundError, EventSourceError)


class TestProjectionError:
    """Tests for ProjectionError."""

    def test_error_with_projection_and_event_info(self):
        projection_name, event_id, message = (
            "OrderProjection",
            uuid4(),
            "Failed to update read model",
        )
        with pytest.raises(ProjectionError) as exc_info:
            raise ProjectionError(projection_name, event_id, message)
        error = exc_info.value
        assert error.projection_name == projection_name and error.event_id == event_id
        assert projection_name in str(error) and str(event_id) in str(error)
        assert message in str(error)

    def test_is_eventsource_error_subclass(self):
        assert issubclass(ProjectionError, EventSourceError)


class TestAggregateNotFoundError:
    """Tests for AggregateNotFoundError."""

    def test_error_without_aggregate_type(self):
        aggregate_id = uuid4()
        with pytest.raises(AggregateNotFoundError) as exc_info:
            raise AggregateNotFoundError(aggregate_id)
        error = exc_info.value
        assert error.aggregate_id == aggregate_id and error.aggregate_type is None
        assert str(aggregate_id) in str(error) and "not found" in str(error)

    def test_error_with_aggregate_type(self):
        aggregate_id, aggregate_type = uuid4(), "Order"
        with pytest.raises(AggregateNotFoundError) as exc_info:
            raise AggregateNotFoundError(aggregate_id, aggregate_type)
        error = exc_info.value
        assert error.aggregate_id == aggregate_id and error.aggregate_type == aggregate_type
        assert str(aggregate_id) in str(error) and "of type Order" in str(error)

    def test_is_eventsource_error_subclass(self):
        assert issubclass(AggregateNotFoundError, EventSourceError)


class TestEventStoreError:
    """Tests for EventStoreError."""

    def test_error_message(self):
        with pytest.raises(EventStoreError) as exc_info:
            raise EventStoreError("Database connection failed")
        assert "Database connection failed" in str(exc_info.value)

    def test_is_eventsource_error_subclass(self):
        assert issubclass(EventStoreError, EventSourceError)


class TestEventBusError:
    """Tests for EventBusError."""

    def test_error_message(self):
        with pytest.raises(EventBusError) as exc_info:
            raise EventBusError("Message queue unavailable")
        assert "Message queue unavailable" in str(exc_info.value)

    def test_is_eventsource_error_subclass(self):
        assert issubclass(EventBusError, EventSourceError)


class TestCheckpointError:
    """Tests for CheckpointError."""

    def test_error_message(self):
        with pytest.raises(CheckpointError) as exc_info:
            raise CheckpointError("Checkpoint storage failed")
        assert "Checkpoint storage failed" in str(exc_info.value)

    def test_is_eventsource_error_subclass(self):
        assert issubclass(CheckpointError, EventSourceError)


class TestSerializationError:
    """Tests for SerializationError."""

    def test_error_with_event_type(self):
        event_type, message = "OrderCreated", "Invalid JSON structure"
        with pytest.raises(SerializationError) as exc_info:
            raise SerializationError(event_type, message)
        error = exc_info.value
        assert error.event_type == event_type and event_type in str(error)
        assert message in str(error) and "Serialization error" in str(error)

    def test_is_eventsource_error_subclass(self):
        assert issubclass(SerializationError, EventSourceError)


class TestExceptionHierarchy:
    """Tests for exception hierarchy and catching."""

    def test_catch_all_with_base_class(self):
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
        aggregate_id = uuid4()
        pairs = [
            (OptimisticLockError, OptimisticLockError(aggregate_id, 1, 2)),
            (EventNotFoundError, EventNotFoundError(uuid4())),
            (ProjectionError, ProjectionError("Test", uuid4(), "msg")),
            (AggregateNotFoundError, AggregateNotFoundError(aggregate_id)),
            (EventStoreError, EventStoreError("test")),
            (EventBusError, EventBusError("test")),
            (CheckpointError, CheckpointError("test")),
            (SerializationError, SerializationError("Test", "msg")),
        ]
        for exc_cls, inst in pairs:
            with pytest.raises(exc_cls):
                raise inst
