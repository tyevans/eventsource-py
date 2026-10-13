"""Unit tests for aggregate exception classes."""

from __future__ import annotations

from uuid import uuid4

from eventsource.domain.exceptions import (
    EventSourceError,
    EventVersionError,
    UnhandledEventError,
)


class TestEventVersionErrorException:
    """Tests for EventVersionError exception class."""

    def test_exception_attributes(self) -> None:
        """EventVersionError should have all expected attributes."""
        event_id = uuid4()
        aggregate_id = uuid4()

        error = EventVersionError(
            expected_version=5,
            actual_version=3,
            event_id=event_id,
            aggregate_id=aggregate_id,
        )

        assert error.expected_version == 5
        assert error.actual_version == 3
        assert error.event_id == event_id
        assert error.aggregate_id == aggregate_id

    def test_exception_message_format(self) -> None:
        """EventVersionError message should be informative."""
        event_id = uuid4()
        aggregate_id = uuid4()

        error = EventVersionError(
            expected_version=5,
            actual_version=3,
            event_id=event_id,
            aggregate_id=aggregate_id,
        )

        message = str(error)
        assert f"aggregate {aggregate_id}" in message
        assert "expected version 5" in message
        assert "got 3" in message
        assert f"event_id: {event_id}" in message

    def test_exception_is_subclass_of_eventsource_error(self) -> None:
        """EventVersionError should be a subclass of EventSourceError."""
        error = EventVersionError(
            expected_version=1,
            actual_version=2,
            event_id=uuid4(),
            aggregate_id=uuid4(),
        )

        assert isinstance(error, EventSourceError)
        assert isinstance(error, Exception)


class TestUnhandledEventErrorException:
    """Tests for UnhandledEventError exception class."""

    def test_exception_attributes(self) -> None:
        """UnhandledEventError should have all expected attributes."""
        event_id = uuid4()

        error = UnhandledEventError(
            event_type="OrderShipped",
            event_id=event_id,
            handler_class="OrderAggregate",
            available_handlers=["OrderCreated", "OrderCancelled"],
        )

        assert error.event_type == "OrderShipped"
        assert error.event_id == event_id
        assert error.handler_class == "OrderAggregate"
        assert error.available_handlers == ["OrderCreated", "OrderCancelled"]

    def test_exception_message_format(self) -> None:
        """UnhandledEventError message should be informative."""
        event_id = uuid4()

        error = UnhandledEventError(
            event_type="OrderShipped",
            event_id=event_id,
            handler_class="OrderAggregate",
            available_handlers=["OrderCreated", "OrderCancelled"],
        )

        message = str(error)
        assert "OrderShipped" in message
        assert "OrderAggregate" in message
        assert "OrderCreated" in message
        assert "OrderCancelled" in message
        assert "@handles" in message

    def test_exception_message_with_no_handlers(self) -> None:
        """UnhandledEventError message handles empty handler list."""
        error = UnhandledEventError(
            event_type="OrderShipped",
            event_id=uuid4(),
            handler_class="OrderAggregate",
            available_handlers=[],
        )

        message = str(error)
        assert "none" in message

    def test_exception_is_subclass_of_eventsource_error(self) -> None:
        """UnhandledEventError should be a subclass of EventSourceError."""
        error = UnhandledEventError(
            event_type="OrderShipped",
            event_id=uuid4(),
            handler_class="OrderAggregate",
            available_handlers=[],
        )

        assert isinstance(error, EventSourceError)
        assert isinstance(error, Exception)
