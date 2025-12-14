"""Unit tests for handler registry and signature validation."""

import pytest

from eventsource.events.base import DomainEvent
from eventsource.handlers import (
    HandlerRegistry,
    HandlerSignatureError,
    handles,
)


class TestEvent(DomainEvent):
    """Test event for registry tests."""

    event_type: str = "TestEvent"
    aggregate_type: str = "TestAggregate"


class AnotherTestEvent(DomainEvent):
    """Another test event for registry tests."""

    event_type: str = "AnotherTestEvent"
    aggregate_type: str = "TestAggregate"


class TestHandlerSignatureError:
    """Tests for HandlerSignatureError exception class."""

    def test_exception_attributes(self):
        """Exception should store all provided attributes."""
        error = HandlerSignatureError(
            handler_name="_handle_order",
            owner_name="OrderProjection",
            event_type=TestEvent,
            param_count=3,
            is_async_required=True,
        )

        assert error.handler_name == "_handle_order"
        assert error.owner_name == "OrderProjection"
        assert error.event_type is TestEvent
        assert error.param_count == 3
        assert error.is_async_required is True

    def test_exception_message_format_async(self):
        """Error message should include expected signatures for async handlers."""
        error = HandlerSignatureError(
            handler_name="_handle",
            owner_name="MyProjection",
            event_type=TestEvent,
            param_count=3,
            is_async_required=True,
        )

        message = str(error)
        assert "Handler '_handle' in MyProjection has invalid signature" in message
        assert "@handles(TestEvent)" in message
        assert "Expected one of:" in message
        assert "async def _handle(self, event: TestEvent) -> None" in message
        assert "async def _handle(self, context, event: TestEvent) -> None" in message
        assert "Got: 3 parameter(s) (excluding self)" in message
        assert "Hint:" in message

    def test_exception_message_format_sync(self):
        """Error message should show sync signatures when async not required."""
        error = HandlerSignatureError(
            handler_name="_apply",
            owner_name="MyAggregate",
            event_type=TestEvent,
            param_count=0,
            is_async_required=False,
        )

        message = str(error)
        assert "def _apply(self, event: TestEvent) -> None" in message
        assert "async def" not in message
        assert "Got: 0 parameter(s)" in message

    def test_exception_is_value_error(self):
        """HandlerSignatureError should be a subclass of ValueError."""
        error = HandlerSignatureError(
            handler_name="_handle",
            owner_name="Test",
            event_type=TestEvent,
            param_count=5,
        )
        assert isinstance(error, ValueError)


class TestHandlerSignatureValidation:
    """Tests for handler signature validation in HandlerRegistry."""

    def test_too_many_parameters_raises_signature_error(self):
        """Handler with too many parameters should raise HandlerSignatureError."""

        class BadHandler:
            @handles(TestEvent)
            async def _handle(self, a, b, c):  # 3 params
                pass

        with pytest.raises(HandlerSignatureError) as exc_info:
            HandlerRegistry(BadHandler(), require_async=True)

        error = exc_info.value
        assert error.handler_name == "_handle"
        assert error.owner_name == "BadHandler"
        assert error.event_type is TestEvent
        assert error.param_count == 3
        assert "Expected one of:" in str(error)
        assert "async def _handle(self, event: TestEvent)" in str(error)
        assert "async def _handle(self, context, event: TestEvent)" in str(error)

    def test_zero_parameters_raises_signature_error(self):
        """Handler with zero parameters should raise HandlerSignatureError."""

        class BadHandler:
            @handles(TestEvent)
            async def _handle(self):  # 0 params
                pass

        with pytest.raises(HandlerSignatureError) as exc_info:
            HandlerRegistry(BadHandler(), require_async=True)

        error = exc_info.value
        assert error.param_count == 0
        assert "Got: 0 parameter(s)" in str(error)

    def test_sync_when_async_required_shows_helpful_message(self):
        """Sync handler when async required should show helpful error message."""

        class BadHandler:
            @handles(TestEvent)
            def _handle(self, event):  # Not async
                pass

        with pytest.raises(ValueError) as exc_info:
            HandlerRegistry(BadHandler(), require_async=True)

        message = str(exc_info.value)
        assert "must be async" in message
        assert "async def" in message
        assert "Change:" in message
        assert "To:" in message

    def test_valid_single_param_handler_accepted(self):
        """Handler with single parameter (event) should be accepted."""

        class GoodHandler:
            @handles(TestEvent)
            async def _handle(self, event):
                pass

        registry = HandlerRegistry(GoodHandler(), require_async=True)
        assert registry.has_handler(TestEvent)
        assert registry.handler_count == 1

    def test_valid_two_param_handler_accepted(self):
        """Handler with two parameters (context, event) should be accepted."""

        class GoodHandler:
            @handles(TestEvent)
            async def _handle(self, context, event):
                pass

        registry = HandlerRegistry(GoodHandler(), require_async=True)
        assert registry.has_handler(TestEvent)
        assert registry.handler_count == 1

    def test_sync_handler_valid_when_async_not_required(self):
        """Sync handler should be valid when async is not required."""

        class SyncHandler:
            @handles(TestEvent)
            def _handle(self, event):
                pass

        registry = HandlerRegistry(SyncHandler(), require_async=False)
        assert registry.has_handler(TestEvent)
        handler_info = registry.get_handler(TestEvent)
        assert handler_info is not None
        assert handler_info.is_async is False

    def test_signature_error_for_sync_handler_shows_sync_example(self):
        """Signature error for sync handler (async not required) shows sync examples."""

        class BadHandler:
            @handles(TestEvent)
            def _handle(self, a, b, c, d):  # 4 params - too many
                pass

        with pytest.raises(HandlerSignatureError) as exc_info:
            HandlerRegistry(BadHandler(), require_async=False)

        message = str(exc_info.value)
        # Should show sync def, not async def
        assert "def _handle(self, event: TestEvent) -> None" in message
        # Should not have "async" in the examples
        assert "async def _handle" not in message

    def test_multiple_handlers_all_validated(self):
        """All handlers in a class should be validated."""

        class MultiHandler:
            @handles(TestEvent)
            async def _handle_test(self, event):
                pass

            @handles(AnotherTestEvent)
            async def _handle_another(self, a, b, c, d):  # Invalid
                pass

        with pytest.raises(HandlerSignatureError) as exc_info:
            HandlerRegistry(MultiHandler(), require_async=True)

        assert exc_info.value.handler_name == "_handle_another"

    def test_validate_on_init_can_be_disabled(self):
        """Validation can be disabled on init."""

        class BadHandler:
            @handles(TestEvent)
            async def _handle(self, a, b, c):  # Would fail validation
                pass

        # Should not raise when validate_on_init=False
        registry = HandlerRegistry(BadHandler(), require_async=True, validate_on_init=False)
        assert registry.has_handler(TestEvent)


class TestHandlerSignatureErrorImport:
    """Tests for HandlerSignatureError import from handlers module."""

    def test_exception_exported_from_handlers_module(self):
        """HandlerSignatureError should be importable from eventsource.handlers."""
        from eventsource.handlers import HandlerSignatureError as ImportedError

        assert ImportedError is HandlerSignatureError

    def test_exception_can_be_caught_programmatically(self):
        """Exception should be catchable for programmatic error handling."""

        class BadHandler:
            @handles(TestEvent)
            async def _handle(self, a, b, c):
                pass

        try:
            HandlerRegistry(BadHandler(), require_async=True)
            pytest.fail("Expected HandlerSignatureError to be raised")
        except HandlerSignatureError as e:
            # Should be able to access attributes for programmatic handling
            assert e.handler_name == "_handle"
            assert e.event_type is TestEvent
            assert e.param_count == 3


class TestHandlerRegistryBasic:
    """Basic tests for HandlerRegistry functionality."""

    def test_get_subscribed_events(self):
        """Should return list of event types with handlers."""

        class Handler:
            @handles(TestEvent)
            async def _handle_test(self, event):
                pass

            @handles(AnotherTestEvent)
            async def _handle_another(self, event):
                pass

        registry = HandlerRegistry(Handler(), require_async=True)
        events = registry.get_subscribed_events()

        assert len(events) == 2
        assert TestEvent in events
        assert AnotherTestEvent in events

    def test_has_handler(self):
        """Should correctly report whether handler exists."""

        class Handler:
            @handles(TestEvent)
            async def _handle(self, event):
                pass

        registry = HandlerRegistry(Handler(), require_async=True)

        assert registry.has_handler(TestEvent) is True
        assert registry.has_handler(AnotherTestEvent) is False

    def test_get_handler_returns_handler_info(self):
        """Should return HandlerInfo for registered event type."""

        class Handler:
            @handles(TestEvent)
            async def _handle_test(self, event):
                pass

        registry = HandlerRegistry(Handler(), require_async=True)
        handler_info = registry.get_handler(TestEvent)

        assert handler_info is not None
        assert handler_info.event_type is TestEvent
        assert handler_info.handler_name == "_handle_test"
        assert handler_info.is_async is True
        assert handler_info.param_count == 1

    def test_get_handler_returns_none_for_unregistered(self):
        """Should return None for event type without handler."""

        class Handler:
            @handles(TestEvent)
            async def _handle(self, event):
                pass

        registry = HandlerRegistry(Handler(), require_async=True)

        assert registry.get_handler(AnotherTestEvent) is None

    def test_repr(self):
        """Registry should have informative repr."""

        class Handler:
            @handles(TestEvent)
            async def _handle(self, event):
                pass

        registry = HandlerRegistry(Handler(), require_async=True)
        repr_str = repr(registry)

        assert "HandlerRegistry" in repr_str
        assert "Handler" in repr_str
        assert "handlers=1" in repr_str
