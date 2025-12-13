"""Unit tests for event handler decorators."""

from eventsource.events.base import DomainEvent
from eventsource.handlers.decorators import (
    get_handled_event_type,
    handles,
    is_event_handler,
)


class TestEvent(DomainEvent):
    """Test event for decorator tests."""

    event_type: str = "TestEvent"
    aggregate_type: str = "TestAggregate"


class AnotherTestEvent(DomainEvent):
    """Another test event for decorator tests."""

    event_type: str = "AnotherTestEvent"
    aggregate_type: str = "TestAggregate"


class TestHandlesDecorator:
    """Tests for the @handles decorator."""

    def test_handles_decorator_attaches_event_type(self):
        """Decorator should attach the event type to the function."""

        @handles(TestEvent)
        def handler(self, event):
            pass

        assert hasattr(handler, "_handles_event_type")
        assert handler._handles_event_type is TestEvent

    def test_handles_preserves_function(self):
        """Decorator should preserve the original function attributes."""

        @handles(TestEvent)
        def my_handler(self, event):
            """Handler docstring."""
            return "result"

        assert my_handler.__name__ == "my_handler"
        assert my_handler.__doc__ == "Handler docstring."

    def test_handles_function_is_callable(self):
        """Decorated function should still be callable."""

        @handles(TestEvent)
        def handler(self, event):
            return "executed"

        # The function should be callable and return the expected value
        result = handler(None, None)
        assert result == "executed"

    def test_handles_works_with_async(self):
        """Decorator should work with async functions."""

        @handles(TestEvent)
        async def async_handler(self, conn, event):
            pass

        assert get_handled_event_type(async_handler) is TestEvent

    def test_handles_with_different_event_types(self):
        """Multiple handlers can be decorated with different event types."""

        @handles(TestEvent)
        def handler_one(self, event):
            pass

        @handles(AnotherTestEvent)
        def handler_two(self, event):
            pass

        assert get_handled_event_type(handler_one) is TestEvent
        assert get_handled_event_type(handler_two) is AnotherTestEvent


class TestGetHandledEventType:
    """Tests for get_handled_event_type utility function."""

    def test_returns_event_type_for_decorated(self):
        """Should return the event type for a decorated function."""

        @handles(TestEvent)
        def handler(self, event):
            pass

        assert get_handled_event_type(handler) is TestEvent

    def test_returns_none_for_undecorated(self):
        """Should return None for a function not decorated with @handles."""

        def regular_func():
            pass

        assert get_handled_event_type(regular_func) is None

    def test_returns_none_for_lambda(self):
        """Should return None for lambda functions."""

        def fn(x):
            return x

        assert get_handled_event_type(fn) is None

    def test_returns_event_type_for_method(self):
        """Should work with class methods."""

        class MyClass:
            @handles(TestEvent)
            def my_method(self, event):
                pass

        # Access the function from the class (not instance)
        assert get_handled_event_type(MyClass.my_method) is TestEvent


class TestIsEventHandler:
    """Tests for is_event_handler utility function."""

    def test_true_for_decorated(self):
        """Should return True for a decorated function."""

        @handles(TestEvent)
        def handler(self, event):
            pass

        assert is_event_handler(handler) is True

    def test_false_for_undecorated(self):
        """Should return False for an undecorated function."""

        def regular_func():
            pass

        assert is_event_handler(regular_func) is False

    def test_false_for_builtin_functions(self):
        """Should return False for builtin functions."""
        assert is_event_handler(len) is False
        assert is_event_handler(print) is False

    def test_true_for_async_handler(self):
        """Should return True for decorated async functions."""

        @handles(TestEvent)
        async def async_handler(self, conn, event):
            pass

        assert is_event_handler(async_handler) is True

    def test_false_for_class_without_handler(self):
        """Should return False for regular class methods."""

        class MyClass:
            def regular_method(self):
                pass

        assert is_event_handler(MyClass.regular_method) is False


class TestDecoratorIntegration:
    """Integration tests for decorator functions working together."""

    def test_decorator_and_utilities_work_together(self):
        """All decorator utilities should work together correctly."""

        @handles(TestEvent)
        def my_handler(self, event):
            pass

        # All three functions should provide consistent results
        assert is_event_handler(my_handler) is True
        assert get_handled_event_type(my_handler) is TestEvent
        assert hasattr(my_handler, "_handles_event_type")

    def test_undecorated_function_utilities_consistent(self):
        """Utilities should be consistent for undecorated functions."""

        def regular_func():
            pass

        assert is_event_handler(regular_func) is False
        assert get_handled_event_type(regular_func) is None
        assert not hasattr(regular_func, "_handles_event_type")
