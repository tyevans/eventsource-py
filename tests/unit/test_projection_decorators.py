"""
Unit tests for projection decorators.

Tests cover:
- @handles decorator functionality
- get_handled_event_type utility
- is_event_handler utility
"""

from pydantic import Field

from eventsource.events.base import DomainEvent
from eventsource.handlers import (
    get_handled_event_type,
    handles,
    is_event_handler,
)


# Sample events for testing
class OrderCreated(DomainEvent):
    """Sample event for testing."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    """Sample event for testing."""

    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


class TestHandlesDecorator:
    """Tests for @handles decorator."""

    def test_decorator_attaches_event_type(self) -> None:
        """@handles attaches event type to function."""

        @handles(OrderCreated)
        async def handler(self, conn, event: OrderCreated) -> None:
            pass

        assert hasattr(handler, "_handles_event_type")
        assert handler._handles_event_type is OrderCreated

    def test_decorator_preserves_function(self) -> None:
        """@handles returns the same function."""

        async def original_handler(self, conn, event: OrderCreated) -> None:
            pass

        decorated = handles(OrderCreated)(original_handler)

        assert decorated is original_handler

    def test_decorator_works_with_sync_functions(self) -> None:
        """@handles works with sync functions (validation is elsewhere)."""

        @handles(OrderCreated)
        def sync_handler(self, conn, event: OrderCreated) -> None:
            pass

        assert sync_handler._handles_event_type is OrderCreated

    def test_decorator_works_with_different_event_types(self) -> None:
        """@handles can be used with different event types."""

        @handles(OrderCreated)
        async def created_handler(self, conn, event: OrderCreated) -> None:
            pass

        @handles(OrderShipped)
        async def shipped_handler(self, conn, event: OrderShipped) -> None:
            pass

        assert created_handler._handles_event_type is OrderCreated
        assert shipped_handler._handles_event_type is OrderShipped

    def test_decorator_preserves_function_name(self) -> None:
        """@handles preserves function __name__."""

        @handles(OrderCreated)
        async def my_custom_handler(self, conn, event: OrderCreated) -> None:
            pass

        assert my_custom_handler.__name__ == "my_custom_handler"

    def test_decorator_preserves_docstring(self) -> None:
        """@handles preserves function docstring."""

        @handles(OrderCreated)
        async def handler_with_docs(self, conn, event: OrderCreated) -> None:
            """This is the docstring."""
            pass

        assert handler_with_docs.__doc__ == """This is the docstring."""


class TestGetHandledEventType:
    """Tests for get_handled_event_type utility."""

    def test_returns_event_type_for_decorated_function(self) -> None:
        """Returns event type for decorated function."""

        @handles(OrderCreated)
        async def handler(self, conn, event: OrderCreated) -> None:
            pass

        result = get_handled_event_type(handler)
        assert result is OrderCreated

    def test_returns_none_for_undecorated_function(self) -> None:
        """Returns None for undecorated function."""

        async def plain_handler(self, conn, event) -> None:
            pass

        result = get_handled_event_type(plain_handler)
        assert result is None

    def test_returns_none_for_lambda(self) -> None:
        """Returns None for lambda functions."""

        def func(event):
            return None

        result = get_handled_event_type(func)
        assert result is None

    def test_works_with_methods(self) -> None:
        """Works with class methods."""

        class MyProjection:
            @handles(OrderShipped)
            async def _handle_shipped(self, conn, event: OrderShipped) -> None:
                pass

        projection = MyProjection()
        result = get_handled_event_type(projection._handle_shipped)
        assert result is OrderShipped


class TestIsEventHandler:
    """Tests for is_event_handler utility."""

    def test_returns_true_for_decorated_function(self) -> None:
        """Returns True for decorated function."""

        @handles(OrderCreated)
        async def handler(self, conn, event: OrderCreated) -> None:
            pass

        assert is_event_handler(handler) is True

    def test_returns_false_for_undecorated_function(self) -> None:
        """Returns False for undecorated function."""

        async def plain_function(self, conn, event) -> None:
            pass

        assert is_event_handler(plain_function) is False

    def test_returns_false_for_regular_function(self) -> None:
        """Returns False for regular function without event handling."""

        def regular_function(x: int) -> int:
            return x * 2

        assert is_event_handler(regular_function) is False

    def test_returns_false_for_class(self) -> None:
        """Returns False for classes."""

        class SomeClass:
            pass

        assert is_event_handler(SomeClass) is False

    def test_works_with_bound_methods(self) -> None:
        """Works with bound methods."""

        class MyClass:
            @handles(OrderCreated)
            async def handler(self, event: OrderCreated) -> None:
                pass

            def not_a_handler(self) -> None:
                pass

        obj = MyClass()
        assert is_event_handler(obj.handler) is True
        assert is_event_handler(obj.not_a_handler) is False


class TestDecoratorEdgeCases:
    """Tests for edge cases in decorator usage."""

    def test_multiple_decorators_preserve_handles(self) -> None:
        """Multiple decorators don't break @handles."""

        def logging_decorator(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            # Preserve attributes
            wrapper._handles_event_type = getattr(func, "_handles_event_type", None)
            return wrapper

        @logging_decorator
        @handles(OrderCreated)
        async def handler(self, conn, event: OrderCreated) -> None:
            pass

        # The custom decorator should preserve the attribute
        assert get_handled_event_type(handler) is OrderCreated

    def test_decorator_on_staticmethod_like_function(self) -> None:
        """Decorator works on functions that look like static methods."""

        @handles(OrderCreated)
        async def static_like_handler(event: OrderCreated) -> None:
            pass

        assert is_event_handler(static_like_handler) is True
        assert get_handled_event_type(static_like_handler) is OrderCreated


# =============================================================================
# TD-006: Decorator Consolidation Tests
# =============================================================================


class TestHandlesDecoratorConsolidation:
    """Tests for consolidated @handles decorator (TD-006).

    These tests verify:
    - AC1: @handles from handlers is the canonical import
    - AC2: IDE autocomplete suggests canonical import (verified via __all__)
    """

    def test_canonical_import_from_handlers(self) -> None:
        """@handles from handlers is the canonical import."""
        from eventsource.handlers import handles as canonical_handles

        @canonical_handles(OrderCreated)
        def handler(self, event: OrderCreated) -> None:
            pass

        assert hasattr(handler, "_handles_event_type")
        assert handler._handles_event_type is OrderCreated

    def test_canonical_import_from_eventsource(self) -> None:
        """@handles from eventsource package is the canonical re-export."""
        from eventsource import handles as main_handles

        @main_handles(OrderShipped)
        def handler(self, event: OrderShipped) -> None:
            pass

        assert hasattr(handler, "_handles_event_type")
        assert handler._handles_event_type is OrderShipped

    def test_canonical_import_from_projections_module(self) -> None:
        """@handles from eventsource.projections module is canonical."""
        from eventsource.projections import handles as projections_handles

        @projections_handles(OrderCreated)
        async def handler(self, conn, event: OrderCreated) -> None:
            pass

        assert hasattr(handler, "_handles_event_type")
        assert handler._handles_event_type is OrderCreated

    def test_handles_in_main_package_all(self) -> None:
        """'handles' is in eventsource.__all__ for IDE autocomplete."""
        import eventsource

        assert "handles" in eventsource.__all__

    def test_handles_in_handlers_all(self) -> None:
        """'handles' is in eventsource.handlers.__all__ for IDE autocomplete."""
        from eventsource import handlers

        assert "handles" in handlers.__all__

    def test_handles_in_projections_all(self) -> None:
        """'handles' is in eventsource.projections.__all__ for IDE autocomplete."""
        from eventsource import projections

        assert "handles" in projections.__all__
