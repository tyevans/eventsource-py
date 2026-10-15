"""Unit tests for projection decorators."""

from pydantic import Field

from eventsource.domain.decorators import (
    get_handled_event_type,
    handles,
    is_event_handler,
)
from eventsource.domain.event import DomainEvent


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str = Field(..., description="Order number")


class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str = Field(..., description="Tracking number")


class TestHandlesDecorator:
    def test_decorator_attaches_event_type(self) -> None:
        @handles(OrderCreated)
        async def handler(self, conn, event: OrderCreated) -> None:
            pass

        assert hasattr(handler, "_handles_event_type")
        assert handler._handles_event_type is OrderCreated

    def test_decorator_preserves_function(self) -> None:
        async def original_handler(self, conn, event: OrderCreated) -> None:
            pass

        decorated = handles(OrderCreated)(original_handler)
        assert decorated is original_handler

    def test_decorator_works_with_sync_functions(self) -> None:
        @handles(OrderCreated)
        def sync_handler(self, conn, event: OrderCreated) -> None:
            pass

        assert sync_handler._handles_event_type is OrderCreated

    def test_decorator_works_with_different_event_types(self) -> None:
        @handles(OrderCreated)
        async def created_handler(self, conn, event: OrderCreated) -> None:
            pass

        @handles(OrderShipped)
        async def shipped_handler(self, conn, event: OrderShipped) -> None:
            pass

        assert created_handler._handles_event_type is OrderCreated
        assert shipped_handler._handles_event_type is OrderShipped

    def test_decorator_preserves_function_name(self) -> None:
        @handles(OrderCreated)
        async def my_custom_handler(self, conn, event: OrderCreated) -> None:
            pass

        assert my_custom_handler.__name__ == "my_custom_handler"

    def test_decorator_preserves_docstring(self) -> None:
        @handles(OrderCreated)
        async def handler_with_docs(self, conn, event: OrderCreated) -> None:
            """This is the docstring."""
            pass

        assert handler_with_docs.__doc__ == """This is the docstring."""


class TestGetHandledEventType:
    def test_returns_event_type_for_decorated_function(self) -> None:
        @handles(OrderCreated)
        async def handler(self, conn, event: OrderCreated) -> None:
            pass

        assert get_handled_event_type(handler) is OrderCreated

    def test_returns_none_for_undecorated_function(self) -> None:
        async def plain_handler(self, conn, event) -> None:
            pass

        assert get_handled_event_type(plain_handler) is None

    def test_returns_none_for_lambda(self) -> None:
        def func(event):
            return None

        assert get_handled_event_type(func) is None

    def test_works_with_methods(self) -> None:
        class MyProjection:
            @handles(OrderShipped)
            async def _handle_shipped(self, conn, event: OrderShipped) -> None:
                pass

        projection = MyProjection()
        assert get_handled_event_type(projection._handle_shipped) is OrderShipped


class TestIsEventHandler:
    def test_returns_true_for_decorated_function(self) -> None:
        @handles(OrderCreated)
        async def handler(self, conn, event: OrderCreated) -> None:
            pass

        assert is_event_handler(handler) is True

    def test_returns_false_for_undecorated_function(self) -> None:
        async def plain_function(self, conn, event) -> None:
            pass

        assert is_event_handler(plain_function) is False

    def test_returns_false_for_regular_function(self) -> None:
        def regular_function(x: int) -> int:
            return x * 2

        assert is_event_handler(regular_function) is False

    def test_returns_false_for_class(self) -> None:
        class SomeClass:
            pass

        assert is_event_handler(SomeClass) is False

    def test_works_with_bound_methods(self) -> None:
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
    def test_multiple_decorators_preserve_handles(self) -> None:
        def logging_decorator(func):
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            wrapper._handles_event_type = getattr(func, "_handles_event_type", None)
            return wrapper

        @logging_decorator
        @handles(OrderCreated)
        async def handler(self, conn, event: OrderCreated) -> None:
            pass

        assert get_handled_event_type(handler) is OrderCreated

    def test_decorator_on_staticmethod_like_function(self) -> None:
        @handles(OrderCreated)
        async def static_like_handler(event: OrderCreated) -> None:
            pass

        assert is_event_handler(static_like_handler) is True
        assert get_handled_event_type(static_like_handler) is OrderCreated


class TestHandlesDecoratorConsolidation:
    def test_canonical_import_from_domain(self) -> None:
        from eventsource.domain.decorators import handles as canonical_handles

        @canonical_handles(OrderCreated)
        def handler(self, event: OrderCreated) -> None:
            pass

        assert hasattr(handler, "_handles_event_type")
        assert handler._handles_event_type is OrderCreated

    def test_canonical_import_from_eventsource(self) -> None:
        from eventsource import handles as main_handles

        @main_handles(OrderShipped)
        def handler(self, event: OrderShipped) -> None:
            pass

        assert hasattr(handler, "_handles_event_type")
        assert handler._handles_event_type is OrderShipped

    def test_handles_in_main_package_all(self) -> None:
        import eventsource

        assert "handles" in eventsource.__all__

    def test_handles_in_domain_decorators_all(self) -> None:
        from eventsource.domain import decorators

        assert "handles" in decorators.__all__
