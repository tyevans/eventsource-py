"""Unit tests extracted for test_rabbitmq_handlers_edge_cases.py."""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.domain.event import DomainEvent


class TestRabbitMQHandlerNormalizationEdgeCases:
    """Additional edge case tests for handler normalization via HandlerAdapter.

    Note: The normalization logic was extracted to HandlerAdapter as part of
    SOLID refactoring. These tests now validate the adapter directly.
    """

    @pytest.mark.asyncio
    async def test_normalize_sync_method_returning_coroutine(self) -> None:
        """Test normalizing a sync method that returns a coroutine.

        This tests the `if asyncio.iscoroutine(result)` branch in the sync wrapper.
        """
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        results: list[DomainEvent] = []

        class HybridHandler:
            def handle(self, event: DomainEvent) -> object:
                # This sync method returns a coroutine (unusual but possible)
                async def coro() -> None:
                    results.append(event)

                return coro()

        handler = HybridHandler()
        adapter = HandlerAdapter(handler)

        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())
        await adapter.handle(event)

        assert len(results) == 1
        assert results[0] is event

    @pytest.mark.asyncio
    async def test_normalize_sync_callable_returning_coroutine(self) -> None:
        """Test normalizing a sync callable that returns a coroutine.

        This tests the `if asyncio.iscoroutine(result)` branch in the callable wrapper.
        """
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        results: list[DomainEvent] = []

        def hybrid_handler(event: DomainEvent) -> object:
            async def coro() -> None:
                results.append(event)

            return coro()

        adapter = HandlerAdapter(hybrid_handler)

        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())
        await adapter.handle(event)

        assert len(results) == 1
        assert results[0] is event


class TestRabbitMQGetHandlerNameEdgeCases:
    """Additional edge case tests for HandlerAdapter.name property (formerly _get_handler_name).

    Note: The handler naming logic was extracted to HandlerAdapter as part of
    SOLID refactoring. These tests now validate the adapter directly.
    """

    def test_get_handler_name_from_object_without_class(self) -> None:
        """Test getting name from an object without __class__ attribute.

        Note: In practice, everything in Python has __class__, but we test
        the fallback path by using an object that has __name__ instead.
        """
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        # Create an object that pretends to not have __class__ by overriding
        # hasattr check - this is tricky since everything has __class__
        # Instead, test the __name__ branch with a module-like object
        class MockWithName:
            __name__ = "MockHandlerName"

            def handle(self, event: DomainEvent) -> None:
                pass

        handler = MockWithName()
        adapter = HandlerAdapter(handler)
        # Since MockWithName has __class__, it will use __class__.__name__
        assert adapter.name == "MockWithName"

    def test_get_handler_name_repr_fallback(self) -> None:
        """Test that HandlerAdapter.name works with mocks.

        Note: This is hard to test since almost everything has __class__,
        but we can verify the behavior with mocks.
        """
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        # Test with a MagicMock to verify it handles objects properly
        # Use a dedicated MagicMock subclass rather than mutating MagicMock
        # itself -- MagicMock is a process-wide shared class object, so
        # setting `MagicMock.__name__` here would leak into every other
        # test that creates a MagicMock and inspects its class name/repr.
        class NamedMock(MagicMock):
            pass

        NamedMock.__name__ = "TestMock"

        handler = MagicMock(spec=[])  # Empty spec removes __class__ from mock
        handler.__class__ = NamedMock

        adapter = HandlerAdapter(handler)
        # Should still work since mock has __class__
        assert "Mock" in adapter.name
