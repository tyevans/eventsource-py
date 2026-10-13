"""Unit tests extracted for test_rabbitmq_handlers.py."""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.domain.event import DomainEvent


class TestRabbitMQHandlerNormalization:
    """Tests for handler normalization via HandlerAdapter (P1-009).

    Note: The normalization logic was extracted to HandlerAdapter as part of
    SOLID refactoring. These tests now validate the adapter directly and
    verify that RabbitMQ bus correctly uses it.
    """

    def test_normalize_async_function(self) -> None:
        """Test normalizing an async function handler via HandlerAdapter."""
        import asyncio

        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        async def async_handler(event: DomainEvent) -> None:
            pass

        adapter = HandlerAdapter(async_handler)
        assert adapter._original is async_handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_normalize_sync_function(self) -> None:
        """Test normalizing a sync function handler via HandlerAdapter."""
        import asyncio

        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        def sync_handler(event: DomainEvent) -> None:
            pass

        adapter = HandlerAdapter(sync_handler)
        assert adapter._original is sync_handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_normalize_async_handler_class(self) -> None:
        """Test normalizing a handler class with async handle method."""
        import asyncio

        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        class AsyncHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = AsyncHandler()
        adapter = HandlerAdapter(handler)
        assert adapter._original is handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_normalize_sync_handler_class(self) -> None:
        """Test normalizing a handler class with sync handle method."""
        import asyncio

        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        class SyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        handler = SyncHandler()
        adapter = HandlerAdapter(handler)
        assert adapter._original is handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_normalize_invalid_handler_raises_type_error(self) -> None:
        """Test that normalizing an invalid handler raises TypeError."""
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        with pytest.raises(TypeError, match="Handler must have a handle\\(\\) method"):
            HandlerAdapter("not a handler")  # type: ignore[arg-type]

    def test_normalize_invalid_handler_with_number(self) -> None:
        """Test that normalizing a number raises TypeError."""
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        with pytest.raises(TypeError, match="Handler must have a handle\\(\\) method"):
            HandlerAdapter(42)  # type: ignore[arg-type]

    def test_normalize_lambda(self) -> None:
        """Test normalizing a lambda handler."""
        import asyncio

        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        handler = lambda event: None  # noqa: E731
        adapter = HandlerAdapter(handler)
        assert adapter._original is handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    @pytest.mark.asyncio
    async def test_normalized_sync_handler_invocation(self) -> None:
        """Test that normalized sync handler can be invoked."""
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        results: list[DomainEvent] = []

        def sync_handler(event: DomainEvent) -> None:
            results.append(event)

        adapter = HandlerAdapter(sync_handler)

        # Create a test event - need a concrete subclass with event_type
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        # Invoke the adapter
        await adapter.handle(event)

        assert len(results) == 1
        assert results[0] is event

    @pytest.mark.asyncio
    async def test_normalized_async_handler_invocation(self) -> None:
        """Test that normalized async handler can be invoked."""
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        results: list[DomainEvent] = []

        async def async_handler(event: DomainEvent) -> None:
            results.append(event)

        adapter = HandlerAdapter(async_handler)

        # Create a test event - need a concrete subclass with event_type
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        # Invoke the adapter
        await adapter.handle(event)

        assert len(results) == 1
        assert results[0] is event

    @pytest.mark.asyncio
    async def test_normalized_handler_class_invocation(self) -> None:
        """Test that normalized handler class can be invoked."""
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        results: list[DomainEvent] = []

        class TestHandler:
            async def handle(self, event: DomainEvent) -> None:
                results.append(event)

        handler = TestHandler()
        adapter = HandlerAdapter(handler)

        # Create a test event - need a concrete subclass with event_type
        class TestEvent(DomainEvent):
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        # Invoke the adapter
        await adapter.handle(event)

        assert len(results) == 1
        assert results[0] is event


class TestRabbitMQGetHandlerName:
    """Tests for HandlerAdapter.name property (formerly _get_handler_name method) (P1-009).

    Note: The handler naming logic was extracted to HandlerAdapter as part of
    SOLID refactoring. These tests now validate the adapter directly.
    """

    def test_get_handler_name_from_class(self) -> None:
        """Test getting name from a class instance."""
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        class MyHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = MyHandler()
        adapter = HandlerAdapter(handler)
        assert adapter.name == "MyHandler"

    def test_get_handler_name_from_function(self) -> None:
        """Test getting name from a function."""
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        def my_handler_func(event: DomainEvent) -> None:
            pass

        adapter = HandlerAdapter(my_handler_func)
        assert adapter.name == "my_handler_func"

    def test_get_handler_name_from_lambda(self) -> None:
        """Test getting name from a lambda."""
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        handler = lambda event: None  # noqa: E731
        adapter = HandlerAdapter(handler)
        assert adapter.name == "<lambda>"

    def test_get_handler_name_from_mock(self) -> None:
        """Test getting name from a MagicMock."""
        from eventsource.adapters._bus.handler_adapter import HandlerAdapter

        handler = MagicMock()
        adapter = HandlerAdapter(handler)
        assert "MagicMock" in adapter.name
