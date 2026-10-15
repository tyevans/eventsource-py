"""Unit tests extracted for test_rabbitmq_handlers.py."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.adapters._bus.handler_adapter import HandlerAdapter
from eventsource.domain.event import DomainEvent


class _TestEvent(DomainEvent):
    aggregate_type: str = "TestAggregate"


class TestRabbitMQHandlerNormalization:
    """Tests for handler normalization via HandlerAdapter (P1-009)."""

    def test_normalize_async_function(self) -> None:
        async def async_handler(event: DomainEvent) -> None:
            pass

        adapter = HandlerAdapter(async_handler)
        assert adapter._original is async_handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_normalize_sync_function(self) -> None:
        def sync_handler(event: DomainEvent) -> None:
            pass

        adapter = HandlerAdapter(sync_handler)
        assert adapter._original is sync_handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_normalize_async_handler_class(self) -> None:
        class AsyncHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = AsyncHandler()
        adapter = HandlerAdapter(handler)
        assert adapter._original is handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_normalize_sync_handler_class(self) -> None:
        class SyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        handler = SyncHandler()
        adapter = HandlerAdapter(handler)
        assert adapter._original is handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_normalize_invalid_handler_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Handler must have a handle\\(\\) method"):
            HandlerAdapter("not a handler")  # type: ignore[arg-type]

    def test_normalize_invalid_handler_with_number(self) -> None:
        with pytest.raises(TypeError, match="Handler must have a handle\\(\\) method"):
            HandlerAdapter(42)  # type: ignore[arg-type]

    def test_normalize_lambda(self) -> None:
        handler = lambda event: None  # noqa: E731
        adapter = HandlerAdapter(handler)
        assert adapter._original is handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    @pytest.mark.asyncio
    async def test_normalized_sync_handler_invocation(self) -> None:
        results: list[DomainEvent] = []

        def sync_handler(event: DomainEvent) -> None:
            results.append(event)

        adapter = HandlerAdapter(sync_handler)
        event = _TestEvent(aggregate_id=uuid4())
        await adapter.handle(event)

        assert results == [event]
        assert results[0] is event

    @pytest.mark.asyncio
    async def test_normalized_async_handler_invocation(self) -> None:
        results: list[DomainEvent] = []

        async def async_handler(event: DomainEvent) -> None:
            results.append(event)

        adapter = HandlerAdapter(async_handler)
        event = _TestEvent(aggregate_id=uuid4())
        await adapter.handle(event)

        assert results == [event]
        assert results[0] is event

    @pytest.mark.asyncio
    async def test_normalized_handler_class_invocation(self) -> None:
        results: list[DomainEvent] = []

        class TestHandler:
            async def handle(self, event: DomainEvent) -> None:
                results.append(event)

        adapter = HandlerAdapter(TestHandler())
        event = _TestEvent(aggregate_id=uuid4())
        await adapter.handle(event)

        assert results == [event]
        assert results[0] is event


class TestRabbitMQGetHandlerName:
    """Tests for HandlerAdapter.name property (P1-009)."""

    def test_get_handler_name_from_class(self) -> None:
        class MyHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        assert HandlerAdapter(MyHandler()).name == "MyHandler"

    def test_get_handler_name_from_function(self) -> None:
        def my_handler_func(event: DomainEvent) -> None:
            pass

        assert HandlerAdapter(my_handler_func).name == "my_handler_func"

    def test_get_handler_name_from_lambda(self) -> None:
        handler = lambda event: None  # noqa: E731
        assert HandlerAdapter(handler).name == "<lambda>"

    def test_get_handler_name_from_mock(self) -> None:
        assert "MagicMock" in HandlerAdapter(MagicMock()).name
