"""Integration tests for event bus tracing."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

import pytest

from eventsource import DomainEvent, InMemoryEventBus
from eventsource.observability import ATTR_EVENT_TYPE, ATTR_HANDLER_NAME

from ..conftest import TracingTestEvent

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = [pytest.mark.integration]


class TestEventBusTracing:
    """Integration tests for event bus tracing."""

    @pytest.mark.asyncio
    async def test_inmemory_bus_dispatch_creates_span(
        self,
        trace_provider: Any,
        find_span: Callable[[str], Any | None],
        sample_aggregate_id: UUID,
    ) -> None:
        """InMemoryEventBus dispatch creates a span with event details."""
        bus = InMemoryEventBus(enable_tracing=True)

        # Register a handler
        handled_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            handled_events.append(event)

        bus.subscribe(TracingTestEvent, handler)

        # Publish an event
        event = TracingTestEvent(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test",
        )
        await bus.publish([event])

        # Find the dispatch span
        span = find_span("dispatch")
        assert span is not None, "Event dispatch should create a span"

        attributes = dict(span.attributes)
        assert ATTR_EVENT_TYPE in attributes
        assert attributes[ATTR_EVENT_TYPE] == "TracingTestEvent"

    @pytest.mark.asyncio
    async def test_inmemory_bus_handler_creates_span(
        self,
        trace_provider: Any,
        find_span: Callable[[str], Any | None],
        sample_aggregate_id: UUID,
    ) -> None:
        """InMemoryEventBus handler execution creates a child span."""
        bus = InMemoryEventBus(enable_tracing=True)

        handled_events: list[DomainEvent] = []

        async def my_handler(event: DomainEvent) -> None:
            handled_events.append(event)

        bus.subscribe(TracingTestEvent, my_handler)

        event = TracingTestEvent(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test",
        )
        await bus.publish([event])

        # Find the handler span
        span = find_span("handle")
        assert span is not None, "Handler execution should create a span"

        attributes = dict(span.attributes)
        assert ATTR_HANDLER_NAME in attributes

    @pytest.mark.asyncio
    async def test_multiple_handlers_create_separate_spans(
        self,
        trace_provider: Any,
        find_spans: Callable[[str], list[Any]],
        sample_aggregate_id: UUID,
    ) -> None:
        """Multiple handlers for same event create separate spans."""
        bus = InMemoryEventBus(enable_tracing=True)

        async def handler1(event: DomainEvent) -> None:
            pass

        async def handler2(event: DomainEvent) -> None:
            pass

        async def handler3(event: DomainEvent) -> None:
            pass

        bus.subscribe(TracingTestEvent, handler1)
        bus.subscribe(TracingTestEvent, handler2)
        bus.subscribe(TracingTestEvent, handler3)

        event = TracingTestEvent(
            aggregate_id=sample_aggregate_id,
            aggregate_version=1,
            name="Test",
        )
        await bus.publish([event])

        # Find all handler spans
        handler_spans = find_spans("handle")
        assert len(handler_spans) == 3, "Should have one span per handler"
