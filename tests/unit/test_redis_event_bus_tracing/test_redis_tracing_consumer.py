"""Consumer, dispatch, and handler tracing tests for RedisEventBus."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from eventsource.domain.event_registry import EventRegistry
from eventsource.domain.exceptions import HandlerDispatchError
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_COUNT,
    ATTR_HANDLER_NAME,
    ATTR_MESSAGING_SYSTEM,
)
from tests.unit.test_redis_event_bus_tracing._fixtures import (
    FailingTracingHandler,
    RedisTracingTestEvent,
    TracingTestHandler,
)


def _make_message_data(event: RedisTracingTestEvent) -> dict[str, str]:
    return {
        "event_id": str(event.event_id),
        "event_type": "RedisTracingTestEvent",
        "aggregate_id": str(event.aggregate_id),
        "aggregate_type": "RedisTracingTestAggregate",
        "occurred_at": datetime.now(UTC).isoformat(),
        "payload": event.model_dump_json(),
    }


class TestRedisEventBusProcessMessageSpanCreation:
    """Tests for span creation in RedisEventBus _process_message operation."""

    @pytest.mark.asyncio
    async def test_process_message_creates_span(
        self, bus, event_registry: EventRegistry, mock_tracer, mock_redis: AsyncMock
    ):
        bus._tracer = mock_tracer
        bus._enable_tracing = True
        bus.subscribe(RedisTracingTestEvent, TracingTestHandler())
        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")
        await bus._process_message("msg-123", _make_message_data(event), "test_consumer")
        assert "eventsource.event_bus.process" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_process_message_span_attributes(
        self, bus, event_registry: EventRegistry, mock_tracer, mock_redis: AsyncMock
    ):
        bus._tracer = mock_tracer
        bus._enable_tracing = True
        bus.subscribe(RedisTracingTestEvent, TracingTestHandler())
        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")
        await bus._process_message("msg-123", _make_message_data(event), "test_consumer")

        process_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.event_bus.process"
        ]
        assert len(process_spans) >= 1
        _, attributes = process_spans[0]
        assert attributes.get(ATTR_EVENT_TYPE) == "RedisTracingTestEvent"
        assert ATTR_EVENT_ID in attributes
        assert attributes.get(ATTR_MESSAGING_SYSTEM) == "redis"


class TestRedisEventBusDispatchSpanCreation:
    """Tests for span creation in RedisEventBus _dispatch_event operation."""

    @pytest.mark.asyncio
    async def test_dispatch_creates_span(self, bus, event_registry: EventRegistry, mock_tracer):
        bus._tracer = mock_tracer
        bus._enable_tracing = True
        bus.subscribe(RedisTracingTestEvent, TracingTestHandler())
        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")
        await bus._dispatch_event(event, "msg-123")
        assert "eventsource.event_bus.dispatch" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_dispatch_span_attributes(self, bus, event_registry: EventRegistry, mock_tracer):
        bus._tracer = mock_tracer
        bus._enable_tracing = True
        bus.subscribe(RedisTracingTestEvent, TracingTestHandler())
        aggregate_id = uuid4()
        event = RedisTracingTestEvent(aggregate_id=aggregate_id, name="Test")
        await bus._dispatch_event(event, "msg-123")

        dispatch_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.event_bus.dispatch"
        ]
        assert len(dispatch_spans) >= 1
        _, attributes = dispatch_spans[0]
        assert attributes.get(ATTR_EVENT_TYPE) == "RedisTracingTestEvent"
        assert str(event.event_id) in attributes[ATTR_EVENT_ID]
        assert str(aggregate_id) in attributes[ATTR_AGGREGATE_ID]
        assert attributes.get(ATTR_HANDLER_COUNT) == 1
        assert attributes.get(ATTR_MESSAGING_SYSTEM) == "redis"


class TestRedisEventBusHandlerSpanCreation:
    """Tests for span creation in RedisEventBus handler invocation."""

    @pytest.mark.asyncio
    async def test_handle_creates_span(self, bus, event_registry: EventRegistry, mock_tracer):
        bus._tracer = mock_tracer
        bus._enable_tracing = True
        bus.subscribe(RedisTracingTestEvent, TracingTestHandler())
        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")
        await bus._dispatch_event(event, "msg-123")
        assert "eventsource.event_bus.handle" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_handle_span_attributes(self, bus, event_registry: EventRegistry, mock_tracer):
        bus._tracer = mock_tracer
        bus._enable_tracing = True
        bus.subscribe(RedisTracingTestEvent, TracingTestHandler())
        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")
        await bus._dispatch_event(event, "msg-123")

        handle_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.event_bus.handle"
        ]
        assert len(handle_spans) >= 1
        _, attributes = handle_spans[0]
        assert attributes.get(ATTR_EVENT_TYPE) == "RedisTracingTestEvent"
        assert ATTR_EVENT_ID in attributes
        assert ATTR_AGGREGATE_ID in attributes
        assert attributes.get(ATTR_HANDLER_NAME) == "TracingTestHandler"
        assert attributes.get(ATTR_MESSAGING_SYSTEM) == "redis"

    @pytest.mark.asyncio
    async def test_handle_sets_success_attribute_on_success(
        self, bus, event_registry: EventRegistry, mock_tracer
    ):
        bus._tracer = mock_tracer
        bus._enable_tracing = True
        bus.subscribe(RedisTracingTestEvent, TracingTestHandler())
        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")
        await bus._dispatch_event(event, "msg-123")
        assert "eventsource.event_bus.handle" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_handle_sets_failure_attribute_on_error(
        self, bus, event_registry: EventRegistry, mock_tracer
    ):
        bus._tracer = mock_tracer
        bus._enable_tracing = True
        bus.subscribe(RedisTracingTestEvent, FailingTracingHandler())
        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")
        with pytest.raises(HandlerDispatchError):
            await bus._dispatch_event(event, "msg-123")
        assert "eventsource.event_bus.handle" in mock_tracer.span_names
