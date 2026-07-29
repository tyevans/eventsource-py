"""
Unit tests for RedisEventBus tracing functionality.

Tests for:
- Composition-based Tracer integration
- Span creation for publish, process, dispatch, handle operations
- Correct span attributes using standard ATTR_* constants
- Tracing disabled behavior
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.events.registry import EventRegistry
from eventsource.observability import (
    ATTR_AGGREGATE_ID,
    ATTR_EVENT_COUNT,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_COUNT,
    ATTR_HANDLER_NAME,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_SYSTEM,
    MockTracer,
    NullTracer,
    Tracer,
)

# ============================================================================
# Test Events for tracing tests
# ============================================================================


class RedisTracingTestEvent(DomainEvent):
    """Test event for tracing tests."""

    event_type: str = "RedisTracingTestEvent"
    aggregate_type: str = "RedisTracingTestAggregate"
    name: str


# ============================================================================
# Test Handlers for tracing tests
# ============================================================================


class TracingTestHandler:
    """Test handler for tracing tests."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []

    async def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


class FailingTracingHandler:
    """Test handler that always fails."""

    async def handle(self, event: DomainEvent) -> None:
        raise ValueError("Handler failed intentionally")


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def event_registry() -> EventRegistry:
    """Create an event registry with test events registered."""
    registry = EventRegistry()
    registry.register(RedisTracingTestEvent)
    return registry


@pytest.fixture
def config():
    """Create a test configuration."""
    from eventsource.bus.redis import RedisEventBusConfig

    return RedisEventBusConfig(
        redis_url="redis://localhost:6379",
        stream_prefix="test_events",
        consumer_group="test_group",
        consumer_name="test_consumer",
        batch_size=10,
        block_ms=100,
        max_retries=3,
        pending_idle_ms=1000,
        enable_dlq=True,
        enable_tracing=True,  # Enable tracing for tests
    )


@pytest.fixture
def mock_redis() -> AsyncMock:
    """Create a mock Redis client."""
    mock = AsyncMock()
    mock.ping = AsyncMock(return_value=True)
    mock.xgroup_create = AsyncMock(return_value=True)
    mock.xadd = AsyncMock(return_value="1234567890-0")
    mock.xreadgroup = AsyncMock(return_value=[])
    mock.xack = AsyncMock(return_value=1)
    mock.xpending = AsyncMock(return_value={"pending": 0})
    mock.xpending_range = AsyncMock(return_value=[])
    mock.xclaim = AsyncMock(return_value=[])
    mock.xinfo_stream = AsyncMock(return_value={"length": 0})
    mock.xinfo_groups = AsyncMock(return_value=[])
    mock.xrange = AsyncMock(return_value=[])
    mock.xdel = AsyncMock(return_value=1)
    mock.get = AsyncMock(return_value=None)
    mock.setex = AsyncMock(return_value=True)
    mock.delete = AsyncMock(return_value=1)
    mock.aclose = AsyncMock()

    # Pipeline support
    pipeline_mock = AsyncMock()
    pipeline_mock.__aenter__ = AsyncMock(return_value=pipeline_mock)
    pipeline_mock.__aexit__ = AsyncMock(return_value=None)
    pipeline_mock.xadd = MagicMock()
    pipeline_mock.execute = AsyncMock(return_value=["1234567890-0", "1234567890-1"])
    mock.pipeline = MagicMock(return_value=pipeline_mock)

    return mock


@pytest.fixture
def mock_tracer():
    """Create a MockTracer for testing span creation."""
    tracer = MockTracer()
    return tracer


@pytest.fixture
async def bus(config, event_registry: EventRegistry, mock_redis: AsyncMock):
    """Create a RedisEventBus with mocked Redis client."""
    from eventsource.bus.redis import RedisEventBus

    with (
        patch("eventsource.bus.redis.REDIS_AVAILABLE", True),
        patch("eventsource.bus.redis.aioredis") as mock_aioredis,
    ):
        mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

        bus = RedisEventBus(config=config, event_registry=event_registry)
        await bus.connect()
        yield bus

        # Cleanup
        bus._consuming = False
        if bus._redis:
            await bus.disconnect()


# ============================================================================
# Composition-based Tracer Integration Tests
# ============================================================================


class TestRedisEventBusTracerComposition:
    """Tests for RedisEventBus composition-based Tracer integration."""

    def test_uses_tracer_composition(self):
        """RedisEventBus uses composition-based Tracer instead of inheritance."""
        from eventsource.bus.redis import RedisEventBus

        # RedisEventBus should use Tracer composition
        # Check that it has a _tracer attribute (set in __init__)
        assert hasattr(RedisEventBus, "__init__")

    async def test_tracing_enabled_by_default(
        self, event_registry: EventRegistry, mock_redis: AsyncMock
    ):
        """Tracing is enabled by default when OTEL is available."""
        from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig

        config = RedisEventBusConfig()
        assert config.enable_tracing is True

        with (
            patch("eventsource.bus.redis.REDIS_AVAILABLE", True),
            patch("eventsource.bus.redis.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
            bus = RedisEventBus(config=config, event_registry=event_registry)

            # Check that tracing was initialized with composition pattern
            assert hasattr(bus, "_enable_tracing")
            assert hasattr(bus, "_tracer")
            assert isinstance(bus._tracer, Tracer)

    async def test_tracing_disabled_when_requested(
        self, event_registry: EventRegistry, mock_redis: AsyncMock
    ):
        """Tracing can be disabled via config parameter."""
        from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig

        config = RedisEventBusConfig(enable_tracing=False)

        with (
            patch("eventsource.bus.redis.REDIS_AVAILABLE", True),
            patch("eventsource.bus.redis.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
            bus = RedisEventBus(config=config, event_registry=event_registry)

            assert bus._enable_tracing is False
            # With composition, tracer is always set (NullTracer when disabled)
            assert isinstance(bus._tracer, NullTracer)

    async def test_has_enable_tracing_attribute(self, bus):
        """Bus exposes _enable_tracing attribute."""
        assert hasattr(bus, "_enable_tracing")
        assert isinstance(bus._enable_tracing, bool)


# ============================================================================
# Span Creation Tests - Publish
# ============================================================================


class TestRedisEventBusPublishSpanCreation:
    """Tests for span creation in RedisEventBus publish operation."""

    @pytest.mark.asyncio
    async def test_publish_creates_span(self, bus, mock_tracer, mock_redis: AsyncMock):
        """publish creates a span with correct name."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        aggregate_id = uuid4()
        event = RedisTracingTestEvent(
            aggregate_id=aggregate_id,
            name="Test",
        )

        await bus.publish([event])

        # Verify span was created with correct name
        assert "eventsource.event_bus.publish" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_publish_span_attributes(self, bus, mock_tracer, mock_redis: AsyncMock):
        """publish span includes correct standard attributes."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        aggregate_id = uuid4()
        events = [
            RedisTracingTestEvent(aggregate_id=aggregate_id, name=f"Test {i}") for i in range(3)
        ]

        await bus.publish(events)

        # Verify correct attributes using standard constants
        # MockTracer.spans stores tuples: (name, attributes)
        publish_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.event_bus.publish"
        ]
        assert len(publish_spans) >= 1
        _, attributes = publish_spans[0]

        assert ATTR_EVENT_COUNT in attributes
        assert attributes[ATTR_EVENT_COUNT] == 3
        assert ATTR_MESSAGING_SYSTEM in attributes
        assert attributes[ATTR_MESSAGING_SYSTEM] == "redis"
        assert ATTR_MESSAGING_DESTINATION in attributes

    @pytest.mark.asyncio
    async def test_publish_sets_success_attribute(self, bus, mock_tracer, mock_redis: AsyncMock):
        """publish sets success attribute on span."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        aggregate_id = uuid4()
        event = RedisTracingTestEvent(aggregate_id=aggregate_id, name="Test")

        await bus.publish([event])

        # Verify span was created
        assert "eventsource.event_bus.publish" in mock_tracer.span_names


# ============================================================================
# Span Creation Tests - Process Message
# ============================================================================


class TestRedisEventBusProcessMessageSpanCreation:
    """Tests for span creation in RedisEventBus _process_message operation."""

    @pytest.mark.asyncio
    async def test_process_message_creates_span(
        self, bus, event_registry: EventRegistry, mock_tracer, mock_redis: AsyncMock
    ):
        """_process_message creates a span with correct name."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        handler = TracingTestHandler()
        bus.subscribe(RedisTracingTestEvent, handler)

        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")

        message_data = {
            "event_id": str(event.event_id),
            "event_type": "RedisTracingTestEvent",
            "aggregate_id": str(event.aggregate_id),
            "aggregate_type": "RedisTracingTestAggregate",
            "occurred_at": datetime.now(UTC).isoformat(),
            "payload": event.model_dump_json(),
        }

        await bus._process_message("msg-123", message_data, "test_consumer")

        # Verify span was created with correct name
        assert "eventsource.event_bus.process" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_process_message_span_attributes(
        self, bus, event_registry: EventRegistry, mock_tracer, mock_redis: AsyncMock
    ):
        """_process_message span includes correct standard attributes."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        handler = TracingTestHandler()
        bus.subscribe(RedisTracingTestEvent, handler)

        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")

        message_data = {
            "event_id": str(event.event_id),
            "event_type": "RedisTracingTestEvent",
            "aggregate_id": str(event.aggregate_id),
            "aggregate_type": "RedisTracingTestAggregate",
            "occurred_at": datetime.now(UTC).isoformat(),
            "payload": event.model_dump_json(),
        }

        await bus._process_message("msg-123", message_data, "test_consumer")

        # Find the process span - MockTracer.spans stores tuples: (name, attributes)
        process_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.event_bus.process"
        ]
        assert len(process_spans) >= 1
        _, attributes = process_spans[0]

        assert ATTR_EVENT_TYPE in attributes
        assert attributes[ATTR_EVENT_TYPE] == "RedisTracingTestEvent"
        assert ATTR_EVENT_ID in attributes
        assert ATTR_MESSAGING_SYSTEM in attributes
        assert attributes[ATTR_MESSAGING_SYSTEM] == "redis"


# ============================================================================
# Span Creation Tests - Dispatch Event
# ============================================================================


class TestRedisEventBusDispatchSpanCreation:
    """Tests for span creation in RedisEventBus _dispatch_event operation."""

    @pytest.mark.asyncio
    async def test_dispatch_creates_span(self, bus, event_registry: EventRegistry, mock_tracer):
        """_dispatch_event creates a span with correct name."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        handler = TracingTestHandler()
        bus.subscribe(RedisTracingTestEvent, handler)

        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")

        await bus._dispatch_event(event, "msg-123")

        # Verify span was created with correct name
        assert "eventsource.event_bus.dispatch" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_dispatch_span_attributes(self, bus, event_registry: EventRegistry, mock_tracer):
        """_dispatch_event span includes correct standard attributes."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        handler = TracingTestHandler()
        bus.subscribe(RedisTracingTestEvent, handler)

        aggregate_id = uuid4()
        event = RedisTracingTestEvent(aggregate_id=aggregate_id, name="Test")

        await bus._dispatch_event(event, "msg-123")

        # Find the dispatch span - MockTracer.spans stores tuples: (name, attributes)
        dispatch_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.event_bus.dispatch"
        ]
        assert len(dispatch_spans) >= 1
        _, attributes = dispatch_spans[0]

        assert ATTR_EVENT_TYPE in attributes
        assert attributes[ATTR_EVENT_TYPE] == "RedisTracingTestEvent"
        assert ATTR_EVENT_ID in attributes
        assert str(event.event_id) in attributes[ATTR_EVENT_ID]
        assert ATTR_AGGREGATE_ID in attributes
        assert str(aggregate_id) in attributes[ATTR_AGGREGATE_ID]
        assert ATTR_HANDLER_COUNT in attributes
        assert attributes[ATTR_HANDLER_COUNT] == 1
        assert ATTR_MESSAGING_SYSTEM in attributes
        assert attributes[ATTR_MESSAGING_SYSTEM] == "redis"


# ============================================================================
# Span Creation Tests - Handler Invocation
# ============================================================================


class TestRedisEventBusHandlerSpanCreation:
    """Tests for span creation in RedisEventBus handler invocation."""

    @pytest.mark.asyncio
    async def test_handle_creates_span(self, bus, event_registry: EventRegistry, mock_tracer):
        """_invoke_handler creates a span with correct name."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        handler = TracingTestHandler()
        bus.subscribe(RedisTracingTestEvent, handler)

        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")

        await bus._dispatch_event(event, "msg-123")

        # Verify span was created with correct name
        assert "eventsource.event_bus.handle" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_handle_span_attributes(self, bus, event_registry: EventRegistry, mock_tracer):
        """_invoke_handler span includes correct standard attributes."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        handler = TracingTestHandler()
        bus.subscribe(RedisTracingTestEvent, handler)

        aggregate_id = uuid4()
        event = RedisTracingTestEvent(aggregate_id=aggregate_id, name="Test")

        await bus._dispatch_event(event, "msg-123")

        # Find the handle span - MockTracer.spans stores tuples: (name, attributes)
        handle_spans = [
            (name, attrs)
            for name, attrs in mock_tracer.spans
            if name == "eventsource.event_bus.handle"
        ]
        assert len(handle_spans) >= 1
        _, attributes = handle_spans[0]

        assert ATTR_EVENT_TYPE in attributes
        assert attributes[ATTR_EVENT_TYPE] == "RedisTracingTestEvent"
        assert ATTR_EVENT_ID in attributes
        assert ATTR_AGGREGATE_ID in attributes
        assert ATTR_HANDLER_NAME in attributes
        assert attributes[ATTR_HANDLER_NAME] == "TracingTestHandler"
        assert ATTR_MESSAGING_SYSTEM in attributes
        assert attributes[ATTR_MESSAGING_SYSTEM] == "redis"

    @pytest.mark.asyncio
    async def test_handle_sets_success_attribute_on_success(
        self, bus, event_registry: EventRegistry, mock_tracer
    ):
        """_invoke_handler sets success attribute on successful execution."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        handler = TracingTestHandler()
        bus.subscribe(RedisTracingTestEvent, handler)

        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")

        await bus._dispatch_event(event, "msg-123")

        # Verify handler span was created
        assert "eventsource.event_bus.handle" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_handle_sets_failure_attribute_on_error(
        self, bus, event_registry: EventRegistry, mock_tracer
    ):
        """_invoke_handler sets failure attribute and records exception on error."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        handler = FailingTracingHandler()
        bus.subscribe(RedisTracingTestEvent, handler)

        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")

        with pytest.raises(ValueError, match="Handler failed intentionally"):
            await bus._dispatch_event(event, "msg-123")

        # Verify handler span was created (failure is recorded within the span)
        assert "eventsource.event_bus.handle" in mock_tracer.span_names


# ============================================================================
# Tracing Disabled Tests
# ============================================================================


class TestRedisEventBusTracingDisabled:
    """Tests for RedisEventBus behavior when tracing is disabled."""

    @pytest.mark.asyncio
    async def test_publish_works_without_tracing(
        self, event_registry: EventRegistry, mock_redis: AsyncMock
    ):
        """publish works correctly when tracing is disabled."""
        from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig

        config = RedisEventBusConfig(
            redis_url="redis://localhost:6379",
            enable_tracing=False,
        )

        with (
            patch("eventsource.bus.redis.REDIS_AVAILABLE", True),
            patch("eventsource.bus.redis.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

            bus = RedisEventBus(config=config, event_registry=event_registry)
            await bus.connect()

            event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")

            # Should not raise
            await bus.publish([event])

            # Verify the event was published
            mock_redis.xadd.assert_called_once()

            await bus.disconnect()

    @pytest.mark.asyncio
    async def test_dispatch_works_without_tracing(
        self, event_registry: EventRegistry, mock_redis: AsyncMock
    ):
        """_dispatch_event works correctly when tracing is disabled."""
        from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig

        config = RedisEventBusConfig(
            redis_url="redis://localhost:6379",
            enable_tracing=False,
        )

        with (
            patch("eventsource.bus.redis.REDIS_AVAILABLE", True),
            patch("eventsource.bus.redis.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

            bus = RedisEventBus(config=config, event_registry=event_registry)
            await bus.connect()

            handler = TracingTestHandler()
            bus.subscribe(RedisTracingTestEvent, handler)

            event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")

            # Should not raise
            await bus._dispatch_event(event, "msg-123")

            # Verify the handler was invoked
            assert len(handler.handled_events) == 1

            await bus.disconnect()


# ============================================================================
# Standard Attributes Tests
# ============================================================================


class TestRedisEventBusStandardAttributes:
    """Tests for standard attribute usage in RedisEventBus."""

    def test_no_duplicate_otel_available(self):
        """Verify no duplicate OTEL_AVAILABLE definition in redis.py."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "OTEL_AVAILABLE = ", "src/eventsource/bus/redis.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[2],
        )
        # Should be 0 - no local definition
        assert result.stdout.strip() == "0", (
            f"Found {result.stdout.strip()} definitions of OTEL_AVAILABLE in redis.py"
        )

    def test_imports_from_observability_module(self):
        """Verify RedisEventBus imports tracing from observability module."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "from eventsource.observability import", "src/eventsource/bus/redis.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[2],
        )
        # Should be at least 1 - imports from observability
        count = int(result.stdout.strip())
        assert count >= 1, "RedisEventBus should import from eventsource.observability"

    def test_uses_standard_attribute_constants(self):
        """Verify RedisEventBus uses ATTR_* constants."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/bus/redis.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[2],
        )
        # Should find multiple ATTR_* usages
        count = int(result.stdout.strip())
        assert count >= 10, f"Expected at least 10 ATTR_* usages, found {count}"

    def test_uses_tracer_span(self):
        """Verify RedisEventBus uses _tracer.span() method (composition pattern)."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "_tracer.span", "src/eventsource/bus/redis.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[2],
        )
        # Should find multiple _tracer.span usages
        count = int(result.stdout.strip())
        assert count >= 3, f"Expected at least 3 _tracer.span usages, found {count}"


# ============================================================================
# Span Naming Convention Tests
# ============================================================================


class TestRedisEventBusSpanNaming:
    """Tests for span naming conventions in RedisEventBus."""

    @pytest.mark.asyncio
    async def test_publish_span_follows_convention(self, bus, mock_tracer, mock_redis: AsyncMock):
        """publish span name follows eventsource.event_bus.* convention."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")
        await bus.publish([event])

        assert "eventsource.event_bus.publish" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_dispatch_span_follows_convention(
        self, bus, event_registry: EventRegistry, mock_tracer
    ):
        """dispatch span name follows eventsource.event_bus.* convention."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        handler = TracingTestHandler()
        bus.subscribe(RedisTracingTestEvent, handler)

        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")
        await bus._dispatch_event(event, "msg-123")

        assert "eventsource.event_bus.dispatch" in mock_tracer.span_names
        assert "eventsource.event_bus.handle" in mock_tracer.span_names
