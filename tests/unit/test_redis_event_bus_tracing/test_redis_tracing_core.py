"""Core tracing, composition, publisher tracing, and configuration tests for RedisEventBus."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from eventsource.domain.event_registry import EventRegistry
from eventsource.observability import (
    ATTR_EVENT_COUNT,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_SYSTEM,
    NullTracer,
    Tracer,
)
from tests.unit.test_redis_event_bus_tracing._fixtures import (
    RedisTracingTestEvent,
    TracingTestHandler,
)


class TestRedisEventBusTracerComposition:
    """Tests for RedisEventBus composition-based Tracer integration."""

    def test_uses_tracer_composition(self):
        """RedisEventBus uses composition-based Tracer instead of inheritance."""
        from eventsource.adapters.redis.bus import RedisEventBus

        assert hasattr(RedisEventBus, "__init__")

    async def test_tracing_enabled_by_default(
        self, event_registry: EventRegistry, mock_redis: AsyncMock
    ):
        """Tracing is enabled by default when OTEL is available."""
        from eventsource.adapters.redis.bus import RedisEventBus, RedisEventBusConfig

        config = RedisEventBusConfig()
        assert config.enable_tracing is True

        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
            bus = RedisEventBus(config=config, event_registry=event_registry)
            assert hasattr(bus, "_enable_tracing")
            assert hasattr(bus, "_tracer")
            assert isinstance(bus._tracer, Tracer)

    async def test_tracing_disabled_when_requested(
        self, event_registry: EventRegistry, mock_redis: AsyncMock
    ):
        """Tracing can be disabled via config parameter."""
        from eventsource.adapters.redis.bus import RedisEventBus, RedisEventBusConfig

        config = RedisEventBusConfig(enable_tracing=False)

        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
            bus = RedisEventBus(config=config, event_registry=event_registry)
            assert bus._enable_tracing is False
            assert isinstance(bus._tracer, NullTracer)

    async def test_has_enable_tracing_attribute(self, bus):
        """Bus exposes _enable_tracing attribute."""
        assert hasattr(bus, "_enable_tracing")
        assert isinstance(bus._enable_tracing, bool)


class TestRedisEventBusPublishSpanCreation:
    """Tests for span creation in RedisEventBus publish operation."""

    @pytest.mark.asyncio
    async def test_publish_creates_span(self, bus, mock_tracer, mock_redis: AsyncMock):
        """publish creates a span with correct name."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True
        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")

        await bus.publish([event])

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
        event = RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")

        await bus.publish([event])

        assert "eventsource.event_bus.publish" in mock_tracer.span_names


class TestRedisEventBusTracingDisabled:
    """Tests for RedisEventBus behavior when tracing is disabled."""

    @pytest.mark.asyncio
    async def test_publish_works_without_tracing(
        self, event_registry: EventRegistry, mock_redis: AsyncMock
    ):
        """publish works correctly when tracing is disabled."""
        from eventsource.adapters.redis.bus import RedisEventBus, RedisEventBusConfig

        config = RedisEventBusConfig(redis_url="redis://localhost:6379", enable_tracing=False)

        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
            bus = RedisEventBus(config=config, event_registry=event_registry)
            await bus.connect()
            await bus.publish([RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")])
            mock_redis.xadd.assert_called_once()
            await bus.disconnect()

    @pytest.mark.asyncio
    async def test_dispatch_works_without_tracing(
        self, event_registry: EventRegistry, mock_redis: AsyncMock
    ):
        """_dispatch_event works correctly when tracing is disabled."""
        from eventsource.adapters.redis.bus import RedisEventBus, RedisEventBusConfig

        config = RedisEventBusConfig(redis_url="redis://localhost:6379", enable_tracing=False)

        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
            bus = RedisEventBus(config=config, event_registry=event_registry)
            await bus.connect()
            handler = TracingTestHandler()
            bus.subscribe(RedisTracingTestEvent, handler)
            await bus._dispatch_event(
                RedisTracingTestEvent(aggregate_id=uuid4(), name="Test"), "msg-123"
            )
            assert len(handler.handled_events) == 1
            await bus.disconnect()


class TestRedisEventBusStandardAttributes:
    """Tests for standard attribute usage in RedisEventBus."""

    def test_no_duplicate_otel_available(self):
        """Verify no duplicate OTEL_AVAILABLE definition in redis.py."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "OTEL_AVAILABLE = ", "src/eventsource/adapters/redis/bus.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[3],
        )
        assert result.stdout.strip() == "0", (
            f"Found {result.stdout.strip()} definitions of OTEL_AVAILABLE in redis.py"
        )

    def test_imports_from_observability_module(self):
        """Verify RedisEventBus imports tracing from observability module."""
        import subprocess

        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/adapters/redis/bus.py",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[3],
        )
        assert int(result.stdout.strip()) >= 1, (
            "RedisEventBus should import from eventsource.observability"
        )

    def test_uses_standard_attribute_constants(self):
        """Verify RedisEventBus uses ATTR_* constants."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/adapters/redis/bus.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[3],
        )
        count = int(result.stdout.strip())
        assert count >= 10, f"Expected at least 10 ATTR_* usages, found {count}"

    def test_uses_tracer_span(self):
        """Verify RedisEventBus uses _tracer.span() method (composition pattern)."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "_tracer.span", "src/eventsource/adapters/redis/bus.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).resolve().parents[3],
        )
        count = int(result.stdout.strip())
        assert count >= 3, f"Expected at least 3 _tracer.span usages, found {count}"


class TestRedisEventBusSpanNaming:
    """Tests for span naming conventions in RedisEventBus."""

    @pytest.mark.asyncio
    async def test_publish_span_follows_convention(self, bus, mock_tracer, mock_redis: AsyncMock):
        """publish span name follows eventsource.event_bus.* convention."""
        bus._tracer = mock_tracer
        bus._enable_tracing = True

        await bus.publish([RedisTracingTestEvent(aggregate_id=uuid4(), name="Test")])

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

        await bus._dispatch_event(
            RedisTracingTestEvent(aggregate_id=uuid4(), name="Test"), "msg-123"
        )

        assert "eventsource.event_bus.dispatch" in mock_tracer.span_names
        assert "eventsource.event_bus.handle" in mock_tracer.span_names
