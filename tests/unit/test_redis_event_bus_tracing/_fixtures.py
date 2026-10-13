"""Shared fixtures and test classes for RedisEventBus tracing tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry
from eventsource.observability import MockTracer


class RedisTracingTestEvent(DomainEvent):
    """Test event for tracing tests."""

    aggregate_type: str = "RedisTracingTestAggregate"
    name: str


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


@pytest.fixture
def event_registry() -> EventRegistry:
    """Create an event registry with test events registered."""
    registry = EventRegistry()
    registry.register(RedisTracingTestEvent)
    return registry


@pytest.fixture
def config():
    """Create a test configuration."""
    from eventsource.adapters.redis.bus import RedisEventBusConfig

    return RedisEventBusConfig(
        redis_url="redis://localhost:6379",
        stream_prefix="test_events",
        consumer_group="test_group",
        consumer_name="test_consumer",
        stream_read_count=10,
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
    return MockTracer()


@pytest.fixture
async def bus(config, event_registry: EventRegistry, mock_redis: AsyncMock):
    """Create a RedisEventBus with mocked Redis client."""
    from eventsource.adapters.redis.bus import RedisEventBus

    with (
        patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
        patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
    ):
        mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

        bus = RedisEventBus(config=config, event_registry=event_registry)
        await bus.connect()
        yield bus

        # Cleanup
        bus._consuming = False
        if bus._redis:
            await bus.disconnect()
