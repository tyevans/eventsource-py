"""Test fixtures and test doubles for RedisEventBus tests."""

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from eventsource.adapters.redis.bus import (
    RedisEventBus,
    RedisEventBusConfig,
)
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry

__all__ = [
    "FailingHandler",
    "OrderHandler",
    "SampleOrderCreated",
    "SampleOrderShipped",
    "SamplePaymentReceived",
    "SampleSubscriber",
    "SyncHandler",
    "bus",
    "config",
    "event_registry",
    "mock_redis",
]

# --- Test Events ---
# Prefixed with Sample to avoid pytest collection warnings


class SampleOrderCreated(DomainEvent):
    """Test event for order creation."""

    aggregate_type: str = "Order"
    order_number: str
    customer_id: UUID


class SampleOrderShipped(DomainEvent):
    """Test event for order shipping."""

    aggregate_type: str = "Order"
    tracking_number: str


class SamplePaymentReceived(DomainEvent):
    """Test event for payment."""

    aggregate_type: str = "Payment"
    amount: float


# --- Test Handlers ---


class OrderHandler:
    """Test handler for order events."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []

    async def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


class SyncHandler:
    """Synchronous handler for testing."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []

    def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


class FailingHandler:
    """Handler that always fails."""

    async def handle(self, event: DomainEvent) -> None:
        raise ValueError("Handler failed intentionally")


class SampleSubscriber:
    """Test subscriber that subscribes to multiple event types."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SampleOrderCreated, SampleOrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


# --- Fixtures ---


@pytest.fixture
def event_registry() -> EventRegistry:
    """Create an event registry with test events registered."""
    registry = EventRegistry()
    registry.register(SampleOrderCreated)
    registry.register(SampleOrderShipped)
    registry.register(SamplePaymentReceived)
    return registry


@pytest.fixture
def config() -> RedisEventBusConfig:
    """Create a test configuration."""
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
        enable_tracing=False,  # Disable tracing for tests
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
async def bus(
    config: RedisEventBusConfig, event_registry: EventRegistry, mock_redis: AsyncMock
) -> AsyncGenerator[RedisEventBus]:
    """Create a RedisEventBus with mocked Redis client."""
    with (
        patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
        patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
    ):
        mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

        bus_instance = RedisEventBus(config=config, event_registry=event_registry)
        await bus_instance.connect()
        yield bus_instance

        # Cleanup
        bus_instance._consuming = False
        if bus_instance._redis:
            await bus_instance.disconnect()
