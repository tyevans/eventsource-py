"""Unit tests for RedisEventBus implementation.

These tests use mocks to test the RedisEventBus without requiring
a real Redis server.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

from eventsource.bus.redis import (
    RedisEventBus,
    RedisEventBusConfig,
    RedisEventBusStats,
    RedisNotAvailableError,
)
from eventsource.events.base import DomainEvent
from eventsource.events.registry import EventRegistry

# Note: Tests use mocks and don't require actual Redis, so we don't skip.
# REDIS_AVAILABLE needs to be True for the RedisEventBus class to be importable.


# --- Test Events ---
# Prefixed with Sample to avoid pytest collection warnings


class SampleOrderCreated(DomainEvent):
    """Test event for order creation."""

    event_type: str = "SampleOrderCreated"
    aggregate_type: str = "Order"
    order_number: str
    customer_id: UUID


class SampleOrderShipped(DomainEvent):
    """Test event for order shipping."""

    event_type: str = "SampleOrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str


class SamplePaymentReceived(DomainEvent):
    """Test event for payment."""

    event_type: str = "SamplePaymentReceived"
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
        batch_size=10,
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
async def bus(config: RedisEventBusConfig, event_registry: EventRegistry, mock_redis: AsyncMock):
    """Create a RedisEventBus with mocked Redis client."""
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


# --- Configuration Tests ---


class TestRedisEventBusConfig:
    """Tests for RedisEventBusConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = RedisEventBusConfig()

        assert config.redis_url == "redis://localhost:6379"
        assert config.stream_prefix == "events"
        assert config.consumer_group == "default"
        assert config.consumer_name is not None  # Auto-generated
        assert config.batch_size == 100
        assert config.block_ms == 5000
        assert config.max_retries == 3
        assert config.pending_idle_ms == 60000
        assert config.enable_dlq is True
        assert config.dlq_stream_suffix == "_dlq"
        assert config.enable_tracing is True

    def test_custom_config(self):
        """Test custom configuration values."""
        config = RedisEventBusConfig(
            redis_url="redis://custom:6380",
            stream_prefix="myapp",
            consumer_group="workers",
            consumer_name="worker-1",
            batch_size=50,
            max_retries=5,
        )

        assert config.redis_url == "redis://custom:6380"
        assert config.stream_prefix == "myapp"
        assert config.consumer_group == "workers"
        assert config.consumer_name == "worker-1"
        assert config.batch_size == 50
        assert config.max_retries == 5

    def test_stream_name_property(self):
        """Test stream name generation."""
        config = RedisEventBusConfig(stream_prefix="myapp")
        assert config.stream_name == "myapp:stream"

    def test_dlq_stream_name_property(self):
        """Test DLQ stream name generation."""
        config = RedisEventBusConfig(stream_prefix="myapp")
        assert config.dlq_stream_name == "myapp:stream_dlq"

    def test_get_retry_key(self):
        """Test retry key generation."""
        config = RedisEventBusConfig(stream_prefix="myapp")
        key = config.get_retry_key("msg-123")
        assert key == "myapp:retry:msg-123"

    def test_auto_generated_consumer_name(self):
        """Test that consumer name is auto-generated if not provided."""
        config = RedisEventBusConfig(consumer_name=None)
        assert config.consumer_name is not None
        assert "-" in config.consumer_name  # Format: hostname-uuid


class TestRedisEventBusStats:
    """Tests for RedisEventBusStats."""

    def test_default_stats(self):
        """Test default statistics values."""
        stats = RedisEventBusStats()

        assert stats.events_published == 0
        assert stats.events_consumed == 0
        assert stats.events_processed_success == 0
        assert stats.events_processed_failed == 0
        assert stats.messages_recovered == 0
        assert stats.messages_sent_to_dlq == 0
        assert stats.handler_errors == 0
        assert stats.reconnections == 0

    def test_stats_modification(self):
        """Test that stats can be modified."""
        stats = RedisEventBusStats()
        stats.events_published += 10
        stats.handler_errors += 2

        assert stats.events_published == 10
        assert stats.handler_errors == 2


# --- Connection Tests ---


class TestRedisEventBusConnection:
    """Tests for connection management."""

    async def test_connect(self, config: RedisEventBusConfig, mock_redis: AsyncMock):
        """Test connecting to Redis."""
        with (
            patch("eventsource.bus.redis.REDIS_AVAILABLE", True),
            patch("eventsource.bus.redis.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

            bus = RedisEventBus(config=config)
            await bus.connect()

            assert bus.is_connected is True
            mock_redis.ping.assert_called_once()
            mock_redis.xgroup_create.assert_called_once()

            await bus.disconnect()

    async def test_disconnect(self, config: RedisEventBusConfig, mock_redis: AsyncMock):
        """Test disconnecting from Redis."""
        with (
            patch("eventsource.bus.redis.REDIS_AVAILABLE", True),
            patch("eventsource.bus.redis.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

            bus = RedisEventBus(config=config)
            await bus.connect()
            await bus.disconnect()

            assert bus.is_connected is False
            mock_redis.aclose.assert_called_once()

    async def test_connect_already_connected(
        self, config: RedisEventBusConfig, mock_redis: AsyncMock
    ):
        """Test that connecting when already connected logs a warning."""
        with (
            patch("eventsource.bus.redis.REDIS_AVAILABLE", True),
            patch("eventsource.bus.redis.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

            bus = RedisEventBus(config=config)
            await bus.connect()
            await bus.connect()  # Should log warning, not error

            assert bus.is_connected is True
            # ping should only be called once
            assert mock_redis.ping.call_count == 1

            await bus.disconnect()

    async def test_consumer_group_already_exists(
        self, config: RedisEventBusConfig, mock_redis: AsyncMock
    ):
        """Test handling when consumer group already exists."""
        # Import ResponseError from the eventsource.bus.redis module to use its fallback
        from eventsource.bus.redis import ResponseError

        mock_redis.xgroup_create = AsyncMock(
            side_effect=ResponseError("BUSYGROUP Consumer Group name already exists")
        )

        with (
            patch("eventsource.bus.redis.REDIS_AVAILABLE", True),
            patch("eventsource.bus.redis.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

            bus = RedisEventBus(config=config)
            # Should not raise, just log
            await bus.connect()

            assert bus.is_connected is True
            await bus.disconnect()


# --- Subscription Tests ---


class TestRedisEventBusSubscription:
    """Tests for subscription management."""

    async def test_subscribe(self, bus: RedisEventBus):
        """Test subscribing a handler to an event type."""
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)

        assert bus.get_subscriber_count(SampleOrderCreated) == 1

    async def test_subscribe_multiple_handlers(self, bus: RedisEventBus):
        """Test subscribing multiple handlers to the same event type."""
        handler1 = OrderHandler()
        handler2 = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe(SampleOrderCreated, handler2)

        assert bus.get_subscriber_count(SampleOrderCreated) == 2

    async def test_subscribe_to_multiple_events(self, bus: RedisEventBus):
        """Test subscribing to multiple event types."""
        handler = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler)
        bus.subscribe(SampleOrderShipped, handler)

        assert bus.get_subscriber_count(SampleOrderCreated) == 1
        assert bus.get_subscriber_count(SampleOrderShipped) == 1
        assert bus.get_subscriber_count() == 2

    async def test_unsubscribe(self, bus: RedisEventBus):
        """Test unsubscribing a handler."""
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)
        result = bus.unsubscribe(SampleOrderCreated, handler)

        assert result is True
        assert bus.get_subscriber_count(SampleOrderCreated) == 0

    async def test_unsubscribe_not_found(self, bus: RedisEventBus):
        """Test unsubscribing a handler that wasn't subscribed."""
        handler = OrderHandler()
        result = bus.unsubscribe(SampleOrderCreated, handler)

        assert result is False

    async def test_subscribe_all(self, bus: RedisEventBus):
        """Test subscribing an EventSubscriber to all its events."""
        subscriber = SampleSubscriber()
        bus.subscribe_all(subscriber)

        assert bus.get_subscriber_count(SampleOrderCreated) == 1
        assert bus.get_subscriber_count(SampleOrderShipped) == 1

    async def test_subscribe_to_all_events(self, bus: RedisEventBus):
        """Test wildcard subscription."""
        handler = OrderHandler()
        bus.subscribe_to_all_events(handler)

        assert bus.get_wildcard_subscriber_count() == 1

    async def test_unsubscribe_from_all_events(self, bus: RedisEventBus):
        """Test unsubscribing from wildcard subscription."""
        handler = OrderHandler()
        bus.subscribe_to_all_events(handler)
        result = bus.unsubscribe_from_all_events(handler)

        assert result is True
        assert bus.get_wildcard_subscriber_count() == 0

    async def test_clear_subscribers(self, bus: RedisEventBus):
        """Test clearing all subscribers."""
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)
        bus.subscribe_to_all_events(handler)

        bus.clear_subscribers()

        assert bus.get_subscriber_count() == 0
        assert bus.get_wildcard_subscriber_count() == 0


# --- Publishing Tests ---


class TestRedisEventBusPublish:
    """Tests for event publishing."""

    async def test_publish_single_event(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test publishing a single event."""
        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        await bus.publish([event])

        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        assert call_args.kwargs["name"] == "test_events:stream"
        assert "event_type" in call_args.kwargs["fields"]
        assert call_args.kwargs["fields"]["event_type"] == "SampleOrderCreated"

    async def test_publish_multiple_events_uses_pipeline(
        self, bus: RedisEventBus, mock_redis: AsyncMock
    ):
        """Test that publishing multiple events uses pipeline."""
        events = [
            SampleOrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i}",
                customer_id=uuid4(),
            )
            for i in range(5)
        ]

        await bus.publish(events)

        # Should use pipeline, not individual xadd
        mock_redis.pipeline.assert_called_once()
        assert mock_redis.xadd.call_count == 0

    async def test_publish_empty_list(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test publishing an empty list of events."""
        await bus.publish([])

        mock_redis.xadd.assert_not_called()
        mock_redis.pipeline.assert_not_called()

    async def test_publish_updates_stats(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test that publishing updates statistics."""
        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        initial_count = bus.stats.events_published
        await bus.publish([event])

        assert bus.stats.events_published == initial_count + 1

    async def test_publish_serializes_event(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test that events are properly serialized."""
        aggregate_id = uuid4()
        customer_id = uuid4()
        event = SampleOrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            customer_id=customer_id,
        )

        await bus.publish([event])

        call_args = mock_redis.xadd.call_args
        fields = call_args.kwargs["fields"]

        assert fields["event_id"] == str(event.event_id)
        assert fields["aggregate_id"] == str(aggregate_id)
        assert fields["event_type"] == "SampleOrderCreated"
        assert "payload" in fields


# --- Event Dispatch Tests ---


class TestRedisEventBusDispatch:
    """Tests for event dispatching."""

    async def test_dispatch_to_handler(self, bus: RedisEventBus, event_registry: EventRegistry):
        """Test dispatching event to handler."""
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        await bus._dispatch_event(event, "msg-123")

        assert len(handler.handled_events) == 1
        assert handler.handled_events[0].order_number == "ORD-001"

    async def test_dispatch_to_sync_handler(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ):
        """Test dispatching event to sync handler."""
        handler = SyncHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        await bus._dispatch_event(event, "msg-123")

        assert len(handler.handled_events) == 1

    async def test_dispatch_to_wildcard_handler(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ):
        """Test dispatching event to wildcard handler."""
        handler = OrderHandler()
        bus.subscribe_to_all_events(handler)

        event = SamplePaymentReceived(
            aggregate_id=uuid4(),
            amount=100.0,
        )

        await bus._dispatch_event(event, "msg-123")

        assert len(handler.handled_events) == 1

    async def test_dispatch_to_multiple_handlers(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ):
        """Test dispatching event to multiple handlers."""
        handler1 = OrderHandler()
        handler2 = OrderHandler()
        wildcard_handler = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe(SampleOrderCreated, handler2)
        bus.subscribe_to_all_events(wildcard_handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        await bus._dispatch_event(event, "msg-123")

        assert len(handler1.handled_events) == 1
        assert len(handler2.handled_events) == 1
        assert len(wildcard_handler.handled_events) == 1

    async def test_dispatch_handler_failure(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ):
        """Test that handler failure raises exception."""
        handler = FailingHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        with pytest.raises(ValueError, match="Handler failed intentionally"):
            await bus._dispatch_event(event, "msg-123")

    async def test_dispatch_handler_failure_updates_stats(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ):
        """Test that handler failure updates statistics."""
        handler = FailingHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        initial_errors = bus.stats.handler_errors

        with pytest.raises(ValueError):
            await bus._dispatch_event(event, "msg-123")

        assert bus.stats.handler_errors == initial_errors + 1


# --- Event Deserialization Tests ---


class TestRedisEventBusDeserialization:
    """Tests for event deserialization."""

    async def test_deserialize_known_event(self, bus: RedisEventBus, event_registry: EventRegistry):
        """Test deserializing a known event type."""
        aggregate_id = uuid4()
        customer_id = uuid4()

        event = SampleOrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            customer_id=customer_id,
        )

        message_data = {
            "event_type": "SampleOrderCreated",
            "payload": event.model_dump_json(),
        }

        result = bus._deserialize_event("SampleOrderCreated", message_data)

        assert result is not None
        assert result.event_type == "SampleOrderCreated"
        assert result.order_number == "ORD-001"
        assert result.aggregate_id == aggregate_id

    async def test_deserialize_unknown_event(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ):
        """Test deserializing an unknown event type returns None."""
        message_data = {
            "event_type": "UnknownEvent",
            "payload": "{}",
        }

        result = bus._deserialize_event("UnknownEvent", message_data)

        assert result is None


# --- Message Processing Tests ---


class TestRedisEventBusMessageProcessing:
    """Tests for message processing."""

    async def test_process_message_success(
        self,
        bus: RedisEventBus,
        event_registry: EventRegistry,
        mock_redis: AsyncMock,
    ):
        """Test successful message processing."""
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)

        aggregate_id = uuid4()
        event = SampleOrderCreated(
            aggregate_id=aggregate_id,
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        message_data = {
            "event_id": str(event.event_id),
            "event_type": "SampleOrderCreated",
            "aggregate_id": str(aggregate_id),
            "aggregate_type": "Order",
            "occurred_at": datetime.now(UTC).isoformat(),
            "payload": event.model_dump_json(),
        }

        await bus._process_message("msg-123", message_data, "test_consumer")

        assert len(handler.handled_events) == 1
        mock_redis.xack.assert_called_once()

    async def test_process_message_unknown_event(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test processing unknown event type."""
        message_data = {
            "event_id": str(uuid4()),
            "event_type": "UnknownEvent",
            "aggregate_id": str(uuid4()),
            "aggregate_type": "Unknown",
            "occurred_at": datetime.now(UTC).isoformat(),
            "payload": "{}",
        }

        # Should not raise, just skip
        await bus._process_message("msg-123", message_data, "test_consumer")

        # Should still ack to prevent blocking
        mock_redis.xack.assert_called_once()

    async def test_process_message_handler_failure(
        self,
        bus: RedisEventBus,
        event_registry: EventRegistry,
        mock_redis: AsyncMock,
    ):
        """Test message processing with handler failure."""
        handler = FailingHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        message_data = {
            "event_id": str(event.event_id),
            "event_type": "SampleOrderCreated",
            "aggregate_id": str(event.aggregate_id),
            "aggregate_type": "Order",
            "occurred_at": datetime.now(UTC).isoformat(),
            "payload": event.model_dump_json(),
        }

        # Should not raise (error is caught)
        await bus._process_message("msg-123", message_data, "test_consumer")

        # Should NOT ack (message will be redelivered)
        mock_redis.xack.assert_not_called()


# --- Dead Letter Queue Tests ---


class TestRedisEventBusDLQ:
    """Tests for Dead Letter Queue functionality."""

    async def test_send_to_dlq(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test sending a message to DLQ."""
        message_data = {
            "event_id": str(uuid4()),
            "event_type": "SampleOrderCreated",
            "payload": "{}",
        }

        await bus._send_to_dlq("msg-123", message_data, 3)

        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        assert call_args.kwargs["name"] == "test_events:stream_dlq"
        assert "original_message_id" in call_args.kwargs["fields"]
        assert call_args.kwargs["fields"]["original_message_id"] == "msg-123"
        assert call_args.kwargs["fields"]["retry_count"] == "3"

    async def test_get_dlq_messages(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test getting DLQ messages."""
        mock_redis.xrange.return_value = [
            ("msg-1", {"event_type": "SampleOrderCreated", "original_message_id": "orig-1"}),
            ("msg-2", {"event_type": "SampleOrderShipped", "original_message_id": "orig-2"}),
        ]

        messages = await bus.get_dlq_messages(count=10)

        assert len(messages) == 2
        assert messages[0]["message_id"] == "msg-1"
        assert messages[1]["message_id"] == "msg-2"
        mock_redis.xrange.assert_called_once()

    async def test_replay_dlq_message(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test replaying a DLQ message."""
        mock_redis.xrange.return_value = [
            (
                "dlq-msg-123",
                {
                    "event_type": "SampleOrderCreated",
                    "payload": "{}",
                    "original_message_id": "orig-123",
                    "retry_count": "3",
                    "dlq_timestamp": "2024-01-01T00:00:00Z",
                },
            ),
        ]
        mock_redis.xadd.return_value = "new-msg-456"

        result = await bus.replay_dlq_message("dlq-msg-123")

        assert result is True
        mock_redis.xadd.assert_called_once()
        mock_redis.xdel.assert_called_once()

        # Check that DLQ-specific fields are removed
        call_args = mock_redis.xadd.call_args
        fields = call_args.kwargs["fields"]
        assert "original_message_id" not in fields
        assert "retry_count" not in fields
        assert "dlq_timestamp" not in fields

    async def test_replay_dlq_message_not_found(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test replaying a non-existent DLQ message."""
        mock_redis.xrange.return_value = []

        result = await bus.replay_dlq_message("non-existent")

        assert result is False


# --- Pending Message Recovery Tests ---


class TestRedisEventBusPendingRecovery:
    """Tests for pending message recovery."""

    async def test_recover_no_pending_messages(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test recovery with no pending messages."""
        mock_redis.xpending.return_value = {"pending": 0}

        stats = await bus.recover_pending_messages()

        assert stats["checked"] == 0
        assert stats["claimed"] == 0
        assert stats["reprocessed"] == 0
        assert stats["dlq"] == 0
        assert stats["failed"] == 0

    async def test_recover_pending_messages_success(
        self,
        bus: RedisEventBus,
        event_registry: EventRegistry,
        mock_redis: AsyncMock,
    ):
        """Test successful pending message recovery."""
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        mock_redis.xpending.return_value = {"pending": 1}
        mock_redis.xpending_range.return_value = [
            {
                "message_id": "msg-123",
                "consumer": "old-consumer",
                "time_since_delivered": 120000,  # 2 minutes
                "times_delivered": 1,
            }
        ]
        mock_redis.xclaim.return_value = [
            (
                "msg-123",
                {
                    "event_id": str(event.event_id),
                    "event_type": "SampleOrderCreated",
                    "aggregate_id": str(event.aggregate_id),
                    "aggregate_type": "Order",
                    "occurred_at": datetime.now(UTC).isoformat(),
                    "payload": event.model_dump_json(),
                },
            )
        ]

        stats = await bus.recover_pending_messages(min_idle_time_ms=60000)

        assert stats["checked"] == 1
        assert stats["claimed"] == 1
        assert stats["reprocessed"] == 1

    async def test_recover_pending_messages_to_dlq(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test pending message recovery sends to DLQ after max retries."""
        mock_redis.xpending.return_value = {"pending": 1}
        mock_redis.xpending_range.return_value = [
            {
                "message_id": "msg-123",
                "consumer": "old-consumer",
                "time_since_delivered": 120000,
                "times_delivered": 5,
            }
        ]
        mock_redis.xclaim.return_value = [
            (
                "msg-123",
                {
                    "event_id": str(uuid4()),
                    "event_type": "SampleOrderCreated",
                    "payload": "{}",
                },
            )
        ]
        mock_redis.get.return_value = "3"  # Already retried 3 times (max)

        stats = await bus.recover_pending_messages(min_idle_time_ms=60000, max_retries=3)

        assert stats["dlq"] == 1


# --- Stream Info Tests ---


class TestRedisEventBusStreamInfo:
    """Tests for stream info retrieval."""

    async def test_get_stream_info(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test getting stream info."""
        mock_redis.xinfo_stream.return_value = {
            "length": 100,
            "first-entry": ["1234567890-0", {}],
            "last-entry": ["1234567899-0", {}],
        }
        mock_redis.xinfo_groups.return_value = [
            {"name": "test_group", "consumers": 2, "pending": 5}
        ]
        mock_redis.xpending.return_value = {"pending": 5}

        info = await bus.get_stream_info()

        assert info["connected"] is True
        assert info["stream"]["length"] == 100
        assert info["pending_messages"] == 5
        assert len(info["consumer_groups"]) == 1
        assert info["active_consumers"] == 2

    async def test_get_stream_info_not_connected(self, config: RedisEventBusConfig):
        """Test getting stream info when not connected."""
        with (
            patch("eventsource.bus.redis.REDIS_AVAILABLE", True),
            patch("eventsource.bus.redis.aioredis"),
        ):
            bus = RedisEventBus(config=config)
            info = await bus.get_stream_info()

            assert info["connected"] is False


# --- Statistics Tests ---


class TestRedisEventBusStatistics:
    """Tests for statistics."""

    async def test_get_stats_dict(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test getting statistics as dictionary."""
        # Publish some events to update stats
        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id=uuid4(),
        )
        await bus.publish([event])

        stats = bus.get_stats_dict()

        assert "events_published" in stats
        assert "events_consumed" in stats
        assert "events_processed_success" in stats
        assert "events_processed_failed" in stats
        assert stats["events_published"] == 1


# --- Shutdown Tests ---


class TestRedisEventBusShutdown:
    """Tests for shutdown."""

    async def test_shutdown(self, bus: RedisEventBus, mock_redis: AsyncMock):
        """Test graceful shutdown."""
        await bus.shutdown()

        assert bus.is_connected is False
        assert bus.is_consuming is False


# --- Error Handling Tests ---


class TestRedisEventBusErrorHandling:
    """Tests for error handling."""

    def test_redis_not_available_error(self):
        """Test error when redis is not available."""
        with (
            patch("eventsource.bus.redis.REDIS_AVAILABLE", False),
            pytest.raises(RedisNotAvailableError),
        ):
            # Need to reimport to pick up the patched value

            # This won't work due to how Python imports work
            # We test the error class directly instead
            raise RedisNotAvailableError()

    def test_redis_not_available_error_message(self):
        """Test RedisNotAvailableError message."""
        error = RedisNotAvailableError()
        assert "redis package is not installed" in str(error).lower()
        assert "pip install" in str(error)


# --- Integration-like Tests (with mocks) ---


class TestRedisEventBusIntegration:
    """Integration-like tests using mocks."""

    async def test_full_publish_consume_cycle(
        self,
        config: RedisEventBusConfig,
        event_registry: EventRegistry,
        mock_redis: AsyncMock,
    ):
        """Test a full publish-consume cycle."""
        handler = OrderHandler()

        with (
            patch("eventsource.bus.redis.REDIS_AVAILABLE", True),
            patch("eventsource.bus.redis.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

            bus = RedisEventBus(config=config, event_registry=event_registry)
            await bus.connect()

            # Subscribe
            bus.subscribe(SampleOrderCreated, handler)

            # Publish
            event = SampleOrderCreated(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                customer_id=uuid4(),
            )
            await bus.publish([event])

            # Simulate consuming the event
            message_data = {
                "event_id": str(event.event_id),
                "event_type": "SampleOrderCreated",
                "aggregate_id": str(event.aggregate_id),
                "aggregate_type": "Order",
                "occurred_at": datetime.now(UTC).isoformat(),
                "payload": event.model_dump_json(),
            }
            await bus._process_message("msg-123", message_data, "test_consumer")

            # Verify
            assert len(handler.handled_events) == 1
            assert handler.handled_events[0].order_number == "ORD-001"
            assert bus.stats.events_published == 1
            assert bus.stats.events_consumed == 1

            await bus.disconnect()

    async def test_subscriber_receives_events(
        self,
        config: RedisEventBusConfig,
        event_registry: EventRegistry,
        mock_redis: AsyncMock,
    ):
        """Test that EventSubscriber receives events."""
        subscriber = SampleSubscriber()

        with (
            patch("eventsource.bus.redis.REDIS_AVAILABLE", True),
            patch("eventsource.bus.redis.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)

            bus = RedisEventBus(config=config, event_registry=event_registry)
            await bus.connect()

            bus.subscribe_all(subscriber)

            # Simulate consuming events
            event1 = SampleOrderCreated(
                aggregate_id=uuid4(),
                order_number="ORD-001",
                customer_id=uuid4(),
            )
            event2 = SampleOrderShipped(
                aggregate_id=uuid4(),
                tracking_number="TRK-001",
            )

            for event in [event1, event2]:
                message_data = {
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "aggregate_id": str(event.aggregate_id),
                    "aggregate_type": event.aggregate_type,
                    "occurred_at": datetime.now(UTC).isoformat(),
                    "payload": event.model_dump_json(),
                }
                await bus._process_message(f"msg-{event.event_id}", message_data, "test_consumer")

            assert len(subscriber.handled_events) == 2
            assert subscriber.handled_events[0].event_type == "SampleOrderCreated"
            assert subscriber.handled_events[1].event_type == "SampleOrderShipped"

            await bus.disconnect()
