"""Core lifecycle, initialization, stats, and subscription tests for KafkaEventBus."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from eventsource.adapters.kafka import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
    KafkaEventBusStats,
    KafkaNotAvailableError,
)
from eventsource.domain.event_registry import EventRegistry
from eventsource.ports.exceptions import EventBusConnectionError

from ._fixtures import (
    OrderHandler,
    SampleOrderCreated,
    SampleOrderShipped,
    SampleSubscriber,
    event_registry,
    sample_event,
)

__all__ = [
    "TestKafkaEventBus",
    "TestKafkaEventBusStats",
    "TestKafkaNotAvailableError",
    "event_registry",
    "sample_event",
]


class TestKafkaEventBusStats:
    """Tests for KafkaEventBusStats dataclass."""

    def test_default_values(self) -> None:
        """Test default statistics values."""
        stats = KafkaEventBusStats()

        assert stats.events_published == 0
        assert stats.events_consumed == 0
        assert stats.events_processed_success == 0
        assert stats.events_processed_failed == 0
        assert stats.messages_sent_to_dlq == 0
        assert stats.handler_errors == 0
        assert stats.reconnections == 0
        assert stats.rebalance_count == 0
        assert stats.last_publish_at is None
        assert stats.last_consume_at is None
        assert stats.last_error_at is None
        assert stats.connected_at is None

    def test_get_stats_dict(self) -> None:
        """Test stats dictionary serialization."""
        now = datetime.now(UTC)
        stats = KafkaEventBusStats(
            events_published=10,
            events_consumed=8,
            events_processed_success=7,
            events_processed_failed=1,
            last_publish_at=now,
            connected_at=now,
        )

        stats_dict = stats.get_stats_dict()

        assert stats_dict["events_published"] == 10
        assert stats_dict["events_consumed"] == 8
        assert stats_dict["events_processed_success"] == 7
        assert stats_dict["events_processed_failed"] == 1
        assert stats_dict["last_publish_at"] == now.isoformat()
        assert stats_dict["connected_at"] == now.isoformat()
        assert stats_dict["last_consume_at"] is None


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
class TestKafkaEventBus:
    """Tests for KafkaEventBus class."""

    def test_initialization(self, event_registry: EventRegistry) -> None:
        """Test basic initialization."""
        config = KafkaEventBusConfig(security_protocol="PLAINTEXT")
        bus = KafkaEventBus(config=config, event_registry=event_registry)

        assert bus.config is config
        assert bus.is_connected is False
        assert bus.is_consuming is False
        assert bus.get_subscriber_count() == 0

    def test_initialization_with_defaults(self, event_registry: EventRegistry) -> None:
        """Test initialization with default config."""
        bus = KafkaEventBus(event_registry=event_registry)

        assert bus.config is not None
        assert bus.config.bootstrap_servers == "localhost:9092"

    def test_subscribe(self, event_registry: EventRegistry) -> None:
        """Test subscribing a handler."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler)

        assert bus.get_subscriber_count(SampleOrderCreated) == 1

    def test_subscribe_multiple_handlers(self, event_registry: EventRegistry) -> None:
        """Test subscribing multiple handlers to the same event."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler1 = OrderHandler()
        handler2 = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe(SampleOrderCreated, handler2)

        assert bus.get_subscriber_count(SampleOrderCreated) == 2

    def test_unsubscribe(self, event_registry: EventRegistry) -> None:
        """Test unsubscribing a handler."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler)
        assert bus.get_subscriber_count(SampleOrderCreated) == 1

        result = bus.unsubscribe(SampleOrderCreated, handler)
        assert result is True
        assert bus.get_subscriber_count(SampleOrderCreated) == 0

    def test_unsubscribe_not_found(self, event_registry: EventRegistry) -> None:
        """Test unsubscribing a handler that was not subscribed."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler = OrderHandler()

        result = bus.unsubscribe(SampleOrderCreated, handler)
        assert result is False

    def test_subscribe_all(self, event_registry: EventRegistry) -> None:
        """Test subscribe_all with a subscriber."""
        bus = KafkaEventBus(event_registry=event_registry)
        subscriber = SampleSubscriber()

        bus.subscribe_all(subscriber)

        assert bus.get_subscriber_count(SampleOrderCreated) == 1
        assert bus.get_subscriber_count(SampleOrderShipped) == 1

    def test_subscribe_to_all_events(self, event_registry: EventRegistry) -> None:
        """Test wildcard subscription."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler = OrderHandler()

        bus.subscribe_to_all_events(handler)

        assert bus.get_wildcard_subscriber_count() == 1

    def test_unsubscribe_from_all_events(self, event_registry: EventRegistry) -> None:
        """Test unsubscribing from wildcard subscription."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler = OrderHandler()

        bus.subscribe_to_all_events(handler)
        assert bus.get_wildcard_subscriber_count() == 1

        result = bus.unsubscribe_from_all_events(handler)
        assert result is True
        assert bus.get_wildcard_subscriber_count() == 0

    def test_clear_subscribers(self, event_registry: EventRegistry) -> None:
        """Test clearing all subscribers."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler1 = OrderHandler()
        handler2 = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe_to_all_events(handler2)

        bus.clear_subscribers()

        assert bus.get_subscriber_count() == 0
        assert bus.get_wildcard_subscriber_count() == 0

    def test_handlers_for_class_lookup(self, event_registry: EventRegistry) -> None:
        """Test the non-deprecated class-keyed handler lookup."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler1 = OrderHandler()
        handler2 = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe_to_all_events(handler2)

        handlers = bus._handlers_for(SampleOrderCreated)
        assert len(handlers) == 2

    @pytest.mark.asyncio
    async def test_publish_without_connection_raises(
        self, event_registry: EventRegistry, sample_event: SampleOrderCreated
    ) -> None:
        """Test publishing without connection raises error."""
        bus = KafkaEventBus(event_registry=event_registry)

        with pytest.raises(EventBusConnectionError, match="Not connected"):
            await bus.publish([sample_event])

    @pytest.mark.asyncio
    async def test_connect_disconnect(self, event_registry: EventRegistry) -> None:
        """Test connect and disconnect lifecycle."""
        bus = KafkaEventBus(event_registry=event_registry)

        with (
            patch("eventsource.adapters.kafka.connection.AIOKafkaProducer") as mock_producer_class,
            patch("eventsource.adapters.kafka.connection.AIOKafkaConsumer") as mock_consumer_class,
        ):
            mock_producer = AsyncMock()
            mock_consumer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            mock_consumer_class.return_value = mock_consumer

            await bus.connect()
            assert bus.is_connected is True

            await bus.disconnect()
            assert bus.is_connected is False

            mock_producer.start.assert_called_once()
            mock_producer.stop.assert_called_once()
            mock_consumer.start.assert_called_once()
            mock_consumer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self, event_registry: EventRegistry) -> None:
        """Test async context manager usage."""
        with (
            patch("eventsource.adapters.kafka.connection.AIOKafkaProducer") as mock_producer_class,
            patch("eventsource.adapters.kafka.connection.AIOKafkaConsumer") as mock_consumer_class,
        ):
            mock_producer = AsyncMock()
            mock_consumer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            mock_consumer_class.return_value = mock_consumer

            async with KafkaEventBus(event_registry=event_registry) as bus:
                assert bus.is_connected is True

            # After exiting context, should be disconnected
            assert bus.is_connected is False


class TestKafkaNotAvailableError:
    """Tests for KafkaNotAvailableError."""

    def test_error_message(self) -> None:
        """Test error message is informative."""
        error = KafkaNotAvailableError()
        assert "aiokafka" in str(error)
        assert "pip install" in str(error)
