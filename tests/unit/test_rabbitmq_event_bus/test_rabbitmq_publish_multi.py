"""Unit tests extracted for test_rabbitmq_publish_multi.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
)

from ._fixtures import PublishTestEvent, SerializationOrderCreatedEvent


class TestRabbitMQPublishSingle:
    """Tests for RabbitMQEventBus._publish_single() internal method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_single_raises_when_exchange_none(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _publish_single raises RuntimeError if exchange is None."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        bus._exchange = None

        with pytest.raises(RuntimeError, match="Exchange not initialized"):
            await bus._publish_single(event)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_single_wait_for_confirm_true(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test _publish_single with wait_for_confirm=True increments confirms."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus._publish_single(event, wait_for_confirm=True)

        assert bus.stats.events_published == 1
        assert bus.stats.publish_confirms == 1

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_single_wait_for_confirm_false(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test _publish_single with wait_for_confirm=False does not increment confirms."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus._publish_single(event, wait_for_confirm=False)

        assert bus.stats.events_published == 1
        assert bus.stats.publish_confirms == 0


class TestRabbitMQPublishMultipleEvents:
    """Tests for publishing multiple events with RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_multiple_events_sequential(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that multiple events are published sequentially."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish(events)

        assert mock_exchange.publish.call_count == 5
        assert bus.stats.events_published == 5
        assert bus.stats.publish_confirms == 5

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_handles_partial_failure_in_batch(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that batch publishing handles partial failures.

        With batch publishing (concurrent), all events are attempted even if
        some fail. The first error encountered is re-raised after all events
        are processed.
        """
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        # First call succeeds, second fails, third succeeds
        mock_exchange.publish = AsyncMock(side_effect=[None, Exception("Connection lost"), None])
        mock_queue = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(3)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        with pytest.raises(Exception, match="Connection lost"):
            await bus.publish(events)

        # With batch publishing, all events are attempted concurrently
        # So 2 events succeed (first and third), 1 fails (second)
        assert bus.stats.events_published == 2
        assert mock_exchange.publish.call_count == 3  # All 3 attempted
        assert bus.stats.batch_publishes == 1
        assert bus.stats.batch_partial_failures == 1


class TestRabbitMQPublishWithDifferentEventTypes:
    """Tests for publishing different event types."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_mixed_event_types(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publishing events of different types uses correct routing keys."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Create different event types
        event1 = PublishTestEvent(aggregate_id=uuid4())
        event2 = SerializationOrderCreatedEvent(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id="customer-123",
        )

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish([event1, event2])

        # Verify different routing keys were used
        calls = mock_exchange.publish.call_args_list
        assert len(calls) == 2
        assert calls[0].kwargs["routing_key"] == "TestAggregate.PublishTestEvent"
        assert calls[1].kwargs["routing_key"] == "Order.SerializationOrderCreated"
