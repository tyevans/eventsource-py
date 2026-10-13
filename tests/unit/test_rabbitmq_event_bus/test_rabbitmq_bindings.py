"""Unit tests extracted for test_rabbitmq_bindings.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from eventsource.domain.event import DomainEvent
from eventsource.ports.exceptions import EventBusConnectionError


class TestBindEventType:
    """Tests for bind_event_type helper method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            exchange_type="direct",
            consumer_group="test-workers",
            enable_dlq=False,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_bind_event_type_creates_binding(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that bind_event_type creates a binding for the event type."""
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

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Reset mock to clear initial bind call
        mock_queue.bind.reset_mock()

        # Bind for specific event type
        await bus.bind_event_type(OrderCreated)

        # Verify binding was created with correct routing key
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == "Order.OrderCreated"

    @pytest.mark.asyncio
    async def test_bind_event_type_raises_when_not_connected(self) -> None:
        """Test that bind_event_type raises RuntimeError when not connected."""
        config = RabbitMQEventBusConfig()

        class TestEvent(DomainEvent):
            aggregate_type: str = "Test"

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(EventBusConnectionError, match="Not connected"):
            await bus.bind_event_type(TestEvent)


class TestBindRoutingKey:
    """Tests for bind_routing_key helper method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            exchange_type="direct",
            consumer_group="test-workers",
            enable_dlq=False,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_bind_routing_key_creates_binding(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that bind_routing_key creates a binding with the specified key."""
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

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Reset mock to clear initial bind call
        mock_queue.bind.reset_mock()

        # Bind with custom routing key
        await bus.bind_routing_key("Order.OrderCreated")

        # Verify binding was created
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == "Order.OrderCreated"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_bind_routing_key_supports_wildcards_for_topic(
        self, mock_aio_pika: MagicMock
    ) -> None:
        """Test that bind_routing_key works with wildcards for topic exchange."""
        config = RabbitMQEventBusConfig(
            exchange_type="topic",
            enable_dlq=False,
        )

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

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Reset mock to clear initial bind call
        mock_queue.bind.reset_mock()

        # Bind with wildcard pattern
        await bus.bind_routing_key("Order.*")

        # Verify binding was created with wildcard
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == "Order.*"

    @pytest.mark.asyncio
    async def test_bind_routing_key_raises_when_not_connected(self) -> None:
        """Test that bind_routing_key raises RuntimeError when not connected."""
        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config)

        with pytest.raises(EventBusConnectionError, match="Not connected"):
            await bus.bind_routing_key("some.key")
