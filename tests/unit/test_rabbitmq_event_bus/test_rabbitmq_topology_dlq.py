"""Unit tests extracted for test_rabbitmq_topology_dlq.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
)


class TestRabbitMQDLQDeclaration:
    """Tests for dead letter queue declaration in RabbitMQEventBus."""

    @pytest.fixture
    def config_with_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration with DLQ enabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_exchange_declared_when_enabled(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ exchange is declared when DLQ is enabled."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_dlq_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Return different mocks for different exchanges
        mock_channel.declare_exchange = AsyncMock(side_effect=[mock_dlq_exchange, mock_exchange])
        mock_channel.declare_queue = AsyncMock(side_effect=[mock_dlq_queue, mock_queue])

        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        # Should have 2 exchange declarations (DLQ + main)
        assert mock_channel.declare_exchange.call_count == 2

        # First call should be DLQ exchange
        first_call_kwargs = mock_channel.declare_exchange.call_args_list[0].kwargs
        assert first_call_kwargs["name"] == config_with_dlq.dlq_exchange_name

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_exchange_is_direct_type(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ exchange is of direct type."""
        from aio_pika import ExchangeType

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

        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        # First call should be DLQ exchange with DIRECT type
        first_call_kwargs = mock_channel.declare_exchange.call_args_list[0].kwargs
        assert first_call_kwargs["type"] == ExchangeType.DIRECT

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_declared_when_enabled(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ queue is declared when DLQ is enabled."""
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

        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        # Should have 2 queue declarations (DLQ + main)
        assert mock_channel.declare_queue.call_count == 2

        # First call should be DLQ queue
        first_call_kwargs = mock_channel.declare_queue.call_args_list[0].kwargs
        assert first_call_kwargs["name"] == config_with_dlq.dlq_queue_name

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_bound_to_dlq_exchange(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ queue is bound to DLQ exchange."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_dlq_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(side_effect=[mock_dlq_exchange, mock_exchange])
        mock_channel.declare_queue = AsyncMock(side_effect=[mock_dlq_queue, mock_queue])

        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        # DLQ queue should be bound to DLQ exchange
        mock_dlq_queue.bind.assert_called_once()
        dlq_bind_kwargs = mock_dlq_queue.bind.call_args.kwargs
        assert dlq_bind_kwargs["exchange"] is mock_dlq_exchange
        assert dlq_bind_kwargs["routing_key"] == config_with_dlq.queue_name

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_references_stored(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ references are stored after declaration."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_dlq_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(side_effect=[mock_dlq_exchange, mock_exchange])
        mock_channel.declare_queue = AsyncMock(side_effect=[mock_dlq_queue, mock_queue])

        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        assert bus._dlq_exchange is mock_dlq_exchange
        assert bus._dlq_queue is mock_dlq_queue

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_no_dlq_when_disabled(self, mock_aio_pika: MagicMock) -> None:
        """Test that no DLQ is declared when DLQ is disabled."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="test-events",
            consumer_group="test-group",
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

        # Should have only 1 exchange declaration (main only)
        assert mock_channel.declare_exchange.call_count == 1

        # Should have only 1 queue declaration (main only)
        assert mock_channel.declare_queue.call_count == 1

        # DLQ references should be None
        assert bus._dlq_exchange is None
        assert bus._dlq_queue is None
