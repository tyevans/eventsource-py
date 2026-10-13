"""Unit tests extracted for test_rabbitmq_topology_connection.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
)


class TestRabbitMQConnectionWithTopology:
    """Tests for the complete connection flow with topology setup."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_sets_up_complete_topology(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect sets up complete topology."""
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

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Verify all components are set up
        assert bus._connection is not None
        assert bus._channel is not None
        assert bus._exchange is not None
        assert bus._consumer_queue is not None
        assert bus._dlq_exchange is not None
        assert bus._dlq_queue is not None
        assert bus.is_connected is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_cleanup_on_failure(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect cleans up on failure."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        # Fail during exchange declaration
        mock_channel.declare_exchange = AsyncMock(
            side_effect=Exception("Exchange declaration failed")
        )

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(Exception, match="Exchange declaration failed"):
            await bus.connect()

        # Verify cleanup was attempted
        assert bus.is_connected is False
        assert bus._exchange is None
        assert bus._consumer_queue is None

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_order_dlq_before_main(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ is declared before main queue."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        call_order: list[str] = []

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        async def track_exchange(name: str, **kwargs: object) -> AsyncMock:
            call_order.append(f"exchange:{name}")
            return mock_exchange

        async def track_queue(name: str, **kwargs: object) -> AsyncMock:
            call_order.append(f"queue:{name}")
            return mock_queue

        mock_channel.declare_exchange = AsyncMock(side_effect=track_exchange)
        mock_channel.declare_queue = AsyncMock(side_effect=track_queue)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # DLQ exchange should be declared before main exchange
        dlq_exchange_idx = call_order.index(f"exchange:{config.dlq_exchange_name}")
        main_exchange_idx = call_order.index(f"exchange:{config.exchange_name}")
        assert dlq_exchange_idx < main_exchange_idx

        # DLQ queue should be declared before main queue
        dlq_queue_idx = call_order.index(f"queue:{config.dlq_queue_name}")
        main_queue_idx = call_order.index(f"queue:{config.queue_name}")
        assert dlq_queue_idx < main_queue_idx
