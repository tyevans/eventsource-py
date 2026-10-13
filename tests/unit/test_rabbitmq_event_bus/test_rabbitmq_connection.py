"""Unit tests extracted for test_rabbitmq_connection.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
)


class TestRabbitMQEventBusConnection:
    """Tests for RabbitMQEventBus connect and disconnect methods."""

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
    async def test_connect_creates_connection(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect creates a robust connection."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_aio_pika.connect_robust.assert_called_once_with(
            config.rabbitmq_url,
            heartbeat=config.heartbeat,
            reconnect_interval=config.reconnect_delay,
        )
        assert bus.is_connected is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_creates_channel(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect creates a channel."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_connection.channel.assert_called_once()
        assert bus._channel is mock_channel

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_sets_prefetch(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect sets prefetch count on channel."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_channel.set_qos.assert_called_once_with(prefetch_count=config.prefetch_count)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_when_already_connected_logs_warning(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that calling connect when already connected logs warning."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Reset mock to track second call
        mock_aio_pika.connect_robust.reset_mock()

        # Connect again
        await bus.connect()

        # Should not call connect_robust again
        mock_aio_pika.connect_robust.assert_not_called()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_handles_connection_error(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect handles connection errors properly."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_aio_pika.connect_robust = AsyncMock(side_effect=Exception("Connection refused"))

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(Exception, match="Connection refused"):
            await bus.connect()

        assert bus.is_connected is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_closes_connection(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that disconnect closes the connection."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.disconnect()

        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()
        assert bus.is_connected is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_clears_references(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that disconnect clears all references."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.disconnect()

        assert bus._connection is None
        assert bus._channel is None
        assert bus._exchange is None
        assert bus._dlq_exchange is None
        assert bus._consumer_queue is None
        assert bus._dlq_queue is None

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that disconnect works when not connected."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        # Should not raise
        await bus.disconnect()

        assert bus.is_connected is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_cancels_consumer_task(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that disconnect cancels consumer task if running."""
        import asyncio

        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Simulate a running consumer task
        async def mock_consumer() -> None:
            await asyncio.sleep(100)

        bus._consumer_task = asyncio.create_task(mock_consumer())

        await bus.disconnect()

        assert bus._consumer_task is None


class TestRabbitMQConnectionStateTransitions:
    """Tests for connection state transitions."""

    def test_initial_state_is_disconnected(self) -> None:
        """Test that a new bus starts in disconnected state."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        assert bus.is_connected is False
        assert bus.is_consuming is False
        assert bus._connection is None
        assert bus._channel is None
        assert bus._exchange is None
        assert bus._consumer_queue is None

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connection_state_after_connect_then_disconnect(
        self, mock_aio_pika: MagicMock
    ) -> None:
        """Test state transitions through connect/disconnect cycle."""
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

        bus = RabbitMQEventBus()

        # Initial state
        assert bus.is_connected is False

        # After connect
        await bus.connect()
        assert bus.is_connected is True
        assert bus._connection is not None
        assert bus._channel is not None

        # After disconnect
        await bus.disconnect()
        assert bus.is_connected is False
        assert bus._connection is None
        assert bus._channel is None
        assert bus._exchange is None
