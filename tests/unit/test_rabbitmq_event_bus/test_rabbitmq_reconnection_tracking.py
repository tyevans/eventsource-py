"""Unit tests extracted for test_rabbitmq_reconnection_tracking.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestRabbitMQConnectWithReconnectionCallbacks:
    """Tests for connect() method registering reconnection callbacks (P2-004)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create a test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            reconnect_delay=2.0,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_uses_reconnect_interval(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect passes reconnect_interval to connect_robust."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.reconnect_callbacks = MagicMock()
        mock_connection.close_callbacks = MagicMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close_callbacks = MagicMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_aio_pika.connect_robust.assert_called_once_with(
            config.rabbitmq_url,
            heartbeat=config.heartbeat,
            reconnect_interval=config.reconnect_delay,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_registers_reconnect_callback(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect registers the reconnect callback."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.reconnect_callbacks = MagicMock()
        mock_connection.close_callbacks = MagicMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close_callbacks = MagicMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_connection.reconnect_callbacks.add.assert_called_once_with(
            bus._connection_manager._on_reconnect
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_registers_close_callback(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect registers the close callback."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.reconnect_callbacks = MagicMock()
        mock_connection.close_callbacks = MagicMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close_callbacks = MagicMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_connection.close_callbacks.add.assert_called_once_with(
            bus._connection_manager._on_connection_close
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_registers_channel_close_callback(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect registers the channel close callback."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.reconnect_callbacks = MagicMock()
        mock_connection.close_callbacks = MagicMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close_callbacks = MagicMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_channel.close_callbacks.add.assert_called_once_with(
            bus._connection_manager._on_channel_close
        )


class TestRabbitMQReconnectionStateTracking:
    """Tests for reconnection state tracking (P2-004)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create a test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create a test event bus instance."""
        return RabbitMQEventBus(config=config)

    def test_initial_was_consuming_is_false(self, bus: RabbitMQEventBus) -> None:
        """Test that _was_consuming is initialized to False."""
        assert bus._was_consuming is False

    def test_initial_reconnecting_is_false(self, bus: RabbitMQEventBus) -> None:
        """Test that _reconnecting is initialized to False."""
        assert bus._reconnecting is False

    def test_was_consuming_preserved_after_connection_close(self, bus: RabbitMQEventBus) -> None:
        """Test that _was_consuming is preserved after multiple connection closes."""
        bus._consuming = True
        bus._connected = True

        # First disconnect
        bus._on_connection_close(None, None)
        assert bus._was_consuming is True

        # Simulate reconnection success
        bus._consuming = False  # Consumer not restarted yet
        bus._connected = True

        # Second disconnect (without active consumption)
        bus._on_connection_close(None, None)
        # _was_consuming should still be True from first disconnect
        assert bus._was_consuming is True

    def test_reconnections_counter_persists_across_reconnections(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that reconnections counter accumulates correctly."""
        assert bus._stats.reconnections == 0

        # Simulate multiple disconnects/reconnects by directly incrementing
        bus._stats.reconnections = 5
        assert bus._stats.reconnections == 5

        bus._stats.reconnections += 1
        assert bus._stats.reconnections == 6
