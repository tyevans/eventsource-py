"""Unit tests extracted for test_rabbitmq_reconnection.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


@pytest.fixture
def config() -> RabbitMQEventBusConfig:
    """Create a test configuration."""
    return RabbitMQEventBusConfig(
        rabbitmq_url="amqp://test:test@localhost/",
        exchange_name="test-events",
        consumer_group="test-group",
        reconnect_delay=1.0,
    )


@pytest.fixture
def bus(config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
    """Create a test event bus instance."""
    return RabbitMQEventBus(config=config)


def _setup_reconnect_mocks(bus: RabbitMQEventBus) -> tuple[AsyncMock, AsyncMock]:
    """Create mock connection, channel, and topology declaration methods on bus."""
    mock_connection = AsyncMock()
    mock_channel = AsyncMock()
    mock_channel.close_callbacks = MagicMock()
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    bus._topology._declare_dlq = AsyncMock()
    bus._topology._declare_exchange = AsyncMock()
    bus._topology._declare_queue = AsyncMock()
    bus._topology._bind_queue = AsyncMock()
    return mock_connection, mock_channel


class TestRabbitMQReconnectionCallbacks:
    """Tests for reconnection callback methods (P2-004)."""

    @pytest.mark.asyncio
    async def test_on_reconnect_increments_reconnection_stats(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect increments the reconnections counter."""
        bus._connection_manager._connected = False
        bus._stats.reconnections = 0
        mock_connection, _ = _setup_reconnect_mocks(bus)
        await bus._on_reconnect(mock_connection)
        assert bus._stats.reconnections == 1

    @pytest.mark.asyncio
    async def test_on_reconnect_multiple_times_increments_counter(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that multiple reconnections increment the counter correctly."""
        bus._connection_manager._connected = False
        bus._stats.reconnections = 0
        mock_connection, _ = _setup_reconnect_mocks(bus)
        await bus._on_reconnect(mock_connection)
        await bus._on_reconnect(mock_connection)
        await bus._on_reconnect(mock_connection)
        assert bus._stats.reconnections == 3

    @pytest.mark.asyncio
    async def test_on_reconnect_recreates_channel(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect recreates the channel."""
        mock_connection, mock_channel = _setup_reconnect_mocks(bus)
        await bus._on_reconnect(mock_connection)
        mock_connection.channel.assert_called_once()
        assert bus._connection_manager._channel is mock_channel

    @pytest.mark.asyncio
    async def test_on_reconnect_sets_prefetch_count(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect sets QoS on the new channel."""
        mock_connection, mock_channel = _setup_reconnect_mocks(bus)
        await bus._on_reconnect(mock_connection)
        mock_channel.set_qos.assert_called_once_with(prefetch_count=bus._config.prefetch_count)

    @pytest.mark.asyncio
    async def test_on_reconnect_redeclares_dlq_when_enabled(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect redeclares DLQ when enabled."""
        bus._config.enable_dlq = True
        mock_connection, _ = _setup_reconnect_mocks(bus)
        await bus._on_reconnect(mock_connection)
        bus._topology._declare_dlq.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_reconnect_skips_dlq_when_disabled(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect skips DLQ declaration when disabled."""
        bus._config.enable_dlq = False
        mock_connection, _ = _setup_reconnect_mocks(bus)
        await bus._on_reconnect(mock_connection)
        bus._topology._declare_dlq.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_reconnect_redeclares_exchange(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect redeclares the exchange."""
        mock_connection, _ = _setup_reconnect_mocks(bus)
        await bus._on_reconnect(mock_connection)
        bus._topology._declare_exchange.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_reconnect_redeclares_queue(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect redeclares the queue."""
        mock_connection, _ = _setup_reconnect_mocks(bus)
        await bus._on_reconnect(mock_connection)
        bus._topology._declare_queue.assert_called_once()
        bus._topology._bind_queue.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_reconnect_sets_connected_true_on_success(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect sets _connected to True on success."""
        bus._connection_manager._connected = False
        mock_connection, _ = _setup_reconnect_mocks(bus)
        await bus._on_reconnect(mock_connection)
        assert bus._connection_manager._connected is True

    @pytest.mark.asyncio
    async def test_on_reconnect_sets_connected_false_on_failure(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that _on_reconnect sets _connected to False on failure."""
        bus._connection_manager._connected = False
        mock_connection = AsyncMock()
        mock_connection.channel = AsyncMock(side_effect=Exception("Channel error"))
        await bus._on_reconnect(mock_connection)
        assert bus._connection_manager._connected is False

    @pytest.mark.asyncio
    async def test_on_reconnect_sets_reconnecting_flag(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect manages the _reconnecting flag."""
        bus._connection_manager._reconnecting = False
        mock_connection, _ = _setup_reconnect_mocks(bus)
        await bus._on_reconnect(mock_connection)
        assert bus._connection_manager._reconnecting is False

    @pytest.mark.asyncio
    async def test_on_reconnect_clears_reconnecting_on_failure(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect clears _reconnecting on failure."""
        mock_connection = AsyncMock()
        mock_connection.channel = AsyncMock(side_effect=Exception("Channel error"))
        await bus._on_reconnect(mock_connection)
        assert bus._connection_manager._reconnecting is False


class TestRabbitMQConnectionCloseCallback:
    """Tests for connection close callback (P2-004)."""

    def test_connection_close_updates_connected_state(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_connection_close sets _connected to False."""
        bus._connection_manager._connected = True
        bus._on_connection_close(None, None)
        assert bus._connection_manager._connected is False

    def test_connection_close_tracks_was_consuming_when_consuming(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that _on_connection_close tracks consuming state."""
        bus._connection_manager._connected = True
        bus._consumer._consuming = True
        bus._connection_manager._was_consuming = False
        bus._on_connection_close(None, None)
        assert bus._connection_manager._was_consuming is True

    def test_connection_close_does_not_set_was_consuming_when_not_consuming(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that _on_connection_close doesn't set _was_consuming when not consuming."""
        bus._connection_manager._connected = True
        bus._consumer._consuming = False
        bus._connection_manager._was_consuming = False
        bus._on_connection_close(None, None)
        assert bus._connection_manager._was_consuming is False

    def test_connection_close_with_exception(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_connection_close handles exceptions."""
        bus._connection_manager._connected = True
        bus._on_connection_close(MagicMock(), ValueError("Connection lost"))
        assert bus._connection_manager._connected is False

    def test_connection_close_without_exception(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_connection_close handles graceful close."""
        bus._connection_manager._connected = True
        bus._on_connection_close(MagicMock(), None)
        assert bus._connection_manager._connected is False


class TestRabbitMQChannelCloseCallback:
    """Tests for channel close callback (P2-004)."""

    def test_channel_close_with_exception(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_channel_close handles exception case."""
        bus._on_channel_close(MagicMock(), ValueError("Channel closed unexpectedly"))

    def test_channel_close_without_exception(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_channel_close handles normal close."""
        bus._on_channel_close(MagicMock(), None)

    def test_channel_close_with_none_channel(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_channel_close handles None channel."""
        bus._on_channel_close(None, None)
