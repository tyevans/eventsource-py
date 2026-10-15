"""Unit tests extracted for test_rabbitmq_connection.py."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig


def _mock_conn_and_chan(mock_aio_pika: MagicMock) -> tuple[AsyncMock, AsyncMock]:
    mock_connection, mock_channel = AsyncMock(is_closed=False), AsyncMock(is_closed=False)
    mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    return mock_connection, mock_channel


class TestRabbitMQEventBusConnection:
    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
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
        _mock_conn_and_chan(mock_aio_pika)
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
        mock_connection, mock_channel = _mock_conn_and_chan(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_connection.channel.assert_called_once()
        assert bus._connection_manager._channel is mock_channel

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_sets_prefetch(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        _, mock_channel = _mock_conn_and_chan(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_channel.set_qos.assert_called_once_with(prefetch_count=config.prefetch_count)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_when_already_connected_logs_warning(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        _mock_conn_and_chan(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        mock_aio_pika.connect_robust.reset_mock()

        await bus.connect()
        mock_aio_pika.connect_robust.assert_not_called()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_handles_connection_error(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
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
        mock_connection, mock_channel = _mock_conn_and_chan(mock_aio_pika)
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
        _mock_conn_and_chan(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.disconnect()

        assert bus._connection_manager._connection is None
        assert bus._connection_manager._channel is None
        assert bus._topology._exchange is None
        assert bus._topology._dlq_exchange is None
        assert bus._topology._consumer_queue is None
        assert bus._topology._dlq_queue is None

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        bus = RabbitMQEventBus(config=config)
        await bus.disconnect()
        assert bus.is_connected is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_cancels_consumer_task(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        _mock_conn_and_chan(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        async def mock_consumer() -> None:
            await asyncio.sleep(100)

        bus._consumer._consumer_task = asyncio.create_task(mock_consumer())
        await bus.disconnect()
        assert bus._consumer._consumer_task is None


class TestRabbitMQConnectionStateTransitions:
    def test_initial_state_is_disconnected(self) -> None:
        bus = RabbitMQEventBus()
        assert bus.is_connected is False
        assert bus.is_consuming is False
        assert bus._connection_manager._connection is None
        assert bus._connection_manager._channel is None
        assert bus._topology._exchange is None
        assert bus._topology._consumer_queue is None

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connection_state_after_connect_then_disconnect(
        self, mock_aio_pika: MagicMock
    ) -> None:
        _, mock_channel = _mock_conn_and_chan(mock_aio_pika)
        mock_channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        mock_channel.declare_queue = AsyncMock(return_value=AsyncMock())

        bus = RabbitMQEventBus()
        assert bus.is_connected is False

        await bus.connect()
        assert bus.is_connected is True
        assert bus._connection_manager._connection is not None
        assert bus._connection_manager._channel is not None

        await bus.disconnect()
        assert bus.is_connected is False
        assert bus._connection_manager._connection is None
        assert bus._connection_manager._channel is None
        assert bus._topology._exchange is None
