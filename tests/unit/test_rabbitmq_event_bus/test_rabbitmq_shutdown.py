"""Unit tests for RabbitMQEventBus graceful shutdown."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig


def _mock_broker(mock_aio_pika: MagicMock) -> tuple[AsyncMock, AsyncMock]:
    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    return mock_connection, mock_channel


class TestGracefulShutdown:
    """Tests for graceful shutdown functionality (P2-005)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            shutdown_timeout=30.0,
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        return RabbitMQEventBus(config=config)

    def test_shutdown_timeout_config_default(self) -> None:
        assert RabbitMQEventBusConfig().shutdown_timeout == 30.0

    def test_shutdown_timeout_config_custom(self) -> None:
        assert RabbitMQEventBusConfig(shutdown_timeout=60.0).shutdown_timeout == 60.0

    def test_is_shutdown_initial_state(self, bus: RabbitMQEventBus) -> None:
        assert bus.is_shutdown is False

    def test_shutdown_initiated_flag_initial(self, bus: RabbitMQEventBus) -> None:
        assert bus._shutdown_initiated is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_sets_shutdown_flag(
        self, mock_aio_pika: MagicMock, bus: RabbitMQEventBus
    ) -> None:
        _mock_broker(mock_aio_pika)
        await bus.connect()
        await bus.shutdown(timeout=5.0)
        assert bus.is_shutdown is True
        assert bus._shutdown_initiated is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_stops_consuming(
        self, mock_aio_pika: MagicMock, bus: RabbitMQEventBus
    ) -> None:
        _mock_broker(mock_aio_pika)
        await bus.connect()
        bus._consumer._consuming = True
        await bus.shutdown(timeout=5.0)
        assert bus._consumer._consuming is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_disconnects(
        self, mock_aio_pika: MagicMock, bus: RabbitMQEventBus
    ) -> None:
        mock_connection, mock_channel = _mock_broker(mock_aio_pika)
        await bus.connect()
        await bus.shutdown(timeout=5.0)
        assert bus.is_connected is False
        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_is_idempotent(
        self, mock_aio_pika: MagicMock, bus: RabbitMQEventBus
    ) -> None:
        _, mock_channel = _mock_broker(mock_aio_pika)
        await bus.connect()
        await bus.shutdown(timeout=5.0)
        assert bus.is_shutdown is True
        await bus.shutdown(timeout=5.0)
        assert bus.is_shutdown is True
        mock_channel.close.assert_called_once()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_cancels_consumer_task(
        self, mock_aio_pika: MagicMock, bus: RabbitMQEventBus
    ) -> None:
        _mock_broker(mock_aio_pika)
        await bus.connect()
        bus._consumer._consumer_task = asyncio.create_task(asyncio.sleep(100))
        bus._consumer._consuming = True
        await bus.shutdown(timeout=2.0)
        assert bus._consumer._consumer_task is None

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_when_not_connected(
        self, mock_aio_pika: MagicMock, bus: RabbitMQEventBus
    ) -> None:
        await bus.shutdown(timeout=5.0)
        assert bus.is_shutdown is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_when_not_consuming(
        self, mock_aio_pika: MagicMock, bus: RabbitMQEventBus
    ) -> None:
        _mock_broker(mock_aio_pika)
        await bus.connect()
        assert bus._consumer._consuming is False
        await bus.shutdown(timeout=5.0)
        assert bus.is_shutdown is True
        assert bus.is_connected is False


class TestGracefulShutdownContextManager:
    """Tests for context manager graceful shutdown (P2-005)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            shutdown_timeout=5.0,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_uses_shutdown(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        _mock_broker(mock_aio_pika)
        async with RabbitMQEventBus(config=config) as bus:
            assert bus.is_connected is True
        assert bus.is_shutdown is True
        assert bus.is_connected is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_uses_config_timeout(self, mock_aio_pika: MagicMock) -> None:
        _mock_broker(mock_aio_pika)
        async with RabbitMQEventBus(config=RabbitMQEventBusConfig(shutdown_timeout=10.0)) as bus:
            pass
        assert bus.is_shutdown is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_shutdown_on_exception(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        _mock_broker(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        with pytest.raises(ValueError, match="Test exception"):
            async with bus:
                raise ValueError("Test exception")
        assert bus.is_shutdown is True
        assert bus.is_connected is False


class TestStopConsumingGracefully:
    """Tests for _stop_consuming_gracefully helper method."""

    @pytest.fixture
    def bus(self) -> RabbitMQEventBus:
        return RabbitMQEventBus(
            config=RabbitMQEventBusConfig(rabbitmq_url="amqp://test:test@localhost/")
        )

    @pytest.mark.asyncio
    async def test_stop_consuming_when_not_consuming(self, bus: RabbitMQEventBus) -> None:
        bus._consumer._consuming = False
        await bus._stop_consuming_gracefully(timeout=5.0)

    @pytest.mark.asyncio
    async def test_stop_consuming_sets_flag(self, bus: RabbitMQEventBus) -> None:
        bus._consumer._consuming = True
        await bus._stop_consuming_gracefully(timeout=5.0)
        assert bus._consumer._consuming is False

    @pytest.mark.asyncio
    async def test_stop_consuming_cancels_slow_task(self, bus: RabbitMQEventBus) -> None:
        bus._consumer._consuming = True
        bus._consumer._consumer_task = asyncio.create_task(asyncio.sleep(100))
        await bus._stop_consuming_gracefully(timeout=0.1)
        assert bus._consumer._consumer_task is None

    @pytest.mark.asyncio
    async def test_stop_consuming_waits_for_graceful_stop(self, bus: RabbitMQEventBus) -> None:
        bus._consumer._consuming = True
        bus._consumer._consumer_task = asyncio.create_task(asyncio.sleep(0.01))
        await bus._stop_consuming_gracefully(timeout=5.0)
        await asyncio.sleep(0.02)
        assert bus._consumer._consumer_task is None
