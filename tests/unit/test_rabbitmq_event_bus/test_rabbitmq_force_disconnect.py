"""Unit tests extracted for test_rabbitmq_force_disconnect.py."""

from __future__ import annotations

import asyncio
import logging
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
    ShutdownError,
)


class TestDrainInFlight:
    """Tests for _drain_in_flight helper method."""

    @pytest.fixture
    def bus(self) -> RabbitMQEventBus:
        return RabbitMQEventBus()

    @pytest.mark.asyncio
    async def test_drain_waits_for_portion_of_timeout(self, bus: RabbitMQEventBus) -> None:
        start = time.monotonic()
        await bus._drain_in_flight(timeout=4.0)
        elapsed = time.monotonic() - start
        assert 0.9 <= elapsed <= 1.5

    @pytest.mark.asyncio
    async def test_drain_caps_at_5_seconds(self, bus: RabbitMQEventBus) -> None:
        start = time.monotonic()
        await bus._drain_in_flight(timeout=100.0)
        elapsed = time.monotonic() - start
        assert 4.5 <= elapsed <= 5.5


class TestForceDisconnect:
    """Tests for _force_disconnect helper method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(rabbitmq_url="amqp://test:test@localhost/")

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_force_disconnect_clears_all_state(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        mock_connection = AsyncMock(is_closed=False)
        mock_channel = AsyncMock(is_closed=False)
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus._force_disconnect()

        assert bus._connection_manager._connection is None
        assert bus._connection_manager._channel is None
        assert bus._topology._exchange is None
        assert bus._topology._dlq_exchange is None
        assert bus._topology._consumer_queue is None
        assert bus._topology._dlq_queue is None
        assert bus._connection_manager._connected is False
        assert bus._consumer._consuming is False
        assert bus._shutdown_initiated is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_force_disconnect_cancels_consumer_task(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        mock_connection = AsyncMock(is_closed=False)
        mock_channel = AsyncMock(is_closed=False)
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        async def long_running() -> None:
            await asyncio.sleep(100)

        bus._consumer._consumer_task = asyncio.create_task(long_running())
        await bus._force_disconnect()
        assert bus._consumer._consumer_task is None

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_force_disconnect_handles_close_errors(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        mock_connection = AsyncMock(
            is_closed=False, close=AsyncMock(side_effect=Exception("Connection error"))
        )
        mock_channel = AsyncMock(
            is_closed=False, close=AsyncMock(side_effect=Exception("Channel error"))
        )
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus._force_disconnect()

        assert bus._connection_manager._connected is False
        assert bus._connection_manager._channel is None
        assert bus._connection_manager._connection is None

    @pytest.mark.asyncio
    async def test_force_disconnect_when_not_connected(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        bus = RabbitMQEventBus(config=config)
        await bus._force_disconnect()
        assert bus._shutdown_initiated is True


class TestShutdownError:
    """Tests for ShutdownError exception."""

    def test_shutdown_error_default_message(self) -> None:
        assert str(ShutdownError()) == "Event bus has been shut down"

    def test_shutdown_error_custom_message(self) -> None:
        assert str(ShutdownError("Custom shutdown message")) == "Custom shutdown message"

    def test_shutdown_error_is_exception(self) -> None:
        assert isinstance(ShutdownError(), Exception)

    def test_shutdown_error_can_be_raised_and_caught(self) -> None:
        with pytest.raises(ShutdownError) as exc_info:
            raise ShutdownError("Test shutdown")
        assert "Test shutdown" in str(exc_info.value)


class TestShutdownLogging:
    """Tests for shutdown logging."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(rabbitmq_url="amqp://test:test@localhost/")

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_logs_initiation(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        mock_connection = AsyncMock(is_closed=False)
        mock_channel = AsyncMock(is_closed=False)
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        with caplog.at_level(logging.INFO):
            await bus.shutdown(timeout=5.0)
        assert "Initiating graceful shutdown" in caplog.text

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_logs_completion(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        mock_connection = AsyncMock(is_closed=False)
        mock_channel = AsyncMock(is_closed=False)
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        with caplog.at_level(logging.INFO):
            await bus.shutdown(timeout=5.0)
        assert "Graceful shutdown completed" in caplog.text


class TestShutdownConfigDocumentation:
    """Tests for shutdown_timeout config documentation."""

    def test_config_has_shutdown_timeout(self) -> None:
        assert hasattr(RabbitMQEventBusConfig(), "shutdown_timeout")

    def test_config_shutdown_timeout_type(self) -> None:
        assert isinstance(RabbitMQEventBusConfig().shutdown_timeout, float)

    def test_config_shutdown_timeout_zero(self) -> None:
        assert RabbitMQEventBusConfig(shutdown_timeout=0.0).shutdown_timeout == 0.0

    def test_config_shutdown_timeout_large_value(self) -> None:
        assert RabbitMQEventBusConfig(shutdown_timeout=3600.0).shutdown_timeout == 3600.0
