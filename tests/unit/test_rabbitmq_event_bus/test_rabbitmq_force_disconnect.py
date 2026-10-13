"""Unit tests extracted for test_rabbitmq_force_disconnect.py."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestDrainInFlight:
    """Tests for _drain_in_flight helper method."""

    @pytest.fixture
    def bus(self) -> RabbitMQEventBus:
        """Create a test event bus instance."""
        return RabbitMQEventBus()

    @pytest.mark.asyncio
    async def test_drain_waits_for_portion_of_timeout(self, bus: RabbitMQEventBus) -> None:
        """Test that _drain_in_flight waits for a portion of the timeout."""
        import time

        start = time.monotonic()
        await bus._drain_in_flight(timeout=4.0)  # Should wait 1.0s (4.0/4)
        elapsed = time.monotonic() - start

        # Should wait roughly 1.0 seconds (timeout / 4)
        assert 0.9 <= elapsed <= 1.5

    @pytest.mark.asyncio
    async def test_drain_caps_at_5_seconds(self, bus: RabbitMQEventBus) -> None:
        """Test that drain time is capped at 5 seconds."""
        import time

        start = time.monotonic()
        await bus._drain_in_flight(timeout=100.0)  # Would be 25s without cap
        elapsed = time.monotonic() - start

        # Should wait at most 5 seconds
        assert 4.5 <= elapsed <= 5.5


class TestForceDisconnect:
    """Tests for _force_disconnect helper method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_force_disconnect_clears_all_state(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _force_disconnect clears all connection state."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus._force_disconnect()

        assert bus._connection is None
        assert bus._channel is None
        assert bus._exchange is None
        assert bus._dlq_exchange is None
        assert bus._consumer_queue is None
        assert bus._dlq_queue is None
        assert bus._connected is False
        assert bus._consuming is False
        assert bus._shutdown_initiated is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_force_disconnect_cancels_consumer_task(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _force_disconnect cancels consumer task immediately."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        async def long_running() -> None:
            await asyncio.sleep(100)

        bus._consumer_task = asyncio.create_task(long_running())

        await bus._force_disconnect()

        assert bus._consumer_task is None

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_force_disconnect_handles_close_errors(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _force_disconnect handles errors during close gracefully."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.close = AsyncMock(side_effect=Exception("Connection error"))
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close = AsyncMock(side_effect=Exception("Channel error"))
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Should not raise even with errors
        await bus._force_disconnect()

        assert bus._connected is False
        assert bus._channel is None
        assert bus._connection is None

    @pytest.mark.asyncio
    async def test_force_disconnect_when_not_connected(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _force_disconnect works when not connected."""
        bus = RabbitMQEventBus(config=config)

        # Should not raise
        await bus._force_disconnect()

        assert bus._shutdown_initiated is True


class TestShutdownError:
    """Tests for ShutdownError exception."""

    def test_shutdown_error_default_message(self) -> None:
        """Test that ShutdownError has a default message."""
        from eventsource.adapters.rabbitmq import ShutdownError

        error = ShutdownError()
        assert str(error) == "Event bus has been shut down"

    def test_shutdown_error_custom_message(self) -> None:
        """Test that ShutdownError accepts custom message."""
        from eventsource.adapters.rabbitmq import ShutdownError

        error = ShutdownError("Custom shutdown message")
        assert str(error) == "Custom shutdown message"

    def test_shutdown_error_is_exception(self) -> None:
        """Test that ShutdownError is an Exception subclass."""
        from eventsource.adapters.rabbitmq import ShutdownError

        error = ShutdownError()
        assert isinstance(error, Exception)

    def test_shutdown_error_can_be_raised_and_caught(self) -> None:
        """Test that ShutdownError can be raised and caught."""
        from eventsource.adapters.rabbitmq import ShutdownError

        with pytest.raises(ShutdownError) as exc_info:
            raise ShutdownError("Test shutdown")

        assert "Test shutdown" in str(exc_info.value)


class TestShutdownLogging:
    """Tests for shutdown logging."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_logs_initiation(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that shutdown logs initiation."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
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
        """Test that shutdown logs completion with duration."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
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
        """Test that RabbitMQEventBusConfig has shutdown_timeout attribute."""
        config = RabbitMQEventBusConfig()
        assert hasattr(config, "shutdown_timeout")

    def test_config_shutdown_timeout_type(self) -> None:
        """Test that shutdown_timeout is a float."""
        config = RabbitMQEventBusConfig()
        assert isinstance(config.shutdown_timeout, float)

    def test_config_shutdown_timeout_zero(self) -> None:
        """Test that shutdown_timeout can be set to zero."""
        config = RabbitMQEventBusConfig(shutdown_timeout=0.0)
        assert config.shutdown_timeout == 0.0

    def test_config_shutdown_timeout_large_value(self) -> None:
        """Test that shutdown_timeout can be set to a large value."""
        config = RabbitMQEventBusConfig(shutdown_timeout=3600.0)
        assert config.shutdown_timeout == 3600.0
