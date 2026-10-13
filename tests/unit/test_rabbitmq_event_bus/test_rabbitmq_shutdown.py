"""Unit tests extracted for test_rabbitmq_shutdown.py."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestGracefulShutdown:
    """Tests for graceful shutdown functionality (P2-005)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            shutdown_timeout=30.0,
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create a test event bus instance."""
        return RabbitMQEventBus(config=config)

    def test_shutdown_timeout_config_default(self) -> None:
        """Test that shutdown_timeout has a default value."""
        config = RabbitMQEventBusConfig()
        assert config.shutdown_timeout == 30.0

    def test_shutdown_timeout_config_custom(self) -> None:
        """Test custom shutdown_timeout configuration."""
        config = RabbitMQEventBusConfig(shutdown_timeout=60.0)
        assert config.shutdown_timeout == 60.0

    def test_is_shutdown_initial_state(self, bus: RabbitMQEventBus) -> None:
        """Test that is_shutdown is False initially."""
        assert bus.is_shutdown is False

    def test_shutdown_initiated_flag_initial(self, bus: RabbitMQEventBus) -> None:
        """Test that _shutdown_initiated is False initially."""
        assert bus._shutdown_initiated is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_sets_shutdown_flag(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that shutdown sets the shutdown flag."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus.shutdown(timeout=5.0)

        assert bus.is_shutdown is True
        assert bus._shutdown_initiated is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_stops_consuming(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that shutdown stops consuming."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        bus._consuming = True

        await bus.shutdown(timeout=5.0)

        assert bus._consuming is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_disconnects(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that shutdown calls disconnect."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus.shutdown(timeout=5.0)

        assert bus.is_connected is False
        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_is_idempotent(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that calling shutdown multiple times is safe."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # First shutdown
        await bus.shutdown(timeout=5.0)
        assert bus.is_shutdown is True

        # Second shutdown should not raise
        await bus.shutdown(timeout=5.0)
        assert bus.is_shutdown is True

        # Channel close should only be called once (during first shutdown)
        mock_channel.close.assert_called_once()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_cancels_consumer_task(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that shutdown cancels the consumer task."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Create a mock consumer task
        async def long_running() -> None:
            await asyncio.sleep(100)

        bus._consumer_task = asyncio.create_task(long_running())
        bus._consuming = True

        await bus.shutdown(timeout=2.0)

        assert bus._consumer_task is None

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_when_not_connected(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that shutdown works when not connected."""
        bus = RabbitMQEventBus(config=config)

        # Should not raise
        await bus.shutdown(timeout=5.0)

        assert bus.is_shutdown is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_when_not_consuming(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that shutdown works when not consuming."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        assert bus._consuming is False

        await bus.shutdown(timeout=5.0)

        assert bus.is_shutdown is True
        assert bus.is_connected is False


class TestGracefulShutdownContextManager:
    """Tests for context manager graceful shutdown (P2-005)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
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
        """Test that context manager calls shutdown instead of disconnect."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        async with RabbitMQEventBus(config=config) as bus:
            assert bus.is_connected is True

        assert bus.is_shutdown is True
        assert bus.is_connected is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_uses_config_timeout(self, mock_aio_pika: MagicMock) -> None:
        """Test that context manager uses shutdown_timeout from config."""
        config = RabbitMQEventBusConfig(shutdown_timeout=10.0)

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        async with RabbitMQEventBus(config=config) as bus:
            pass

        assert bus.is_shutdown is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_shutdown_on_exception(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that context manager performs shutdown even on exception."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(ValueError, match="Test exception"):
            async with bus:
                raise ValueError("Test exception")

        assert bus.is_shutdown is True
        assert bus.is_connected is False


class TestStopConsumingGracefully:
    """Tests for _stop_consuming_gracefully helper method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create a test event bus instance."""
        return RabbitMQEventBus(config=config)

    @pytest.mark.asyncio
    async def test_stop_consuming_when_not_consuming(self, bus: RabbitMQEventBus) -> None:
        """Test _stop_consuming_gracefully when not consuming does nothing."""
        bus._consuming = False

        await bus._stop_consuming_gracefully(timeout=5.0)

        # Should complete without error

    @pytest.mark.asyncio
    async def test_stop_consuming_sets_flag(self, bus: RabbitMQEventBus) -> None:
        """Test that _stop_consuming_gracefully sets consuming flag to False."""
        bus._consuming = True

        await bus._stop_consuming_gracefully(timeout=5.0)

        assert bus._consuming is False

    @pytest.mark.asyncio
    async def test_stop_consuming_cancels_slow_task(self, bus: RabbitMQEventBus) -> None:
        """Test that slow consumer task is cancelled if it doesn't stop in time."""
        bus._consuming = True

        # Create a task that won't respond to _consuming flag
        async def slow_consumer() -> None:
            await asyncio.sleep(100)

        bus._consumer_task = asyncio.create_task(slow_consumer())

        await bus._stop_consuming_gracefully(timeout=0.1)

        assert bus._consumer_task is None

    @pytest.mark.asyncio
    async def test_stop_consuming_waits_for_graceful_stop(self, bus: RabbitMQEventBus) -> None:
        """Test that _stop_consuming_gracefully waits for task to complete."""
        bus._consuming = True
        completed = False

        async def quick_consumer() -> None:
            nonlocal completed
            # Simulate quick completion
            await asyncio.sleep(0.01)
            completed = True

        bus._consumer_task = asyncio.create_task(quick_consumer())

        await bus._stop_consuming_gracefully(timeout=5.0)

        # Allow task to complete
        await asyncio.sleep(0.02)
        assert bus._consumer_task is None
