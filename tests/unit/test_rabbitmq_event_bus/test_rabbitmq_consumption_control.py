"""Unit tests extracted for test_rabbitmq_consumption_control.py."""

from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
)


class TestRabbitMQStartConsuming:
    """Tests for start_consuming() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            consumer_name="test-consumer",
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_start_consuming_auto_connects(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that start_consuming auto-connects if not connected."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        # Create a mock async iterator that yields no messages
        async def empty_iterator():
            if False:  # Never yields
                yield

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(return_value=empty_iterator())
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        # Start consuming - should auto-connect
        task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(0.1)
        await bus.stop_consuming()
        await asyncio.sleep(0.1)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        mock_aio_pika.connect_robust.assert_called_once()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_start_consuming_raises_if_queue_not_initialized(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that start_consuming raises RuntimeError if queue not initialized."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)
        bus._connection_manager._connected = True  # Pretend connected but no queue

        with pytest.raises(RuntimeError, match="Consumer queue not initialized"):
            await bus.start_consuming()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_start_consuming_sets_consuming_flag(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that start_consuming sets is_consuming to True."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        consuming_flag_during_loop = []

        async def async_iterator():
            consuming_flag_during_loop.append(bus.is_consuming)
            if False:
                yield

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(return_value=async_iterator())
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(0.1)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        # During the loop, consuming should have been True
        if consuming_flag_during_loop:
            assert consuming_flag_during_loop[0] is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_start_consuming_handles_cancellation(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that start_consuming handles CancelledError gracefully."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        async def infinite_iterator():
            while True:
                await asyncio.sleep(0.1)
                yield MagicMock()

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(return_value=infinite_iterator())
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(0.05)
        task.cancel()

        # The consumer loop catches CancelledError and handles it gracefully
        # It should not propagate the exception
        with contextlib.suppress(asyncio.CancelledError):
            await task

        # Consuming flag should be reset in any case
        assert bus.is_consuming is False


class TestRabbitMQStopConsuming:
    """Tests for stop_consuming() method."""

    @pytest.mark.asyncio
    async def test_stop_consuming_sets_flag(self) -> None:
        """Test that stop_consuming sets consuming flag to False."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        bus._consumer._consuming = True

        await bus.stop_consuming()

        assert bus._consumer._consuming is False

    @pytest.mark.asyncio
    async def test_stop_consuming_when_already_stopped(self) -> None:
        """Test that stop_consuming works even when not consuming."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        assert bus._consumer._consuming is False

        await bus.stop_consuming()

        assert bus._consumer._consuming is False


class TestRabbitMQStartConsumingInBackground:
    """Tests for start_consuming_in_background() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            consumer_name="bg-consumer",
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_start_consuming_in_background_returns_task(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that start_consuming_in_background returns an asyncio Task."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        async def empty_iterator():
            if False:
                yield

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(return_value=empty_iterator())
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        task = bus.start_consuming_in_background()

        assert isinstance(task, asyncio.Task)
        assert task.get_name() == f"rabbitmq-consumer-{config.consumer_name}"

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_start_consuming_in_background_raises_if_already_running(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that start_consuming_in_background raises if already running."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        async def empty_iterator():
            if False:
                yield

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(return_value=empty_iterator())
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        task1 = bus.start_consuming_in_background()
        await asyncio.sleep(0.05)

        with pytest.raises(RuntimeError, match="Consumer already running"):
            bus.start_consuming_in_background()

        task1.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task1
