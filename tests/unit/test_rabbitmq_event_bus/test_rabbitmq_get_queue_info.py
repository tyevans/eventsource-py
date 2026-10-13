"""Unit tests extracted for test_rabbitmq_get_queue_info.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
)


class TestGetQueueInfo:
    """Tests for RabbitMQEventBus.get_queue_info() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @pytest.mark.asyncio
    async def test_queue_info_when_not_connected(self, config: RabbitMQEventBusConfig) -> None:
        """Test get_queue_info returns error state when not connected."""
        from eventsource.adapters.rabbitmq import QueueInfo, RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        info = await bus.get_queue_info()

        assert isinstance(info, QueueInfo)
        assert info.name == config.queue_name
        assert info.message_count == 0
        assert info.consumer_count == 0
        assert info.state == "error"
        assert info.error == "Not connected to RabbitMQ"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_queue_info_when_connected(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test get_queue_info returns queue statistics when connected."""
        from eventsource.adapters.rabbitmq import QueueInfo, RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue declaration result with bind as AsyncMock
        mock_queue = AsyncMock()
        mock_queue.declaration_result = MagicMock()
        mock_queue.declaration_result.message_count = 42
        mock_queue.declaration_result.consumer_count = 3
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        info = await bus.get_queue_info()

        assert isinstance(info, QueueInfo)
        assert info.name == config.queue_name
        assert info.message_count == 42
        assert info.consumer_count == 3
        assert info.state == "running"
        assert info.error is None

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_queue_info_idle_state_when_no_consumers(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test get_queue_info returns idle state when no consumers."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue with zero consumers (use AsyncMock for bind method)
        mock_queue = AsyncMock()
        mock_queue.declaration_result = MagicMock()
        mock_queue.declaration_result.message_count = 10
        mock_queue.declaration_result.consumer_count = 0
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        info = await bus.get_queue_info()

        assert info.state == "idle"
        assert info.consumer_count == 0
        assert info.message_count == 10

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_queue_info_handles_exception(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test get_queue_info handles exceptions gracefully."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue for connect (succeeds)
        mock_queue_for_connect = AsyncMock()
        mock_queue_for_connect.declaration_result = MagicMock()
        mock_queue_for_connect.declaration_result.message_count = 0
        mock_queue_for_connect.declaration_result.consumer_count = 0

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

        # First calls during connect succeed, then get_queue_info call fails
        call_count = 0

        async def declare_queue_side_effect(**kwargs: dict) -> AsyncMock:
            nonlocal call_count
            call_count += 1
            if kwargs.get("passive"):
                raise Exception("Queue not found")
            return mock_queue_for_connect

        mock_channel.declare_queue = AsyncMock(side_effect=declare_queue_side_effect)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        info = await bus.get_queue_info()

        assert info.state == "error"
        assert info.error == "Queue not found"
        assert info.message_count == 0
        assert info.consumer_count == 0

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_queue_info_uses_passive_declaration(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test get_queue_info uses passive queue declaration."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue
        mock_queue = AsyncMock()
        mock_queue.declaration_result = MagicMock()
        mock_queue.declaration_result.message_count = 5
        mock_queue.declaration_result.consumer_count = 1
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus.get_queue_info()

        # Verify passive=True was passed in the last call (for get_queue_info)
        last_call = mock_channel.declare_queue.call_args_list[-1]
        assert last_call.kwargs.get("passive") is True
        assert last_call.kwargs.get("name") == config.queue_name
