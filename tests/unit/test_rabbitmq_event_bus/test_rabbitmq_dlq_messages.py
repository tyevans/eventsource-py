"""Unit tests extracted for test_rabbitmq_dlq_messages.py."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestGetDLQMessages:
    """Tests for get_dlq_messages method."""

    @pytest.fixture
    def config_with_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration with DLQ enabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @pytest.fixture
    def config_without_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration with DLQ disabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @pytest.mark.asyncio
    async def test_returns_empty_when_not_connected(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test get_dlq_messages returns empty list when not connected."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        assert bus._connection_manager._connected is False

        result = await bus.get_dlq_messages(limit=10)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_dlq_disabled(
        self, config_without_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test get_dlq_messages returns empty list when DLQ disabled."""
        bus = RabbitMQEventBus(config=config_without_dlq)
        bus._connection_manager._connected = True

        result = await bus.get_dlq_messages(limit=10)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_channel_not_initialized(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test get_dlq_messages returns empty list when channel not initialized."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True
        bus._connection_manager._channel = None

        result = await bus.get_dlq_messages(limit=10)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_dlq_messages(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test get_dlq_messages returns list of DLQMessage objects."""
        from eventsource.adapters.rabbitmq import DLQMessage

        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True

        mock_channel = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.message_id = "test-msg-id"
        mock_message.routing_key = "Test.TestEvent"
        mock_message.body = b'{"data": "test"}'
        mock_message.headers = {
            "event_type": "TestEvent",
            "x-dlq-reason": "Handler error",
            "x-dlq-error-type": "ValueError",
            "x-dlq-retry-count": 3,
            "x-dlq-timestamp": "2024-01-15T10:00:00Z",
            "x-original-routing-key": "Test.TestEvent",
        }

        # First call returns message, second call returns None
        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue
        bus._connection_manager._channel = mock_channel

        result = await bus.get_dlq_messages(limit=10)

        assert len(result) == 1
        assert isinstance(result[0], DLQMessage)
        assert result[0].message_id == "test-msg-id"
        assert result[0].routing_key == "Test.TestEvent"
        assert result[0].body == '{"data": "test"}'
        assert result[0].event_type == "TestEvent"
        assert result[0].dlq_reason == "Handler error"
        assert result[0].dlq_error_type == "ValueError"
        assert result[0].dlq_retry_count == 3
        assert result[0].dlq_timestamp == "2024-01-15T10:00:00Z"
        assert result[0].original_routing_key == "Test.TestEvent"

    @pytest.mark.asyncio
    async def test_rejects_messages_with_requeue(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that messages are rejected with requeue to preserve them."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True

        mock_channel = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.message_id = "test-id"
        mock_message.routing_key = "Test.Event"
        mock_message.body = b"{}"
        mock_message.headers = {}

        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue
        bus._connection_manager._channel = mock_channel

        await bus.get_dlq_messages(limit=10)

        # Verify message was rejected with requeue=True
        mock_message.reject.assert_called_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_respects_limit_parameter(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test that limit parameter is respected."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True

        mock_channel = AsyncMock()
        mock_queue = AsyncMock()

        messages = []
        for i in range(5):
            msg = AsyncMock()
            msg.message_id = f"msg-{i}"
            msg.routing_key = "Test.Event"
            msg.body = b"{}"
            msg.headers = {}
            messages.append(msg)

        mock_queue.get.side_effect = messages + [None]
        mock_channel.get_queue.return_value = mock_queue
        bus._connection_manager._channel = mock_channel

        result = await bus.get_dlq_messages(limit=3)

        # Should only get 3 messages even though 5 are available
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_handles_exception(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test that exceptions are handled gracefully."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True

        mock_channel = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("Queue not found")
        bus._connection_manager._channel = mock_channel

        result = await bus.get_dlq_messages(limit=10)

        assert result == []

    @pytest.mark.asyncio
    async def test_handles_retry_count_as_string(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that string retry count is converted to int."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True

        mock_channel = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.message_id = "test-id"
        mock_message.routing_key = "Test.Event"
        mock_message.body = b"{}"
        mock_message.headers = {"x-dlq-retry-count": "5"}  # String instead of int

        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue
        bus._connection_manager._channel = mock_channel

        result = await bus.get_dlq_messages(limit=10)

        assert result[0].dlq_retry_count == 5
