"""Unit tests extracted for test_rabbitmq_message_replay.py."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestReplayDLQMessage:
    """Tests for replay_dlq_message method."""

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
    async def test_returns_false_when_not_connected(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test replay_dlq_message returns False when not connected."""
        bus = RabbitMQEventBus(config=config_with_dlq)

        result = await bus.replay_dlq_message("test-id")

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_exchange_not_initialized(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test replay_dlq_message returns False when exchange not initialized."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True
        bus._exchange = None

        result = await bus.replay_dlq_message("test-id")

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_dlq_disabled(
        self, config_without_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test replay_dlq_message returns False when DLQ disabled."""
        bus = RabbitMQEventBus(config=config_without_dlq)
        bus._connected = True
        bus._exchange = AsyncMock()
        bus._channel = AsyncMock()

        result = await bus.replay_dlq_message("test-id")

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_channel_not_initialized(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test replay_dlq_message returns False when channel not initialized."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True
        bus._exchange = AsyncMock()
        bus._channel = None

        result = await bus.replay_dlq_message("test-id")

        assert result is False

    @pytest.mark.asyncio
    async def test_replays_message_successfully(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test successful message replay."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.message_id = "target-id"
        mock_message.routing_key = "dlq-routing-key"
        mock_message.body = b'{"data": "test"}'
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.headers = {
            "event_type": "TestEvent",
            "x-dlq-reason": "Error",
            "x-dlq-error-type": "ValueError",
            "x-dlq-retry-count": 3,
            "x-dlq-timestamp": "2024-01-15T10:00:00Z",
            "x-original-routing-key": "Test.TestEvent",
        }

        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue
        bus._channel = mock_channel
        bus._exchange = mock_exchange

        result = await bus.replay_dlq_message("target-id")

        assert result is True
        mock_message.ack.assert_called_once()
        mock_exchange.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_false_when_message_not_found(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test replay_dlq_message returns False when message not found."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.message_id = "other-id"
        mock_message.routing_key = "key"
        mock_message.body = b"{}"
        mock_message.headers = {}

        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue
        bus._channel = mock_channel
        bus._exchange = mock_exchange

        result = await bus.replay_dlq_message("non-existent-id")

        assert result is False
        # The non-matching message should be rejected with requeue
        mock_message.reject.assert_called_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_handles_exception(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test that exceptions are handled gracefully."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("Queue error")
        bus._channel = mock_channel
        bus._exchange = mock_exchange

        result = await bus.replay_dlq_message("test-id")

        assert result is False
