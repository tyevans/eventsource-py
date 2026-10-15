"""Unit tests extracted for test_rabbitmq_dlq_operations.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestSendToDLQ:
    """Tests for the _send_to_dlq method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(enable_dlq=True)

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance."""
        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def mock_message(self) -> MagicMock:
        """Create mock incoming message."""
        message = MagicMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-retry-count": 3,
        }
        message.body = b'{"test": "data"}'
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        message.message_id = "test-message-id"
        message.routing_key = "TestAggregate.TestEvent"
        return message

    @pytest.mark.asyncio
    async def test_send_to_dlq_adds_failure_metadata(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that failure metadata is added to DLQ message."""
        mock_dlq_exchange = AsyncMock()
        bus._topology._dlq_exchange = mock_dlq_exchange

        error = ValueError("Test error")
        await bus._send_to_dlq(mock_message, error, 3)

        call_args = mock_dlq_exchange.publish.call_args
        dlq_message = call_args[0][0]

        assert dlq_message.headers["x-dlq-reason"] == "Test error"
        assert dlq_message.headers["x-dlq-error-type"] == "ValueError"
        assert dlq_message.headers["x-dlq-retry-count"] == 3
        assert "x-dlq-timestamp" in dlq_message.headers
        assert dlq_message.headers["x-original-routing-key"] == "TestAggregate.TestEvent"

    @pytest.mark.asyncio
    async def test_send_to_dlq_preserves_original_headers(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that original headers are preserved in DLQ message."""
        mock_dlq_exchange = AsyncMock()
        bus._topology._dlq_exchange = mock_dlq_exchange

        error = ValueError("Test error")
        await bus._send_to_dlq(mock_message, error, 3)

        call_args = mock_dlq_exchange.publish.call_args
        dlq_message = call_args[0][0]

        assert dlq_message.headers["event_type"] == "TestEvent"
        assert dlq_message.headers["aggregate_type"] == "TestAggregate"

    @pytest.mark.asyncio
    async def test_send_to_dlq_increments_stats(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that messages_sent_to_dlq stat is incremented."""
        mock_dlq_exchange = AsyncMock()
        bus._topology._dlq_exchange = mock_dlq_exchange

        initial_count = bus.stats.messages_sent_to_dlq

        error = ValueError("Test error")
        await bus._send_to_dlq(mock_message, error, 3)

        assert bus.stats.messages_sent_to_dlq == initial_count + 1

    @pytest.mark.asyncio
    async def test_send_to_dlq_uses_queue_name_as_routing_key(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that queue name is used as routing key for DLQ."""
        mock_dlq_exchange = AsyncMock()
        bus._topology._dlq_exchange = mock_dlq_exchange

        error = ValueError("Test error")
        await bus._send_to_dlq(mock_message, error, 3)

        call_args = mock_dlq_exchange.publish.call_args
        routing_key = call_args[1]["routing_key"]

        assert routing_key == bus._config.queue_name

    @pytest.mark.asyncio
    async def test_send_to_dlq_does_nothing_when_disabled(self, mock_message: MagicMock) -> None:
        """Test that nothing happens when DLQ is disabled."""
        config = RabbitMQEventBusConfig(enable_dlq=False)
        bus = RabbitMQEventBus(config=config)

        mock_dlq_exchange = AsyncMock()
        bus._topology._dlq_exchange = mock_dlq_exchange

        initial_count = bus.stats.messages_sent_to_dlq

        error = ValueError("Test error")
        await bus._send_to_dlq(mock_message, error, 3)

        # Should not publish or increment stats
        mock_dlq_exchange.publish.assert_not_called()
        assert bus.stats.messages_sent_to_dlq == initial_count

    @pytest.mark.asyncio
    async def test_send_to_dlq_does_nothing_when_exchange_not_initialized(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that nothing happens when DLQ exchange not initialized."""
        bus._topology._dlq_exchange = None

        initial_count = bus.stats.messages_sent_to_dlq

        error = ValueError("Test error")
        await bus._send_to_dlq(mock_message, error, 3)

        assert bus.stats.messages_sent_to_dlq == initial_count


class TestPurgeDLQ:
    """Tests for purge_dlq method."""

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
    async def test_returns_zero_when_not_connected(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test purge_dlq returns 0 when not connected."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        assert bus._connection_manager._connected is False

        result = await bus.purge_dlq()

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_dlq_disabled(
        self, config_without_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test purge_dlq returns 0 when DLQ disabled."""
        bus = RabbitMQEventBus(config=config_without_dlq)
        bus._connection_manager._connected = True

        result = await bus.purge_dlq()

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_channel_not_initialized(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test purge_dlq returns 0 when channel not initialized."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True
        bus._connection_manager._channel = None

        result = await bus.purge_dlq()

        assert result == 0

    @pytest.mark.asyncio
    async def test_purges_dlq_successfully(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test successful DLQ purge."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True

        mock_channel = AsyncMock()
        mock_queue = AsyncMock()
        # purge() returns PurgeOk which has message_count attribute
        mock_purge_result = AsyncMock()
        mock_purge_result.message_count = 15
        mock_queue.purge.return_value = mock_purge_result
        mock_channel.get_queue.return_value = mock_queue
        bus._connection_manager._channel = mock_channel

        result = await bus.purge_dlq()

        assert result == 15
        mock_channel.get_queue.assert_called_once_with(config_with_dlq.dlq_queue_name)
        mock_queue.purge.assert_called_once()

    @pytest.mark.asyncio
    async def test_handles_exception(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test that exceptions are handled gracefully."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True

        mock_channel = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("Queue error")
        bus._connection_manager._channel = mock_channel

        result = await bus.purge_dlq()

        assert result == 0
