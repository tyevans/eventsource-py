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
        return RabbitMQEventBusConfig(enable_dlq=True)

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def mock_message(self) -> MagicMock:
        return MagicMock(
            headers={
                "event_type": "TestEvent",
                "aggregate_type": "TestAggregate",
                "x-retry-count": 3,
            },
            body=b'{"test": "data"}',
            content_type="application/json",
            content_encoding="utf-8",
            message_id="test-message-id",
            routing_key="TestAggregate.TestEvent",
        )

    @pytest.mark.asyncio
    async def test_send_to_dlq_adds_failure_metadata(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        mock_dlq_exchange = AsyncMock()
        bus._topology._dlq_exchange = mock_dlq_exchange
        await bus._send_to_dlq(mock_message, ValueError("Test error"), 3)

        dlq_message = mock_dlq_exchange.publish.call_args[0][0]
        assert dlq_message.headers["x-dlq-reason"] == "Test error"
        assert dlq_message.headers["x-dlq-error-type"] == "ValueError"
        assert dlq_message.headers["x-dlq-retry-count"] == 3
        assert "x-dlq-timestamp" in dlq_message.headers
        assert dlq_message.headers["x-original-routing-key"] == "TestAggregate.TestEvent"

    @pytest.mark.asyncio
    async def test_send_to_dlq_preserves_original_headers(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        mock_dlq_exchange = AsyncMock()
        bus._topology._dlq_exchange = mock_dlq_exchange
        await bus._send_to_dlq(mock_message, ValueError("Test error"), 3)

        dlq_message = mock_dlq_exchange.publish.call_args[0][0]
        assert dlq_message.headers["event_type"] == "TestEvent"
        assert dlq_message.headers["aggregate_type"] == "TestAggregate"

    @pytest.mark.asyncio
    async def test_send_to_dlq_increments_stats(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        mock_dlq_exchange = AsyncMock()
        bus._topology._dlq_exchange = mock_dlq_exchange
        initial_count = bus.stats.messages_sent_to_dlq
        await bus._send_to_dlq(mock_message, ValueError("Test error"), 3)
        assert bus.stats.messages_sent_to_dlq == initial_count + 1

    @pytest.mark.asyncio
    async def test_send_to_dlq_uses_queue_name_as_routing_key(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        mock_dlq_exchange = AsyncMock()
        bus._topology._dlq_exchange = mock_dlq_exchange
        await bus._send_to_dlq(mock_message, ValueError("Test error"), 3)
        assert mock_dlq_exchange.publish.call_args[1]["routing_key"] == bus._config.queue_name

    @pytest.mark.asyncio
    async def test_send_to_dlq_does_nothing_when_disabled(self, mock_message: MagicMock) -> None:
        bus = RabbitMQEventBus(config=RabbitMQEventBusConfig(enable_dlq=False))
        mock_dlq_exchange = AsyncMock()
        bus._topology._dlq_exchange = mock_dlq_exchange
        initial_count = bus.stats.messages_sent_to_dlq
        await bus._send_to_dlq(mock_message, ValueError("Test error"), 3)
        mock_dlq_exchange.publish.assert_not_called()
        assert bus.stats.messages_sent_to_dlq == initial_count

    @pytest.mark.asyncio
    async def test_send_to_dlq_does_nothing_when_exchange_not_initialized(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        bus._topology._dlq_exchange = None
        initial_count = bus.stats.messages_sent_to_dlq
        await bus._send_to_dlq(mock_message, ValueError("Test error"), 3)
        assert bus.stats.messages_sent_to_dlq == initial_count


class TestPurgeDLQ:
    """Tests for purge_dlq method."""

    @pytest.fixture
    def config_with_dlq(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @pytest.fixture
    def config_without_dlq(self) -> RabbitMQEventBusConfig:
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
        bus = RabbitMQEventBus(config=config_with_dlq)
        assert bus._connection_manager._connected is False
        assert await bus.purge_dlq() == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_dlq_disabled(
        self, config_without_dlq: RabbitMQEventBusConfig
    ) -> None:
        bus = RabbitMQEventBus(config=config_without_dlq)
        bus._connection_manager._connected = True
        assert await bus.purge_dlq() == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_channel_not_initialized(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True
        bus._connection_manager._channel = None
        assert await bus.purge_dlq() == 0

    @pytest.mark.asyncio
    async def test_purges_dlq_successfully(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True
        mock_channel, mock_queue, mock_purge_result = AsyncMock(), AsyncMock(), AsyncMock()
        mock_purge_result.message_count = 15
        mock_queue.purge.return_value = mock_purge_result
        mock_channel.get_queue.return_value = mock_queue
        bus._connection_manager._channel = mock_channel

        assert await bus.purge_dlq() == 15
        mock_channel.get_queue.assert_called_once_with(config_with_dlq.dlq_queue_name)
        mock_queue.purge.assert_called_once()

    @pytest.mark.asyncio
    async def test_handles_exception(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True
        mock_channel = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("Queue error")
        bus._connection_manager._channel = mock_channel
        assert await bus.purge_dlq() == 0
