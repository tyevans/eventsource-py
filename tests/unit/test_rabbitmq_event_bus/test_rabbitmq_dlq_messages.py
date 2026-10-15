"""Unit tests extracted for test_rabbitmq_dlq_messages.py."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from eventsource.adapters.rabbitmq import DLQMessage, RabbitMQEventBus, RabbitMQEventBusConfig


class TestGetDLQMessages:
    """Tests for get_dlq_messages method."""

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

    def _setup_bus_with_messages(
        self, config: RabbitMQEventBusConfig, messages: list[AsyncMock]
    ) -> RabbitMQEventBus:
        bus = RabbitMQEventBus(config=config)
        bus._connection_manager._connected = True
        mock_channel, mock_queue = AsyncMock(), AsyncMock()
        mock_queue.get.side_effect = [*messages, None]
        mock_channel.get_queue.return_value = mock_queue
        bus._connection_manager._channel = mock_channel
        return bus

    @pytest.mark.asyncio
    async def test_returns_empty_when_not_connected(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        bus = RabbitMQEventBus(config=config_with_dlq)
        assert bus._connection_manager._connected is False
        assert await bus.get_dlq_messages(limit=10) == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_dlq_disabled(
        self, config_without_dlq: RabbitMQEventBusConfig
    ) -> None:
        bus = RabbitMQEventBus(config=config_without_dlq)
        bus._connection_manager._connected = True
        assert await bus.get_dlq_messages(limit=10) == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_channel_not_initialized(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True
        bus._connection_manager._channel = None
        assert await bus.get_dlq_messages(limit=10) == []

    @pytest.mark.asyncio
    async def test_returns_dlq_messages(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        mock_message = AsyncMock(
            message_id="test-msg-id",
            routing_key="Test.TestEvent",
            body=b'{"data": "test"}',
            headers={
                "event_type": "TestEvent",
                "x-dlq-reason": "Handler error",
                "x-dlq-error-type": "ValueError",
                "x-dlq-retry-count": 3,
                "x-dlq-timestamp": "2024-01-15T10:00:00Z",
                "x-original-routing-key": "Test.TestEvent",
            },
        )
        bus = self._setup_bus_with_messages(config_with_dlq, [mock_message])
        result = await bus.get_dlq_messages(limit=10)

        assert len(result) == 1 and isinstance(result[0], DLQMessage)
        msg = result[0]
        assert (msg.message_id, msg.routing_key) == ("test-msg-id", "Test.TestEvent")
        assert (msg.body, msg.event_type) == ('{"data": "test"}', "TestEvent")
        assert (msg.dlq_reason, msg.dlq_error_type, msg.dlq_retry_count) == (
            "Handler error",
            "ValueError",
            3,
        )
        assert (msg.dlq_timestamp, msg.original_routing_key) == (
            "2024-01-15T10:00:00Z",
            "Test.TestEvent",
        )

    @pytest.mark.asyncio
    async def test_rejects_messages_with_requeue(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        mock_message = AsyncMock(
            message_id="test-id", routing_key="Test.Event", body=b"{}", headers={}
        )
        bus = self._setup_bus_with_messages(config_with_dlq, [mock_message])
        await bus.get_dlq_messages(limit=10)
        mock_message.reject.assert_called_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_respects_limit_parameter(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        messages = [
            AsyncMock(message_id=f"msg-{i}", routing_key="Test.Event", body=b"{}", headers={})
            for i in range(5)
        ]
        bus = self._setup_bus_with_messages(config_with_dlq, messages)
        result = await bus.get_dlq_messages(limit=3)
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_handles_exception(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True
        mock_channel = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("Queue not found")
        bus._connection_manager._channel = mock_channel
        assert await bus.get_dlq_messages(limit=10) == []

    @pytest.mark.asyncio
    async def test_handles_retry_count_as_string(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        mock_message = AsyncMock(
            message_id="test-id",
            routing_key="Test.Event",
            body=b"{}",
            headers={"x-dlq-retry-count": "5"},
        )
        bus = self._setup_bus_with_messages(config_with_dlq, [mock_message])
        result = await bus.get_dlq_messages(limit=10)
        assert result[0].dlq_retry_count == 5
