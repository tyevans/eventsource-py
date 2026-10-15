"""Unit tests extracted for test_rabbitmq_dlq_count.py."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestGetDLQMessageCount:
    """Tests for get_dlq_message_count method."""

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
        """Test get_dlq_message_count returns 0 when not connected."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        assert bus._connection_manager._connected is False

        result = await bus.get_dlq_message_count()

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_dlq_disabled(
        self, config_without_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test get_dlq_message_count returns 0 when DLQ disabled."""
        bus = RabbitMQEventBus(config=config_without_dlq)
        bus._connection_manager._connected = True

        result = await bus.get_dlq_message_count()

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_channel_not_initialized(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test get_dlq_message_count returns 0 when channel not initialized."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True
        bus._connection_manager._channel = None

        result = await bus.get_dlq_message_count()

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_message_count(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test get_dlq_message_count returns correct count."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True

        mock_channel = AsyncMock()
        mock_queue_info = AsyncMock()
        mock_queue_info.declaration_result.message_count = 42
        mock_channel.declare_queue.return_value = mock_queue_info
        bus._connection_manager._channel = mock_channel

        result = await bus.get_dlq_message_count()

        assert result == 42
        mock_channel.declare_queue.assert_called_once_with(
            name=config_with_dlq.dlq_queue_name,
            passive=True,
        )

    @pytest.mark.asyncio
    async def test_handles_exception(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test that exceptions are handled gracefully."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connection_manager._connected = True

        mock_channel = AsyncMock()
        mock_channel.declare_queue.side_effect = Exception("Queue not found")
        bus._connection_manager._channel = mock_channel

        result = await bus.get_dlq_message_count()

        assert result == 0
