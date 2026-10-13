"""Unit tests extracted for test_rabbitmq_topology_errors.py."""

from __future__ import annotations

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
)


class TestRabbitMQDeclarationErrors:
    """Tests for error handling during declaration."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @pytest.mark.asyncio
    async def test_declare_exchange_without_channel_raises(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _declare_exchange raises if not connected."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(RuntimeError, match="Not connected to RabbitMQ"):
            await bus._declare_exchange()

    @pytest.mark.asyncio
    async def test_declare_queue_without_channel_raises(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _declare_queue raises if not connected."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(RuntimeError, match="Not connected to RabbitMQ"):
            await bus._declare_queue()

    @pytest.mark.asyncio
    async def test_bind_queue_without_queue_raises(self, config: RabbitMQEventBusConfig) -> None:
        """Test that _bind_queue raises if queue not initialized."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(RuntimeError, match="internal call-order bug"):
            await bus._bind_queue()

    @pytest.mark.asyncio
    async def test_declare_dlq_without_channel_raises(self, config: RabbitMQEventBusConfig) -> None:
        """Test that _declare_dlq raises if not connected."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(RuntimeError, match="Not connected to RabbitMQ"):
            await bus._declare_dlq()
