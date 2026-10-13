"""Unit tests extracted for test_rabbitmq_exchanges_direct_patterns.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from eventsource.domain.event import DomainEvent


class TestDirectExchangeWorkQueuePattern:
    """Tests for work queue pattern with direct exchange.

    Work queue pattern: Multiple consumers share the same queue and compete
    for messages. RabbitMQ load-balances messages between consumers.
    """

    @pytest.fixture
    def work_queue_config(self) -> RabbitMQEventBusConfig:
        """Create configuration for work queue pattern."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="tasks",
            exchange_type="direct",
            consumer_group="workers",  # All workers share this queue
            enable_dlq=False,
        )

    def test_work_queue_uses_consistent_queue_name(
        self, work_queue_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that work queue config produces consistent queue name."""
        # Multiple workers with same config should use same queue
        config1 = RabbitMQEventBusConfig(
            exchange_name="tasks",
            exchange_type="direct",
            consumer_group="workers",
            consumer_name="worker-1",
        )
        config2 = RabbitMQEventBusConfig(
            exchange_name="tasks",
            exchange_type="direct",
            consumer_group="workers",
            consumer_name="worker-2",
        )

        # Queue names should be identical (based on exchange + consumer_group)
        assert config1.queue_name == config2.queue_name
        assert config1.queue_name == "tasks.workers"

    def test_work_queue_routing_key_matches_queue_name(
        self, work_queue_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that work queue routing key matches queue name."""
        # This ensures published messages with queue_name as routing key
        # are delivered to the correct queue
        routing_key = work_queue_config.get_effective_routing_key()
        assert routing_key == work_queue_config.queue_name

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_work_queue_multiple_consumers_same_binding(
        self, mock_aio_pika: MagicMock, work_queue_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that multiple workers create same binding for work queue pattern."""
        # Set up mocks
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Create two workers
        worker1_config = RabbitMQEventBusConfig(
            exchange_name="tasks",
            exchange_type="direct",
            consumer_group="workers",
            consumer_name="worker-1",
            enable_dlq=False,
        )
        worker2_config = RabbitMQEventBusConfig(
            exchange_name="tasks",
            exchange_type="direct",
            consumer_group="workers",
            consumer_name="worker-2",
            enable_dlq=False,
        )

        bus1 = RabbitMQEventBus(config=worker1_config)
        bus2 = RabbitMQEventBus(config=worker2_config)

        await bus1.connect()
        first_bind_call = mock_queue.bind.call_args

        mock_queue.bind.reset_mock()
        await bus2.connect()
        second_bind_call = mock_queue.bind.call_args

        # Both should bind with the same routing key
        assert first_bind_call.kwargs["routing_key"] == "tasks.workers"
        assert second_bind_call.kwargs["routing_key"] == "tasks.workers"


class TestDirectExchangeRoutingKeyGeneration:
    """Tests for routing key generation in publish for direct exchange."""

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_uses_aggregate_event_routing_key(self, mock_aio_pika: MagicMock) -> None:
        """Test that publish uses {aggregate_type}.{event_type} as routing key."""
        config = RabbitMQEventBusConfig(
            exchange_type="direct",
            enable_dlq=False,
        )

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_exchange.publish = AsyncMock()

        class OrderCreated(DomainEvent):
            aggregate_type: str = "Order"

        event = OrderCreated(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish([event])

        # Verify publish was called with correct routing key
        mock_exchange.publish.assert_called_once()
        call_kwargs = mock_exchange.publish.call_args.kwargs
        assert call_kwargs["routing_key"] == "Order.OrderCreated"
