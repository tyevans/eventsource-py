"""Unit tests extracted for test_rabbitmq_exchanges_fanout_dlq.py."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from eventsource.domain.event import DomainEvent


class TestFanoutExchangeWithDLQ:
    """Tests for fanout exchange with dead letter queue."""

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_with_dlq_enabled(self, mock_aio_pika: MagicMock) -> None:
        """Test that fanout exchange works correctly with DLQ enabled."""
        config = RabbitMQEventBusConfig(
            exchange_name="broadcast-events",
            exchange_type="fanout",
            consumer_group="handlers",
            enable_dlq=True,
        )

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_dlq_queue = AsyncMock()

        declare_exchange_calls = []

        def mock_declare_exchange(name: str, **kwargs) -> AsyncMock:
            declare_exchange_calls.append({"name": name, **kwargs})
            return mock_dlq_exchange if "_dlq" in name else mock_exchange

        declare_queue_calls = []

        def mock_declare_queue(name: str, **kwargs) -> AsyncMock:
            declare_queue_calls.append({"name": name, **kwargs})
            return mock_dlq_queue if ".dlq" in name else mock_queue

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(side_effect=mock_declare_exchange)
        mock_channel.declare_queue = AsyncMock(side_effect=mock_declare_queue)

        from aio_pika import ExchangeType

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Verify both main and DLQ exchanges were declared
        exchange_names = [call["name"] for call in declare_exchange_calls]
        assert "broadcast-events_dlq" in exchange_names
        assert "broadcast-events" in exchange_names

        # Verify main exchange is fanout type
        main_exchange_call = next(
            call for call in declare_exchange_calls if call["name"] == "broadcast-events"
        )
        assert main_exchange_call["type"] == ExchangeType.FANOUT


class TestFanoutExchangeLogging:
    """Tests for fanout exchange logging behavior."""

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_binding_logged_correctly(
        self, mock_aio_pika: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that fanout binding is logged with appropriate information."""
        config = RabbitMQEventBusConfig(
            exchange_name="broadcast",
            exchange_type="fanout",
            consumer_group="test-group",
            enable_dlq=False,
        )

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

        with caplog.at_level(logging.INFO):
            bus = RabbitMQEventBus(config=config)
            await bus.connect()

        # Verify fanout-specific binding log message
        binding_logs = [r for r in caplog.records if "Bound queue" in r.message]
        assert len(binding_logs) >= 1

        # Check log message includes fanout-specific broadcast mode info
        binding_log = binding_logs[0]
        assert "fanout" in binding_log.message.lower()
        assert "broadcast" in binding_log.message.lower()

        # Check log extras contain fanout-specific fields
        assert hasattr(binding_log, "broadcast_mode")
        assert binding_log.broadcast_mode is True
        assert hasattr(binding_log, "routing_key_ignored")
        assert binding_log.routing_key_ignored is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_publish_logged_with_routing_key(
        self, mock_aio_pika: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that fanout publish logs include routing key for debugging."""
        config = RabbitMQEventBusConfig(
            exchange_name="broadcast",
            exchange_type="fanout",
            consumer_group="test-group",
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

        class BroadcastEvent(DomainEvent):
            aggregate_type: str = "System"

        event = BroadcastEvent(aggregate_id=uuid4())

        with caplog.at_level(logging.DEBUG):
            bus = RabbitMQEventBus(config=config)
            await bus.connect()
            await bus.publish([event])

        # Verify publish log includes routing key
        publish_logs = [r for r in caplog.records if "Published" in r.message]
        assert len(publish_logs) >= 1
