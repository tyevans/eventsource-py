"""Unit tests extracted for test_rabbitmq_publish_batch_method.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
)

from ._fixtures import PublishTestEvent


class TestPublishBatchMethod:
    """Tests for the publish_batch() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration with batch settings."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            publish_chunk_size=10,
            max_concurrent_publishes=5,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_empty_list(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publish_batch with empty list returns zero stats."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)
        result = await bus.publish_batch([])

        assert result == {"total": 0, "published": 0, "failed": 0, "chunks": 0}

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_success(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publish_batch successfully publishes all events."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

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

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        result = await bus.publish_batch(events)

        assert result["total"] == 5
        assert result["published"] == 5
        assert result["failed"] == 0
        assert result["chunks"] == 1
        assert bus.stats.batch_publishes == 1
        assert bus.stats.batch_events_published == 5

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_with_chunking(self, mock_aio_pika: MagicMock) -> None:
        """Test publish_batch splits large batches into chunks."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            publish_chunk_size=5,  # Small chunk size to force chunking
            max_concurrent_publishes=3,
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

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(12)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        result = await bus.publish_batch(events)

        assert result["total"] == 12
        assert result["published"] == 12
        assert result["chunks"] == 3  # 12 events / 5 publish_chunk_size = 3 chunks

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_preserve_order(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publish_batch with preserve_order=True publishes sequentially."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

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

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        result = await bus.publish_batch(events, preserve_order=True)

        assert result["total"] == 5
        assert result["published"] == 5
        assert result["chunks"] == 1  # Sequential is always 1 chunk
        assert mock_exchange.publish.call_count == 5

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_partial_failure_raises_error(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publish_batch raises BatchPublishError on partial failure."""
        from eventsource.adapters.rabbitmq import BatchPublishError, RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        # Second call fails
        mock_exchange.publish = AsyncMock(
            side_effect=[None, Exception("Publish failed"), None, None, None]
        )
        mock_queue = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        with pytest.raises(BatchPublishError) as exc_info:
            await bus.publish_batch(events)

        assert exc_info.value.results["total"] == 5
        assert exc_info.value.results["published"] == 4
        assert exc_info.value.results["failed"] == 1
        assert len(exc_info.value.errors) == 1
        assert bus.stats.batch_partial_failures == 1

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_auto_connects(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publish_batch auto-connects if not connected."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

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

        events = [PublishTestEvent(aggregate_id=uuid4(), data="test")]
        bus = RabbitMQEventBus(config=config)

        # Not connected yet
        assert not bus.is_connected

        result = await bus.publish_batch(events)

        # Should have auto-connected
        assert bus.is_connected
        assert result["published"] == 1
