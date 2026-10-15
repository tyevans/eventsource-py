"""Unit tests extracted for test_rabbitmq_publish_batch_method.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    BatchPublishError,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)

from ._fixtures import PublishTestEvent


class TestPublishBatchMethod:
    """Tests for the publish_batch() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            publish_chunk_size=10,
            max_concurrent_publishes=5,
        )

    def _setup_pika_mocks(self, mock_aio_pika: MagicMock) -> AsyncMock:
        mock_connection = AsyncMock(is_closed=False)
        mock_channel = AsyncMock(is_closed=False)
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        return mock_exchange

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_empty_list(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        bus = RabbitMQEventBus(config=config)
        assert await bus.publish_batch([]) == {
            "total": 0,
            "published": 0,
            "failed": 0,
            "chunks": 0,
        }

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_success(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        self._setup_pika_mocks(mock_aio_pika)
        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        result = await bus.publish_batch(events)
        assert result == {"total": 5, "published": 5, "failed": 0, "chunks": 1}
        assert (bus.stats.batch_publishes, bus.stats.batch_events_published) == (1, 5)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_with_chunking(self, mock_aio_pika: MagicMock) -> None:
        self._setup_pika_mocks(mock_aio_pika)
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            publish_chunk_size=5,
            max_concurrent_publishes=3,
        )
        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(12)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        result = await bus.publish_batch(events)
        assert (result["total"], result["published"], result["chunks"]) == (12, 12, 3)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_preserve_order(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        mock_exchange = self._setup_pika_mocks(mock_aio_pika)
        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        result = await bus.publish_batch(events, preserve_order=True)
        assert (result["total"], result["published"], result["chunks"]) == (5, 5, 1)
        assert mock_exchange.publish.call_count == 5

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_partial_failure_raises_error(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        mock_exchange = self._setup_pika_mocks(mock_aio_pika)
        mock_exchange.publish = AsyncMock(
            side_effect=[None, Exception("Publish failed"), None, None, None]
        )
        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        with pytest.raises(BatchPublishError) as exc_info:
            await bus.publish_batch(events)

        res = exc_info.value.results
        assert (res["total"], res["published"], res["failed"]) == (5, 4, 1)
        assert len(exc_info.value.errors) == 1 and bus.stats.batch_partial_failures == 1

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_auto_connects(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        self._setup_pika_mocks(mock_aio_pika)
        events = [PublishTestEvent(aggregate_id=uuid4(), data="test")]
        bus = RabbitMQEventBus(config=config)
        assert not bus.is_connected

        result = await bus.publish_batch(events)
        assert bus.is_connected and result["published"] == 1
