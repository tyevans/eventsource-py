"""Unit tests extracted for test_rabbitmq_publish_batch.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
    RabbitMQEventBusStats,
)

from ._fixtures import PublishTestEvent


class TestBatchPublishingConfig:
    """Tests for batch publishing configuration options."""

    def test_default_batch_size(self) -> None:
        """Test default publish_chunk_size is 100."""
        config = RabbitMQEventBusConfig()
        assert config.publish_chunk_size == 100

    def test_custom_batch_size(self) -> None:
        """Test custom publish_chunk_size configuration."""
        config = RabbitMQEventBusConfig(publish_chunk_size=50)
        assert config.publish_chunk_size == 50

    def test_default_max_concurrent_publishes(self) -> None:
        """Test default max_concurrent_publishes is 10."""
        config = RabbitMQEventBusConfig()
        assert config.max_concurrent_publishes == 10

    def test_custom_max_concurrent_publishes(self) -> None:
        """Test custom max_concurrent_publishes configuration."""
        config = RabbitMQEventBusConfig(max_concurrent_publishes=20)
        assert config.max_concurrent_publishes == 20


class TestBatchPublishingStats:
    """Tests for batch publishing statistics fields."""

    def test_stats_have_batch_fields(self) -> None:
        """Test that stats include batch publishing fields."""
        stats = RabbitMQEventBusStats()
        assert hasattr(stats, "batch_publishes")
        assert hasattr(stats, "batch_events_published")
        assert hasattr(stats, "batch_partial_failures")

    def test_batch_stats_default_to_zero(self) -> None:
        """Test that batch stats default to zero."""
        stats = RabbitMQEventBusStats()
        assert stats.batch_publishes == 0
        assert stats.batch_events_published == 0
        assert stats.batch_partial_failures == 0


class TestBatchPublishError:
    """Tests for BatchPublishError exception."""

    def test_batch_publish_error_can_be_raised(self) -> None:
        """Test that BatchPublishError can be raised and caught."""
        from eventsource.adapters.rabbitmq import BatchPublishError

        with pytest.raises(BatchPublishError):
            raise BatchPublishError("Test error")

    def test_batch_publish_error_has_results(self) -> None:
        """Test that BatchPublishError contains results dict."""
        from eventsource.adapters.rabbitmq import BatchPublishError

        results = {"total": 10, "published": 8, "failed": 2, "chunks": 1}
        error = BatchPublishError("Partial failure", results=results)

        assert error.results == results
        assert error.results["total"] == 10
        assert error.results["published"] == 8
        assert error.results["failed"] == 2

    def test_batch_publish_error_has_errors_list(self) -> None:
        """Test that BatchPublishError contains list of errors."""
        from eventsource.adapters.rabbitmq import BatchPublishError

        errors = [Exception("Error 1"), Exception("Error 2")]
        error = BatchPublishError("Multiple failures", errors=errors)

        assert len(error.errors) == 2
        assert str(error.errors[0]) == "Error 1"
        assert str(error.errors[1]) == "Error 2"

    def test_batch_publish_error_defaults(self) -> None:
        """Test that BatchPublishError has sensible defaults."""
        from eventsource.adapters.rabbitmq import BatchPublishError

        error = BatchPublishError("Test")

        assert error.results == {"total": 0, "published": 0, "failed": 0, "chunks": 0}
        assert error.errors == []


class TestPublishMethodBatchOptimization:
    """Tests for batch optimization in the publish() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            publish_chunk_size=10,
            max_concurrent_publishes=5,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_single_event_no_batch(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publishing a single event does not use batch optimization."""
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

        event = PublishTestEvent(aggregate_id=uuid4(), data="single")
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus.publish([event])

        # Single event should not increment batch_publishes
        assert bus.stats.events_published == 1
        assert bus.stats.batch_publishes == 0

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_multiple_events_uses_batch(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publishing multiple events uses batch optimization."""
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

        await bus.publish(events)

        # Multiple events should use batch optimization
        assert bus.stats.events_published == 5
        assert bus.stats.batch_publishes == 1
        assert bus.stats.batch_events_published == 5
