"""Unit tests extracted for test_rabbitmq_publish.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from aio_pika import Message

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

from ._fixtures import PublishTestEvent


def _setup_mock_aio_pika(mock_aio_pika: MagicMock) -> AsyncMock:
    """Configure mock aio_pika connection, channel, exchange, and queue."""
    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_exchange = AsyncMock()
    mock_queue = AsyncMock()
    mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
    mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
    return mock_exchange


class TestRabbitMQPublish:
    """Tests for RabbitMQEventBus.publish() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @pytest.fixture
    def sample_events(self) -> list[PublishTestEvent]:
        """Create sample events for testing."""
        return [
            PublishTestEvent(aggregate_id=uuid4(), data="event-1"),
            PublishTestEvent(aggregate_id=uuid4(), data="event-2"),
        ]

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_empty_list_returns_immediately(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publishing empty list does nothing and doesn't raise."""
        bus = RabbitMQEventBus(config=config)

        await bus.publish([])

        mock_aio_pika.connect_robust.assert_not_called()
        assert bus.is_connected is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_auto_connects_if_not_connected(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish auto-connects if not already connected."""
        _setup_mock_aio_pika(mock_aio_pika)
        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)

        assert bus.is_connected is False

        await bus.publish([event])

        assert bus.is_connected is True
        mock_aio_pika.connect_robust.assert_called_once()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_calls_exchange_publish(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        sample_events: list[PublishTestEvent],
    ) -> None:
        """Test that publish calls exchange.publish for each event."""
        mock_exchange = _setup_mock_aio_pika(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish(sample_events)

        assert mock_exchange.publish.call_count == len(sample_events)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_increments_events_published_stat(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        sample_events: list[PublishTestEvent],
    ) -> None:
        """Test that events_published stat is incremented for each event."""
        _setup_mock_aio_pika(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        assert bus.stats.events_published == 0

        await bus.publish(sample_events)

        assert bus.stats.events_published == len(sample_events)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_increments_publish_confirms_stat(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        sample_events: list[PublishTestEvent],
    ) -> None:
        """Test that publish_confirms stat is incremented when background=False."""
        _setup_mock_aio_pika(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        assert bus.stats.publish_confirms == 0

        await bus.publish(sample_events, background=False)

        assert bus.stats.publish_confirms == len(sample_events)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_background_does_not_increment_confirms(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        sample_events: list[PublishTestEvent],
    ) -> None:
        """Test that publish_confirms stat is NOT incremented when background=True."""
        _setup_mock_aio_pika(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus.publish(sample_events, background=True)

        assert bus.stats.events_published == len(sample_events)
        assert bus.stats.publish_confirms == 0

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_uses_correct_routing_key(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish uses the correct routing key for each event."""
        mock_exchange = _setup_mock_aio_pika(mock_aio_pika)
        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish([event])

        call_kwargs = mock_exchange.publish.call_args.kwargs
        assert call_kwargs["routing_key"] == "TestAggregate.PublishTestEvent"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_creates_message_using_create_message(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish creates AMQP message using _create_message."""
        mock_exchange = _setup_mock_aio_pika(mock_aio_pika)
        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish([event])

        call_args = mock_exchange.publish.call_args
        published_message = call_args.args[0] if call_args.args else call_args.kwargs.get("message")
        assert isinstance(published_message, Message)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_single_event(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publishing a single event."""
        mock_exchange = _setup_mock_aio_pika(mock_aio_pika)
        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish([event])

        assert mock_exchange.publish.call_count == 1
        assert bus.stats.events_published == 1

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_raises_runtime_error_when_exchange_none(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish raises RuntimeError if exchange is None after connect."""
        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)

        bus._connection_manager._connected = True
        bus._topology._exchange = None

        with pytest.raises(RuntimeError, match="Exchange not initialized"):
            await bus.publish([event])

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_reraises_exception_on_failure(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish re-raises exceptions from exchange.publish."""
        mock_exchange = _setup_mock_aio_pika(mock_aio_pika)
        mock_exchange.publish = AsyncMock(side_effect=Exception("Connection lost"))

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        with pytest.raises(Exception, match="Connection lost"):
            await bus.publish([event])

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_does_not_increment_stats_on_failure(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that stats are not incremented when publish fails."""
        mock_exchange = _setup_mock_aio_pika(mock_aio_pika)
        mock_exchange.publish = AsyncMock(side_effect=Exception("Publish failed"))

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        initial_published = bus.stats.events_published
        initial_confirms = bus.stats.publish_confirms

        with pytest.raises(Exception, match="Publish failed"):
            await bus.publish([event])

        assert bus.stats.events_published == initial_published
        assert bus.stats.publish_confirms == initial_confirms
