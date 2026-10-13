"""Unit tests extracted for test_rabbitmq_exchanges_direct.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestDirectExchangeConfig:
    """Tests for direct exchange configuration options."""

    def test_routing_key_pattern_default_is_none(self) -> None:
        """Test that routing_key_pattern is None by default."""
        config = RabbitMQEventBusConfig()
        assert config.routing_key_pattern is None

    def test_routing_key_pattern_can_be_set(self) -> None:
        """Test that routing_key_pattern can be explicitly set."""
        config = RabbitMQEventBusConfig(routing_key_pattern="custom.key")
        assert config.routing_key_pattern == "custom.key"

    def test_get_effective_routing_key_topic_default(self) -> None:
        """Test effective routing key for topic exchange without explicit pattern."""
        config = RabbitMQEventBusConfig(exchange_type="topic")
        assert config.get_effective_routing_key() == "#"

    def test_get_effective_routing_key_direct_default(self) -> None:
        """Test effective routing key for direct exchange without explicit pattern."""
        config = RabbitMQEventBusConfig(
            exchange_name="events",
            exchange_type="direct",
            consumer_group="workers",
        )
        # Direct exchange defaults to queue_name for work queue pattern
        assert config.get_effective_routing_key() == "events.workers"

    def test_get_effective_routing_key_fanout_default(self) -> None:
        """Test effective routing key for fanout exchange."""
        config = RabbitMQEventBusConfig(exchange_type="fanout")
        # Fanout ignores routing key
        assert config.get_effective_routing_key() == ""

    def test_get_effective_routing_key_headers_default(self) -> None:
        """Test effective routing key for headers exchange."""
        config = RabbitMQEventBusConfig(exchange_type="headers")
        # Headers ignores routing key
        assert config.get_effective_routing_key() == ""

    def test_get_effective_routing_key_explicit_pattern_overrides(self) -> None:
        """Test that explicit routing_key_pattern overrides defaults."""
        config = RabbitMQEventBusConfig(
            exchange_type="direct",
            routing_key_pattern="my.custom.pattern",
        )
        assert config.get_effective_routing_key() == "my.custom.pattern"

    def test_get_effective_routing_key_empty_pattern_allowed(self) -> None:
        """Test that empty string routing_key_pattern is allowed."""
        config = RabbitMQEventBusConfig(
            exchange_type="topic",
            routing_key_pattern="",  # Explicitly empty
        )
        assert config.get_effective_routing_key() == ""

    def test_get_effective_routing_key_unknown_exchange_type(self) -> None:
        """Test behavior with unknown exchange type defaults to topic behavior."""
        config = RabbitMQEventBusConfig(exchange_type="unknown")
        assert config.get_effective_routing_key() == "#"

    def test_get_effective_routing_key_case_insensitive(self) -> None:
        """Test that exchange type comparison is case insensitive."""
        config1 = RabbitMQEventBusConfig(exchange_type="DIRECT", consumer_group="test")
        config2 = RabbitMQEventBusConfig(exchange_type="Direct", consumer_group="test")
        config3 = RabbitMQEventBusConfig(exchange_type="direct", consumer_group="test")

        expected = "events.test"  # queue_name = exchange_name.consumer_group
        assert config1.get_effective_routing_key() == expected
        assert config2.get_effective_routing_key() == expected
        assert config3.get_effective_routing_key() == expected


class TestDirectExchangeBinding:
    """Tests for direct exchange binding behavior."""

    @pytest.fixture
    def direct_exchange_config(self) -> RabbitMQEventBusConfig:
        """Create configuration for direct exchange testing."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            exchange_type="direct",
            consumer_group="test-workers",
            enable_dlq=False,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_direct_exchange_uses_queue_name_as_routing_key(
        self, mock_aio_pika: MagicMock, direct_exchange_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that direct exchange binds with queue_name as routing key."""
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

        bus = RabbitMQEventBus(config=direct_exchange_config)
        await bus.connect()

        # Verify queue was bound with queue_name as routing key
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == direct_exchange_config.queue_name

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_topic_exchange_uses_wildcard_routing_key(self, mock_aio_pika: MagicMock) -> None:
        """Test that topic exchange binds with '#' wildcard routing key."""
        config = RabbitMQEventBusConfig(
            exchange_type="topic",
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

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Verify queue was bound with '#' routing key
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == "#"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_exchange_uses_empty_routing_key(self, mock_aio_pika: MagicMock) -> None:
        """Test that fanout exchange binds with empty routing key."""
        config = RabbitMQEventBusConfig(
            exchange_type="fanout",
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

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Verify queue was bound with empty routing key
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == ""

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_custom_routing_key_pattern_overrides_default(
        self, mock_aio_pika: MagicMock
    ) -> None:
        """Test that custom routing_key_pattern overrides exchange type default."""
        config = RabbitMQEventBusConfig(
            exchange_type="direct",
            routing_key_pattern="custom.routing.key",
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

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Verify queue was bound with custom routing key
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == "custom.routing.key"
