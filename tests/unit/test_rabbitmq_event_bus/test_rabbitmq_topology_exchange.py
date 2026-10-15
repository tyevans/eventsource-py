"""Unit tests extracted for test_rabbitmq_topology_exchange.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBusConfig


class TestRabbitMQExchangeDeclaration:
    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            exchange_type="topic",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_topic(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
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
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        mock_channel.declare_exchange.assert_called_once()
        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["name"] == config.exchange_name
        assert call_kwargs["durable"] == config.durable
        assert call_kwargs["auto_delete"] == config.auto_delete

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_direct(self, mock_aio_pika: MagicMock) -> None:
        from aio_pika import ExchangeType

        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="direct-events", exchange_type="direct", enable_dlq=False
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
        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["type"] == ExchangeType.DIRECT

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_fanout(self, mock_aio_pika: MagicMock) -> None:
        from aio_pika import ExchangeType

        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="fanout-events", exchange_type="fanout", enable_dlq=False
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
        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["type"] == ExchangeType.FANOUT

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_headers(self, mock_aio_pika: MagicMock) -> None:
        from aio_pika import ExchangeType

        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="headers-events", exchange_type="headers", enable_dlq=False
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
        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["type"] == ExchangeType.HEADERS

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_unknown_type_defaults_to_topic(
        self, mock_aio_pika: MagicMock
    ) -> None:
        from aio_pika import ExchangeType

        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="events", exchange_type="unknown_type", enable_dlq=False
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
        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["type"] == ExchangeType.TOPIC

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_case_insensitive(self, mock_aio_pika: MagicMock) -> None:
        from aio_pika import ExchangeType

        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="events", exchange_type="TOPIC", enable_dlq=False
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
        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["type"] == ExchangeType.TOPIC

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_durable_setting(self, mock_aio_pika: MagicMock) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(exchange_name="events", durable=False, enable_dlq=False)
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
        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["durable"] is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_auto_delete_setting(self, mock_aio_pika: MagicMock) -> None:
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(exchange_name="events", auto_delete=True, enable_dlq=False)
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
        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["auto_delete"] is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_exchange_reference_stored(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
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
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        assert bus._topology._exchange is mock_exchange
