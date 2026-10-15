"""Unit tests extracted for test_rabbitmq_topology_queue.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig


def _setup_mocks(
    mock_aio_pika: MagicMock,
) -> tuple[AsyncMock, AsyncMock, AsyncMock, AsyncMock]:
    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_exchange = AsyncMock()
    mock_queue = AsyncMock()
    mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
    mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
    return mock_connection, mock_channel, mock_exchange, mock_queue


class TestRabbitMQQueueDeclaration:
    """Tests for queue declaration in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_queue_basic(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        _, mock_channel, _, _ = _setup_mocks(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_channel.declare_queue.assert_called_once()
        call_kwargs = mock_channel.declare_queue.call_args.kwargs
        assert call_kwargs["name"] == config.queue_name
        assert call_kwargs["durable"] == config.durable
        assert call_kwargs["auto_delete"] == config.auto_delete

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_queue_with_dlq_arguments(self, mock_aio_pika: MagicMock) -> None:
        config = RabbitMQEventBusConfig(
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )
        _, mock_channel, _, _ = _setup_mocks(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        main_queue_call = next(
            (
                call
                for call in mock_channel.declare_queue.call_args_list
                if call.kwargs["name"] == config.queue_name
            ),
            None,
        )
        assert main_queue_call is not None
        call_kwargs = main_queue_call.kwargs
        assert "arguments" in call_kwargs
        assert call_kwargs["arguments"]["x-dead-letter-exchange"] == config.dlq_exchange_name
        assert call_kwargs["arguments"]["x-dead-letter-routing-key"] == config.queue_name

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_queue_without_dlq_arguments(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        _, mock_channel, _, _ = _setup_mocks(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        call_kwargs = mock_channel.declare_queue.call_args.kwargs
        assert call_kwargs["arguments"] is None

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_queue_reference_stored(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        _, _, _, mock_queue = _setup_mocks(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        assert bus._topology._consumer_queue is mock_queue


class TestRabbitMQQueueBinding:
    """Tests for queue binding in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_bind_queue_to_exchange(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        _, _, _, mock_queue = _setup_mocks(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_queue.bind.assert_called_once()

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_bind_queue_with_wildcard_routing_key(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        _, _, _, mock_queue = _setup_mocks(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == "#"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_bind_queue_to_correct_exchange(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        _, _, mock_exchange, mock_queue = _setup_mocks(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["exchange"] is mock_exchange
