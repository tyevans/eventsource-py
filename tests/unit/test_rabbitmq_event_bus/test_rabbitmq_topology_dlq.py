"""Unit tests extracted for test_rabbitmq_topology_dlq.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aio_pika import ExchangeType

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig


def _wire_channel(
    mock_aio_pika: MagicMock,
    *,
    with_dlq_mocks: bool = False,
) -> tuple[AsyncMock, AsyncMock, AsyncMock]:
    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_exchange, mock_dlq_exchange = AsyncMock(), AsyncMock()
    mock_queue, mock_dlq_queue = AsyncMock(), AsyncMock()
    mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    if with_dlq_mocks:
        mock_channel.declare_exchange = AsyncMock(side_effect=[mock_dlq_exchange, mock_exchange])
        mock_channel.declare_queue = AsyncMock(side_effect=[mock_dlq_queue, mock_queue])
    else:
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
    return mock_channel, mock_dlq_exchange, mock_dlq_queue


class TestRabbitMQDLQDeclaration:
    """Tests for dead letter queue declaration in RabbitMQEventBus."""

    @pytest.fixture
    def config_with_dlq(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_exchange_declared_when_enabled(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        mock_channel, _, _ = _wire_channel(mock_aio_pika, with_dlq_mocks=True)
        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        assert mock_channel.declare_exchange.call_count == 2
        first_call_kwargs = mock_channel.declare_exchange.call_args_list[0].kwargs
        assert first_call_kwargs["name"] == config_with_dlq.dlq_exchange_name

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_exchange_is_direct_type(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        mock_channel, _, _ = _wire_channel(mock_aio_pika)
        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        first_call_kwargs = mock_channel.declare_exchange.call_args_list[0].kwargs
        assert first_call_kwargs["type"] == ExchangeType.DIRECT

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_declared_when_enabled(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        mock_channel, _, _ = _wire_channel(mock_aio_pika)
        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        assert mock_channel.declare_queue.call_count == 2
        first_call_kwargs = mock_channel.declare_queue.call_args_list[0].kwargs
        assert first_call_kwargs["name"] == config_with_dlq.dlq_queue_name

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_bound_to_dlq_exchange(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        _, mock_dlq_exchange, mock_dlq_queue = _wire_channel(mock_aio_pika, with_dlq_mocks=True)
        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        mock_dlq_queue.bind.assert_called_once()
        dlq_bind_kwargs = mock_dlq_queue.bind.call_args.kwargs
        assert dlq_bind_kwargs["exchange"] is mock_dlq_exchange
        assert dlq_bind_kwargs["routing_key"] == config_with_dlq.queue_name

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_references_stored(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        _, mock_dlq_exchange, mock_dlq_queue = _wire_channel(mock_aio_pika, with_dlq_mocks=True)
        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        assert bus._topology._dlq_exchange is mock_dlq_exchange
        assert bus._topology._dlq_queue is mock_dlq_queue

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_no_dlq_when_disabled(self, mock_aio_pika: MagicMock) -> None:
        config = RabbitMQEventBusConfig(
            exchange_name="test-events", consumer_group="test-group", enable_dlq=False
        )
        mock_channel, _, _ = _wire_channel(mock_aio_pika)
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        assert mock_channel.declare_exchange.call_count == 1
        assert mock_channel.declare_queue.call_count == 1
        assert bus._topology._dlq_exchange is None
        assert bus._topology._dlq_queue is None
