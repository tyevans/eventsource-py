"""Unit tests extracted for test_rabbitmq_exchanges_direct.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


def _setup_mock_queue(mock_aio_pika: MagicMock) -> AsyncMock:
    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_exchange = AsyncMock()
    mock_queue = AsyncMock()
    mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
    mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
    return mock_queue


class TestDirectExchangeConfig:
    """Tests for direct exchange configuration options."""

    def test_routing_key_pattern_default_is_none(self) -> None:
        assert RabbitMQEventBusConfig().routing_key_pattern is None

    def test_routing_key_pattern_can_be_set(self) -> None:
        config = RabbitMQEventBusConfig(routing_key_pattern="custom.key")
        assert config.routing_key_pattern == "custom.key"

    def test_get_effective_routing_key_topic_default(self) -> None:
        config = RabbitMQEventBusConfig(exchange_type="topic")
        assert config.get_effective_routing_key() == "#"

    def test_get_effective_routing_key_direct_default(self) -> None:
        config = RabbitMQEventBusConfig(
            exchange_name="events", exchange_type="direct", consumer_group="workers"
        )
        assert config.get_effective_routing_key() == "events.workers"

    def test_get_effective_routing_key_fanout_default(self) -> None:
        config = RabbitMQEventBusConfig(exchange_type="fanout")
        assert config.get_effective_routing_key() == ""

    def test_get_effective_routing_key_headers_default(self) -> None:
        config = RabbitMQEventBusConfig(exchange_type="headers")
        assert config.get_effective_routing_key() == ""

    def test_get_effective_routing_key_explicit_pattern_overrides(self) -> None:
        config = RabbitMQEventBusConfig(
            exchange_type="direct", routing_key_pattern="my.custom.pattern"
        )
        assert config.get_effective_routing_key() == "my.custom.pattern"

    def test_get_effective_routing_key_empty_pattern_allowed(self) -> None:
        config = RabbitMQEventBusConfig(exchange_type="topic", routing_key_pattern="")
        assert config.get_effective_routing_key() == ""

    def test_get_effective_routing_key_unknown_exchange_type(self) -> None:
        config = RabbitMQEventBusConfig(exchange_type="unknown")
        assert config.get_effective_routing_key() == "#"

    def test_get_effective_routing_key_case_insensitive(self) -> None:
        for ex_type in ("DIRECT", "Direct", "direct"):
            config = RabbitMQEventBusConfig(exchange_type=ex_type, consumer_group="test")
            assert config.get_effective_routing_key() == "events.test"


class TestDirectExchangeBinding:
    """Tests for direct exchange binding behavior."""

    @pytest.fixture
    def direct_exchange_config(self) -> RabbitMQEventBusConfig:
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
        mock_queue = _setup_mock_queue(mock_aio_pika)
        bus = RabbitMQEventBus(config=direct_exchange_config)
        await bus.connect()

        mock_queue.bind.assert_called_once()
        assert mock_queue.bind.call_args.kwargs["routing_key"] == direct_exchange_config.queue_name

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_topic_exchange_uses_wildcard_routing_key(self, mock_aio_pika: MagicMock) -> None:
        mock_queue = _setup_mock_queue(mock_aio_pika)
        bus = RabbitMQEventBus(
            config=RabbitMQEventBusConfig(exchange_type="topic", enable_dlq=False)
        )
        await bus.connect()

        mock_queue.bind.assert_called_once()
        assert mock_queue.bind.call_args.kwargs["routing_key"] == "#"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_exchange_uses_empty_routing_key(self, mock_aio_pika: MagicMock) -> None:
        mock_queue = _setup_mock_queue(mock_aio_pika)
        bus = RabbitMQEventBus(
            config=RabbitMQEventBusConfig(exchange_type="fanout", enable_dlq=False)
        )
        await bus.connect()

        mock_queue.bind.assert_called_once()
        assert mock_queue.bind.call_args.kwargs["routing_key"] == ""

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_custom_routing_key_pattern_overrides_default(
        self, mock_aio_pika: MagicMock
    ) -> None:
        mock_queue = _setup_mock_queue(mock_aio_pika)
        config = RabbitMQEventBusConfig(
            exchange_type="direct", routing_key_pattern="custom.routing.key", enable_dlq=False
        )
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_queue.bind.assert_called_once()
        assert mock_queue.bind.call_args.kwargs["routing_key"] == "custom.routing.key"
