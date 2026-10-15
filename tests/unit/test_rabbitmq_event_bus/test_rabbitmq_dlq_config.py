"""Unit tests extracted for test_rabbitmq_dlq_config.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig


class TestRabbitMQDLQConfigOptions:
    """Tests for enhanced DLQ configuration options (P2-001)."""

    def test_dlq_message_ttl_default_is_none(self) -> None:
        config = RabbitMQEventBusConfig()
        assert config.dlq_message_ttl is None

    def test_dlq_max_length_default_is_none(self) -> None:
        config = RabbitMQEventBusConfig()
        assert config.dlq_max_length is None

    def test_dlq_message_ttl_can_be_set(self) -> None:
        config = RabbitMQEventBusConfig(dlq_message_ttl=3600000)
        assert config.dlq_message_ttl == 3600000

    def test_dlq_max_length_can_be_set(self) -> None:
        config = RabbitMQEventBusConfig(dlq_max_length=10000)
        assert config.dlq_max_length == 10000

    def test_dlq_message_ttl_zero_is_valid(self) -> None:
        config = RabbitMQEventBusConfig(dlq_message_ttl=0)
        assert config.dlq_message_ttl == 0

    def test_dlq_max_length_one_is_valid(self) -> None:
        config = RabbitMQEventBusConfig(dlq_max_length=1)
        assert config.dlq_max_length == 1

    def test_combined_dlq_options(self) -> None:
        config = RabbitMQEventBusConfig(dlq_message_ttl=86400000, dlq_max_length=5000)
        assert config.dlq_message_ttl == 86400000
        assert config.dlq_max_length == 5000


class TestRabbitMQDLQQueueDeclarationWithOptions:
    """Tests for DLQ queue declaration with TTL and max_length (P2-001)."""

    @pytest.fixture
    def config_with_dlq_options(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
            dlq_message_ttl=3600000,
            dlq_max_length=1000,
        )

    @pytest.fixture
    def config_without_dlq_options(self) -> RabbitMQEventBusConfig:
        return RabbitMQEventBusConfig(
            exchange_name="test-events", consumer_group="test-group", enable_dlq=True
        )

    def _setup_mock_pika(self, mock_aio_pika: MagicMock) -> AsyncMock:
        mock_channel = AsyncMock(is_closed=False)
        mock_channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        mock_channel.declare_queue = AsyncMock(return_value=AsyncMock())
        mock_conn = AsyncMock(is_closed=False, channel=AsyncMock(return_value=mock_channel))
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_conn)
        return mock_channel

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_declared_with_ttl(
        self, mock_aio_pika: MagicMock, config_with_dlq_options: RabbitMQEventBusConfig
    ) -> None:
        mock_channel = self._setup_mock_pika(mock_aio_pika)
        bus = RabbitMQEventBus(config=config_with_dlq_options)
        await bus.connect()

        dlq_queue_call = mock_channel.declare_queue.call_args_list[0]
        assert dlq_queue_call.kwargs["name"] == config_with_dlq_options.dlq_queue_name
        assert "arguments" in dlq_queue_call.kwargs
        assert dlq_queue_call.kwargs["arguments"]["x-message-ttl"] == 3600000

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_declared_with_max_length(
        self, mock_aio_pika: MagicMock, config_with_dlq_options: RabbitMQEventBusConfig
    ) -> None:
        mock_channel = self._setup_mock_pika(mock_aio_pika)
        bus = RabbitMQEventBus(config=config_with_dlq_options)
        await bus.connect()

        dlq_queue_call = mock_channel.declare_queue.call_args_list[0]
        assert dlq_queue_call.kwargs["arguments"]["x-max-length"] == 1000

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_declared_without_arguments_when_options_not_set(
        self, mock_aio_pika: MagicMock, config_without_dlq_options: RabbitMQEventBusConfig
    ) -> None:
        mock_channel = self._setup_mock_pika(mock_aio_pika)
        bus = RabbitMQEventBus(config=config_without_dlq_options)
        await bus.connect()

        dlq_queue_call = mock_channel.declare_queue.call_args_list[0]
        assert dlq_queue_call.kwargs.get("arguments") is None


class TestRabbitMQDLQHelperEdgeCases:
    """Edge case tests for DLQ helper methods."""

    def test_get_death_count_invalid_count_type(self) -> None:
        message = MagicMock(headers={"x-death": [{"count": "not-a-number"}, {"count": 5}]})
        assert RabbitMQEventBus.get_death_count(message) == 5

    def test_get_death_count_missing_count_field(self) -> None:
        message = MagicMock(headers={"x-death": [{"reason": "rejected"}, {"count": 3}]})
        assert RabbitMQEventBus.get_death_count(message) == 3

    def test_get_original_routing_key_empty_routing_keys(self) -> None:
        message = MagicMock(headers={"x-death": [{"routing-keys": []}]})
        assert RabbitMQEventBus.get_original_routing_key(message) is None

    def test_get_original_routing_key_no_routing_keys(self) -> None:
        message = MagicMock(headers={"x-death": [{"count": 1, "reason": "rejected"}]})
        assert RabbitMQEventBus.get_original_routing_key(message) is None

    def test_x_death_non_list_type(self) -> None:
        message = MagicMock(headers={"x-death": "not-a-list"})
        assert RabbitMQEventBus.get_death_count(message) == 0
        assert RabbitMQEventBus.is_from_dlq(message) is False
        assert RabbitMQEventBus.get_original_routing_key(message) is None

    def test_x_death_non_dict_entries(self) -> None:
        message = MagicMock(headers={"x-death": ["not-a-dict", {"count": 2}, None]})
        assert RabbitMQEventBus.get_death_count(message) == 2
