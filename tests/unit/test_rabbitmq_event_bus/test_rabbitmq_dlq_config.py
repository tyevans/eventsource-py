"""Unit tests extracted for test_rabbitmq_dlq_config.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
)


class TestRabbitMQDLQConfigOptions:
    """Tests for enhanced DLQ configuration options (P2-001)."""

    def test_dlq_message_ttl_default_is_none(self) -> None:
        """Test that dlq_message_ttl defaults to None."""
        config = RabbitMQEventBusConfig()
        assert config.dlq_message_ttl is None

    def test_dlq_max_length_default_is_none(self) -> None:
        """Test that dlq_max_length defaults to None."""
        config = RabbitMQEventBusConfig()
        assert config.dlq_max_length is None

    def test_dlq_message_ttl_can_be_set(self) -> None:
        """Test that dlq_message_ttl can be configured."""
        config = RabbitMQEventBusConfig(dlq_message_ttl=3600000)  # 1 hour in ms
        assert config.dlq_message_ttl == 3600000

    def test_dlq_max_length_can_be_set(self) -> None:
        """Test that dlq_max_length can be configured."""
        config = RabbitMQEventBusConfig(dlq_max_length=10000)
        assert config.dlq_max_length == 10000

    def test_dlq_message_ttl_zero_is_valid(self) -> None:
        """Test that dlq_message_ttl of 0 is valid (immediate expiration)."""
        config = RabbitMQEventBusConfig(dlq_message_ttl=0)
        assert config.dlq_message_ttl == 0

    def test_dlq_max_length_one_is_valid(self) -> None:
        """Test that dlq_max_length of 1 is valid (single message)."""
        config = RabbitMQEventBusConfig(dlq_max_length=1)
        assert config.dlq_max_length == 1

    def test_combined_dlq_options(self) -> None:
        """Test that both DLQ options can be set together."""
        config = RabbitMQEventBusConfig(
            dlq_message_ttl=86400000,  # 24 hours
            dlq_max_length=5000,
        )
        assert config.dlq_message_ttl == 86400000
        assert config.dlq_max_length == 5000


class TestRabbitMQDLQQueueDeclarationWithOptions:
    """Tests for DLQ queue declaration with TTL and max_length (P2-001)."""

    @pytest.fixture
    def config_with_dlq_options(self) -> RabbitMQEventBusConfig:
        """Create test config with DLQ options."""
        return RabbitMQEventBusConfig(
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
            dlq_message_ttl=3600000,
            dlq_max_length=1000,
        )

    @pytest.fixture
    def config_without_dlq_options(self) -> RabbitMQEventBusConfig:
        """Create test config without DLQ options."""
        return RabbitMQEventBusConfig(
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_declared_with_ttl(
        self, mock_aio_pika: MagicMock, config_with_dlq_options: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ queue is declared with x-message-ttl when configured."""
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

        bus = RabbitMQEventBus(config=config_with_dlq_options)
        await bus.connect()

        # Find DLQ queue declaration call (first call)
        dlq_queue_call = mock_channel.declare_queue.call_args_list[0]
        assert dlq_queue_call.kwargs["name"] == config_with_dlq_options.dlq_queue_name
        assert "arguments" in dlq_queue_call.kwargs
        assert dlq_queue_call.kwargs["arguments"]["x-message-ttl"] == 3600000

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_declared_with_max_length(
        self, mock_aio_pika: MagicMock, config_with_dlq_options: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ queue is declared with x-max-length when configured."""
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

        bus = RabbitMQEventBus(config=config_with_dlq_options)
        await bus.connect()

        # Find DLQ queue declaration call (first call)
        dlq_queue_call = mock_channel.declare_queue.call_args_list[0]
        assert dlq_queue_call.kwargs["arguments"]["x-max-length"] == 1000

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_declared_without_arguments_when_options_not_set(
        self, mock_aio_pika: MagicMock, config_without_dlq_options: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ queue has no arguments when options are not set."""
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

        bus = RabbitMQEventBus(config=config_without_dlq_options)
        await bus.connect()

        # Find DLQ queue declaration call (first call)
        dlq_queue_call = mock_channel.declare_queue.call_args_list[0]
        # arguments should be None when no options are set
        assert dlq_queue_call.kwargs.get("arguments") is None


class TestRabbitMQDLQHelperEdgeCases:
    """Edge case tests for DLQ helper methods."""

    def test_get_death_count_invalid_count_type(self) -> None:
        """Test get_death_count handles non-integer count values."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {
            "x-death": [
                {"count": "not-a-number"},
                {"count": 5},
            ]
        }

        # Should only count valid integer values
        count = RabbitMQEventBus.get_death_count(message)
        assert count == 5

    def test_get_death_count_missing_count_field(self) -> None:
        """Test get_death_count handles death records without count field."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {
            "x-death": [
                {"reason": "rejected"},  # No count field
                {"count": 3},
            ]
        }

        count = RabbitMQEventBus.get_death_count(message)
        assert count == 3

    def test_get_original_routing_key_empty_routing_keys(self) -> None:
        """Test get_original_routing_key handles empty routing-keys list."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {
            "x-death": [
                {"routing-keys": []},
            ]
        }

        routing_key = RabbitMQEventBus.get_original_routing_key(message)
        assert routing_key is None

    def test_get_original_routing_key_no_routing_keys(self) -> None:
        """Test get_original_routing_key handles missing routing-keys field."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {
            "x-death": [
                {"count": 1, "reason": "rejected"},
            ]
        }

        routing_key = RabbitMQEventBus.get_original_routing_key(message)
        assert routing_key is None

    def test_x_death_non_list_type(self) -> None:
        """Test helper methods handle non-list x-death header."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {"x-death": "not-a-list"}

        assert RabbitMQEventBus.get_death_count(message) == 0
        assert RabbitMQEventBus.is_from_dlq(message) is False
        assert RabbitMQEventBus.get_original_routing_key(message) is None

    def test_x_death_non_dict_entries(self) -> None:
        """Test get_death_count handles non-dict entries in x-death list."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {
            "x-death": [
                "not-a-dict",
                {"count": 2},
                None,
            ]
        }

        count = RabbitMQEventBus.get_death_count(message)
        assert count == 2
