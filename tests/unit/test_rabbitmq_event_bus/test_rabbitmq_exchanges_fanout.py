"""Unit tests extracted for test_rabbitmq_exchanges_fanout.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig
from eventsource.domain.event import DomainEvent


class TestFanoutExchangeBroadcastBehavior:
    """Comprehensive tests for fanout exchange broadcast behavior."""

    @pytest.fixture
    def fanout_config(self) -> RabbitMQEventBusConfig:
        """Create configuration for fanout exchange testing."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="broadcast-events",
            exchange_type="fanout",
            consumer_group="notifications",
            enable_dlq=False,
        )

    def test_fanout_config_returns_empty_routing_key(
        self, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that fanout config correctly returns empty routing key."""
        assert fanout_config.get_effective_routing_key() == ""
        assert fanout_config.exchange_type == "fanout"

    def test_fanout_config_routing_key_pattern_override(self) -> None:
        """Test that explicit routing_key_pattern still works for fanout."""
        config = RabbitMQEventBusConfig(
            exchange_type="fanout",
            routing_key_pattern="custom.pattern",
        )
        assert config.get_effective_routing_key() == "custom.pattern"

    def test_fanout_config_case_insensitive(self) -> None:
        """Test that 'FANOUT' and 'Fanout' work the same as 'fanout'."""
        config_upper = RabbitMQEventBusConfig(exchange_type="FANOUT")
        config_mixed = RabbitMQEventBusConfig(exchange_type="Fanout")
        config_lower = RabbitMQEventBusConfig(exchange_type="fanout")

        assert config_upper.get_effective_routing_key() == ""
        assert config_mixed.get_effective_routing_key() == ""
        assert config_lower.get_effective_routing_key() == ""

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_exchange_declared_with_correct_type(
        self, mock_aio_pika: MagicMock, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that fanout exchange is declared with ExchangeType.FANOUT."""
        mock_connection = AsyncMock(is_closed=False)
        mock_channel = AsyncMock(is_closed=False)
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        from aio_pika import ExchangeType

        bus = RabbitMQEventBus(config=fanout_config)
        await bus.connect()

        mock_channel.declare_exchange.assert_called()
        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["type"] == ExchangeType.FANOUT
        assert call_kwargs["name"] == "broadcast-events"
        assert call_kwargs["durable"] is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_queue_bound_with_empty_routing_key(
        self, mock_aio_pika: MagicMock, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that queue is bound with empty routing key for fanout."""
        mock_connection = AsyncMock(is_closed=False)
        mock_channel = AsyncMock(is_closed=False)
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=fanout_config)
        await bus.connect()

        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == ""
        assert call_kwargs["exchange"] == mock_exchange

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_publish_includes_routing_key_for_logging(
        self, mock_aio_pika: MagicMock, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish still generates routing key for logging/tracing."""
        mock_connection = AsyncMock(is_closed=False)
        mock_channel = AsyncMock(is_closed=False)
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_exchange.publish = AsyncMock()

        class NotificationSent(DomainEvent):
            aggregate_type: str = "Notification"
            message: str = "test"

        event = NotificationSent(aggregate_id=uuid4(), message="Hello!")

        bus = RabbitMQEventBus(config=fanout_config)
        await bus.connect()
        await bus.publish([event])

        mock_exchange.publish.assert_called_once()
        call_kwargs = mock_exchange.publish.call_args.kwargs
        assert call_kwargs["routing_key"] == "Notification.NotificationSent"


class TestFanoutExchangeMultipleConsumerGroups:
    """Tests for multiple consumer groups with fanout exchange."""

    def test_different_consumer_groups_have_different_queue_names(self) -> None:
        """Test that different consumer groups create different queue names."""
        config1 = RabbitMQEventBusConfig(
            exchange_name="broadcast", exchange_type="fanout", consumer_group="notifications"
        )
        config2 = RabbitMQEventBusConfig(
            exchange_name="broadcast", exchange_type="fanout", consumer_group="audit-log"
        )
        config3 = RabbitMQEventBusConfig(
            exchange_name="broadcast", exchange_type="fanout", consumer_group="cache-invalidation"
        )

        assert config1.exchange_name == config2.exchange_name == config3.exchange_name
        assert config1.queue_name == "broadcast.notifications"
        assert config2.queue_name == "broadcast.audit-log"
        assert config3.queue_name == "broadcast.cache-invalidation"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_multiple_buses_same_fanout_exchange(self, mock_aio_pika: MagicMock) -> None:
        """Test that multiple buses can connect to the same fanout exchange."""
        mock_connection = AsyncMock(is_closed=False)
        mock_channel1 = AsyncMock(is_closed=False)
        mock_channel2 = AsyncMock(is_closed=False)
        mock_exchange = AsyncMock()
        mock_queue1 = AsyncMock()
        mock_queue2 = AsyncMock()

        call_count = {"channel": 0, "queue": 0}

        def get_channel() -> AsyncMock:
            call_count["channel"] += 1
            return mock_channel1 if call_count["channel"] == 1 else mock_channel2

        def get_queue(name: str, **kwargs) -> AsyncMock:
            call_count["queue"] += 1
            return mock_queue1 if call_count["queue"] == 1 else mock_queue2

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(side_effect=get_channel)
        mock_channel1.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel2.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel1.declare_queue = AsyncMock(side_effect=get_queue)
        mock_channel2.declare_queue = AsyncMock(side_effect=get_queue)

        config1 = RabbitMQEventBusConfig(
            exchange_name="system-events",
            exchange_type="fanout",
            consumer_group="email-service",
            enable_dlq=False,
        )
        config2 = RabbitMQEventBusConfig(
            exchange_name="system-events",
            exchange_type="fanout",
            consumer_group="sms-service",
            enable_dlq=False,
        )

        bus1 = RabbitMQEventBus(config=config1)
        bus2 = RabbitMQEventBus(config=config2)

        await bus1.connect()
        await bus2.connect()

        assert bus1._connection_manager._connected
        assert bus2._connection_manager._connected

        queue_declare_calls = [
            mock_channel1.declare_queue.call_args,
            mock_channel2.declare_queue.call_args,
        ]
        queue_names = {call.kwargs.get("name") for call in queue_declare_calls if call}
        assert "system-events.email-service" in queue_names
        assert "system-events.sms-service" in queue_names


class TestFanoutExchangeUseCases:
    """Tests demonstrating fanout exchange use cases."""

    def test_notification_broadcast_config(self) -> None:
        """Test configuration for notification broadcast use case."""
        config = RabbitMQEventBusConfig(
            exchange_name="notifications",
            exchange_type="fanout",
            consumer_group="email-handler",
            durable=True,
            auto_delete=False,
        )
        assert config.exchange_type == "fanout"
        assert config.get_effective_routing_key() == ""
        assert config.queue_name == "notifications.email-handler"

    def test_audit_log_broadcast_config(self) -> None:
        """Test configuration for audit logging use case."""
        config = RabbitMQEventBusConfig(
            exchange_name="domain-events",
            exchange_type="fanout",
            consumer_group="audit-logger",
            durable=True,
            prefetch_count=50,
        )
        assert config.exchange_type == "fanout"
        assert config.queue_name == "domain-events.audit-logger"

    def test_cache_invalidation_broadcast_config(self) -> None:
        """Test configuration for cache invalidation use case."""
        config = RabbitMQEventBusConfig(
            exchange_name="cache-invalidation",
            exchange_type="fanout",
            consumer_group="frontend-cache",
            durable=False,
            auto_delete=True,
        )
        assert config.exchange_type == "fanout"
        assert config.durable is False
        assert config.auto_delete is True

    def test_development_monitoring_config(self) -> None:
        """Test configuration for development event monitoring."""
        config = RabbitMQEventBusConfig(
            exchange_name="all-events",
            exchange_type="fanout",
            consumer_group="dev-monitor",
            durable=False,
            auto_delete=True,
            enable_dlq=False,
        )
        assert config.exchange_type == "fanout"
        assert config.enable_dlq is False
        assert config.auto_delete is True
