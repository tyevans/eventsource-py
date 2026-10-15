"""Unit tests extracted for test_rabbitmq_exchanges_fanout.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from eventsource.domain.event import DomainEvent


class TestFanoutExchangeBroadcastBehavior:
    """Comprehensive tests for fanout exchange broadcast behavior.

    Fanout exchanges broadcast all messages to all bound queues, regardless
    of routing key. These tests validate:
    - Routing key is ignored for fanout exchanges
    - Multiple consumer groups can receive the same broadcast
    - Configuration is correct for fanout use cases
    """

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
        """Test that explicit routing_key_pattern still works for fanout.

        Even though fanout ignores routing keys, explicit pattern should
        still be used if configured (for logging/tracing purposes).
        """
        config = RabbitMQEventBusConfig(
            exchange_type="fanout",
            routing_key_pattern="custom.pattern",
        )
        # Explicit pattern takes precedence
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

        from aio_pika import ExchangeType

        bus = RabbitMQEventBus(config=fanout_config)
        await bus.connect()

        # Verify exchange was declared with FANOUT type
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

        bus = RabbitMQEventBus(config=fanout_config)
        await bus.connect()

        # Verify queue was bound with empty routing key
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == ""
        assert call_kwargs["exchange"] == mock_exchange

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_publish_includes_routing_key_for_logging(
        self, mock_aio_pika: MagicMock, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish still generates routing key for logging/tracing.

        Even though fanout exchanges ignore routing keys, we still generate
        them for observability purposes (logging, tracing, debugging).
        """
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
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

        # Routing key is still generated and passed (for logging)
        # Fanout exchange ignores it, but it's included for observability
        mock_exchange.publish.assert_called_once()
        call_kwargs = mock_exchange.publish.call_args.kwargs
        assert call_kwargs["routing_key"] == "Notification.NotificationSent"


class TestFanoutExchangeMultipleConsumerGroups:
    """Tests for multiple consumer groups with fanout exchange.

    With fanout exchanges, each consumer group gets its own queue, and all
    queues bound to the exchange receive all messages (broadcast pattern).
    """

    def test_different_consumer_groups_have_different_queue_names(self) -> None:
        """Test that different consumer groups create different queue names."""
        config1 = RabbitMQEventBusConfig(
            exchange_name="broadcast",
            exchange_type="fanout",
            consumer_group="notifications",
        )
        config2 = RabbitMQEventBusConfig(
            exchange_name="broadcast",
            exchange_type="fanout",
            consumer_group="audit-log",
        )
        config3 = RabbitMQEventBusConfig(
            exchange_name="broadcast",
            exchange_type="fanout",
            consumer_group="cache-invalidation",
        )

        # Same exchange, different queues
        assert config1.exchange_name == config2.exchange_name == config3.exchange_name
        assert config1.queue_name == "broadcast.notifications"
        assert config2.queue_name == "broadcast.audit-log"
        assert config3.queue_name == "broadcast.cache-invalidation"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_multiple_buses_same_fanout_exchange(self, mock_aio_pika: MagicMock) -> None:
        """Test that multiple buses can connect to the same fanout exchange.

        This simulates the pattern where multiple consumer groups (services)
        each receive all broadcast messages independently.
        """
        # Create shared mock connection infrastructure
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel1 = AsyncMock()
        mock_channel1.is_closed = False
        mock_channel2 = AsyncMock()
        mock_channel2.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue1 = AsyncMock()
        mock_queue2 = AsyncMock()

        # Track which channel/queue is being used
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

        # Configure two consumer groups on the same fanout exchange
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

        # Both should connect successfully
        assert bus1._connection_manager._connected
        assert bus2._connection_manager._connected

        # Verify different queue names were declared
        queue_declare_calls = [
            mock_channel1.declare_queue.call_args,
            mock_channel2.declare_queue.call_args,
        ]
        queue_names = {call.kwargs.get("name") for call in queue_declare_calls if call}
        assert "system-events.email-service" in queue_names
        assert "system-events.sms-service" in queue_names


class TestFanoutExchangeUseCases:
    """Tests demonstrating fanout exchange use cases.

    Fanout exchanges are ideal for:
    1. Notifications - broadcast to all notification handlers
    2. Logging/Audit - all services receive all events
    3. Cache Invalidation - broadcast cache clear commands
    4. Development/Debug - monitor all events during development
    """

    def test_notification_broadcast_config(self) -> None:
        """Test configuration for notification broadcast use case."""
        config = RabbitMQEventBusConfig(
            exchange_name="notifications",
            exchange_type="fanout",
            consumer_group="email-handler",
            durable=True,  # Survive restarts
            auto_delete=False,  # Don't delete on disconnect
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
            prefetch_count=50,  # Higher throughput for logging
        )

        assert config.exchange_type == "fanout"
        assert config.queue_name == "domain-events.audit-logger"

    def test_cache_invalidation_broadcast_config(self) -> None:
        """Test configuration for cache invalidation use case."""
        config = RabbitMQEventBusConfig(
            exchange_name="cache-invalidation",
            exchange_type="fanout",
            consumer_group="frontend-cache",
            durable=False,  # Cache invalidation can be transient
            auto_delete=True,  # OK to lose messages if consumer disconnects
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
            durable=False,  # Don't need durability for dev
            auto_delete=True,  # Clean up when dev session ends
            enable_dlq=False,  # No need for DLQ in dev
        )

        assert config.exchange_type == "fanout"
        assert config.enable_dlq is False
        assert config.auto_delete is True
