"""Unit tests extracted for test_rabbitmq_exchanges_fanout.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig
from eventsource.domain.event import DomainEvent


def _setup_mock_aio_pika(mock_aio_pika: MagicMock) -> tuple[AsyncMock, AsyncMock, AsyncMock]:
    mock_connection, mock_channel = AsyncMock(is_closed=False), AsyncMock(is_closed=False)
    mock_exchange, mock_queue = AsyncMock(), AsyncMock()
    mock_queue.declaration_result.message_count = 0
    mock_queue.declaration_result.consumer_count = 0
    mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
    mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
    mock_exchange.publish = AsyncMock()
    return mock_channel, mock_exchange, mock_queue


def _fanout_cfg(exchange: str, group: str, **kwargs: object) -> RabbitMQEventBusConfig:
    return RabbitMQEventBusConfig(
        exchange_name=exchange, exchange_type="fanout", consumer_group=group, **kwargs
    )


class TestFanoutExchangeBroadcastBehavior:
    @pytest.fixture
    def fanout_config(self) -> RabbitMQEventBusConfig:
        return _fanout_cfg(
            "broadcast-events",
            "notifications",
            rabbitmq_url="amqp://test:test@localhost/",
            enable_dlq=False,
        )

    def test_fanout_config_returns_empty_routing_key(
        self, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        assert fanout_config.get_effective_routing_key() == ""
        assert fanout_config.exchange_type == "fanout"

    def test_fanout_config_routing_key_pattern_override(self) -> None:
        config = RabbitMQEventBusConfig(
            exchange_type="fanout", routing_key_pattern="custom.pattern"
        )
        assert config.get_effective_routing_key() == "custom.pattern"

    def test_fanout_config_case_insensitive(self) -> None:
        for exchange_type in ("FANOUT", "Fanout", "fanout"):
            cfg = RabbitMQEventBusConfig(exchange_type=exchange_type)
            assert cfg.get_effective_routing_key() == ""

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_exchange_declared_with_correct_type(
        self, mock_aio_pika: MagicMock, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        from aio_pika import ExchangeType

        mock_channel, _, _ = _setup_mock_aio_pika(mock_aio_pika)
        await RabbitMQEventBus(config=fanout_config).connect()

        mock_channel.declare_exchange.assert_called()
        kw = mock_channel.declare_exchange.call_args.kwargs
        assert kw["type"] == ExchangeType.FANOUT
        assert (kw["name"], kw["durable"]) == ("broadcast-events", True)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_queue_bound_with_empty_routing_key(
        self, mock_aio_pika: MagicMock, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        _, mock_exchange, mock_queue = _setup_mock_aio_pika(mock_aio_pika)
        await RabbitMQEventBus(config=fanout_config).connect()

        mock_queue.bind.assert_called_once()
        kw = mock_queue.bind.call_args.kwargs
        assert (kw["routing_key"], kw["exchange"]) == ("", mock_exchange)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_publish_includes_routing_key_for_logging(
        self, mock_aio_pika: MagicMock, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        _, mock_exchange, _ = _setup_mock_aio_pika(mock_aio_pika)

        class NotificationSent(DomainEvent):
            aggregate_type: str = "Notification"
            message: str = "test"

        bus = RabbitMQEventBus(config=fanout_config)
        await bus.connect()
        await bus.publish([NotificationSent(aggregate_id=uuid4(), message="Hello!")])

        mock_exchange.publish.assert_called_once()
        kw = mock_exchange.publish.call_args.kwargs
        assert kw["routing_key"] == "Notification.NotificationSent"


class TestFanoutExchangeMultipleConsumerGroups:
    def test_different_consumer_groups_have_different_queue_names(self) -> None:
        c1 = _fanout_cfg("broadcast", "notifications")
        c2 = _fanout_cfg("broadcast", "audit-log")
        c3 = _fanout_cfg("broadcast", "cache-invalidation")
        assert c1.exchange_name == c2.exchange_name == c3.exchange_name
        assert c1.queue_name == "broadcast.notifications"
        assert c2.queue_name == "broadcast.audit-log"
        assert c3.queue_name == "broadcast.cache-invalidation"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_multiple_buses_same_fanout_exchange(self, mock_aio_pika: MagicMock) -> None:
        mock_connection = AsyncMock(is_closed=False)
        ch1, ch2 = AsyncMock(is_closed=False), AsyncMock(is_closed=False)
        mock_exchange = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(side_effect=[ch1, ch2])
        ch1.declare_exchange = AsyncMock(return_value=mock_exchange)
        ch2.declare_exchange = AsyncMock(return_value=mock_exchange)
        ch1.declare_queue = AsyncMock(return_value=AsyncMock())
        ch2.declare_queue = AsyncMock(return_value=AsyncMock())

        c1 = _fanout_cfg("system-events", "email-service", enable_dlq=False)
        c2 = _fanout_cfg("system-events", "sms-service", enable_dlq=False)
        bus1, bus2 = RabbitMQEventBus(config=c1), RabbitMQEventBus(config=c2)
        await bus1.connect()
        await bus2.connect()

        assert bus1._connection_manager._connected and bus2._connection_manager._connected
        calls = (ch1.declare_queue.call_args, ch2.declare_queue.call_args)
        queue_names = {call.kwargs.get("name") for call in calls if call}
        assert queue_names == {"system-events.email-service", "system-events.sms-service"}


class TestFanoutExchangeUseCases:
    def test_notification_broadcast_config(self) -> None:
        cfg = _fanout_cfg("notifications", "email-handler", durable=True, auto_delete=False)
        assert cfg.exchange_type == "fanout"
        assert cfg.get_effective_routing_key() == ""
        assert cfg.queue_name == "notifications.email-handler"

    def test_audit_log_broadcast_config(self) -> None:
        cfg = _fanout_cfg("domain-events", "audit-logger", durable=True, prefetch_count=50)
        assert (cfg.exchange_type, cfg.queue_name) == ("fanout", "domain-events.audit-logger")

    def test_cache_invalidation_broadcast_config(self) -> None:
        cfg = _fanout_cfg("cache-invalidation", "frontend-cache", durable=False, auto_delete=True)
        assert (cfg.exchange_type, cfg.durable, cfg.auto_delete) == ("fanout", False, True)

    def test_development_monitoring_config(self) -> None:
        cfg = _fanout_cfg(
            "all-events", "dev-monitor", durable=False, auto_delete=True, enable_dlq=False
        )
        assert (cfg.exchange_type, cfg.enable_dlq, cfg.auto_delete) == ("fanout", False, True)
