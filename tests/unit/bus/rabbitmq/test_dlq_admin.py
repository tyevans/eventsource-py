"""Tests for RabbitMQDLQAdmin.

Extracted from ``RabbitMQEventBus`` (bus.py) as part of the bus god-class
decomposition (Task 8). These tests exercise the collaborator directly
(mocked connection/topology), independent of the facade. Facade-level
white-box tests for the delegating methods remain in
tests/unit/test_rabbitmq_event_bus.py.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from eventsource.bus.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.bus.rabbitmq.dlq import RabbitMQDLQAdmin
from eventsource.bus.rabbitmq.models import DLQMessage, RabbitMQEventBusStats


def make_config(*, enable_dlq: bool = True) -> RabbitMQEventBusConfig:
    return RabbitMQEventBusConfig(
        rabbitmq_url="amqp://test:test@localhost/",
        exchange_name="test-events",
        consumer_group="test-group",
        enable_dlq=enable_dlq,
    )


def make_admin(
    *,
    config: RabbitMQEventBusConfig | None = None,
    connected: bool = True,
    channel: AsyncMock | None = None,
    exchange: AsyncMock | None = None,
) -> tuple[RabbitMQDLQAdmin, MagicMock, MagicMock]:
    """Build a RabbitMQDLQAdmin with mocked connection/topology collaborators."""
    cfg = config or make_config()

    connection = MagicMock()
    connection._connected = connected
    connection.channel = channel
    connection.require_channel.return_value = channel

    topology = MagicMock()
    topology.exchange = exchange

    admin = RabbitMQDLQAdmin(
        config=cfg,
        connection=connection,
        topology=topology,
        stats=RabbitMQEventBusStats(),
    )
    return admin, connection, topology


class TestGetMessages:
    async def test_returns_empty_when_not_connected(self) -> None:
        admin, _, _ = make_admin(connected=False, channel=AsyncMock())

        result = await admin.get_messages(limit=10)

        assert result == []

    async def test_returns_empty_when_dlq_disabled(self) -> None:
        admin, _, _ = make_admin(config=make_config(enable_dlq=False), channel=AsyncMock())

        result = await admin.get_messages(limit=10)

        assert result == []

    async def test_returns_empty_when_channel_not_initialized(self) -> None:
        admin, _, _ = make_admin(channel=None)

        result = await admin.get_messages(limit=10)

        assert result == []

    async def test_returns_dlq_messages_and_honors_limit(self) -> None:
        mock_channel = AsyncMock()
        mock_queue = AsyncMock()

        messages = []
        for i in range(5):
            msg = AsyncMock()
            msg.message_id = f"msg-{i}"
            msg.routing_key = "Test.Event"
            msg.body = b'{"data": "test"}'
            msg.headers = {
                "event_type": "TestEvent",
                "x-dlq-reason": "Handler error",
            }
            messages.append(msg)

        mock_queue.get.side_effect = messages + [None]
        mock_channel.get_queue.return_value = mock_queue

        admin, _, _ = make_admin(channel=mock_channel)

        result = await admin.get_messages(limit=3)

        assert len(result) == 3
        assert all(isinstance(m, DLQMessage) for m in result)
        assert result[0].message_id == "msg-0"
        assert result[0].dlq_reason == "Handler error"
        # Non-destructive read: messages rejected with requeue
        messages[0].reject.assert_called_once_with(requeue=True)

    async def test_handles_exception(self) -> None:
        mock_channel = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("Queue not found")

        admin, _, _ = make_admin(channel=mock_channel)

        result = await admin.get_messages(limit=10)

        assert result == []


class TestGetMessageCount:
    async def test_returns_declared_message_count(self) -> None:
        mock_channel = AsyncMock()
        queue_info = AsyncMock()
        queue_info.declaration_result.message_count = 7
        mock_channel.declare_queue.return_value = queue_info

        admin, _, _ = make_admin(channel=mock_channel)

        result = await admin.get_message_count()

        assert result == 7

    async def test_returns_zero_when_not_connected(self) -> None:
        admin, _, _ = make_admin(connected=False, channel=AsyncMock())

        result = await admin.get_message_count()

        assert result == 0

    async def test_returns_zero_when_dlq_disabled(self) -> None:
        admin, _, _ = make_admin(config=make_config(enable_dlq=False), channel=AsyncMock())

        result = await admin.get_message_count()

        assert result == 0

    async def test_returns_zero_when_channel_not_initialized(self) -> None:
        admin, _, _ = make_admin(channel=None)

        result = await admin.get_message_count()

        assert result == 0

    async def test_handles_exception(self) -> None:
        mock_channel = AsyncMock()
        mock_channel.declare_queue.side_effect = Exception("boom")

        admin, _, _ = make_admin(channel=mock_channel)

        result = await admin.get_message_count()

        assert result == 0


class TestPurge:
    async def test_purge_calls_queue_purge_and_returns_count(self) -> None:
        mock_channel = AsyncMock()
        mock_queue = AsyncMock()
        purge_result = AsyncMock()
        purge_result.message_count = 15
        mock_queue.purge.return_value = purge_result
        mock_channel.get_queue.return_value = mock_queue

        admin, _, _ = make_admin(channel=mock_channel)

        result = await admin.purge()

        assert result == 15
        mock_queue.purge.assert_called_once()

    async def test_returns_zero_when_not_connected(self) -> None:
        admin, _, _ = make_admin(connected=False, channel=AsyncMock())

        result = await admin.purge()

        assert result == 0

    async def test_returns_zero_when_channel_not_initialized(self) -> None:
        admin, _, _ = make_admin(channel=None)

        result = await admin.purge()

        assert result == 0

    async def test_handles_exception(self) -> None:
        mock_channel = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("boom")

        admin, _, _ = make_admin(channel=mock_channel)

        result = await admin.purge()

        assert result == 0


class TestReplayMessage:
    async def test_republishes_to_main_exchange_and_acks_dlq_message(self) -> None:
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.message_id = "target-id"
        mock_message.routing_key = "dlq-routing-key"
        mock_message.body = b'{"data": "test"}'
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.headers = {
            "event_type": "TestEvent",
            "x-dlq-reason": "Error",
            "x-original-routing-key": "Test.TestEvent",
        }

        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue

        admin, _, _ = make_admin(channel=mock_channel, exchange=mock_exchange)

        result = await admin.replay_message("target-id")

        assert result is True
        mock_exchange.publish.assert_called_once()
        publish_call = mock_exchange.publish.call_args
        assert publish_call.kwargs["routing_key"] == "Test.TestEvent"
        mock_message.ack.assert_called_once()

    async def test_returns_false_when_message_not_found(self) -> None:
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.message_id = "other-id"
        mock_message.routing_key = "key"
        mock_message.body = b"{}"
        mock_message.headers = {}

        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue

        admin, _, _ = make_admin(channel=mock_channel, exchange=mock_exchange)

        result = await admin.replay_message("non-existent-id")

        assert result is False
        mock_message.reject.assert_called_once_with(requeue=True)

    async def test_returns_false_when_not_connected(self) -> None:
        admin, _, _ = make_admin(connected=False, channel=AsyncMock(), exchange=AsyncMock())

        result = await admin.replay_message("test-id")

        assert result is False

    async def test_returns_false_when_exchange_not_initialized(self) -> None:
        admin, _, _ = make_admin(channel=AsyncMock(), exchange=None)

        result = await admin.replay_message("test-id")

        assert result is False

    async def test_returns_false_when_channel_not_initialized(self) -> None:
        admin, _, _ = make_admin(channel=None, exchange=AsyncMock())

        result = await admin.replay_message("test-id")

        assert result is False

    async def test_handles_exception(self) -> None:
        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("boom")

        admin, _, _ = make_admin(channel=mock_channel, exchange=mock_exchange)

        result = await admin.replay_message("test-id")

        assert result is False


class TestReplayMessageHelper:
    async def test_raises_when_exchange_not_initialized(self) -> None:
        admin, _, _ = make_admin(channel=AsyncMock(), exchange=None)
        mock_message = AsyncMock()

        with pytest.raises(RuntimeError, match="Exchange not initialized"):
            await admin._replay_message(mock_message)

    async def test_removes_dlq_headers_and_resets_retry_count(self) -> None:
        mock_exchange = AsyncMock()
        admin, _, _ = make_admin(channel=AsyncMock(), exchange=mock_exchange)

        mock_message = AsyncMock()
        mock_message.body = b'{"data": "test"}'
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "dlq-key"
        mock_message.headers = {
            "event_type": "TestEvent",
            "x-dlq-reason": "Error",
            "x-death": [{"queue": "test"}],
            "x-retry-count": 5,
        }

        await admin._replay_message(mock_message)

        publish_call = mock_exchange.publish.call_args
        replay_msg = publish_call[0][0]

        assert "x-dlq-reason" not in replay_msg.headers
        assert "x-death" not in replay_msg.headers
        assert replay_msg.headers["x-retry-count"] == 0
        assert "x-replayed-from-dlq" in replay_msg.headers
        assert replay_msg.headers["event_type"] == "TestEvent"
