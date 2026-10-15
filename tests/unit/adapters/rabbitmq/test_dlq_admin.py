"""Tests for RabbitMQDLQAdmin collaborator (extracted from RabbitMQEventBus)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.dlq import RabbitMQDLQAdmin
from eventsource.adapters.rabbitmq.models import DLQMessage, RabbitMQEventBusStats


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
    cfg = config or make_config()
    connection = MagicMock(_connected=connected, channel=channel)
    connection.require_channel.return_value = channel
    topology = MagicMock(exchange=exchange)
    admin = RabbitMQDLQAdmin(
        config=cfg, connection=connection, topology=topology, stats=RabbitMQEventBusStats()
    )
    return admin, connection, topology


class TestGetMessages:
    async def test_returns_empty_when_not_connected(self) -> None:
        admin, _, _ = make_admin(connected=False, channel=AsyncMock())
        assert await admin.get_messages(limit=10) == []

    async def test_returns_empty_when_dlq_disabled(self) -> None:
        admin, _, _ = make_admin(config=make_config(enable_dlq=False), channel=AsyncMock())
        assert await admin.get_messages(limit=10) == []

    async def test_returns_empty_when_channel_not_initialized(self) -> None:
        assert await make_admin(channel=None)[0].get_messages(limit=10) == []

    async def test_returns_dlq_messages_and_honors_limit(self) -> None:
        mock_channel, mock_queue = AsyncMock(), AsyncMock()
        hdrs = {"event_type": "TestEvent", "x-dlq-reason": "Handler error"}
        body = b'{"data": "test"}'
        messages = [
            AsyncMock(message_id=f"msg-{i}", routing_key="Test.Event", body=body, headers=hdrs)
            for i in range(5)
        ]
        mock_queue.get.side_effect = messages + [None]
        mock_channel.get_queue.return_value = mock_queue
        result = await make_admin(channel=mock_channel)[0].get_messages(limit=3)
        assert len(result) == 3 and all(isinstance(m, DLQMessage) for m in result)
        assert (result[0].message_id, result[0].dlq_reason) == ("msg-0", "Handler error")
        messages[0].reject.assert_called_once_with(requeue=True)

    async def test_handles_exception(self) -> None:
        mock_channel = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("Queue not found")
        assert await make_admin(channel=mock_channel)[0].get_messages(limit=10) == []


class TestGetMessageCount:
    async def test_returns_declared_message_count(self) -> None:
        mock_channel, queue_info = AsyncMock(), AsyncMock()
        queue_info.declaration_result.message_count = 7
        mock_channel.declare_queue.return_value = queue_info
        assert await make_admin(channel=mock_channel)[0].get_message_count() == 7

    async def test_returns_zero_when_not_connected(self) -> None:
        assert await make_admin(connected=False, channel=AsyncMock())[0].get_message_count() == 0

    async def test_returns_zero_when_dlq_disabled(self) -> None:
        cfg = make_config(enable_dlq=False)
        assert await make_admin(config=cfg, channel=AsyncMock())[0].get_message_count() == 0

    async def test_returns_zero_when_channel_not_initialized(self) -> None:
        assert await make_admin(channel=None)[0].get_message_count() == 0

    async def test_handles_exception(self) -> None:
        mock_channel = AsyncMock()
        mock_channel.declare_queue.side_effect = Exception("boom")
        assert await make_admin(channel=mock_channel)[0].get_message_count() == 0


class TestPurge:
    async def test_purge_calls_queue_purge_and_returns_count(self) -> None:
        mock_channel, mock_queue, purge_result = AsyncMock(), AsyncMock(), AsyncMock()
        purge_result.message_count = 15
        mock_queue.purge.return_value = purge_result
        mock_channel.get_queue.return_value = mock_queue
        assert await make_admin(channel=mock_channel)[0].purge() == 15
        mock_queue.purge.assert_called_once()

    async def test_returns_zero_when_not_connected(self) -> None:
        assert await make_admin(connected=False, channel=AsyncMock())[0].purge() == 0

    async def test_returns_zero_when_channel_not_initialized(self) -> None:
        assert await make_admin(channel=None)[0].purge() == 0

    async def test_handles_exception(self) -> None:
        mock_channel = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("boom")
        assert await make_admin(channel=mock_channel)[0].purge() == 0


class TestReplayMessage:
    async def test_republishes_to_main_exchange_and_acks_dlq_message(self) -> None:
        mock_channel, mock_exchange, mock_queue = AsyncMock(), AsyncMock(), AsyncMock()
        hdrs = {
            "event_type": "TestEvent",
            "x-dlq-reason": "Error",
            "x-original-routing-key": "Test.TestEvent",
        }
        mock_message = AsyncMock(
            message_id="target-id", routing_key="dlq-routing-key", body=b'{"data": "test"}',
            content_type="application/json", content_encoding="utf-8", headers=hdrs,
        )  # fmt: skip
        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue
        admin, _, _ = make_admin(channel=mock_channel, exchange=mock_exchange)
        assert await admin.replay_message("target-id") is True
        mock_exchange.publish.assert_called_once()
        assert mock_exchange.publish.call_args.kwargs["routing_key"] == "Test.TestEvent"
        mock_message.ack.assert_called_once()

    async def test_returns_false_when_message_not_found(self) -> None:
        mock_channel, mock_exchange, mock_queue = AsyncMock(), AsyncMock(), AsyncMock()
        mock_message = AsyncMock(message_id="other-id", routing_key="key", body=b"{}", headers={})
        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue
        admin, _, _ = make_admin(channel=mock_channel, exchange=mock_exchange)
        assert await admin.replay_message("non-existent-id") is False
        mock_message.reject.assert_called_once_with(requeue=True)

    async def test_returns_false_when_not_connected(self) -> None:
        admin, _, _ = make_admin(connected=False, channel=AsyncMock(), exchange=AsyncMock())
        assert await admin.replay_message("test-id") is False

    async def test_returns_false_when_exchange_not_initialized(self) -> None:
        admin, _, _ = make_admin(channel=AsyncMock(), exchange=None)
        assert await admin.replay_message("test-id") is False

    async def test_returns_false_when_channel_not_initialized(self) -> None:
        admin, _, _ = make_admin(channel=None, exchange=AsyncMock())
        assert await admin.replay_message("test-id") is False

    async def test_handles_exception(self) -> None:
        mock_channel, mock_exchange = AsyncMock(), AsyncMock()
        mock_channel.get_queue.side_effect = Exception("boom")
        admin, _, _ = make_admin(channel=mock_channel, exchange=mock_exchange)
        assert await admin.replay_message("test-id") is False


class TestReplayMessageHelper:
    async def test_raises_when_exchange_not_initialized(self) -> None:
        admin, _, _ = make_admin(channel=AsyncMock(), exchange=None)
        with pytest.raises(RuntimeError, match="Exchange not initialized"):
            await admin._replay_message(AsyncMock())

    async def test_removes_dlq_headers_and_resets_retry_count(self) -> None:
        mock_exchange = AsyncMock()
        admin, _, _ = make_admin(channel=AsyncMock(), exchange=mock_exchange)
        hdrs = {
            "event_type": "TestEvent",
            "x-dlq-reason": "Error",
            "x-death": [{"queue": "test"}],
            "x-retry-count": 5,
        }
        msg = AsyncMock(
            body=b'{"data": "test"}', content_type="application/json", content_encoding="utf-8",
            message_id="test-id", routing_key="dlq-key", headers=hdrs,
        )  # fmt: skip
        await admin._replay_message(msg)
        replay_hdrs = mock_exchange.publish.call_args[0][0].headers
        assert "x-dlq-reason" not in replay_hdrs and "x-death" not in replay_hdrs
        assert (replay_hdrs["x-retry-count"], replay_hdrs["event_type"]) == (0, "TestEvent")
        assert "x-replayed-from-dlq" in replay_hdrs
