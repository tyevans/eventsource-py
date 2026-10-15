"""Message processing, deserialization, DLQ, and pending recovery tests for RedisEventBus."""

import asyncio
import contextlib
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from eventsource.adapters.redis.bus import RedisEventBus
from eventsource.domain.event_registry import EventRegistry

from ._fixtures import (
    FailingHandler,
    OrderHandler,
    SampleOrderCreated,
    bus,
    config,
    event_registry,
    mock_redis,
)

__all__ = [
    "TestRedisEventBusDLQ",
    "TestRedisEventBusDeserialization",
    "TestRedisEventBusMessageProcessing",
    "TestRedisEventBusPendingRecovery",
    "bus",
    "config",
    "event_registry",
    "mock_redis",
]


def _order_msg(event: SampleOrderCreated) -> dict[str, str]:
    return {
        "event_id": str(event.event_id),
        "event_type": "SampleOrderCreated",
        "aggregate_id": str(event.aggregate_id),
        "aggregate_type": "Order",
        "occurred_at": datetime.now(UTC).isoformat(),
        "payload": event.model_dump_json(),
    }


def _new_order(agg_id=None) -> SampleOrderCreated:
    return SampleOrderCreated(
        aggregate_id=agg_id or uuid4(), order_number="ORD-001", customer_id=uuid4()
    )


def _pending_entry(delivered: int = 1, idle_ms: int = 120000) -> list[dict[str, object]]:
    return [
        {
            "message_id": "msg-123",
            "consumer": "old-consumer",
            "time_since_delivered": idle_ms,
            "times_delivered": delivered,
        }
    ]


class TestRedisEventBusDeserialization:
    async def test_deserialize_known_event(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        aggregate_id = uuid4()
        event = _new_order(aggregate_id)
        msg = {"event_type": "SampleOrderCreated", "payload": event.model_dump_json()}
        result = bus._deserialize_event("SampleOrderCreated", msg)
        assert result is not None
        assert (result.event_type, result.order_number, result.aggregate_id) == (
            "SampleOrderCreated",
            "ORD-001",
            aggregate_id,
        )

    async def test_deserialize_unknown_event(
        self, bus: RedisEventBus, event_registry: EventRegistry
    ) -> None:
        msg = {"event_type": "UnknownEvent", "payload": "{}"}
        assert bus._deserialize_event("UnknownEvent", msg) is None


class TestRedisEventBusMessageProcessing:
    async def test_process_message_success(
        self, bus: RedisEventBus, event_registry: EventRegistry, mock_redis: AsyncMock
    ) -> None:
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)
        await bus._process_message("msg-123", _order_msg(_new_order()), "test_consumer")
        assert len(handler.handled_events) == 1
        mock_redis.xack.assert_called_once()

    async def test_process_message_unknown_event(
        self, bus: RedisEventBus, mock_redis: AsyncMock
    ) -> None:
        msg = {
            "event_id": str(uuid4()),
            "event_type": "UnknownEvent",
            "aggregate_id": str(uuid4()),
            "aggregate_type": "Unknown",
            "occurred_at": datetime.now(UTC).isoformat(),
            "payload": "{}",
        }
        await bus._process_message("msg-123", msg, "test_consumer")
        mock_redis.xack.assert_called_once()

    async def test_process_message_handler_failure(
        self, bus: RedisEventBus, event_registry: EventRegistry, mock_redis: AsyncMock
    ) -> None:
        bus.subscribe(SampleOrderCreated, FailingHandler())
        await bus._process_message("msg-123", _order_msg(_new_order()), "test_consumer")
        mock_redis.xack.assert_not_called()


class TestRedisEventBusDLQ:
    async def test_send_to_dlq(self, bus: RedisEventBus, mock_redis: AsyncMock) -> None:
        msg = {"event_id": str(uuid4()), "event_type": "SampleOrderCreated", "payload": "{}"}
        await bus._send_to_dlq("msg-123", msg, 3)
        mock_redis.xadd.assert_called_once()
        kw = mock_redis.xadd.call_args.kwargs
        assert kw["name"] == "test_events:stream_dlq"
        assert kw["fields"]["original_message_id"] == "msg-123"
        assert kw["fields"]["retry_count"] == "3"

    async def test_get_dlq_messages(self, bus: RedisEventBus, mock_redis: AsyncMock) -> None:
        mock_redis.xrange.return_value = [
            ("msg-1", {"event_type": "SampleOrderCreated", "original_message_id": "orig-1"}),
            ("msg-2", {"event_type": "SampleOrderShipped", "original_message_id": "orig-2"}),
        ]
        messages = await bus.get_dlq_messages(count=10)
        assert [m["message_id"] for m in messages] == ["msg-1", "msg-2"]
        mock_redis.xrange.assert_called_once()

    async def test_replay_dlq_message(self, bus: RedisEventBus, mock_redis: AsyncMock) -> None:
        dlq_fields = {
            "event_type": "SampleOrderCreated",
            "payload": "{}",
            "original_message_id": "orig-123",
            "retry_count": "3",
            "dlq_timestamp": "2024-01-01T00:00:00Z",
        }
        mock_redis.xrange.return_value = [("dlq-msg-123", dlq_fields)]
        mock_redis.xadd.return_value = "new-msg-456"
        assert await bus.replay_dlq_message("dlq-msg-123") is True
        mock_redis.xadd.assert_called_once()
        mock_redis.xdel.assert_called_once()
        fields = mock_redis.xadd.call_args.kwargs["fields"]
        assert {"original_message_id", "retry_count", "dlq_timestamp"}.isdisjoint(fields)

    async def test_replay_dlq_message_not_found(
        self, bus: RedisEventBus, mock_redis: AsyncMock
    ) -> None:
        mock_redis.xrange.return_value = []
        assert await bus.replay_dlq_message("non-existent") is False


class TestRedisEventBusPendingRecovery:
    async def test_recover_no_pending_messages(
        self, bus: RedisEventBus, mock_redis: AsyncMock
    ) -> None:
        mock_redis.xpending.return_value = {"pending": 0}
        s = await bus.recover_pending_messages()
        assert (s["checked"], s["claimed"], s["reprocessed"]) == (0, 0, 0)
        assert (s["dlq"], s["failed"]) == (0, 0)

    async def test_recover_pending_messages_success(
        self, bus: RedisEventBus, event_registry: EventRegistry, mock_redis: AsyncMock
    ) -> None:
        bus.subscribe(SampleOrderCreated, OrderHandler())
        mock_redis.xpending.return_value = {"pending": 1}
        mock_redis.xpending_range.return_value = _pending_entry(delivered=1)
        mock_redis.xclaim.return_value = [("msg-123", _order_msg(_new_order()))]
        stats = await bus.recover_pending_messages(min_idle_time_ms=60000)
        assert (stats["checked"], stats["claimed"], stats["reprocessed"]) == (1, 1, 1)

    async def test_recover_pending_messages_to_dlq(
        self, bus: RedisEventBus, mock_redis: AsyncMock
    ) -> None:
        mock_redis.xpending.return_value = {"pending": 1}
        mock_redis.xpending_range.return_value = _pending_entry(delivered=5)
        payload = {"event_id": str(uuid4()), "event_type": "SampleOrderCreated", "payload": "{}"}
        mock_redis.xclaim.return_value = [("msg-123", payload)]
        mock_redis.get.return_value = "3"
        stats = await bus.recover_pending_messages(min_idle_time_ms=60000, max_retries=3)
        assert stats["dlq"] == 1

    async def test_recover_pending_messages_honors_an_explicit_zero(
        self, bus: RedisEventBus, mock_redis: AsyncMock
    ) -> None:
        mock_redis.xpending.return_value = {"pending": 1}
        mock_redis.xpending_range.return_value = _pending_entry(delivered=1, idle_ms=0)
        mock_redis.xclaim.return_value = []
        stats = await bus.recover_pending_messages(min_idle_time_ms=0)
        assert stats["claimed"] == 0
        mock_redis.xclaim.assert_awaited()

    async def test_start_consuming_in_background_populates_the_consumer_task(
        self, bus: RedisEventBus, mock_redis: AsyncMock
    ) -> None:
        bus._connected = True
        task = bus.start_consuming_in_background()
        assert bus._consumer_task is task
        with pytest.raises(RuntimeError, match="already running"):
            bus.start_consuming_in_background()
        await bus.stop_consuming()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
