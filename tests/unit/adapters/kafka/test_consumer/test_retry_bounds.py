"""Bounds on broker-driven retry scheduling: retry_after clamping, parse safety, capacity DLQ.

Each test drives ``KafkaConsumerLoop.start()`` over a fake record stream, so the
retry path is reached through the same caller a real consumer uses.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

pytest.importorskip("aiokafka", reason="aiokafka not installed")

from eventsource.adapters._bus.handler_adapter import HandlerAdapter  # noqa: E402
from eventsource.adapters._bus.retry import RetryPolicy  # noqa: E402
from eventsource.adapters.kafka.config import KafkaEventBusConfig  # noqa: E402
from eventsource.adapters.kafka.connection import KafkaConnectionManager  # noqa: E402
from eventsource.adapters.kafka.consumer import KafkaConsumerLoop  # noqa: E402
from eventsource.adapters.kafka.models import KafkaEventBusStats  # noqa: E402
from eventsource.adapters.kafka.serialization import EventSerializer  # noqa: E402
from eventsource.domain.event import DomainEvent  # noqa: E402
from eventsource.observability import create_tracer  # noqa: E402


class BoundedRetryEvent(DomainEvent):
    aggregate_type: str = "BoundedRetryAggregate"
    data: str = ""


def _record(data: str, offset: int, headers_extra: list[tuple[str, bytes]] | None = None) -> Any:
    event = BoundedRetryEvent(aggregate_id=uuid4(), aggregate_version=1, data=data)
    headers: list[tuple[str, bytes]] = [
        ("event_type", event.event_type.encode("utf-8")),
        ("event_id", str(event.event_id).encode("utf-8")),
        *(headers_extra or []),
    ]
    return SimpleNamespace(
        value=EventSerializer().serialize(event),
        key=str(event.aggregate_id).encode("utf-8"),
        headers=headers,
        topic="events.stream",
        partition=0,
        offset=offset,
    )


def _retry_after(seconds_from_now: float) -> list[tuple[str, bytes]]:
    due = datetime.now(UTC).timestamp() + seconds_from_now
    return [("retry_after", str(due).encode("utf-8"))]


class _Stream:
    """Fake aiokafka consumer: yields records, then ends the consume loop."""

    def __init__(self, records: list[Any]) -> None:
        self._records = list(records)
        self.commit = AsyncMock()
        self._shutdown: asyncio.Event | None = None

    def __aiter__(self) -> _Stream:
        return self

    async def __anext__(self) -> Any:
        if not self._records:
            if self._shutdown is not None:
                self._shutdown.set()
            raise StopAsyncIteration
        return self._records.pop(0)


def _build_loop(
    stream: _Stream, *, retry_max_delay: float = 60.0
) -> tuple[KafkaConsumerLoop, AsyncMock, list[str], KafkaEventBusStats]:
    """Wire a consumer loop to a fake stream; return it with the producer, handled data, stats."""
    with patch.object(KafkaEventBusConfig, "_validate_security_config"):
        config = KafkaEventBusConfig(  # type: ignore[arg-type]
            bootstrap_servers="localhost:9092",
            max_retries=3,
            retry_max_delay=retry_max_delay,
        )
    handled: list[str] = []

    async def handle(event: DomainEvent) -> None:
        if isinstance(event, BoundedRetryEvent):
            handled.append(event.data)

    stats = KafkaEventBusStats()
    producer = AsyncMock()
    connection = KafkaConnectionManager(config=config, stats=stats, metrics=None)
    connection._producer = producer
    connection._consumer = stream  # type: ignore[assignment]
    connection._connected = True
    shutdown = asyncio.Event()
    stream._shutdown = shutdown
    loop = KafkaConsumerLoop(
        config=config,
        connection=connection,
        serializer=EventSerializer(),
        stats=stats,
        metrics=MagicMock(),
        retry_policy=RetryPolicy(base_delay=0.1, max_delay=1.0, jitter=0.0),
        handlers_for=lambda _cls: (HandlerAdapter(handle),),
        resolve_event_class=lambda _name: BoundedRetryEvent,
        tracer=create_tracer(__name__, False),
        enable_tracing=False,
        shutdown_event=shutdown,
    )
    return loop, producer, handled, stats


@pytest.mark.asyncio
async def test_far_future_retry_after_is_clamped_to_retry_max_delay() -> None:
    """A retry_after a day away runs after retry_max_delay, not after a day."""
    stream = _Stream([_record("far-future", 1, _retry_after(86_400))])
    loop, _producer, handled, _stats = _build_loop(stream, retry_max_delay=0.2)

    await loop.start(auto_reconnect=False)
    assert handled == []

    await loop.drain_retries(timeout=2.0)
    assert handled == ["far-future"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_value",
    [b"inf", b"-inf", b"nan", b"NaN", b"-5", b"soon", b"\xff\xfe"],
    ids=["inf", "neg-inf", "nan", "NaN", "negative", "non-numeric", "non-utf8"],
)
async def test_unusable_retry_after_is_processed_immediately(raw_value: bytes) -> None:
    """Infinite, NaN, negative, and non-numeric retry_after values give a zero delay."""
    stream = _Stream([_record("immediate", 1, [("retry_after", raw_value)])])
    loop, producer, handled, _stats = _build_loop(stream)

    await loop.start(auto_reconnect=False)

    assert handled == ["immediate"]
    producer.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_full_retry_scheduler_routes_message_to_dlq_and_commits() -> None:
    """At the pending-task limit, the next deferred retry goes to the DLQ and is committed."""
    limit = 1000
    deferred = [_record(f"d{i}", i, _retry_after(0.5)) for i in range(limit)]
    overflow = _record("overflow", limit, _retry_after(0.5))
    after = _record("after-overflow", limit + 1)
    stream = _Stream([*deferred, overflow, after])
    loop, producer, handled, stats = _build_loop(stream)

    await loop.start(auto_reconnect=False)

    # Consumption continued past the overflow: the next record was processed inline.
    assert handled == ["after-overflow"]

    dlq_calls = [c for c in producer.send.await_args_list if c.kwargs["topic"].endswith(".dlq")]
    assert len(dlq_calls) == 1
    headers = dict(dlq_calls[0].kwargs["headers"])
    assert headers["dlq_reason"] == b"retry_capacity_exceeded"
    assert stats.messages_sent_to_dlq == 1
    assert stream.commit.await_count >= 1

    # The deferred records still run once their delay elapses.
    await loop.drain_retries(timeout=5.0)
    assert len(handled) == limit + 1
    assert "overflow" not in handled
