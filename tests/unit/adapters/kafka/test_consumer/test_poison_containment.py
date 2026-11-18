"""Poison-message containment for the Kafka consume loop.

Each test drives ``KafkaConsumerLoop.start()`` with a fake consumer that yields a
poison record followed by a good one. The good record must be handled and
committed, and the poison record must reach the DLQ (or be retried). A header
that cannot be read must never end the loop or leave an offset uncommitted.
"""

from __future__ import annotations

import asyncio
from collections import deque
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

from ._fixtures import SampleConsumerEvent, _make_message  # noqa: E402


def _build_loop(max_retries: int = 3) -> tuple[KafkaConsumerLoop, AsyncMock, AsyncMock, list[str]]:
    """Consumer loop whose resolver knows only SampleConsumerEvent (unknown names are undecodable)."""
    with patch.object(KafkaEventBusConfig, "_validate_security_config"):
        config = KafkaEventBusConfig(  # type: ignore[arg-type]
            bootstrap_servers="localhost:9092", max_retries=max_retries, enable_dlq=True
        )
    stats = KafkaEventBusStats()
    metrics = MagicMock()
    producer, consumer = AsyncMock(), AsyncMock()
    connection = KafkaConnectionManager(config=config, stats=stats, metrics=metrics)
    connection._producer, connection._consumer, connection._connected = producer, consumer, True
    handled: list[str] = []

    async def handler(event: DomainEvent) -> None:
        assert isinstance(event, SampleConsumerEvent)
        handled.append(event.payload)
        if event.payload == "poison":
            raise ValueError("handler rejects poison")

    loop = KafkaConsumerLoop(
        config=config,
        connection=connection,
        serializer=EventSerializer(),
        stats=stats,
        metrics=metrics,
        retry_policy=RetryPolicy(
            base_delay=config.retry_base_delay,
            max_delay=config.retry_max_delay,
            jitter=config.retry_jitter,
            max_retries=config.max_retries,
        ),
        handlers_for=lambda _cls: (HandlerAdapter(handler),),
        resolve_event_class=lambda name: (
            SampleConsumerEvent if name == "SampleConsumerEvent" else None
        ),  # fmt: skip
        tracer=create_tracer(__name__, False),
        enable_tracing=False,
        shutdown_event=asyncio.Event(),
    )
    return loop, producer, consumer, handled


async def _run(loop: KafkaConsumerLoop, consumer: AsyncMock, records: list[Any]) -> None:
    """Feed records through start(); the loop stops itself once they are consumed."""
    pending = deque(records)

    async def fake_aiter(self: Any) -> Any:
        while pending:
            yield pending.popleft()
        await loop.stop()

    consumer.__aiter__ = fake_aiter
    # Reconnection is the behavior under test to avoid; a fake must not open real sockets.
    with patch.object(loop._connection, "reconnect_consumer", new=AsyncMock()):
        await asyncio.wait_for(loop.start(), timeout=10)
    await loop.drain_retries()


def _with_headers(record: Any, headers: list[tuple[str, bytes]]) -> Any:
    return SimpleNamespace(**{**vars(record), "headers": headers})


def _record(payload: str, extra: list[tuple[str, bytes]] = (), drop: tuple[str, ...] = ()) -> Any:  # fmt: skip
    """Build a Kafka record for a SampleConsumerEvent carrying ``payload``."""
    event = SampleConsumerEvent(aggregate_id=uuid4(), aggregate_version=1, payload=payload)
    record = _make_message(event)
    kept = [(k, v) for k, v in record.headers if k not in drop]
    return _with_headers(record, [*kept, *extra])


def _dlq_sends(producer: AsyncMock, topic: str) -> list[dict[str, Any]]:
    return [c.kwargs for c in producer.send.await_args_list if c.kwargs["topic"] == topic]


@pytest.mark.asyncio
async def test_undecodable_event_type_is_dead_lettered_and_good_record_handled() -> None:
    loop, producer, consumer, handled = _build_loop()
    poison = _record("poison", [("event_type", b"\xff")], drop=("event_type",))
    good = _record("good")

    await _run(loop, consumer, [poison, good])

    assert handled == ["good"]
    dlq = _dlq_sends(producer, loop._config.dlq_topic_name)
    assert len(dlq) == 1
    assert dict(dlq[0]["headers"])["dlq_reason"] == b"deserialization_error"
    assert consumer.commit.await_count == 2


@pytest.mark.asyncio
async def test_undecodable_retry_after_is_ignored_and_record_processed() -> None:
    loop, _producer, consumer, handled = _build_loop()
    poison = _record("poison", [("retry_after", b"\xff")])
    good = _record("good")

    await _run(loop, consumer, [poison, good])

    assert handled == ["poison", "good"]
    assert consumer.commit.await_count == 2


@pytest.mark.asyncio
async def test_negative_retry_count_with_failing_handler_is_republished() -> None:
    loop, producer, consumer, handled = _build_loop(max_retries=3)
    poison = _record("poison", [("retry_count", b"-1")])
    good = _record("good")

    await _run(loop, consumer, [poison, good])

    assert handled == ["poison", "good"]
    retries = _dlq_sends(producer, loop._config.topic_name)
    assert len(retries) == 1
    assert dict(retries[0]["headers"])["retry_count"] == b"1"
    assert consumer.commit.await_count == 2


@pytest.mark.asyncio
async def test_dlq_writer_strips_forged_inbound_dlq_headers() -> None:
    loop, producer, consumer, handled = _build_loop(max_retries=0)
    forged = [("dlq_replay_count", b"999"), ("dlq_reason", b"forged")]
    poison = _record("poison", forged)
    good = _record("good")

    await _run(loop, consumer, [poison, good])

    assert handled == ["poison", "good"]
    dlq = _dlq_sends(producer, loop._config.dlq_topic_name)
    assert len(dlq) == 1
    headers = dlq[0]["headers"]
    assert [v for k, v in headers if k == "dlq_replay_count"] == []
    assert [v for k, v in headers if k == "dlq_reason"] == [b"max_retries_exceeded"]


@pytest.mark.asyncio
async def test_missing_event_type_is_dead_lettered_not_silently_dropped() -> None:
    loop, producer, consumer, handled = _build_loop()
    poison = _record("poison", drop=("event_type",))
    good = _record("good")

    await _run(loop, consumer, [poison, good])

    assert handled == ["good"]
    dlq = _dlq_sends(producer, loop._config.dlq_topic_name)
    assert len(dlq) == 1
    assert dict(dlq[0]["headers"])["dlq_reason"] == b"deserialization_error"
    assert consumer.commit.await_count == 2
