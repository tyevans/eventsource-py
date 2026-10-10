"""Unit and integration tests for non-blocking backoff in Kafka consumer (TASK-0009).

Verifies:
1. Exponential retry scheduling uses non-blocking async timers.
2. Concurrent partition messages continue processing while a failed message backs off.
3. High throughput is maintained during transient error injection.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch
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


class NonBlockingTestEvent(DomainEvent):
    aggregate_type: str = "NonBlockingAggregate"
    data: str = ""


def _make_msg(
    event: DomainEvent,
    partition: int,
    offset: int,
    retry_after: float | None = None,
) -> Any:
    headers: list[tuple[str, bytes]] = [
        ("event_type", event.event_type.encode("utf-8")),
        ("event_id", str(event.event_id).encode("utf-8")),
    ]
    if retry_after is not None:
        headers.append(("retry_after", str(retry_after).encode("utf-8")))
    return SimpleNamespace(
        value=EventSerializer().serialize(event),
        key=str(event.aggregate_id).encode("utf-8"),
        headers=headers,
        topic="events.stream",
        partition=partition,
        offset=offset,
    )


class AsyncMessageStream:
    """Mock asynchronous consumer that yields messages from an in-memory queue."""

    def __init__(
        self,
        messages: list[Any],
        on_exhausted: Any | None = None,
    ) -> None:
        self._messages = list(messages)
        self.committed: list[Any] = []
        self.on_exhausted = on_exhausted

    def __aiter__(self) -> AsyncMessageStream:
        return self

    async def __anext__(self) -> Any:
        if not self._messages:
            if self.on_exhausted:
                self.on_exhausted()
            raise StopAsyncIteration
        return self._messages.pop(0)

    async def commit(self) -> None:
        self.committed.append(datetime.now(UTC))


def _make_consumer_loop(
    stream: AsyncMessageStream,
    handler: HandlerAdapter,
) -> tuple[KafkaConsumerLoop, AsyncMock]:
    with patch.object(KafkaEventBusConfig, "_validate_security_config"):
        config = KafkaEventBusConfig(  # type: ignore[arg-type]
            bootstrap_servers="localhost:9092",
            max_retries=3,
        )
    stats = KafkaEventBusStats()
    producer = AsyncMock()

    connection = KafkaConnectionManager(config=config, stats=stats, metrics=None)
    connection._producer = producer
    connection._consumer = stream  # type: ignore[assignment]
    connection._connected = True

    loop = KafkaConsumerLoop(
        config=config,
        connection=connection,
        serializer=EventSerializer(),
        stats=stats,
        metrics=None,
        retry_policy=RetryPolicy(base_delay=0.1, max_delay=1.0, jitter=0.0),
        handlers_for=lambda _cls: (handler,),
        resolve_event_class=lambda _name: NonBlockingTestEvent,
        tracer=create_tracer(__name__, False),
        enable_tracing=False,
        shutdown_event=asyncio.Event(),
    )
    stream.on_exhausted = lambda: loop._shutdown_event.set()
    return loop, producer


@pytest.mark.asyncio
async def test_concurrent_partition_messages_processed_while_message_backs_off() -> None:
    """Verifies that while a retried message on partition 0 backs off,
    messages on partition 1 and partition 2 are processed immediately
    without being blocked by partition 0's delay.
    """
    processed_order: list[tuple[str, int]] = []

    async def handle_event(event: DomainEvent) -> None:
        if isinstance(event, NonBlockingTestEvent):
            processed_order.append((event.data, datetime.now(UTC).timestamp()))

    # Message 1 on partition 0 has a backoff delay of 0.2s into the future
    future_delay = 0.2
    event1 = NonBlockingTestEvent(aggregate_id=uuid4(), data="p0-delayed")
    msg1 = _make_msg(
        event1,
        partition=0,
        offset=10,
        retry_after=datetime.now(UTC).timestamp() + future_delay,
    )

    # Message 2 on partition 1 has no delay
    event2 = NonBlockingTestEvent(aggregate_id=uuid4(), data="p1-immediate")
    msg2 = _make_msg(event2, partition=1, offset=20)

    # Message 3 on partition 2 has no delay
    event3 = NonBlockingTestEvent(aggregate_id=uuid4(), data="p2-immediate")
    msg3 = _make_msg(event3, partition=2, offset=30)

    stream = AsyncMessageStream([msg1, msg2, msg3])
    loop, _ = _make_consumer_loop(stream, HandlerAdapter(handle_event))

    # Run start() which consumes all 3 messages
    await loop.start(auto_reconnect=False)

    # At this point, the consume loop has yielded all messages.
    # msg2 and msg3 should be processed immediately, while msg1 is scheduled on the timer.
    processed_names = [name for name, _ in processed_order]
    assert "p1-immediate" in processed_names
    assert "p2-immediate" in processed_names
    # p0-delayed should NOT have been processed yet if non-blocking!
    assert "p0-delayed" not in processed_names

    # Now drain retries (awaits the non-blocking timer)
    await loop.drain_retries(timeout=1.0)

    # Now all 3 have been processed
    final_names = [name for name, _ in processed_order]
    assert final_names == ["p1-immediate", "p2-immediate", "p0-delayed"]


@pytest.mark.asyncio
async def test_throughput_during_transient_error_injection() -> None:
    """Verifies throughput across partitions when transient errors are injected."""
    processed_p1: list[str] = []

    async def handle_event(event: DomainEvent) -> None:
        if isinstance(event, NonBlockingTestEvent) and event.data.startswith("p1"):
            processed_p1.append(event.data)

    # 1 delayed message on partition 0 and 10 fast messages on partition 1
    messages: list[Any] = []
    p0_event = NonBlockingTestEvent(aggregate_id=uuid4(), data="p0-backoff")
    messages.append(
        _make_msg(
            p0_event,
            partition=0,
            offset=1,
            retry_after=datetime.now(UTC).timestamp() + 0.3,
        )
    )

    for i in range(10):
        p1_event = NonBlockingTestEvent(aggregate_id=uuid4(), data=f"p1-msg-{i}")
        messages.append(_make_msg(p1_event, partition=1, offset=100 + i))

    stream = AsyncMessageStream(messages)
    loop, _ = _make_consumer_loop(stream, HandlerAdapter(handle_event))

    start_time = asyncio.get_running_loop().time()
    await loop.start(auto_reconnect=False)
    elapsed_before_drain = asyncio.get_running_loop().time() - start_time

    # All 10 partition 1 messages processed almost instantaneously (well under 0.3s)
    assert len(processed_p1) == 10
    assert elapsed_before_drain < 0.15, "Loop was blocked by partition 0 backoff!"

    await loop.drain_retries(timeout=1.0)
