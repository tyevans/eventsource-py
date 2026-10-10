"""Unit and integration tests for non-blocking backoff in RabbitMQ consumer (TASK-0009).

Verifies:
1. Exponential retry scheduling uses non-blocking async timers.
2. Concurrent queue messages continue processing while a failed message backs off.
3. High throughput is maintained during transient error injection.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any
from unittest import mock
from uuid import uuid4

import pytest

from eventsource.adapters._bus.handler_adapter import HandlerAdapter
from eventsource.adapters._bus.retry import RetryPolicy
from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.consumer import RabbitMQConsumer
from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats
from eventsource.domain.event import DomainEvent


class NonBlockingRabbitMQEvent(DomainEvent):
    aggregate_type: str = "Order"
    data: str = ""


def _make_msg(event: DomainEvent, data: str) -> mock.MagicMock:
    message = mock.MagicMock()
    message.headers = {
        "event_type": "NonBlockingRabbitMQEvent",
        "x-retry-count": 0,
    }
    message.message_id = f"msg-{data}"
    message.routing_key = "Order.NonBlockingRabbitMQEvent"
    message.content_type = "application/json"
    message.content_encoding = "utf-8"
    message.body = json.dumps(
        {
            "event_type": "NonBlockingRabbitMQEvent",
            "aggregate_type": "Order",
            "aggregate_id": str(event.aggregate_id),
            "data": data,
        }
    ).encode()
    message.ack = mock.AsyncMock()
    message.reject = mock.AsyncMock()
    return message


class AsyncQueueIterator:
    """Mock asynchronous queue iterator that yields messages."""

    def __init__(self, messages: list[Any]) -> None:
        self._messages = list(messages)

    def __aiter__(self) -> AsyncQueueIterator:
        return self

    async def __anext__(self) -> Any:
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)

    async def __aenter__(self) -> AsyncQueueIterator:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


def _make_consumer(
    handler: HandlerAdapter,
    base_delay: float = 0.2,
) -> tuple[RabbitMQConsumer, mock.AsyncMock]:
    config = RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="test-events",
        consumer_group="test-group",
        max_retries=3,
        retry_base_delay=base_delay,
        retry_jitter=0.0,
    )
    stats = RabbitMQEventBusStats()
    policy = RetryPolicy(base_delay=base_delay, max_delay=1.0, jitter=0.0)

    connection = mock.MagicMock()
    connection.is_connected = True

    exchange = mock.AsyncMock()
    topology = mock.MagicMock()
    topology.exchange = exchange

    consumer = RabbitMQConsumer(
        config=config,
        connection=connection,
        topology=topology,
        stats=stats,
        retry_policy=policy,
        handlers_for=lambda _cls: (handler,),
        resolve_event_class=lambda _name: NonBlockingRabbitMQEvent,
        tracer=None,
        enable_tracing=False,
    )
    return consumer, exchange


@pytest.mark.asyncio
async def test_concurrent_queue_messages_processed_while_message_backs_off() -> None:
    """Verifies that when a message fails and enters backoff, subsequent messages
    in the queue continue processing immediately without waiting for the backoff delay.
    """
    processed_order: list[tuple[str, float]] = []

    async def handle_event(event: DomainEvent) -> None:
        if isinstance(event, NonBlockingRabbitMQEvent):
            if event.data == "msg-1-fail":
                raise ValueError("Transient error on message 1")
            processed_order.append((event.data, datetime.now(UTC).timestamp()))

    consumer, mock_exchange = _make_consumer(HandlerAdapter(handle_event), base_delay=0.2)

    event1 = NonBlockingRabbitMQEvent(aggregate_id=uuid4(), data="msg-1-fail")
    event2 = NonBlockingRabbitMQEvent(aggregate_id=uuid4(), data="msg-2-fast")
    event3 = NonBlockingRabbitMQEvent(aggregate_id=uuid4(), data="msg-3-fast")

    msg1 = _make_msg(event1, "msg-1-fail")
    msg2 = _make_msg(event2, "msg-2-fast")
    msg3 = _make_msg(event3, "msg-3-fast")

    queue_iter = AsyncQueueIterator([msg1, msg2, msg3])
    consumer._topology.consumer_queue = mock.MagicMock()
    consumer._topology.consumer_queue.iterator.return_value = queue_iter

    # Start consumer
    await consumer.start()

    # Message 2 and Message 3 should be processed immediately
    processed_names = [name for name, _ in processed_order]
    assert processed_names == ["msg-2-fast", "msg-3-fast"]

    # msg1 should have been acked and republished only AFTER the delay
    mock_exchange.publish.assert_not_called()
    msg1.ack.assert_not_awaited()

    # Drain retries (awaits the non-blocking timer)
    await consumer.drain_retries(timeout=1.0)

    # Now msg1 retry has been republished and acknowledged
    mock_exchange.publish.assert_called_once()
    msg1.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_throughput_during_transient_error_injection() -> None:
    """Verifies throughput is maintained when transient errors occur."""
    processed_success: list[str] = []

    async def handle_event(event: DomainEvent) -> None:
        if isinstance(event, NonBlockingRabbitMQEvent):
            if event.data == "failing-message":
                raise ValueError("Transient injection")
            processed_success.append(event.data)

    consumer, mock_exchange = _make_consumer(HandlerAdapter(handle_event), base_delay=0.3)

    messages: list[Any] = []
    fail_event = NonBlockingRabbitMQEvent(aggregate_id=uuid4(), data="failing-message")
    messages.append(_make_msg(fail_event, "failing-message"))

    for i in range(10):
        ok_event = NonBlockingRabbitMQEvent(aggregate_id=uuid4(), data=f"success-{i}")
        messages.append(_make_msg(ok_event, f"success-{i}"))

    queue_iter = AsyncQueueIterator(messages)
    consumer._topology.consumer_queue = mock.MagicMock()
    consumer._topology.consumer_queue.iterator.return_value = queue_iter

    start_time = asyncio.get_running_loop().time()
    await consumer.start()
    elapsed = asyncio.get_running_loop().time() - start_time

    # All 10 success messages were processed in < 0.15s, completely unblocked by 0.3s backoff
    assert len(processed_success) == 10
    assert elapsed < 0.15, f"Consume loop was blocked by backoff: {elapsed:.2f}s"

    await consumer.drain_retries(timeout=1.0)
    mock_exchange.publish.assert_called_once()
