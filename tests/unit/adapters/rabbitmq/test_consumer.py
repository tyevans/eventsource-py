"""Unit tests for RabbitMQConsumer (bus god-class decomposition, Task 7)."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest import mock
from uuid import UUID, uuid4

import pytest

from eventsource.adapters._bus.handler_adapter import HandlerAdapter
from eventsource.adapters._bus.retry import RetryPolicy
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerDispatchError

from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig  # isort: skip
from eventsource.adapters.rabbitmq.consumer import RabbitMQConsumer  # isort: skip
from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats  # isort: skip


class ConsumerTestEvent(DomainEvent):
    """Sample event used to exercise the consumer."""

    aggregate_type: str = "Order"
    aggregate_id: UUID = uuid4()


@pytest.fixture
def config() -> RabbitMQEventBusConfig:
    return RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="test-events",
        consumer_group="test-group",
        max_retries=3,
        retry_base_delay=0.001,
        retry_jitter=0.0,
    )


@pytest.fixture
def stats() -> RabbitMQEventBusStats:
    return RabbitMQEventBusStats()


def _message(headers: dict[str, Any] | None = None) -> mock.MagicMock:
    """Build a mock incoming AMQP message matching what _process_message reads."""
    message = mock.MagicMock()
    message.headers = headers if headers is not None else {"event_type": "ConsumerTestEvent"}
    message.message_id = "test-message-id"
    message.routing_key = "Order.ConsumerTestEvent"
    message.content_type = "application/json"
    message.content_encoding = "utf-8"
    message.body = json.dumps(
        {
            "event_type": "ConsumerTestEvent",
            "aggregate_type": "Order",
            "aggregate_id": str(uuid4()),
        }
    ).encode()
    message.ack = mock.AsyncMock()
    message.reject = mock.AsyncMock()
    return message


def _consumer(
    config: RabbitMQEventBusConfig,
    stats: RabbitMQEventBusStats,
    *,
    handlers: tuple[HandlerAdapter, ...] = (),
    exchange: mock.AsyncMock | None = None,
    dlq_exchange: mock.AsyncMock | None = None,
    connection: mock.MagicMock | None = None,
) -> RabbitMQConsumer:
    topology = mock.MagicMock()
    topology.exchange = exchange if exchange is not None else mock.AsyncMock()
    topology.dlq_exchange = dlq_exchange if dlq_exchange is not None else mock.AsyncMock()
    topology.consumer_queue = mock.MagicMock()
    return RabbitMQConsumer(
        config=config,
        connection=connection if connection is not None else mock.MagicMock(),
        topology=topology,
        stats=stats,
        retry_policy=RetryPolicy(
            base_delay=config.retry_base_delay,
            max_delay=config.retry_max_delay,
            jitter=config.retry_jitter,
            max_retries=config.max_retries,
        ),
        handlers_for=lambda _event_type: handlers,
        resolve_event_class=lambda _name: ConsumerTestEvent,
        tracer=None,
        enable_tracing=False,
    )


@pytest.mark.asyncio
async def test_all_handlers_run_when_first_fails_and_message_not_acked(
    config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
) -> None:
    """ADR 0011: every handler runs; failures aggregate; no ack on failure."""
    calls: list[str] = []

    async def failing(event: DomainEvent) -> None:
        calls.append("failing")
        raise ValueError("boom")

    async def succeeding(event: DomainEvent) -> None:
        calls.append("succeeding")

    handlers = (HandlerAdapter(failing), HandlerAdapter(succeeding))
    consumer = _consumer(config, stats, handlers=handlers)
    message = _message()

    await consumer._process_message(message)

    assert calls == ["failing", "succeeding"]
    # Message is retried (republished + original acked), never acked as success
    assert stats.events_processed_failed == 1
    assert stats.events_processed_success == 0


@pytest.mark.asyncio
async def test_dispatch_event_raises_aggregate_error(
    config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
) -> None:
    async def failing_a(event: DomainEvent) -> None:
        raise ValueError("a")

    async def failing_b(event: DomainEvent) -> None:
        raise RuntimeError("b")

    consumer = _consumer(
        config, stats, handlers=(HandlerAdapter(failing_a), HandlerAdapter(failing_b))
    )
    event = ConsumerTestEvent()

    with pytest.raises(HandlerDispatchError) as exc_info:
        await consumer._dispatch_event(event, _message())

    assert len(exc_info.value.failures) == 2


@pytest.mark.asyncio
async def test_success_path_acks(
    config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
) -> None:
    seen: list[DomainEvent] = []

    async def handler(event: DomainEvent) -> None:
        seen.append(event)

    consumer = _consumer(config, stats, handlers=(HandlerAdapter(handler),))
    message = _message()

    await consumer._process_message(message)

    message.ack.assert_awaited_once()
    assert len(seen) == 1
    assert stats.events_processed_success == 1


@pytest.mark.asyncio
async def test_failure_below_max_retries_republishes_with_incremented_count(
    config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
) -> None:
    async def failing(event: DomainEvent) -> None:
        raise ValueError("boom")

    exchange = mock.AsyncMock()
    consumer = _consumer(config, stats, handlers=(HandlerAdapter(failing),), exchange=exchange)
    message = _message({"event_type": "ConsumerTestEvent", "x-retry-count": 1})

    await consumer._process_message(message)

    exchange.publish.assert_awaited_once()
    retry_message = exchange.publish.await_args.args[0]
    assert retry_message.headers["x-retry-count"] == 2
    assert exchange.publish.await_args.kwargs["routing_key"] == "Order.ConsumerTestEvent"
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_failure_at_max_retries_goes_to_dlq_with_single_failure_unwrap(
    config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
) -> None:
    async def failing(event: DomainEvent) -> None:
        raise ValueError("boom")

    dlq_exchange = mock.AsyncMock()
    consumer = _consumer(
        config, stats, handlers=(HandlerAdapter(failing),), dlq_exchange=dlq_exchange
    )
    message = _message({"event_type": "ConsumerTestEvent", "x-retry-count": config.max_retries})

    await consumer._process_message(message)

    dlq_exchange.publish.assert_awaited_once()
    dlq_message = dlq_exchange.publish.await_args.args[0]
    # Single-failure unwrap: DLQ metadata reflects the handler's own exception
    assert dlq_message.headers["x-dlq-error-type"] == "ValueError"
    assert dlq_message.headers["x-dlq-reason"] == "boom"
    assert dlq_message.headers["x-dlq-retry-count"] == config.max_retries
    assert stats.messages_sent_to_dlq == 1
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_unknown_event_type_is_acked(
    config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
) -> None:
    consumer = _consumer(config, stats)
    consumer._resolve_event_class = lambda _name: None  # type: ignore[assignment]
    message = _message()

    await consumer._process_message(message)

    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_resume_if_was_consuming_restarts_only_when_previously_active(
    config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
) -> None:
    connection = mock.MagicMock()
    connection._was_consuming = False
    consumer = _consumer(config, stats, connection=connection)
    consumer.start = mock.AsyncMock()  # type: ignore[method-assign]

    await consumer.resume_if_was_consuming()
    consumer.start.assert_not_called()

    connection._was_consuming = True
    await consumer.resume_if_was_consuming()
    await asyncio.sleep(0)
    consumer.start.assert_called_once()
    assert connection._was_consuming is False


@pytest.mark.asyncio
async def test_stop_sets_is_consuming_false(
    config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
) -> None:
    consumer = _consumer(config, stats)
    consumer._consuming = True
    assert consumer.is_consuming is True

    await consumer.stop()

    assert consumer.is_consuming is False
