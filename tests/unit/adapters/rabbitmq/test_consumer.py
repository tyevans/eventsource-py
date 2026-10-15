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
    body = json.dumps(
        {
            "event_type": "ConsumerTestEvent",
            "aggregate_type": "Order",
            "aggregate_id": str(uuid4()),
        }
    ).encode()
    return mock.MagicMock(
        headers=headers if headers is not None else {"event_type": "ConsumerTestEvent"},
        message_id="test-message-id",
        routing_key="Order.ConsumerTestEvent",
        content_type="application/json",
        content_encoding="utf-8",
        body=body,
        ack=mock.AsyncMock(),
        reject=mock.AsyncMock(),
    )


def _consumer(
    config: RabbitMQEventBusConfig,
    stats: RabbitMQEventBusStats,
    *,
    handlers: tuple[HandlerAdapter, ...] = (),
    exchange: mock.AsyncMock | None = None,
    dlq_exchange: mock.AsyncMock | None = None,
    connection: mock.MagicMock | None = None,
) -> RabbitMQConsumer:
    topology = mock.MagicMock(
        exchange=exchange or mock.AsyncMock(),
        dlq_exchange=dlq_exchange or mock.AsyncMock(),
        consumer_queue=mock.MagicMock(),
    )
    retry = RetryPolicy(
        config.retry_base_delay, config.retry_max_delay, config.retry_jitter, config.max_retries
    )
    return RabbitMQConsumer(
        config=config,
        connection=connection or mock.MagicMock(),
        topology=topology,
        stats=stats,
        retry_policy=retry,
        handlers_for=lambda _event_type: handlers,
        resolve_event_class=lambda _name: ConsumerTestEvent,
        tracer=None,
        enable_tracing=False,
    )


async def _boom(event: DomainEvent) -> None:
    raise ValueError("boom")


@pytest.mark.asyncio
async def test_all_handlers_run_when_first_fails_and_message_not_acked(
    config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
) -> None:
    calls: list[str] = []

    async def failing(event: DomainEvent) -> None:
        calls.append("failing")
        raise ValueError("boom")

    async def succeeding(event: DomainEvent) -> None:
        calls.append("succeeding")

    consumer = _consumer(
        config, stats, handlers=(HandlerAdapter(failing), HandlerAdapter(succeeding))
    )
    await consumer._process_message(_message())
    assert calls == ["failing", "succeeding"]
    assert stats.events_processed_failed == 1
    assert stats.events_processed_success == 0


@pytest.mark.asyncio
async def test_dispatch_event_raises_aggregate_error(
    config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
) -> None:
    async def failing_b(event: DomainEvent) -> None:
        raise RuntimeError("b")

    consumer = _consumer(config, stats, handlers=(HandlerAdapter(_boom), HandlerAdapter(failing_b)))
    with pytest.raises(HandlerDispatchError) as exc_info:
        await consumer._dispatch_event(ConsumerTestEvent(), _message())
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
    exchange = mock.AsyncMock()
    consumer = _consumer(config, stats, handlers=(HandlerAdapter(_boom),), exchange=exchange)
    message = _message({"event_type": "ConsumerTestEvent", "x-retry-count": 1})

    await consumer._process_message(message)
    await consumer.drain_retries()

    exchange.publish.assert_awaited_once()
    assert exchange.publish.await_args.args[0].headers["x-retry-count"] == 2
    assert exchange.publish.await_args.kwargs["routing_key"] == "Order.ConsumerTestEvent"
    message.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_failure_at_max_retries_goes_to_dlq_with_single_failure_unwrap(
    config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats
) -> None:
    dlq_exchange = mock.AsyncMock()
    consumer = _consumer(
        config, stats, handlers=(HandlerAdapter(_boom),), dlq_exchange=dlq_exchange
    )
    message = _message({"event_type": "ConsumerTestEvent", "x-retry-count": config.max_retries})

    await consumer._process_message(message)

    dlq_exchange.publish.assert_awaited_once()
    dlq_message = dlq_exchange.publish.await_args.args[0]
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
    connection = mock.MagicMock(_was_consuming=False)
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
