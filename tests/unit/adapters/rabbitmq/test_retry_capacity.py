"""A failing message is dead-lettered, not dropped, when the retry scheduler is full.

Drives the real consume loop with a fake broker iterator. The scheduler is
capped at one pending retry; the second failure must reach the DLQ and the
consume loop must keep running.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from typing import Any
from unittest import mock
from uuid import uuid4

from eventsource.adapters._bus.handler_adapter import HandlerAdapter
from eventsource.adapters._bus.retry import RetryPolicy
from eventsource.adapters._bus.retry_scheduler import RetryScheduler
from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.consumer import RabbitMQConsumer
from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats
from eventsource.domain.event import DomainEvent


class CapacityProbed(DomainEvent):
    aggregate_type: str = "Capacity"


def _message(label: str) -> mock.MagicMock:
    return mock.MagicMock(
        headers={"event_type": "CapacityProbed"},
        message_id=f"msg-{label}-{uuid4()}",
        routing_key="Capacity.CapacityProbed",
        body=CapacityProbed(aggregate_id=uuid4()).model_dump_json().encode(),
        ack=mock.AsyncMock(),
        reject=mock.AsyncMock(),
    )


def _broker(messages: list[Any]) -> mock.MagicMock:
    @asynccontextmanager
    async def _iterator() -> AsyncIterator[AsyncIterator[Any]]:
        async def _gen() -> AsyncIterator[Any]:
            for message in messages:
                yield message

        yield _gen()

    return mock.MagicMock(iterator=_iterator)


async def test_retry_beyond_scheduler_capacity_is_dead_lettered_and_consumption_continues() -> None:
    config = RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="test-events",
        consumer_group="test-group",
        max_retries=3,
        retry_base_delay=0.2,  # first retry stays pending while the second message arrives
        retry_jitter=0.0,
    )
    dlq_exchange = mock.AsyncMock()

    async def handler(event: DomainEvent) -> None:
        raise RuntimeError("handler refuses every event")

    first, second = _message("first"), _message("second")
    with mock.patch(
        "eventsource.adapters.rabbitmq.consumer.RetryScheduler",
        partial(RetryScheduler, max_pending=1),
    ):
        consumer = RabbitMQConsumer(
            config=config,
            connection=mock.MagicMock(),
            topology=mock.MagicMock(
                exchange=mock.AsyncMock(),
                dlq_exchange=dlq_exchange,
                consumer_queue=_broker([first, second]),
            ),
            stats=RabbitMQEventBusStats(),
            retry_policy=RetryPolicy(
                config.retry_base_delay, config.retry_max_delay, config.retry_jitter, 0
            ),
            handlers_for=lambda _cls: (HandlerAdapter(handler),),
            resolve_event_class=lambda name: CapacityProbed if name == "CapacityProbed" else None,
            tracer=None,
            enable_tracing=False,
        )

    await consumer.start()
    await consumer.drain_retries()

    dlq_reasons = [call.args[0].headers["x-dlq-reason"] for call in dlq_exchange.publish.await_args_list]  # fmt: skip
    assert dlq_reasons == ["retry_capacity_exceeded"]
    second.ack.assert_awaited_once()
    first.ack.assert_awaited_once()
