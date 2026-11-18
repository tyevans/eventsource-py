"""Shared consume-loop harness for the RabbitMQ poison-containment tests.

``_Harness`` drives ``RabbitMQConsumer.start()`` against a fake queue and
exposes the handled labels and the DLQ publish headers to the test modules.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest import mock
from uuid import uuid4

from eventsource.adapters._bus.handler_adapter import HandlerAdapter
from eventsource.adapters._bus.retry import RetryPolicy
from eventsource.domain.event import DomainEvent

from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig  # isort: skip
from eventsource.adapters.rabbitmq.consumer import RabbitMQConsumer  # isort: skip
from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats  # isort: skip


class ProbeEvent(DomainEvent):
    aggregate_type: str = "Order"
    label: str = "good"


def _body(label: str = "good") -> bytes:
    payload = {"aggregate_type": "Order", "aggregate_id": str(uuid4()), "label": label}
    return json.dumps(payload).encode()


def _message(
    *,
    headers: dict[str, Any] | None = None,
    body: bytes | None = None,
    label: str = "good",
) -> mock.MagicMock:
    return mock.MagicMock(
        headers=headers if headers is not None else {"event_type": "ProbeEvent"},
        message_id=f"msg-{label}",
        routing_key="Order.ProbeEvent",
        content_type="application/json",
        content_encoding="utf-8",
        body=body if body is not None else _body(label),
        ack=mock.AsyncMock(),
        reject=mock.AsyncMock(),
    )


def _queue(messages: list[Any]) -> mock.MagicMock:
    @asynccontextmanager
    async def _iterator() -> AsyncIterator[AsyncIterator[Any]]:
        async def _gen() -> AsyncIterator[Any]:
            for message in messages:
                yield message

        yield _gen()

    return mock.MagicMock(iterator=_iterator)


def _config(max_retries: int = 3) -> RabbitMQEventBusConfig:
    return RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="test-events",
        consumer_group="test-group",
        max_retries=max_retries,
        retry_base_delay=0.001,
        retry_jitter=0.0,
    )


class _Harness:
    def __init__(self, messages: list[Any], *, max_retries: int = 3) -> None:
        self.config = _config(max_retries)
        self.stats = RabbitMQEventBusStats()
        self.handled: list[str] = []
        self.exchange = mock.AsyncMock()
        self.dlq_exchange = mock.AsyncMock()

        async def handler(event: DomainEvent) -> None:
            assert isinstance(event, ProbeEvent)
            self.handled.append(event.label)
            if event.label == "poison":
                raise ValueError("handler rejects poison")

        self.consumer = RabbitMQConsumer(
            config=self.config,
            connection=mock.MagicMock(),
            topology=mock.MagicMock(
                exchange=self.exchange,
                dlq_exchange=self.dlq_exchange,
                consumer_queue=_queue(messages),
            ),
            stats=self.stats,
            retry_policy=RetryPolicy(
                self.config.retry_base_delay,
                self.config.retry_max_delay,
                self.config.retry_jitter,
                self.config.max_retries,
            ),
            handlers_for=lambda _cls: (HandlerAdapter(handler),),
            resolve_event_class=lambda name: ProbeEvent if name == "ProbeEvent" else None,
            tracer=None,
            enable_tracing=False,
        )

    async def run(self) -> None:
        await self.consumer.start()
        await self.consumer.drain_retries()

    def dlq_headers(self) -> list[dict[str, Any]]:
        return [call.args[0].headers for call in self.dlq_exchange.publish.await_args_list]
