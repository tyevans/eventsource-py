"""Payload values stay out of RabbitMQ consumer logs and DLQ headers.

The consumer receives an event whose ``aggregate_id`` is the marker. That field
fails UUID validation, so pydantic's message would echo the marker if the
consumer logged ``str(error)``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
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

MARKER = "victim-secret@example.com"


class PaymentRecorded(DomainEvent):
    aggregate_type: str = "Payment"
    amount: int = 0


def _bad_body() -> bytes:
    return json.dumps({"aggregate_type": "Payment", "aggregate_id": MARKER, "amount": 1}).encode()


def _message(body: bytes, headers: dict[str, Any] | None = None) -> mock.MagicMock:
    return mock.MagicMock(
        headers=headers if headers is not None else {"event_type": "PaymentRecorded"},
        message_id=f"msg-{uuid4()}",
        routing_key="Payment.PaymentRecorded",
        body=body,
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


def _consumer(messages: list[Any]) -> tuple[RabbitMQConsumer, mock.AsyncMock]:
    config = RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost:5672/",
        exchange_name="test-events",
        consumer_group="test-group",
        max_retries=0,
        retry_base_delay=0.001,
        retry_jitter=0.0,
    )
    dlq_exchange = mock.AsyncMock()

    async def handler(event: DomainEvent) -> None:  # never reached for invalid input
        raise AssertionError("handler must not run for an undecodable event")

    consumer = RabbitMQConsumer(
        config=config,
        connection=mock.MagicMock(),
        topology=mock.MagicMock(
            exchange=mock.AsyncMock(),
            dlq_exchange=dlq_exchange,
            consumer_queue=_queue(messages),
        ),
        stats=RabbitMQEventBusStats(),
        retry_policy=RetryPolicy(
            config.retry_base_delay, config.retry_max_delay, config.retry_jitter, 0
        ),
        handlers_for=lambda _cls: (HandlerAdapter(handler),),
        resolve_event_class=lambda name: PaymentRecorded if name == "PaymentRecorded" else None,
        tracer=None,
        enable_tracing=False,
    )
    return consumer, dlq_exchange


def _assert_marker_absent_from_logs(caplog: pytest.LogCaptureFixture) -> None:
    assert caplog.records, "expected log records from the failure path"
    for record in caplog.records:
        assert MARKER not in record.getMessage(), record.getMessage()
        assert MARKER not in repr(record.args)
        assert MARKER not in repr(record.__dict__)


async def test_undecodable_event_keeps_marker_out_of_logs_and_dlq_headers(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    consumer, dlq_exchange = _consumer([_message(_bad_body())])

    await consumer.start()
    await consumer.drain_retries()

    assert any("Failed to deserialize event" in r.getMessage() for r in caplog.records)
    _assert_marker_absent_from_logs(caplog)
    dlq_headers = [call.args[0].headers for call in dlq_exchange.publish.await_args_list]
    assert dlq_headers, "undecodable message should reach the DLQ"
    assert MARKER not in repr(dlq_headers)


async def test_unknown_event_type_header_stays_out_of_message_text(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    forged = "Forged\nFAKE-LEVEL victim-secret@example.com"
    consumer, _dlq = _consumer([_message(_bad_body(), headers={"event_type": forged})])

    await consumer.start()
    await consumer.drain_retries()

    for record in caplog.records:
        assert forged not in record.getMessage()
        assert "\nFAKE-LEVEL" not in record.getMessage()
