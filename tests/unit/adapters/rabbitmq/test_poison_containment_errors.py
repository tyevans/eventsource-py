"""Poison-message containment for the RabbitMQ consume loop: routing and error paths.

Covers unknown event types, failures outside the handler, and messages that
cannot be dead-lettered. The good message must still be handled.
"""

from __future__ import annotations

from typing import Any
from unittest import mock

import pytest

from tests.unit.adapters.rabbitmq._poison_harness import _body, _Harness, _message


@pytest.mark.asyncio
async def test_unknown_event_type_is_dead_lettered_and_consumption_continues() -> None:
    poison = _message(headers={"event_type": "Mystery"}, label="poison")
    good = _message(label="good")
    harness = _Harness([poison, good])

    await harness.run()

    assert harness.handled == ["good"]
    assert harness.dlq_headers()[0]["x-dlq-error-type"] == "EventDecodeError"
    assert harness.dlq_headers()[0]["x-dlq-reason"] == "EventDecodeError: Unknown event type"
    poison.ack.assert_awaited_once()
    good.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_unexpected_error_outside_handler_path_is_contained() -> None:
    class _FailsOnFirstHeaderRead(mock.MagicMock):
        """Headers raise once: simulates an unexpected error in per-message processing."""

        def __init__(self) -> None:
            super().__init__(
                message_id="msg-poison",
                routing_key="Order.ProbeEvent",
                body=_body("poison"),
                content_type="application/json",
                content_encoding="utf-8",
                ack=mock.AsyncMock(),
                reject=mock.AsyncMock(),
            )
            self._reads = 0

        @property
        def headers(self) -> dict[str, Any]:
            self._reads += 1
            if self._reads == 1:
                raise RuntimeError("header table corrupted")
            return {"event_type": "ProbeEvent"}

    poison = _FailsOnFirstHeaderRead()
    good = _message(label="good")
    harness = _Harness([poison, good])

    await harness.run()

    assert harness.handled == ["good"]
    assert harness.dlq_headers()[0]["x-dlq-error-type"] == "RuntimeError"
    poison.ack.assert_awaited_once()
    good.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_message_that_cannot_be_dead_lettered_is_rejected_without_requeue() -> None:
    class _AlwaysBroken(mock.MagicMock):
        @property
        def headers(self) -> dict[str, Any]:
            raise RuntimeError("header table corrupted")

    poison = _AlwaysBroken(
        message_id="msg-poison",
        routing_key="Order.ProbeEvent",
        body=_body("poison"),
        ack=mock.AsyncMock(),
        reject=mock.AsyncMock(),
    )
    good = _message(label="good")
    harness = _Harness([poison, good])
    harness.dlq_exchange.publish.side_effect = RuntimeError("broker unavailable")

    await harness.run()

    assert harness.handled == ["good"]
    poison.reject.assert_awaited_once_with(requeue=False)
    good.ack.assert_awaited_once()
