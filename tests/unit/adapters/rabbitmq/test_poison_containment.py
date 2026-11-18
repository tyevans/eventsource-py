"""Poison-message containment for the RabbitMQ consume loop: retry and payload decoding.

Each test drives ``RabbitMQConsumer.start()`` with a fake queue that delivers a
poison message followed by a good one. The good message must be handled and the
poison message must reach the DLQ (or be retried), never stop the loop.
"""

from __future__ import annotations

import pytest

from tests.unit.adapters.rabbitmq._poison_harness import _Harness, _message


@pytest.mark.asyncio
async def test_non_integer_retry_count_does_not_stop_consumption() -> None:
    poison = _message(headers={"event_type": "ProbeEvent", "x-retry-count": "abc"}, label="poison")
    good = _message(label="good")
    harness = _Harness([poison, good], max_retries=0)

    await harness.run()

    assert harness.handled == ["poison", "good"]
    assert harness.dlq_headers()[0]["x-dlq-error-type"] == "ValueError"
    assert harness.dlq_headers()[0]["x-dlq-retry-count"] == 0
    poison.ack.assert_awaited_once()
    good.ack.assert_awaited_once()
    assert harness.stats.events_processed_success == 1


@pytest.mark.asyncio
async def test_negative_retry_count_with_failing_handler_is_retried_not_crashed() -> None:
    poison = _message(headers={"event_type": "ProbeEvent", "x-retry-count": "-1"}, label="poison")
    good = _message(label="good")
    harness = _Harness([poison, good], max_retries=3)

    await harness.run()

    assert harness.handled == ["poison", "good"]
    harness.exchange.publish.assert_awaited_once()
    republished = harness.exchange.publish.await_args.args[0]
    assert republished.headers["x-retry-count"] == 1
    poison.ack.assert_awaited_once()
    good.ack.assert_awaited_once()


@pytest.mark.asyncio
async def test_invalid_payload_is_dead_lettered_and_consumption_continues() -> None:
    poison = _message(body=b"\xff\xfe not json", label="poison")
    good = _message(label="good")
    harness = _Harness([poison, good])

    await harness.run()

    assert harness.handled == ["good"]
    assert harness.dlq_headers()[0]["x-dlq-error-type"] == "EventDecodeError"
    poison.ack.assert_awaited_once()
    good.ack.assert_awaited_once()
    poison.reject.assert_not_awaited()
