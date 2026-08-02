"""Unit tests for the extracted ``KafkaPublisher`` (split-phase send/ack).

Covers partition key derivation, sequential send handoff before acks are
awaited, ack-failure propagation after all sends are handed off, and header
construction -- in isolation from ``KafkaEventBus``.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

pytest.importorskip("aiokafka", reason="aiokafka not installed")

from eventsource.adapters.kafka.config import KafkaEventBusConfig  # noqa: E402
from eventsource.adapters.kafka.connection import KafkaConnectionManager  # noqa: E402
from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics  # noqa: E402
from eventsource.adapters.kafka.models import KafkaEventBusStats  # noqa: E402
from eventsource.adapters.kafka.publisher import KafkaPublisher  # noqa: E402
from eventsource.adapters.kafka.serialization import EventSerializer  # noqa: E402
from eventsource.domain.event import DomainEvent  # noqa: E402
from eventsource.observability import create_tracer  # noqa: E402


class SamplePublisherEvent(DomainEvent):
    aggregate_type: str = "SampleAggregate"
    payload: str = ""


def _make_publisher(producer: Any) -> KafkaPublisher:
    with patch.object(KafkaEventBusConfig, "_validate_security_config"):
        config = KafkaEventBusConfig(bootstrap_servers="localhost:9092")  # type: ignore[arg-type]
    stats = KafkaEventBusStats()
    metrics = MagicMock(spec=KafkaEventBusMetrics)
    metrics.publish_errors = MagicMock()
    metrics.messages_published = MagicMock()

    connection = KafkaConnectionManager(config=config, stats=stats, metrics=metrics)
    connection._producer = producer
    connection._connected = True

    tracer = create_tracer(__name__, False)

    return KafkaPublisher(
        config=config,
        connection=connection,
        serializer=EventSerializer(),
        stats=stats,
        metrics=metrics,
        tracer=tracer,
        enable_tracing=False,
    )


class FakeProducer:
    """Mimics AIOKafkaProducer.send() returning a plain asyncio.Future."""

    def __init__(self) -> None:
        self.send_calls: list[dict[str, Any]] = []
        self._futures: list[asyncio.Future[Any]] = []

    async def send(self, **kwargs: Any) -> asyncio.Future[Any]:
        self.send_calls.append(kwargs)
        future: asyncio.Future[Any] = asyncio.get_event_loop().create_future()
        self._futures.append(future)
        return future

    def resolve_all(self, record_metadata_factory: Any) -> None:
        for future in self._futures:
            if not future.done():
                future.set_result(record_metadata_factory())

    def fail(self, index: int, exc: Exception) -> None:
        self._futures[index].set_exception(exc)


def _record_metadata(topic: str = "t", partition: int = 0, offset: int = 0) -> Any:
    md = MagicMock()
    md.topic = topic
    md.partition = partition
    md.offset = offset
    return md


class TestPartitionKey:
    def test_partition_key_is_aggregate_id_bytes(self) -> None:
        producer = FakeProducer()
        publisher = _make_publisher(producer)
        event = SamplePublisherEvent(aggregate_id=uuid4())

        key = publisher._get_partition_key(event)

        assert key == str(event.aggregate_id).encode("utf-8")


class TestSplitPhaseOrdering:
    @pytest.mark.asyncio
    async def test_two_events_same_aggregate_sent_in_order_before_acks_awaited(self) -> None:
        producer = FakeProducer()
        publisher = _make_publisher(producer)
        aggregate_id = uuid4()
        events = [
            SamplePublisherEvent(aggregate_id=aggregate_id, payload="first"),
            SamplePublisherEvent(aggregate_id=aggregate_id, payload="second"),
        ]

        task = asyncio.ensure_future(publisher.publish_all(events, background=False))
        # Yield control so publish_all can run up to the ack-gather point.
        for _ in range(5):
            await asyncio.sleep(0)
            if len(producer.send_calls) == 2:
                break

        # Both sends must have been handed off before we resolve any ack.
        assert len(producer.send_calls) == 2
        assert not task.done()

        producer.resolve_all(_record_metadata)
        await task

        # Sent in order for the same aggregate/partition key.
        assert producer.send_calls[0]["key"] == producer.send_calls[1]["key"]
        assert producer.send_calls[0]["value"] != producer.send_calls[1]["value"]


class TestAckFailure:
    @pytest.mark.asyncio
    async def test_failing_ack_raises_only_after_all_sends_handed_off(self) -> None:
        producer = FakeProducer()
        publisher = _make_publisher(producer)
        events = [SamplePublisherEvent(aggregate_id=uuid4()) for _ in range(3)]

        task = asyncio.ensure_future(publisher.publish_all(events, background=False))
        for _ in range(5):
            await asyncio.sleep(0)
            if len(producer.send_calls) == 3:
                break

        # All three sends handed off before any ack resolves.
        assert len(producer.send_calls) == 3

        producer.fail(1, RuntimeError("broker unavailable"))
        producer.resolve_all(_record_metadata)

        with pytest.raises(RuntimeError, match="broker unavailable"):
            await task


class TestHeaders:
    def test_headers_carry_event_type_name(self) -> None:
        producer = FakeProducer()
        publisher = _make_publisher(producer)
        event = SamplePublisherEvent(aggregate_id=uuid4())

        headers = publisher._create_headers(event)

        header_dict = dict(headers)
        assert header_dict["event_type"] == event.event_type.encode("utf-8")
