"""Shared test helpers and sample events for KafkaConsumerLoop unit tests."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
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


class SampleConsumerEvent(DomainEvent):
    aggregate_type: str = "SampleAggregate"
    payload: str = ""


def _make_event() -> SampleConsumerEvent:
    return SampleConsumerEvent(aggregate_id=uuid4(), aggregate_version=1, payload="x")


def _make_message(event: DomainEvent, *, retry_count: int | None = None) -> Any:
    """Build a stand-in for an aiokafka ConsumerRecord."""
    headers: list[tuple[str, bytes]] = [
        ("event_type", event.event_type.encode("utf-8")),
        ("event_id", str(event.event_id).encode("utf-8")),
    ]
    if retry_count is not None:
        headers.append(("retry_count", str(retry_count).encode("utf-8")))
    return SimpleNamespace(
        value=EventSerializer().serialize(event),
        key=str(event.aggregate_id).encode("utf-8"),
        headers=headers,
        topic="events.stream",
        partition=0,
        offset=42,
    )


def _make_loop(
    *,
    handlers: tuple[HandlerAdapter, ...] = (),
    event_class: type[DomainEvent] | None = SampleConsumerEvent,
    max_retries: int = 3,
    enable_dlq: bool = True,
) -> tuple[KafkaConsumerLoop, Any, Any]:
    """Build a consumer loop wired to mock producer/consumer clients."""
    with patch.object(KafkaEventBusConfig, "_validate_security_config"):
        config = KafkaEventBusConfig(  # type: ignore[arg-type]
            bootstrap_servers="localhost:9092",
            max_retries=max_retries,
            enable_dlq=enable_dlq,
        )
    stats = KafkaEventBusStats()
    metrics = MagicMock()
    producer = AsyncMock()
    consumer = AsyncMock()

    connection = KafkaConnectionManager(config=config, stats=stats, metrics=metrics)
    connection._producer = producer
    connection._consumer = consumer
    connection._connected = True

    loop = KafkaConsumerLoop(
        config=config,
        connection=connection,
        serializer=EventSerializer(),
        stats=stats,
        metrics=metrics,
        retry_policy=RetryPolicy(
            base_delay=config.retry_base_delay,
            max_delay=config.retry_max_delay,
            jitter=config.retry_jitter,
            max_retries=config.max_retries,
        ),
        handlers_for=lambda _cls: handlers,
        resolve_event_class=lambda _name: event_class,
        tracer=create_tracer(__name__, False),
        enable_tracing=False,
        shutdown_event=asyncio.Event(),
    )
    return loop, producer, consumer
