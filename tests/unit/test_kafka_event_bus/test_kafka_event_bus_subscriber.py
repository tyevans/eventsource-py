"""Subscriber, retry policy, and dispatch ordering regression tests for KafkaEventBus."""

from typing import Any
from uuid import uuid4

import pytest

from eventsource.adapters.kafka import KafkaEventBus, KafkaEventBusConfig
from eventsource.domain.event import DomainEvent

from ._fixtures import SampleKafkaEvent, mock_producer

__all__ = [
    "mock_producer",
    "test_handlers_resolve_when_event_type_field_differs_from_class_name",
    "test_publish_sends_all_events_before_awaiting_any",
    "test_retry_delay_comes_from_the_shared_policy",
]


def test_handlers_resolve_when_event_type_field_differs_from_class_name() -> None:
    """Regression: Kafka keyed subscriptions by class __name__ but looked them
    up by the event_type field from the message header. When those differ, the
    handler silently never fired."""

    class RenamedEvent(DomainEvent):
        event_type: str = "renamed.v1"
        aggregate_type: str = "Renamed"

    bus = KafkaEventBus(config=KafkaEventBusConfig(bootstrap_servers="localhost:9092"))

    async def handler(event: DomainEvent) -> None: ...

    bus.subscribe(RenamedEvent, handler)

    assert len(bus._handlers_for(RenamedEvent)) == 1


def test_retry_delay_comes_from_the_shared_policy() -> None:
    from eventsource.adapters._bus.retry import RetryPolicy

    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        retry_base_delay=2.0,
        retry_max_delay=30.0,
        retry_jitter=0.0,
        max_retries=5,
    )
    bus = KafkaEventBus(config=config)

    assert isinstance(bus._retry_policy, RetryPolicy)
    assert bus._consumer_loop._calculate_retry_delay(2) == 8.0


@pytest.mark.asyncio
async def test_publish_sends_all_events_before_awaiting_any(
    mock_producer: Any,
) -> None:
    """Regression: publish awaited a full broker round-trip per event."""
    bus = KafkaEventBus(config=KafkaEventBusConfig(bootstrap_servers="localhost:9092"))
    bus._connection_manager._producer = mock_producer
    bus._connection_manager._connected = True

    send_order: list[str] = []

    async def record_send(*args: Any, **kwargs: Any) -> Any:
        send_order.append("send")

        async def result() -> None:
            send_order.append("await")

        return result()

    mock_producer.send = record_send

    events: list[DomainEvent] = [SampleKafkaEvent(aggregate_id=uuid4()) for _ in range(3)]
    await bus.publish(events)

    # All three sends happen before any await completes.
    assert send_order[:3] == ["send", "send", "send"]
