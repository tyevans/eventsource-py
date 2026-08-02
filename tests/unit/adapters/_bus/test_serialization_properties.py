"""Roundtrip properties for each backend's serialize/deserialize pair.

Redis, Kafka, and RabbitMQ each implement serialization independently. These
properties assert the three agree on the one thing that matters: an event
survives the round trip unchanged.
"""

from types import SimpleNamespace

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource import KAFKA_AVAILABLE, RABBITMQ_AVAILABLE, REDIS_AVAILABLE
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry


class RoundtripEvent(DomainEvent):
    aggregate_type: str = "Roundtrip"
    name: str = ""
    quantity: int = 0
    ratio: float = 0.0
    flag: bool = False


_events = st.builds(
    RoundtripEvent,
    aggregate_id=st.uuids(),
    name=st.text(max_size=200),
    quantity=st.integers(min_value=-(2**31), max_value=2**31),
    ratio=st.floats(allow_nan=False, allow_infinity=False, width=32),
    flag=st.booleans(),
)


def _registry() -> EventRegistry:
    registry = EventRegistry()
    registry.register(RoundtripEvent)
    return registry


@pytest.mark.skipif(not REDIS_AVAILABLE, reason="Redis not installed")
@given(event=_events)
def test_redis_serialization_roundtrip_is_identity(event: RoundtripEvent) -> None:
    from eventsource.adapters.redis.bus import RedisEventBus, RedisEventBusConfig

    bus = RedisEventBus(
        config=RedisEventBusConfig(redis_url="redis://localhost:6379"),
        event_registry=_registry(),
    )

    wire = bus._serialize_event(event)
    restored = bus._deserialize_event(event.event_type, wire)

    assert restored is not None
    assert restored.event_id == event.event_id
    assert restored.aggregate_id == event.aggregate_id
    assert restored.name == event.name
    assert restored.quantity == event.quantity
    assert restored.ratio == event.ratio
    assert restored.flag == event.flag


@pytest.mark.skipif(not REDIS_AVAILABLE, reason="Redis not installed")
@given(event=_events)
def test_redis_payload_is_authoritative_not_the_flat_fields(
    event: RoundtripEvent,
) -> None:
    """The flat top-level fields are write-only index columns.

    tenant_id is written as "" when None, which would be wrong if anything
    read it back. Deserialization must use `payload` alone.
    """
    from eventsource.adapters.redis.bus import RedisEventBus, RedisEventBusConfig

    bus = RedisEventBus(
        config=RedisEventBusConfig(redis_url="redis://localhost:6379"),
        event_registry=_registry(),
    )

    wire = bus._serialize_event(event)
    wire["tenant_id"] = "garbage-not-a-uuid"
    wire["aggregate_type"] = "WrongType"

    restored = bus._deserialize_event(event.event_type, wire)

    assert restored is not None
    assert restored.tenant_id == event.tenant_id
    assert restored.aggregate_type == event.aggregate_type


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="Kafka not installed")
@given(event=_events)
def test_kafka_serialization_roundtrip_is_identity(event: RoundtripEvent) -> None:
    from eventsource.adapters.kafka import KafkaEventBus, KafkaEventBusConfig

    bus = KafkaEventBus(
        config=KafkaEventBusConfig(bootstrap_servers="localhost:9092"),
        event_registry=_registry(),
    )

    body = bus._serialize_event(event)
    headers = [
        ("event_id", str(event.event_id).encode("utf-8")),
        ("event_type", event.event_type.encode("utf-8")),
        ("aggregate_id", str(event.aggregate_id).encode("utf-8")),
        ("aggregate_type", event.aggregate_type.encode("utf-8")),
    ]
    message = SimpleNamespace(headers=headers, value=body)

    restored = bus._deserialize_message(message)

    assert restored.event_id == event.event_id
    assert restored.aggregate_id == event.aggregate_id
    assert restored.name == event.name
    assert restored.quantity == event.quantity
    assert restored.ratio == event.ratio
    assert restored.flag == event.flag


@pytest.mark.skipif(not RABBITMQ_AVAILABLE, reason="RabbitMQ not installed")
@given(event=_events)
def test_rabbitmq_serialization_roundtrip_is_identity(event: RoundtripEvent) -> None:
    import logging

    from eventsource.adapters.rabbitmq import serialization

    registry = _registry()

    body, headers = serialization.serialize_event(event)
    message = SimpleNamespace(headers=headers, body=body, message_id=str(event.event_id))

    restored = serialization.deserialize_event(
        message,
        resolve_event_class=registry.get_or_none,
        logger=logging.getLogger("test"),
    )

    assert restored is not None
    assert restored.event_id == event.event_id
    assert restored.aggregate_id == event.aggregate_id
    assert restored.name == event.name
    assert restored.quantity == event.quantity
    assert restored.ratio == event.ratio
    assert restored.flag == event.flag
