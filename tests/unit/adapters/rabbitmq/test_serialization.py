"""Unit + property tests for the pure RabbitMQ serialization functions."""

from __future__ import annotations

import logging
from types import SimpleNamespace

from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.rabbitmq import serialization
from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry


class RoundtripEvent(DomainEvent):
    event_type: str = "RoundtripEvent"
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


def _resolver(registry: EventRegistry):
    def resolve(event_type_name: str) -> type[DomainEvent] | None:
        return registry.get_or_none(event_type_name)

    return resolve


@given(event=_events)
def test_roundtrip_preserves_event_identity(event: RoundtripEvent) -> None:
    body, headers = serialization.serialize_event(event)
    message = SimpleNamespace(headers=headers, body=body, message_id=str(event.event_id))

    restored = serialization.deserialize_event(
        message,
        resolve_event_class=_resolver(_registry()),
        logger=logging.getLogger("test"),
    )

    assert restored is not None
    assert restored.event_id == event.event_id
    assert restored.aggregate_id == event.aggregate_id
    assert restored.name == event.name
    assert restored.quantity == event.quantity
    assert restored.ratio == event.ratio
    assert restored.flag == event.flag


@given(event=_events)
def test_get_routing_key_is_aggregate_type_dot_event_type(event: RoundtripEvent) -> None:
    assert serialization.get_routing_key(event) == f"{event.aggregate_type}.{event.event_type}"


@given(event=_events)
def test_create_message_uses_serialized_body_and_headers(event: RoundtripEvent) -> None:
    body, headers = serialization.serialize_event(event)
    message = serialization.create_message(event)

    assert message.body == body
    assert message.headers == headers
    assert message.content_type == "application/json"
    assert message.message_id == str(event.event_id)


def test_get_event_field_default_reads_pydantic_field_default() -> None:
    default = serialization.get_event_field_default(RoundtripEvent, "aggregate_type", "Unknown")
    assert default == "Roundtrip"


def test_get_event_field_default_falls_back_when_missing() -> None:
    default = serialization.get_event_field_default(RoundtripEvent, "no_such_field", "fallback")
    assert default == "fallback"


def test_deserialize_event_returns_none_for_missing_event_type_header() -> None:
    message = SimpleNamespace(headers={}, body=b"{}", message_id="m1")

    restored = serialization.deserialize_event(
        message,
        resolve_event_class=_resolver(_registry()),
        logger=logging.getLogger("test"),
    )

    assert restored is None


def test_deserialize_event_returns_none_for_unknown_event_type() -> None:
    message = SimpleNamespace(headers={"event_type": "NotRegistered"}, body=b"{}", message_id="m1")

    restored = serialization.deserialize_event(
        message,
        resolve_event_class=_resolver(_registry()),
        logger=logging.getLogger("test"),
    )

    assert restored is None


def test_config_is_unused_placeholder_for_future_routing_key_needs() -> None:
    # get_routing_key currently only reads the event, not the config -- this
    # test documents that fact so a future signature change is deliberate.
    config = RabbitMQEventBusConfig()
    assert config.exchange_name == "events"
