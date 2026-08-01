"""Property: DomainEvent payload survives the library's canonical wire round trip.

Canonical pair identified by reading `src/eventsource/serialization/`:
`eventsource.adapters.serialization.json_dumps`/`json_loads` (orjson-backed) is the
pair the adapters actually use on the wire (e.g.
`eventsource.adapters._sql.dialect`), combined with
`DomainEvent.to_dict()`/`from_dict()` (pydantic `model_dump(mode="json")`
/ `model_validate`) for the object<->dict boundary. Rehydration goes
through `eventsource.events.registry.get_event_class`, keyed by the
event's own `event_type`, so the round trip is registry-mediated rather
than hardcoding the class.
"""

from typing import Any
from uuid import uuid4

from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.serialization import json_dumps, json_loads
from eventsource.events import DomainEvent
from eventsource.events.registry import default_registry, get_event_class


class RoundTripEvent(DomainEvent):
    """Test event with an arbitrary JSON-safe payload dict.

    Deliberately NOT decorated with ``@register_event`` at import time --
    other unit test modules (e.g. ``test_event_registry.py``) clear the
    default registry, and under randomized test ordering (pytest-randomly)
    an import-time registration can be wiped out before this module's own
    test runs. Instead, register/unregister around the test body so it
    passes regardless of collection order.
    """

    aggregate_type: str = "RoundTrip"
    payload: dict[str, Any]


json_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**53), max_value=2**53),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(max_size=50),
)

payloads = st.dictionaries(st.text(max_size=20), json_scalars, max_size=5)


@given(payload=payloads)
def test_event_round_trips_through_canonical_wire_path(payload: dict[str, Any]) -> None:
    default_registry.register(RoundTripEvent)
    try:
        original = RoundTripEvent(aggregate_id=uuid4(), payload=payload)

        wire = json_dumps(original.to_dict())
        data = json_loads(wire)

        event_class = get_event_class(data["event_type"])
        assert event_class is RoundTripEvent
        rehydrated = RoundTripEvent.from_dict(data)

        assert rehydrated.payload == original.payload
        assert rehydrated.event_id == original.event_id
        assert rehydrated == original
    finally:
        default_registry.unregister("RoundTripEvent")
