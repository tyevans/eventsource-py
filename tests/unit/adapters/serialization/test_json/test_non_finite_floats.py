"""Tests verifying DomainEvent rejects non-finite floats at model construction time."""

from __future__ import annotations

from uuid import uuid4

import pytest


class TestDomainEventRejectsNonFiniteFloats:
    """
    Round 5: non-finite float rejection moved from json_dumps (a
    pre-serialization scan, deleted for the throughput hit it caused) to
    DomainEvent.model_config (allow_inf_nan=False in
    src/eventsource/events/base.py). This is the real fix for the main
    event path: the outbox writes via
    json.loads(event.model_dump_json()) (outbox.py:264, 557, 837), and
    pydantic's own model_dump_json() already nulls a non-finite float
    silently -- so the old json_dumps-level scan never actually protected
    that path at all; it only ever covered hand-built dicts. Rejecting at
    construction, before pydantic's own serialization ever runs, is
    earlier and covers every event, not just ones passed through
    json_dumps directly.
    """

    def test_domain_event_rejects_inf_at_construction(self) -> None:
        from pydantic import ValidationError

        from eventsource.domain.event import DomainEvent

        class MyEvent(DomainEvent):
            value: float

        with pytest.raises(ValidationError, match="finite_number|finite"):
            MyEvent(
                aggregate_id=uuid4(),
                aggregate_type="Test",
                value=float("inf"),
            )

    def test_domain_event_rejects_neg_inf_at_construction(self) -> None:
        from pydantic import ValidationError

        from eventsource.domain.event import DomainEvent

        class MyEvent(DomainEvent):
            value: float

        with pytest.raises(ValidationError, match="finite_number|finite"):
            MyEvent(
                aggregate_id=uuid4(),
                aggregate_type="Test",
                value=float("-inf"),
            )

    def test_domain_event_rejects_nan_at_construction(self) -> None:
        from pydantic import ValidationError

        from eventsource.domain.event import DomainEvent

        class MyEvent(DomainEvent):
            value: float

        with pytest.raises(ValidationError, match="finite_number|finite"):
            MyEvent(
                aggregate_id=uuid4(),
                aggregate_type="Test",
                value=float("nan"),
            )

    def test_domain_event_accepts_finite_floats(self) -> None:
        from eventsource.domain.event import DomainEvent

        class MyEvent(DomainEvent):
            value: float

        event = MyEvent(aggregate_id=uuid4(), aggregate_type="Test", value=3.14)
        assert event.value == 3.14

    def test_domain_event_rejects_non_finite_float_nested_in_payload_field(self) -> None:
        from pydantic import ValidationError

        from eventsource.domain.event import DomainEvent

        class MyEvent(DomainEvent):
            amounts: list[float]

        with pytest.raises(ValidationError, match="finite_number|finite"):
            MyEvent(
                aggregate_id=uuid4(),
                aggregate_type="Test",
                amounts=[1.0, 2.0, float("nan")],
            )

    def test_domain_event_still_frozen(self) -> None:
        from eventsource.domain.event import DomainEvent

        class MyEvent(DomainEvent):
            value: float

        event = MyEvent(aggregate_id=uuid4(), aggregate_type="Test", value=1.0)
        with pytest.raises(Exception):  # noqa: B017 - pydantic's ValidationError for frozen models
            event.value = 2.0
