"""An event declaring a different aggregate_type than its aggregate.

`aggregate_type` has one source -- the aggregate class (ADR 0046) -- and the
emit path stamps it. A divergent declaration on the event class used to be
silently overwritten, which turned a wrong declaration into a wrong stream
category that no save/load round-trip could reveal.
"""

from uuid import UUID, uuid4

import pytest

from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.decider import DeciderAggregate
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import AggregateTypeMismatchError


class Shipped(DomainEvent):
    """Declares the *wrong* aggregate_type on purpose."""

    aggregate_type: str = "Shipment"


class Created(DomainEvent):
    """Declares the matching aggregate_type -- redundant, but not wrong."""

    aggregate_type: str = "Order"


class Order(AggregateRoot[dict[str, str]]):
    aggregate_type = "Order"

    def _apply(self, event: DomainEvent) -> None:
        pass

    def _get_initial_state(self) -> dict[str, str]:
        return {}


def test_create_event_rejects_a_divergent_declaration() -> None:
    order = Order(uuid4())

    with pytest.raises(AggregateTypeMismatchError) as exc_info:
        order.create_event(Shipped)

    error = exc_info.value
    assert error.event_aggregate_type == "Shipment"
    assert error.aggregate_type == "Order"
    assert "Shipped" in str(error) and "Order" in str(error)


def test_create_event_accepts_a_matching_declaration() -> None:
    order = Order(uuid4())

    event = order.create_event(Created)

    assert event.aggregate_type == "Order"


class OrderDecider(DeciderAggregate[dict[str, str], object]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state(aggregate_id: UUID) -> dict[str, str]:
        return {"id": str(aggregate_id)}

    @staticmethod
    def evolve(state: dict[str, str], event: DomainEvent) -> dict[str, str]:
        return state

    @staticmethod
    def decide(command: object, state: dict[str, str]) -> list[DomainEvent]:
        return [Shipped(aggregate_id=UUID(state["id"]))]


def test_decider_rejects_a_divergent_declaration() -> None:
    decider = OrderDecider(uuid4())

    with pytest.raises(AggregateTypeMismatchError):
        decider.execute(object())
