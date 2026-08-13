"""An event naming a different aggregate_id than the aggregate emitting it.

`aggregate_id` is the stream key. An event that names a foreign aggregate is
appended to a stream that disowns it: the aggregate it claims to belong to
never loads it, and the aggregate that emitted it carries an event about
someone else. Neither side can see the disagreement on a save/load round-trip,
which is the exact failure event sourcing is supposed to preclude.

Mirrors `test_aggregate_type_mismatch.py` -- same guard, one field over.
"""

from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest

from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.decider import DeciderAggregate
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import AggregateIdMismatchError


class Shipped(DomainEvent):
    pass


class Order(AggregateRoot[dict[str, str]]):
    aggregate_type = "Order"

    def _apply(self, event: DomainEvent) -> None:
        pass

    def _get_initial_state(self) -> dict[str, str]:
        return {}


def test_create_event_rejects_a_foreign_aggregate_id() -> None:
    order = Order(uuid4())
    foreign = uuid4()

    with pytest.raises(AggregateIdMismatchError) as exc_info:
        order.create_event(Shipped, aggregate_id=foreign)

    error = exc_info.value
    assert error.event_aggregate_id == foreign
    assert error.aggregate_id == order.aggregate_id
    assert "Shipped" in str(error)


def test_create_event_accepts_a_matching_aggregate_id() -> None:
    order = Order(uuid4())

    event = order.create_event(Shipped, aggregate_id=order.aggregate_id)

    assert event.aggregate_id == order.aggregate_id


def test_create_event_stamps_the_aggregate_id_when_unset() -> None:
    order = Order(uuid4())

    event = order.create_event(Shipped)

    assert event.aggregate_id == order.aggregate_id


@dataclass(frozen=True)
class ShipIt:
    target_id: UUID


class OrderDecider(DeciderAggregate[dict[str, str], ShipIt]):
    aggregate_type = "Order"

    @staticmethod
    def initial_state() -> dict[str, str]:
        return {}

    @staticmethod
    def evolve(state: dict[str, str], event: DomainEvent) -> dict[str, str]:
        return state

    @staticmethod
    def decide(command: ShipIt, state: dict[str, str]) -> list[DomainEvent]:
        return [Shipped(aggregate_id=command.target_id, aggregate_type="Order")]


def test_decider_rejects_an_event_targeting_another_aggregate() -> None:
    decider = OrderDecider(uuid4())
    foreign = uuid4()

    with pytest.raises(AggregateIdMismatchError) as exc_info:
        decider.execute(ShipIt(target_id=foreign))

    error = exc_info.value
    assert error.event_aggregate_id == foreign
    assert error.aggregate_id == decider.aggregate_id
    # The command is the culprit on this path, so the message names it.
    assert "ShipIt" in str(error)


def test_decider_accepts_an_event_targeting_itself() -> None:
    decider = OrderDecider(uuid4())

    events = decider.execute(ShipIt(target_id=decider.aggregate_id))

    assert [event.aggregate_id for event in events] == [decider.aggregate_id]


def test_decider_leaves_state_untouched_when_targeting_is_rejected() -> None:
    decider = OrderDecider(uuid4())

    with pytest.raises(AggregateIdMismatchError):
        decider.execute(ShipIt(target_id=uuid4()))

    assert decider.uncommitted_events == []
