"""Property-based test for the aggregate memento (snapshot) round trip.

For any generated state, serialize -> restore must reproduce the same state
and version on a fresh instance. Example-based tests only ever exercise the
field values someone thought to write down; this attacks the round trip with
arbitrary text, integers, and list lengths instead.
"""

from uuid import uuid4

from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel

from eventsource.domain.aggregate import AggregateRoot
from eventsource.events.base import DomainEvent


class ThingState(BaseModel):
    name: str = ""
    count: int = 0
    tags: list[str] = []


class ThingAggregate(AggregateRoot[ThingState]):
    aggregate_type = "Thing"

    def _get_initial_state(self) -> ThingState:
        return ThingState()

    def _apply(self, event: DomainEvent) -> None:
        pass


@given(
    name=st.text(max_size=50),
    count=st.integers(),
    tags=st.lists(st.text(max_size=10), max_size=5),
)
def test_serialize_restore_round_trip(name: str, count: int, tags: list[str]) -> None:
    agg = ThingAggregate(uuid4())
    agg._state = ThingState(name=name, count=count, tags=tags)
    agg._version = 7

    dumped = agg._serialize_state()

    restored = ThingAggregate(agg.aggregate_id)
    restored._restore_from_snapshot(dumped, 7)

    assert restored.state == agg.state
    assert restored.version == 7
