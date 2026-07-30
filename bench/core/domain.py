"""Shared events, payload generators, and a tiny aggregate for benchmarks.

Payload sizes are defined here once so every scenario benchmarks identical
data (spec: fairness rules).
"""

from typing import Any
from uuid import UUID

from pydantic import BaseModel

from eventsource import DomainEvent, EventRegistry
from eventsource.aggregates.base import DeclarativeAggregate
from eventsource.handlers import handles

PAYLOAD_SIZES: dict[str, int] = {"small": 200, "large": 5_000}
SNAPSHOT_SIZES: dict[str, int] = {"small": 1_000, "medium": 50_000, "large": 500_000}


class BenchEvent(DomainEvent):
    """Generic benchmark event with a size-controlled payload."""

    event_type: str = "BenchEvent"
    aggregate_type: str = "Bench"
    payload: str = ""
    seq: int = 0


class BenchCounterIncremented(DomainEvent):
    event_type: str = "BenchCounterIncremented"
    aggregate_type: str = "BenchCounter"
    increment: int = 1


class BenchCounterState(BaseModel):
    counter_id: UUID
    value: int = 0


class BenchCounter(DeclarativeAggregate[BenchCounterState]):
    """Minimal aggregate for the end-to-end repository benchmark."""

    aggregate_type = "BenchCounter"

    def _get_initial_state(self) -> BenchCounterState:
        return BenchCounterState(counter_id=self.aggregate_id)

    @handles(BenchCounterIncremented)
    def _on_incremented(self, event: BenchCounterIncremented) -> None:
        if self._state is None:
            self._state = self._get_initial_state()
        self._state = self._state.model_copy(update={"value": self._state.value + event.increment})

    def increment(self, amount: int = 1) -> None:
        event = BenchCounterIncremented(
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            aggregate_version=self.get_next_version(),
            increment=amount,
        )
        self._raise_event(event)


def make_events(
    aggregate_id: UUID,
    count: int,
    start_version: int = 1,
    payload: str = "small",
) -> list[BenchEvent]:
    body = "x" * PAYLOAD_SIZES[payload]
    return [
        BenchEvent(
            aggregate_id=aggregate_id,
            aggregate_version=start_version + i,
            payload=body,
            seq=i,
        )
        for i in range(count)
    ]


def make_snapshot_state(size_bytes: int) -> dict[str, Any]:
    return {"blob": "x" * size_bytes}


def make_registry() -> EventRegistry:
    registry = EventRegistry()
    registry.register(BenchEvent)
    registry.register(BenchCounterIncremented)
    return registry
