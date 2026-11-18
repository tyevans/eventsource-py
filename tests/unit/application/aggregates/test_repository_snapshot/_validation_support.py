"""Shared helpers for the snapshot load-validation test modules (TASK-0469)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.snapshots import Snapshot
from tests.unit.application.aggregates.test_repository_snapshot._fixtures import (
    _REGISTRY,
    TestAggregate,
    TestEvent,
)

LOGGER_NAME = "eventsource.application.aggregates.repository_query"
FORGED_STATE: dict[str, Any] = {"value": "forged-state-marker", "count": 999, "items": []}


def make_snapshot(
    aggregate_id: UUID,
    version: int,
    *,
    aggregate_type: str = "Test",
    state: dict[str, Any] | None = None,
) -> Snapshot:
    return Snapshot(
        aggregate_id=aggregate_id,
        aggregate_type=aggregate_type,
        version=version,
        state=dict(FORGED_STATE) if state is None else state,
        schema_version=1,
        created_at=datetime.now(UTC),
    )


async def append_events(
    store: InMemoryEventStore, aggregate_id: UUID, count: int, *, prefix: str = "event"
) -> None:
    stream = StreamId(aggregate_id=aggregate_id, category="Test")
    events = [
        TestEvent(
            aggregate_id=aggregate_id,
            aggregate_type="Test",
            aggregate_version=i + 1,
            value=f"{prefix}_{i}",
        )
        for i in range(count)
    ]
    await store.append(stream, events, ExpectedVersion.exact(0))


class StoreReturning(InMemorySnapshotStore):
    """Public snapshot store that hands back a fixed row for any lookup."""

    def __init__(self, row: Snapshot) -> None:
        super().__init__(enable_tracing=False)
        self._row = row

    async def get_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> Snapshot | None:
        return self._row


def build_repo(event_store: InMemoryEventStore, snapshot_store: Any = None) -> AggregateRepository:
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=TestAggregate,
        snapshot_store=snapshot_store,
    )


def new_event_store() -> InMemoryEventStore:
    return InMemoryEventStore(event_registry=_REGISTRY)
