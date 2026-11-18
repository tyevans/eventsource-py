"""BackgroundScheduler bounds: finite max_pending and inline writes at capacity (TASK-0486).

Every wait in these tests is bounded by GATE_TIMEOUT, and every gate is released
in a `finally`, so a regression fails in seconds with a clear assertion instead
of hanging the shared session event loop until pytest-timeout fires.
"""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.application.aggregates.snapshotting import BackgroundScheduler, EveryNEvents
from eventsource.ports.snapshots import Snapshot
from tests.unit.application.aggregates.test_repository_snapshot._fixtures import (
    _REGISTRY,
    TestAggregate,
)

GATE_TIMEOUT = 5.0


class _GatedSnapshotStore(InMemorySnapshotStore):
    """Snapshot store that holds writes for `gated` aggregates until a gate opens."""

    def __init__(self, gate: asyncio.Event) -> None:
        super().__init__()
        self._gate = gate
        self.gated: set[UUID] = set()
        self.completed: list[UUID] = []

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        if snapshot.aggregate_id in self.gated:
            await asyncio.wait_for(self._gate.wait(), timeout=GATE_TIMEOUT)
        await super().save_snapshot(snapshot)
        self.completed.append(snapshot.aggregate_id)


async def test_default_capacity_runs_next_write_inline() -> None:
    gate = asyncio.Event()
    sched = BackgroundScheduler()
    try:
        blocked = [sched.schedule(_wait_then_none(gate), aggregate_type="T", aggregate_id=uuid4()) for _ in range(1000)]  # type: ignore[arg-type] # fmt: skip
        for call in blocked:
            await call
        assert sched.pending_count == 1000

        ran_inline: list[bool] = []

        async def inline_write() -> None:
            ran_inline.append(True)

        await sched.schedule(inline_write(), aggregate_type="T", aggregate_id=uuid4())  # type: ignore[arg-type]
        assert ran_inline == [True]
        assert sched.pending_count == 1000
    finally:
        gate.set()
        await asyncio.wait_for(sched.await_pending(), timeout=GATE_TIMEOUT)


async def test_repository_writes_inline_when_scheduler_is_at_capacity() -> None:
    gate = asyncio.Event()
    store = _GatedSnapshotStore(gate)
    repo = AggregateRepository(
        event_store=InMemoryEventStore(event_registry=_REGISTRY),
        aggregate_factory=TestAggregate,
        snapshot_store=store,
        snapshot_policy=EveryNEvents(1),
        snapshot_scheduler=BackgroundScheduler(max_pending=1),
    )

    first = TestAggregate(uuid4())
    second = TestAggregate(uuid4())
    try:
        first.do_something("first")
        store.gated.add(first.aggregate_id)
        await asyncio.wait_for(repo.save(first), timeout=GATE_TIMEOUT)

        # The first snapshot is parked behind the gate: scheduled, not finished.
        assert repo.pending_snapshot_count == 1
        assert store.completed == []

        second.do_something("second")
        await asyncio.wait_for(repo.save(second), timeout=GATE_TIMEOUT)

        # At capacity, the second snapshot ran inline during save(), while the
        # first one is still pending.
        assert store.completed == [second.aggregate_id]
        assert repo.pending_snapshot_count == 1
    finally:
        gate.set()
        drained = await asyncio.wait_for(repo.await_pending_snapshots(), timeout=GATE_TIMEOUT)

    assert drained == 1
    assert store.completed == [second.aggregate_id, first.aggregate_id]


async def _wait_then_none(gate: asyncio.Event) -> None:
    await gate.wait()
