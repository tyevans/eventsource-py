"""End-to-end AggregateRepository benchmark: load -> mutate -> save."""

import time
from typing import Any
from uuid import UUID, uuid4

from bench.adapters.base import BenchAdapter
from bench.core.domain import BenchCounter
from bench.core.runner import Measurement, Scenario
from eventsource.aggregates.repository import AggregateRepository
from eventsource.snapshots.interface import SnapshotStore
from eventsource.stores.interface import EventStore

SNAPSHOT_THRESHOLD = 100


def _make_repo(
    store: EventStore, snapshot_store: SnapshotStore | None, snapshots: str
) -> AggregateRepository[BenchCounter]:
    if snapshots == "threshold":
        return AggregateRepository(
            event_store=store,
            aggregate_factory=BenchCounter,
            snapshot_store=snapshot_store,
            snapshot_threshold=SNAPSHOT_THRESHOLD,
            snapshot_mode="sync",
            enable_tracing=False,
        )
    return AggregateRepository(
        event_store=store,
        aggregate_factory=BenchCounter,
        enable_tracing=False,
    )


async def _prepare_e2e(
    adapter: BenchAdapter[Any],
    resource: tuple[EventStore, SnapshotStore],
    params: dict[str, Any],
) -> UUID:
    store, snapshot_store = resource
    repo = _make_repo(store, snapshot_store, params["snapshots"])
    aggregate_id = uuid4()
    aggregate = repo.create_new(aggregate_id)
    remaining = params["stream_length"]
    chunk = 100
    while remaining > 0:
        for _ in range(min(chunk, remaining)):
            aggregate.increment()
        await repo.save(aggregate)
        remaining -= chunk
    return aggregate_id


async def _load_mutate_save(
    resource: tuple[EventStore, SnapshotStore],
    params: dict[str, Any],
    iterations: int,
    prepared: Any,
) -> Measurement:
    store, snapshot_store = resource
    aggregate_id: UUID = prepared
    repo = _make_repo(store, snapshot_store, params["snapshots"])
    durations: list[float] = []
    start = time.perf_counter()
    for _ in range(iterations):
        t0 = time.perf_counter()
        aggregate = await repo.load(aggregate_id)
        aggregate.increment()
        await repo.save(aggregate)
        durations.append(time.perf_counter() - t0)
    return Measurement(
        elapsed_s=time.perf_counter() - start,
        operations=iterations,
        durations_s=durations,
    )


E2E_SCENARIOS: list[Scenario] = [
    Scenario(
        name="e2e.load_mutate_save",
        interface="e2e",
        metric="latency",
        grid={"stream_length": [100, 1000, 10000], "snapshots": ["none", "threshold"]},
        func=_load_mutate_save,
        prepare=_prepare_e2e,
    ),
]
