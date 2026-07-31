"""SnapshotStore scenarios: save latency and load latency by state size."""

import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from bench.adapters.base import BenchAdapter
from bench.core.domain import SNAPSHOT_SIZES, make_snapshot_state
from bench.core.runner import Measurement, Scenario
from eventsource.ports.snapshots import Snapshot, SnapshotStore


def _make_snapshot(size: str, version: int = 1) -> Snapshot:
    return Snapshot(
        aggregate_id=uuid4(),
        aggregate_type="Bench",
        version=version,
        state=make_snapshot_state(SNAPSHOT_SIZES[size]),
        schema_version=1,
        created_at=datetime.now(UTC),
    )


async def _save(
    store: SnapshotStore, params: dict[str, Any], iterations: int, prepared: Any
) -> Measurement:
    durations: list[float] = []
    start = time.perf_counter()
    for _ in range(iterations):
        snapshot = _make_snapshot(params["size"])
        t0 = time.perf_counter()
        await store.save_snapshot(snapshot)
        durations.append(time.perf_counter() - t0)
    return Measurement(
        elapsed_s=time.perf_counter() - start,
        operations=iterations,
        durations_s=durations,
    )


async def _prepare_load(
    adapter: BenchAdapter[Any], store: SnapshotStore, params: dict[str, Any]
) -> Snapshot:
    snapshot = _make_snapshot(params["size"])
    await store.save_snapshot(snapshot)
    return snapshot


async def _load(
    store: SnapshotStore, params: dict[str, Any], iterations: int, prepared: Any
) -> Measurement:
    snapshot: Snapshot = prepared
    durations: list[float] = []
    start = time.perf_counter()
    for _ in range(iterations):
        t0 = time.perf_counter()
        loaded = await store.get_snapshot(snapshot.aggregate_id, snapshot.aggregate_type)
        durations.append(time.perf_counter() - t0)
        if loaded is None:
            raise RuntimeError("snapshot vanished during load benchmark")
    return Measurement(
        elapsed_s=time.perf_counter() - start,
        operations=iterations,
        durations_s=durations,
    )


SNAPSHOT_SCENARIOS: list[Scenario] = [
    Scenario(
        name="snapshot.save",
        interface="snapshot",
        metric="latency",
        grid={"size": ["small", "medium", "large"]},
        func=_save,
    ),
    Scenario(
        name="snapshot.load",
        interface="snapshot",
        metric="latency",
        grid={"size": ["small", "medium", "large"]},
        func=_load,
        prepare=_prepare_load,
    ),
]
