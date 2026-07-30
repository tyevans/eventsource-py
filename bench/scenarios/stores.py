"""EventStore scenarios: append, read, concurrency, contention."""

import asyncio
import time
from typing import Any
from uuid import UUID, uuid4

from bench.adapters.base import BenchAdapter
from bench.core.domain import make_events
from bench.core.runner import Measurement, Scenario
from eventsource.exceptions import OptimisticLockError
from eventsource.stores.interface import EventStore


async def populate_stream(
    store: EventStore,
    aggregate_id: UUID,
    count: int,
    payload: str = "small",
    chunk: int = 500,
) -> None:
    version = 0
    while version < count:
        n = min(chunk, count - version)
        events = make_events(aggregate_id, n, start_version=version + 1, payload=payload)
        await store.append_events(aggregate_id, "Bench", events, expected_version=version)
        version += n


async def _append_batch(
    store: EventStore, params: dict[str, Any], iterations: int, prepared: Any
) -> Measurement:
    batch_size: int = params["batch_size"]
    payload: str = params["payload"]
    durations: list[float] = []
    start = time.perf_counter()
    for _ in range(iterations):
        aggregate_id = uuid4()
        events = make_events(aggregate_id, batch_size, payload=payload)
        t0 = time.perf_counter()
        await store.append_events(aggregate_id, "Bench", events, expected_version=0)
        durations.append(time.perf_counter() - t0)
    return Measurement(
        elapsed_s=time.perf_counter() - start,
        operations=iterations * batch_size,
        durations_s=durations,
    )


async def _prepare_read_stream(
    adapter: BenchAdapter[Any], store: EventStore, params: dict[str, Any]
) -> UUID:
    aggregate_id = uuid4()
    await populate_stream(store, aggregate_id, params["stream_length"])
    return aggregate_id


async def _read_stream(
    store: EventStore, params: dict[str, Any], iterations: int, prepared: Any
) -> Measurement:
    aggregate_id: UUID = prepared
    stream_length: int = params["stream_length"]
    durations: list[float] = []
    start = time.perf_counter()
    for _ in range(iterations):
        t0 = time.perf_counter()
        stream = await store.get_events(aggregate_id, "Bench")
        durations.append(time.perf_counter() - t0)
        if stream.version != stream_length:
            raise RuntimeError(f"expected {stream_length} events, read {stream.version}")
    return Measurement(
        elapsed_s=time.perf_counter() - start,
        operations=iterations * stream_length,
        durations_s=durations,
    )


async def _concurrent_append(
    store: EventStore, params: dict[str, Any], iterations: int, prepared: Any
) -> Measurement:
    writers: int = params["writers"]
    ops_per_writer = max(1, iterations // writers)

    async def writer() -> None:
        aggregate_id = uuid4()
        for version in range(ops_per_writer):
            events = make_events(aggregate_id, 1, start_version=version + 1)
            await store.append_events(aggregate_id, "Bench", events, expected_version=version)

    start = time.perf_counter()
    async with asyncio.TaskGroup() as group:
        for _ in range(writers):
            group.create_task(writer())
    return Measurement(
        elapsed_s=time.perf_counter() - start,
        operations=writers * ops_per_writer,
    )


async def _contended_append(
    store: EventStore, params: dict[str, Any], iterations: int, prepared: Any
) -> Measurement:
    writers: int = params["writers"]
    ops_per_writer = max(1, iterations // writers)
    aggregate_id = uuid4()
    conflicts = 0
    lock = asyncio.Lock()

    async def writer() -> None:
        nonlocal conflicts
        done = 0
        while done < ops_per_writer:
            version = await store.get_stream_version(aggregate_id, "Bench")
            events = make_events(aggregate_id, 1, start_version=version + 1)
            try:
                result = await store.append_events(
                    aggregate_id, "Bench", events, expected_version=version
                )
                conflicted = result.conflict
            except OptimisticLockError:
                conflicted = True
            if conflicted:
                async with lock:
                    conflicts += 1
                continue
            done += 1

    start = time.perf_counter()
    async with asyncio.TaskGroup() as group:
        for _ in range(writers):
            group.create_task(writer())
    return Measurement(
        elapsed_s=time.perf_counter() - start,
        operations=writers * ops_per_writer,
        counters={"conflicts": conflicts},
    )


STORE_SCENARIOS: list[Scenario] = [
    Scenario(
        name="store.append_batch",
        interface="store",
        metric="latency",
        grid={"batch_size": [1, 10, 100, 1000], "payload": ["small", "large"]},
        func=_append_batch,
    ),
    Scenario(
        name="store.read_stream",
        interface="store",
        metric="latency",
        grid={"stream_length": [100, 1000, 10000]},
        func=_read_stream,
        prepare=_prepare_read_stream,
    ),
    Scenario(
        name="store.concurrent_append",
        interface="store",
        metric="throughput",
        grid={"writers": [1, 10, 50]},
        func=_concurrent_append,
    ),
    Scenario(
        name="store.contended_append",
        interface="store",
        metric="throughput",
        grid={"writers": [1, 10, 50]},
        func=_contended_append,
    ),
]
