"""EventBus scenarios: publish throughput, fan-out delivery, roundtrip latency.

Publish site and handlers capture perf_counter() in the same process, so
deltas are valid monotonic-clock latencies (spec: methodology).
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from bench.adapters.base import BenchAdapter, BusAdapter
from bench.core.domain import BenchEvent, make_events
from bench.core.runner import Measurement, Scenario
from eventsource.bus.interface import EventBus
from eventsource.events.base import DomainEvent


@dataclass
class _BusHarness:
    adapter: BusAdapter
    publish_times: dict[int, float] = field(default_factory=dict)
    deliveries: list[tuple[int, float]] = field(default_factory=list)
    delivered: asyncio.Event = field(default_factory=asyncio.Event)
    expected: int = 0
    next_seq: int = 0

    def on_delivery(self, event: BenchEvent) -> None:
        self.deliveries.append((event.seq, time.perf_counter()))
        if len(self.deliveries) >= self.expected:
            self.delivered.set()

    def begin_wave(self, expected: int) -> None:
        self.publish_times.clear()
        self.deliveries.clear()
        self.delivered = asyncio.Event()
        self.expected = expected

    def take_seq(self) -> int:
        seq = self.next_seq
        self.next_seq += 1
        return seq


def _sequenced_events(harness: _BusHarness, count: int) -> list[BenchEvent]:
    aggregate_id = uuid4()
    events = make_events(aggregate_id, count)
    stamped = []
    for event in events:
        seq = harness.take_seq()
        stamped.append(event.model_copy(update={"seq": seq}))
    return stamped


async def _prepare_publish(
    adapter: BenchAdapter[Any], bus: EventBus, params: dict[str, Any]
) -> _BusHarness:
    assert isinstance(adapter, BusAdapter)
    return _BusHarness(adapter=adapter)


async def _publish_throughput(
    bus: EventBus, params: dict[str, Any], iterations: int, prepared: Any
) -> Measurement:
    batch_size: int = params["batch_size"]
    harness: _BusHarness = prepared
    durations: list[float] = []
    start = time.perf_counter()
    for _ in range(iterations):
        events = _sequenced_events(harness, batch_size)
        t0 = time.perf_counter()
        await bus.publish(list(events))
        durations.append(time.perf_counter() - t0)
    return Measurement(
        elapsed_s=time.perf_counter() - start,
        operations=iterations * batch_size,
        durations_s=durations,
    )


async def _prepare_fanout(
    adapter: BenchAdapter[Any], bus: EventBus, params: dict[str, Any]
) -> _BusHarness:
    assert isinstance(adapter, BusAdapter)
    harness = _BusHarness(adapter=adapter)

    async def handler(event: DomainEvent) -> None:
        assert isinstance(event, BenchEvent)
        harness.on_delivery(event)

    for _ in range(params["subscribers"]):
        bus.subscribe(BenchEvent, handler)
    await adapter.start_delivery(bus)
    return harness


async def _fanout(
    bus: EventBus, params: dict[str, Any], iterations: int, prepared: Any
) -> Measurement:
    subscribers: int = params["subscribers"]
    harness: _BusHarness = prepared
    harness.begin_wave(expected=iterations * subscribers)
    start = time.perf_counter()
    for _ in range(iterations):
        events = _sequenced_events(harness, 1)
        harness.publish_times[events[0].seq] = time.perf_counter()
        await bus.publish(list(events))
    await harness.delivered.wait()
    elapsed = time.perf_counter() - start
    latencies = [
        received - harness.publish_times[seq]
        for seq, received in harness.deliveries
        if seq in harness.publish_times
    ]
    return Measurement(
        elapsed_s=elapsed,
        operations=len(harness.deliveries),
        durations_s=latencies,
    )


async def _prepare_roundtrip(
    adapter: BenchAdapter[Any], bus: EventBus, params: dict[str, Any]
) -> _BusHarness:
    return await _prepare_fanout(adapter, bus, {"subscribers": 1})


async def _roundtrip(
    bus: EventBus, params: dict[str, Any], iterations: int, prepared: Any
) -> Measurement:
    harness: _BusHarness = prepared
    durations: list[float] = []
    start = time.perf_counter()
    for _ in range(iterations):
        harness.begin_wave(expected=1)
        events = _sequenced_events(harness, 1)
        t0 = time.perf_counter()
        await bus.publish(list(events))
        await harness.delivered.wait()
        durations.append(harness.deliveries[0][1] - t0)
    return Measurement(
        elapsed_s=time.perf_counter() - start,
        operations=iterations,
        durations_s=durations,
    )


BUS_SCENARIOS: list[Scenario] = [
    Scenario(
        name="bus.publish_throughput",
        interface="bus",
        metric="throughput",
        grid={"batch_size": [1, 10, 100]},
        func=_publish_throughput,
        prepare=_prepare_publish,
    ),
    Scenario(
        name="bus.fanout",
        interface="bus",
        metric="throughput",
        grid={"subscribers": [1, 10, 50]},
        func=_fanout,
        prepare=_prepare_fanout,
    ),
    Scenario(
        name="bus.roundtrip",
        interface="bus",
        metric="latency",
        grid={},
        func=_roundtrip,
        prepare=_prepare_roundtrip,
    ),
]
