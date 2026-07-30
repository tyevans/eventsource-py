# Backend Benchmark Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A standalone async benchmark harness (`bench/`) that runs the same scenario catalog across every EventStore, EventBus, and SnapshotStore backend plus the end-to-end aggregate path, emitting JSON results and a Markdown report.

**Architecture:** Adapters (one small class per backend, mirroring the conformance-suite factory pattern) plug backends into declarative scenarios (async callable + parameter grid). A runner expands scenarios × params × backends into cells, measures each with warmup/calibration/3 rounds inside one event loop, and writes a versioned JSON file that a report generator renders to Markdown tables.

**Tech Stack:** Python 3.11, stdlib only for the harness itself (argparse, asyncio, statistics, json, gc, time). Backends come from `eventsource` and its optional extras, imported only behind availability checks. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-07-29-backend-benchmark-matrix-design.md`

**Deliberate deviations from the spec (both minor, flagged for review):**
1. The spec's `snapshot.save_load` row is implemented as two uniform scenarios, `snapshot.save` and `snapshot.load`, so every cell has exactly one metric. Both metrics the spec asks for are preserved.
2. The spec's metadata list includes "per-service versions"; the harness records commit/python/platform/cpu/library version but not live service versions — service identity comes from the pinned images in `docker-compose.bench.yml` (postgres:15, redis:7, rabbitmq:3-management, apache/kafka:3.7.0), which the report's audience can cross-reference. Wiring live version queries through every adapter was judged not worth the complexity.

## Global Constraints

- **Test scope for workers:** Implementation agents run ONLY the test file(s) named in their task (e.g. `uv run pytest tests/unit/bench/test_results.py -v`) — never the full suite. Review agents run NO tests and assume tests pass unless the implementer's report says otherwise. The orchestrator runs the full suite once all tasks are done.
- `bench/` is internal repo tooling: never imported from `src/eventsource/`, never packaged, adds no dependencies (spec + ADR-0015).
- Do not modify `src/eventsource/`, `tests/benchmarks/`, `src/eventsource/migrations/`, or CI workflows.
- mypy runs `strict = true`; `bench/` must pass it (task 12 wires it into `make types`). Annotate everything.
- ruff: line-length 100, py311, rules `E,F,I,N,W,UP,B,C4,SIM` — ruff already lints the whole repo, so bench code is covered from the first commit. Run `uv run ruff check bench/ tests/unit/bench/ --fix && uv run ruff format bench/ tests/unit/bench/` before each commit.
- pytest: `asyncio_mode = "auto"` (no `@pytest.mark.asyncio` needed), global `--timeout=60` — keep harness unit tests tiny (iterations ≤ 5, budgets ≤ 0.2s).
- Tracing disabled in every backend instantiation (`enable_tracing=False` or config field).
- Commits: conventional style, lowercase, no trailing period (`.claude/rules/commits.md`).
- Service endpoints are env-overridable with these exact names/defaults:
  - `BENCH_POSTGRES_URL` = `postgresql+asyncpg://bench:bench@localhost:5434/eventsource_bench`
  - `BENCH_REDIS_URL` = `redis://localhost:6381`
  - `BENCH_KAFKA_SERVERS` = `localhost:9094`
  - `BENCH_RABBITMQ_URL` = `amqp://guest:guest@localhost:5673/`

---

### Task 1: Package scaffolding and results model

**Files:**
- Create: `bench/__init__.py`, `bench/core/__init__.py`, `bench/adapters/__init__.py`, `bench/scenarios/__init__.py` (all empty except a one-line docstring)
- Create: `bench/core/results.py`
- Create: `bench/results/.gitkeep`
- Modify: `.gitignore` (append a `bench/results/` ignore block)
- Test: `tests/unit/bench/__init__.py` (empty), `tests/unit/bench/test_results.py`

**Interfaces:**
- Consumes: nothing (first task)
- Produces: `SCHEMA_VERSION: int`; `LatencyStats(p50_ms, p95_ms, p99_ms, mean_ms, min_ms)` with classmethod `from_durations(durations_s: list[float]) -> LatencyStats`; `Round(elapsed_s: float, operations: int, ops_per_sec: float, latency: LatencyStats | None, counters: dict[str, int])`; `CellResult(scenario: str, interface: str, backend: str, metric: str, params: dict[str, Any], status: str, reason: str | None, rounds: list[Round])` with properties `cell_id: str` and `median_round: Round | None`; `RunResult(schema_version: int, metadata: dict[str, Any], cells: list[CellResult])` with `to_json() -> str`, classmethod `from_json(text: str) -> RunResult`, `save(path: Path) -> None`. `metric` is `"latency"` or `"throughput"`. `status` is `"ok"`, `"skipped"`, or `"failed"`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/bench/test_results.py
"""Tests for the bench result data model and JSON round-trip."""

from pathlib import Path

from bench.core.results import (
    SCHEMA_VERSION,
    CellResult,
    LatencyStats,
    Round,
    RunResult,
)


def _cell(status: str = "ok") -> CellResult:
    latency = LatencyStats.from_durations([0.001, 0.002, 0.003, 0.004, 0.005])
    rounds = [
        Round(elapsed_s=0.5, operations=100, ops_per_sec=200.0, latency=latency, counters={}),
        Round(elapsed_s=0.4, operations=100, ops_per_sec=250.0, latency=latency, counters={}),
        Round(
            elapsed_s=0.6,
            operations=100,
            ops_per_sec=166.0,
            latency=latency,
            counters={"conflicts": 3},
        ),
    ]
    return CellResult(
        scenario="store.append_batch",
        interface="store",
        backend="memory",
        metric="throughput",
        params={"batch_size": 10, "payload": "small"},
        status=status,
        reason=None,
        rounds=rounds,
    )


def test_latency_stats_from_durations() -> None:
    stats = LatencyStats.from_durations([0.001, 0.002, 0.003, 0.004, 0.010])
    assert stats.min_ms == 1.0
    assert stats.p50_ms == 3.0
    assert stats.p99_ms > stats.p50_ms
    assert stats.mean_ms == 4.0


def test_cell_id_is_stable_and_param_sorted() -> None:
    cell = _cell()
    assert cell.cell_id == "store.append_batch[memory](batch_size=10,payload=small)"


def test_median_round_selected_by_ops_per_sec() -> None:
    cell = _cell()
    median = cell.median_round
    assert median is not None
    assert median.ops_per_sec == 200.0


def test_median_round_none_when_no_rounds() -> None:
    cell = _cell(status="skipped")
    cell.rounds = []
    assert cell.median_round is None


def test_json_round_trip(tmp_path: Path) -> None:
    run = RunResult(
        schema_version=SCHEMA_VERSION,
        metadata={"commit": "abc123", "python": "3.11"},
        cells=[_cell()],
    )
    path = tmp_path / "run.json"
    run.save(path)
    loaded = RunResult.from_json(path.read_text())
    assert loaded.schema_version == SCHEMA_VERSION
    assert loaded.metadata["commit"] == "abc123"
    assert loaded.cells[0].cell_id == run.cells[0].cell_id
    assert loaded.cells[0].rounds[2].counters == {"conflicts": 3}
    assert loaded.cells[0].rounds[0].latency is not None
    assert loaded.cells[0].rounds[0].latency.p50_ms == 3.0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/bench/test_results.py -v -p no:randomly`
Expected: FAIL — `ModuleNotFoundError: No module named 'bench'`

- [ ] **Step 3: Create the package and implement results.py**

Create the four `__init__.py` files, each containing only a docstring, e.g. `bench/__init__.py`:

```python
"""Internal cross-backend benchmark harness. Not part of the eventsource package."""
```

Create `bench/results/.gitkeep` (empty file). Append to `.gitignore`:

```
# Benchmark harness output
bench/results/
!bench/results/.gitkeep
```

```python
# bench/core/results.py
"""Result data model for benchmark runs.

The JSON schema is versioned via SCHEMA_VERSION so future regression
tooling can diff two run files (spec: Reporting section).
"""

import json
import statistics
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        raise ValueError("no samples")
    k = (len(sorted_values) - 1) * pct / 100.0
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = k - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


@dataclass(frozen=True)
class LatencyStats:
    p50_ms: float
    p95_ms: float
    p99_ms: float
    mean_ms: float
    min_ms: float

    @classmethod
    def from_durations(cls, durations_s: list[float]) -> "LatencyStats":
        ms = sorted(d * 1000.0 for d in durations_s)
        return cls(
            p50_ms=_percentile(ms, 50),
            p95_ms=_percentile(ms, 95),
            p99_ms=_percentile(ms, 99),
            mean_ms=statistics.fmean(ms),
            min_ms=ms[0],
        )


@dataclass(frozen=True)
class Round:
    elapsed_s: float
    operations: int
    ops_per_sec: float
    latency: LatencyStats | None = None
    counters: dict[str, int] = field(default_factory=dict)


@dataclass
class CellResult:
    scenario: str
    interface: str
    backend: str
    metric: str  # "latency" | "throughput"
    params: dict[str, Any]
    status: str  # "ok" | "skipped" | "failed"
    reason: str | None = None
    rounds: list[Round] = field(default_factory=list)

    @property
    def cell_id(self) -> str:
        rendered = ",".join(f"{key}={self.params[key]}" for key in sorted(self.params))
        return f"{self.scenario}[{self.backend}]({rendered})"

    @property
    def median_round(self) -> Round | None:
        if not self.rounds:
            return None
        ordered = sorted(self.rounds, key=lambda r: r.ops_per_sec)
        return ordered[len(ordered) // 2]


@dataclass
class RunResult:
    schema_version: int
    metadata: dict[str, Any]
    cells: list[CellResult]

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)

    @classmethod
    def from_json(cls, text: str) -> "RunResult":
        raw = json.loads(text)
        cells = []
        for cell_raw in raw["cells"]:
            rounds = []
            for round_raw in cell_raw["rounds"]:
                latency_raw = round_raw.get("latency")
                latency = LatencyStats(**latency_raw) if latency_raw else None
                rounds.append(
                    Round(
                        elapsed_s=round_raw["elapsed_s"],
                        operations=round_raw["operations"],
                        ops_per_sec=round_raw["ops_per_sec"],
                        latency=latency,
                        counters=round_raw.get("counters", {}),
                    )
                )
            cells.append(
                CellResult(
                    scenario=cell_raw["scenario"],
                    interface=cell_raw["interface"],
                    backend=cell_raw["backend"],
                    metric=cell_raw["metric"],
                    params=cell_raw["params"],
                    status=cell_raw["status"],
                    reason=cell_raw.get("reason"),
                    rounds=rounds,
                )
            )
        return cls(
            schema_version=raw["schema_version"],
            metadata=raw["metadata"],
            cells=cells,
        )

    def save(self, path: Path) -> None:
        path.write_text(self.to_json())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/bench/test_results.py -v -p no:randomly`
Expected: 5 passed

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check bench/ tests/unit/bench/ --fix && uv run ruff format bench/ tests/unit/bench/
git add bench/ tests/unit/bench/ .gitignore
git commit -m "feat: add bench package scaffolding and results model"
```

---

### Task 2: Bench domain — events, payloads, aggregate, snapshot state

**Files:**
- Create: `bench/core/domain.py`
- Test: `tests/unit/bench/test_domain.py`

**Interfaces:**
- Consumes: `eventsource` public API (`DomainEvent`, `DeclarativeAggregate`, `handles`, `EventRegistry`)
- Produces: `PAYLOAD_SIZES: dict[str, int]` (`{"small": 200, "large": 5_000}`); `SNAPSHOT_SIZES: dict[str, int]` (`{"small": 1_000, "medium": 50_000, "large": 500_000}`); `BenchEvent(DomainEvent)` with fields `payload: str`, `seq: int`; `BenchCounterIncremented(DomainEvent)` with field `increment: int`; `BenchCounterState(BaseModel)`; `BenchCounter(DeclarativeAggregate[BenchCounterState])` with command `increment(amount: int = 1) -> None`; `make_events(aggregate_id: UUID, count: int, start_version: int = 1, payload: str = "small") -> list[BenchEvent]`; `make_snapshot_state(size_bytes: int) -> dict[str, Any]`; `make_registry() -> EventRegistry` returning a fresh registry with both bench events registered.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/bench/test_domain.py
"""Tests for bench domain events, payload generators, and the bench aggregate."""

from uuid import uuid4

from bench.core.domain import (
    PAYLOAD_SIZES,
    SNAPSHOT_SIZES,
    BenchCounter,
    BenchEvent,
    make_events,
    make_registry,
    make_snapshot_state,
)


def test_make_events_versions_and_payload_size() -> None:
    aggregate_id = uuid4()
    events = make_events(aggregate_id, count=3, start_version=5, payload="large")
    assert [e.aggregate_version for e in events] == [5, 6, 7]
    assert all(e.aggregate_id == aggregate_id for e in events)
    assert all(len(e.payload) == PAYLOAD_SIZES["large"] for e in events)
    assert [e.seq for e in events] == [0, 1, 2]


def test_make_snapshot_state_size() -> None:
    state = make_snapshot_state(SNAPSHOT_SIZES["medium"])
    assert len(state["blob"]) == SNAPSHOT_SIZES["medium"]


def test_bench_counter_applies_increments() -> None:
    counter = BenchCounter(uuid4())
    counter.increment()
    counter.increment(2)
    assert counter.state.value == 3
    assert counter.version == 0  # not yet persisted
    assert len(counter.get_uncommitted_events()) == 2


def test_make_registry_contains_bench_events() -> None:
    registry = make_registry()
    assert registry.get_event_class("BenchEvent") is BenchEvent
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/bench/test_domain.py -v -p no:randomly`
Expected: FAIL — `ModuleNotFoundError` / `ImportError` for `bench.core.domain`

Note: `tests/fixtures/aggregates.py` has `DeclarativeCounterAggregate` as the reference for aggregate mechanics, and `tests/unit/` aggregate tests show accessor names. If `counter.state` / `counter.version` / `get_uncommitted_events()` differ in the real `DeclarativeAggregate` API, adjust the test to the real accessors (check `src/eventsource/aggregates/base.py`) — do not change the aggregate.

- [ ] **Step 3: Implement domain.py**

```python
# bench/core/domain.py
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
        self._state = self._state.model_copy(
            update={"value": self._state.value + event.increment}
        )

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
```

If `EventRegistry.get_event_class` or `.register` have different names, mirror what `tests/integration/conftest.py` does with its registry and adjust the test accordingly.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/bench/test_domain.py -v -p no:randomly`
Expected: 4 passed

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check bench/ tests/unit/bench/ --fix && uv run ruff format bench/ tests/unit/bench/
git add bench/core/domain.py tests/unit/bench/test_domain.py
git commit -m "feat: add bench domain events and counter aggregate"
```

---

### Task 3: Adapter contract and in-memory adapters

**Files:**
- Create: `bench/adapters/base.py`
- Create: `bench/adapters/stores.py` (memory only; SQL backends come in task 8)
- Create: `bench/adapters/buses.py` (memory only; brokers come in task 9)
- Create: `bench/adapters/snapshots.py` (memory only)
- Test: `tests/unit/bench/test_adapters_memory.py`

**Interfaces:**
- Consumes: `eventsource` in-memory backends
- Produces: `BenchAdapter(ABC, Generic[T])` with `name: ClassVar[str]`, `async available() -> str | None` (None = available, string = skip reason), `async setup() -> None`, `async teardown() -> None`, `async create() -> T` (abstract; returns a fresh resource per cell), `async destroy(resource: T) -> None`; `BusAdapter(BenchAdapter[EventBus])` adding `async start_delivery(bus: EventBus) -> None` and `async stop_delivery(bus: EventBus) -> None`; concrete `MemoryStoreAdapter` (name `"memory"`), `MemoryBusAdapter` (`"memory"`), `MemorySnapshotAdapter` (`"memory"`); registries `STORE_ADAPTERS: dict[str, type[BenchAdapter[EventStore]]]` in `stores.py`, `BUS_ADAPTERS` in `buses.py`, `SNAPSHOT_ADAPTERS` in `snapshots.py` (each mapping name → adapter class).

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/bench/test_adapters_memory.py
"""Tests for the adapter contract using the always-available memory adapters."""

from uuid import uuid4

from bench.adapters.buses import BUS_ADAPTERS, MemoryBusAdapter
from bench.adapters.snapshots import SNAPSHOT_ADAPTERS, MemorySnapshotAdapter
from bench.adapters.stores import STORE_ADAPTERS, MemoryStoreAdapter
from bench.core.domain import make_events


async def test_memory_store_adapter_lifecycle() -> None:
    adapter = MemoryStoreAdapter()
    assert adapter.name == "memory"
    assert await adapter.available() is None
    await adapter.setup()
    store = await adapter.create()
    aggregate_id = uuid4()
    result = await store.append_events(
        aggregate_id, "Bench", make_events(aggregate_id, 2), expected_version=0
    )
    assert result.success
    await adapter.destroy(store)
    await adapter.teardown()


async def test_memory_bus_adapter_delivers() -> None:
    adapter = MemoryBusAdapter()
    assert await adapter.available() is None
    await adapter.setup()
    bus = await adapter.create()
    received: list[object] = []

    async def handler(event: object) -> None:
        received.append(event)

    from bench.core.domain import BenchEvent

    bus.subscribe(BenchEvent, handler)
    await adapter.start_delivery(bus)
    aggregate_id = uuid4()
    await bus.publish(make_events(aggregate_id, 1))
    assert len(received) == 1
    await adapter.stop_delivery(bus)
    await adapter.destroy(bus)
    await adapter.teardown()


async def test_memory_snapshot_adapter_roundtrip() -> None:
    from datetime import UTC, datetime

    from eventsource.snapshots.interface import Snapshot

    adapter = MemorySnapshotAdapter()
    await adapter.setup()
    snapshot_store = await adapter.create()
    aggregate_id = uuid4()
    snapshot = Snapshot(
        aggregate_id=aggregate_id,
        aggregate_type="Bench",
        version=1,
        state={"blob": "x"},
        schema_version=1,
        created_at=datetime.now(UTC),
    )
    await snapshot_store.save_snapshot(snapshot)
    loaded = await snapshot_store.get_snapshot(aggregate_id, "Bench")
    assert loaded is not None and loaded.version == 1
    await adapter.destroy(snapshot_store)
    await adapter.teardown()


def test_registries_contain_memory() -> None:
    assert STORE_ADAPTERS["memory"] is MemoryStoreAdapter
    assert BUS_ADAPTERS["memory"] is MemoryBusAdapter
    assert SNAPSHOT_ADAPTERS["memory"] is MemorySnapshotAdapter
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/bench/test_adapters_memory.py -v -p no:randomly`
Expected: FAIL — `ModuleNotFoundError: No module named 'bench.adapters.base'` (or similar)

- [ ] **Step 3: Implement base and memory adapters**

```python
# bench/adapters/base.py
"""Adapter contract: how a backend plugs into the harness.

Mirrors the conformance-suite factory pattern (see
src/eventsource/testing/conformance.py): scenarios never know which
backend they run on.
"""

from abc import ABC, abstractmethod
from typing import ClassVar, Generic, TypeVar

from eventsource.bus.interface import EventBus

T = TypeVar("T")


class BenchAdapter(ABC, Generic[T]):
    """Lifecycle: available? -> setup -> (create -> destroy)* -> teardown.

    create() is called once per matrix cell and must return an isolated,
    ready-to-use resource. destroy() releases it.
    """

    name: ClassVar[str] = ""

    async def available(self) -> str | None:
        """Return None if this backend can run, else a skip reason."""
        return None

    async def setup(self) -> None:
        """One-time session setup (schema creation, temp dirs)."""

    async def teardown(self) -> None:
        """One-time session cleanup."""

    @abstractmethod
    async def create(self) -> T:
        """Create a fresh resource for one cell."""

    async def destroy(self, resource: T) -> None:
        """Release a resource created by create()."""


class BusAdapter(BenchAdapter[EventBus]):
    """Bus adapters additionally manage consumer delivery.

    Scenarios subscribe handlers first, then call start_delivery();
    ordering matters for broker consumers.
    """

    async def start_delivery(self, bus: EventBus) -> None:
        """Begin delivering published events to subscribers (no-op for memory)."""

    async def stop_delivery(self, bus: EventBus) -> None:
        """Stop the consumer started by start_delivery()."""
```

```python
# bench/adapters/stores.py
"""EventStore adapters. SQL backends are added by later tasks."""

from eventsource import InMemoryEventStore
from eventsource.stores.interface import EventStore

from bench.adapters.base import BenchAdapter


class MemoryStoreAdapter(BenchAdapter[EventStore]):
    name = "memory"

    async def create(self) -> EventStore:
        return InMemoryEventStore(enable_tracing=False)


STORE_ADAPTERS: dict[str, type[BenchAdapter[EventStore]]] = {
    MemoryStoreAdapter.name: MemoryStoreAdapter,
}
```

```python
# bench/adapters/buses.py
"""EventBus adapters. Broker backends are added by later tasks."""

from eventsource import InMemoryEventBus
from eventsource.bus.interface import EventBus

from bench.adapters.base import BusAdapter


class MemoryBusAdapter(BusAdapter):
    name = "memory"

    async def create(self) -> EventBus:
        return InMemoryEventBus(enable_tracing=False)

    async def destroy(self, resource: EventBus) -> None:
        await resource.shutdown()


BUS_ADAPTERS: dict[str, type[BusAdapter]] = {
    MemoryBusAdapter.name: MemoryBusAdapter,
}
```

```python
# bench/adapters/snapshots.py
"""SnapshotStore adapters. SQL backends are added by later tasks."""

from eventsource.snapshots.in_memory import InMemorySnapshotStore
from eventsource.snapshots.interface import SnapshotStore

from bench.adapters.base import BenchAdapter


class MemorySnapshotAdapter(BenchAdapter[SnapshotStore]):
    name = "memory"

    async def create(self) -> SnapshotStore:
        return InMemorySnapshotStore(enable_tracing=False)


SNAPSHOT_ADAPTERS: dict[str, type[BenchAdapter[SnapshotStore]]] = {
    MemorySnapshotAdapter.name: MemorySnapshotAdapter,
}
```

If `InMemoryEventStore` / `InMemoryEventBus` are not re-exported from the top-level `eventsource` package, import from their modules (`eventsource.stores.in_memory`, `eventsource.bus.memory`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/bench/test_adapters_memory.py -v -p no:randomly`
Expected: 4 passed

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check bench/ tests/unit/bench/ --fix && uv run ruff format bench/ tests/unit/bench/
git add bench/adapters/ tests/unit/bench/test_adapters_memory.py
git commit -m "feat: add bench adapter contract and in-memory adapters"
```

---

### Task 4: Runner core — grid expansion, calibration, rounds, timeout, metadata

**Files:**
- Create: `bench/core/runner.py`
- Create: `bench/core/meta.py`
- Test: `tests/unit/bench/test_runner.py`

**Interfaces:**
- Consumes: `BenchAdapter`, `BusAdapter` (task 3); `CellResult`, `Round`, `LatencyStats`, `RunResult`, `SCHEMA_VERSION` (task 1)
- Produces:
  - `Measurement(elapsed_s: float, operations: int, durations_s: list[float] | None = None, counters: dict[str, int] = {})` (frozen dataclass)
  - `PrepareFunc = Callable[[BenchAdapter[Any], Any, dict[str, Any]], Awaitable[Any]]`
  - `ScenarioFunc = Callable[[Any, dict[str, Any], int, Any], Awaitable[Measurement]]` — args are `(resource, params, iterations, prepared)`
  - `Scenario(name: str, interface: str, metric: str, grid: dict[str, list[Any]], func: ScenarioFunc, prepare: PrepareFunc | None = None)` (frozen dataclass)
  - `RunnerConfig(rounds: int = 3, warmup_iterations: int = 3, calibration_iterations: int = 5, target_round_seconds: float = 2.0, max_iterations: int = 10_000, cell_timeout_seconds: float = 60.0, quick: bool = False)` — quick mode forces `rounds=1`, `target_round_seconds=0.2`, and trims each grid axis to its first value
  - `expand_grid(grid: dict[str, list[Any]], quick: bool) -> list[dict[str, Any]]`
  - `run_cell(adapter: BenchAdapter[Any], scenario: Scenario, params: dict[str, Any], config: RunnerConfig) -> CellResult` (async)
  - `run_matrix(scenarios: list[Scenario], adapters: dict[str, list[BenchAdapter[Any]]], config: RunnerConfig) -> RunResult` (async; `adapters` maps interface name → adapter instances; skips unavailable adapters with reason; calls each adapter's `setup()` once before its first cell and `teardown()` after its last)
  - `bench/core/meta.py`: `collect_metadata() -> dict[str, Any]` with keys `timestamp`, `commit`, `python`, `platform`, `cpu_count`, `eventsource_version`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/bench/test_runner.py
"""Runner tests using a fake in-process adapter and scenario."""

import asyncio
from typing import Any

from bench.adapters.base import BenchAdapter
from bench.core.runner import (
    Measurement,
    RunnerConfig,
    Scenario,
    expand_grid,
    run_cell,
    run_matrix,
)


class FakeAdapter(BenchAdapter[dict[str, Any]]):
    name = "fake"

    def __init__(self, reason: str | None = None) -> None:
        self.reason = reason
        self.setup_calls = 0
        self.teardown_calls = 0

    async def available(self) -> str | None:
        return self.reason

    async def setup(self) -> None:
        self.setup_calls += 1

    async def teardown(self) -> None:
        self.teardown_calls += 1

    async def create(self) -> dict[str, Any]:
        return {}


async def _fast_scenario_func(
    resource: dict[str, Any], params: dict[str, Any], iterations: int, prepared: Any
) -> Measurement:
    durations = [0.001] * iterations
    return Measurement(
        elapsed_s=0.001 * iterations, operations=iterations, durations_s=durations
    )


FAST = Scenario(
    name="fake.fast",
    interface="store",
    metric="latency",
    grid={"size": [1, 2]},
    func=_fast_scenario_func,
)

TINY_CONFIG = RunnerConfig(
    rounds=2,
    warmup_iterations=1,
    calibration_iterations=1,
    target_round_seconds=0.01,
    max_iterations=10,
    cell_timeout_seconds=5.0,
)


def test_expand_grid_full_and_quick() -> None:
    grid = {"a": [1, 2], "b": ["x", "y"]}
    assert len(expand_grid(grid, quick=False)) == 4
    assert expand_grid(grid, quick=True) == [{"a": 1, "b": "x"}]


async def test_run_cell_produces_rounds_and_latency() -> None:
    cell = await run_cell(FakeAdapter(), FAST, {"size": 1}, TINY_CONFIG)
    assert cell.status == "ok"
    assert len(cell.rounds) == 2
    assert cell.rounds[0].latency is not None
    assert cell.rounds[0].ops_per_sec > 0


async def test_run_cell_failure_is_captured() -> None:
    async def boom(
        resource: dict[str, Any], params: dict[str, Any], iterations: int, prepared: Any
    ) -> Measurement:
        raise RuntimeError("kaboom")

    scenario = Scenario(
        name="fake.boom", interface="store", metric="latency", grid={}, func=boom
    )
    cell = await run_cell(FakeAdapter(), scenario, {}, TINY_CONFIG)
    assert cell.status == "failed"
    assert cell.reason is not None and "kaboom" in cell.reason


async def test_run_cell_timeout_is_captured() -> None:
    async def hang(
        resource: dict[str, Any], params: dict[str, Any], iterations: int, prepared: Any
    ) -> Measurement:
        await asyncio.sleep(60)
        return Measurement(elapsed_s=0.0, operations=0)

    scenario = Scenario(
        name="fake.hang", interface="store", metric="latency", grid={}, func=hang
    )
    config = RunnerConfig(
        rounds=1,
        warmup_iterations=1,
        calibration_iterations=1,
        target_round_seconds=0.01,
        max_iterations=1,
        cell_timeout_seconds=0.2,
    )
    cell = await run_cell(FakeAdapter(), scenario, {}, config)
    assert cell.status == "failed"
    assert cell.reason is not None and "timeout" in cell.reason.lower()


class BadAdapter(FakeAdapter):
    name = "bad"


async def test_run_matrix_skips_unavailable_and_sets_metadata() -> None:
    good = FakeAdapter()
    bad = BadAdapter(reason="service down")
    result = await run_matrix([FAST], {"store": [good, bad]}, TINY_CONFIG)
    ok_cells = [c for c in result.cells if c.status == "ok"]
    skipped = [c for c in result.cells if c.status == "skipped"]
    assert len(ok_cells) == 2  # grid size 2 for the good adapter
    assert len(skipped) == 2 and skipped[0].reason == "service down"
    assert good.setup_calls == 1 and good.teardown_calls == 1
    assert bad.setup_calls == 0
    assert "python" in result.metadata and "timestamp" in result.metadata
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/bench/test_runner.py -v -p no:randomly`
Expected: FAIL — `ModuleNotFoundError: No module named 'bench.core.runner'`

- [ ] **Step 3: Implement meta.py and runner.py**

```python
# bench/core/meta.py
"""Run metadata so reported numbers are never context-free (spec: methodology)."""

import os
import platform
import subprocess
import sys
from datetime import UTC, datetime
from typing import Any


def _git_commit() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return out.stdout.strip() or "unknown"
    except OSError:
        return "unknown"


def collect_metadata() -> dict[str, Any]:
    try:
        from importlib.metadata import version

        eventsource_version = version("eventsource-py")
    except Exception:
        eventsource_version = "unknown"
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "commit": _git_commit(),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "cpu_count": os.cpu_count(),
        "eventsource_version": eventsource_version,
    }
```

(If the distribution name differs, check `[project] name` in `pyproject.toml` and use that string.)

```python
# bench/core/runner.py
"""Matrix runner: warmup -> calibration -> timed rounds per cell.

All measurement for a cell happens inside the caller's event loop --
never a per-measurement asyncio.run() (spec: engine decision).
"""

import asyncio
import gc
import time
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from itertools import product
from typing import Any

from bench.adapters.base import BenchAdapter
from bench.core.meta import collect_metadata
from bench.core.results import (
    SCHEMA_VERSION,
    CellResult,
    LatencyStats,
    Round,
    RunResult,
)


@dataclass(frozen=True)
class Measurement:
    elapsed_s: float
    operations: int
    durations_s: list[float] | None = None
    counters: dict[str, int] = field(default_factory=dict)


PrepareFunc = Callable[[BenchAdapter[Any], Any, dict[str, Any]], Awaitable[Any]]
ScenarioFunc = Callable[[Any, dict[str, Any], int, Any], Awaitable[Measurement]]


@dataclass(frozen=True)
class Scenario:
    name: str
    interface: str  # "store" | "bus" | "snapshot" | "e2e"
    metric: str  # "latency" | "throughput"
    grid: dict[str, list[Any]]
    func: ScenarioFunc
    prepare: PrepareFunc | None = None


@dataclass(frozen=True)
class RunnerConfig:
    rounds: int = 3
    warmup_iterations: int = 3
    calibration_iterations: int = 5
    target_round_seconds: float = 2.0
    max_iterations: int = 10_000
    cell_timeout_seconds: float = 60.0
    quick: bool = False

    def effective(self) -> "RunnerConfig":
        if not self.quick:
            return self
        return replace(self, rounds=1, target_round_seconds=0.2)


def expand_grid(grid: dict[str, list[Any]], quick: bool) -> list[dict[str, Any]]:
    if not grid:
        return [{}]
    keys = list(grid)
    values = [[grid[k][0]] if quick else grid[k] for k in keys]
    return [dict(zip(keys, combo, strict=True)) for combo in product(*values)]


def _round_from_measurement(measurement: Measurement) -> Round:
    ops_per_sec = (
        measurement.operations / measurement.elapsed_s if measurement.elapsed_s > 0 else 0.0
    )
    latency = (
        LatencyStats.from_durations(measurement.durations_s)
        if measurement.durations_s
        else None
    )
    return Round(
        elapsed_s=measurement.elapsed_s,
        operations=measurement.operations,
        ops_per_sec=ops_per_sec,
        latency=latency,
        counters=measurement.counters,
    )


async def run_cell(
    adapter: BenchAdapter[Any],
    scenario: Scenario,
    params: dict[str, Any],
    config: RunnerConfig,
) -> CellResult:
    config = config.effective()
    cell = CellResult(
        scenario=scenario.name,
        interface=scenario.interface,
        backend=adapter.name,
        metric=scenario.metric,
        params=params,
        status="ok",
    )
    resource: Any = None
    try:
        async with asyncio.timeout(config.cell_timeout_seconds):
            resource = await adapter.create()
            prepared: Any = None
            if scenario.prepare is not None:
                prepared = await scenario.prepare(adapter, resource, params)

            await scenario.func(resource, params, config.warmup_iterations, prepared)

            calibration = await scenario.func(
                resource, params, config.calibration_iterations, prepared
            )
            per_iteration = max(
                calibration.elapsed_s / config.calibration_iterations, 1e-9
            )
            iterations = max(
                1,
                min(
                    int(config.target_round_seconds / per_iteration),
                    config.max_iterations,
                ),
            )

            for _ in range(config.rounds):
                gc.collect()
                measurement = await scenario.func(resource, params, iterations, prepared)
                cell.rounds.append(_round_from_measurement(measurement))
    except TimeoutError:
        cell.status = "failed"
        cell.reason = f"timeout after {config.cell_timeout_seconds}s"
    except Exception as exc:  # noqa: BLE001 - a failed cell must not kill the run
        cell.status = "failed"
        cell.reason = "".join(
            traceback.format_exception_only(type(exc), exc)
        ).strip()
    finally:
        if resource is not None:
            try:
                await adapter.destroy(resource)
            except Exception:  # noqa: BLE001 - teardown must not mask the cell result
                pass
    return cell


async def run_matrix(
    scenarios: list[Scenario],
    adapters: dict[str, list[BenchAdapter[Any]]],
    config: RunnerConfig,
) -> RunResult:
    cells: list[CellResult] = []
    for interface, interface_adapters in adapters.items():
        interface_scenarios = [s for s in scenarios if s.interface == interface]
        if not interface_scenarios:
            continue
        for adapter in interface_adapters:
            reason = await adapter.available()
            if reason is not None:
                for scenario in interface_scenarios:
                    for params in expand_grid(scenario.grid, config.quick):
                        cells.append(
                            CellResult(
                                scenario=scenario.name,
                                interface=scenario.interface,
                                backend=adapter.name,
                                metric=scenario.metric,
                                params=params,
                                status="skipped",
                                reason=reason,
                            )
                        )
                continue
            await adapter.setup()
            try:
                for scenario in interface_scenarios:
                    for params in expand_grid(scenario.grid, config.quick):
                        cells.append(await run_cell(adapter, scenario, params, config))
            finally:
                await adapter.teardown()
    return RunResult(
        schema_version=SCHEMA_VERSION,
        metadata=collect_metadata(),
        cells=cells,
    )
```

Note: `asyncio.timeout()` requires Python 3.11+ (the project floor).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/bench/test_runner.py -v -p no:randomly`
Expected: 5 passed

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check bench/ tests/unit/bench/ --fix && uv run ruff format bench/ tests/unit/bench/
git add bench/core/runner.py bench/core/meta.py tests/unit/bench/test_runner.py
git commit -m "feat: add bench matrix runner with calibration and timeouts"
```

---

### Task 5: EventStore scenarios

**Files:**
- Create: `bench/scenarios/stores.py`
- Modify: `bench/scenarios/__init__.py` (start the scenario registry)
- Test: `tests/unit/bench/test_scenarios_stores.py`

**Interfaces:**
- Consumes: `Scenario`, `Measurement`, `run_cell`, `RunnerConfig` (task 4); `make_events` (task 2); `MemoryStoreAdapter` (task 3); `eventsource.stores.interface.EventStore`, `ExpectedVersion`; `eventsource.exceptions.OptimisticLockError`
- Produces: `STORE_SCENARIOS: list[Scenario]` containing `store.append_batch`, `store.read_stream`, `store.concurrent_append`, `store.contended_append`; helper `populate_stream(store: EventStore, aggregate_id: UUID, count: int, payload: str = "small", chunk: int = 500) -> None` (async). `bench/scenarios/__init__.py` exports `all_scenarios() -> list[Scenario]` which concatenates the per-interface lists (buses/snapshots/e2e appended by later tasks).

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/bench/test_scenarios_stores.py
"""Run every store scenario against the memory adapter with tiny budgets."""

import pytest

from bench.adapters.stores import MemoryStoreAdapter
from bench.core.runner import RunnerConfig, run_cell
from bench.scenarios.stores import STORE_SCENARIOS

TINY = RunnerConfig(
    rounds=1,
    warmup_iterations=1,
    calibration_iterations=1,
    target_round_seconds=0.02,
    max_iterations=5,
    cell_timeout_seconds=30.0,
)

SMALLEST_PARAMS = {
    "store.append_batch": {"batch_size": 1, "payload": "small"},
    "store.read_stream": {"stream_length": 100},
    "store.concurrent_append": {"writers": 2},
    "store.contended_append": {"writers": 2},
}


@pytest.mark.parametrize("scenario", STORE_SCENARIOS, ids=lambda s: s.name)
async def test_store_scenario_runs_on_memory(scenario) -> None:  # type: ignore[no-untyped-def]
    adapter = MemoryStoreAdapter()
    await adapter.setup()
    cell = await run_cell(adapter, scenario, SMALLEST_PARAMS[scenario.name], TINY)
    await adapter.teardown()
    assert cell.status == "ok", cell.reason
    assert cell.rounds and cell.rounds[0].operations > 0


def test_grids_match_spec() -> None:
    by_name = {s.name: s for s in STORE_SCENARIOS}
    assert by_name["store.append_batch"].grid == {
        "batch_size": [1, 10, 100, 1000],
        "payload": ["small", "large"],
    }
    assert by_name["store.read_stream"].grid == {"stream_length": [100, 1000, 10000]}
    assert by_name["store.concurrent_append"].grid == {"writers": [1, 10, 50]}
    assert by_name["store.contended_append"].grid == {"writers": [1, 10, 50]}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/bench/test_scenarios_stores.py -v -p no:randomly`
Expected: FAIL — import error on `bench.scenarios.stores`

- [ ] **Step 3: Implement the store scenarios**

```python
# bench/scenarios/stores.py
"""EventStore scenarios: append, read, concurrency, contention."""

import asyncio
import time
from typing import Any
from uuid import UUID, uuid4

from eventsource.exceptions import OptimisticLockError
from eventsource.stores.interface import EventStore

from bench.adapters.base import BenchAdapter
from bench.core.domain import make_events
from bench.core.runner import Measurement, Scenario


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
            raise RuntimeError(
                f"expected {stream_length} events, read {stream.version}"
            )
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
            await store.append_events(
                aggregate_id, "Bench", events, expected_version=version
            )

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
```

Update `bench/scenarios/__init__.py`:

```python
"""Scenario registry. Later tasks append bus, snapshot, and e2e scenarios."""

from bench.core.runner import Scenario
from bench.scenarios.stores import STORE_SCENARIOS


def all_scenarios() -> list[Scenario]:
    return [*STORE_SCENARIOS]
```

Behavior note for `_contended_append`: `append_events` signals a version conflict either by raising `OptimisticLockError` or by returning `AppendResult(conflict=True)` depending on backend — the code above handles both. Check `eventsource.exceptions` for the exact exception name; the conformance suite's `test_optimistic_locking` shows the authoritative behavior.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/bench/test_scenarios_stores.py -v -p no:randomly`
Expected: 5 passed (4 parametrized + grid check)

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check bench/ tests/unit/bench/ --fix && uv run ruff format bench/ tests/unit/bench/
git add bench/scenarios/ tests/unit/bench/test_scenarios_stores.py
git commit -m "feat: add event store benchmark scenarios"
```

---

### Task 6: EventBus scenarios

**Files:**
- Create: `bench/scenarios/buses.py`
- Modify: `bench/scenarios/__init__.py` (append `BUS_SCENARIOS` to `all_scenarios()`)
- Test: `tests/unit/bench/test_scenarios_buses.py`

**Interfaces:**
- Consumes: `Scenario`, `Measurement` (task 4); `BusAdapter` (task 3); `BenchEvent`, `make_events` (task 2)
- Produces: `BUS_SCENARIOS: list[Scenario]` containing `bus.publish_throughput`, `bus.fanout`, `bus.roundtrip`. Bus scenarios' `prepare` receives the adapter (to call `start_delivery` after subscribing) and returns a `_BusHarness` carrying the delivery queue/counters.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/bench/test_scenarios_buses.py
"""Run every bus scenario against the memory adapter with tiny budgets."""

import pytest

from bench.adapters.buses import MemoryBusAdapter
from bench.core.runner import RunnerConfig, run_cell
from bench.scenarios.buses import BUS_SCENARIOS

TINY = RunnerConfig(
    rounds=1,
    warmup_iterations=1,
    calibration_iterations=1,
    target_round_seconds=0.02,
    max_iterations=5,
    cell_timeout_seconds=30.0,
)

SMALLEST_PARAMS = {
    "bus.publish_throughput": {"batch_size": 1},
    "bus.fanout": {"subscribers": 1},
    "bus.roundtrip": {},
}


@pytest.mark.parametrize("scenario", BUS_SCENARIOS, ids=lambda s: s.name)
async def test_bus_scenario_runs_on_memory(scenario) -> None:  # type: ignore[no-untyped-def]
    adapter = MemoryBusAdapter()
    await adapter.setup()
    cell = await run_cell(adapter, scenario, SMALLEST_PARAMS[scenario.name], TINY)
    await adapter.teardown()
    assert cell.status == "ok", cell.reason
    assert cell.rounds and cell.rounds[0].operations > 0


def test_grids_match_spec() -> None:
    by_name = {s.name: s for s in BUS_SCENARIOS}
    assert by_name["bus.publish_throughput"].grid == {"batch_size": [1, 10, 100]}
    assert by_name["bus.fanout"].grid == {"subscribers": [1, 10, 50]}
    assert by_name["bus.roundtrip"].grid == {}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/bench/test_scenarios_buses.py -v -p no:randomly`
Expected: FAIL — import error on `bench.scenarios.buses`

- [ ] **Step 3: Implement the bus scenarios**

```python
# bench/scenarios/buses.py
"""EventBus scenarios: publish throughput, fan-out delivery, roundtrip latency.

Publish site and handlers capture perf_counter() in the same process, so
deltas are valid monotonic-clock latencies (spec: methodology).
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from eventsource.bus.interface import EventBus

from bench.adapters.base import BenchAdapter, BusAdapter
from bench.core.domain import BenchEvent, make_events
from bench.core.runner import Measurement, Scenario


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
    for _ in range(params["subscribers"]):
        async def handler(event: BenchEvent, _h: _BusHarness = harness) -> None:
            _h.on_delivery(event)

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
```

Append to `bench/scenarios/__init__.py`:

```python
from bench.scenarios.buses import BUS_SCENARIOS


def all_scenarios() -> list[Scenario]:
    return [*STORE_SCENARIOS, *BUS_SCENARIOS]
```

Caveat: the memory bus dispatches inline during `publish`, so `delivered` may already be set when awaited — that is fine. Handlers must be tolerant of duplicate delivery on broker buses (at-least-once); `expected` counts are minimums, which `>=` already handles.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/bench/test_scenarios_buses.py -v -p no:randomly`
Expected: 4 passed

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check bench/ tests/unit/bench/ --fix && uv run ruff format bench/ tests/unit/bench/
git add bench/scenarios/ tests/unit/bench/test_scenarios_buses.py
git commit -m "feat: add event bus benchmark scenarios"
```

---

### Task 7: Snapshot and end-to-end aggregate scenarios

**Files:**
- Create: `bench/scenarios/snapshots.py`
- Create: `bench/scenarios/aggregate.py`
- Create: `bench/adapters/e2e.py`
- Modify: `bench/scenarios/__init__.py` (append both to `all_scenarios()`)
- Test: `tests/unit/bench/test_scenarios_snapshot_e2e.py`

**Interfaces:**
- Consumes: tasks 2-5 outputs; `eventsource.snapshots.interface.Snapshot`, `SnapshotStore`; `eventsource.aggregates.repository.AggregateRepository`
- Produces: `SNAPSHOT_SCENARIOS: list[Scenario]` (`snapshot.save`, `snapshot.load` — the spec's `save_load` row split into two uniform cells, both metrics preserved); `E2E_SCENARIOS: list[Scenario]` (`e2e.load_mutate_save`); `E2EAdapter(BenchAdapter[tuple[EventStore, SnapshotStore]])` in `bench/adapters/e2e.py` composing a store adapter + snapshot adapter with matching backend `name`, plus `E2E_ADAPTERS: dict[str, type[...]]`-style factory `make_e2e_adapters() -> list[E2EAdapter]` pairing memory/postgresql/sqlite (pairs whose parts both exist).

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/bench/test_scenarios_snapshot_e2e.py
"""Snapshot and end-to-end scenarios against memory backends."""

import pytest

from bench.adapters.e2e import make_e2e_adapters
from bench.adapters.snapshots import MemorySnapshotAdapter
from bench.core.runner import RunnerConfig, run_cell
from bench.scenarios.aggregate import E2E_SCENARIOS
from bench.scenarios.snapshots import SNAPSHOT_SCENARIOS

TINY = RunnerConfig(
    rounds=1,
    warmup_iterations=1,
    calibration_iterations=1,
    target_round_seconds=0.02,
    max_iterations=5,
    cell_timeout_seconds=30.0,
)


@pytest.mark.parametrize("scenario", SNAPSHOT_SCENARIOS, ids=lambda s: s.name)
async def test_snapshot_scenario_runs_on_memory(scenario) -> None:  # type: ignore[no-untyped-def]
    adapter = MemorySnapshotAdapter()
    await adapter.setup()
    cell = await run_cell(adapter, scenario, {"size": "small"}, TINY)
    await adapter.teardown()
    assert cell.status == "ok", cell.reason


@pytest.mark.parametrize("snapshots", ["none", "threshold"])
async def test_e2e_scenario_runs_on_memory(snapshots: str) -> None:
    adapter = next(a for a in make_e2e_adapters() if a.name == "memory")
    await adapter.setup()
    scenario = E2E_SCENARIOS[0]
    params = {"stream_length": 100, "snapshots": snapshots}
    cell = await run_cell(adapter, scenario, params, TINY)
    await adapter.teardown()
    assert cell.status == "ok", cell.reason
    assert cell.rounds and cell.rounds[0].latency is not None


def test_grids_match_spec() -> None:
    by_name = {s.name: s for s in SNAPSHOT_SCENARIOS + E2E_SCENARIOS}
    assert by_name["snapshot.save"].grid == {"size": ["small", "medium", "large"]}
    assert by_name["snapshot.load"].grid == {"size": ["small", "medium", "large"]}
    assert by_name["e2e.load_mutate_save"].grid == {
        "stream_length": [100, 1000, 10000],
        "snapshots": ["none", "threshold"],
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/bench/test_scenarios_snapshot_e2e.py -v -p no:randomly`
Expected: FAIL — import errors

- [ ] **Step 3: Implement snapshots.py, e2e.py, aggregate.py**

```python
# bench/scenarios/snapshots.py
"""SnapshotStore scenarios: save latency and load latency by state size."""

import time
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from eventsource.snapshots.interface import Snapshot, SnapshotStore

from bench.adapters.base import BenchAdapter
from bench.core.domain import SNAPSHOT_SIZES, make_snapshot_state
from bench.core.runner import Measurement, Scenario


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
```

```python
# bench/adapters/e2e.py
"""Composite adapter pairing a store backend with its matching snapshot backend."""

from eventsource.snapshots.interface import SnapshotStore
from eventsource.stores.interface import EventStore

from bench.adapters.base import BenchAdapter
from bench.adapters.snapshots import SNAPSHOT_ADAPTERS
from bench.adapters.stores import STORE_ADAPTERS


class E2EAdapter(BenchAdapter[tuple[EventStore, SnapshotStore]]):
    def __init__(
        self,
        store_adapter: BenchAdapter[EventStore],
        snapshot_adapter: BenchAdapter[SnapshotStore],
    ) -> None:
        self._store = store_adapter
        self._snapshot = snapshot_adapter
        # instance attribute shadows the ClassVar on purpose: name = backend pair
        self.name = store_adapter.name

    async def available(self) -> str | None:
        return await self._store.available() or await self._snapshot.available()

    async def setup(self) -> None:
        await self._store.setup()
        await self._snapshot.setup()

    async def teardown(self) -> None:
        await self._snapshot.teardown()
        await self._store.teardown()

    async def create(self) -> tuple[EventStore, SnapshotStore]:
        return (await self._store.create(), await self._snapshot.create())

    async def destroy(self, resource: tuple[EventStore, SnapshotStore]) -> None:
        store, snapshot_store = resource
        await self._store.destroy(store)
        await self._snapshot.destroy(snapshot_store)


def make_e2e_adapters() -> list[E2EAdapter]:
    adapters = []
    for backend, store_cls in STORE_ADAPTERS.items():
        snapshot_cls = SNAPSHOT_ADAPTERS.get(backend)
        if snapshot_cls is not None:
            adapters.append(E2EAdapter(store_cls(), snapshot_cls()))
    return adapters
```

```python
# bench/scenarios/aggregate.py
"""End-to-end AggregateRepository benchmark: load -> mutate -> save."""

import time
from typing import Any
from uuid import UUID, uuid4

from eventsource.aggregates.repository import AggregateRepository
from eventsource.snapshots.interface import SnapshotStore
from eventsource.stores.interface import EventStore

from bench.adapters.base import BenchAdapter
from bench.core.domain import BenchCounter
from bench.core.runner import Measurement, Scenario

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
```

Update `bench/scenarios/__init__.py` to its final form:

```python
"""Scenario registry."""

from bench.core.runner import Scenario
from bench.scenarios.aggregate import E2E_SCENARIOS
from bench.scenarios.buses import BUS_SCENARIOS
from bench.scenarios.snapshots import SNAPSHOT_SCENARIOS
from bench.scenarios.stores import STORE_SCENARIOS


def all_scenarios() -> list[Scenario]:
    return [*STORE_SCENARIOS, *BUS_SCENARIOS, *SNAPSHOT_SCENARIOS, *E2E_SCENARIOS]
```

Note: `AggregateRepository.create_new(aggregate_id)` and `repo.load(...)`/`repo.save(...)` signatures are confirmed against `src/eventsource/aggregates/repository.py:109-677`. If mypy complains about the `E2EAdapter.name` instance/ClassVar shadow, change `BenchAdapter.name` to a plain class attribute `name: str = ""` — it is not mutated anywhere else.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/bench/test_scenarios_snapshot_e2e.py -v -p no:randomly`
Expected: 5 passed

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check bench/ tests/unit/bench/ --fix && uv run ruff format bench/ tests/unit/bench/
git add bench/scenarios/ bench/adapters/e2e.py tests/unit/bench/test_scenarios_snapshot_e2e.py
git commit -m "feat: add snapshot and end-to-end aggregate benchmark scenarios"
```

---

### Task 8: PostgreSQL and SQLite adapters (store + snapshot)

**Files:**
- Modify: `bench/adapters/stores.py` (add `PostgresStoreAdapter`, `SQLiteStoreAdapter`; register in `STORE_ADAPTERS`)
- Modify: `bench/adapters/snapshots.py` (add `PostgresSnapshotAdapter`, `SQLiteSnapshotAdapter`; register in `SNAPSHOT_ADAPTERS`)
- Create: `bench/adapters/_postgres.py` (shared engine/schema helpers)
- Test: `tests/unit/bench/test_adapters_sql.py`

**Interfaces:**
- Consumes: `BenchAdapter` (task 3); `make_registry` (task 2); `eventsource.migrations.get_schema`; `PostgreSQLEventStore(session_factory, *, event_registry=..., outbox_enabled=False, enable_tracing=False)`; `SQLiteEventStore(database, event_registry=..., wal_mode=True, enable_tracing=False)` (+ `await store.initialize()`, `await store.close()`); `PostgreSQLSnapshotStore(session_factory, *, enable_tracing=False)`; `SQLiteSnapshotStore(database_path, *, enable_tracing=False)`
- Produces: `PostgresStoreAdapter` (name `"postgresql"`), `SQLiteStoreAdapter` (`"sqlite"`), `PostgresSnapshotAdapter` (`"postgresql"`), `SQLiteSnapshotAdapter` (`"sqlite"`); `bench/adapters/_postgres.py` with `postgres_url() -> str` (env `BENCH_POSTGRES_URL` with the Global Constraints default), `asyncpg_dsn(url: str) -> str` (strips the `+asyncpg` driver marker), `async ensure_schema(dsn: str) -> None`, `async truncate(dsn: str) -> None`, `async ping(dsn: str) -> str | None` (None if reachable, else reason string)

- [ ] **Step 1: Write the failing tests**

The environment always has `aiosqlite` (dev extra), so the SQLite adapters are tested for real. PostgreSQL adapters are tested for availability-probing behavior only — no live service in unit tests.

```python
# tests/unit/bench/test_adapters_sql.py
"""SQLite adapters run for real; PostgreSQL adapters are probed only."""

from uuid import uuid4

from bench.adapters._postgres import asyncpg_dsn, postgres_url
from bench.adapters.snapshots import SNAPSHOT_ADAPTERS, SQLiteSnapshotAdapter
from bench.adapters.stores import (
    STORE_ADAPTERS,
    PostgresStoreAdapter,
    SQLiteStoreAdapter,
)
from bench.core.domain import make_events


def test_registries_contain_sql_backends() -> None:
    assert STORE_ADAPTERS["postgresql"] is PostgresStoreAdapter
    assert STORE_ADAPTERS["sqlite"] is SQLiteStoreAdapter
    assert "postgresql" in SNAPSHOT_ADAPTERS and "sqlite" in SNAPSHOT_ADAPTERS


def test_asyncpg_dsn_strips_driver() -> None:
    assert asyncpg_dsn("postgresql+asyncpg://u:p@h:5/db") == "postgresql://u:p@h:5/db"
    assert postgres_url().startswith("postgresql+asyncpg://")


async def test_sqlite_store_adapter_appends_and_reads() -> None:
    adapter = SQLiteStoreAdapter()
    assert await adapter.available() is None
    await adapter.setup()
    store = await adapter.create()
    aggregate_id = uuid4()
    result = await store.append_events(
        aggregate_id, "Bench", make_events(aggregate_id, 3), expected_version=0
    )
    assert result.success
    stream = await store.get_events(aggregate_id, "Bench")
    assert stream.version == 3
    await adapter.destroy(store)
    await adapter.teardown()


async def test_sqlite_snapshot_adapter_roundtrip() -> None:
    from datetime import UTC, datetime

    from eventsource.snapshots.interface import Snapshot

    adapter = SQLiteSnapshotAdapter()
    await adapter.setup()
    store = await adapter.create()
    aggregate_id = uuid4()
    await store.save_snapshot(
        Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Bench",
            version=1,
            state={"blob": "x"},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
    )
    loaded = await store.get_snapshot(aggregate_id, "Bench")
    assert loaded is not None
    await adapter.destroy(store)
    await adapter.teardown()


async def test_postgres_adapter_unavailable_without_service() -> None:
    adapter = PostgresStoreAdapter(url="postgresql+asyncpg://x:x@localhost:1/nope")
    reason = await adapter.available()
    assert reason is not None and "postgres" in reason.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/bench/test_adapters_sql.py -v -p no:randomly`
Expected: FAIL — import errors on the new names

- [ ] **Step 3: Implement the SQL adapters**

```python
# bench/adapters/_postgres.py
"""Shared PostgreSQL helpers: DSN handling, schema setup, cleanup.

Schema is applied over a raw asyncpg connection because asyncpg's simple
query protocol accepts multi-statement scripts, so get_schema("all") does
not need fragile statement splitting (unlike SQLAlchemy text()).
"""

import os

DEFAULT_URL = "postgresql+asyncpg://bench:bench@localhost:5434/eventsource_bench"


def postgres_url() -> str:
    return os.environ.get("BENCH_POSTGRES_URL", DEFAULT_URL)


def asyncpg_dsn(url: str) -> str:
    return url.replace("postgresql+asyncpg://", "postgresql://")


async def ping(dsn: str) -> str | None:
    try:
        import asyncpg
    except ImportError:
        return "postgresql extra not installed (asyncpg missing)"
    try:
        conn = await asyncpg.connect(dsn, timeout=3)
        await conn.close()
    except Exception as exc:  # noqa: BLE001 - any failure means "not available"
        return f"postgres unreachable at {dsn}: {exc}"
    return None


async def ensure_schema(dsn: str) -> None:
    import asyncpg

    from eventsource.migrations import get_schema

    conn = await asyncpg.connect(dsn, timeout=10)
    try:
        await conn.execute(get_schema("events", backend="postgresql"))
        await conn.execute(get_schema("snapshots", backend="postgresql"))
    finally:
        await conn.close()


async def truncate(dsn: str) -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn, timeout=10)
    try:
        await conn.execute("TRUNCATE TABLE events, snapshots CASCADE")
    finally:
        await conn.close()
```

Add to `bench/adapters/stores.py`:

```python
import tempfile
from pathlib import Path
from typing import Any
from uuid import uuid4

from bench.adapters._postgres import asyncpg_dsn, ensure_schema, ping, postgres_url, truncate
from bench.core.domain import make_registry


class PostgresStoreAdapter(BenchAdapter[EventStore]):
    name = "postgresql"

    def __init__(self, url: str | None = None) -> None:
        self._url = url or postgres_url()
        self._engine: Any = None
        self._session_factory: Any = None

    async def available(self) -> str | None:
        return await ping(asyncpg_dsn(self._url))

    async def setup(self) -> None:
        from sqlalchemy.ext.asyncio import (
            AsyncSession,
            async_sessionmaker,
            create_async_engine,
        )

        await ensure_schema(asyncpg_dsn(self._url))
        self._engine = create_async_engine(
            self._url, echo=False, pool_size=10, max_overflow=20
        )
        self._session_factory = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )

    async def teardown(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()

    async def create(self) -> EventStore:
        from eventsource import PostgreSQLEventStore

        await truncate(asyncpg_dsn(self._url))
        return PostgreSQLEventStore(
            self._session_factory,
            event_registry=make_registry(),
            outbox_enabled=False,
            enable_tracing=False,
        )


class SQLiteStoreAdapter(BenchAdapter[EventStore]):
    name = "sqlite"

    def __init__(self) -> None:
        self._tmpdir: tempfile.TemporaryDirectory[str] | None = None

    async def available(self) -> str | None:
        try:
            import aiosqlite  # noqa: F401
        except ImportError:
            return "sqlite extra not installed (aiosqlite missing)"
        return None

    async def setup(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(prefix="bench-sqlite-")

    async def teardown(self) -> None:
        if self._tmpdir is not None:
            self._tmpdir.cleanup()

    async def create(self) -> EventStore:
        from eventsource.stores.sqlite import SQLiteEventStore

        assert self._tmpdir is not None
        database = str(Path(self._tmpdir.name) / f"{uuid4().hex}.db")
        store = SQLiteEventStore(
            database, event_registry=make_registry(), wal_mode=True, enable_tracing=False
        )
        await store.initialize()
        return store

    async def destroy(self, resource: EventStore) -> None:
        close = getattr(resource, "close", None)
        if close is not None:
            await close()


STORE_ADAPTERS: dict[str, type[BenchAdapter[EventStore]]] = {
    MemoryStoreAdapter.name: MemoryStoreAdapter,
    PostgresStoreAdapter.name: PostgresStoreAdapter,
    SQLiteStoreAdapter.name: SQLiteStoreAdapter,
}
```

Add to `bench/adapters/snapshots.py` (same shape):

```python
import tempfile
from pathlib import Path
from typing import Any
from uuid import uuid4

from bench.adapters._postgres import asyncpg_dsn, ensure_schema, ping, postgres_url, truncate


class PostgresSnapshotAdapter(BenchAdapter[SnapshotStore]):
    name = "postgresql"

    def __init__(self, url: str | None = None) -> None:
        self._url = url or postgres_url()
        self._engine: Any = None
        self._session_factory: Any = None

    async def available(self) -> str | None:
        return await ping(asyncpg_dsn(self._url))

    async def setup(self) -> None:
        from sqlalchemy.ext.asyncio import (
            AsyncSession,
            async_sessionmaker,
            create_async_engine,
        )

        await ensure_schema(asyncpg_dsn(self._url))
        self._engine = create_async_engine(self._url, echo=False, pool_size=10)
        self._session_factory = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )

    async def teardown(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()

    async def create(self) -> SnapshotStore:
        from eventsource.snapshots.postgresql import PostgreSQLSnapshotStore

        await truncate(asyncpg_dsn(self._url))
        return PostgreSQLSnapshotStore(self._session_factory, enable_tracing=False)


class SQLiteSnapshotAdapter(BenchAdapter[SnapshotStore]):
    name = "sqlite"

    def __init__(self) -> None:
        self._tmpdir: tempfile.TemporaryDirectory[str] | None = None

    async def available(self) -> str | None:
        try:
            import aiosqlite  # noqa: F401
        except ImportError:
            return "sqlite extra not installed (aiosqlite missing)"
        return None

    async def setup(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory(prefix="bench-sqlite-snap-")

    async def teardown(self) -> None:
        if self._tmpdir is not None:
            self._tmpdir.cleanup()

    async def create(self) -> SnapshotStore:
        from eventsource.snapshots.sqlite import SQLiteSnapshotStore

        assert self._tmpdir is not None
        database = str(Path(self._tmpdir.name) / f"{uuid4().hex}.db")
        return SQLiteSnapshotStore(database, enable_tracing=False)


SNAPSHOT_ADAPTERS: dict[str, type[BenchAdapter[SnapshotStore]]] = {
    MemorySnapshotAdapter.name: MemorySnapshotAdapter,
    PostgresSnapshotAdapter.name: PostgresSnapshotAdapter,
    SQLiteSnapshotAdapter.name: SQLiteSnapshotAdapter,
}
```

Notes: `SQLiteSnapshotStore` opens connections per-operation and creates its own table lazily — verify by reading `src/eventsource/snapshots/sqlite.py`; if it needs explicit table creation, mirror what `tests/unit/` snapshot tests do. If two adapters (store + snapshot) truncate the same PostgreSQL database in an e2e cell, that is harmless — `E2EAdapter.create()` calls both, truncation is idempotent, and cells own their data.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/bench/test_adapters_sql.py -v -p no:randomly`
Expected: 5 passed

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check bench/ tests/unit/bench/ --fix && uv run ruff format bench/ tests/unit/bench/
git add bench/adapters/ tests/unit/bench/test_adapters_sql.py
git commit -m "feat: add postgresql and sqlite bench adapters"
```

---

### Task 9: Broker bus adapters (Redis, Kafka, RabbitMQ)

**Files:**
- Modify: `bench/adapters/buses.py` (add three adapters; register in `BUS_ADAPTERS`)
- Test: `tests/unit/bench/test_adapters_brokers.py`

**Interfaces:**
- Consumes: `BusAdapter` (task 3); `make_registry` (task 2); bus configs/constructors confirmed in `src/eventsource/bus/{redis,kafka,rabbitmq}.py`
- Produces: `RedisBusAdapter` (name `"redis"`), `KafkaBusAdapter` (`"kafka"`), `RabbitMQBusAdapter` (`"rabbitmq"`). Each: `available()` = import guard + TCP/service probe; `create()` = fresh bus with unique prefixes, `await bus.connect()`; `start_delivery()` = begin consuming (background); `destroy()` = stop consuming + shutdown.

**Reference files (read before implementing):** `tests/integration/bus/test_redis.py`, `test_kafka.py`, `test_rabbitmq.py` — they show the exact working config, connect/consume/shutdown call order, and quirks (`single_connection_client=True` for Redis, unique topic/exchange suffixes for isolation). Mirror them; only the endpoints change (env vars from Global Constraints instead of testcontainers).

- [ ] **Step 1: Write the failing tests**

Unit tests cover import-guard behavior and registry membership only — broker correctness is exercised by the live smoke run in task 12.

```python
# tests/unit/bench/test_adapters_brokers.py
"""Broker adapters: registry membership and unreachable-service probing."""

from bench.adapters.buses import (
    BUS_ADAPTERS,
    KafkaBusAdapter,
    RabbitMQBusAdapter,
    RedisBusAdapter,
)


def test_registry_contains_all_buses() -> None:
    assert set(BUS_ADAPTERS) == {"memory", "redis", "kafka", "rabbitmq"}


async def test_redis_unavailable_without_service() -> None:
    adapter = RedisBusAdapter(url="redis://localhost:1")
    reason = await adapter.available()
    assert reason is not None


async def test_kafka_unavailable_without_service() -> None:
    adapter = KafkaBusAdapter(servers="localhost:1")
    reason = await adapter.available()
    assert reason is not None


async def test_rabbitmq_unavailable_without_service() -> None:
    adapter = RabbitMQBusAdapter(url="amqp://guest:guest@localhost:1/")
    reason = await adapter.available()
    assert reason is not None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/bench/test_adapters_brokers.py -v -p no:randomly`
Expected: FAIL — import errors on the adapter names

- [ ] **Step 3: Implement the broker adapters**

Add to `bench/adapters/buses.py` (Redis shown in full; Kafka and RabbitMQ follow the same skeleton with their own config classes and probe):

```python
import asyncio
import os
from uuid import uuid4

from bench.core.domain import make_registry


async def _tcp_probe(host: str, port: int, service: str) -> str | None:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=3
        )
        writer.close()
        await writer.wait_closed()
    except (OSError, asyncio.TimeoutError) as exc:
        return f"{service} unreachable at {host}:{port}: {exc}"
    return None


class RedisBusAdapter(BusAdapter):
    name = "redis"

    def __init__(self, url: str | None = None) -> None:
        self._url = url or os.environ.get("BENCH_REDIS_URL", "redis://localhost:6381")
        self._consume_tasks: dict[int, asyncio.Task[None]] = {}

    async def available(self) -> str | None:
        try:
            import redis  # noqa: F401
        except ImportError:
            return "redis extra not installed"
        host, _, port = self._url.removeprefix("redis://").partition(":")
        return await _tcp_probe(host or "localhost", int(port or 6379), "redis")

    async def create(self) -> EventBus:
        from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig

        suffix = uuid4().hex[:8]
        config = RedisEventBusConfig(
            redis_url=self._url,
            stream_prefix=f"bench_{suffix}",
            consumer_group=f"bench_group_{suffix}",
            enable_tracing=False,
        )
        bus = RedisEventBus(config=config, event_registry=make_registry())
        await bus.connect()
        return bus

    async def start_delivery(self, bus: EventBus) -> None:
        # Mirror tests/integration/bus/test_redis.py: consumption runs as a
        # background task (use start_consuming_in_background() if the bus
        # provides it, otherwise create_task(bus.start_consuming())).
        task = asyncio.create_task(bus.start_consuming())  # type: ignore[attr-defined]
        self._consume_tasks[id(bus)] = task
        await asyncio.sleep(0.5)  # let the consumer join the group

    async def stop_delivery(self, bus: EventBus) -> None:
        await bus.stop_consuming()  # type: ignore[attr-defined]
        task = self._consume_tasks.pop(id(bus), None)
        if task is not None:
            task.cancel()

    async def destroy(self, resource: EventBus) -> None:
        await self.stop_delivery(resource)
        await resource.shutdown()
```

```python
class KafkaBusAdapter(BusAdapter):
    name = "kafka"

    def __init__(self, servers: str | None = None) -> None:
        self._servers = servers or os.environ.get("BENCH_KAFKA_SERVERS", "localhost:9094")

    async def available(self) -> str | None:
        try:
            import aiokafka  # noqa: F401
        except ImportError:
            return "kafka extra not installed (aiokafka missing)"
        host, _, port = self._servers.partition(":")
        return await _tcp_probe(host or "localhost", int(port or 9092), "kafka")

    async def create(self) -> EventBus:
        from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

        suffix = uuid4().hex[:8]
        config = KafkaEventBusConfig(
            bootstrap_servers=self._servers,
            topic_prefix=f"bench_{suffix}",
            consumer_group=f"bench_group_{suffix}",
            enable_dlq=True,
            enable_tracing=False,
        )
        bus = KafkaEventBus(config=config, event_registry=make_registry())
        await bus.connect()
        return bus

    async def start_delivery(self, bus: EventBus) -> None:
        from eventsource.bus.kafka import KafkaEventBus

        assert isinstance(bus, KafkaEventBus)
        await bus.start_consuming_in_background()
        await asyncio.sleep(0.5)  # let the consumer join the group

    async def stop_delivery(self, bus: EventBus) -> None:
        from eventsource.bus.kafka import KafkaEventBus

        assert isinstance(bus, KafkaEventBus)
        if bus.is_consuming:
            await bus.stop_consuming()

    async def destroy(self, resource: EventBus) -> None:
        from eventsource.bus.kafka import KafkaEventBus

        assert isinstance(resource, KafkaEventBus)
        await self.stop_delivery(resource)
        if resource.is_connected:
            await resource.disconnect()


class RabbitMQBusAdapter(BusAdapter):
    name = "rabbitmq"

    def __init__(self, url: str | None = None) -> None:
        self._url = url or os.environ.get(
            "BENCH_RABBITMQ_URL", "amqp://guest:guest@localhost:5673/"
        )

    async def available(self) -> str | None:
        try:
            import aio_pika  # noqa: F401
        except ImportError:
            return "rabbitmq extra not installed (aio-pika missing)"
        hostport = self._url.split("@")[-1].split("/")[0]
        host, _, port = hostport.partition(":")
        return await _tcp_probe(host or "localhost", int(port or 5672), "rabbitmq")

    async def create(self) -> EventBus:
        from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        suffix = uuid4().hex[:8]
        config = RabbitMQEventBusConfig(
            rabbitmq_url=self._url,
            exchange_name=f"bench_{suffix}",
            consumer_group=f"bench_group_{suffix}",
            durable=False,
            auto_delete=True,
            enable_tracing=False,
        )
        bus = RabbitMQEventBus(config=config, event_registry=make_registry())
        await bus.connect()
        return bus

    async def start_delivery(self, bus: EventBus) -> None:
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        assert isinstance(bus, RabbitMQEventBus)
        await bus.start_consuming_in_background()
        await asyncio.sleep(0.5)

    async def stop_delivery(self, bus: EventBus) -> None:
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        assert isinstance(bus, RabbitMQEventBus)
        if bus.is_consuming:
            await bus.stop_consuming()

    async def destroy(self, resource: EventBus) -> None:
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        assert isinstance(resource, RabbitMQEventBus)
        await self.stop_delivery(resource)
        if resource.is_connected:
            await resource.disconnect()
```

If `is_consuming` / `is_connected` properties differ on a bus class, use the names the corresponding `tests/integration/bus/test_*.py` fixture teardown uses — those are the authoritative call sequences.

Final registry:

```python
BUS_ADAPTERS: dict[str, type[BusAdapter]] = {
    MemoryBusAdapter.name: MemoryBusAdapter,
    RedisBusAdapter.name: RedisBusAdapter,
    KafkaBusAdapter.name: KafkaBusAdapter,
    RabbitMQBusAdapter.name: RabbitMQBusAdapter,
}
```

If the `type: ignore[attr-defined]` comments are needed because `EventBus` lacks consume methods, prefer narrowing with `isinstance` checks against the concrete bus class inside the adapter (the adapter already knows its concrete type) — that keeps mypy strict clean without ignores.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/bench/test_adapters_brokers.py -v -p no:randomly`
Expected: 4 passed

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check bench/ tests/unit/bench/ --fix && uv run ruff format bench/ tests/unit/bench/
git add bench/adapters/buses.py tests/unit/bench/test_adapters_brokers.py
git commit -m "feat: add redis kafka and rabbitmq bench adapters"
```

---

### Task 10: Markdown report generator

**Files:**
- Create: `bench/core/report.py`
- Test: `tests/unit/bench/test_report.py`

**Interfaces:**
- Consumes: `RunResult`, `CellResult`, `Round` (task 1)
- Produces: `render_markdown(run: RunResult) -> str` — metadata header first, then one `##` section per interface (order: store, bus, snapshot, e2e), one `###` per scenario, each scenario a table: rows = param combos (sorted, rendered `key=value, key=value`), columns = backends (sorted, memory first), cell text = median round's primary metric — `latency` → `p50 ms (p95 ms)`, `throughput` → `N/s`; a trailing ` [x% conflicts]` when the median round has a `conflicts` counter (rate = conflicts / (operations + conflicts)); `skipped: <reason>` / `failed: <reason>` for non-ok cells.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/bench/test_report.py
"""Report generator tests against a hand-built RunResult."""

from bench.core.report import render_markdown
from bench.core.results import (
    SCHEMA_VERSION,
    CellResult,
    LatencyStats,
    Round,
    RunResult,
)


def _round(ops: float = 100.0, conflicts: int = 0) -> Round:
    return Round(
        elapsed_s=1.0,
        operations=100,
        ops_per_sec=ops,
        latency=LatencyStats(p50_ms=2.0, p95_ms=4.0, p99_ms=5.0, mean_ms=2.5, min_ms=1.0),
        counters={"conflicts": conflicts} if conflicts else {},
    )


def _run() -> RunResult:
    return RunResult(
        schema_version=SCHEMA_VERSION,
        metadata={"commit": "abc1234", "platform": "linux-test", "timestamp": "t"},
        cells=[
            CellResult(
                scenario="store.append_batch",
                interface="store",
                backend="memory",
                metric="throughput",
                params={"batch_size": 1},
                status="ok",
                rounds=[_round(500.0)],
            ),
            CellResult(
                scenario="store.append_batch",
                interface="store",
                backend="postgresql",
                metric="throughput",
                params={"batch_size": 1},
                status="skipped",
                reason="postgres unreachable",
            ),
            CellResult(
                scenario="store.contended_append",
                interface="store",
                backend="memory",
                metric="throughput",
                params={"writers": 10},
                status="ok",
                rounds=[_round(200.0, conflicts=25)],
            ),
        ],
    )


def test_report_contains_metadata_and_sections() -> None:
    text = render_markdown(_run())
    assert text.index("abc1234") < text.index("## store")
    assert "### store.append_batch" in text
    assert "| batch_size=1 |" in text


def test_report_renders_throughput_skip_and_conflicts() -> None:
    text = render_markdown(_run())
    assert "500/s" in text
    assert "skipped: postgres unreachable" in text
    assert "[20% conflicts]" in text  # 25 / (100 + 25)


def test_report_latency_metric_uses_percentiles() -> None:
    run = _run()
    run.cells[0].metric = "latency"
    text = render_markdown(run)
    assert "2.00ms" in text and "p95 4.00ms" in text
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/bench/test_report.py -v -p no:randomly`
Expected: FAIL — `ModuleNotFoundError: No module named 'bench.core.report'`

- [ ] **Step 3: Implement report.py**

```python
# bench/core/report.py
"""Render a RunResult as a Markdown report.

Rows are parameter combinations, columns are backends, so scaling curves
read down a column and backend comparison reads across a row.
"""

from collections import defaultdict
from typing import Any

from bench.core.results import CellResult, RunResult

_INTERFACE_ORDER = ["store", "bus", "snapshot", "e2e"]


def _params_key(cell: CellResult) -> str:
    if not cell.params:
        return "(default)"
    return ", ".join(f"{k}={cell.params[k]}" for k in sorted(cell.params))


def _backend_order(backends: set[str]) -> list[str]:
    ordered = sorted(backends)
    if "memory" in ordered:
        ordered.remove("memory")
        ordered.insert(0, "memory")
    return ordered


def _format_cell(cell: CellResult) -> str:
    if cell.status != "ok":
        return f"{cell.status}: {cell.reason}"
    median = cell.median_round
    if median is None:
        return "no data"
    if cell.metric == "latency" and median.latency is not None:
        text = f"{median.latency.p50_ms:.2f}ms (p95 {median.latency.p95_ms:.2f}ms)"
    else:
        text = f"{median.ops_per_sec:,.0f}/s"
    conflicts = median.counters.get("conflicts", 0)
    if conflicts:
        rate = conflicts / (median.operations + conflicts)
        text += f" [{rate:.0%} conflicts]"
    return text


def _render_metadata(metadata: dict[str, Any]) -> list[str]:
    lines = ["# Benchmark Report", ""]
    for key in sorted(metadata):
        lines.append(f"- **{key}**: {metadata[key]}")
    lines.append("")
    return lines


def render_markdown(run: RunResult) -> str:
    lines = _render_metadata(run.metadata)
    by_interface: dict[str, dict[str, list[CellResult]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for cell in run.cells:
        by_interface[cell.interface][cell.scenario].append(cell)

    for interface in _INTERFACE_ORDER:
        if interface not in by_interface:
            continue
        lines.append(f"## {interface}")
        lines.append("")
        for scenario_name in sorted(by_interface[interface]):
            cells = by_interface[interface][scenario_name]
            lines.append(f"### {scenario_name}")
            lines.append("")
            backends = _backend_order({c.backend for c in cells})
            lines.append("| params | " + " | ".join(backends) + " |")
            lines.append("|" + "---|" * (len(backends) + 1))
            by_row: dict[str, dict[str, CellResult]] = defaultdict(dict)
            row_order: list[str] = []
            for cell in cells:
                key = _params_key(cell)
                if key not in by_row:
                    row_order.append(key)
                by_row[key][cell.backend] = cell
            for key in row_order:
                row_cells = [
                    _format_cell(by_row[key][b]) if b in by_row[key] else "—"
                    for b in backends
                ]
                lines.append(f"| {key} | " + " | ".join(row_cells) + " |")
            lines.append("")
    return "\n".join(lines)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/bench/test_report.py -v -p no:randomly`
Expected: 3 passed

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check bench/ tests/unit/bench/ --fix && uv run ruff format bench/ tests/unit/bench/
git add bench/core/report.py tests/unit/bench/test_report.py
git commit -m "feat: add markdown report generator for bench results"
```

---

### Task 11: CLI entry point

**Files:**
- Create: `bench/__main__.py`
- Test: `tests/unit/bench/test_cli.py`

**Interfaces:**
- Consumes: `run_matrix`, `RunnerConfig` (task 4); `all_scenarios()` (tasks 5-7); `STORE_ADAPTERS`, `BUS_ADAPTERS`, `SNAPSHOT_ADAPTERS`, `make_e2e_adapters` (tasks 3, 7-9); `render_markdown` (task 10); `RunResult` (task 1)
- Produces: `main(argv: list[str] | None = None) -> int` and `python -m bench` entry. Subcommands:
  - `run [--interface {store,bus,snapshot,e2e}] [--backend NAME] [--scenario NAME] [--quick] [--out DIR]` — filters are repeatable; default `--out` is `bench/results/`; writes `bench-<UTC timestamp YYYYmmdd-HHMMSS>.json`; prints per-cell one-liners and the output path; returns 1 if any cell failed, else 0
  - `report FILE [FILE ...] [--out FILE]` — renders Markdown to stdout or `--out`; returns 0

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/bench/test_cli.py
"""CLI tests: memory-only quick run end to end, then report over its output."""

from pathlib import Path

from bench.__main__ import main
from bench.core.results import RunResult


def test_run_quick_memory_only(tmp_path: Path) -> None:
    code = main(
        [
            "run",
            "--quick",
            "--backend",
            "memory",
            "--scenario",
            "store.append_batch",
            "--scenario",
            "snapshot.save",
            "--out",
            str(tmp_path),
        ]
    )
    assert code == 0
    files = list(tmp_path.glob("bench-*.json"))
    assert len(files) == 1
    run = RunResult.from_json(files[0].read_text())
    names = {c.scenario for c in run.cells}
    assert names == {"store.append_batch", "snapshot.save"}
    assert all(c.backend == "memory" for c in run.cells)
    assert all(c.status == "ok" for c in run.cells)


def test_report_command(tmp_path: Path) -> None:
    main(
        [
            "run",
            "--quick",
            "--backend",
            "memory",
            "--scenario",
            "store.append_batch",
            "--out",
            str(tmp_path),
        ]
    )
    result_file = next(tmp_path.glob("bench-*.json"))
    out_file = tmp_path / "report.md"
    code = main(["report", str(result_file), "--out", str(out_file)])
    assert code == 0
    text = out_file.read_text()
    assert "# Benchmark Report" in text
    assert "store.append_batch" in text


def test_unknown_filter_values_error() -> None:
    assert main(["run", "--backend", "nope"]) == 2
    assert main(["run", "--scenario", "nope.nope"]) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/bench/test_cli.py -v -p no:randomly`
Expected: FAIL — no `main` in `bench.__main__`

- [ ] **Step 3: Implement the CLI**

```python
# bench/__main__.py
"""CLI: `python -m bench run` and `python -m bench report`."""

import argparse
import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bench.adapters.base import BenchAdapter
from bench.adapters.buses import BUS_ADAPTERS
from bench.adapters.e2e import make_e2e_adapters
from bench.adapters.snapshots import SNAPSHOT_ADAPTERS
from bench.adapters.stores import STORE_ADAPTERS
from bench.core.report import render_markdown
from bench.core.results import RunResult
from bench.core.runner import RunnerConfig, run_matrix
from bench.scenarios import all_scenarios

DEFAULT_OUT = Path(__file__).parent / "results"


def _build_adapters(backends: list[str] | None) -> dict[str, list[BenchAdapter[Any]]]:
    def wanted(name: str) -> bool:
        return backends is None or name in backends

    return {
        "store": [cls() for name, cls in STORE_ADAPTERS.items() if wanted(name)],
        "bus": [cls() for name, cls in BUS_ADAPTERS.items() if wanted(name)],
        "snapshot": [cls() for name, cls in SNAPSHOT_ADAPTERS.items() if wanted(name)],
        "e2e": [a for a in make_e2e_adapters() if wanted(a.name)],
    }


def _cmd_run(args: argparse.Namespace) -> int:
    scenarios = all_scenarios()
    known_backends = (
        set(STORE_ADAPTERS) | set(BUS_ADAPTERS) | set(SNAPSHOT_ADAPTERS)
    )
    if args.backend:
        unknown = set(args.backend) - known_backends
        if unknown:
            print(f"unknown backend(s): {', '.join(sorted(unknown))}", file=sys.stderr)
            return 2
    if args.scenario:
        known = {s.name for s in scenarios}
        unknown = set(args.scenario) - known
        if unknown:
            print(f"unknown scenario(s): {', '.join(sorted(unknown))}", file=sys.stderr)
            return 2
        scenarios = [s for s in scenarios if s.name in set(args.scenario)]
    if args.interface:
        scenarios = [s for s in scenarios if s.interface in set(args.interface)]

    adapters = _build_adapters(args.backend or None)
    config = RunnerConfig(quick=args.quick)
    result = asyncio.run(run_matrix(scenarios, adapters, config))

    for cell in result.cells:
        print(f"{cell.status:>7}  {cell.cell_id}" + (f"  ({cell.reason})" if cell.reason else ""))

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    out_path = out_dir / f"bench-{stamp}.json"
    result.save(out_path)
    print(f"\nresults written to {out_path}")

    return 1 if any(c.status == "failed" for c in result.cells) else 0


def _cmd_report(args: argparse.Namespace) -> int:
    for path in args.files:
        run = RunResult.from_json(Path(path).read_text())
        text = render_markdown(run)
        if args.out:
            Path(args.out).write_text(text)
            print(f"report written to {args.out}")
        else:
            print(text)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bench", description="eventsource backend benchmarks")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="run the benchmark matrix")
    run_parser.add_argument("--interface", action="append", choices=["store", "bus", "snapshot", "e2e"])
    run_parser.add_argument("--backend", action="append")
    run_parser.add_argument("--scenario", action="append")
    run_parser.add_argument("--quick", action="store_true")
    run_parser.add_argument("--out", default=str(DEFAULT_OUT))

    report_parser = sub.add_parser("report", help="render results as markdown")
    report_parser.add_argument("files", nargs="+")
    report_parser.add_argument("--out", default=None)

    args = parser.parse_args(argv)
    if args.command == "run":
        return _cmd_run(args)
    return _cmd_report(args)


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/bench/test_cli.py -v -p no:randomly`
Expected: 3 passed

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check bench/ tests/unit/bench/ --fix && uv run ruff format bench/ tests/unit/bench/
git add bench/__main__.py tests/unit/bench/test_cli.py
git commit -m "feat: add bench cli with run and report subcommands"
```

---

### Task 12: Infrastructure — compose file, Makefile targets, mypy wiring, README

**Files:**
- Create: `docker-compose.bench.yml`
- Create: `bench/README.md`
- Modify: `Makefile` (add bench targets; extend `types` target with `bench/`)
- Modify: `pyproject.toml` (add `"bench"` to `[tool.ruff.lint.isort] known-first-party`)

**Interfaces:**
- Consumes: CLI (`python -m bench`), env var names from Global Constraints
- Produces: `make bench-up`, `make bench-down`, `make bench`, `make bench-quick`, `make bench-report RESULTS=<file>`; services postgres:5434, redis:6381, kafka:9094, rabbitmq:5673

- [ ] **Step 1: Create docker-compose.bench.yml**

```yaml
# Docker Compose services for the benchmark harness (bench/).
#
# Coexists with docker-compose.test.yml -- all host ports are offset.
# No volumes: benchmark data is disposable by design.
#
#   make bench-up      # start all four services
#   make bench         # run the full matrix
#   make bench-down    # stop and remove

services:
  postgres:
    image: postgres:15
    container_name: eventsource-bench-postgres
    environment:
      POSTGRES_DB: eventsource_bench
      POSTGRES_USER: bench
      POSTGRES_PASSWORD: bench
    ports:
      - "${BENCH_POSTGRES_PORT:-5434}:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U bench -d eventsource_bench"]
      interval: 5s
      timeout: 5s
      retries: 5
      start_period: 10s

  redis:
    image: redis:7
    container_name: eventsource-bench-redis
    ports:
      - "${BENCH_REDIS_PORT:-6381}:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

  rabbitmq:
    image: rabbitmq:3-management
    container_name: eventsource-bench-rabbitmq
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    ports:
      - "${BENCH_RABBITMQ_PORT:-5673}:5672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 10s
      timeout: 10s
      retries: 5
      start_period: 20s

  kafka:
    image: apache/kafka:3.7.0
    container_name: eventsource-bench-kafka
    ports:
      - "${BENCH_KAFKA_PORT:-9094}:9094"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:${BENCH_KAFKA_PORT:-9094}
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 10
      start_period: 30s

networks:
  default:
    name: eventsource-bench-network
```

- [ ] **Step 2: Add Makefile targets and mypy wiring**

First run `grep -n "types:\|COMPOSE :=\|.PHONY" Makefile` to see the exact current text. Then:

1. Add near the existing `COMPOSE :=` line:

```make
BENCH_COMPOSE := docker compose -f docker-compose.bench.yml
```

2. Extend the existing `types` target so it also checks `bench/` (keep the existing src invocation exactly as-is and append ` bench/` to its path list, e.g. `uv run mypy src/ bench/ --config-file=pyproject.toml`).

3. Add the new targets and register them in `.PHONY`:

```make
bench-up:  ## Start Postgres + Redis + Kafka + RabbitMQ benchmark services
	$(BENCH_COMPOSE) up -d --wait

bench-down:  ## Stop and remove the benchmark services
	$(BENCH_COMPOSE) down -v

bench:  ## Run the full benchmark matrix (services optional; unavailable backends skip)
	uv run python -m bench run

bench-quick:  ## Fast sanity pass over the matrix
	uv run python -m bench run --quick

bench-report:  ## Render a results file: make bench-report RESULTS=bench/results/bench-<ts>.json
	uv run python -m bench report $(RESULTS)
```

4. In `pyproject.toml`, change `known-first-party = ["eventsource"]` to `known-first-party = ["eventsource", "bench"]`.

- [ ] **Step 3: Write bench/README.md**

```markdown
# bench — cross-backend benchmark harness

Internal tooling (not part of the `eventsource` package). Runs the same
scenario catalog across every backend and reports comparable numbers.
Design: `docs/superpowers/specs/2026-07-29-backend-benchmark-matrix-design.md`.

## Usage

    make bench-up       # start postgres/redis/kafka/rabbitmq (docker-compose.bench.yml)
    make bench          # full matrix (~20 min); unavailable backends are skipped
    make bench-quick    # trimmed grids, ~2 min
    make bench-report RESULTS=bench/results/bench-<ts>.json
    make bench-down

Without Docker, `make bench` still runs the memory and SQLite backends.

Filters: `uv run python -m bench run --interface store --backend postgresql
--scenario store.append_batch --quick`

## Endpoints (env-overridable)

| Variable | Default |
|---|---|
| `BENCH_POSTGRES_URL` | `postgresql+asyncpg://bench:bench@localhost:5434/eventsource_bench` |
| `BENCH_REDIS_URL` | `redis://localhost:6381` |
| `BENCH_KAFKA_SERVERS` | `localhost:9094` |
| `BENCH_RABBITMQ_URL` | `amqp://guest:guest@localhost:5673/` |

## Interpreting results

Raw JSON lands in `bench/results/` (gitignored, schema-versioned). The
Markdown report renders one table per scenario: rows are parameter
combinations, columns are backends. Numbers are only comparable within a
single run on one machine -- the metadata header records the context.
Numbers reach docs by manual curation, never automatically.
```

- [ ] **Step 4: Verify**

Run: `uv run mypy src/ bench/ --config-file=pyproject.toml`
Expected: no errors (fix any strict-mode findings in bench/ now)

Run: `uv run ruff check bench/ tests/unit/bench/ && uv run ruff format --check bench/ tests/unit/bench/`
Expected: clean

Run: `uv run python -m bench run --quick --backend memory`
Expected: exit 0, all-`ok` cell lines for memory scenarios, a results file under `bench/results/`

Run: `docker compose -f docker-compose.bench.yml config --quiet`
Expected: exit 0 (compose file is syntactically valid — services are NOT started in this task)

- [ ] **Step 5: Commit**

```bash
git add docker-compose.bench.yml bench/README.md Makefile pyproject.toml
git commit -m "feat: add bench compose services makefile targets and readme"
```

---

### Task 13: Full-suite verification and live smoke (ORCHESTRATOR ONLY)

Not dispatched to a worker — the orchestrator runs this after all tasks land, per the Global Constraints test-scope rule.

- [ ] **Step 1: Full check**

Run: `make check` (lint, mypy, import-linter, bandit, unit suite)
Expected: green. Dispatch any failures back to a fix-up worker with the failing output.

- [ ] **Step 2: Live smoke against real services (requires Docker)**

```bash
make bench-up
uv run python -m bench run --quick
make bench-down
```

Expected: exit 0; postgres/sqlite/redis/kafka/rabbitmq cells all `ok` (not skipped); a results file written. Broker adapters were only probe-tested in unit tests — this is where their consume paths are actually proven. If a broker cell fails, dispatch the failure output plus the relevant `tests/integration/bus/test_*.py` reference to a fix-up worker scoped to task 9's files.

- [ ] **Step 3: Render a report and eyeball it**

```bash
uv run python -m bench report bench/results/bench-*.json --out /tmp/bench-report.md
```

Expected: tables render, metadata header present, no `failed:` cells.

---

## Execution Notes

- Tasks 1→7 are strictly sequential (each consumes the previous task's interfaces). Tasks 8, 9, 10 are independent of each other (all depend on ≤7). Task 11 needs 8-10. Task 12 needs 11. Task 13 is the orchestrator's.
- Every worker prompt must include: the task text verbatim, the Global Constraints section, and the reminder "run only the test file(s) named in this task — never the full suite; ruff+format before committing."
- Reviewer prompts must include: "do not run tests; assume they pass as reported and judge the diff."
