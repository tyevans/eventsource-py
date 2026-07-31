# Aggregates Application Ring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the aggregates vertical into the `domain/` and `application/` rings, dissolve `AggregateSnapshotManager` into policy + scheduler + single-construction-path collaborators, and delete the transition/deprecation cruft this makes obsolete.

**Architecture:** `AggregateRoot`/`DeclarativeAggregate` move to the entities ring (`domain/aggregate.py`). A new use-case ring package `application/aggregates/` holds the slimmed `AggregateRepository` and its snapshotting collaborators (`SnapshotPolicy`, `SnapshotScheduler`, `take_snapshot`, `read_valid_snapshot`). `Snapshot`/`SnapshotStore` physically relocate to `ports/snapshots.py`; snapshot exceptions consolidate into `exceptions.py`; the `aggregates/` and `snapshots/` packages are deleted with no aliases (unreleased software).

**Tech Stack:** Python 3.13, pydantic v2, pytest + pytest-asyncio + hypothesis, mypy strict, ruff, import-linter, mutmut.

**Spec:** `docs/superpowers/specs/2026-07-30-aggregates-application-ring-design.md`

## Global Constraints

- **Behavior preservation:** existing tests keep their assertions; only imports/paths change. Log messages and levels on snapshot paths are preserved verbatim.
- **No aliases/shims:** deleted modules are deleted; all importers move to new paths.
- **Public API names unchanged:** `eventsource.AggregateRoot`, `DeclarativeAggregate`, `AggregateRepository`, `Snapshot`, `SnapshotStore`, `InMemorySnapshotStore`, `SnapshotError` hierarchy stay importable from `eventsource`.
- **Test discipline:** each task runs ONLY the test commands listed in that task — never the full suite, never `make check`. The orchestrator runs the full suite between tasks.
- **Every commit message** follows `.claude/rules/commits.md` (`<type>: <lowercase description>`) and ends with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- **mypy strict:** all new code fully annotated; `uv run mypy src/eventsource/ --config-file=pyproject.toml` scoped runs are listed per task.
- Pre-commit hooks run on commit (ruff, mypy, import-linter); a task is not done until its commit succeeds.

---

### Task 1: Move AggregateRoot/DeclarativeAggregate to `domain/aggregate.py`

**Files:**
- Move: `src/eventsource/aggregates/base.py` → `src/eventsource/domain/aggregate.py` (git mv, content unchanged except docstring header)
- Modify: `src/eventsource/domain/__init__.py`, `src/eventsource/__init__.py:41`, `src/eventsource/aggregates/__init__.py`, `src/eventsource/aggregates/repository.py:12`, `src/eventsource/aggregates/snapshot_manager.py:36`, `src/eventsource/snapshots/strategies.py:35`, `src/eventsource/testing/bdd.py:47`, `src/eventsource/multitenancy/repository.py:14`
- Move tests: `tests/unit/test_aggregate_root.py` → `tests/unit/domain/test_aggregate_root.py`; `tests/unit/aggregates/test_aggregate_snapshot_methods.py`, `test_create_event.py`, `test_deferred_state.py` → `tests/unit/domain/`
- Modify tests (imports only): the moved files plus `tests/unit/test_additional_coverage.py`, `tests/unit/testing/test_module_structure.py`, `tests/unit/testing/test_bdd.py`, `tests/fixtures/aggregates.py`, `tests/unit/aggregates/test_repository_snapshot.py`, `tests/unit/aggregates/test_repository_tracing.py`, `tests/unit/aggregates/test_aggregate_type_inference.py`, `tests/unit/test_aggregate_repository.py`

**Interfaces:**
- Produces: `from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate` — the canonical import for all later tasks. `eventsource.domain` re-exports both.

- [ ] **Step 1: Move the module**

```bash
git mv src/eventsource/aggregates/base.py src/eventsource/domain/aggregate.py
```

- [ ] **Step 2: Update `domain/__init__.py`**

```python
"""Entities ring. Pure: stdlib + pydantic only.

TRANSITION: DomainEvent/EventRegistry still live in eventsource.events and
count as this ring until sub-project 3 moves them.
"""

from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
from eventsource.domain.stream_id import CATEGORY_PATTERN, StreamId

__all__ = ["CATEGORY_PATTERN", "AggregateRoot", "DeclarativeAggregate", "StreamId"]
```

- [ ] **Step 3: Update every `eventsource.aggregates.base` import in src/**

Replace `from eventsource.aggregates.base import ...` with `from eventsource.domain.aggregate import ...` in: `src/eventsource/__init__.py`, `aggregates/__init__.py`, `aggregates/repository.py`, `aggregates/snapshot_manager.py` (TYPE_CHECKING block), `snapshots/strategies.py` (TYPE_CHECKING block), `testing/bdd.py`, `multitenancy/repository.py`. Verify none remain:

```bash
grep -rn "aggregates.base\|aggregates import base" src/ --include="*.py"
```

Expected: no matches.

- [ ] **Step 4: Move and re-point the tests**

```bash
git mv tests/unit/test_aggregate_root.py tests/unit/domain/test_aggregate_root.py
git mv tests/unit/aggregates/test_aggregate_snapshot_methods.py tests/unit/domain/
git mv tests/unit/aggregates/test_create_event.py tests/unit/domain/
git mv tests/unit/aggregates/test_deferred_state.py tests/unit/domain/
grep -rln "eventsource.aggregates.base" tests/ | xargs sed -i 's/eventsource\.aggregates\.base/eventsource.domain.aggregate/g'
```

- [ ] **Step 5: Run targeted tests**

Run: `uv run pytest tests/unit/domain/ tests/unit/aggregates/ tests/unit/test_aggregate_repository.py tests/unit/test_additional_coverage.py tests/unit/testing/ -q`
Expected: all pass.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "refactor: move aggregate root into domain ring"
```

---

### Task 2: Create `application/aggregates/snapshotting.py` (TDD)

**Files:**
- Create: `src/eventsource/application/__init__.py`, `src/eventsource/application/aggregates/__init__.py`, `src/eventsource/application/aggregates/snapshotting.py`
- Create: `tests/unit/application/__init__.py`, `tests/unit/application/aggregates/__init__.py`, `tests/unit/application/aggregates/test_snapshotting.py`

**Interfaces:**
- Consumes: `eventsource.domain.aggregate.AggregateRoot` (Task 1); `eventsource.ports.snapshots.Snapshot, SnapshotStore` (existing alias; becomes physical in Task 4 — import path already correct); `eventsource._internal.background_tasks.BackgroundTaskManager`.
- Produces (exact, used by Task 3):
  - `class SnapshotPolicy(Protocol)`: `should_snapshot(aggregate: AggregateRoot[Any], events_since_snapshot: int) -> bool`
  - `EveryNEvents(n: int)` frozen dataclass policy; `Never()` frozen dataclass policy
  - `async take_snapshot(aggregate: AggregateRoot[Any], aggregate_type: str, store: SnapshotStore) -> Snapshot` (errors propagate)
  - `async read_valid_snapshot(store: SnapshotStore, aggregate_id: UUID, aggregate_type: str, aggregate_factory: type[AggregateRoot[Any]]) -> Snapshot | None`
  - `class SnapshotScheduler(Protocol)`: `async schedule(write: Coroutine[Any, Any, Snapshot], *, aggregate_type: str, aggregate_id: UUID) -> Snapshot | None`; `pending_count: int` property; `async await_pending() -> int`
  - `ImmediateScheduler()`, `BackgroundScheduler()`

- [ ] **Step 1: Write the failing tests**

`tests/unit/application/aggregates/test_snapshotting.py` (use `tests/fixtures/aggregates.py` aggregate fixtures where a real aggregate is needed; otherwise a minimal local `DeclarativeAggregate`):

```python
"""Tests for application.aggregates.snapshotting collaborators."""

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.application.aggregates.snapshotting import (
    BackgroundScheduler,
    EveryNEvents,
    ImmediateScheduler,
    Never,
    SnapshotPolicy,
    SnapshotScheduler,
    read_valid_snapshot,
    take_snapshot,
)
from eventsource.ports.snapshots import Snapshot
from tests.fixtures.aggregates import OrderAggregate  # adjust name to the actual fixture class


class TestEveryNEvents:
    def test_true_on_threshold_boundary(self, order_at_version_100):
        assert EveryNEvents(100).should_snapshot(order_at_version_100, 1) is True

    def test_false_off_boundary(self, order_at_version_101):
        assert EveryNEvents(100).should_snapshot(order_at_version_101, 1) is False

    def test_false_at_version_zero(self, fresh_order):
        assert EveryNEvents(1).should_snapshot(fresh_order, 0) is False

    def test_rejects_nonpositive_n(self):
        with pytest.raises(ValueError):
            EveryNEvents(0)
        with pytest.raises(ValueError):
            EveryNEvents(-5)

    def test_satisfies_policy_protocol(self):
        assert isinstance(EveryNEvents(10), SnapshotPolicy)


class TestNever:
    def test_always_false(self, order_at_version_100):
        assert Never().should_snapshot(order_at_version_100, 10_000) is False

    def test_satisfies_policy_protocol(self):
        assert isinstance(Never(), SnapshotPolicy)


class TestTakeSnapshot:
    async def test_writes_snapshot_with_aggregate_state(self, order_at_version_3):
        store = InMemorySnapshotStore()
        snap = await take_snapshot(order_at_version_3, "Order", store)
        assert snap.version == order_at_version_3.version
        assert snap.aggregate_id == order_at_version_3.aggregate_id
        assert snap.schema_version == getattr(type(order_at_version_3), "schema_version", 1)
        stored = await store.get_snapshot(snap.aggregate_id, "Order")
        assert stored == snap

    async def test_store_errors_propagate(self, order_at_version_3, failing_store):
        with pytest.raises(RuntimeError):
            await take_snapshot(order_at_version_3, "Order", failing_store)


class TestReadValidSnapshot:
    async def test_returns_snapshot_when_schema_matches(self, order_at_version_3):
        store = InMemorySnapshotStore()
        snap = await take_snapshot(order_at_version_3, "Order", store)
        got = await read_valid_snapshot(store, snap.aggregate_id, "Order", type(order_at_version_3))
        assert got == snap

    async def test_returns_none_when_missing(self):
        store = InMemorySnapshotStore()
        assert await read_valid_snapshot(store, uuid4(), "Order", OrderAggregate) is None

    async def test_returns_none_on_schema_mismatch(self):
        store = InMemorySnapshotStore()
        await store.save_snapshot(Snapshot(
            aggregate_id=(aid := uuid4()), aggregate_type="Order", version=5,
            state={}, schema_version=999, created_at=datetime.now(UTC),
        ))
        assert await read_valid_snapshot(store, aid, "Order", OrderAggregate) is None

    async def test_returns_none_on_store_error(self, failing_store):
        assert await read_valid_snapshot(failing_store, uuid4(), "Order", OrderAggregate) is None


class TestImmediateScheduler:
    async def test_awaits_write_and_returns_snapshot(self, order_at_version_3):
        store = InMemorySnapshotStore()
        sched = ImmediateScheduler()
        snap = await sched.schedule(
            take_snapshot(order_at_version_3, "Order", store),
            aggregate_type="Order", aggregate_id=order_at_version_3.aggregate_id,
        )
        assert snap is not None
        assert await store.get_snapshot(snap.aggregate_id, "Order") == snap

    async def test_swallows_write_failure_returns_none(self, order_at_version_3, failing_store, caplog):
        sched = ImmediateScheduler()
        result = await sched.schedule(
            take_snapshot(order_at_version_3, "Order", failing_store),
            aggregate_type="Order", aggregate_id=order_at_version_3.aggregate_id,
        )
        assert result is None
        assert "Failed to create snapshot" in caplog.text

    async def test_uniform_pending_surface(self):
        sched = ImmediateScheduler()
        assert sched.pending_count == 0
        assert await sched.await_pending() == 0
        assert isinstance(sched, SnapshotScheduler)


class TestBackgroundScheduler:
    async def test_schedules_and_completes(self, order_at_version_3):
        store = InMemorySnapshotStore()
        sched = BackgroundScheduler()
        result = await sched.schedule(
            take_snapshot(order_at_version_3, "Order", store),
            aggregate_type="Order", aggregate_id=order_at_version_3.aggregate_id,
        )
        assert result is None  # deferred
        await sched.await_pending()
        assert await store.get_snapshot(order_at_version_3.aggregate_id, "Order") is not None

    async def test_swallows_background_failure(self, order_at_version_3, failing_store, caplog):
        sched = BackgroundScheduler()
        await sched.schedule(
            take_snapshot(order_at_version_3, "Order", failing_store),
            aggregate_type="Order", aggregate_id=order_at_version_3.aggregate_id,
        )
        await sched.await_pending()
        assert "Background snapshot creation failed" in caplog.text

    async def test_satisfies_scheduler_protocol(self):
        assert isinstance(BackgroundScheduler(), SnapshotScheduler)
```

Add module-level fixtures in the same file: `failing_store` (an `InMemorySnapshotStore` subclass whose `save_snapshot`/`get_snapshot` raise `RuntimeError("store down")`), and `fresh_order` / `order_at_version_3` / `order_at_version_100` / `order_at_version_101` built by applying N events to the fixture aggregate from `tests/fixtures/aggregates.py` (read that file first and reuse its event/aggregate classes; if it lacks a convenient one, define a minimal `DeclarativeAggregate` + one `DomainEvent` subclass locally in the test module).

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/application/ -q`
Expected: FAIL — `ModuleNotFoundError: eventsource.application`.

- [ ] **Step 3: Implement `snapshotting.py`**

`src/eventsource/application/__init__.py`:

```python
"""Use-case ring: application services composing domain objects with ports."""
```

`src/eventsource/application/aggregates/__init__.py`:

```python
"""Aggregate use cases: repository and snapshotting collaborators."""

from eventsource.application.aggregates.snapshotting import (
    BackgroundScheduler,
    EveryNEvents,
    ImmediateScheduler,
    Never,
    SnapshotPolicy,
    SnapshotScheduler,
    read_valid_snapshot,
    take_snapshot,
)

__all__ = [
    "BackgroundScheduler",
    "EveryNEvents",
    "ImmediateScheduler",
    "Never",
    "SnapshotPolicy",
    "SnapshotScheduler",
    "read_valid_snapshot",
    "take_snapshot",
]
```

(`AggregateRepository` is added to this `__init__` in Task 3.)

`src/eventsource/application/aggregates/snapshotting.py`:

```python
"""Snapshotting collaborators for the aggregate repository.

Snapshots are disposable optimizations, never the source of truth (ADR 0021,
superseding ADR 0017): every automatic-path failure degrades to full event
replay instead of raising. Four collaborators replace the former
AggregateSnapshotManager:

- SnapshotPolicy (EveryNEvents / Never): *when* to snapshot — pure predicate.
- SnapshotScheduler (ImmediateScheduler / BackgroundScheduler): *how* the
  write executes — sync-and-swallow or fire-and-forget with a join point.
- take_snapshot(): the single spelling of snapshot construction. Errors
  propagate; degradation is the scheduler's job, so the manual path
  (AggregateRepository.create_snapshot) stays strict.
- read_valid_snapshot(): load-path fetch + schema validation; all failure
  modes collapse to None.
"""

import logging
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from eventsource._internal.background_tasks import BackgroundTaskManager
from eventsource.domain.aggregate import AggregateRoot
from eventsource.ports.snapshots import Snapshot, SnapshotStore

logger = logging.getLogger(__name__)


@runtime_checkable
class SnapshotPolicy(Protocol):
    """Decides *when* a snapshot should be taken. Pure and synchronous."""

    def should_snapshot(
        self, aggregate: AggregateRoot[Any], events_since_snapshot: int
    ) -> bool: ...


@dataclass(frozen=True)
class EveryNEvents:
    """Snapshot at deterministic version boundaries: version % n == 0.

    Keyed off the aggregate version (not events_since_snapshot) so that two
    processes saving the same aggregate agree on where boundaries fall. A
    save jumping the version across a boundary without landing on it takes
    no snapshot — acceptable, snapshots are an optimization.
    """

    n: int

    def __post_init__(self) -> None:
        if self.n <= 0:
            raise ValueError(f"EveryNEvents requires n >= 1, got {self.n}")

    def should_snapshot(
        self, aggregate: AggregateRoot[Any], events_since_snapshot: int
    ) -> bool:
        return aggregate.version > 0 and aggregate.version % self.n == 0


@dataclass(frozen=True)
class Never:
    """Manual mode: automatic snapshotting disabled."""

    def should_snapshot(
        self, aggregate: AggregateRoot[Any], events_since_snapshot: int
    ) -> bool:
        return False


async def take_snapshot(
    aggregate: AggregateRoot[Any], aggregate_type: str, store: SnapshotStore
) -> Snapshot:
    """Build and persist a snapshot of the aggregate. The single spelling
    of snapshot construction. Errors propagate to the caller."""
    schema_version = getattr(type(aggregate), "schema_version", 1)
    snapshot = Snapshot(
        aggregate_id=aggregate.aggregate_id,
        aggregate_type=aggregate_type,
        version=aggregate.version,
        state=aggregate._serialize_state(),
        schema_version=schema_version,
        created_at=datetime.now(UTC),
    )
    await store.save_snapshot(snapshot)
    logger.info(
        "Created snapshot for %s/%s at version %d (schema_version=%d)",
        aggregate_type,
        aggregate.aggregate_id,
        snapshot.version,
        schema_version,
    )
    return snapshot


async def read_valid_snapshot(
    store: SnapshotStore,
    aggregate_id: UUID,
    aggregate_type: str,
    aggregate_factory: type[AggregateRoot[Any]],
) -> Snapshot | None:
    """Fetch and validate a snapshot for the load path.

    Store error, missing snapshot, and schema mismatch all collapse to
    None: the repository falls back to full event replay."""
    try:
        snapshot = await store.get_snapshot(aggregate_id, aggregate_type)
    except Exception as e:
        logger.warning(
            "Error loading snapshot for %s/%s: %s. Falling back to event replay.",
            aggregate_type,
            aggregate_id,
            e,
        )
        return None
    if snapshot is None:
        return None
    expected = getattr(aggregate_factory, "schema_version", 1)
    if snapshot.schema_version != expected:
        logger.info(
            "Snapshot schema version mismatch for %s/%s: "
            "snapshot has v%d, aggregate expects v%d. "
            "Falling back to full event replay.",
            aggregate_type,
            aggregate_id,
            snapshot.schema_version,
            expected,
        )
        return None
    logger.debug(
        "Loaded valid snapshot for %s/%s at version %d",
        aggregate_type,
        aggregate_id,
        snapshot.version,
    )
    return snapshot


@runtime_checkable
class SnapshotScheduler(Protocol):
    """Decides *how* a snapshot write executes: inline or in background.

    Every implementation carries the full surface — pending_count/
    await_pending are 0/0 for schedulers with nothing in flight — so no
    caller ever needs to sniff the concrete type."""

    async def schedule(
        self,
        write: Coroutine[Any, Any, Snapshot],
        *,
        aggregate_type: str,
        aggregate_id: UUID,
    ) -> Snapshot | None: ...

    @property
    def pending_count(self) -> int: ...

    async def await_pending(self) -> int: ...


class ImmediateScheduler:
    """Awaits the write inline; failures are logged and swallowed so a
    snapshot problem never fails a save whose events already committed."""

    async def schedule(
        self,
        write: Coroutine[Any, Any, Snapshot],
        *,
        aggregate_type: str,
        aggregate_id: UUID,
    ) -> Snapshot | None:
        try:
            return await write
        except Exception as e:
            logger.warning(
                "Failed to create snapshot for %s/%s: %s",
                aggregate_type,
                aggregate_id,
                e,
                exc_info=True,
            )
            return None

    @property
    def pending_count(self) -> int:
        return 0

    async def await_pending(self) -> int:
        return 0


class BackgroundScheduler:
    """Fire-and-forget via BackgroundTaskManager; await_pending() is the
    join point for tests and graceful shutdown."""

    def __init__(self) -> None:
        self._tasks = BackgroundTaskManager()

    async def schedule(
        self,
        write: Coroutine[Any, Any, Snapshot],
        *,
        aggregate_type: str,
        aggregate_id: UUID,
    ) -> Snapshot | None:
        self._tasks.submit(self._guarded(write, aggregate_type, aggregate_id))
        return None

    async def _guarded(
        self,
        write: Coroutine[Any, Any, Snapshot],
        aggregate_type: str,
        aggregate_id: UUID,
    ) -> None:
        try:
            await write
            logger.debug(
                "Background snapshot created for %s/%s", aggregate_type, aggregate_id
            )
        except Exception as e:
            logger.warning(
                "Background snapshot creation failed for %s/%s: %s",
                aggregate_type,
                aggregate_id,
                e,
                exc_info=True,
            )

    @property
    def pending_count(self) -> int:
        return self._tasks.pending_count

    async def await_pending(self) -> int:
        return await self._tasks.await_all()


__all__ = [
    "BackgroundScheduler",
    "EveryNEvents",
    "ImmediateScheduler",
    "Never",
    "SnapshotPolicy",
    "SnapshotScheduler",
    "read_valid_snapshot",
    "take_snapshot",
]
```

Check `eventsource._internal.background_tasks.BackgroundTaskManager` for the exact names of `submit`/`pending_count`/`await_all` before writing — match whatever it actually exposes.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/application/ -q`
Expected: PASS.
Run: `uv run mypy src/eventsource/application/ --config-file=pyproject.toml`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: add snapshotting policy and scheduler collaborators in application ring"
```

---

### Task 3: Move AggregateRepository; dissolve manager and strategies; delete `aggregates/`

**Files:**
- Move: `src/eventsource/aggregates/repository.py` → `src/eventsource/application/aggregates/repository.py`
- Delete: `src/eventsource/aggregates/` (remaining: `__init__.py`, `snapshot_manager.py`, `README.md` — fold README content into a new `src/eventsource/application/aggregates/README.md`), `src/eventsource/snapshots/strategies.py`
- Modify: `src/eventsource/application/aggregates/__init__.py` (add `AggregateRepository`, `TAggregate`), `src/eventsource/__init__.py`, `src/eventsource/multitenancy/repository.py`
- Move tests: `tests/unit/aggregates/test_repository_snapshot.py`, `test_repository_tracing.py`, `test_aggregate_type_inference.py` → `tests/unit/application/aggregates/`; `tests/unit/test_aggregate_repository.py` → `tests/unit/application/aggregates/test_repository.py`; delete `tests/unit/aggregates/`
- Modify tests: `tests/integration/observability/test_tracing_integration.py` (import path only)

**Interfaces:**
- Consumes: everything Task 2 produces (exact signatures above).
- Produces: `from eventsource.application.aggregates.repository import AggregateRepository, TAggregate`. Constructor signature (all existing params unchanged, two added):
  `AggregateRepository(event_store, aggregate_factory, aggregate_type=None, event_publisher=None, snapshot_store=None, snapshot_threshold=None, snapshot_mode="sync", snapshot_policy=None, snapshot_scheduler=None, tracer=None, enable_tracing=True)`

- [ ] **Step 1: Move the module and re-point imports**

```bash
git mv src/eventsource/aggregates/repository.py src/eventsource/application/aggregates/repository.py
```

In the moved file, replace the imports of `AggregateSnapshotManager` and `snapshots.strategies` with:

```python
from eventsource.application.aggregates.snapshotting import (
    BackgroundScheduler,
    EveryNEvents,
    ImmediateScheduler,
    Never,
    SnapshotPolicy,
    SnapshotScheduler,
    read_valid_snapshot,
    take_snapshot,
)
```

and change the `Snapshot`/`SnapshotStore` TYPE_CHECKING import to `from eventsource.ports.snapshots import Snapshot, SnapshotStore`.

- [ ] **Step 2: Rewire the constructor**

Replace the snapshot-wiring block at the end of `__init__` (the `create_snapshot_strategy`/`AggregateSnapshotManager` section) with:

```python
        # Snapshot configuration (exposed via public properties)
        self._snapshot_store = snapshot_store
        self._snapshot_threshold = snapshot_threshold
        self._snapshot_mode = snapshot_mode

        if (snapshot_policy is not None or snapshot_scheduler is not None) and (
            snapshot_threshold is not None or snapshot_mode != "sync"
        ):
            raise ValueError(
                "Pass either snapshot_mode/snapshot_threshold or "
                "snapshot_policy/snapshot_scheduler, not both."
            )
        if snapshot_policy is not None:
            self._snapshot_policy: SnapshotPolicy = snapshot_policy
        elif snapshot_mode != "manual" and snapshot_threshold is not None:
            self._snapshot_policy = EveryNEvents(snapshot_threshold)
        else:
            self._snapshot_policy = Never()
        if snapshot_scheduler is not None:
            self._snapshot_scheduler: SnapshotScheduler = snapshot_scheduler
        elif snapshot_mode == "background":
            self._snapshot_scheduler = BackgroundScheduler()
        else:
            self._snapshot_scheduler = ImmediateScheduler()
```

New constructor params (typed): `snapshot_policy: SnapshotPolicy | None = None, snapshot_scheduler: SnapshotScheduler | None = None`, documented in the docstring alongside the existing mode knobs.

- [ ] **Step 3: Rewire load/save/create_snapshot/pending**

In `load()`, replace the `self._snapshot_manager.load_valid_snapshot(...)` call with:

```python
            if self._snapshot_store is not None:
                snapshot = await read_valid_snapshot(
                    self._snapshot_store,
                    aggregate_id,
                    self._aggregate_type,
                    self._aggregate_factory,
                )
```

In `save()`, replace the `maybe_create_snapshot` block with:

```python
                if self._snapshot_store is not None and self._snapshot_policy.should_snapshot(
                    aggregate, len(uncommitted_events)
                ):
                    with self._tracer.span(
                        "eventsource.repository.snapshot",
                        {
                            ATTR_AGGREGATE_ID: str(aggregate.aggregate_id),
                            ATTR_AGGREGATE_TYPE: self._aggregate_type,
                            ATTR_VERSION: aggregate.version,
                        },
                    ):
                        await self._snapshot_scheduler.schedule(
                            take_snapshot(aggregate, self._aggregate_type, self._snapshot_store),
                            aggregate_type=self._aggregate_type,
                            aggregate_id=aggregate.aggregate_id,
                        )
```

In `create_snapshot()`, replace the manager guard/delegation: guard becomes `if self._snapshot_store is None: raise RuntimeError(...)` (same message), body becomes `return await take_snapshot(aggregate, self._aggregate_type, self._snapshot_store)` inside the existing span. `await_pending_snapshots()` returns `await self._snapshot_scheduler.await_pending()`; `pending_snapshot_count` returns `self._snapshot_scheduler.pending_count` (no None guards needed — schedulers always exist).

Note: the `eventsource.snapshot_manager.*` span names disappear; repository spans (`eventsource.repository.*`) are the surviving trace surface. `tests/unit/application/aggregates/test_repository_tracing.py` may assert on manager span names — update those assertions to the repository span names; this is the one sanctioned assertion change, flag it in the commit message.

- [ ] **Step 4: Delete the dissolved modules**

```bash
git rm src/eventsource/aggregates/__init__.py src/eventsource/aggregates/snapshot_manager.py src/eventsource/snapshots/strategies.py
git mv src/eventsource/aggregates/README.md src/eventsource/application/aggregates/README.md
```

Update the README's paths/examples to the new module locations and remove manager/strategy references. Add `AggregateRepository`, `TAggregate` to `application/aggregates/__init__.py` imports and `__all__`. Update `src/eventsource/__init__.py`: `from eventsource.application.aggregates.repository import AggregateRepository` (public `__all__` unchanged). Update `multitenancy/repository.py` import. Verify:

```bash
grep -rn "eventsource.aggregates\|snapshot_manager\|snapshots.strategies\|SnapshotStrategy\|create_snapshot_strategy" src/ --include="*.py"
```

Expected: no matches.

- [ ] **Step 5: Move and re-point the tests**

```bash
git mv tests/unit/aggregates/test_repository_snapshot.py tests/unit/application/aggregates/
git mv tests/unit/aggregates/test_repository_tracing.py tests/unit/application/aggregates/
git mv tests/unit/aggregates/test_aggregate_type_inference.py tests/unit/application/aggregates/
git mv tests/unit/test_aggregate_repository.py tests/unit/application/aggregates/test_repository.py
git rm tests/unit/aggregates/__init__.py
```

`test_repository_snapshot.py` imports `ThresholdSnapshotStrategy` (5 sites) and `NoSnapshotStrategy` (1 site): rewrite those tests against the new surface — `ThresholdSnapshotStrategy(threshold=N)` usages become `EveryNEvents(N)` passed as `snapshot_policy=` (or plain `snapshot_threshold=N` where the test targets the mode-string path), `NoSnapshotStrategy()` becomes `snapshot_mode="manual"`. Keep every assertion's substance; only the construction changes. Update `eventsource.aggregates.repository` imports throughout tests and `tests/integration/observability/test_tracing_integration.py`.

- [ ] **Step 6: Run targeted tests**

Run: `uv run pytest tests/unit/application/ tests/unit/domain/ tests/unit/multitenancy/ tests/unit/test_public_api.py -q`
Expected: PASS.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "refactor: dissolve snapshot manager into repository-composed policy and scheduler"
```

---

### Task 4: Dissolve the `snapshots/` package into ports, exceptions, and adapters

**Files:**
- Move content: `src/eventsource/snapshots/interface.py` → `src/eventsource/ports/snapshots.py` (replacing the alias body; module docstring updated to say it's the port, no TRANSITION note)
- Move content: `src/eventsource/snapshots/exceptions.py` → append to `src/eventsource/exceptions.py` (classes verbatim: `SnapshotError`, `SnapshotDeserializationError`, `SnapshotSchemaVersionError`, `SnapshotNotFoundError`)
- Delete: `src/eventsource/snapshots/` entirely (`__init__.py`, `interface.py`, `exceptions.py`, `in_memory.py`, `postgresql.py`, `sqlite.py`)
- Modify: `src/eventsource/__init__.py`, `src/eventsource/testing/conformance_ports/snapshots.py`, plus every remaining `eventsource.snapshots` importer
- Modify tests: `tests/unit/snapshots/*` (imports; `test_snapshots_module.py` largely tests the transition package itself — delete tests that only assert alias/lazy-import behavior, keep and re-point the rest), `tests/unit/bench/test_adapters_sql.py`, `tests/unit/bench/test_adapters_memory.py`
- Move tests: `tests/unit/snapshots/` → `tests/unit/ports/test_snapshot_*` for interface/VO tests and `tests/unit/adapters/` for store-implementation tests, following where those directories already put sibling tests (inspect `tests/unit/ports/` and `tests/unit/adapters/` first and mirror their naming)

**Interfaces:**
- Produces: `from eventsource.ports.snapshots import Snapshot, SnapshotStore` (physical); `from eventsource.exceptions import SnapshotError, SnapshotDeserializationError, SnapshotSchemaVersionError, SnapshotNotFoundError`; adapter imports `from eventsource.adapters.{memory,postgresql,sqlite}.snapshots import ...` everywhere.

- [ ] **Step 1: Physically relocate the port**

Replace `src/eventsource/ports/snapshots.py` with the full contents of `snapshots/interface.py` (docstring adjusted: this is the snapshot port owned by the inner rings; contract unchanged — `SnapshotStore` stays an ABC in this slice, contract redesign is out of scope). Keep `__all__ = ["Snapshot", "SnapshotStore"]`.

- [ ] **Step 2: Consolidate exceptions**

Append the four snapshot exception classes from `snapshots/exceptions.py` to `src/eventsource/exceptions.py` verbatim (keep docstrings), and add them to its `__all__`. If `snapshots/exceptions.py` defines `SQLiteNotAvailableError`, note it actually lives in `adapters/sqlite/snapshots.py` — check before assuming; only the four `Snapshot*` classes move.

- [ ] **Step 3: Delete the package and re-point every importer**

```bash
git rm -r src/eventsource/snapshots/
grep -rln "eventsource.snapshots" src/ tests/ --include="*.py"
```

Re-point each hit:
- `Snapshot`/`SnapshotStore` → `eventsource.ports.snapshots`
- `Snapshot*Error` → `eventsource.exceptions`
- `InMemorySnapshotStore` → `eventsource.adapters.memory.snapshots`
- `PostgreSQLSnapshotStore` → `eventsource.adapters.postgresql.snapshots`
- `SQLiteSnapshotStore`, `SQLITE_AVAILABLE`, `SQLiteNotAvailableError` → `eventsource.adapters.sqlite.snapshots`

In `src/eventsource/__init__.py`, the `from eventsource.snapshots import (...)` block becomes:

```python
from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.exceptions import (
    SnapshotDeserializationError,
    SnapshotError,
    SnapshotNotFoundError,
    SnapshotSchemaVersionError,
)
from eventsource.ports.snapshots import Snapshot, SnapshotStore
```

(top-level `__all__` unchanged). The circular-import hazard documented in the old `snapshots/__init__.py` dies with the alias: `ports.snapshots` no longer imports from `snapshots/`, so no lazy `__getattr__` is needed anywhere.

- [ ] **Step 4: Re-home the snapshot tests**

Inspect `tests/unit/ports/` and `tests/unit/adapters/` layouts, then move: `test_snapshot.py` + `test_snapshot_store_interface.py` → `tests/unit/ports/`; `test_in_memory_snapshot_store.py`, `test_postgresql_snapshot_store.py`, `test_sqlite_snapshot_store.py`, `test_snapshot_store_tracing.py` → `tests/unit/adapters/` (mirroring existing adapter test naming); `test_snapshot_exceptions.py` → `tests/unit/test_exceptions_snapshots.py` or merge into an existing exceptions test module if one exists. Delete `tests/unit/snapshots/` including `test_snapshots_module.py` tests that only assert the transition package's lazy-export mechanics; port over any residual value (e.g. public-name coverage) into `tests/unit/test_public_api.py` if not already covered there.

- [ ] **Step 5: Run targeted tests**

Run: `uv run pytest tests/unit/ports/ tests/unit/adapters/ tests/unit/application/ tests/unit/test_public_api.py tests/unit/bench/ -q`
Expected: PASS.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "refactor: dissolve snapshots package into ports, exceptions, and adapter imports"
```

---

### Task 5: Remove 0.8.0-scheduled deprecation shims

**Files:**
- Modify: `src/eventsource/bus/memory.py` (delete `published_events` property and `clear_published_events()`, their `warnings` import usage if now unused, and the `_published_events`/`_published_lock` machinery IF nothing else uses it — check `publish()` first: if it appends to `_published_events`, delete the recording entirely; `RecordingEventBus` in `eventsource.testing` is the replacement)
- Modify: `src/eventsource/bus/kafka/bus.py` (delete `record_reconnection()` and `record_rebalance()`)
- Delete: `src/eventsource/repositories/_json.py` (whole module is a deprecation shim; verify nothing imports it: `grep -rn "repositories._json\|repositories import _json" src/ tests/`)
- Modify tests: `tests/unit/bus/test_memory.py`, `tests/unit/test_kafka_event_bus.py`, `tests/unit/bus/kafka/*` — delete tests that exercise the deprecated members/warnings; rewrite tests that used `published_events` as a convenience to wrap the bus in `RecordingEventBus` (see `eventsource.testing`)

**Interfaces:**
- Consumes: nothing from earlier tasks (independent).
- Produces: nothing consumed later.

- [ ] **Step 1: Delete the deprecated members**

Remove the code blocks listed above. For `bus/memory.py`, first read `publish()` and the class body: if `_published_events` is written but now never read, remove the list, the lock, and the append. Keep `warnings` imports only if still used elsewhere in each file.

- [ ] **Step 2: Fix the tests**

```bash
grep -rn "published_events\|clear_published_events\|record_reconnection\|record_rebalance" tests/ --include="*.py"
```

Decision rule: a test whose purpose is "the deprecated API warns/works" is deleted; a test using the deprecated API as scaffolding keeps its real assertions and switches to `RecordingEventBus` (import from `eventsource.testing`; read its class to get construction right).

- [ ] **Step 3: Run targeted tests**

Run: `uv run pytest tests/unit/bus/ tests/unit/test_kafka_event_bus.py tests/unit/repositories/ tests/unit/serialization/ -q`
Expected: PASS.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "refactor: remove deprecation shims scheduled for 0.8.0"
```

---

### Task 6: Property-based tests (hypothesis)

**Files:**
- Create: `tests/unit/application/aggregates/test_snapshotting_properties.py`
- Create: `tests/unit/domain/test_aggregate_memento_properties.py`

**Interfaces:**
- Consumes: Task 2's surface; `eventsource.domain.aggregate.DeclarativeAggregate`; `eventsource.adapters.memory.snapshots.InMemorySnapshotStore`.

- [ ] **Step 1: Write the policy/read-path property tests**

`tests/unit/application/aggregates/test_snapshotting_properties.py`:

```python
"""Property-based tests for snapshotting collaborators."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.application.aggregates.snapshotting import (
    EveryNEvents,
    Never,
    read_valid_snapshot,
)
from eventsource.ports.snapshots import Snapshot


def fake_aggregate(version: int):
    """Policies only read .version — a stub keeps these tests pure."""
    return SimpleNamespace(version=version, aggregate_id=uuid4())


@given(version=st.integers(min_value=0, max_value=10_000), n=st.integers(min_value=1, max_value=500))
def test_every_n_events_matches_boundary_predicate(version: int, n: int):
    expected = version > 0 and version % n == 0
    assert EveryNEvents(n).should_snapshot(fake_aggregate(version), 1) is expected


@given(version=st.integers(min_value=0, max_value=10_000), since=st.integers(min_value=0, max_value=10_000))
def test_never_is_never(version: int, since: int):
    assert Never().should_snapshot(fake_aggregate(version), since) is False


@given(n=st.integers(max_value=0))
def test_every_n_events_rejects_nonpositive(n: int):
    with pytest.raises(ValueError):
        EveryNEvents(n)


class VersionedFactory:
    schema_version = 3


@given(stored_schema=st.integers(min_value=1, max_value=10))
@pytest.mark.asyncio
async def test_read_valid_snapshot_iff_schema_matches(stored_schema: int):
    store = InMemorySnapshotStore()
    aid = uuid4()
    await store.save_snapshot(Snapshot(
        aggregate_id=aid, aggregate_type="Thing", version=7,
        state={"x": 1}, schema_version=stored_schema, created_at=datetime.now(UTC),
    ))
    result = await read_valid_snapshot(store, aid, "Thing", VersionedFactory)  # type: ignore[arg-type]
    if stored_schema == VersionedFactory.schema_version:
        assert result is not None and result.schema_version == stored_schema
    else:
        assert result is None
```

NOTE — hypothesis + async: this repo hit hypothesis/async interaction issues before (see MEMORY: event-bus contract work). Check how `tests/unit/bus/test_serialization_properties.py` and `tests/unit/multitenancy/test_context_properties.py` drive async code under `@given` and copy that exact pattern (they may run coroutines via `asyncio.run` inside a sync test instead of `pytest.mark.asyncio`). Match the house style; do not invent a new one.

- [ ] **Step 2: Write the memento round-trip property test**

`tests/unit/domain/test_aggregate_memento_properties.py` — define a small `DeclarativeAggregate` with a pydantic state model having `name: str`, `count: int`, `tags: list[str]`; hypothesis-generate state values; drive the aggregate to that state via an event or by constructing state directly, then:

```python
@given(name=st.text(max_size=50), count=st.integers(), tags=st.lists(st.text(max_size=10), max_size=5))
def test_serialize_restore_round_trip(name: str, count: int, tags: list[str]):
    agg = ThingAggregate(uuid4())
    agg._state = ThingState(name=name, count=count, tags=tags)  # use the real state-setting path found in domain/aggregate.py
    agg._version = 7
    dumped = agg._serialize_state()
    restored = ThingAggregate(agg.aggregate_id)
    restored._restore_from_snapshot(dumped, 7)
    assert restored.state == agg.state
    assert restored.version == 7
```

Read `domain/aggregate.py` first for the real attribute names (`_state`/`_version` are guesses — use whatever `_restore_from_snapshot` actually sets and the public accessors actually read; if state can't be assigned directly, apply a single event carrying the generated values instead).

- [ ] **Step 3: Run the property tests**

Run: `uv run pytest tests/unit/application/aggregates/test_snapshotting_properties.py tests/unit/domain/test_aggregate_memento_properties.py -q`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test: property-based coverage for snapshot policy, schema validation, and memento round-trip"
```

---

### Task 7: Mutation-testing and import-linter configuration

**Files:**
- Modify: `pyproject.toml` (`[tool.mutmut]` and `[tool.importlinter]` sections)

**Interfaces:** none (config only).

- [ ] **Step 1: Update mutmut targets**

In `[tool.mutmut] only_mutate`, the core-rings block already lists `src/eventsource/domain`, `src/eventsource/ports`, `src/eventsource/adapters`. Add:

```toml
    "src/eventsource/application",
```

In `pytest_add_cli_args_test_selection`, add:

```toml
    "tests/unit/application/",
    "tests/unit/domain/",
    "tests/unit/ports/",
```

(dedupe if already present).

- [ ] **Step 2: Update import-linter contracts**

Read the full `[tool.importlinter]` section first. Then:
1. Any contract naming a deleted module (`eventsource.aggregates.*`, `eventsource.snapshots.*`) is re-pointed to the new location (`eventsource.domain.aggregate`, `eventsource.application.aggregates.*`, `eventsource.ports.snapshots`) — e.g. the "Tier 0 must not import sqlalchemy" contract's `eventsource.aggregates.base` entry becomes `eventsource.domain.aggregate`.
2. Add a layers contract for the new ring if one doesn't already cover it:

```toml
[[tool.importlinter.contracts]]
name = "Application ring must not import adapters"
type = "forbidden"
source_modules = ["eventsource.application"]
forbidden_modules = ["eventsource.adapters", "eventsource.bus", "eventsource.repositories", "eventsource.locks"]
```

Note `eventsource.stores` is deliberately NOT forbidden yet — the repository still consumes `stores.interface.EventStore` (transition port, per spec Out of Scope).

- [ ] **Step 3: Verify both tools parse and pass**

Run: `uv run lint-imports`
Expected: all contracts pass.
Run: `uv run mutmut run --help > /dev/null && python -c "import tomllib; tomllib.load(open('pyproject.toml','rb'))"`
Expected: exit 0 (config parses; do NOT run a mutation campaign — that's an offline orchestrator activity).

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml && git commit -m "chore: extend mutmut and import-linter coverage to application ring"
```

---

### Task 8: ADR 0021, ADR-0017 supersession, and documentation sweep

**Files:**
- Create: `docs/adrs/0021-snapshot-policy-scheduler-composition.md`
- Modify: `docs/adrs/0017-snapshot-strategy-pattern.md` (Status section only), `docs/adrs/index.md`
- Modify: `docs/guides/snapshotting.md`, `docs/tutorials/14-snapshotting.md`, `docs/api/snapshots.md`, `docs/development/code-structure.md`
- Modify: `CLAUDE.md` (Project Structure block: remove `aggregates/`, `snapshots/`; add `domain/`, `application/`, `ports/`, `adapters/`), `.claude/rules/architecture.md` (transition lists: `aggregates/` no longer listed as transitional use-case ring; `snapshots/` no longer a transitional adapter location)
- Modify: `src/eventsource/application/aggregates/README.md` and `src/eventsource/domain/` README if one exists (check)

**Interfaces:** none (docs only).

- [ ] **Step 1: Write ADR 0021**

`docs/adrs/0021-snapshot-policy-scheduler-composition.md`, following the house ADR structure visible in ADR-0017 (title, Status, Context, Decision, Rationale, Consequences, Alternatives Considered, References). Content requirements — cover exactly these decisions:
- **Status:** Accepted; supersedes ADR 0017. Implemented in `src/eventsource/application/aggregates/snapshotting.py` and `repository.py`.
- **Context:** the manager anti-pattern — four responsibilities, duplicated construction (ADR-0017's own documented negative), isinstance-sniffing of `BackgroundSnapshotStrategy` (LSP), when/how merged in one protocol (ISP: `NoSnapshotStrategy` implements an unrunnable `execute_snapshot`); unreleased library ⇒ no shims.
- **Decision:** `SnapshotPolicy` (when; `EveryNEvents` keeps the deterministic `version % n == 0` boundary predicate and its straddle caveat; `Never` for manual), `SnapshotScheduler` (how; uniform pending surface on every implementation), `take_snapshot()` single construction path (strict — errors propagate; degradation lives in schedulers), `read_valid_snapshot()` (all failures → `None` → replay), repository composes all four; mode-string knobs preserved and mapped in `__init__`; new `snapshot_policy=`/`snapshot_scheduler=` escape hatches, mutually exclusive with the knobs.
- **Carried-forward rationale:** snapshots are disposable optimizations; every automatic failure degrades rather than raises; manual path stays strict — cite ADR 0017 rather than restating in full.
- **Consequences:** positive — one spelling of construction, no type-sniffing, policies/schedulers unit-testable in isolation, custom policies without library changes; negative — `BackgroundScheduler` still has no backpressure bound (carried from 0017); observable change — `eventsource.snapshot_manager.*` spans removed, repository spans remain.
- **References:** the spec, the new modules, the property tests.

- [ ] **Step 2: Mark ADR-0017 superseded and update the index**

In ADR-0017, change only the Status section: keep the existing text and prepend `Superseded by [ADR 0021](0021-snapshot-policy-scheduler-composition.md). Historical record of the strategy-pattern design it replaced.` Add 0021 to `docs/adrs/index.md` following its format.

- [ ] **Step 3: Documentation sweep**

```bash
grep -rln "snapshot_manager\|AggregateSnapshotManager\|SnapshotStrategy\|create_snapshot_strategy\|eventsource.aggregates\|eventsource.snapshots" docs/ CLAUDE.md .claude/rules/ --include="*.md"
```

Fix every hit: import paths to new locations, strategy examples to policy/scheduler examples (`ThresholdSnapshotStrategy(threshold=100)` → `snapshot_threshold=100` or `snapshot_policy=EveryNEvents(100)`; `BackgroundSnapshotStrategy` → `snapshot_mode="background"` or `snapshot_scheduler=BackgroundScheduler()`). Update `CLAUDE.md`'s Project Structure block and `.claude/rules/architecture.md` transition lists as described in Files. Do not touch `docs/superpowers/specs/` history or ADR bodies other than 0017's Status.

- [ ] **Step 4: Verify docs build/tests if present**

Run: `uv run pytest tests/unit/test_public_api.py -q` (public API doc-name assertions)
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "docs: adr 0021 snapshot composition, supersede adr 0017, migrate docs to ring layout"
```

---

## Self-Review Notes

- Spec coverage: packaging (T1-T4), snapshotting design (T2-T3), cruft removal (T3-T5), property tests (T6), mutation config (T7), ADR + docs (T8). Repository→ports/store migration and `stores/legacy.py` correctly absent (spec Out of Scope).
- Type consistency: `EveryNEvents(n)` positional; scheduler `schedule(write, *, aggregate_type, aggregate_id)`; `take_snapshot(aggregate, aggregate_type, store)`; `read_valid_snapshot(store, aggregate_id, aggregate_type, aggregate_factory)` — consistent across T2/T3/T6.
- Known judgment calls delegated with decision rules: fixture reuse (T2), hypothesis-async house style (T6), `_published_events` machinery removal (T5), test re-homing layout (T4).
