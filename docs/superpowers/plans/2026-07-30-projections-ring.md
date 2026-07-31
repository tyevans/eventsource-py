# Projections Ring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the projections vertical into `ports/` + `application/` + `adapters/`, dissolve `ProjectionCheckpointManager` and `ProjectionDLQManager` into application-ring functions, split the seven-method `CheckpointRepository` god-interface along its real consumer groups, and delete the `pyproject.toml` KNOWN VIOLATIONS block because the last documented Tier-0 blocker is gone.

**Architecture:** `CheckpointData`/`LagMetrics`/`DLQEntry`/`DLQStats`/`ProjectionFailureCount` and their protocols move to pure port modules `ports/checkpoints.py` and `ports/dlq.py`; `CheckpointRepository` splits into `ProjectionCheckpoints` + `SubscriptionPositions` + a composed alias. The projection base classes, coordinator, and retry policies move to `application/projections/`, with the two managers replaced by six module-level async functions. `SQLCheckpointRepository`, `SQLDLQRepository`, and `DatabaseProjection` become adapters under `adapters/sql/`; the in-memory repositories become adapters under `adapters/memory/`. `src/eventsource/projections/`, `repositories/checkpoint.py`, `repositories/dlq.py`, and `repositories/_dialect.py` are deleted outright.

**Tech Stack:** Python 3.13, pydantic v2, sqlalchemy 2 (adapters only), pytest + pytest-asyncio + hypothesis, mypy strict, ruff, import-linter, mutmut.

**Spec:** `docs/superpowers/specs/2026-07-30-projections-ring-design.md`

## Global Constraints

- **Unreleased software — no shims.** Deleted modules are deleted. No deprecation aliases, no back-compat re-exports, no `__getattr__` forwarding. `CheckpointRepositoryProtocol`, `DLQRepositoryProtocol`, `DLQRepository.list_failed_events`, and `DLQRepository.get_failed_event` are removed, not redirected.
- **No Manager classes.** The dissolution replaces classes with module-level async functions. Do not reintroduce a class that wraps one repository plus a tracer under any name.
- **Test discipline.** Implementers run ONLY the test commands listed in the task they are executing — never the full suite, never `make check`. The orchestrator runs the full suite after each task and dispatches fixes.
- **Grep sweeps include `bench/`.** Every module-move verification grep must cover `src/`, `tests/`, and `bench/`. (`examples/` imports only from top-level `eventsource`, so it is unaffected — but include it in the final sweep of Task 5 to keep that true.)
- **Commit messages** follow `.claude/rules/commits.md` — `<type>: <lowercase description>` — and every commit ends with the trailer:

  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

- **Observability is preserved byte-for-byte through the dissolution.** Span names (`eventsource.checkpoint_manager.update`, `.get_checkpoint`, `.get_lag_metrics`, `.reset`, `eventsource.dlq_manager.send_to_dlq`, `.get_failed_events`), span attributes, log messages, log levels, and `extra=` payloads are copied verbatim from the manager methods into the new functions. The span names deliberately outlive the classes they name; renaming them is out of scope.
- **Public API names unchanged.** Every name currently re-exported from `eventsource` keeps its re-export: `Projection`, `CheckpointTrackingProjection`, `DeclarativeProjection`, `DatabaseProjection`, `CheckpointRepository`, `CheckpointData`, `LagMetrics`, `InMemoryCheckpointRepository`, `SQLCheckpointRepository`, `DLQRepository`, `DLQEntry`, `DLQStats`, `ProjectionFailureCount`, `InMemoryDLQRepository`, `SQLDLQRepository`. `tests/unit/test_public_api.py` must keep passing **unmodified**; if it needs an edit, compatibility broke unintentionally — stop and report.
- **mypy strict:** all new code fully annotated. `uv run mypy src/eventsource/ --config-file=pyproject.toml` is listed per task.
- Pre-commit hooks run on commit (ruff, mypy, import-linter). A task is not done until its commit succeeds.

---

### Task 1: Create the pure port modules `ports/checkpoints.py` and `ports/dlq.py`

**Files:**
- Create: `src/eventsource/ports/checkpoints.py`, `src/eventsource/ports/dlq.py`
- Modify: `src/eventsource/ports/__init__.py` (re-export the new names)
- Create: `tests/unit/ports/test_checkpoint_ports.py`, `tests/unit/ports/test_dlq_ports.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces (exact, used by every later task):
  - `eventsource.ports.checkpoints`: `CheckpointData`, `LagMetrics` (frozen dataclasses), `ProjectionCheckpoints`, `SubscriptionPositions`, `CheckpointRepository` (all `@runtime_checkable` Protocols)
  - `eventsource.ports.dlq`: `DLQEntry` (mutable dataclass), `DLQStats`, `ProjectionFailureCount` (frozen), `DLQRepository` (`@runtime_checkable` Protocol, eight methods)

- [ ] **Step 1: Write `src/eventsource/ports/checkpoints.py`**

Copy `CheckpointData` (currently `src/eventsource/repositories/checkpoint.py:37-56`) and `LagMetrics` (`:59-78`) verbatim — field names, defaults, and docstrings unchanged. Then write the split protocols. Method docstrings come verbatim from the existing `CheckpointRepository` protocol (`:81-193`); only the class-level docstrings are new.

```python
"""Projection checkpoint and subscription position ports.

Pure boundary interfaces: stdlib, typing, uuid, datetime, dataclasses only.
No sqlalchemy, no observability, no implementation code.

The contract splits along its two real consumer groups (ISP, ADR 0019):
subscription runners persist an opaque global position; projections persist
a checkpoint plus lag metadata. Both land in one table in the SQL adapter,
which is why the composed `CheckpointRepository` exists.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID


@dataclass(frozen=True)
class CheckpointData:
    ...  # verbatim from repositories/checkpoint.py:37-56


@dataclass(frozen=True)
class LagMetrics:
    ...  # verbatim from repositories/checkpoint.py:59-78


@runtime_checkable
class ProjectionCheckpoints(Protocol):
    """Checkpoint persistence for projections: position, lag, reset."""

    async def get_checkpoint(self, projection_name: str) -> UUID | None: ...

    async def update_checkpoint(
        self,
        projection_name: str,
        event_id: UUID,
        event_type: str,
    ) -> None: ...

    async def reset_checkpoint(self, projection_name: str) -> None: ...

    async def get_lag_metrics(
        self,
        projection_name: str,
        event_types: list[str] | None = None,
    ) -> LagMetrics | None: ...

    async def get_all_checkpoints(self) -> list[CheckpointData]: ...


@runtime_checkable
class SubscriptionPositions(Protocol):
    """Global-position persistence for subscription runners."""

    async def get_position(self, subscription_id: str) -> int | None: ...

    async def save_position(
        self,
        subscription_id: str,
        position: int,
        event_id: UUID,
        event_type: str,
    ) -> None: ...


@runtime_checkable
class CheckpointRepository(ProjectionCheckpoints, SubscriptionPositions, Protocol):
    """Composed convenience protocol: both capabilities in one table."""


__all__ = [
    "CheckpointData",
    "CheckpointRepository",
    "LagMetrics",
    "ProjectionCheckpoints",
    "SubscriptionPositions",
]
```

- [ ] **Step 2: Write `src/eventsource/ports/dlq.py`**

Copy `DLQEntry` (`src/eventsource/repositories/dlq.py:45-78` — **mutable**, not frozen: `get_failed_event_by_id` sets `resolved_at`/`resolved_by` post-construction), `DLQStats` (`:81-96`, frozen), and `ProjectionFailureCount` (`:99-114`, frozen) verbatim. Copy the `DLQRepository` protocol's **eight real methods** with their docstrings (`:126-231`): `add_failed_event`, `get_failed_events`, `get_failed_event_by_id`, `mark_resolved`, `mark_retrying`, `get_failure_stats`, `get_projection_failure_counts`, `delete_resolved_events`. **Do not copy** `list_failed_events` (`:233-253`) or `get_failed_event` (`:255-267`) — they are pure aliases and are deleted by this slice.

```python
"""Dead letter queue port.

Pure boundary interface: stdlib, typing, uuid, datetime, dataclasses only.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID
```

End with:

```python
__all__ = [
    "DLQEntry",
    "DLQRepository",
    "DLQStats",
    "ProjectionFailureCount",
]
```

- [ ] **Step 3: Re-export from `ports/__init__.py`**

Add to `src/eventsource/ports/__init__.py`, keeping the file's existing grouping-comment style:

```python
from eventsource.ports.checkpoints import (
    CheckpointData,
    CheckpointRepository,
    LagMetrics,
    ProjectionCheckpoints,
    SubscriptionPositions,
)
from eventsource.ports.dlq import (
    DLQEntry,
    DLQRepository,
    DLQStats,
    ProjectionFailureCount,
)
```

and to `__all__` under a new `# Checkpoint / DLQ ports` comment block: `"CheckpointData"`, `"CheckpointRepository"`, `"LagMetrics"`, `"ProjectionCheckpoints"`, `"SubscriptionPositions"`, `"DLQEntry"`, `"DLQRepository"`, `"DLQStats"`, `"ProjectionFailureCount"`.

- [ ] **Step 4: Write the port tests**

`tests/unit/ports/test_checkpoint_ports.py` — value-object defaults plus structural-subtyping checks against local stubs (no adapter exists yet):

```python
"""Tests for the checkpoint and position ports."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from eventsource.ports.checkpoints import (
    CheckpointData,
    CheckpointRepository,
    LagMetrics,
    ProjectionCheckpoints,
    SubscriptionPositions,
)


class TestCheckpointData:
    def test_defaults(self) -> None:
        data = CheckpointData(projection_name="P")
        assert data.last_event_id is None
        assert data.last_event_type is None
        assert data.last_processed_at is None
        assert data.events_processed == 0
        assert data.global_position is None

    def test_is_frozen(self) -> None:
        import dataclasses

        import pytest

        data = CheckpointData(projection_name="P")
        with pytest.raises(dataclasses.FrozenInstanceError):
            data.projection_name = "Q"  # type: ignore[misc]

    def test_holds_values(self) -> None:
        eid = uuid4()
        now = datetime.now(UTC)
        data = CheckpointData(
            projection_name="P",
            last_event_id=eid,
            last_event_type="Created",
            last_processed_at=now,
            events_processed=3,
            global_position=17,
        )
        assert (data.last_event_id, data.events_processed, data.global_position) == (eid, 3, 17)


class TestLagMetrics:
    def test_defaults(self) -> None:
        m = LagMetrics(projection_name="P")
        assert m.last_event_id is None
        assert m.latest_event_id is None
        assert m.lag_seconds == 0.0
        assert m.events_processed == 0
        assert m.last_processed_at is None


class PositionsOnly:
    async def get_position(self, subscription_id: str) -> int | None:
        return None

    async def save_position(
        self, subscription_id: str, position: int, event_id: UUID, event_type: str
    ) -> None:
        return None


class CheckpointsOnly:
    async def get_checkpoint(self, projection_name: str) -> UUID | None:
        return None

    async def update_checkpoint(
        self, projection_name: str, event_id: UUID, event_type: str
    ) -> None:
        return None

    async def reset_checkpoint(self, projection_name: str) -> None:
        return None

    async def get_lag_metrics(
        self, projection_name: str, event_types: list[str] | None = None
    ) -> LagMetrics | None:
        return None

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        return []


class Both(CheckpointsOnly, PositionsOnly):
    pass


class TestProtocolSplit:
    def test_positions_only_satisfies_positions_port(self) -> None:
        assert isinstance(PositionsOnly(), SubscriptionPositions)

    def test_positions_only_does_not_satisfy_checkpoints_port(self) -> None:
        assert not isinstance(PositionsOnly(), ProjectionCheckpoints)

    def test_checkpoints_only_satisfies_checkpoints_port(self) -> None:
        assert isinstance(CheckpointsOnly(), ProjectionCheckpoints)

    def test_checkpoints_only_does_not_satisfy_composed_port(self) -> None:
        assert not isinstance(CheckpointsOnly(), CheckpointRepository)

    def test_both_satisfies_all_three(self) -> None:
        both = Both()
        assert isinstance(both, ProjectionCheckpoints)
        assert isinstance(both, SubscriptionPositions)
        assert isinstance(both, CheckpointRepository)
```

`tests/unit/ports/test_dlq_ports.py` — `DLQEntry` defaults and post-construction mutability (the reason it is not frozen), `DLQStats`/`ProjectionFailureCount` defaults and frozenness, a structural stub satisfying `DLQRepository`, and an explicit assertion that the aliases are gone:

```python
def test_dlq_repository_protocol_has_no_alias_methods() -> None:
    members = set(DLQRepository.__protocol_attrs__)
    assert "list_failed_events" not in members
    assert "get_failed_event" not in members
    assert "get_failed_events" in members
    assert "get_failed_event_by_id" in members
```

- [ ] **Step 5: Verify the ports are pure**

```bash
grep -n "sqlalchemy\|observability\|eventsource\." src/eventsource/ports/checkpoints.py src/eventsource/ports/dlq.py
```

Expected: no matches at all (the port modules import nothing from `eventsource` and nothing from sqlalchemy).

- [ ] **Step 6: Run targeted tests**

Run: `uv run pytest tests/unit/ports/ -q`
Expected: PASS.
Run: `uv run mypy src/eventsource/ports/ --config-file=pyproject.toml`
Expected: clean.
Run: `uv run ruff check src/eventsource/ports/ tests/unit/ports/`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat: add checkpoint and dlq ports with isp split"
```

---

### Task 2: Move the checkpoint and DLQ implementations into adapters

**Files:**
- Create: `src/eventsource/adapters/_sql/connection.py`
- Create: `src/eventsource/adapters/sql/__init__.py`, `src/eventsource/adapters/sql/checkpoints.py`, `src/eventsource/adapters/sql/dlq.py`
- Create: `src/eventsource/adapters/memory/checkpoints.py`, `src/eventsource/adapters/memory/dlq.py`
- Modify: `src/eventsource/adapters/memory/__init__.py`
- Create: `tests/unit/adapters/sql/__init__.py`, `tests/unit/adapters/memory/__init__.py` (only if the sibling directories use package markers — check `tests/unit/adapters/` first; today it is flat, so prefer flat `tests/unit/adapters/test_*.py` names and skip the subpackages)
- Move tests: `tests/unit/test_checkpoint_repository.py` → `tests/unit/adapters/test_memory_checkpoints.py`; `tests/unit/test_dlq_repository.py` → `tests/unit/adapters/test_memory_dlq.py`; `tests/unit/repositories/test_checkpoint_tracing.py` → `tests/unit/adapters/test_sql_checkpoint_tracing.py`; `tests/unit/test_checkpoint_position.py` → `tests/unit/adapters/test_checkpoint_position.py`

**Note:** `repositories/checkpoint.py` and `repositories/dlq.py` are **not deleted in this task** — they are deleted in Task 5, after every importer has moved. Until then the classes exist in two places; that is intentional and short-lived. Do not add re-export shims between them.

**Interfaces:**
- Consumes: `eventsource.ports.checkpoints.{CheckpointData, LagMetrics}`, `eventsource.ports.dlq.{DLQEntry, DLQStats, ProjectionFailureCount}` (Task 1).
- Produces:
  - `eventsource.adapters._sql.connection.sql_connection(conn: AsyncConnection | AsyncEngine, *, write: bool) -> AsyncIterator[AsyncConnection]` (asynccontextmanager)
  - `eventsource.adapters.sql.checkpoints.SQLCheckpointRepository(conn, tracer=None, enable_tracing=True)`
  - `eventsource.adapters.sql.dlq.SQLDLQRepository(conn, tracer=None, enable_tracing=True)`
  - `eventsource.adapters.memory.checkpoints.InMemoryCheckpointRepository(tracer=None, enable_tracing=True)`
  - `eventsource.adapters.memory.dlq.InMemoryDLQRepository(tracer=None, enable_tracing=True)`

- [ ] **Step 1: Write the shared connection helper**

`src/eventsource/adapters/_sql/connection.py` — new code, replacing the byte-identical `_connect` methods at `repositories/checkpoint.py:235-253` and `repositories/dlq.py:310-328`:

```python
"""Shared connection handling for SQL adapters.

Deliberately distinct from `eventsource.repositories._connection`: that
helper has a different signature (`transactional=`) and different call
sites, and the outbox, read-model, and migration repositories still use
it. The two merge when the outbox slice removes its last non-adapter
caller.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


@asynccontextmanager
async def sql_connection(
    conn: AsyncConnection | AsyncEngine, *, write: bool
) -> AsyncIterator[AsyncConnection]:
    """Yield a connection to execute on.

    A live `AsyncConnection` is yielded directly and NOT committed -- the
    caller owns the transaction. An `AsyncEngine` gets `begin()` for writes
    (committed on successful exit) and `connect()` for reads.
    """
    if isinstance(conn, AsyncEngine):
        if write:
            async with conn.begin() as connection:
                yield connection
        else:
            async with conn.connect() as connection:
                yield connection
    else:
        yield conn


__all__ = ["sql_connection"]
```

- [ ] **Step 2: Create `adapters/sql/checkpoints.py`**

Copy `SQLCheckpointRepository` from `src/eventsource/repositories/checkpoint.py:196-564` into the new module. Exactly four edits — everything else, including all SQL text, dialect branching, span names, span attributes, and the `# nosec B608` annotation at `:389`, is verbatim:

1. Import dialect helpers from the real module rather than the shim being deleted:
   ```python
   from eventsource.adapters._sql.dialect import (
       Dialect,
       dialect_of,
       ts_param,
       ts_result,
       uuid_param,
       uuid_result,
   )
   ```
2. Import the value objects from the port: `from eventsource.ports.checkpoints import CheckpointData, LagMetrics`.
3. Delete the `_connect` method (`:235-253`) and replace every `self._connect(write=...)` call site with `sql_connection(self._conn, write=...)`, importing `from eventsource.adapters._sql.connection import sql_connection`. There are eight call sites; grep the new file for `_connect(` afterwards and expect zero matches.
4. Update the class docstring's dialect reference from `eventsource.repositories._dialect` to `eventsource.adapters._sql.dialect`.

Keep `self.conn = conn` (the public attribute; tests and the harness read it) alongside `self._conn`. Keep the `tracer`/`enable_tracing` constructor parameters and `self._enable_tracing`.

Module header:

```python
"""SQLAlchemy checkpoint adapter, serving both PostgreSQL and SQLite."""
```

`__all__ = ["SQLCheckpointRepository"]`.

- [ ] **Step 3: Create `adapters/sql/dlq.py`**

Same treatment for `SQLDLQRepository` (`src/eventsource/repositories/dlq.py:270-668`), with the same four edits plus one deletion:

- Dialect imports from `eventsource.adapters._sql.dialect` (`Dialect`, `dialect_of`, `json_param`, `json_result`, `ts_param`, `ts_result`, `uuid_param`, `uuid_result`).
- Value objects from `eventsource.ports.dlq`.
- `_connect` deleted; six call sites become `sql_connection(self._conn, write=...)`.
- **Delete `list_failed_events` (`:657-664`) and `get_failed_event` (`:666-668`)** — the alias methods.
- Keep `_row_to_entry`, `self.conn`, both `# nosec B608` annotations, and `from eventsource.serialization import json_dumps` only if still referenced (it is not used by the SQL class — check and drop the import if unused).

`__all__ = ["SQLDLQRepository"]`.

- [ ] **Step 4: Create `adapters/sql/__init__.py`**

```python
"""Dialect-parameterized SQL adapters (PostgreSQL + SQLite).

Sits beside `adapters/_sql/`, which holds the private dialect and
connection helpers these modules build on.
"""

from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository
from eventsource.adapters.sql.dlq import SQLDLQRepository

__all__ = ["SQLCheckpointRepository", "SQLDLQRepository"]
```

(`DatabaseProjection` is added to this `__init__` in Task 4.)

- [ ] **Step 5: Create the memory adapters**

`src/eventsource/adapters/memory/checkpoints.py` — `InMemoryCheckpointRepository` verbatim from `repositories/checkpoint.py:567-788`, including `clear()` and the `asyncio.Lock` guard. Only the value-object import changes: `from eventsource.ports.checkpoints import CheckpointData, LagMetrics`. Keep the `eventsource.observability` imports and every span name.

`src/eventsource/adapters/memory/dlq.py` — `InMemoryDLQRepository` verbatim from `repositories/dlq.py:671-1023`, including `clear()`, `_make_key`, and the function-local `from datetime import timedelta` inside `delete_resolved_events` (move it to the module-level import block instead — that is the one tidy-up allowed here, and it is behavior-neutral). **Delete `list_failed_events` (`:1002-1009`) and `get_failed_event` (`:1011-1013`).** Value objects from `eventsource.ports.dlq`; `json_dumps` still from `eventsource.serialization`.

Update `src/eventsource/adapters/memory/__init__.py`:

```python
"""In-process memory adapters implementing the store, snapshot, checkpoint, and DLQ ports."""

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import MemoryEventStore

__all__ = [
    "InMemoryCheckpointRepository",
    "InMemoryDLQRepository",
    "InMemorySnapshotStore",
    "MemoryEventStore",
]
```

- [ ] **Step 6: Relocate the adapter tests**

```bash
git mv tests/unit/test_checkpoint_repository.py tests/unit/adapters/test_memory_checkpoints.py
git mv tests/unit/test_dlq_repository.py tests/unit/adapters/test_memory_dlq.py
git mv tests/unit/repositories/test_checkpoint_tracing.py tests/unit/adapters/test_sql_checkpoint_tracing.py
git mv tests/unit/test_checkpoint_position.py tests/unit/adapters/test_checkpoint_position.py
```

Re-point their imports to `eventsource.adapters.memory.{checkpoints,dlq}`, `eventsource.adapters.sql.{checkpoints,dlq}`, and `eventsource.ports.{checkpoints,dlq}` for value objects. **Delete any test that exercises `list_failed_events` or `get_failed_event`** as a deprecated-alias check; a test that merely *uses* an alias as scaffolding switches to the canonical spelling and keeps its assertions. Verify:

```bash
grep -rn "list_failed_events\|get_failed_event\b" tests/ src/ bench/ --include="*.py"
```

Expected: no matches (`get_failed_events` and `get_failed_event_by_id` are fine — the `\b` guards against those).

- [ ] **Step 7: Run targeted tests**

Run: `uv run pytest tests/unit/adapters/ tests/unit/ports/ -q`
Expected: PASS.
Run: `uv run mypy src/eventsource/adapters/ --config-file=pyproject.toml`
Expected: clean.
Run: `uv run ruff check src/eventsource/adapters/ tests/unit/adapters/`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "refactor: move checkpoint and dlq implementations into sql and memory adapters"
```

---

### Task 3: Build `application/projections/` and dissolve the two managers

**Files:**
- Create: `src/eventsource/application/projections/__init__.py`, `base.py`, `checkpoints.py`, `dlq.py`, `coordinator.py`, `retry.py`
- Move: `src/eventsource/projections/README.md` → `src/eventsource/application/projections/README.md` (rewritten for the new module set)
- Create: `tests/unit/application/projections/__init__.py`, `tests/unit/application/projections/test_checkpoints.py`, `tests/unit/application/projections/test_dlq.py`
- Move tests: `tests/unit/test_projection_base.py`, `tests/unit/test_projection_decorators.py`, `tests/unit/test_projection_coordinator.py`, `tests/unit/test_projection_protocols.py`, `tests/unit/projections/test_tenant_filter.py` → `tests/unit/application/projections/`
- Modify tests (None-default injections, see Step 6): the five moved files above plus `tests/unit/observability/test_projection_tracing.py`, `tests/unit/readmodels/test_projection.py`, `tests/unit/readmodels/test_handler_integration.py`, `tests/integration/projections/test_database_projection.py`, `tests/integration/readmodels/test_projection.py`

**Note:** `src/eventsource/projections/` still exists after this task; it is deleted in Task 5. `DatabaseProjection` stays in `projections/base.py` until Task 4.

**Interfaces:**
- Consumes: `eventsource.ports.checkpoints.{ProjectionCheckpoints, LagMetrics}`, `eventsource.ports.dlq.{DLQRepository, DLQEntry}` (Task 1).
- Produces:
  - `eventsource.application.projections.checkpoints`: `record_checkpoint`, `read_checkpoint`, `lag_metrics_dict`, `reset_checkpoint`
  - `eventsource.application.projections.dlq`: `send_to_dlq`, `read_failed_events`
  - `eventsource.application.projections.base`: `Projection`, `SyncProjection`, `EventHandlerBase`, `CheckpointTrackingProjection`, `DeclarativeProjection`, `TenantFilter`, `UnregisteredEventHandling`
  - `eventsource.application.projections.coordinator`: `ProjectionRegistry`, `ProjectionCoordinator`, `SubscriberRegistry`
  - `eventsource.application.projections.retry`: `RetryPolicy`, `ExponentialBackoffRetryPolicy`, `NoRetryPolicy`, `FilteredRetryPolicy`, `DEFAULT_RETRY_POLICY`
  - `CheckpointTrackingProjection(checkpoint_repo=None, dlq_repo=None, retry_policy=None, tracer=None, enable_tracing=False)` — signature unchanged, **semantics changed**: `None` disables the concern.
  - `DeclarativeProjection(checkpoint_repo=None, dlq_repo=None, enable_tracing=False, *, tenant_filter=None)` — unchanged signature, same semantic change.

- [ ] **Step 1: Move `coordinator.py` and `retry.py` verbatim**

```bash
git mv src/eventsource/projections/coordinator.py src/eventsource/application/projections/coordinator.py
git mv src/eventsource/projections/retry.py src/eventsource/application/projections/retry.py
```

(Create `src/eventsource/application/projections/` first if `git mv` needs it to exist.) `coordinator.py:27` imports `from eventsource.projections.base import EventHandlerBase, Projection` — change to `eventsource.application.projections.base`. `retry.py` imports `eventsource.subscriptions.retry`, which is stdlib-only; leave that import alone. Update the docstring example at `retry.py:12` to the new path.

- [ ] **Step 2: Write `application/projections/checkpoints.py`**

Four functions carrying the bodies of `ProjectionCheckpointManager.update` / `.get_checkpoint` / `.get_lag_metrics` / `.reset` (`src/eventsource/projections/checkpoint_manager.py:112-225`). Span names, log messages, log levels, and `extra=` payloads are verbatim.

```python
"""Checkpoint operations for projections.

These four functions replace ProjectionCheckpointManager, which was a
stateless wrapper around one repository plus a tracer and held no invariant
of its own (ADR 0024). The tracer is passed in rather than constructed:
the projection already owns one.

Span names still read `eventsource.checkpoint_manager.*`. That class no
longer exists; renaming the spans would break users' dashboards for no
functional gain, so the names are kept deliberately.
"""

import logging
from typing import Any

from eventsource.events.base import DomainEvent
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
)
from eventsource.ports.checkpoints import ProjectionCheckpoints

logger = logging.getLogger(__name__)


async def record_checkpoint(
    repo: ProjectionCheckpoints,
    projection_name: str,
    event: DomainEvent,
    tracer: Tracer,
) -> None:
    """Record the checkpoint after successfully processing an event."""
    with tracer.span(
        "eventsource.checkpoint_manager.update",
        {
            ATTR_PROJECTION_NAME: projection_name,
            ATTR_EVENT_TYPE: event.event_type,
        },
    ):
        await repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event.event_id,
            event_type=event.event_type,
        )

        logger.debug(
            "Updated checkpoint for %s: event_id=%s, type=%s",
            projection_name,
            event.event_id,
            event.event_type,
            extra={
                "projection": projection_name,
                "event_id": str(event.event_id),
                "event_type": event.event_type,
            },
        )


async def read_checkpoint(
    repo: ProjectionCheckpoints,
    projection_name: str,
    tracer: Tracer,
) -> str | None:
    """Return the last processed event ID as a string, or None."""
    with tracer.span(
        "eventsource.checkpoint_manager.get_checkpoint",
        {ATTR_PROJECTION_NAME: projection_name},
    ):
        event_id = await repo.get_checkpoint(projection_name)
        return str(event_id) if event_id else None


async def lag_metrics_dict(
    repo: ProjectionCheckpoints,
    projection_name: str,
    event_types: list[str] | None,
    tracer: Tracer,
) -> dict[str, Any] | None:
    """Return projection lag metrics as a plain dict, or None.

    The dict shape (six keys) is the projection's public surface;
    returning `LagMetrics` instead would be a public API change.
    """
    with tracer.span(
        "eventsource.checkpoint_manager.get_lag_metrics",
        {ATTR_PROJECTION_NAME: projection_name},
    ):
        metrics = await repo.get_lag_metrics(projection_name, event_types=event_types)

        if metrics is None:
            return None

        return {
            "projection_name": metrics.projection_name,
            "last_event_id": metrics.last_event_id,
            "latest_event_id": metrics.latest_event_id,
            "lag_seconds": metrics.lag_seconds,
            "events_processed": metrics.events_processed,
            "last_processed_at": metrics.last_processed_at,
        }


async def reset_checkpoint(
    repo: ProjectionCheckpoints,
    projection_name: str,
    tracer: Tracer,
) -> None:
    """Delete the checkpoint so the projection restarts from the beginning."""
    with tracer.span(
        "eventsource.checkpoint_manager.reset",
        {ATTR_PROJECTION_NAME: projection_name},
    ):
        await repo.reset_checkpoint(projection_name)

        logger.info(
            "Reset checkpoint for projection %s",
            projection_name,
            extra={"projection": projection_name},
        )


__all__ = [
    "lag_metrics_dict",
    "read_checkpoint",
    "record_checkpoint",
    "reset_checkpoint",
]
```

- [ ] **Step 3: Write `application/projections/dlq.py`**

Two functions carrying the bodies of `ProjectionDLQManager.send_to_dlq` / `.get_failed_events` (`src/eventsource/projections/dlq_manager.py:104-212`). The catch-log-`critical`-return-`False` contract and the swallow-to-empty-list contract are load-bearing, not accidents: a DLQ write failure must never mask the original processing error, which is about to be re-raised.

```python
"""Dead letter queue operations for projections.

These two functions replace ProjectionDLQManager (ADR 0024). Span names
still read `eventsource.dlq_manager.*` deliberately -- see the sibling
checkpoints module.
"""

import logging

from eventsource.events.base import DomainEvent
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)
from eventsource.ports.dlq import DLQEntry, DLQRepository

logger = logging.getLogger(__name__)


async def send_to_dlq(
    repo: DLQRepository,
    projection_name: str,
    event: DomainEvent,
    error: Exception,
    retry_count: int,
    tracer: Tracer,
) -> bool:
    """Send a permanently failed event to the DLQ.

    Returns True on success, False if the DLQ write itself failed. A DLQ
    failure is logged at critical and swallowed: the caller is about to
    re-raise the original processing error and must not have it masked.
    """
    with tracer.span(
        "eventsource.dlq_manager.send_to_dlq",
        {
            ATTR_PROJECTION_NAME: projection_name,
            ATTR_EVENT_TYPE: event.event_type,
            ATTR_EVENT_ID: str(event.event_id),
            ATTR_RETRY_COUNT: retry_count,
        },
    ):
        try:
            await repo.add_failed_event(
                event_id=event.event_id,
                projection_name=projection_name,
                event_type=event.event_type,
                event_data=event.model_dump(mode="json"),
                error=error,
                retry_count=retry_count,
            )

            logger.warning(
                "Event %s sent to DLQ for projection %s after %d attempts",
                event.event_id,
                projection_name,
                retry_count,
                extra={
                    "projection": projection_name,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "retry_count": retry_count,
                    "error": str(error),
                    "error_type": type(error).__name__,
                },
            )
            return True

        except Exception as dlq_error:
            logger.critical(
                "Failed to write event %s to DLQ for projection %s: %s",
                event.event_id,
                projection_name,
                dlq_error,
                exc_info=True,
                extra={
                    "projection": projection_name,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "original_error": str(error),
                    "dlq_error": str(dlq_error),
                },
            )
            return False


async def read_failed_events(
    repo: DLQRepository,
    projection_name: str,
    tracer: Tracer,
    limit: int = 100,
) -> list[DLQEntry]:
    """Read this projection's failed events. Errors collapse to an empty list."""
    with tracer.span(
        "eventsource.dlq_manager.get_failed_events",
        {ATTR_PROJECTION_NAME: projection_name},
    ):
        try:
            return await repo.get_failed_events(
                projection_name=projection_name,
                limit=limit,
            )
        except Exception as e:
            logger.error(
                "Failed to get DLQ events for projection %s: %s",
                projection_name,
                e,
                exc_info=True,
            )
            return []


__all__ = ["read_failed_events", "send_to_dlq"]
```

- [ ] **Step 4: Write `application/projections/base.py`**

Copy `src/eventsource/projections/base.py` lines 1-676 (module docstring through the end of `DeclarativeProjection`) into the new module. **Do not copy** `DatabaseProjection` (`:678-968`) or the trailing guarded sqlalchemy import block (`:971-983`) — those go to the adapter in Task 4. Update the module docstring's bullet list to drop the `DatabaseProjection` line and add a pointer: "DatabaseProjection now lives in `eventsource.adapters.sql.projection`."

Replace the import block (`:36-47`) with:

```python
from eventsource.application.projections.checkpoints import (
    lag_metrics_dict,
    read_checkpoint,
    record_checkpoint,
    reset_checkpoint,
)
from eventsource.application.projections.dlq import send_to_dlq
from eventsource.application.projections.retry import (
    ExponentialBackoffRetryPolicy,
    RetryPolicy,
)
from eventsource.ports.checkpoints import ProjectionCheckpoints
from eventsource.ports.dlq import DLQRepository
from eventsource.protocols import EventSubscriber
```

Rewrite `CheckpointTrackingProjection.__init__`'s manager block (`:245-273`) as plain attributes — **`None` now means the concern is disabled** (see Step 5):

```python
        self._checkpoint_repo: ProjectionCheckpoints | None = checkpoint_repo
        self._dlq_repo: DLQRepository | None = dlq_repo
```

and change the two constructor annotations to `checkpoint_repo: ProjectionCheckpoints | None = None` and `dlq_repo: DLQRepository | None = None`. The retry-policy block (`:257-270`) is unchanged. `DeclarativeProjection.__init__`'s annotations change the same way.

Rewrite the four call sites:

| Site | Was | Becomes |
|------|-----|---------|
| `_handle_with_retry` success path (`:316`) | `await self._checkpoint_manager.update(event)` | `if self._checkpoint_repo is not None:` → `await record_checkpoint(self._checkpoint_repo, self._projection_name, event, self._tracer)` |
| `_handle_with_retry` exhaustion path (`:362`) | `await self._dlq_manager.send_to_dlq(event, e, attempt + 1)` | `if self._dlq_repo is not None:` → `await send_to_dlq(self._dlq_repo, self._projection_name, event, e, attempt + 1, self._tracer)` |
| `get_checkpoint` (`:410`) | `return await self._checkpoint_manager.get_checkpoint()` | `if self._checkpoint_repo is None: return None` then `return await read_checkpoint(self._checkpoint_repo, self._projection_name, self._tracer)` |
| `get_lag_metrics` (`:419`) | `return await self._checkpoint_manager.get_lag_metrics(...)` | `if self._checkpoint_repo is None: return None` then `return await lag_metrics_dict(self._checkpoint_repo, self._projection_name, [et.__name__ for et in self.subscribed_to()], self._tracer)` |
| `reset` (`:436`) | `await self._checkpoint_manager.reset()` | `if self._checkpoint_repo is not None:` → `await reset_checkpoint(self._checkpoint_repo, self._projection_name, self._tracer)`; **`await self._truncate_read_models()` still runs unconditionally** |

The `span.set_attribute("checkpoint.updated", True)` line stays where it is — it reports that the success path completed, and it fires whether or not a checkpoint repo is configured. The `logger.critical("Event %s sent to DLQ after %d attempts", ...)` block at `:363-373` also stays unconditional: with no DLQ configured it is the one observable difference from today's behavior, and it is the point of the change.

Update the class docstrings for `CheckpointTrackingProjection` and `DeclarativeProjection`: the `checkpoint_repo`/`dlq_repo` argument docs change from "If None, uses InMemoryCheckpointRepository" to "If None, checkpoint tracking is disabled: no checkpoint is written, and `get_checkpoint()` / `get_lag_metrics()` return None." / "If None, DLQ capture is disabled: permanent failures are logged at critical and re-raised, as before." Update the `>>> from eventsource.projections.retry import ...` example at `:210` to the new path.

- [ ] **Step 5: Write `application/projections/__init__.py`**

Re-exports what `projections/__init__.py` does, minus `DatabaseProjection` and minus the `AsyncEventHandler` re-export from the deleted `protocols.py` shim (`AsyncEventHandler` keeps coming from `eventsource.protocols` and from `eventsource`):

```python
"""Projection use cases: base classes, checkpoint/DLQ operations, coordination.

DatabaseProjection is not here -- it takes an `async_sessionmaker` in its
constructor, which makes it an adapter (`eventsource.adapters.sql.projection`).
"""

from eventsource.application.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
    EventHandlerBase,
    Projection,
    SyncProjection,
    TenantFilter,
)
from eventsource.application.projections.checkpoints import (
    lag_metrics_dict,
    read_checkpoint,
    record_checkpoint,
    reset_checkpoint,
)
from eventsource.application.projections.coordinator import (
    ProjectionCoordinator,
    ProjectionRegistry,
    SubscriberRegistry,
)
from eventsource.application.projections.dlq import read_failed_events, send_to_dlq
from eventsource.handlers import (
    get_handled_event_type,
    handles,
    is_event_handler,
)
from eventsource.protocols import (
    EventHandler,
    EventSubscriber,
    SyncEventHandler,
)

__all__ = [
    "CheckpointTrackingProjection",
    "DeclarativeProjection",
    "EventHandler",
    "EventHandlerBase",
    "EventSubscriber",
    "Projection",
    "ProjectionCoordinator",
    "ProjectionRegistry",
    "SubscriberRegistry",
    "SyncEventHandler",
    "SyncProjection",
    "TenantFilter",
    "get_handled_event_type",
    "handles",
    "is_event_handler",
    "lag_metrics_dict",
    "read_checkpoint",
    "read_failed_events",
    "record_checkpoint",
    "reset_checkpoint",
    "send_to_dlq",
]
```

Then `git mv src/eventsource/projections/README.md src/eventsource/application/projections/README.md` and rewrite it for the new module set: the six functions replacing the two managers, the `None`-means-disabled semantics, and a pointer to `adapters/sql/projection.py` for `DatabaseProjection`.

- [ ] **Step 6: Write the dissolution unit tests**

`tests/unit/application/projections/test_checkpoints.py` and `test_dlq.py` — drive each function against `InMemoryCheckpointRepository` / `InMemoryDLQRepository` (Task 2) with a real `Tracer` from `eventsource.observability.create_tracer(__name__, False)`. Cover, at minimum:

- `record_checkpoint` writes; a second call increments `events_processed`.
- `read_checkpoint` returns `str(event_id)` — a **string**, not a UUID — and `None` when absent.
- `lag_metrics_dict` returns exactly the six documented keys when a checkpoint exists, and `None` when the repository returns `None`.
- `reset_checkpoint` makes `read_checkpoint` return `None` again.
- `send_to_dlq` returns `True` on success and stores the entry; against a repo whose `add_failed_event` raises, it returns `False`, logs at `CRITICAL` (assert via `caplog.at_level(logging.CRITICAL)`), and does **not** raise.
- `read_failed_events` returns entries for the named projection only; against a repo whose `get_failed_events` raises, it returns `[]` and logs at `ERROR`.

- [ ] **Step 7: Relocate the projection tests and inject repositories where None now disables**

```bash
git mv tests/unit/test_projection_base.py tests/unit/application/projections/
git mv tests/unit/test_projection_decorators.py tests/unit/application/projections/
git mv tests/unit/test_projection_coordinator.py tests/unit/application/projections/
git mv tests/unit/test_projection_protocols.py tests/unit/application/projections/
git mv tests/unit/projections/test_tenant_filter.py tests/unit/application/projections/
git rm tests/unit/projections/__init__.py 2>/dev/null || true
```

Re-point imports to `eventsource.application.projections.*`. Then apply the behavior-change fix to the eight suites named in the spec:

`tests/unit/application/projections/test_projection_base.py`, `test_projection_decorators.py`, `test_tenant_filter.py`, `tests/unit/observability/test_projection_tracing.py`, `tests/unit/readmodels/test_projection.py`, `tests/unit/readmodels/test_handler_integration.py`, `tests/integration/projections/test_database_projection.py`, `tests/integration/readmodels/test_projection.py`.

Decision rule, applied per construction site: a projection constructed with no repositories that later asserts on `get_checkpoint()`, `get_lag_metrics()`, or DLQ contents gains an explicit `checkpoint_repo=InMemoryCheckpointRepository()` and/or `dlq_repo=InMemoryDLQRepository()` (from `eventsource.adapters.memory`). A projection that never touches those is left alone. These are one-line constructor edits — **do not rewrite assertions**. Where a test's whole subject is "a bare projection still checkpoints," invert it into a test that a bare projection returns `None` from `get_checkpoint()`; that is the new contract and deserves direct coverage.

Note for `test_projection_decorators.py`: the parent-init ordering in `DeclarativeProjection.__init__` (registry built *before* `super().__init__()`, because `subscribed_to()` may be called during parent init) is unchanged and must stay unchanged.

- [ ] **Step 8: Run targeted tests**

Run: `uv run pytest tests/unit/application/ tests/unit/observability/test_projection_tracing.py tests/unit/readmodels/ -q`
Expected: PASS.
Run: `uv run mypy src/eventsource/application/ --config-file=pyproject.toml`
Expected: clean.
Run: `uv run ruff check src/eventsource/application/ tests/unit/application/`
Expected: clean.

- [ ] **Step 9: Commit**

```bash
git add -A && git commit -m "refactor: dissolve projection managers into application-ring functions"
```

---

### Task 4: Move `DatabaseProjection` to `adapters/sql/projection.py`

**Files:**
- Create: `src/eventsource/adapters/sql/projection.py`
- Modify: `src/eventsource/adapters/sql/__init__.py`, `src/eventsource/readmodels/projection.py`
- Modify tests: whichever of `tests/unit/readmodels/*`, `tests/integration/projections/test_database_projection.py`, `tests/integration/readmodels/test_projection.py` import `DatabaseProjection` by module path

**Interfaces:**
- Consumes: `eventsource.application.projections.base.{DeclarativeProjection, TenantFilter}`, `eventsource.application.projections.checkpoints.record_checkpoint`, `eventsource.application.projections.dlq.send_to_dlq` (Task 3); `eventsource.ports.{checkpoints,dlq}` (Task 1).
- Produces: `from eventsource.adapters.sql.projection import DatabaseProjection` with signature `DatabaseProjection(session_factory, checkpoint_repo=None, dlq_repo=None, enable_tracing=False, *, tenant_filter=None)`.

- [ ] **Step 1: Create the adapter module**

Copy `DatabaseProjection` (`src/eventsource/projections/base.py:678-968`) and the trailing guarded sqlalchemy import block (`:971-983`) into `src/eventsource/adapters/sql/projection.py`. Module docstring:

```python
"""DatabaseProjection: the SQL adapter for declarative projections.

Takes an `async_sessionmaker[AsyncSession]` and opens transactions -- a
framework dependency in a class signature, which makes this an adapter,
not a use case (ADR 0024). It subclasses DeclarativeProjection from the
application ring: an adapter depending inward is exactly the dependency
rule.
"""
```

Imports:

```python
import asyncio
import logging
from typing import TYPE_CHECKING

from eventsource.application.projections.base import DeclarativeProjection, TenantFilter
from eventsource.application.projections.checkpoints import record_checkpoint
from eventsource.application.projections.dlq import send_to_dlq
from eventsource.events.base import DomainEvent
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)
from eventsource.ports.checkpoints import ProjectionCheckpoints
from eventsource.ports.dlq import DLQRepository

if TYPE_CHECKING:
    from opentelemetry.trace import Span

logger = logging.getLogger(__name__)
```

Keep the guarded runtime sqlalchemy import block at the bottom of the file exactly as it is in `projections/base.py:971-983` (`AsyncConnection`, `AsyncSession`, `async_sessionmaker`) — `DatabaseProjection.__init__` annotates `self._current_connection: AsyncConnection | None`, so the runtime names must be present.

`__all__ = ["DatabaseProjection"]`.

- [ ] **Step 2: Update `_handle_with_retry` to call the functions**

The override exists because PostgreSQL aborts a transaction after any error, so each retry attempt needs a fresh one. That structure is unchanged. Only the two manager calls move, with the same `None` guards Task 3 established in the parent:

- `await self._checkpoint_manager.update(event)` (`base.py:794`) becomes
  ```python
                if self._checkpoint_repo is not None:
                    await record_checkpoint(
                        self._checkpoint_repo, self._projection_name, event, self._tracer
                    )
  ```
- `await self._dlq_manager.send_to_dlq(event, e, attempt + 1)` (`base.py:840`) becomes
  ```python
                    if self._dlq_repo is not None:
                        await send_to_dlq(
                            self._dlq_repo,
                            self._projection_name,
                            event,
                            e,
                            attempt + 1,
                            self._tracer,
                        )
  ```

Everything else in the method — the loop, both logger calls with their `extra=` payloads, `span.set_attribute` calls, the backoff sleep, the re-raise — is verbatim. Update the constructor's `checkpoint_repo` / `dlq_repo` annotations to `ProjectionCheckpoints | None` / `DLQRepository | None` and their docstrings to the disabled semantics.

- [ ] **Step 3: Export it and retarget `readmodels/projection.py`**

Add to `src/eventsource/adapters/sql/__init__.py`:

```python
from eventsource.adapters.sql.projection import DatabaseProjection
```

and to its `__all__`.

In `src/eventsource/readmodels/projection.py`, replace lines 20-23:

```python
from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.ports.checkpoints import ProjectionCheckpoints
from eventsource.ports.dlq import DLQRepository
```

Then update `ReadModelProjection.__init__`'s `checkpoint_repo` annotation from `CheckpointRepository` to `ProjectionCheckpoints` — check the constructor body first: it only forwards the argument to `super().__init__()`, so the narrowing is correct. If it calls a position method anywhere, keep `CheckpointRepository` and note the discrepancy in the commit message. Update the `>>> from eventsource.projections import handles` docstring example at `:78` to `from eventsource.handlers import handles`.

Note: `readmodels/base`, `query`, `schema`, `repository`, `in_memory`, and `exceptions` are Tier-0-clean and stay that way; `readmodels/projection.py` is not in the Tier-0 contract and now depends on `adapters.sql` — that is correct, it is a SQL-backed projection base class.

- [ ] **Step 4: Run targeted tests**

Run: `uv run pytest tests/unit/readmodels/ tests/unit/application/ tests/unit/adapters/ -q`
Expected: PASS.
Run: `uv run mypy src/eventsource/adapters/ src/eventsource/readmodels/ --config-file=pyproject.toml`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "refactor: reclassify database projection as a sql adapter"
```

---

### Task 5: Delete the old packages and rewire every remaining importer

**Files:**
- Delete: `src/eventsource/projections/` entirely (`__init__.py`, `base.py`, `checkpoint_manager.py`, `dlq_manager.py`, `protocols.py`; `coordinator.py`, `retry.py`, and `README.md` already moved)
- Delete: `src/eventsource/repositories/checkpoint.py`, `src/eventsource/repositories/dlq.py`, `src/eventsource/repositories/_dialect.py`
- Modify: `src/eventsource/repositories/__init__.py` (outbox + serialization only), `src/eventsource/__init__.py`, `src/eventsource/testing/harness.py`, `src/eventsource/handlers/decorators.py` (docstring example)
- Modify (TYPE_CHECKING import retargets): `src/eventsource/subscriptions/lifecycle.py:36`, `subscriptions/manager.py:76-77`, `subscriptions/transition.py:36`, `subscriptions/runners/live.py:43`, `subscriptions/runners/catchup.py:42`, `subscriptions/error_handling.py:29`, `migration/coordinator.py:109`, `migration/subscription_migrator.py:67`
- Modify tests: every remaining file matching the sweep in Step 5

**Interfaces:**
- Consumes: everything Tasks 1-4 produce.
- Produces: `eventsource.projections`, `eventsource.repositories.checkpoint`, `eventsource.repositories.dlq`, and `eventsource.repositories._dialect` cease to exist. The top-level `eventsource` name set is unchanged.

- [ ] **Step 1: Delete the modules**

```bash
git rm src/eventsource/projections/__init__.py \
       src/eventsource/projections/base.py \
       src/eventsource/projections/checkpoint_manager.py \
       src/eventsource/projections/dlq_manager.py \
       src/eventsource/projections/protocols.py
git rm src/eventsource/repositories/checkpoint.py \
       src/eventsource/repositories/dlq.py \
       src/eventsource/repositories/_dialect.py
```

- [ ] **Step 2: Trim `repositories/__init__.py`**

Delete the checkpoint block (`:31-40`) and the DLQ block (`:42-51`), and their `__all__` entries (`:70-84`). Keep the outbox imports and the serialization re-exports. Rewrite the module docstring: `repositories/` now holds the transactional outbox only; checkpoint and DLQ moved to `ports/` + `adapters/`. Delete the "Some methods have both styles available for backward compatibility / both get_failed_events() and list_failed_events() work" paragraph (`:26-28`) — that statement is now false.

- [ ] **Step 3: Retarget the TYPE_CHECKING annotations, narrowest port each**

Each narrowing is checked against actual method use at the call site before applying — a `SubscriptionPositions` hint on a consumer that also calls `update_checkpoint` is a type error, not a style choice. Verify with `grep -n "checkpoint_repo\.\|_checkpoints\." <file>` per file, then apply:

| File | New import |
|------|-----------|
| `subscriptions/lifecycle.py:36` | `from eventsource.ports.checkpoints import SubscriptionPositions` |
| `subscriptions/manager.py:76-77` | `from eventsource.ports.checkpoints import SubscriptionPositions` + `from eventsource.ports.dlq import DLQRepository` |
| `subscriptions/transition.py:36` | `SubscriptionPositions` |
| `subscriptions/runners/live.py:43` | `SubscriptionPositions` |
| `subscriptions/runners/catchup.py:42` | `SubscriptionPositions` |
| `subscriptions/error_handling.py:29` | `from eventsource.ports.dlq import DLQRepository` (calls `add_failed_event`; stays composed) |
| `migration/coordinator.py:109` | `SubscriptionPositions` |
| `migration/subscription_migrator.py:67` | `from eventsource.ports.checkpoints import CheckpointRepository` — needs positions **and** `get_all_checkpoints`, so it keeps the composed protocol |

Rename the annotation occurrences in each file's bodies to match the new type names.

- [ ] **Step 4: Rewire `__init__.py` and `testing/harness.py`**

In `src/eventsource/__init__.py`, replace the projections block (`:159-165`):

```python
from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.application.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
    Projection,
)
```

and replace the repositories block (`:180-198`) with the outbox-only import plus the ports/adapters imports:

```python
from eventsource.adapters.memory import (
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
)
from eventsource.adapters.sql import SQLCheckpointRepository, SQLDLQRepository
from eventsource.ports.checkpoints import CheckpointData, CheckpointRepository, LagMetrics
from eventsource.ports.dlq import (
    DLQEntry,
    DLQRepository,
    DLQStats,
    ProjectionFailureCount,
)
from eventsource.repositories import (
    InMemoryOutboxRepository,
    OutboxEntry,
    OutboxRepository,
    OutboxStats,
    PostgreSQLOutboxRepository,
)
```

Place each import in the alphabetically-correct position for ruff's isort rules rather than leaving them as one block where the old block was. **`__all__` is not edited** — the name set is unchanged.

In `src/eventsource/testing/harness.py`, replace lines 30-31 with `from eventsource.adapters.memory import InMemoryCheckpointRepository, InMemoryDLQRepository`. No semantic change: the harness already constructs both repositories explicitly (~lines 79-80 and ~191-192) and passes them in.

In `src/eventsource/handlers/decorators.py:77`, the docstring example `>>> from eventsource.projections import DeclarativeProjection` becomes `>>> from eventsource.application.projections import DeclarativeProjection`.

- [ ] **Step 5: Sweep every remaining importer, including tests and bench**

```bash
grep -rn "eventsource\.projections\|repositories\.checkpoint\|repositories\.dlq\|repositories\._dialect\|CheckpointRepositoryProtocol\|DLQRepositoryProtocol\|ProjectionCheckpointManager\|ProjectionDLQManager\|_checkpoint_manager\|_dlq_manager" src/ tests/ bench/ examples/ --include="*.py"
```

Expected after fixing: no matches. Re-point each hit using this map:

| Old | New |
|-----|-----|
| `eventsource.projections.base.DatabaseProjection` | `eventsource.adapters.sql.projection` |
| `eventsource.projections.base` (other names) | `eventsource.application.projections.base` |
| `eventsource.projections.coordinator` | `eventsource.application.projections.coordinator` |
| `eventsource.projections.retry` | `eventsource.application.projections.retry` |
| `eventsource.projections.protocols.AsyncEventHandler` | `eventsource.protocols` |
| `eventsource.repositories.checkpoint` protocols/VOs | `eventsource.ports.checkpoints` |
| `eventsource.repositories.checkpoint.SQLCheckpointRepository` | `eventsource.adapters.sql.checkpoints` |
| `eventsource.repositories.checkpoint.InMemoryCheckpointRepository` | `eventsource.adapters.memory.checkpoints` |
| `eventsource.repositories.dlq` protocols/VOs | `eventsource.ports.dlq` |
| `eventsource.repositories.dlq.SQLDLQRepository` | `eventsource.adapters.sql.dlq` |
| `eventsource.repositories.dlq.InMemoryDLQRepository` | `eventsource.adapters.memory.dlq` |
| `eventsource.repositories._dialect` | `eventsource.adapters._sql.dialect` |

The test files needing this sweep (from the current tree) are: `tests/conftest.py`, `tests/benchmarks/conftest.py`, `tests/benchmarks/test_projections.py`, `tests/benchmarks/test_repositories.py`, `tests/repositories/test_sqlite_repos.py`, `tests/integration/test_imports.py`, `tests/integration/subscriptions/conftest.py`, `tests/integration/subscriptions/test_resilience.py`, `tests/integration/projections/test_database_projection.py`, `tests/integration/readmodels/test_projection.py`, `tests/unit/migration/test_subscription_migrator.py`, `tests/unit/subscriptions/test_manager_pause_resume.py`, `tests/unit/test_additional_coverage.py`, `tests/unit/test_catchup_runner.py`, `tests/unit/test_edge_cases.py`, `tests/unit/test_fixtures.py`, `tests/unit/test_live_runner.py`, `tests/unit/test_protocols.py`, `tests/unit/test_subscription_manager.py`, `tests/unit/test_transition.py`, `tests/unit/testing/test_harness.py`, `tests/unit/observability/test_projection_tracing.py`. Treat that list as a starting point, not an authority — the grep is the authority.

- [ ] **Step 6: Confirm the public surface is intact**

Run: `uv run pytest tests/unit/test_public_api.py -q`
Expected: PASS, **with the file unmodified**. Confirm with `git status --short tests/unit/test_public_api.py` — expected: no output. If that file changed, the slice broke compatibility it did not intend to; stop and report rather than editing the test.

- [ ] **Step 7: Run targeted tests**

Run: `uv run pytest tests/unit/ -q --ignore=tests/unit/bench`
Expected: PASS. (This is the widest run in the plan; it is still not the full suite — no integration, no benchmarks. The orchestrator runs `make check`.)
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml`
Expected: clean.
Run: `uv run ruff check src/ tests/`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "refactor: delete projections package and checkpoint/dlq repositories"
```

---

### Task 6: Contracts — collapse Tier 0 to the application ring, delete KNOWN VIOLATIONS

**Files:**
- Modify: `pyproject.toml` (`[tool.mutmut]` and `[tool.importlinter]` sections)
- Modify tests: `tests/unit/repositories/test_dialect.py:9`, `tests/unit/repositories/test_dialect_properties.py:25`

**Interfaces:** none (config only).

- [ ] **Step 1: Update the Tier-0 forbidden contract**

In the `"Tier 0 modules must not import sqlalchemy"` contract's `source_modules`:

- Replace `"eventsource.application.aggregates.snapshotting"` with `"eventsource.application"` — import-linter's `forbidden` contract covers a package's descendants, and once `DatabaseProjection` leaves for `adapters/sql/`, nothing under `eventsource.application` imports sqlalchemy.
- Delete `"eventsource.application.aggregates.repository"` — now subsumed by the entry above.
- Delete `"eventsource.projections.protocols"` — the module is gone.
- Add `"eventsource.adapters.memory.checkpoints"` and `"eventsource.adapters.memory.dlq"` beside the existing `"eventsource.adapters.memory.snapshots"`.

Leave every other entry alone. The `"Application ring must not import adapters"` contract is unchanged in text and becomes load-bearing: it is the mechanical reason the `None`-means-disabled default exists.

- [ ] **Step 2: Delete the KNOWN VIOLATIONS block**

Remove the entire trailing comment block in `[tool.importlinter]` (the `# KNOWN VIOLATIONS (not enforced -- see task-2e-report.md for detail):` paragraph through `# it, and do not fix the violations here.`). Every violation it records is resolved: `projections/base.py` no longer imports `repositories.*` because neither exists; the two manager modules do not exist; and `checkpoint.py` / `dlq.py` no longer mix protocol with implementation because the protocols are in `ports/` and the implementations in `adapters/`. Deleting it because the violation is gone — not because the contract was relaxed — is the headline outcome of this slice.

- [ ] **Step 3: Update mutmut config**

In `[tool.mutmut] only_mutate`, delete `"src/eventsource/repositories/_dialect.py"` — the module is gone, and the dialect helpers are still mutated via `src/eventsource/adapters`. No additions needed: `only_mutate` already lists `src/eventsource/{domain,ports,adapters,application}`, covering every module this slice creates.

In `pytest_add_cli_args_test_selection`, `tests/unit/repositories/test_dialect.py` stays listed — the file survives, only its import target changes (Step 4).

- [ ] **Step 4: Retarget the two dialect tests**

`tests/unit/repositories/test_dialect.py:9` and `tests/unit/repositories/test_dialect_properties.py:25` import `eventsource.repositories._dialect`; change both to `eventsource.adapters._sql.dialect`. `test_dialect.py:103` already imports the new path directly, so afterwards the two files are internally consistent. Verify:

```bash
grep -rn "repositories._dialect" src/ tests/ bench/ --include="*.py"
```

Expected: no matches.

- [ ] **Step 5: Verify both tools**

Run: `uv run lint-imports`
Expected: all contracts pass. A failure here means an import crossed a ring boundary — fix the import, never the contract.
Run: `uv run pytest tests/unit/repositories/ -q`
Expected: PASS.
Run: `python -c "import tomllib; tomllib.load(open('pyproject.toml','rb'))"`
Expected: exit 0. Do NOT run a mutation campaign — that is an offline orchestrator activity.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "chore: enforce tier-0 purity across the application ring"
```

---

### Task 7: Port conformance suites and property tests

**Files:**
- Create: `src/eventsource/testing/conformance_ports/checkpoints.py`, `src/eventsource/testing/conformance_ports/dlq.py`
- Modify: `src/eventsource/testing/conformance_ports/__init__.py`
- Create: `tests/unit/adapters/test_memory_checkpoint_conformance.py`, `tests/unit/adapters/test_memory_dlq_conformance.py`
- Modify: `tests/unit/adapters/test_sqlite_conformance.py` (add the two suites), `tests/integration/adapters/test_postgresql_conformance.py` (add the two suites)
- Create: `tests/unit/adapters/test_memory_checkpoints_properties.py`, `tests/unit/adapters/test_memory_dlq_properties.py`

**Interfaces:**
- Consumes: `eventsource.ports.{checkpoints,dlq}` (Task 1); `eventsource.adapters.{memory,sql}.{checkpoints,dlq}` (Task 2).
- Produces: `ProjectionCheckpointsConformance`, `SubscriptionPositionsConformance`, `CheckpointRepositoryConformance`, `DLQRepositoryConformance` — ABC mixins with an abstract `store` fixture, exported from `eventsource.testing.conformance_ports`.

- [ ] **Step 1: Write the checkpoint conformance suite**

`src/eventsource/testing/conformance_ports/checkpoints.py`, following the established pattern in `conformance_ports/snapshots.py`: ABC, abstract `@pytest.fixture def store`, imports restricted to `eventsource.ports` + stdlib + pytest. The suite mirrors the protocol split so a future positions-only adapter can run the suite it actually satisfies.

```python
"""Conformance suites for the checkpoint and position ports.

Subclass and provide a `store` fixture yielding a fresh adapter instance.
`ProjectionCheckpointsConformance` and `SubscriptionPositionsConformance`
mirror the ISP split; `CheckpointRepositoryConformance` composes both for
adapters that back one table with both capabilities.
"""

from abc import ABC, abstractmethod
from uuid import uuid4

import pytest

from eventsource.ports.checkpoints import ProjectionCheckpoints, SubscriptionPositions


class ProjectionCheckpointsConformance(ABC):
    """Conformance suite for `ProjectionCheckpoints` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `ProjectionCheckpoints`."""
        raise NotImplementedError

    async def test_absent_checkpoint_reads_none(self, store: ProjectionCheckpoints) -> None:
        assert await store.get_checkpoint("Missing") is None

    async def test_update_then_read_round_trips(self, store: ProjectionCheckpoints) -> None:
        event_id = uuid4()
        await store.update_checkpoint("P", event_id, "Created")
        assert await store.get_checkpoint("P") == event_id

    async def test_events_processed_increments_across_updates(
        self, store: ProjectionCheckpoints
    ) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")
        await store.update_checkpoint("P", uuid4(), "Updated")
        await store.update_checkpoint("P", uuid4(), "Updated")

        (checkpoint,) = [c for c in await store.get_all_checkpoints() if c.projection_name == "P"]
        assert checkpoint.events_processed == 3

    async def test_last_write_wins(self, store: ProjectionCheckpoints) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")
        last = uuid4()
        await store.update_checkpoint("P", last, "Updated")
        assert await store.get_checkpoint("P") == last

    async def test_reset_makes_checkpoint_absent(self, store: ProjectionCheckpoints) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")
        await store.reset_checkpoint("P")
        assert await store.get_checkpoint("P") is None

    async def test_reset_of_absent_checkpoint_is_a_no_op(
        self, store: ProjectionCheckpoints
    ) -> None:
        await store.reset_checkpoint("NeverExisted")
        assert await store.get_checkpoint("NeverExisted") is None

    async def test_distinct_projections_do_not_interfere(
        self, store: ProjectionCheckpoints
    ) -> None:
        a, b = uuid4(), uuid4()
        await store.update_checkpoint("A", a, "Created")
        await store.update_checkpoint("B", b, "Created")
        await store.reset_checkpoint("A")

        assert await store.get_checkpoint("A") is None
        assert await store.get_checkpoint("B") == b

    async def test_get_all_checkpoints_returns_every_projection_sorted_by_name(
        self, store: ProjectionCheckpoints
    ) -> None:
        for name in ("Charlie", "Alpha", "Bravo"):
            await store.update_checkpoint(name, uuid4(), "Created")

        names = [c.projection_name for c in await store.get_all_checkpoints()]
        assert names == ["Alpha", "Bravo", "Charlie"]

    async def test_get_lag_metrics_is_none_without_a_checkpoint(
        self, store: ProjectionCheckpoints
    ) -> None:
        assert await store.get_lag_metrics("Missing") is None

    async def test_get_lag_metrics_is_not_none_with_a_checkpoint(
        self, store: ProjectionCheckpoints
    ) -> None:
        event_id = uuid4()
        await store.update_checkpoint("P", event_id, "Created")

        metrics = await store.get_lag_metrics("P")

        assert metrics is not None
        assert metrics.projection_name == "P"
        assert metrics.last_event_id == str(event_id)
        assert metrics.events_processed == 1


class SubscriptionPositionsConformance(ABC):
    """Conformance suite for `SubscriptionPositions` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `SubscriptionPositions`."""
        raise NotImplementedError

    async def test_absent_position_reads_none(self, store: SubscriptionPositions) -> None:
        assert await store.get_position("Missing") is None

    async def test_save_then_get_round_trips(self, store: SubscriptionPositions) -> None:
        await store.save_position("S", 42, uuid4(), "Created")
        assert await store.get_position("S") == 42

    async def test_last_saved_position_wins(self, store: SubscriptionPositions) -> None:
        await store.save_position("S", 1, uuid4(), "Created")
        await store.save_position("S", 99, uuid4(), "Updated")
        assert await store.get_position("S") == 99

    async def test_distinct_subscriptions_do_not_interfere(
        self, store: SubscriptionPositions
    ) -> None:
        await store.save_position("A", 1, uuid4(), "Created")
        await store.save_position("B", 2, uuid4(), "Created")
        assert await store.get_position("A") == 1
        assert await store.get_position("B") == 2


class CheckpointRepositoryConformance(
    ProjectionCheckpointsConformance, SubscriptionPositionsConformance
):
    """Both capabilities, one table."""

    async def test_position_is_none_before_any_position_is_saved(self, store: object) -> None:
        # A checkpoint exists but carries no global position: the two
        # capabilities share a row, and a checkpoint-only write must not
        # invent one.
        await store.update_checkpoint("P", uuid4(), "Created")  # type: ignore[attr-defined]
        assert await store.get_position("P") is None  # type: ignore[attr-defined]

    async def test_save_position_also_advances_the_checkpoint(self, store: object) -> None:
        event_id = uuid4()
        await store.save_position("P", 7, event_id, "Created")  # type: ignore[attr-defined]
        assert await store.get_checkpoint("P") == event_id  # type: ignore[attr-defined]
        assert await store.get_position("P") == 7  # type: ignore[attr-defined]
```

Lag-metric cases that need the `events` table are postgres/sqlite-only. The in-memory implementation documents that it cannot compute real lag, so its suite asserts the documented placeholder shape rather than skipping — add to the memory-only test class (Step 3), not to the shared suite:

```python
    async def test_memory_lag_metrics_use_the_documented_placeholder_shape(self, store):
        await store.update_checkpoint("P", uuid4(), "Created")
        metrics = await store.get_lag_metrics("P")
        assert metrics.latest_event_id is None
        assert metrics.lag_seconds == 0.0
```

- [ ] **Step 2: Write the DLQ conformance suite**

`src/eventsource/testing/conformance_ports/dlq.py` — `DLQRepositoryConformance(ABC)` with an abstract `store` fixture. Cases:

- **add-then-list**: one `add_failed_event` produces one entry from `get_failed_events()`, with `projection_name`, `event_type`, `error_message`, and `retry_count` round-tripped.
- **upsert key**: a second `add_failed_event` for the same `(event_id, projection_name)` pair updates rather than duplicates — count stays 1, `retry_count` and `last_failed_at` refresh, `first_failed_at` is preserved. A different `projection_name` for the same `event_id` creates a second entry.
- **status filtering**: `get_failed_events(status="retrying")` returns only retrying entries.
- **projection filtering**: `get_failed_events(projection_name="A")` excludes B's entries.
- **limit**: with three entries, `get_failed_events(limit=2)` returns 2.
- **transitions**: `mark_retrying(id)` moves an entry out of the `"failed"` filter and into `"retrying"`; `mark_resolved(id, "alice")` sets status `"resolved"` and populates `resolved_by`, observable via `get_failed_event_by_id`.
- **aggregates**: `get_failure_stats()` counts failed and retrying separately and reports `affected_projections` as the distinct projection count; `get_projection_failure_counts()` returns one row per affected projection, ordered by count descending.
- **cleanup**: `delete_resolved_events(older_than_days=0)` deletes only resolved entries past the cutoff, returns the count, and leaves failed entries intact.

For time-sensitive cases (`delete_resolved_events`, `first_failed_at` preservation), do not sleep — assert on ordering and presence rather than exact timestamps, and use `older_than_days=0` so "now minus zero days" puts already-resolved entries past the cutoff on both backends. If the memory adapter's day-truncated cutoff makes a case backend-dependent, keep that case out of the shared suite and put it in the memory-specific module instead; record which cases you moved and why in the commit message.

- [ ] **Step 3: Export and wire the suites to backends**

Add both modules' classes to `src/eventsource/testing/conformance_ports/__init__.py` imports and `__all__`.

`tests/unit/adapters/test_memory_checkpoint_conformance.py`:

```python
"""Conformance tests for InMemoryCheckpointRepository against the port suites."""

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

from eventsource.adapters.memory import InMemoryCheckpointRepository
from eventsource.testing.conformance_ports import CheckpointRepositoryConformance


class TestMemoryCheckpointRepository(CheckpointRepositoryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryCheckpointRepository]:
        yield InMemoryCheckpointRepository()

    async def test_memory_lag_metrics_use_the_documented_placeholder_shape(
        self, store: InMemoryCheckpointRepository
    ) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")
        metrics = await store.get_lag_metrics("P")
        assert metrics is not None
        assert metrics.latest_event_id is None
        assert metrics.lag_seconds == 0.0

    async def test_clear_empties_the_repository(
        self, store: InMemoryCheckpointRepository
    ) -> None:
        await store.update_checkpoint("P", uuid4(), "Created")
        await store.clear()
        assert await store.get_all_checkpoints() == []
```

`tests/unit/adapters/test_memory_dlq_conformance.py` follows the same shape with `DLQRepositoryConformance` and an added `clear()` case.

For sqlite, add two classes to `tests/unit/adapters/test_sqlite_conformance.py` mirroring how that file already builds a store (read its existing fixtures first — it manages temp files and schema via `eventsource.migrations.get_schema`). The checkpoint suite needs the `projection_checkpoints` table and the DLQ suite needs `dead_letter_queue`; the lag-metric cases additionally need `events`. The module is already `SQLITE_AVAILABLE`-guarded via `skip_if_no_aiosqlite` and `pytestmark`.

For postgres, add the same two classes to `tests/integration/adapters/test_postgresql_conformance.py`, inheriting that module's existing `@pytest.mark.postgres` marker and connection fixture.

- [ ] **Step 4: Write the property tests**

House style, confirmed against `tests/unit/application/aggregates/test_snapshotting_properties.py`: bare `async def` under `@given`, no `@pytest.mark.asyncio` (`asyncio_mode=auto`), file named `test_*_properties.py`. Read that file before writing to match it exactly.

`tests/unit/adapters/test_memory_checkpoints_properties.py`:

```python
"""Property-based tests for InMemoryCheckpointRepository."""

from uuid import UUID, uuid4

from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.memory import InMemoryCheckpointRepository

uuids = st.builds(uuid4)


@given(event_ids=st.lists(uuids, min_size=1, max_size=20))
async def test_get_checkpoint_is_the_last_event_written(event_ids: list[UUID]) -> None:
    repo = InMemoryCheckpointRepository()
    for event_id in event_ids:
        await repo.update_checkpoint("P", event_id, "Created")

    assert await repo.get_checkpoint("P") == event_ids[-1]


@given(event_ids=st.lists(uuids, min_size=1, max_size=20))
async def test_events_processed_equals_the_update_count(event_ids: list[UUID]) -> None:
    repo = InMemoryCheckpointRepository()
    for event_id in event_ids:
        await repo.update_checkpoint("P", event_id, "Created")

    (checkpoint,) = await repo.get_all_checkpoints()
    assert checkpoint.events_processed == len(event_ids)


@given(positions=st.lists(st.integers(min_value=0, max_value=10**9), min_size=1, max_size=20))
async def test_get_position_is_the_last_position_written(positions: list[int]) -> None:
    repo = InMemoryCheckpointRepository()
    for position in positions:
        await repo.save_position("S", position, uuid4(), "Created")

    assert await repo.get_position("S") == positions[-1]


@given(event_ids=st.lists(uuids, max_size=20))
async def test_reset_returns_the_projection_to_the_empty_state(event_ids: list[UUID]) -> None:
    repo = InMemoryCheckpointRepository()
    for event_id in event_ids:
        await repo.update_checkpoint("P", event_id, "Created")

    await repo.reset_checkpoint("P")

    assert await repo.get_checkpoint("P") is None
    assert await repo.get_lag_metrics("P") is None
    assert await repo.get_all_checkpoints() == []


@given(
    a_events=st.lists(uuids, min_size=1, max_size=10),
    b_events=st.lists(uuids, min_size=1, max_size=10),
)
async def test_distinct_projection_names_never_interfere(
    a_events: list[UUID], b_events: list[UUID]
) -> None:
    repo = InMemoryCheckpointRepository()
    for event_id in a_events:
        await repo.update_checkpoint("A", event_id, "Created")
    for event_id in b_events:
        await repo.update_checkpoint("B", event_id, "Created")

    assert await repo.get_checkpoint("A") == a_events[-1]
    assert await repo.get_checkpoint("B") == b_events[-1]
```

`tests/unit/adapters/test_memory_dlq_properties.py`:

```python
"""Property-based tests for InMemoryDLQRepository."""

from uuid import UUID, uuid4

from hypothesis import given
from hypothesis import strategies as st

from eventsource.adapters.memory import InMemoryDLQRepository

adds = st.lists(
    st.tuples(st.builds(uuid4), st.sampled_from(["A", "B", "C"])),
    min_size=1,
    max_size=15,
)


@given(pairs=adds)
async def test_entry_count_equals_distinct_event_projection_pairs(
    pairs: list[tuple[UUID, str]],
) -> None:
    repo = InMemoryDLQRepository()
    for event_id, projection in pairs:
        await repo.add_failed_event(
            event_id=event_id,
            projection_name=projection,
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )

    entries = await repo.get_failed_events(limit=1000)
    assert len(entries) == len(set(pairs))


@given(pairs=adds, limit=st.integers(min_value=1, max_value=20))
async def test_limit_caps_results_and_order_is_non_increasing_by_first_failed_at(
    pairs: list[tuple[UUID, str]], limit: int
) -> None:
    repo = InMemoryDLQRepository()
    for event_id, projection in pairs:
        await repo.add_failed_event(
            event_id=event_id,
            projection_name=projection,
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )

    entries = await repo.get_failed_events(limit=limit)

    assert len(entries) == min(limit, len(set(pairs)))
    timestamps = [e.first_failed_at for e in entries]
    assert timestamps == sorted(timestamps, reverse=True)


@given(pairs=adds)
async def test_clear_empties_entries_and_resets_the_id_counter(
    pairs: list[tuple[UUID, str]],
) -> None:
    repo = InMemoryDLQRepository()
    for event_id, projection in pairs:
        await repo.add_failed_event(
            event_id=event_id,
            projection_name=projection,
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
        )

    await repo.clear()

    assert await repo.get_failed_events(limit=1000) == []
    await repo.add_failed_event(
        event_id=uuid4(),
        projection_name="A",
        event_type="Created",
        event_data={},
        error=RuntimeError("boom"),
    )
    (entry,) = await repo.get_failed_events(limit=1000)
    assert entry.id == 1
```

- [ ] **Step 5: Run targeted tests**

Run: `uv run pytest tests/unit/adapters/ -q`
Expected: PASS (sqlite classes skip if aiosqlite is unavailable).
Run: `uv run mypy src/eventsource/testing/ --config-file=pyproject.toml`
Expected: clean.
Run: `uv run ruff check src/eventsource/testing/ tests/unit/adapters/`
Expected: clean.

Do **not** run the postgres integration tests — they need Docker and belong to the orchestrator's pass.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "test: conformance suites and property coverage for checkpoint and dlq ports"
```

---

### Task 8: ADR 0024, ADR-0015 amendment, and the documentation sweep

**Files:**
- Create: `docs/adrs/0024-projection-persistence-ports.md`
- Modify: `docs/adrs/0015-optional-dependency-extras.md` (Status + Consequences only), `docs/adrs/index.md`
- Modify: `docs/architecture.md`, `docs/api/projections.md`, `docs/api/repositories.md`, `docs/core-surface.md`, `docs/guides/multi-tenant.md`, `docs/development/code-structure.md`
- Modify: `CLAUDE.md` (Project Structure block), `.claude/rules/architecture.md` (transition lists), `CHANGELOG.md`, `BACKLOG.md`

**Interfaces:** none (docs only).

- [ ] **Step 1: Write ADR 0024**

`docs/adrs/0024-projection-persistence-ports.md`, following the house structure visible in `0021-snapshot-policy-scheduler-composition.md` (Title, Status, Context, Decision, Rationale, Consequences, Alternatives Considered, References). Cover exactly these decisions:

- **Status:** Accepted. Amends ADR 0015. Sibling of ADR 0021 (same manager-dissolution move, applied to projections). Implemented in `ports/{checkpoints,dlq}.py`, `application/projections/`, `adapters/sql/`, `adapters/memory/`.
- **Context:** the KNOWN VIOLATIONS block and what it recorded; `CheckpointRepository` as a seven-method god-interface serving two disjoint consumer groups; the two managers as stateless one-repository-plus-tracer wrappers holding no invariant; unreleased library, so no shims.
- **Decision 1 — ISP split.** `ProjectionCheckpoints` + `SubscriptionPositions` + composed `CheckpointRepository`. Not speculative: subscription runners and `migration/coordinator.py` use position methods only; `application/projections/*` uses checkpoint methods only; `migration/subscription_migrator.py` needs both and keeps the composed annotation. `get_all_checkpoints` sits on `ProjectionCheckpoints` because it is a checkpoint-table query and a one-method protocol for one consumer buys nothing. The composed protocol exists because both capabilities land in one table.
- **Decision 2 — manager dissolution.** Six module-level async functions. Record explicitly that span names (`eventsource.checkpoint_manager.*`, `eventsource.dlq_manager.*`) were kept even though the classes are gone: renaming them breaks users' dashboards for no functional gain, and a rename is its own change with its own release note.
- **Decision 3 — `DatabaseProjection` is an adapter,** because `async_sessionmaker` appears in its constructor signature. It continues to subclass `DeclarativeProjection` from the application ring; an adapter depending inward is the dependency rule working.
- **Decision 4 — `None` means disabled.** Two independent reasons: mechanically, the old default lived in the application ring and named a concrete adapter, which the "Application ring must not import adapters" contract forbids; by design, the in-memory default is a production footgun — a projection with no checkpoint repository *looks* durable (`get_checkpoint()` returns a value, `get_lag_metrics()` returns numbers) and silently reprocesses from the beginning on restart, with the lag metric that would have revealed it computed from the same amnesiac store. Matches the precedent `AggregateRepository(snapshot_store=None)` set in ADR 0021.
- **Consequences:** positive — the KNOWN VIOLATIONS block is deleted because the violation is gone; `eventsource.application` is Tier-0-clean as a whole ring; consumers annotate the narrowest port they use. Negative/observable — the eight test suites needing explicit repository injection; one extra `critical` log line when an event fails permanently with no DLQ configured (the exception is re-raised either way, so no caller's control flow changes); the checkpoint/DLQ functions now trace under the projection's tracer rather than per-manager tracers, so the OpenTelemetry instrumentation-scope name for those spans changes from `eventsource.projections.{checkpoint,dlq}_manager` to `eventsource.application.projections.base` while the span names themselves are unchanged.
- **Alternatives considered:** keep the god-interface (rejected: ISP, and the split is already latent in the call sites); keep the managers as thin classes (rejected: no invariant, no state, a Manager by name and construction); rename the spans while dissolving (rejected: user-visible, unrelated to this change); make `None` construct a null-object repository (rejected: reintroduces the "looks durable, isn't" failure mode with extra machinery).
- **References:** the spec, ADR 0019, ADR 0021, ADR 0015, the new modules, the conformance suites.

- [ ] **Step 2: Amend ADR-0015**

`docs/adrs/0015-optional-dependency-extras.md` — ADR bodies are immutable records; touch only two sections. Status gains `Amended by [ADR 0024](0024-projection-persistence-ports.md).` Consequences gains one line: the checkpoint and DLQ repositories now go through `adapters/_sql/connection.py`, while the outbox, read-model, and migration repositories keep `repositories/_connection.py`; the rationale at line 53 named `repositories/_connection.py` as the single shared layer, and the file name no longer holds — but the shared-abstraction argument does, so the conclusion (sqlalchemy stays a core dependency) is unchanged. Do **not** rewrite the Decision.

- [ ] **Step 3: Update the ADR index**

`docs/adrs/index.md`: add the 0024 entry following the file's existing format. Amend the **ADR-0004: Projection Error Handling** entry (the "Not yet written" paragraph at ~line 95): the persistence half — checkpoints, DLQ, their ports and implementations, and the disabled-by-default semantics — is now recorded in ADR 0024; retry, flow control, health, and shutdown remain unwritten. Also fix that paragraph's stale pointer to "`src/eventsource/repositories/` (checkpoint and DLQ repositories, with postgres, sqlite, and memory backends)" — those live in `ports/` and `adapters/` now.

- [ ] **Step 4: Documentation sweep**

```bash
grep -rln "checkpoint_manager\|dlq_manager\|ProjectionCheckpointManager\|ProjectionDLQManager\|eventsource\.projections\|repositories\.checkpoint\|repositories\.dlq\|repositories\._dialect\|list_failed_events" docs/ CLAUDE.md .claude/rules/ README.md --include="*.md"
```

Fix every hit, plus these specific passages:

- `docs/architecture.md`: the manager narrative at ~343, ~901-904, ~1296-1308, ~1441-1495, ~1656-1659 becomes a function-collaborator narrative. **The ordering point at ~1480 — process the event, *then* advance the checkpoint — is the load-bearing content and must survive the rewrite intact.**
- `docs/api/projections.md`: module table (lines 14-15), pipeline description (53-57), example imports (136-137), and the collaborator tables (396-398, 428-429). The last of these currently documents the in-memory defaults and must document the disabled semantics instead. The parent-init warning at ~511 still applies — restate it against attributes rather than managers.
- `docs/api/repositories.md`: the checkpoint and DLQ sections move to ports + adapters; the outbox section stays.
- `docs/core-surface.md`: rows 199-200 and the Tier-0 blocker narrative. The blocker is resolved — record what replaced it (ports in `ports/`, implementations in `adapters/`, `eventsource.application` enforced whole) rather than deleting the history.
- `docs/guides/multi-tenant.md` ~621: the `_checkpoint_manager.update` reference becomes `record_checkpoint(...)`.
- `docs/development/code-structure.md` and the `CLAUDE.md` Project Structure block: add `ports/` (checkpoints, dlq), `adapters/sql/`, `application/projections/`; remove `projections/`; describe `repositories/` as outbox-only.
- `.claude/rules/architecture.md`: the ring-2 transition list no longer names `projections/`; the ring-3 list should note that checkpoint and DLQ adapters live under `adapters/sql/` and `adapters/memory/`, and that `repositories/` is now outbox-only.

- [ ] **Step 5: CHANGELOG and BACKLOG**

`CHANGELOG.md` Unreleased — three groups:
- **Changed:** the module moves (`eventsource.projections` → `eventsource.application.projections`; checkpoint/DLQ protocols → `eventsource.ports.*`; implementations → `eventsource.adapters.{sql,memory}.*`; `DatabaseProjection` → `eventsource.adapters.sql.projection`), noting that top-level `eventsource` imports are unaffected.
- **Removed:** `CheckpointRepositoryProtocol`, `DLQRepositoryProtocol`, `DLQRepository.list_failed_events`, `DLQRepository.get_failed_event`, `ProjectionCheckpointManager`, `ProjectionDLQManager`, `eventsource.repositories._dialect`.
- **Behavior change (call it out explicitly):** `checkpoint_repo=None` / `dlq_repo=None` now disable the concern instead of constructing a per-instance in-memory repository. Include the migration line: pass `InMemoryCheckpointRepository()` / `InMemoryDLQRepository()` from `eventsource` explicitly to keep the old behavior.

`BACKLOG.md`:
- Add **"Migrate outbox repository to ports/adapters (P2)"** — `repositories/outbox.py` mixes `OutboxRepository` Protocol with its three implementations, the same defect this slice fixed for checkpoint and DLQ; moving it is what finally lets `repositories/` disappear and the two connection helpers (`adapters/_sql/connection.py` and `repositories/_connection.py`) merge.
- Update **"Add CI boundary check for core surface purity (P2)"** — import-linter now covers the whole `eventsource.application` ring plus the memory adapters, so the remaining question is narrower: whether a runtime `sys.modules` assertion adds anything over the static contract.

- [ ] **Step 6: Verify**

Run: `uv run pytest tests/unit/test_public_api.py -q`
Expected: PASS.
Run the sweep grep from Step 4 again.
Expected: no matches outside `docs/superpowers/specs/` and `docs/superpowers/plans/` (historical documents — do not edit them) and ADR bodies other than 0015's Status/Consequences.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "docs: adr 0024 projection persistence ports and ring-layout doc sweep"
```

---

## Self-Review Notes

**Spec coverage sweep.** Ports (T1); adapters incl. `sql_connection`, alias-method deletion, memory relocation (T2); application ring, manager dissolution, `None`-means-disabled with all eight named test suites (T3); `DatabaseProjection` reclassification and readmodels retarget (T4); deletions, `repositories/__init__` trim, top-level rewire, all thirteen migration-table rows (T5); pyproject Tier-0 collapse, memory-adapter entries, KNOWN VIOLATIONS deletion, mutmut `_dialect` removal, both dialect test retargets, `lint-imports` (T6); conformance suites and hypothesis property tests (T7); ADR 0024, ADR-0015 amendment, index, six doc files, CLAUDE.md, CHANGELOG, BACKLOG (T8). Out-of-scope items (outbox, `repositories/_connection.py`, lazy `__init__`, `SnapshotStore` redesign, subscriptions retry/flow-control/health/shutdown, span renames, `lag_metrics_dict` return shape) are correctly absent.

**Signature consistency across tasks.** `record_checkpoint(repo, projection_name, event, tracer)`, `read_checkpoint(repo, projection_name, tracer)`, `lag_metrics_dict(repo, projection_name, event_types, tracer)`, `reset_checkpoint(repo, projection_name, tracer)`, `send_to_dlq(repo, projection_name, event, error, retry_count, tracer)`, `read_failed_events(repo, projection_name, tracer, limit=100)` — identical in T3 (definitions), T3 Step 4 (base call sites), and T4 Step 2 (adapter call sites). `sql_connection(conn, *, write)` identical in T2 Steps 1-3. Port names identical in T1, T3, T4, T5, T7.

**Placeholder scan.** No `TODO`, no `...` standing in for code that must be written. The three `...  # verbatim from <path>:<lines>` markers in T1 Step 1 are deliberate move instructions with exact source ranges, per the "do not paste hundreds of moved lines" constraint.

**Judgment calls delegated with decision rules.** Test-directory layout for the relocated adapter tests (T2 Step 6 — inspect `tests/unit/adapters/`, which is flat today, and mirror it). Which DLQ conformance cases are backend-portable versus memory-specific (T7 Step 2 — a stated rule plus a requirement to record what moved and why). Whether `ReadModelProjection` can narrow to `ProjectionCheckpoints` (T4 Step 3 — check the constructor body first; report if it cannot). Which alias-using tests are deleted versus rewritten (T2 Step 6 — purpose test versus scaffolding).

**Spec observations reported, not silently resolved.**

1. `read_failed_events` has no caller anywhere in `src/` — `ProjectionDLQManager.get_failed_events` had none either, and `CheckpointTrackingProjection` never exposed it. The spec requires the function, so the plan produces it and gives it direct unit coverage (T3 Step 6), but it is a public module function with zero internal consumers.
2. The dissolution changes the OpenTelemetry **instrumentation-scope name** for the checkpoint and DLQ spans (from the two manager modules' `__name__` to the projection's), because the functions now take the projection's tracer. The spec mandates that tracer-passing and separately mandates that span *names* be preserved; both hold, but the scope change is a real observable difference and is recorded in ADR 0024's Consequences (T8 Step 1).
3. The spec's migration table says `readmodels/projection.py:22-23` retargets to `ports.checkpoints` / `ports.dlq` without naming which checkpoint protocol. The plan narrows to `ProjectionCheckpoints` after verifying the constructor only forwards the argument, with an explicit fallback if that check fails (T4 Step 3).
