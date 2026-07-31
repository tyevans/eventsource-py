# Structure Slice A — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the last three ring-placement entries left over from the clean-architecture campaign. `locks/` becomes `ports/locks.py` + `adapters/{postgresql,memory}/locks.py` behind a `DistributedLock` Protocol; the nine-module `readmodels/` package splits into `ports/readmodels/` (four pure modules) and five adapter modules under `adapters/{memory,postgresql,sqlite,sql}/`; `engine.py` moves to `adapters/_sql/engine.py`. Two new conformance suites (`DistributedLockConformance`, `ReadModelRepositoryConformance`) replace triplicated per-backend assertions. After this slice, `src/eventsource/` has no top-level non-ring sqlalchemy module, `src/eventsource/locks/` and `src/eventsource/readmodels/` each contain exactly one file (a deprecation shim), and `eventsource.__all__` is byte-identical to what it is today.

**Architecture:** Three independent ring migrations following the pattern set by ADRs 0019/0021/0024/0025/0026 — small composed Protocols in `ports/`, backend-colocated implementations in `adapters/`, conformance suites in `testing/conformance_ports/` that import only ports and never an adapter. Two packages whose *import path* is documented public API (`eventsource.locks`, `eventsource.readmodels`) get module-level `__getattr__` + `__dir__` deprecation shims scheduled for removal in 0.8.0, matching the `bus/` decomposition precedent. `engine.py` has no such obligation — its public name is `eventsource.create_async_engine` — so it is deleted outright. One ADR (0029) records all of it, amending ADR 0023.

**Tech Stack:** Python 3.13, pydantic v2, sqlalchemy 2 (adapters only), asyncpg, aiosqlite, pytest + pytest-asyncio, mypy strict, ruff, import-linter.

**Spec:** `docs/superpowers/specs/2026-07-31-structure-slice-a-design.md` (in full). Read the section named in each task before starting it. The spec's move tables (§2.5) and Protocol shapes (§1.3) are verbatim requirements, not sketches.

**Baseline:** authored against the tree at commit `8e9ccd4`. All line numbers below were read from that tree, not inferred. If a line number does not match what you find, re-read the file and proceed on the code, not the number — but if the *code* differs, stop and report.

## Global Constraints

- **Commit messages** follow `.claude/rules/commits.md` — `<type>: <lowercase description>`, no trailing period — and every commit ends with the trailer:

  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

- **Path-scoped `git add` only.** Other agents work concurrently in this worktree. Never `git add -A`, never `git add .`; stage exactly the files the task names, and prefer `git commit --only <paths>`. Use `git mv` for the file moves so rename detection works (the A4 reviewer check depends on it). On `index.lock` contention, wait 5 seconds and retry.
- **Implementers do not push.** Commit only. Branch pushes and PRs are the orchestrator's.
- **Every task is independently green.** `make check` passes at every task boundary — spec §5 asserts this for all seven tasks, and the plan holds itself to it. No task may leave a half-moved package, a dangling import, or a failing test "for the next task". If you find yourself wanting to, you have misread the plan: stop and report.
- **A2 and A4 must not run concurrently.** Both edit `pyproject.toml`'s Tier-0 `source_modules` list and both add exports to `src/eventsource/adapters/{memory,postgresql}/__init__.py`. Serialize them. The three chain roots (A1, A3, A6) touch disjoint files and *may* be dispatched together. This constraint is restated in both A2 and A4.
- **Red-first per task.** Every task writes its failing test first and observes the stated failure symptom before implementing. Steps are ordered so the red step precedes the green one; do not reorder. Where a task's red test is a new file, it must fail with `ModuleNotFoundError` at collection — not with an assertion — because the module it names does not exist yet.
- **Conformance suites follow `testing/conformance_ports/` house conventions.** An ABC with an `@abstractmethod @pytest.fixture def store(self)`, importing only `eventsource.ports`, `eventsource.domain`, `eventsource.events`, `eventsource.exceptions`, pytest and stdlib. **No adapter import in a suite file, ever** — not even under `TYPE_CHECKING`. Backend bindings own construction and provisioning. New suites are added to `conformance_ports/__init__.py`'s imports, `__all__`, and the module-docstring suite list.
- **Public API preserved via shims exactly as the spec specifies.** `eventsource.__all__` must be byte-identical before and after the slice: no name added, none removed, none reordered. The two shims (§1.3, §2.5) resolve every legacy name lazily with a `DeprecationWarning` that names the new import path per attribute and states removal in 0.8.0, and each defines `__dir__` alongside `__getattr__` — a lazy `__getattr__` without `__dir__` broke introspection during the snapshots migration and had to be fixed after the fact.
- **import-linter deltas exactly per spec §4.3, no more.** A3 swaps the six `eventsource.readmodels.*` Tier-0 entries out; A1 and A4 add the two `adapters.memory.*` entries; A7 adds the new "Ports must not import outward" contract. Nothing else in `[tool.importlinter]` changes. `uv run lint-imports` is in every task's verify step, not just the last.
- **No behavior changes in the moves.** Every moved module body is byte-identical apart from import rewrites. The single sanctioned semantic change in the whole slice is A2's rebasing of `LockAcquisitionError`/`LockNotHeldError` onto `EventSourceError` (spec §1.3), and it is called out in the CHANGELOG under **Changed** rather than buried in a re-home paragraph.
- **ADR 0029, CHANGELOG, `docs/`, `BACKLOG.md`, and `docs/core-surface.md` land in A7 only.** Implementers of A1–A6 write no CHANGELOG entries and no ADR text — this differs from the correctness slice, and it is deliberate: 0029 is one ADR spanning six tasks, and splitting its authorship six ways produces an incoherent record. The one exception the spec grants is none; A7 owns all of §4.
- **mypy strict:** all new and modified code fully annotated. `uv run mypy src/eventsource/ --config-file=pyproject.toml` is listed per task.
- **Prefix every pytest invocation with `timeout 120`.**
- **No live-src mutation probes.** Do not "try an edit and see what mypy says" on shared source while other agents are active. Reason from the code you read; experiment on a copy under `$CLAUDE_JOB_DIR/tmp`.
- Pre-commit hooks run on commit (ruff, mypy, import-linter). A task is not done until its commit succeeds, which means every task must leave the tree importable and type-clean.

### Dependency order

```
A1 ──▶ A2          (locks: port+memory, then postgres+shim+consumers)
A3 ──▶ A4 ──▶ A5   (readmodels: port, then adapters+shim, then conformance)
A6                 (engine, independent)
A1..A6 ──▶ A7      (docs, ADR, CHANGELOG, import-linter contract, backlog)
```

A1, A3, A6 are the three roots and may run in parallel. **A2 and A4 may not run concurrently with each other** (shared `pyproject.toml` and `adapters/*/__init__.py` edits).

### Integration tests

A2 and A5 have `@pytest.mark.postgres` bindings. Bring PostgreSQL up once:

```bash
docker compose -f docker-compose.test.yml up -d
```

and run with `timeout 300 uv run pytest tests/integration/... -m postgres -v`. If Docker is unavailable, complete the unit steps, run the integration steps as far as they go, and **report the gap explicitly** rather than claiming the acceptance criteria met.

---

### Task A1: `ports/locks.py`, the in-memory adapter, and `DistributedLockConformance`

Spec §1.1–1.4, §1.5 first bullet. Chain root, no dependencies. Creates the lock port and its test-scoped adapter; `PostgreSQLLockManager` does not move until A2, so `eventsource.locks` keeps working unchanged throughout this task.

**Files:**
- Create: `src/eventsource/ports/locks.py`
- Create: `src/eventsource/adapters/memory/locks.py`
- Create: `src/eventsource/testing/conformance_ports/locks.py`
- Create: `tests/unit/adapters/test_memory_locks_conformance.py`
- Modify: `src/eventsource/adapters/memory/__init__.py`
- Modify: `src/eventsource/testing/conformance_ports/__init__.py`
- Modify: `src/eventsource/ports/__init__.py`
- Modify: `pyproject.toml` (Tier-0 contract: add `eventsource.adapters.memory.locks`)

**Interfaces:**

- Produces, consumed by A2 — `eventsource.ports.locks`:
  ```python
  @dataclass(frozen=True)
  class LockInfo:
      key: str
      lock_id: int
      acquired_at: datetime
      holder_id: str | None = None

  def migration_lock_key(tenant_id: UUID, operation: str = "migration") -> str: ...

  class DistributedLock(Protocol):
      def acquire(
          self, key: str, *, timeout: float | None = None, retry_interval: float = 0.1
      ) -> AbstractAsyncContextManager[LockInfo]: ...
      async def try_acquire(self, key: str) -> LockInfo | None: ...
      async def release(self, key: str) -> None: ...
      async def is_held(self, key: str) -> bool: ...

  class LockRegistry(Protocol):
      async def release_all(self) -> int: ...
      @property
      def held_lock_count(self) -> int: ...

  class LockManager(DistributedLock, LockRegistry, Protocol): ...
  ```
  A2 annotates `migration/cutover.py` and `migration/coordinator.py` with `DistributedLock` (the narrow port), imports `LockInfo`/`migration_lock_key` from here, and makes `PostgreSQLLockManager` satisfy `LockManager` without declaring it.
- Produces, consumed by A2 — `DistributedLockConformance` in `eventsource.testing.conformance_ports`, which A2 binds to `PostgreSQLLockManager`.
- Consumes: `eventsource.exceptions` — **not yet.** `LockAcquisitionError`/`LockNotHeldError` still live in `eventsource.locks.postgresql` during A1, so this task's suite and adapter import them from `eventsource.locks`. A2 retargets both onto `eventsource.exceptions`. This is the one import in the slice that is knowingly temporary, and A2's step list names it.

- [ ] **Step 1 (red): write the memory conformance binding before anything it names exists**

Create `tests/unit/adapters/test_memory_locks_conformance.py`:

```python
"""Conformance tests for InMemoryLockManager against the DistributedLock port suite."""

from collections.abc import AsyncIterator

import pytest

from eventsource.adapters.memory import InMemoryLockManager
from eventsource.testing.conformance_ports import DistributedLockConformance


class TestMemoryLockManager(DistributedLockConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryLockManager]:
        manager = InMemoryLockManager(holder_id="conformance")
        yield manager
        await manager.release_all()

    async def test_lock_ids_are_stable_for_a_key(self, store: InMemoryLockManager) -> None:
        first = await store.try_acquire("stable")
        assert first is not None
        await store.release("stable")
        second = await store.try_acquire("stable")
        assert second is not None
        assert first.lock_id == second.lock_id
```

Run: `timeout 120 uv run pytest tests/unit/adapters/test_memory_locks_conformance.py -q`

Expected: FAIL at collection — `ModuleNotFoundError: No module named 'eventsource.adapters.memory.locks'` (raised from `eventsource.adapters.memory`'s `__init__`), or `ImportError: cannot import name 'InMemoryLockManager'` if you happen to have created the module first. Either symptom is the red; an assertion failure is not.

- [ ] **Step 2 (green): create `src/eventsource/ports/locks.py`**

`LockInfo` and `migration_lock_key` move **verbatim** from `src/eventsource/locks/postgresql.py:40-55` and `:507-525` — same field order, same defaults, same docstrings. Do not reformat them.

```python
"""Distributed lock port.

Pure boundary interface: stdlib, typing, dataclasses, datetime, uuid only.
No sqlalchemy, no observability, no implementation code.

The contract splits along its two real consumer groups (ISP, ADR 0019):
`migration/cutover.py` and `migration/coordinator.py` acquire and release
individual locks (`DistributedLock`), while bulk lifecycle over everything
one manager holds (`LockRegistry`) is a shutdown-and-introspection concern
for whoever owns the manager.

Scope of the promise (ADR 0029, amending ADR 0023): these protocols describe
the *shape of the dependency* -- mutual exclusion among callers sharing one
manager instance, plus the error contract. They do **not** promise
cross-process exclusion, release on crash, or fairness. Those are
PostgreSQL-specific guarantees documented on
`eventsource.adapters.postgresql.locks.PostgreSQLLockManager` and pinned only
by its integration tests.
"""

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID


@dataclass(frozen=True)
class LockInfo:
    """
    Information about an acquired lock.

    Attributes:
        key: The string key used to identify the lock
        lock_id: The numeric PostgreSQL lock ID (derived from key hash)
        acquired_at: When the lock was acquired
        holder_id: Optional identifier for the lock holder (for debugging)
    """

    key: str
    lock_id: int
    acquired_at: datetime
    holder_id: str | None = None


class DistributedLock(Protocol):
    """Acquire/release mutual exclusion on a string key."""

    def acquire(
        self,
        key: str,
        *,
        timeout: float | None = None,
        retry_interval: float = 0.1,
    ) -> AbstractAsyncContextManager[LockInfo]:
        """Acquire `key`, releasing it when the returned context exits.

        Declared as a plain method returning an async context manager rather
        than as `async def ... -> AsyncIterator[LockInfo]`: implementations
        decorate their `acquire` with `@asynccontextmanager`, whose runtime
        type is exactly `AbstractAsyncContextManager`, and the async-generator
        spelling would fail to type against it.

        Raises:
            LockAcquisitionError: if the lock cannot be acquired within `timeout`.
        """
        ...

    async def try_acquire(self, key: str) -> LockInfo | None:
        """Acquire `key` without blocking; `None` if it is already held.

        The caller owns the release.
        """
        ...

    async def release(self, key: str) -> None:
        """Release a lock previously taken with `try_acquire`.

        Raises:
            LockNotHeldError: if this manager does not hold `key`.
        """
        ...

    async def is_held(self, key: str) -> bool:
        """Is `key` currently held by this manager?"""
        ...


class LockRegistry(Protocol):
    """Bulk lifecycle over everything one manager instance holds."""

    async def release_all(self) -> int:
        """Release every held lock; returns the number released."""
        ...

    @property
    def held_lock_count(self) -> int:
        """Number of locks currently held by this manager."""
        ...


class LockManager(DistributedLock, LockRegistry, Protocol):
    """Composed convenience protocol: both capabilities in one object."""


def migration_lock_key(tenant_id: UUID, operation: str = "migration") -> str:
    """
    Create a lock key for migration operations.

    Provides a consistent naming convention for migration-related locks.

    Args:
        tenant_id: Tenant UUID
        operation: Operation type (migration, cutover, etc.)

    Returns:
        Lock key string in format "{operation}:{tenant_id}"

    Example:
        >>> key = migration_lock_key(tenant_id, "cutover")
        >>> async with lock_manager.acquire(key):
        ...     await perform_cutover()
    """
    return f"{operation}:{tenant_id}"


__all__ = [
    "DistributedLock",
    "LockInfo",
    "LockManager",
    "LockRegistry",
    "migration_lock_key",
]
```

None of the three protocols is `@runtime_checkable` — no consumer does `isinstance`, matching `ports/store.py`'s policy. (`ports/checkpoints.py` *is* runtime-checkable; that is not the precedent to copy here.)

- [ ] **Step 3 (green): export the port names from `ports/__init__.py`**

In `src/eventsource/ports/__init__.py`, add the import block in alphabetical position (after the `eventsource.ports.envelopes` block at line 17-24, before `eventsource.ports.outbox` at line 25):

```python
from eventsource.ports.locks import (
    DistributedLock,
    LockInfo,
    LockManager,
    LockRegistry,
    migration_lock_key,
)
```

and append to `__all__`, after the `"ProjectionFailureCount",` entry that currently closes it:

```python
    # Lock port
    "DistributedLock",
    "LockInfo",
    "LockManager",
    "LockRegistry",
    "migration_lock_key",
```

Do not touch `src/eventsource/__init__.py`: no lock name becomes a top-level export (spec §1.1 — `docs/api/locks.md:189` says so deliberately, and A7 keeps that statement true).

- [ ] **Step 4 (green): create `src/eventsource/adapters/memory/locks.py`**

Stdlib only — `asyncio`, `dataclasses` (via the port's `LockInfo`), `datetime`, `hashlib`, `contextlib`. It must import no sqlalchemy and no observability; the Tier-0 contract entry added in Step 7 enforces the first.

`_key_to_lock_id` is copied from `PostgreSQLLockManager._key_to_lock_id` (`locks/postgresql.py:154-176`) so `LockInfo.lock_id` has the same value for the same key across both adapters — that parity is what the binding's `test_lock_ids_are_stable_for_a_key` exercises.

```python
"""In-process lock manager implementing the DistributedLock port.

**This is a test double, not a distributed lock.** It excludes only
coroutines running in one asyncio event loop in one process. It offers no
cross-process or cross-connection exclusion, no release on crash (a killed
process takes its lock table with it), and no fairness or FIFO ordering
among waiters. Use it in unit tests that need a `DistributedLock` and
nothing more; use
`eventsource.adapters.postgresql.locks.PostgreSQLLockManager` anywhere two
processes must coordinate. See ADR 0029 and ADR 0023.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from eventsource.locks.postgresql import LockAcquisitionError, LockNotHeldError
from eventsource.ports.locks import LockInfo


class InMemoryLockManager:
    """Single-process `LockManager` backed by a dict and an `asyncio.Condition`."""

    def __init__(self, *, holder_id: str | None = None) -> None:
        self._holder_id = holder_id
        self._held: dict[str, LockInfo] = {}
        self._condition = asyncio.Condition()

    @staticmethod
    def _key_to_lock_id(key: str) -> int:
        """63-bit lock id for `key`, identical to the PostgreSQL adapter's."""
        hash_bytes = hashlib.sha256(key.encode()).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big") & 0x7FFFFFFFFFFFFFFF

    def _make_info(self, key: str) -> LockInfo:
        return LockInfo(
            key=key,
            lock_id=self._key_to_lock_id(key),
            acquired_at=datetime.now(UTC),
            holder_id=self._holder_id,
        )

    @asynccontextmanager
    async def acquire(
        self,
        key: str,
        *,
        timeout: float | None = None,
        retry_interval: float = 0.1,
    ) -> AsyncIterator[LockInfo]:
        """Acquire `key`, releasing it on context exit.

        `retry_interval` is accepted for port compatibility and ignored: this
        implementation waits on a condition variable rather than polling.
        """
        info = await self._acquire(key, timeout)
        try:
            yield info
        finally:
            await self._release(key)

    async def _acquire(self, key: str, timeout: float | None) -> LockInfo:
        async def wait_and_take() -> LockInfo:
            async with self._condition:
                await self._condition.wait_for(lambda: key not in self._held)
                info = self._make_info(key)
                self._held[key] = info
                return info

        if timeout is None:
            return await wait_and_take()
        try:
            return await asyncio.wait_for(wait_and_take(), timeout)
        except TimeoutError as exc:
            raise LockAcquisitionError(
                key=key,
                reason=f"Timeout after {timeout}s",
                timeout=timeout,
            ) from exc

    async def _release(self, key: str) -> None:
        async with self._condition:
            self._held.pop(key, None)
            self._condition.notify_all()

    async def try_acquire(self, key: str) -> LockInfo | None:
        async with self._condition:
            if key in self._held:
                return None
            info = self._make_info(key)
            self._held[key] = info
            return info

    async def release(self, key: str) -> None:
        async with self._condition:
            if key not in self._held:
                raise LockNotHeldError(key)
            del self._held[key]
            self._condition.notify_all()

    async def is_held(self, key: str) -> bool:
        async with self._condition:
            return key in self._held

    async def release_all(self) -> int:
        async with self._condition:
            released = len(self._held)
            self._held.clear()
            self._condition.notify_all()
            return released

    @property
    def held_lock_count(self) -> int:
        return len(self._held)


__all__ = ["InMemoryLockManager"]
```

**Note the temporary import** on line `from eventsource.locks.postgresql import LockAcquisitionError, LockNotHeldError`. A2 replaces it with `from eventsource.exceptions import LockAcquisitionError, LockNotHeldError`. It is correct for A1 (that is where the classes are at 8e9ccd4) and A2's step list retargets it explicitly. It does *not* make this module sqlalchemy-dependent for import-linter purposes — `eventsource.locks.postgresql` imports sqlalchemy, so the Tier-0 contract entry added in Step 7 would fail. **Therefore in A1 the entry is not yet added**; see Step 7's exact instruction.

- [ ] **Step 5 (green): register the adapter export**

In `src/eventsource/adapters/memory/__init__.py`, add the import in alphabetical position (after `dlq`, before `outbox`) and the `__all__` entry (after `"InMemoryEventStore"`, before `"InMemoryOutboxRepository"`):

```python
from eventsource.adapters.memory.locks import InMemoryLockManager
```
```python
    "InMemoryLockManager",
```

Also extend the module docstring's port list: `"""In-process memory adapters implementing the store, snapshot, checkpoint, DLQ, outbox, and lock ports."""`

- [ ] **Step 6 (green): create the conformance suite**

`src/eventsource/testing/conformance_ports/locks.py`. Nine cases, exactly the intersection both backends honestly meet (spec §1.4). The module docstring must state what the suite deliberately does *not* pin.

```python
"""Conformance suite for the `DistributedLock` port.

Subclass and provide a `store` fixture yielding a fresh lock manager.

Pins only the intersection every backend honestly meets: exclusion among
callers of one manager instance, the context-manager release contract, and
the two error types. **Deliberately not pinned here** -- cross-process or
cross-connection exclusion, release on crash, fairness/FIFO ordering among
waiters, and the numeric value of `LockInfo.lock_id`. Those are
PostgreSQL-specific and stay in
`tests/integration/locks/test_postgresql_locks_integration.py`.
"""

from abc import ABC, abstractmethod

import pytest

from eventsource.exceptions import LockAcquisitionError, LockNotHeldError
from eventsource.ports.locks import LockManager


class DistributedLockConformance(ABC):
    """Conformance suite for `DistributedLock` + `LockRegistry` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh manager implementing `LockManager`."""
        raise NotImplementedError

    async def test_acquire_yields_info_for_the_requested_key(self, store: LockManager) -> None:
        async with store.acquire("alpha") as info:
            assert info.key == "alpha"

    async def test_is_held_inside_and_outside_the_block(self, store: LockManager) -> None:
        async with store.acquire("alpha"):
            assert await store.is_held("alpha") is True
        assert await store.is_held("alpha") is False

    async def test_acquire_releases_on_exception_and_propagates_it(
        self, store: LockManager
    ) -> None:
        sentinel = RuntimeError("boom")
        with pytest.raises(RuntimeError) as caught:
            async with store.acquire("alpha"):
                raise sentinel
        assert caught.value is sentinel
        assert await store.is_held("alpha") is False

    async def test_try_acquire_succeeds_on_free_key_and_fails_on_held(
        self, store: LockManager
    ) -> None:
        first = await store.try_acquire("alpha")
        assert first is not None
        assert first.key == "alpha"
        assert await store.try_acquire("alpha") is None

    async def test_release_makes_the_key_acquirable_again(self, store: LockManager) -> None:
        assert await store.try_acquire("alpha") is not None
        await store.release("alpha")
        assert await store.try_acquire("alpha") is not None

    async def test_release_of_a_never_held_key_raises(self, store: LockManager) -> None:
        with pytest.raises(LockNotHeldError):
            await store.release("never-held")

    async def test_acquire_with_timeout_on_a_held_key_raises(self, store: LockManager) -> None:
        assert await store.try_acquire("alpha") is not None
        with pytest.raises(LockAcquisitionError) as caught:
            async with store.acquire("alpha", timeout=0.05):
                pass  # pragma: no cover - acquisition must fail
        assert caught.value.timeout == 0.05

    async def test_different_keys_are_independent(self, store: LockManager) -> None:
        async with store.acquire("alpha"):
            async with store.acquire("beta") as beta:
                assert beta.key == "beta"
            assert await store.is_held("alpha") is True

    async def test_release_all_returns_the_count_and_empties_the_registry(
        self, store: LockManager
    ) -> None:
        assert await store.try_acquire("alpha") is not None
        assert await store.try_acquire("beta") is not None
        assert store.held_lock_count == 2
        assert await store.release_all() == 2
        assert store.held_lock_count == 0


__all__ = ["DistributedLockConformance"]
```

**`from eventsource.exceptions import LockAcquisitionError, LockNotHeldError` is deliberate and forward-looking**: those classes do not live there until A2. This suite therefore does not import successfully until A2 lands — which is fine for the *port* but not for A1's green gate. **In A1, import them from `eventsource.locks` instead** (`from eventsource.locks import LockAcquisitionError, LockNotHeldError`), and A2 retargets this line to `eventsource.exceptions` as an explicit step. Note that `eventsource.locks` is not a permitted suite import under the house convention, so this is a knowingly-temporary two-task deviation, closed by A2 — record it as such in the A1 report.

- [ ] **Step 7 (green): register the suite; leave the Tier-0 contract entry for A2**

In `src/eventsource/testing/conformance_ports/__init__.py`: add the import (alphabetically, after `feed`, before `outbox`), add `"DistributedLockConformance"` to `__all__`, and add it to the docstring's suite list at line 10-14.

```python
from eventsource.testing.conformance_ports.locks import DistributedLockConformance
```

**Do not add `eventsource.adapters.memory.locks` to `pyproject.toml`'s Tier-0 `source_modules` in this task.** While the memory adapter still imports `eventsource.locks.postgresql` (Step 4's temporary import), that entry would make `lint-imports` fail. A2 adds the entry in the same step that retargets the import. This is the one deviation from "each task carries its own contract delta" and it is forced by the ordering; the A1 report must say so.

- [ ] **Step 8: verify and commit**

```bash
timeout 120 uv run pytest tests/unit/adapters/test_memory_locks_conformance.py -q
uv run ruff check src/eventsource tests --fix && uv run ruff format src/eventsource tests
uv run mypy src/eventsource/ --config-file=pyproject.toml
uv run lint-imports
make check
```

All green. Then:

```bash
git add src/eventsource/ports/locks.py \
        src/eventsource/adapters/memory/locks.py \
        src/eventsource/testing/conformance_ports/locks.py \
        tests/unit/adapters/test_memory_locks_conformance.py \
        src/eventsource/adapters/memory/__init__.py \
        src/eventsource/testing/conformance_ports/__init__.py \
        src/eventsource/ports/__init__.py
git commit -m "feat: add distributed lock port and in-memory adapter

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

**Acceptance:** `DistributedLockConformance` passes against `InMemoryLockManager`; `eventsource.ports.locks` imports with sqlalchemy absent from `sys.modules`; `eventsource.locks` still exports all five legacy names unchanged; `eventsource.__all__` unchanged; `make check` green.

---

### Task A2: move `PostgreSQLLockManager`, rehome the lock exceptions, install the `locks/` shim

Spec §1.3 (target layout + the exceptions ruling), §1.5 second and third bullets, §1.6. **Depends on A1.**

> **Serialization constraint:** this task edits `pyproject.toml`'s Tier-0 `source_modules` list and `src/eventsource/adapters/postgresql/__init__.py`. **Task A4 edits the same `pyproject.toml` block and the sibling `adapters/{memory,sqlite,postgresql}/__init__.py` files.** A2 and A4 must not run concurrently.

**Files:**
- Create: `src/eventsource/adapters/postgresql/locks.py` (via `git mv` from `src/eventsource/locks/postgresql.py`, then edit)
- Delete: `src/eventsource/locks/postgresql.py`
- Rewrite: `src/eventsource/locks/__init__.py` (becomes the shim)
- Modify: `src/eventsource/exceptions.py`
- Modify: `src/eventsource/adapters/postgresql/__init__.py`
- Modify: `src/eventsource/adapters/memory/locks.py` (retarget the exception import)
- Modify: `src/eventsource/testing/conformance_ports/locks.py` (retarget the exception import)
- Modify: `src/eventsource/migration/cutover.py`, `src/eventsource/migration/coordinator.py`
- Create: `tests/unit/test_locks_shim.py`
- Create: `tests/integration/locks/test_postgresql_locks_conformance.py`
- Modify: `tests/locks/test_postgresql_locks.py`, `tests/integration/locks/test_postgresql_locks_integration.py` (import lines only)
- Modify: `pyproject.toml` (Tier-0 contract: add the two memory entries)

**Interfaces:**
- Consumes A1's `eventsource.ports.locks` (`DistributedLock`, `LockInfo`, `migration_lock_key`) and `DistributedLockConformance`.
- Produces: `eventsource.adapters.postgresql.locks.PostgreSQLLockManager`, structurally satisfying `LockManager` without declaring it (adapters never inherit from ports here).
- Produces: `eventsource.exceptions.LockAcquisitionError` and `LockNotHeldError`, both now `EventSourceError` subclasses. Nothing later in the slice consumes them, but A7's CHANGELOG **Changed** entry names this.
- Consumer signature change (spec §1.6): `MigrationCoordinator.__init__`'s `lock_manager` parameter is annotated `DistributedLock | None`, and `CutoverManager.__init__`'s is `DistributedLock`. Runtime behavior unchanged — both were `TYPE_CHECKING`-only annotations.

- [ ] **Step 1 (red): the shim test**

Create `tests/unit/test_locks_shim.py`:

```python
"""The `eventsource.locks` deprecation shim resolves every legacy name."""

import warnings

import pytest

MOVED = {
    "LockInfo": "eventsource.ports.locks",
    "migration_lock_key": "eventsource.ports.locks",
    "LockAcquisitionError": "eventsource.exceptions",
    "LockNotHeldError": "eventsource.exceptions",
    "PostgreSQLLockManager": "eventsource.adapters.postgresql.locks",
}


@pytest.mark.parametrize(("name", "new_path"), sorted(MOVED.items()))
def test_legacy_name_resolves_with_a_deprecation_warning(name: str, new_path: str) -> None:
    import eventsource.locks as shim

    with pytest.warns(DeprecationWarning, match=new_path):
        attribute = getattr(shim, name)
    assert attribute is not None


def test_dir_lists_every_moved_name() -> None:
    import eventsource.locks as shim

    assert set(MOVED) <= set(dir(shim))


def test_unknown_attribute_raises_attribute_error() -> None:
    import eventsource.locks as shim

    with pytest.raises(AttributeError):
        shim.NotAThing  # noqa: B018


def test_lock_exceptions_are_eventsource_errors() -> None:
    from eventsource.exceptions import (
        EventSourceError,
        LockAcquisitionError,
        LockNotHeldError,
    )

    assert issubclass(LockAcquisitionError, EventSourceError)
    assert issubclass(LockNotHeldError, EventSourceError)


def test_importing_the_shim_emits_no_warning_by_itself() -> None:
    import importlib

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        importlib.import_module("eventsource.locks")
```

Run: `timeout 120 uv run pytest tests/unit/test_locks_shim.py -q`

Expected: FAIL. `test_legacy_name_resolves_with_a_deprecation_warning` fails with `Failed: DID NOT WARN` (the current `__init__.py` re-exports eagerly), and `test_lock_exceptions_are_eventsource_errors` fails with `ImportError: cannot import name 'LockAcquisitionError' from 'eventsource.exceptions'`.

- [ ] **Step 2 (red): the PostgreSQL conformance binding**

Create `tests/integration/locks/test_postgresql_locks_conformance.py`:

```python
"""Conformance tests for PostgreSQLLockManager against the DistributedLock port suite."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

from eventsource.adapters.postgresql.locks import PostgreSQLLockManager
from eventsource.testing.conformance_ports import DistributedLockConformance

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


class TestPostgreSQLLockManager(DistributedLockConformance):
    @pytest_asyncio.fixture
    async def store(
        self, postgres_session_factory: async_sessionmaker[AsyncSession]
    ) -> AsyncIterator[PostgreSQLLockManager]:
        manager = PostgreSQLLockManager(
            postgres_session_factory,
            holder_id="conformance",
            enable_tracing=False,
        )
        yield manager
        await manager.release_all()
```

Run: `timeout 120 uv run pytest tests/integration/locks/test_postgresql_locks_conformance.py -q`

Expected: FAIL at collection — `ModuleNotFoundError: No module named 'eventsource.adapters.postgresql.locks'`.

- [ ] **Step 3 (green): rehome the two exceptions onto `EventSourceError`**

Append to `src/eventsource/exceptions.py`, after the last existing class, the two classes moved from `locks/postgresql.py:58-90` — bodies verbatim, base class changed from `Exception` to `EventSourceError`:

```python
class LockAcquisitionError(EventSourceError):
    """
    Raised when a lock cannot be acquired.

    Attributes:
        key: The lock key that could not be acquired
        reason: Description of why acquisition failed
        timeout: The timeout value if timeout was the cause
    """

    def __init__(
        self,
        key: str,
        reason: str,
        timeout: float | None = None,
    ):
        self.key = key
        self.reason = reason
        self.timeout = timeout
        super().__init__(f"Failed to acquire lock '{key}': {reason}")


class LockNotHeldError(EventSourceError):
    """
    Raised when attempting to release a lock not held.

    Attributes:
        key: The lock key that was not held
    """

    def __init__(self, key: str):
        self.key = key
        super().__init__(f"Lock '{key}' is not held by this session")
```

If `exceptions.py` has an `__all__`, add both names to it in the existing order convention; check before assuming (at 8e9ccd4 it does not, and in that case add nothing).

This is the slice's one semantic change. It widens only: every existing `except LockAcquisitionError` and `except Exception` still catches, and the newly-catching `except EventSourceError` catches nothing lock-related today.

- [ ] **Step 4 (green): move the manager**

```bash
git mv src/eventsource/locks/postgresql.py src/eventsource/adapters/postgresql/locks.py
```

Then edit `src/eventsource/adapters/postgresql/locks.py`:

1. Delete the `LockInfo` dataclass (old lines 40-55), both exception classes (old lines 58-90), and the `migration_lock_key` function (old lines 507-525). They now live in `eventsource.ports.locks` and `eventsource.exceptions`.
2. Replace the import block. Old:
   ```python
   from collections.abc import AsyncIterator
   from contextlib import asynccontextmanager
   from dataclasses import dataclass
   from datetime import UTC, datetime
   from typing import TYPE_CHECKING
   from uuid import UUID

   from sqlalchemy import text
   from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

   from eventsource.observability import Tracer, create_tracer

   if TYPE_CHECKING:
       pass
   ```
   New:
   ```python
   from collections.abc import AsyncIterator
   from contextlib import asynccontextmanager
   from datetime import UTC, datetime

   from sqlalchemy import text
   from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

   from eventsource.exceptions import LockAcquisitionError, LockNotHeldError
   from eventsource.observability import Tracer, create_tracer
   from eventsource.ports.locks import LockInfo
   ```
   (`dataclass`, `UUID`, and the empty `TYPE_CHECKING` block go with the code that used them. `hashlib`, `asyncio`, `logging` stay.)
3. Extend the module docstring's first paragraph with one sentence naming the port it implements and the guarantees that are *its own*, not the port's:

   > Implements `eventsource.ports.locks.LockManager`. The cross-process exclusion, release-on-connection-loss, and PostgreSQL-fairness guarantees documented below are this adapter's, not the port's -- see ADR 0029.
4. Add `__all__ = ["PostgreSQLLockManager"]` at the end.

**Change nothing else.** The class body, the 63-bit key hashing, the per-lock `AsyncSession`, the poll loop, and every tracing attribute stay byte-identical. `git diff -M` should show this as a rename plus the deletions and import rewrite above, and nothing more.

- [ ] **Step 5 (green): register the adapter export**

In `src/eventsource/adapters/postgresql/__init__.py`, add the import first alphabetically (before `outbox`) and `"PostgreSQLLockManager"` to `__all__` between `"PostgreSQLEventStore"` and `"PostgreSQLOutboxRepository"`:

```python
from eventsource.adapters.postgresql.locks import PostgreSQLLockManager
```

Update the docstring: `"""PostgreSQL adapter implementing the store, snapshot, outbox, and lock ports."""`

- [ ] **Step 6 (green): close A1's two temporary imports and add the Tier-0 entries**

In `src/eventsource/adapters/memory/locks.py`, replace:
```python
from eventsource.locks.postgresql import LockAcquisitionError, LockNotHeldError
```
with:
```python
from eventsource.exceptions import LockAcquisitionError, LockNotHeldError
```

In `src/eventsource/testing/conformance_ports/locks.py`, replace `from eventsource.locks import LockAcquisitionError, LockNotHeldError` with `from eventsource.exceptions import LockAcquisitionError, LockNotHeldError` (the form already written in A1's Step 6 code block).

Then in `pyproject.toml`, in the "Tier 0 modules must not import sqlalchemy" contract's `source_modules`, add after `"eventsource.adapters.memory.outbox",`:

```toml
    "eventsource.adapters.memory.locks",
```

(The `eventsource.adapters.memory.readmodels` entry that spec §4.3 pairs with this one belongs to A4 — do not add it here; the module does not exist yet and `lint-imports` will fail on a missing source module.)

- [ ] **Step 7 (green): the `locks/` shim**

Replace the entire contents of `src/eventsource/locks/__init__.py`:

```python
"""Deprecated import path for the distributed-lock subsystem.

Every name below still resolves, each with a `DeprecationWarning` naming its
new home. This package is removed in 0.8.0.

- `LockInfo`, `migration_lock_key` -> `eventsource.ports.locks`
- `LockAcquisitionError`, `LockNotHeldError` -> `eventsource.exceptions`
- `PostgreSQLLockManager` -> `eventsource.adapters.postgresql.locks`

Resolution is lazy: importing this module pulls in neither sqlalchemy nor
the PostgreSQL adapter until a name that needs them is actually read.
"""

import importlib
import warnings

_MOVED = {
    "LockInfo": "eventsource.ports.locks",
    "migration_lock_key": "eventsource.ports.locks",
    "LockAcquisitionError": "eventsource.exceptions",
    "LockNotHeldError": "eventsource.exceptions",
    "PostgreSQLLockManager": "eventsource.adapters.postgresql.locks",
}

__all__ = [
    "LockAcquisitionError",
    "LockInfo",
    "LockNotHeldError",
    "PostgreSQLLockManager",
    "migration_lock_key",
]


def __getattr__(name: str) -> object:
    try:
        module_name = _MOVED[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    warnings.warn(
        f"eventsource.locks.{name} is deprecated; "
        f"import it from {module_name} instead. "
        f"eventsource.locks is removed in 0.8.0.",
        DeprecationWarning,
        stacklevel=2,
    )
    return getattr(importlib.import_module(module_name), name)


def __dir__() -> list[str]:
    return sorted(__all__)
```

`__dir__` is not optional — the snapshots migration shipped a lazy `__getattr__` without it and broke introspection.

- [ ] **Step 8 (green): retarget the migration consumers onto the narrow port**

`src/eventsource/migration/cutover.py`:
- line 63: `from eventsource.locks import LockAcquisitionError, migration_lock_key` →
  ```python
  from eventsource.exceptions import LockAcquisitionError
  from eventsource.ports.locks import migration_lock_key
  ```
  placed in alphabetical order within the existing `eventsource.*` import block (`exceptions` before `migration`, `ports.locks` after `observability` beside the existing `from eventsource.ports import Position`).
- line 78, inside `if TYPE_CHECKING:`: `from eventsource.locks import PostgreSQLLockManager` → **delete**. `DistributedLock` is needed at runtime for nothing but the annotation, and the module has `from __future__ import annotations` (line 55), so a `TYPE_CHECKING` import is still correct — replace it with `from eventsource.ports.locks import DistributedLock`.
- line 146: `lock_manager: PostgreSQLLockManager,` → `lock_manager: DistributedLock,`.
- lines 138 and 158 are docstring prose naming "PostgreSQL advisory lock manager"; change both to "Distributed lock manager". Line 32 is a docstring usage example that names the variable only — leave it.

`src/eventsource/migration/coordinator.py`:
- line 103, inside `if TYPE_CHECKING:`: `from eventsource.locks import PostgreSQLLockManager` → `from eventsource.ports.locks import DistributedLock`.
- line 202: `lock_manager: PostgreSQLLockManager | None = None,` → `lock_manager: DistributedLock | None = None,`.
- lines 181 and 217 docstring prose: "PostgreSQL advisory lock manager for cutover coordination" → "Distributed lock manager for cutover coordination".
- lines 1384-1386: the `MigrationError` message says "Provide a PostgreSQLLockManager when creating the coordinator". Change to "Provide a lock manager (e.g. PostgreSQLLockManager) when creating the coordinator". Check whether a unit test asserts on this string before editing: `grep -rn "lock_manager not provided" tests/`. If one does, update it in the same step.

`grep -rn "eventsource.locks" src/` must now match only `src/eventsource/locks/__init__.py`.

- [ ] **Step 9 (green): retarget the two existing lock test files, import lines only**

`tests/locks/test_postgresql_locks.py` line 15 and `tests/integration/locks/test_postgresql_locks_integration.py` line 20 both read `from eventsource.locks import (...)`. Replace each with the three-source form:

```python
from eventsource.adapters.postgresql.locks import PostgreSQLLockManager
from eventsource.exceptions import LockAcquisitionError, LockNotHeldError
from eventsource.ports.locks import LockInfo, migration_lock_key
```

taking only the names each file actually imports (check its current import list; the integration file imports four names, not five). **Nothing else in either file changes** — that is the behavior-preservation evidence for the move, and the A2 reviewer checks exactly this with `git diff tests/locks/test_postgresql_locks.py`.

- [ ] **Step 10: verify and commit**

```bash
timeout 120 uv run pytest tests/unit/test_locks_shim.py tests/locks/ tests/unit/adapters/test_memory_locks_conformance.py -q
timeout 300 uv run pytest tests/integration/locks/ -m postgres -q   # needs Docker
uv run ruff check src/eventsource tests --fix && uv run ruff format src/eventsource tests
uv run mypy src/eventsource/ --config-file=pyproject.toml
uv run lint-imports
make check
```

Confirm the acceptance greps before committing:

```bash
grep -rn "eventsource\.locks" src/          # only src/eventsource/locks/__init__.py
test ! -e src/eventsource/locks/postgresql.py && echo "deleted"
git diff --stat tests/locks/test_postgresql_locks.py   # 1 file, small
```

```bash
git add src/eventsource/adapters/postgresql/locks.py src/eventsource/locks/__init__.py \
        src/eventsource/exceptions.py src/eventsource/adapters/postgresql/__init__.py \
        src/eventsource/adapters/memory/locks.py \
        src/eventsource/testing/conformance_ports/locks.py \
        src/eventsource/migration/cutover.py src/eventsource/migration/coordinator.py \
        tests/unit/test_locks_shim.py \
        tests/integration/locks/test_postgresql_locks_conformance.py \
        tests/locks/test_postgresql_locks.py \
        tests/integration/locks/test_postgresql_locks_integration.py \
        pyproject.toml
git add -u src/eventsource/locks/
git commit -m "refactor: move postgresql lock manager to adapters ring

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

**Acceptance (spec §1.6):** `grep -rn "eventsource.locks" src/` matches only the shim; `src/eventsource/locks/postgresql.py` gone; `DistributedLockConformance` green against both adapters; migration consumers annotate `DistributedLock`; `eventsource.adapters.memory.locks` passes the Tier-0 contract; all five legacy names import with a `DeprecationWarning`; `tests/locks/test_postgresql_locks.py` passes with only import lines changed; `make check` green.

---

### Task A3: create `ports/readmodels/`, retarget the rest of `readmodels/` onto it

Spec §2.1–2.2, §2.4, §2.5 (first four rows), §2.7 first bullet. Chain root, no dependencies. **This task moves only the four pure modules.** `readmodels/` keeps working identically from outside — `schema`, `in_memory`, `postgresql`, `sqlite`, `projection` and `__init__` stay put and simply import from the new port.

**Files:**
- Create: `src/eventsource/ports/readmodels/__init__.py`, `model.py`, `query.py`, `repository.py`, `exceptions.py` (the last four via `git mv`, then edit)
- Delete: `src/eventsource/readmodels/base.py`, `query.py`, `repository.py`, `exceptions.py`
- Modify: `src/eventsource/readmodels/__init__.py`, `schema.py`, `in_memory.py`, `postgresql.py`, `sqlite.py`, `projection.py`
- Create: `tests/unit/ports/test_readmodels_port_surface.py`
- Modify: `pyproject.toml` (Tier-0 contract: remove six `eventsource.readmodels.*` entries)
- Modify: existing `tests/unit/readmodels/test_base.py`, `test_query.py`, `test_repository_protocol.py` import lines **only if they import the private module paths**; if they import from `eventsource.readmodels`, leave them alone (the package still re-exports).

**Interfaces:**
- Produces, consumed by A4 and A5 — `eventsource.ports.readmodels`, re-exporting six public names from four submodules:
  ```
  ports/readmodels/__init__.py     ReadModel, Query, Filter, ReadModelRepository,
                                   ReadModelRepositoryProtocol, ReadModelError,
                                   OptimisticLockError, ReadModelNotFoundError
  ports/readmodels/model.py        ReadModel                      (<- readmodels/base.py)
  ports/readmodels/query.py        Query, Filter                  (<- readmodels/query.py)
  ports/readmodels/repository.py   ReadModelRepository,
                                   ReadModelRepositoryProtocol    (<- readmodels/repository.py)
  ports/readmodels/exceptions.py   ReadModelError, OptimisticLockError,
                                   ReadModelNotFoundError         (<- readmodels/exceptions.py)
  ```
  `ReadModelRepository` keeps its `Protocol[TModel]` genericity and its `@runtime_checkable`. `ReadModelRepositoryProtocol = ReadModelRepository` (the alias at the tail of `repository.py`) moves with it. A4's five adapter modules import from these paths; A5's conformance suite imports `ReadModel`, `Query`, `Filter`, and the exception trio from here and nothing else.
- **`OptimisticLockError` here is *not* `eventsource.exceptions.OptimisticLockError`.** Two unrelated classes share the name (spec §2.4): this one takes `(model_id, expected_version, actual_version=None)` and derives from `ReadModelError`; the core one takes `(aggregate_id, expected_version, actual_version)` and derives from `EventSourceError`. They do not catch each other. Do not "fix" this — A7 files it as a P2 backlog item.

- [ ] **Step 1 (red): the port-surface purity test**

Create `tests/unit/ports/test_readmodels_port_surface.py` (create `tests/unit/ports/__init__.py` if the directory does not exist):

```python
"""`eventsource.ports.readmodels` is importable without sqlalchemy."""

import subprocess
import sys


def test_public_names_import_from_the_port() -> None:
    from eventsource.ports.readmodels import (
        Filter,
        OptimisticLockError,
        Query,
        ReadModel,
        ReadModelError,
        ReadModelNotFoundError,
        ReadModelRepository,
        ReadModelRepositoryProtocol,
    )

    assert ReadModelRepositoryProtocol is ReadModelRepository
    assert issubclass(OptimisticLockError, ReadModelError)
    assert issubclass(ReadModelNotFoundError, ReadModelError)
    assert hasattr(ReadModel, "table_name")
    assert hasattr(Query, "with_filter")
    assert hasattr(Filter, "eq")


def test_no_submodule_pulls_in_sqlalchemy() -> None:
    """Import every port submodule in a fresh interpreter and assert sqlalchemy stayed out.

    A subprocess rather than an in-process check: by the time this test runs,
    another test module has almost certainly imported an adapter, so
    `sys.modules` in this process says nothing.
    """
    program = (
        "import sys\n"
        "import eventsource.ports.readmodels\n"
        "import eventsource.ports.readmodels.model\n"
        "import eventsource.ports.readmodels.query\n"
        "import eventsource.ports.readmodels.repository\n"
        "import eventsource.ports.readmodels.exceptions\n"
        "assert 'sqlalchemy' not in sys.modules, sorted(\n"
        "    m for m in sys.modules if m.startswith('sqlalchemy')\n"
        ")\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", program], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr
```

Run: `timeout 120 uv run pytest tests/unit/ports/test_readmodels_port_surface.py -q`

Expected: FAIL — `ModuleNotFoundError: No module named 'eventsource.ports.readmodels'` in both tests.

- [ ] **Step 2 (green): move the four modules**

```bash
mkdir -p src/eventsource/ports/readmodels
git mv src/eventsource/readmodels/base.py       src/eventsource/ports/readmodels/model.py
git mv src/eventsource/readmodels/query.py      src/eventsource/ports/readmodels/query.py
git mv src/eventsource/readmodels/repository.py src/eventsource/ports/readmodels/repository.py
git mv src/eventsource/readmodels/exceptions.py src/eventsource/ports/readmodels/exceptions.py
```

Bodies are unchanged. Only two import rewrites exist in the whole set, both in `repository.py`:

```python
from eventsource.readmodels.base import ReadModel
```
→
```python
from eventsource.ports.readmodels.model import ReadModel
```

and inside its `if TYPE_CHECKING:` block:

```python
    from eventsource.readmodels.query import Query
```
→
```python
    from eventsource.ports.readmodels.query import Query
```

`model.py` (pydantic + stdlib), `query.py` (stdlib only), and `exceptions.py` (stdlib only) have no intra-package imports and need no edit at all. Note that `model.py` keeps the module name `base` nowhere: if its docstring or any docstring example says "readmodels.base", update the prose. Do not rename the `ReadModel` class.

- [ ] **Step 3 (green): the port package `__init__.py`**

Create `src/eventsource/ports/readmodels/__init__.py`:

```python
"""Read-model port: the pure contract half of read-model persistence.

A subpackage rather than a flat `ports/readmodels.py` (ADR 0029): four
genuinely distinct pure artifacts -- a user-subclassable pydantic base, a
query specification language, a 15-method repository Protocol, and an
exception family -- that users import for four different reasons.
`eventsource.ports.readmodels` is the import path users see either way.

The adapter half lives under `eventsource.adapters.{memory,postgresql,
sqlite,sql}`.

Note: `OptimisticLockError` here is the read-model one
(`model_id`, `expected_version`, `actual_version`), a `ReadModelError`
subclass. It is a **different class** from
`eventsource.exceptions.OptimisticLockError`, which is raised on event
append and derives from `EventSourceError`. Neither catches the other. See
ADR 0029's recorded exception and the backlog item that resolves the name
collision.
"""

from eventsource.ports.readmodels.exceptions import (
    OptimisticLockError,
    ReadModelError,
    ReadModelNotFoundError,
)
from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.query import Filter, Query
from eventsource.ports.readmodels.repository import (
    ReadModelRepository,
    ReadModelRepositoryProtocol,
)

__all__ = [
    "Filter",
    "OptimisticLockError",
    "Query",
    "ReadModel",
    "ReadModelError",
    "ReadModelNotFoundError",
    "ReadModelRepository",
    "ReadModelRepositoryProtocol",
]
```

**Do not** add these names to `src/eventsource/ports/__init__.py`. `ports/__init__.py` re-exports flat port modules; `ports.readmodels` is its own namespace, and adding eight more names to the flat `ports` surface is a public-API addition this slice does not make. (A7's CHANGELOG **Added** entry names `eventsource.ports.readmodels`, the package — not new `eventsource.ports.*` names.)

- [ ] **Step 4 (green): retarget the five remaining `readmodels/` modules**

Each of these is a pure import rewrite; no other line changes.

`src/eventsource/readmodels/schema.py` line 41:
```python
from eventsource.readmodels.base import ReadModel
```
→ `from eventsource.ports.readmodels.model import ReadModel`

`src/eventsource/readmodels/in_memory.py` lines 22-24:
```python
from eventsource.readmodels.base import ReadModel
from eventsource.readmodels.exceptions import OptimisticLockError, ReadModelNotFoundError
from eventsource.readmodels.query import Filter, Query
```
→
```python
from eventsource.ports.readmodels.exceptions import (
    OptimisticLockError,
    ReadModelNotFoundError,
)
from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.query import Filter, Query
```
(these sort before the `eventsource.observability` block, so move them above it)

`src/eventsource/readmodels/postgresql.py` lines 27-29: same three-line rewrite as `in_memory.py`.

`src/eventsource/readmodels/sqlite.py` lines 33-34 and line 43:
```python
from eventsource.readmodels.exceptions import OptimisticLockError, ReadModelNotFoundError
from eventsource.readmodels.query import Filter, Query
```
→ `eventsource.ports.readmodels.{exceptions,query}`, and the mid-file
```python
from eventsource.readmodels.base import ReadModel as _BaseReadModel
```
→ `from eventsource.ports.readmodels.model import ReadModel as _BaseReadModel`.
Leave the unusual mid-file placement and the `_BaseReadModel` alias exactly where they are — this task changes import targets, not import hygiene.

`src/eventsource/readmodels/projection.py` line 23 and the `TYPE_CHECKING` import at line 28:
```python
from eventsource.readmodels.base import ReadModel
```
→ `from eventsource.ports.readmodels.model import ReadModel`
```python
    from eventsource.readmodels.repository import ReadModelRepository
```
→ `    from eventsource.ports.readmodels.repository import ReadModelRepository`

- [ ] **Step 5 (green): retarget `readmodels/__init__.py`'s re-exports**

`src/eventsource/readmodels/__init__.py` stays an eager re-export package in this task (it becomes the shim in A4). Rewrite only its import block (lines 67-85), leaving the docstring and `__all__` untouched:

```python
from eventsource.ports.readmodels.exceptions import (
    OptimisticLockError,
    ReadModelError,
    ReadModelNotFoundError,
)
from eventsource.ports.readmodels.model import ReadModel
from eventsource.ports.readmodels.query import Filter, Query
from eventsource.ports.readmodels.repository import ReadModelRepository
from eventsource.readmodels.in_memory import InMemoryReadModelRepository
from eventsource.readmodels.postgresql import PostgreSQLReadModelRepository
from eventsource.readmodels.projection import ReadModelProjection
from eventsource.readmodels.schema import (
    POSTGRESQL_TYPE_MAP,
    SQLITE_TYPE_MAP,
    generate_full_schema,
    generate_indexes,
    generate_schema,
)
from eventsource.readmodels.sqlite import SQLiteReadModelRepository
```

All sixteen `__all__` names still resolve from `eventsource.readmodels`. Nothing outside the package sees a difference.

- [ ] **Step 6 (green): swap the Tier-0 contract entries**

In `pyproject.toml`'s "Tier 0 modules must not import sqlalchemy" contract, **delete these six lines**:

```toml
    "eventsource.readmodels.base",
    "eventsource.readmodels.query",
    "eventsource.readmodels.schema",
    "eventsource.readmodels.repository",
    "eventsource.readmodels.in_memory",
    "eventsource.readmodels.exceptions",
```

Add nothing in their place. `"eventsource.ports"` is already in `source_modules` as a whole package, so `ports/readmodels/` is covered on arrival — that is the guard that replaces four of the six. `readmodels.schema` and `readmodels.in_memory` are covered again in A4 (as `adapters.sql.readmodel_schema` — see the note below — and `adapters.memory.readmodels`).

`readmodel_schema` deliberately loses its Tier-0 guard when A4 moves it to `adapters/sql/` (spec §2.3): the guard asserted the wrong property about a module that emits dialect-specific DDL. **A7 adds the explanatory comment** for that in the contract block; do not write it here, and do not add `eventsource.adapters.sql.readmodel_schema` to `source_modules`.

> **Serialization note:** A4 edits this same contract block (adding `eventsource.adapters.memory.readmodels`), as does A2 (adding `eventsource.adapters.memory.locks`). A3 is a chain root and runs before both, so it has no conflict — but do not batch A3 and A4 into one agent run.

- [ ] **Step 7: verify and commit**

```bash
timeout 120 uv run pytest tests/unit/ports/test_readmodels_port_surface.py tests/unit/readmodels/ -q
uv run ruff check src/eventsource tests --fix && uv run ruff format src/eventsource tests
uv run mypy src/eventsource/ --config-file=pyproject.toml
uv run lint-imports
make check
```

`tests/unit/readmodels/` must pass **unmodified** unless a test imports a private module path (`eventsource.readmodels.base` etc.). Check with `grep -rn "readmodels\.\(base\|query\|repository\|exceptions\)" tests/` and retarget only what that finds.

```bash
git add src/eventsource/ports/readmodels/ src/eventsource/readmodels/ \
        tests/unit/ports/ pyproject.toml
git add -u src/eventsource/readmodels/
git commit -m "refactor: extract read model port into ports/readmodels

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

**Acceptance:** `eventsource.ports.readmodels` and all four submodules import with sqlalchemy absent from `sys.modules` in a fresh interpreter; `eventsource.readmodels` still exports all sixteen names; `eventsource.__all__` unchanged; `make check` green.

---

### Task A4: move the five read-model adapters and install the `readmodels/` shim

Spec §2.3, §2.5 (last five rows + shim), §2.7 second bullet, §2.8. **Depends on A3.**

> **Serialization constraint:** this task edits `pyproject.toml`'s Tier-0 `source_modules` list and `src/eventsource/adapters/{memory,postgresql,sqlite,sql}/__init__.py`. **Task A2 edits the same `pyproject.toml` block and `adapters/{memory,postgresql}/__init__.py`.** A2 and A4 must not run concurrently.

**Files:**
- Create (all via `git mv`, then edit): `src/eventsource/adapters/memory/readmodels.py`, `src/eventsource/adapters/postgresql/readmodels.py`, `src/eventsource/adapters/sqlite/readmodels.py`, `src/eventsource/adapters/sql/readmodel_schema.py`, `src/eventsource/adapters/sql/readmodel_projection.py`
- Delete: `src/eventsource/readmodels/{in_memory,postgresql,sqlite,schema,projection}.py`
- Rewrite: `src/eventsource/readmodels/__init__.py` (becomes the shim)
- Modify: `src/eventsource/__init__.py` (line 199 only)
- Modify: `src/eventsource/adapters/{memory,postgresql,sqlite,sql}/__init__.py`
- Create: `tests/unit/test_readmodels_shim.py`
- Rewrite: `tests/unit/readmodels/test_exports.py` (retargeted, not deleted)
- Modify: `pyproject.toml` (Tier-0 contract: add `eventsource.adapters.memory.readmodels`)

**Interfaces:**
- Consumes A3's `eventsource.ports.readmodels.*`.
- Produces, consumed by A5 — three repository adapters at their new paths:
  `eventsource.adapters.memory.readmodels.InMemoryReadModelRepository`,
  `eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository`,
  `eventsource.adapters.sqlite.readmodels.SQLiteReadModelRepository`. All three constructors are unchanged: memory takes `(model_class, ...)`, PostgreSQL takes `(conn: AsyncConnection | AsyncEngine, model_class, ...)`, SQLite takes `(conn: aiosqlite.Connection, model_class, ...)` — A5's bindings depend on this.
- Produces, consumed by A5 — `eventsource.adapters.sql.readmodel_schema.generate_schema(model, dialect=...)`, which A5's SQL bindings call to provision `ConformanceReadModel`'s table.
- Preserves: `eventsource.ReadModelProjection` (top-level, `__init__.py:199` and `__all__:327`) now resolving to `eventsource.adapters.sql.readmodel_projection.ReadModelProjection`. `eventsource.__all__` is unchanged.

- [ ] **Step 1 (red): the shim test**

Create `tests/unit/test_readmodels_shim.py`:

```python
"""The `eventsource.readmodels` deprecation shim resolves every legacy name."""

import warnings

import pytest

MOVED = {
    "ReadModel": "eventsource.ports.readmodels.model",
    "ReadModelRepository": "eventsource.ports.readmodels.repository",
    "Query": "eventsource.ports.readmodels.query",
    "Filter": "eventsource.ports.readmodels.query",
    "ReadModelError": "eventsource.ports.readmodels.exceptions",
    "OptimisticLockError": "eventsource.ports.readmodels.exceptions",
    "ReadModelNotFoundError": "eventsource.ports.readmodels.exceptions",
    "InMemoryReadModelRepository": "eventsource.adapters.memory.readmodels",
    "PostgreSQLReadModelRepository": "eventsource.adapters.postgresql.readmodels",
    "SQLiteReadModelRepository": "eventsource.adapters.sqlite.readmodels",
    "ReadModelProjection": "eventsource.adapters.sql.readmodel_projection",
    "generate_schema": "eventsource.adapters.sql.readmodel_schema",
    "generate_indexes": "eventsource.adapters.sql.readmodel_schema",
    "generate_full_schema": "eventsource.adapters.sql.readmodel_schema",
    "POSTGRESQL_TYPE_MAP": "eventsource.adapters.sql.readmodel_schema",
    "SQLITE_TYPE_MAP": "eventsource.adapters.sql.readmodel_schema",
}


def test_every_legacy_name_is_covered() -> None:
    import eventsource.readmodels as shim

    assert set(shim.__all__) == set(MOVED)


@pytest.mark.parametrize(("name", "new_path"), sorted(MOVED.items()))
def test_legacy_name_resolves_with_a_deprecation_warning(name: str, new_path: str) -> None:
    import eventsource.readmodels as shim

    with pytest.warns(DeprecationWarning, match=new_path):
        assert getattr(shim, name) is not None


def test_dir_lists_every_moved_name() -> None:
    import eventsource.readmodels as shim

    assert set(MOVED) <= set(dir(shim))


def test_unknown_attribute_raises_attribute_error() -> None:
    import eventsource.readmodels as shim

    with pytest.raises(AttributeError):
        shim.NotAThing  # noqa: B018


def test_top_level_projection_comes_from_the_sql_adapter() -> None:
    import eventsource
    from eventsource.adapters.sql.readmodel_projection import ReadModelProjection

    assert eventsource.ReadModelProjection is ReadModelProjection


def test_importing_the_shim_emits_no_warning_by_itself() -> None:
    import importlib

    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        importlib.import_module("eventsource.readmodels")
```

Run: `timeout 120 uv run pytest tests/unit/test_readmodels_shim.py -q`

Expected: FAIL — `DID NOT WARN` on the parametrized cases (the package re-exports eagerly after A3) and `ModuleNotFoundError: No module named 'eventsource.adapters.sql.readmodel_projection'` on the top-level test.

- [ ] **Step 2 (green): move the five modules**

```bash
git mv src/eventsource/readmodels/in_memory.py  src/eventsource/adapters/memory/readmodels.py
git mv src/eventsource/readmodels/postgresql.py src/eventsource/adapters/postgresql/readmodels.py
git mv src/eventsource/readmodels/sqlite.py     src/eventsource/adapters/sqlite/readmodels.py
git mv src/eventsource/readmodels/schema.py     src/eventsource/adapters/sql/readmodel_schema.py
git mv src/eventsource/readmodels/projection.py src/eventsource/adapters/sql/readmodel_projection.py
```

**No body changes.** After A3 these modules already import from `eventsource.ports.readmodels.*`, so the only edits are:

- `adapters/postgresql/readmodels.py`: line 15's `from eventsource.adapters._sql.connection import sql_connection` is now a sibling-package import. It is textually identical and needs no change — but this is the line that closes the ADR 0026 §4 accepted-debt item for this module (spec §2.8), because the 16 `sql_connection` call sites now sit under `adapters/`. Do not touch it; note it in the report.
- `adapters/sql/readmodel_projection.py`: the `TYPE_CHECKING` import `from eventsource.ports.readmodels.repository import ReadModelRepository` and the runtime import `from eventsource.adapters.sql.projection import DatabaseProjection` both stay. Its two lazy in-function imports inside `_create_repository` (around lines 218 and 232 of the pre-move file) **do** change:
  ```python
  from eventsource.readmodels.postgresql import PostgreSQLReadModelRepository
  ```
  → `from eventsource.adapters.postgresql.readmodels import PostgreSQLReadModelRepository`
  ```python
  from eventsource.readmodels.sqlite import SQLiteReadModelRepository
  ```
  → `from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository`

  Keep them lazy (`# Import here to avoid circular imports` stays): `adapters/sql/` importing `adapters/postgresql/` and `adapters/sqlite/` at module scope would violate the "Infrastructure backends must not import each other" contract in spirit and add two eager sqlalchemy/aiosqlite chains. Verify `lint-imports` after this step specifically.
- `adapters/sql/readmodel_schema.py`: add one sentence to the module docstring saying why it is here rather than in `ports/` — it hardcodes `POSTGRESQL_TYPE_MAP` / `SQLITE_TYPE_MAP` and emits dialect-specific `CREATE TABLE` text, and dialect knowledge in a port is the boundary error this campaign removes (ADR 0029, spec §2.3).
- Each moved module gains/keeps an `__all__` naming exactly its public export(s); `readmodel_schema.py` already has one at its line 456 — leave it as is.

`git diff -M --stat` for this step should show five pure renames plus the small edits above. The A4 reviewer checks exactly that.

- [ ] **Step 3 (green): register the four adapter exports**

`src/eventsource/adapters/memory/__init__.py` — import after `locks` (alphabetical), `"InMemoryReadModelRepository"` into `__all__` after `"InMemoryOutboxRepository"`; docstring gains "read model":
```python
from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
```

`src/eventsource/adapters/postgresql/__init__.py` — import after `outbox`, `"PostgreSQLReadModelRepository"` after `"PostgreSQLOutboxRepository"`:
```python
from eventsource.adapters.postgresql.readmodels import PostgreSQLReadModelRepository
```

`src/eventsource/adapters/sqlite/__init__.py` — import after `outbox`, `"SQLiteReadModelRepository"` after `"SQLiteOutboxRepository"`:
```python
from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository
```

`src/eventsource/adapters/sql/__init__.py` — both new modules, with `__all__` re-sorted:
```python
from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository
from eventsource.adapters.sql.dlq import SQLDLQRepository
from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
from eventsource.adapters.sql.readmodel_schema import (
    POSTGRESQL_TYPE_MAP,
    SQLITE_TYPE_MAP,
    generate_full_schema,
    generate_indexes,
    generate_schema,
)

__all__ = [
    "POSTGRESQL_TYPE_MAP",
    "SQLITE_TYPE_MAP",
    "DatabaseProjection",
    "ReadModelProjection",
    "SQLCheckpointRepository",
    "SQLDLQRepository",
    "generate_full_schema",
    "generate_indexes",
    "generate_schema",
]
```

- [ ] **Step 4 (green): retarget the top-level re-export**

`src/eventsource/__init__.py` line 199:
```python
from eventsource.readmodels import ReadModelProjection
```
→
```python
from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
```

The comment on line 198 (`# ReadModel Projections (Phase 3)`) stays. `__all__` line 327 is untouched. **`eventsource.__all__` must be byte-identical to its pre-slice value** — verify before committing:
```bash
git show 8e9ccd4:src/eventsource/__init__.py | grep -n '^__all__' -A 400 > "$CLAUDE_JOB_DIR/tmp/all_before.txt"
grep -n '^__all__' -A 400 src/eventsource/__init__.py > "$CLAUDE_JOB_DIR/tmp/all_after.txt"
diff "$CLAUDE_JOB_DIR/tmp/all_before.txt" "$CLAUDE_JOB_DIR/tmp/all_after.txt" && echo IDENTICAL
```

- [ ] **Step 5 (green): the `readmodels/` shim**

Replace the entire contents of `src/eventsource/readmodels/__init__.py`. **Keep the existing module docstring's usage example** but update its import line to `from eventsource.ports.readmodels import ...` / `from eventsource.adapters.memory.readmodels import ...` so the example is not itself deprecated advice; then append the deprecation notice and the mechanism:

```python
"""Deprecated import path for read-model persistence.

Every name below still resolves, each with a `DeprecationWarning` naming its
new home. This package is removed in 0.8.0.

- `ReadModel`, `Query`, `Filter`, `ReadModelRepository`, `ReadModelError`,
  `OptimisticLockError`, `ReadModelNotFoundError`
  -> `eventsource.ports.readmodels`
- `InMemoryReadModelRepository` -> `eventsource.adapters.memory.readmodels`
- `PostgreSQLReadModelRepository` -> `eventsource.adapters.postgresql.readmodels`
- `SQLiteReadModelRepository` -> `eventsource.adapters.sqlite.readmodels`
- `ReadModelProjection` -> `eventsource.adapters.sql.readmodel_projection`
- `generate_schema`, `generate_indexes`, `generate_full_schema`,
  `POSTGRESQL_TYPE_MAP`, `SQLITE_TYPE_MAP`
  -> `eventsource.adapters.sql.readmodel_schema`

Resolution is lazy: importing this module pulls in no sqlalchemy and no
aiosqlite until a name that needs them is actually read.

Example:
    >>> from uuid import uuid4
    >>> from decimal import Decimal
    >>> from eventsource.ports.readmodels import ReadModel, Query, Filter
    >>> from eventsource.adapters.memory.readmodels import (
    ...     InMemoryReadModelRepository,
    ... )
    >>>
    >>> class OrderSummary(ReadModel):
    ...     order_number: str
    ...     status: str
    ...     total_amount: Decimal
    ...
    >>> repo = InMemoryReadModelRepository(OrderSummary)
    >>> shipped = await repo.find(Query(filters=[Filter.eq("status", "shipped")]))
"""

import importlib
import warnings

_MOVED = {
    "ReadModel": "eventsource.ports.readmodels.model",
    "ReadModelRepository": "eventsource.ports.readmodels.repository",
    "Query": "eventsource.ports.readmodels.query",
    "Filter": "eventsource.ports.readmodels.query",
    "ReadModelError": "eventsource.ports.readmodels.exceptions",
    "OptimisticLockError": "eventsource.ports.readmodels.exceptions",
    "ReadModelNotFoundError": "eventsource.ports.readmodels.exceptions",
    "InMemoryReadModelRepository": "eventsource.adapters.memory.readmodels",
    "PostgreSQLReadModelRepository": "eventsource.adapters.postgresql.readmodels",
    "SQLiteReadModelRepository": "eventsource.adapters.sqlite.readmodels",
    "ReadModelProjection": "eventsource.adapters.sql.readmodel_projection",
    "generate_schema": "eventsource.adapters.sql.readmodel_schema",
    "generate_indexes": "eventsource.adapters.sql.readmodel_schema",
    "generate_full_schema": "eventsource.adapters.sql.readmodel_schema",
    "POSTGRESQL_TYPE_MAP": "eventsource.adapters.sql.readmodel_schema",
    "SQLITE_TYPE_MAP": "eventsource.adapters.sql.readmodel_schema",
}

__all__ = [
    # Base class
    "ReadModel",
    # Protocol
    "ReadModelRepository",
    # Projection integration
    "ReadModelProjection",
    # Query building
    "Query",
    "Filter",
    # Exceptions
    "ReadModelError",
    "OptimisticLockError",
    "ReadModelNotFoundError",
    # In-memory implementation
    "InMemoryReadModelRepository",
    # PostgreSQL implementation
    "PostgreSQLReadModelRepository",
    # SQLite implementation
    "SQLiteReadModelRepository",
    # Schema generation
    "generate_schema",
    "generate_indexes",
    "generate_full_schema",
    "POSTGRESQL_TYPE_MAP",
    "SQLITE_TYPE_MAP",
]


def __getattr__(name: str) -> object:
    try:
        module_name = _MOVED[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    warnings.warn(
        f"eventsource.readmodels.{name} is deprecated; "
        f"import it from {module_name} instead. "
        f"eventsource.readmodels is removed in 0.8.0.",
        DeprecationWarning,
        stacklevel=2,
    )
    return getattr(importlib.import_module(module_name), name)


def __dir__() -> list[str]:
    return sorted(__all__)
```

`__all__` keeps its exact sixteen entries in their exact order and with their existing comments — the shim test asserts set equality against `_MOVED`, and A7's docs reference this list.

- [ ] **Step 6 (green): retarget `tests/unit/readmodels/test_exports.py`**

The spec is explicit that this file is retargeted, not deleted: it becomes the shim's export-completeness test. Rewrite it so each import goes through the *new* paths and one case asserts the shim still covers everything:

```python
"""Read-model public surface: new port/adapter paths, plus shim completeness."""

import pytest


def test_port_and_adapter_paths_export_the_public_names() -> None:
    from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
    from eventsource.ports.readmodels import (
        Filter,
        Query,
        ReadModel,
        ReadModelRepository,
    )

    assert hasattr(ReadModel, "table_name")
    assert hasattr(ReadModel, "field_names")
    assert hasattr(ReadModel, "custom_field_names")
    assert hasattr(ReadModel, "is_deleted")

    assert hasattr(ReadModelRepository, "get")
    assert hasattr(ReadModelRepository, "save")
    assert hasattr(ReadModelRepository, "find")

    q = Query()
    assert hasattr(q, "filters")
    assert hasattr(q, "order_by")
    assert hasattr(q, "limit")

    assert hasattr(Filter, "eq")
    assert hasattr(Filter, "ne")
    assert hasattr(Filter, "gt")
    assert hasattr(Filter, "in_")

    assert hasattr(InMemoryReadModelRepository, "model_class")


def test_legacy_package_still_covers_all_sixteen_names() -> None:
    import eventsource.readmodels

    assert len(eventsource.readmodels.__all__) == 16
    for name in eventsource.readmodels.__all__:
        with pytest.warns(DeprecationWarning):
            assert getattr(eventsource.readmodels, name) is not None
```

Other files under `tests/unit/readmodels/` that import from `eventsource.readmodels` will now emit `DeprecationWarning`s. If the project's pytest config turns warnings into errors, retarget those imports onto the new paths in this step — check `pyproject.toml`'s `[tool.pytest.ini_options] filterwarnings` first, and report which files you touched.

- [ ] **Step 7 (green): the Tier-0 contract entry**

In `pyproject.toml`, add to the "Tier 0 modules must not import sqlalchemy" `source_modules`, immediately after `"eventsource.adapters.memory.locks",` (added by A2):

```toml
    "eventsource.adapters.memory.readmodels",
```

If A2 has not yet run, place it after `"eventsource.adapters.memory.outbox",` instead and let the merge resolve. Nothing else in `[tool.importlinter]` changes here.

- [ ] **Step 8: verify and commit**

```bash
timeout 120 uv run pytest tests/unit/test_readmodels_shim.py tests/unit/readmodels/ -q
timeout 300 uv run pytest tests/integration/readmodels/ -m postgres -q   # needs Docker
uv run ruff check src/eventsource tests --fix && uv run ruff format src/eventsource tests
uv run mypy src/eventsource/ --config-file=pyproject.toml
uv run lint-imports
make check
```

Acceptance greps:
```bash
ls src/eventsource/readmodels/            # exactly: __init__.py
grep -rn "eventsource\.readmodels" src/   # only src/eventsource/readmodels/__init__.py
git diff -M --stat HEAD~0 -- src/eventsource/adapters/  # renames, not rewrites
```

```bash
git add src/eventsource/adapters/ src/eventsource/readmodels/ src/eventsource/__init__.py \
        tests/unit/test_readmodels_shim.py tests/unit/readmodels/ pyproject.toml
git add -u src/eventsource/readmodels/
git commit -m "refactor: move read model adapters into the adapters ring

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

**Acceptance (spec §2.8):** `src/eventsource/readmodels/` contains only `__init__.py`, which imports no sqlalchemy at module scope; `grep -rn "eventsource.readmodels" src/` matches only the shim; `eventsource.__all__` byte-identical; the 16 `sql_connection` call sites now sit under `adapters/`; `make check` green; `tests/integration/readmodels/` green with Docker up.

---

### Task A5: `ReadModelRepositoryConformance` and de-duplication of the per-backend suites

Spec §2.6, §2.7 third bullet. **Depends on A4.**

**Files:**
- Create: `src/eventsource/testing/conformance_ports/readmodels.py`
- Modify: `src/eventsource/testing/conformance_ports/_fixtures.py` (add `ConformanceReadModel`)
- Modify: `src/eventsource/testing/conformance_ports/__init__.py`
- Create: `tests/unit/adapters/test_memory_readmodels_conformance.py`
- Create: `tests/unit/adapters/test_sqlite_readmodels_conformance.py`
- Create: `tests/integration/readmodels/test_postgresql_readmodels_conformance.py`
- Modify: `tests/unit/readmodels/test_in_memory.py`, `test_postgresql.py`, `test_sqlite.py` (delete covered cases only)

**Interfaces:**
- Consumes A3's `eventsource.ports.readmodels` (`ReadModel`, `Query`, `Filter`, `OptimisticLockError`, `ReadModelNotFoundError`) and A4's three adapters + `generate_schema`.
- Produces: `ReadModelRepositoryConformance` in `eventsource.testing.conformance_ports`, and `ConformanceReadModel` in `conformance_ports/_fixtures.py`:
  ```python
  class ConformanceReadModel(ReadModel):
      name: str = "conformance"
      count: int = 0
  ```
  The abstract `store` fixture yields a repository **already bound to `ConformanceReadModel` with its table provisioned**. Bindings own provisioning; the suite stays adapter-free and dialect-free.
- `_fixtures.py`'s "no sqlalchemy" property is preserved: `ReadModel` is pydantic-only, imported from `eventsource.ports.readmodels`.

- [ ] **Step 1 (red): the three bindings, written before the suite exists**

`tests/unit/adapters/test_memory_readmodels_conformance.py`:

```python
"""Conformance tests for InMemoryReadModelRepository against the port suite."""

from collections.abc import AsyncIterator

import pytest

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.testing.conformance_ports import ReadModelRepositoryConformance
from eventsource.testing.conformance_ports._fixtures import ConformanceReadModel


class TestMemoryReadModelRepository(ReadModelRepositoryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[InMemoryReadModelRepository[ConformanceReadModel]]:
        yield InMemoryReadModelRepository(ConformanceReadModel, enable_tracing=False)
```

`tests/unit/adapters/test_sqlite_readmodels_conformance.py`:

```python
"""Conformance tests for SQLiteReadModelRepository against the port suite."""

from collections.abc import AsyncIterator

import aiosqlite
import pytest

from eventsource.adapters.sql.readmodel_schema import generate_schema
from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository
from eventsource.testing.conformance_ports import ReadModelRepositoryConformance
from eventsource.testing.conformance_ports._fixtures import ConformanceReadModel

pytestmark = pytest.mark.sqlite


class TestSQLiteReadModelRepository(ReadModelRepositoryConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[SQLiteReadModelRepository[ConformanceReadModel]]:
        async with aiosqlite.connect(":memory:") as conn:
            await conn.executescript(generate_schema(ConformanceReadModel, dialect="sqlite"))
            await conn.commit()
            yield SQLiteReadModelRepository(conn, ConformanceReadModel, enable_tracing=False)
```

`tests/integration/readmodels/test_postgresql_readmodels_conformance.py`:

```python
"""Conformance tests for PostgreSQLReadModelRepository against the port suite."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio
from sqlalchemy import text

from eventsource.adapters.postgresql.readmodels import PostgreSQLReadModelRepository
from eventsource.adapters.sql.readmodel_schema import generate_schema
from eventsource.testing.conformance_ports import ReadModelRepositoryConformance
from eventsource.testing.conformance_ports._fixtures import ConformanceReadModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = [pytest.mark.integration, pytest.mark.postgres]


class TestPostgreSQLReadModelRepository(ReadModelRepositoryConformance):
    @pytest_asyncio.fixture
    async def store(
        self, postgres_engine: AsyncEngine
    ) -> AsyncIterator[PostgreSQLReadModelRepository[ConformanceReadModel]]:
        table = ConformanceReadModel.table_name()
        async with postgres_engine.begin() as conn:
            await conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
            for statement in generate_schema(
                ConformanceReadModel, dialect="postgresql"
            ).split(";"):
                if statement.strip():
                    await conn.execute(text(statement))
        yield PostgreSQLReadModelRepository(
            postgres_engine, ConformanceReadModel, enable_tracing=False
        )
        async with postgres_engine.begin() as conn:
            await conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
```

Before writing the PostgreSQL binding, **read `tests/integration/readmodels/conftest.py`** and use whatever engine fixture it actually provides (it may be named other than `postgres_engine`, and it may already have a schema-provisioning helper worth reusing). Match the local conventions; the shape above is the contract, not the fixture names.

Also read `generate_schema`'s signature at `src/eventsource/adapters/sql/readmodel_schema.py:74` before writing these — confirm the keyword is `dialect` and whether it returns one statement or several, and adjust the provisioning lines accordingly.

Run: `timeout 120 uv run pytest tests/unit/adapters/test_memory_readmodels_conformance.py tests/unit/adapters/test_sqlite_readmodels_conformance.py -q`

Expected: FAIL at collection — `ImportError: cannot import name 'ReadModelRepositoryConformance' from 'eventsource.testing.conformance_ports'`.

- [ ] **Step 2 (green): add `ConformanceReadModel` to `_fixtures.py`**

Append to `src/eventsource/testing/conformance_ports/_fixtures.py`, and extend its module docstring's import list to name `eventsource.ports.readmodels`:

```python
class ConformanceReadModel(ReadModel):
    """Minimal read model used by `ReadModelRepositoryConformance`.

    Two custom fields only: one text, one integer -- enough to exercise
    filtering, ordering, and update-visibility without depending on any
    dialect's handling of decimals, JSON, or dates.
    """

    name: str = "conformance"
    count: int = 0
```

with `from eventsource.ports.readmodels import ReadModel` added to the imports. This keeps the module sqlalchemy-free.

- [ ] **Step 3 (green): write the conformance suite**

`src/eventsource/testing/conformance_ports/readmodels.py`. One method per case group from spec §2.6; imports limited to `eventsource.ports.readmodels`, the `_fixtures` model, pytest and stdlib. **No adapter import, no `generate_schema` import, no dialect knowledge.**

```python
"""Conformance suite for the `ReadModelRepository` port.

Subclass and provide a `store` fixture yielding a repository already bound
to `ConformanceReadModel` with its table provisioned -- bindings own
provisioning so this module stays adapter-free and dialect-free.

**Not pinned here**, deliberately: the ordering of `get_many` results (the
Protocol does not guarantee one), the resolution of `updated_at`, and any
dialect-specific type coercion. Those stay in the per-backend test modules.
"""

import asyncio
from abc import ABC, abstractmethod
from uuid import uuid4

import pytest

from eventsource.ports.readmodels import (
    Filter,
    OptimisticLockError,
    Query,
    ReadModelNotFoundError,
    ReadModelRepository,
)
from eventsource.testing.conformance_ports._fixtures import ConformanceReadModel

Repo = ReadModelRepository[ConformanceReadModel]


def _model(name: str = "conformance", count: int = 0) -> ConformanceReadModel:
    return ConformanceReadModel(id=uuid4(), name=name, count=count)


class ReadModelRepositoryConformance(ABC):
    """Conformance suite for `ReadModelRepository` implementations."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a repository bound to `ConformanceReadModel`, table provisioned."""
        raise NotImplementedError

    async def test_save_then_get_round_trips(self, store: Repo) -> None:
        model = _model(name="alpha", count=3)
        await store.save(model)
        loaded = await store.get(model.id)
        assert loaded is not None
        assert loaded.id == model.id
        assert loaded.name == "alpha"
        assert loaded.count == 3

    async def test_get_of_an_absent_id_returns_none(self, store: Repo) -> None:
        assert await store.get(uuid4()) is None

    async def test_save_upserts_and_advances_updated_at(self, store: Repo) -> None:
        model = _model(name="before")
        await store.save(model)
        first = await store.get(model.id)
        assert first is not None

        await asyncio.sleep(0.01)
        first.name = "after"
        await store.save(first)

        second = await store.get(model.id)
        assert second is not None
        assert second.name == "after"
        assert second.updated_at >= first.updated_at
        assert await store.count() == 1

    async def test_get_many_skips_missing_ids(self, store: Repo) -> None:
        present = _model(name="present")
        await store.save(present)
        found = await store.get_many([present.id, uuid4()])
        assert [m.id for m in found] == [present.id]

    async def test_save_many_persists_every_model(self, store: Repo) -> None:
        models = [_model(name=f"m{i}", count=i) for i in range(3)]
        await store.save_many(models)
        for model in models:
            assert await store.get(model.id) is not None

    async def test_exists_reflects_presence(self, store: Repo) -> None:
        model = _model()
        assert await store.exists(model.id) is False
        await store.save(model)
        assert await store.exists(model.id) is True

    async def test_delete_returns_whether_a_row_was_removed(self, store: Repo) -> None:
        model = _model()
        await store.save(model)
        assert await store.delete(model.id) is True
        assert await store.get(model.id) is None
        assert await store.delete(model.id) is False

    async def test_soft_delete_hides_from_get_but_not_from_get_deleted(
        self, store: Repo
    ) -> None:
        model = _model()
        await store.save(model)
        assert await store.soft_delete(model.id) is True
        assert await store.get(model.id) is None
        recovered = await store.get_deleted(model.id)
        assert recovered is not None
        assert recovered.id == model.id

    async def test_restore_makes_a_soft_deleted_model_visible_again(
        self, store: Repo
    ) -> None:
        model = _model()
        await store.save(model)
        await store.soft_delete(model.id)
        assert await store.restore(model.id) is True
        assert await store.get(model.id) is not None

    async def test_find_deleted_returns_only_soft_deleted_models(self, store: Repo) -> None:
        live = _model(name="live")
        gone = _model(name="gone")
        await store.save_many([live, gone])
        await store.soft_delete(gone.id)
        assert [m.id for m in await store.find_deleted()] == [gone.id]

    async def test_find_filters_on_equality(self, store: Repo) -> None:
        alpha = _model(name="alpha")
        beta = _model(name="beta")
        await store.save_many([alpha, beta])
        found = await store.find(Query(filters=[Filter.eq("name", "alpha")]))
        assert [m.id for m in found] == [alpha.id]

    async def test_find_orders_and_limits(self, store: Repo) -> None:
        await store.save_many([_model(name=f"m{i}", count=i) for i in range(3)])
        found = await store.find(
            Query(order_by="count", order_direction="desc", limit=2)
        )
        assert [m.count for m in found] == [2, 1]

    async def test_find_excludes_soft_deleted_models(self, store: Repo) -> None:
        live = _model(name="live")
        gone = _model(name="gone")
        await store.save_many([live, gone])
        await store.soft_delete(gone.id)
        assert [m.id for m in await store.find()] == [live.id]

    async def test_count_with_and_without_filters(self, store: Repo) -> None:
        await store.save_many([_model(name="alpha"), _model(name="beta")])
        assert await store.count() == 2
        assert await store.count(Query(filters=[Filter.eq("name", "alpha")])) == 1

    async def test_truncate_returns_the_count_and_empties_the_table(
        self, store: Repo
    ) -> None:
        live = _model(name="live")
        gone = _model(name="gone")
        await store.save_many([live, gone])
        await store.soft_delete(gone.id)
        assert await store.truncate() == 2
        assert await store.count() == 0
        assert await store.get_deleted(gone.id) is None

    async def test_save_with_version_check_increments_version(self, store: Repo) -> None:
        model = _model()
        await store.save(model)
        loaded = await store.get(model.id)
        assert loaded is not None
        before = loaded.version
        loaded.name = "bumped"
        await store.save_with_version_check(loaded)
        after = await store.get(model.id)
        assert after is not None
        assert after.version == before + 1

    async def test_save_with_version_check_rejects_a_stale_version(
        self, store: Repo
    ) -> None:
        model = _model()
        await store.save(model)
        first = await store.get(model.id)
        second = await store.get(model.id)
        assert first is not None and second is not None

        first.name = "winner"
        await store.save_with_version_check(first)

        second.name = "loser"
        with pytest.raises(OptimisticLockError):
            await store.save_with_version_check(second)

    async def test_save_with_version_check_rejects_an_absent_model(
        self, store: Repo
    ) -> None:
        with pytest.raises(ReadModelNotFoundError):
            await store.save_with_version_check(_model())


__all__ = ["ReadModelRepositoryConformance"]
```

Before finalizing, **read the three adapters' actual semantics** for `truncate` (does its return count include soft-deleted rows?), `save_with_version_check` (does it bump `version` itself or expect the caller to?), and `ReadModel.version`'s default. The spec's §2.6 list is the required coverage; the exact assertions must match what all three adapters honestly do today, since this is a pure structural slice and no adapter behavior changes here. If two adapters genuinely disagree on a case, **that is a finding, not a thing to weaken the suite around** — report it and drop the case rather than writing an assertion that hides the divergence.

- [ ] **Step 4 (green): register the suite**

In `src/eventsource/testing/conformance_ports/__init__.py`: import (alphabetically, after `outbox`, before `snapshots`), add `"ReadModelRepositoryConformance"` to `__all__`, and add it to the docstring's suite list.

```python
from eventsource.testing.conformance_ports.readmodels import ReadModelRepositoryConformance
```

- [ ] **Step 5 (green): de-duplicate the per-backend unit files**

Delete from `tests/unit/readmodels/test_in_memory.py`, `test_postgresql.py`, and `test_sqlite.py` only the cases the suite now covers. **Keep** everything backend-specific: SQLite TEXT/JSON coercion, PostgreSQL JSONB behavior, connection/engine handling, `__repr__`, `model_class`, `clear()` (memory-only), tracing assertions.

Working method — for each candidate deletion, name the suite method that covers it. Write that mapping into the commit message body as a `deleted -> covered by` list. The A5 reviewer's stated focus is exactly this: that the deleted cases are genuinely covered and not merely dropped. If you cannot name a covering suite method for a case, **keep the case**.

`test_soft_delete.py` and `test_optimistic_locking.py` are cross-cutting files that assert the same semantics across backends; the suite now covers their core. Reduce them the same way — by named mapping — or leave them entirely if the mapping is not clean. Both outcomes are acceptable; a silent deletion is not.

- [ ] **Step 6: verify and commit**

```bash
timeout 120 uv run pytest tests/unit/adapters/test_memory_readmodels_conformance.py \
    tests/unit/adapters/test_sqlite_readmodels_conformance.py tests/unit/readmodels/ -q
timeout 300 uv run pytest tests/integration/readmodels/ -m postgres -q   # needs Docker
uv run ruff check src/eventsource tests --fix && uv run ruff format src/eventsource tests
uv run mypy src/eventsource/ --config-file=pyproject.toml
uv run lint-imports
make check
```

Confirm the suite is adapter-free before committing:
```bash
grep -n "adapters" src/eventsource/testing/conformance_ports/readmodels.py   # no output
```

```bash
git add src/eventsource/testing/conformance_ports/ tests/unit/adapters/ \
        tests/integration/readmodels/ tests/unit/readmodels/
git commit -m "test: add read model repository conformance suite

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

**Acceptance (spec §2.8):** `ReadModelRepositoryConformance` passes against all three adapters; the suite file imports no adapter; coverage does not drop for the three repository modules; `make check` green.

---

### Task A6: move `engine.py` to `adapters/_sql/engine.py`

Spec §3 in full. Chain root, independent of A1–A5 and A7-blocking only.

**Files:**
- Create: `src/eventsource/adapters/_sql/engine.py` (via `git mv`)
- Delete: `src/eventsource/engine.py`
- Modify: `src/eventsource/__init__.py` (line 103)
- Modify: `pyproject.toml` (`[tool.mutmut] only_mutate` line 204)
- Modify: `cosmic-ray/engine.toml` (line 20)
- Modify: `scripts/_mutmut_configure.py` (line 24)
- Modify: `scripts/mutation.sh` (line 9 comment)
- Modify: `tests/unit/test_engine.py`, `tests/conftest.py`, and five other test importers
- Modify: `docs/development/mutation-testing.md`, `docs/core-surface.md`, `docs/adrs/0008-mutation-testing-tool-selection.md`

**Interfaces:**
- Produces: `eventsource.adapters._sql.engine.create_async_engine(url: str, **kwargs: Any) -> AsyncEngine` and `SQLITE_PRAGMAS: dict[str, str | int]` — both unchanged in signature and behavior.
- Preserves: `eventsource.create_async_engine`, the canonical public name. `SQLITE_PRAGMAS` is **not** added to the top-level surface (it is not exported today).
- `src/eventsource/adapters/_sql/__init__.py` stays import-free. Adding a convenience re-export there would reintroduce the eager cost this placement was chosen to avoid.
- Consumed by no other task in this slice. A7 rewrites the docs rows this task's path change makes stale in `docs/core-surface.md`; A6 does the mechanical path substitution there, A7 rewrites the surrounding prose.

- [ ] **Step 1 (red): pin the ruling**

Append to `tests/unit/test_engine.py`:

```python
def test_module_lives_under_sql_adapters():
    """`create_async_engine` is adapter-ring content (ADR 0029).

    It constructs a SQLAlchemy AsyncEngine and registers dialect-specific
    driver listeners, so it belongs under `adapters/`. `_sql/` rather than
    `sql/` because `adapters/_sql/__init__.py` is import-free while
    `adapters/sql/__init__.py` eagerly imports three modules, one of which
    reaches into `application/projections/` -- the front-door import chain
    must not widen while relocating the module that chain names.
    """
    assert create_async_engine.__module__ == "eventsource.adapters._sql.engine"
```

Run: `timeout 120 uv run pytest tests/unit/test_engine.py -q -k module_lives`

Expected: FAIL — `AssertionError: assert 'eventsource.engine' == 'eventsource.adapters._sql.engine'`.

(This is a pure relocation with no behavioral deficiency to write a failing test against, so the red test pins the ruling itself.)

- [ ] **Step 2 (green): move the module**

```bash
git mv src/eventsource/engine.py src/eventsource/adapters/_sql/engine.py
```

**No code edits.** The only change to the file is one sentence appended to the module docstring, after the "See https://docs.sqlalchemy.org/..." line:

```
Why ``adapters/_sql/`` and not ``adapters/sql/``: ``adapters/_sql/__init__.py``
is import-free, while ``adapters/sql/__init__.py`` eagerly imports
``checkpoints``, ``dlq``, and ``projection``, and ``projection`` reaches into
``application/projections/``. Placing a leaf engine factory there would drag
the application projection ring into the front-door import chain that
``docs/core-surface.md``'s finding 12 exists to narrow. The public name is and
remains ``eventsource.create_async_engine``; the module path was never the
advertised surface. See ADR 0029.
```

`SQLITE_PRAGMAS`, the three module-level helpers, `_configure_sqlite`, `create_async_engine`, and `__all__` are untouched.

- [ ] **Step 3 (green): retarget the front door**

`src/eventsource/__init__.py` line 103:
```python
from eventsource.engine import create_async_engine
```
→
```python
from eventsource.adapters._sql.engine import create_async_engine
```
The comment on line 102 stays. `__all__` line 234 is untouched.

Do **not** add anything to `src/eventsource/adapters/_sql/__init__.py`.

- [ ] **Step 4 (green): mutation configuration — all four locations**

The A6 reviewer's stated focus is that all four were updated, not just `pyproject.toml`.

1. `pyproject.toml` line 204, in `[tool.mutmut] only_mutate`:
   `"src/eventsource/engine.py",` → `"src/eventsource/adapters/_sql/engine.py",`
2. `cosmic-ray/engine.toml` line 20:
   `module-path = "src/eventsource/engine.py"` → `module-path = "src/eventsource/adapters/_sql/engine.py"`
   Its `test-command` already points at `tests/unit/test_engine.py`, which does not move — leave it. The header comment at line 5 says "the shape of `engine.py`'s `@event.listens_for` listeners"; that prose is still accurate, leave it.
3. `scripts/_mutmut_configure.py` line 24, in `MODULES`:
   ```python
   "engine": ("src/eventsource/adapters/_sql/engine.py", "tests/unit/test_engine.py"),
   ```
   The selector key `"engine"` is kept. **Do not touch** the `dialect`, `checkpoint`, and `json` entries — three of them name `src/eventsource/repositories/...`, a package deleted in an earlier slice, and repairing that is out of scope. Note it in your report.
4. `scripts/mutation.sh` line 9, a usage comment:
   `#   scripts/mutation.sh engine       # just src/eventsource/engine.py` → `... # just src/eventsource/adapters/_sql/engine.py`
   Lines 24 and 48 (`VALID=(engine dialect json checkpoint dlq all)` and the `for m in engine dialect json` loop) contain **selector names, not paths** — the spec names them as needing a path update; they do not have one. Leave both unchanged and flag the discrepancy in your report.

Verify the selector still resolves:
```bash
uv run python scripts/_mutmut_configure.py engine && grep -n "only_mutate" -A 3 pyproject.toml
git checkout pyproject.toml   # _mutmut_configure rewrites in place; restore, then re-apply step 4.1
```
Better: do this check on a copy under `$CLAUDE_JOB_DIR/tmp` rather than mutating the shared `pyproject.toml`.

- [ ] **Step 5 (green): retarget the test importers**

`tests/unit/test_engine.py` — two lines:
- line 10: `from eventsource.engine import create_async_engine` → `from eventsource import create_async_engine`
- line 116: `import eventsource.engine as engine_module` → `import eventsource.adapters._sql.engine as engine_module`
  (this one must reach the *module*, because the test monkeypatches `engine_module.SQLITE_PRAGMAS`; `from eventsource import ...` cannot express that)

**Nothing else in `tests/unit/test_engine.py` changes** other than Step 1's appended test. That is the behavior-preservation evidence for the move.

Every other importer takes the docs-recommended path, `from eventsource import create_async_engine`:
- `tests/conftest.py` lines 621 and 680
- `tests/unit/adapters/test_sql_checkpoint_tracing.py` line 301
- `tests/unit/adapters/test_checkpoint_position.py` line 297
- `tests/unit/adapters/test_memory_dlq.py` lines 707 and 1194
- `tests/unit/adapters/test_memory_checkpoints.py` lines 350 and 926
- `tests/unit/adapters/test_sqlite_conformance.py` line 41 (keep its `# noqa: E402`)
- `tests/integration/repositories/test_dlq.py` lines 482 and 523

(The spec says "five adapter/repository test modules"; the actual count at 8e9ccd4 is six besides `conftest.py` and `test_engine.py`. Use the list above.)

- [ ] **Step 6 (green): documentation path sweep**

- `docs/development/mutation-testing.md` lines 25, 49, 394: `src/eventsource/engine.py` → `src/eventsource/adapters/_sql/engine.py` in all three.
- `docs/core-surface.md` lines 178, 254, 286, 321: path substitution only. Lines 254 and 286 keep their substance — the front door still imports sqlalchemy at module level, with the module renamed. A7 rewrites the surrounding prose; A6 only makes the paths true.
- `docs/adrs/0008-mutation-testing-tool-selection.md` line 4 names `src/eventsource/engine.py` in a historical list. **Leave the historical text** and append a parenthetical:
  `(now \`src/eventsource/adapters/_sql/engine.py\`)`.
  ADRs record what was decided then; they are not retro-edited.

- [ ] **Step 7: verify and commit**

```bash
timeout 120 uv run pytest tests/unit/test_engine.py -q
timeout 120 uv run pytest tests/unit/adapters/ tests/unit/ -q
uv run ruff check src/eventsource tests --fix && uv run ruff format src/eventsource tests
uv run mypy src/eventsource/ --config-file=pyproject.toml
uv run lint-imports
make check
```

Acceptance greps:
```bash
test ! -e src/eventsource/engine.py && echo "deleted"
grep -rn "eventsource\.engine\|eventsource/engine\.py" src/ tests/ scripts/ pyproject.toml cosmic-ray/
# expected: no output. (docs/ still matches ADR 0008's historical note, by design.)
git diff --stat tests/unit/test_engine.py   # small: 2 import lines + 1 appended test
```

```bash
git add src/eventsource/adapters/_sql/engine.py src/eventsource/__init__.py \
        pyproject.toml cosmic-ray/engine.toml scripts/_mutmut_configure.py scripts/mutation.sh \
        tests/unit/test_engine.py tests/conftest.py tests/unit/adapters/ \
        tests/integration/repositories/test_dlq.py \
        docs/development/mutation-testing.md docs/core-surface.md \
        docs/adrs/0008-mutation-testing-tool-selection.md
git add -u src/eventsource/
git commit -m "refactor: move engine factory to adapters/_sql

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

**Acceptance (spec §3.6):** `src/eventsource/engine.py` gone; the grep above empty except ADR 0008's historical note; `eventsource.create_async_engine` behaves identically and `tests/unit/test_engine.py` passes with only its import lines changed; `scripts/mutation.sh engine` resolves against the new path; `make check` green.

---

### Task A7: ADR 0029, CHANGELOG, the new import-linter contract, and the documentation sweep

Spec §4 in full, plus §2.4's follow-up backlog item and §3.5's lazy-`__init__` note. **Depends on A1–A6.** This task writes no source code and moves no module.

**Files:**
- Create: `docs/adrs/0029-locks-readmodels-and-engine-rings.md`
- Modify: `docs/adrs/index.md`, `docs/adrs/0023-*.md` (Status pointer only)
- Modify: `CHANGELOG.md`
- Modify: `pyproject.toml` (new contract + two comments)
- Modify: `docs/core-surface.md`, `docs/api/locks.md`, `docs/api/readmodels.md`, `docs/api/projections.md`, `docs/api/index.md`
- Modify (import-path sweep): `docs/architecture.md`, `docs/guides/distributed-locks.md`, `docs/guides/live-migration.md`, `docs/guides/error-handling.md`, `docs/explanation/schema-design.md`, `docs/development/testing.md`, `docs/index.md`
- Modify: `BACKLOG.md`

**Interfaces:** none produced or consumed. This task documents the six that preceded it.

- [ ] **Step 1: write ADR 0029**

`docs/adrs/0029-locks-readmodels-and-engine-rings.md`. Match the house ADR format — read `docs/adrs/0026-*.md` for the current shape (Status / Context / Decision / Consequences / Alternatives Considered) before writing. Required content, per spec §4.1:

- **Status: Accepted.** It **amends ADR 0023** with spec §1.2's paragraph verbatim in substance: the port describes the *shape of the dependency*, not an equivalence of *distributed* semantics; ADR 0023's single-primitive argument stands and its "When Not to Use This" section is untouched; the in-memory adapter is a conforming implementation of the narrower contract, restricted by its docstring to single-process testing. ADR 0023 is **not** superseded. Sibling of 0024/0026 — the same split applied to the last two pre-ring packages.
- The `DistributedLock` / `LockRegistry` / `LockManager` ISP split, along the two real consumer groups, and why `acquire` is typed as returning `AbstractAsyncContextManager[LockInfo]` rather than declared `async def` (implementations use `@asynccontextmanager`, whose runtime type is exactly that).
- The `ports/readmodels/` subpackage decision with both rejected alternatives (flat `ports/readmodels.py`; `domain/readmodel.py` for `ReadModel`) — spec §2.2.
- The `schema.py` → `adapters/sql/readmodel_schema.py` ruling and its consequence: the module loses its Tier-0 sqlalchemy guard because that guard asserted the wrong property about it — spec §2.3.
- The `engine.py` → `adapters/_sql/engine.py` ruling with both rejected alternatives (dissolve into `connection.py`; `adapters/sql/engine.py`), including the eager-import-chain argument — spec §3.2.
- **Two recorded exceptions, each labeled as such:**
  (a) the read-model exception trio staying out of `exceptions.py` while the `OptimisticLockError` name collision is unresolved (§2.4), with the follow-up backlog item referenced by name;
  (b) `LockAcquisitionError` / `LockNotHeldError` rebased onto `EventSourceError` — the one semantic change in the slice, widening only (§1.3).
- What each deprecation shim covers and that both are removed in 0.8.0.

Then: add the 0029 row to `docs/adrs/index.md` (match the surrounding rows' format), and add an "Amended by ADR 0029" pointer to ADR 0023's **Status** section. Do not rewrite ADR 0023's Decision — ADR bodies are immutable records.

- [ ] **Step 2: CHANGELOG under `## [Unreleased]`**

Read the existing "Projection persistence re-homed" and "Outbox ring migration" entries first and match their shape exactly (each is a paragraph plus a from→to table plus the statement that top-level `eventsource` imports are unaffected).

**Added:**
- `eventsource.ports.locks` — `DistributedLock`, `LockRegistry`, `LockManager`, `LockInfo`, `migration_lock_key`.
- `eventsource.adapters.memory.locks.InMemoryLockManager` — test-scoped: single-process only, no crash release, no fairness.
- `eventsource.ports.readmodels` — `ReadModel`, `Query`, `Filter`, `ReadModelRepository`, and the read-model exception family.
- `DistributedLockConformance` and `ReadModelRepositoryConformance` in `eventsource.testing.conformance_ports`.

**Changed:**
- The locks re-home paragraph + from→to table (spec §1.3's five names).
- The readmodels re-home paragraph + from→to table (spec §2.5's nine rows).
- The `engine.py` move: the module moved to `eventsource/adapters/_sql/engine.py`; `eventsource.create_async_engine` is unchanged; anyone importing `eventsource.engine` directly — which the docs never told them to do — should import from `eventsource` instead.
- **Called out separately, not buried in the locks paragraph:** `LockAcquisitionError` and `LockNotHeldError` now subclass `EventSourceError` and live in `eventsource.exceptions`. Widening only — every existing `except LockAcquisitionError` and `except Exception` still catches; the newly-catching clause is `except EventSourceError`, which caught nothing lock-related before.

**Deprecated:**
- `eventsource.locks` and `eventsource.readmodels` as import paths. Every name still resolves, each with a `DeprecationWarning` naming its replacement. Both packages are removed in 0.8.0. No names are removed in this release.

There is **no Removed section** for this slice: deleting `src/eventsource/engine.py` removes no public name and is covered under Changed.

- [ ] **Step 3: the new import-linter contract and the two comments**

Add to `pyproject.toml`, after the existing "Application ring must not import adapters" contract:

```toml
[[tool.importlinter.contracts]]
name = "Ports must not import adapters, application, or bus"
type = "forbidden"
source_modules = ["eventsource.ports"]
forbidden_modules = [
    "eventsource.adapters",
    "eventsource.application",
    "eventsource.bus",
    "eventsource.migration",
]
```

Rationale to record in the trailing comment block: slice A created the first port with a sibling that moved to `adapters/` (`ports/readmodels/` and `adapters/sql/readmodel_schema.py`), and an accidental `from eventsource.adapters.sql.readmodel_schema import generate_schema` inside `ports/readmodels/model.py` would be a real regression the existing sqlalchemy-only contract cannot catch — `readmodel_schema.py` imports no sqlalchemy.

Two comments to add, matching the style of the existing explanatory block at the end of the `[tool.importlinter]` section:

1. On the Tier-0 contract: `eventsource.readmodels.schema` lost its "must not import sqlalchemy" guard **by design** when it became `eventsource.adapters.sql.readmodel_schema` (ADR 0029 §2.3) — the guard asserted the wrong property about a module that emits dialect-specific DDL. The guard was not silently dropped.
2. On "Application ring must not import adapters": the `eventsource.locks` entry in `forbidden_modules` stays correct while the shim exists — the shim resolves to adapters — and is deleted when the shim is removed in 0.8.0.

"Infrastructure backends must not import each other" needs no change: it names the `adapters.postgresql` / `adapters.sqlite` / `adapters.memory` packages, so the new `locks.py` and `readmodels.py` modules inside them are covered automatically.

Run `uv run lint-imports` after this step. If the new contract fails, **a port is importing outward and that is a real finding** — report it, do not weaken the contract.

- [ ] **Step 4: `docs/api/locks.md`**

Rewrite the import-path sections at `:16`, `:133`, `:154-168`, `:189`, `:197`, `:210`, and every code sample, onto `eventsource.ports.locks` / `eventsource.adapters.postgresql.locks` / `eventsource.exceptions`. Two constraints:

- The "no top-level re-export" statement at `:189` — "They are not reachable via `eventsource`" — **stays true and stays**. This slice adds no top-level lock names.
- Add a section documenting `InMemoryLockManager` that leads with what it does *not* guarantee: no cross-process exclusion, no release on crash, no fairness; single event loop, single process; for tests.

Mention the `eventsource.locks` deprecation and the 0.8.0 removal inline.

- [ ] **Step 5: `docs/api/readmodels.md`, `docs/api/projections.md`, `docs/api/index.md`**

- `docs/api/readmodels.md`: import paths onto `eventsource.ports.readmodels` and the three adapter modules throughout; note the `eventsource.readmodels` deprecation and 0.8.0 removal.
- `docs/api/projections.md`: the `ReadModelProjection` section notes its new `adapters/sql/` home. `eventsource.ReadModelProjection` is unchanged and stays the recommended import.
- `docs/api/index.md` lines 69, 240, 353, 370, 438, 680, 796: module table and subsystem lists updated for both packages, with the deprecation of the old paths noted inline. Re-read these lines before editing — earlier tasks may have shifted them.

- [ ] **Step 6: `docs/core-surface.md`**

- Rows `:178` (engine), `:193`–`:195` (readmodels), `:201`–`:204` (migration/locks): rewritten for the new paths. A6 already substituted the engine paths; this step rewrites the surrounding prose.
- The `readmodels/` "splits down the middle" paragraph at `:204` is **replaced by a statement that the split has been performed**, naming ADR 0029.
- `:240`/`:242` — the ADR 0026 §4 accepted-debt item — updated to record `readmodels/postgresql.py` as **resolved** (its 16 `sql_connection` call sites now sit under `adapters/`) and the four `migration/repositories/` modules as **still open**.
- The lines A6 touched at `:254`/`:286` keep their substance: the front door still imports sqlalchemy at module level, now via `eventsource.adapters._sql.engine` and `eventsource.adapters.postgresql`.

- [ ] **Step 7: the remaining documentation sweep**

`docs/architecture.md`, `docs/guides/distributed-locks.md`, `docs/guides/live-migration.md`, `docs/guides/error-handling.md`, `docs/explanation/schema-design.md`, `docs/development/testing.md`, `docs/index.md` — import-path sweep. `docs/guides/error-handling.md` also needs the `EventSourceError` rebasing noted if it documents the lock exceptions' hierarchy.

Gate:
```bash
grep -rn "eventsource\.locks\|eventsource\.readmodels\|eventsource\.engine" docs/
```
must return only: the ADRs (historical), the migration/deprecation notes, and `docs/superpowers/` (specs and plans, which are historical records — do not edit them).

`mkdocs.yml` nav: no new pages are added by this slice, so nav is unchanged. **Still run the nav-completeness check** — a strict build does not catch omissions:
```bash
uv run mkdocs build --strict
```

- [ ] **Step 8: `BACKLOG.md`**

- Strike the three entries this slice closes: "Migrate locks/ to ports/adapters (P2)", "Split readmodels/ into port + adapter (P2)", "Decide engine.py's ring placement (P3)".
- File two new entries, in the file's existing entry format:
  - **P2 — "Resolve the duplicate `OptimisticLockError` name (readmodels vs core)."** Two unrelated classes share the name: `eventsource.ports.readmodels.OptimisticLockError(ReadModelError)` with `(model_id, expected_version, actual_version=None)`, and `eventsource.exceptions.OptimisticLockError(EventSourceError)` with `(aggregate_id, expected_version, actual_version)`. They do not catch each other. Proposed resolution: rename the read-model one to `ReadModelVersionConflictError` with a deprecation alias. **The collision predates this slice** — record that, so a future reader does not attribute it to slice A.
  - **P3 — "Remove the `eventsource.locks` and `eventsource.readmodels` deprecation shims"**, scheduled for 0.8.0, alongside the existing `bus/` shim removal entry if there is one. Include: delete both `__init__.py` shims, delete the `eventsource.locks` entry from the "Application ring must not import adapters" `forbidden_modules`, and delete the two shim test modules.
- Update the lazy-`__init__` entry's import-chain notes per spec §3.5: the chain is now `eventsource/__init__` → `adapters/_sql/engine` → `sqlalchemy` (one module deeper, no cheaper) plus `eventsource.adapters.postgresql`. Slice A added nothing to that chain — the locks and readmodels adapters are reached only through their shims' lazy `__getattr__`, and `ReadModelProjection`'s top-level re-export already pulled sqlalchemy in via `adapters/sql/projection.py` before this slice. **Do not expand that entry's scope.**

- [ ] **Step 9: verify and commit**

```bash
uv run lint-imports
uv run mkdocs build --strict
make check
```

Coverage check of spec §1–§4 before committing — for each requirement, name the task that landed it. Anything unmapped is a gap: report it rather than absorbing it into A7.

```bash
git add docs/adrs/ CHANGELOG.md pyproject.toml docs/core-surface.md docs/api/ \
        docs/architecture.md docs/guides/ docs/explanation/schema-design.md \
        docs/development/testing.md docs/index.md BACKLOG.md
git commit -m "docs: adr 0029 and documentation for structure slice a

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

**Acceptance:** ADR 0029 exists and is indexed; ADR 0023 carries an "Amended by" pointer; CHANGELOG has the four sections above with the exception rebasing called out separately; the new import-linter contract passes; the docs grep gate is clean; `mkdocs build --strict` succeeds; `make check` green.

---

## Reviewer focus per task (spec §5)

| Task | What the reviewer checks first |
|---|---|
| A1 | The suite file imports no adapter; `InMemoryLockManager`'s docstring leads with what it does not guarantee; the two knowingly-temporary imports are reported, not hidden |
| A2 | `git diff tests/locks/test_postgresql_locks.py` shows **only** import lines changed; the exception rebasing is widening-only; the shim defines `__dir__` |
| A3 | `ports/readmodels/` imports no sqlalchemy in a fresh interpreter; `eventsource.readmodels` still exports all sixteen names |
| A4 | `git diff -M --stat` shows **pure renames** — no moved module body changed apart from imports; `eventsource.__all__` byte-identical; the two lazy imports in `readmodel_projection.py` stayed lazy |
| A5 | The deleted per-backend cases are **genuinely covered** by the suite and not merely dropped — check the `deleted -> covered by` mapping in the commit message |
| A6 | **All four** mutation-config locations updated (`pyproject.toml`, `cosmic-ray/engine.toml`, `scripts/_mutmut_configure.py`, `scripts/mutation.sh`), not just `pyproject.toml` |
| A7 | Every §1–§4 spec requirement maps to a landed task; ADR 0023's Decision was not retro-edited; the two recorded exceptions are labeled as exceptions |
