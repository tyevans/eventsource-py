# Legacy Store Retirement — Slice (a): Leaf Consumers

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the four leaf consumers of the legacy `EventStore` ABC — `AggregateRepository` (via a new composed `AggregateStore` port), `SyncEventStoreAdapter`, `InMemoryTestHarness`, and `bench/` — onto the `eventsource.ports` store surface. Nothing is deleted: `src/eventsource/stores/` stays in place and green, and every other legacy consumer (subscriptions, migration, top-level `__init__`) is untouched by this slice.

**Architecture:** `ports/store.py` gains `AggregateStore` (`EventAppender` + `StreamReader`), the narrowest port an aggregate repository needs. `AggregateRepository` types against it and translates its three legacy calls per the spec's semantic map: `append_events` → `append(StreamId, events, ExpectedVersion.exact(...))`, `get_events(from_version=n)` → `read_stream(stream, StreamReadOptions(from_version=n+1))`, and the existence/version probes → `get_stream_version`. `SyncEventStoreAdapter` keeps its `_run_sync` loop machinery and timeout handling verbatim but exposes port-shaped methods over a `FullEventStore`. `InMemoryTestHarness` swaps `stores.in_memory.InMemoryEventStore` for `adapters.memory.MemoryEventStore`. `bench/` retypes to `FullEventStore` and constructs the three adapters.

**Tech Stack:** Python 3.13, pydantic v2, sqlalchemy 2 (adapters only), pytest + pytest-asyncio + hypothesis, mypy strict, ruff, import-linter, mutmut.

**Spec:** `docs/superpowers/specs/2026-07-31-legacy-store-retirement-design.md` (slice (a), §3, §6, §7)

## Global Constraints

- **Unreleased software — no shims, no back-compat aliases.** Per the spec: consumers are retyped onto the ports outright. Do not add `append_events_sync`-style forwarding methods, do not keep a `get_events`-shaped helper on the repository, do not alias `AggregateStore` to the ABC. The `_sync` method-name suffixes on `SyncEventStoreAdapter` die; they are not deprecated.
- **Nothing is deleted in this slice.** `src/eventsource/stores/` (all eight modules) stays in place and must remain green. `LegacyStoreAdapter` is the authoritative executable translation reference (`src/eventsource/stores/legacy.py`) and survives until slice (d). If a translation rule here disagrees with `legacy.py`, `legacy.py` wins for the mechanics and the spec wins for the deliberate deltas — report the disagreement rather than silently choosing.
- **Test discipline.** Implementers run ONLY the test commands listed in the task they are executing — never the full suite, never `make check`. The orchestrator runs the full suite after each task and dispatches fixes.
- **Grep sweeps include `bench/`.** Every verification grep covers `src/`, `tests/`, and `bench/`. (`examples/` imports only from top-level `eventsource`, whose exports this slice leaves untouched, so it is unaffected — but the final sweep in Task 7 includes it to keep that true.)
- **Path-scoped `git add` only.** Other agents are working concurrently in this worktree. Never `git add -A` and never `git add .`; stage exactly the files the task names. If git reports `index.lock` contention, wait 5 seconds and retry.
- **Commit messages** follow `.claude/rules/commits.md` — `<type>: <lowercase description>` — and every commit ends with the trailer:

  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

- **Red/green TDD.** Every task that changes behavior writes or edits its failing test first, observes the failure, then implements. Steps are ordered so the red step precedes the green one; do not reorder them.
- **Hypothesis property tests** where this plan names them (Task 2's version-round-trip property). Do not add speculative property tests elsewhere in this slice — the spec explicitly declines a property test for the sync adapter's loop dispatch, whose machinery is unchanged.
- **mypy strict:** all new and modified code fully annotated. `uv run mypy src/eventsource/ --config-file=pyproject.toml` is listed per task.
- Pre-commit hooks run on commit (ruff, mypy, import-linter). A task is not done until its commit succeeds.

### Behavior deltas this slice makes visible

These are spec decisions (§1.1, §1.2, §1.6, §4.4, ADR 0016 amendment), not implementation choices. Each is repeated in the task that first encounters it; they are collected here so no implementer has to infer them.

| Delta | Legacy | Ports | Who feels it in slice (a) |
|---|---|---|---|
| `from_version` meaning | "skip the first n events" | inclusive 1-based version | `AggregateRepository.load` (Task 2): `n > 0` → `from_version=n+1`; `n == 0` → no options |
| Append position | `AppendResult.global_position` = position of the **last** appended event | `AppendResult.position` = position of the **first** appended event | Nothing in slice (a) reads it. `AggregateRepository` ignores the result's position entirely; `bench` must not start reading it. Only `migration/bulk_copier.py` (slice c) does position arithmetic. |
| Append conflict signalling | `AppendResult.success` / `.conflict` fields | raises `OptimisticLockError`; no such fields | Task 2 (repository `if result.success:` branch collapses) and Task 5 (`bench/scenarios/stores.py:_contended_append` reads `result.conflict`) |
| Duplicate `event_id` | memory/postgres **silently skip** the already-stored id | adapters raise `DuplicateEventError` | No slice (a) consumer relies on the skip (verified: repository always appends fresh uncommitted events; bench mints new ids per iteration). If a retargeted test starts failing with `DuplicateEventError`, the test was relying on the skip — rewrite it, do not re-add skipping. |
| Empty append batch | returns `AppendResult.successful(expected_version)` | adapters raise `ValueError` | `AggregateRepository.save` early-returns on empty uncommitted events at `repository.py:497-499` — **re-verify that guard survives your edit** (Task 2 Step 5). |
| `stored_at` on the memory store | legacy fabricated `stored_at = event.occurred_at` | memory adapter stamps real `datetime.now(UTC)` | Any retargeted test asserting `stored_at == occurred_at` (Tasks 3, 6) must change. Watch for time-frozen tests failing in non-obvious places (spec §11 risk 6). |
| Store-level spans | legacy stores emit `inmemory_event_store.*` / `sqlite_event_store.*` / `postgresql_event_store.*` spans | adapters emit **none** (accepted loss, ADR 0016 amendment) | Task 2: `tests/integration/observability/test_tracing_integration.py` cases asserting store spans must be **deleted**, not retargeted. Repository-level spans (`eventsource.repository.*`) are unaffected and must keep passing. |
| Adapter constructors are not drop-in | see spec §4.4 | postgres takes `engine` not `session_factory`; sqlite self-initializes (no `initialize()`, no `async with`); memory takes no `enable_tracing` | Tasks 3, 5, 6 |

---

### Task 1: Add the `AggregateStore` port

**Files:**
- Modify: `src/eventsource/ports/store.py`, `src/eventsource/ports/__init__.py`, `src/eventsource/__init__.py`
- Modify: `tests/unit/ports/test_store_ports.py` (or create it if absent — check `ls tests/unit/ports/` first and follow the existing naming), `tests/unit/test_public_api.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces (used by Tasks 2, 5, 6): `eventsource.ports.store.AggregateStore`, re-exported as `eventsource.ports.AggregateStore` and `eventsource.AggregateStore`.

- [ ] **Step 1 (red): assert the port exists and composes correctly**

Add to the ports store-protocol test module (create `tests/unit/ports/test_store_ports.py` only if no existing module covers `ports/store.py` — check first, and extend rather than duplicate):

```python
from eventsource.ports.store import (
    AggregateStore,
    EventAppender,
    FullEventStore,
    StreamReader,
)


class TestAggregateStorePort:
    def test_composes_appender_and_stream_reader(self) -> None:
        bases = AggregateStore.__mro__
        assert EventAppender in bases
        assert StreamReader in bases

    def test_does_not_require_feed_or_lookup_or_category(self) -> None:
        members = set(AggregateStore.__protocol_attrs__)
        assert members == {
            "append",
            "max_append_batch",
            "read_stream",
            "get_stream_version",
        }

    def test_full_event_store_is_assignable_to_aggregate_store(self) -> None:
        def takes_aggregate_store(store: AggregateStore) -> AggregateStore:
            return store

        def give(store: FullEventStore) -> AggregateStore:
            return takes_aggregate_store(store)

        assert give is not None  # compile-time assertion; mypy is the real check
```

Run: `uv run pytest tests/unit/ports/ -q` — Expected: FAIL (`ImportError: cannot import name 'AggregateStore'`).

- [ ] **Step 2 (green): define the port**

In `src/eventsource/ports/store.py`, insert immediately after `CategoryQuery` and before `FullEventStore` (composed ports stay together, narrowest first):

```python
class AggregateStore(EventAppender, StreamReader, Protocol):
    """What an aggregate repository needs: append plus stream read/version.

    Narrower than `FullEventStore` on purpose -- a repository never reads
    the global feed, never queries a category, and never probes for an
    individual event id, so it must not type-require those capabilities
    (ISP; see `.claude/rules/architecture.md`).
    """
```

~~Note for implementers writing test doubles against this port: `EventAppender` declares a `max_append_batch: int | None` **attribute**, not just methods. A hand-rolled fake or `Mock(spec=...)` that omits it does not satisfy `AggregateStore` under mypy. Set `max_append_batch: int | None = None` on every double.~~ **Obsolete:** `max_append_batch` was never read anywhere in the tree and has been deleted from the port and every implementer. Test doubles no longer need to declare it.

- [ ] **Step 3: re-export from `ports/__init__.py`**

Add `AggregateStore` to the `from eventsource.ports.store import (...)` block (alphabetical within the block: it sorts before `CategoryQuery`) and add `"AggregateStore"` to `__all__` under the existing `# Store ports` comment block, first in that group.

- [ ] **Step 4: re-export from the top-level `__init__.py`**

The spec's end-state export table (§4.1) blesses `AggregateStore` at top level, and §6 states slice (a) adds it. This is purely additive — no existing binding changes, so no name collision with the legacy surface arises.

Add `AggregateStore` to the existing `from eventsource.ports import (...)` block at `src/eventsource/__init__.py:158` and add `"AggregateStore"` to `__all__` beside the other port names (`"EventAppender"` at `:389`, `"FullEventStore"` at `:394`).

- [ ] **Step 5: pin it in the public-API acceptance test**

In `tests/unit/test_public_api.py`, add `"AggregateStore"` to the `CORE_RINGS_EXPORTS` list (after `"FullEventStore"`). This is the only edit to that file in this slice; do not touch the `TestCollisionDecisions` class, whose assertions about the legacy top-level bindings must all keep passing unchanged.

- [ ] **Step 6: run targeted tests**

Run: `uv run pytest tests/unit/ports/ tests/unit/test_public_api.py -q`
Expected: PASS.
Run: `uv run mypy src/eventsource/ports/ --config-file=pyproject.toml`
Expected: clean.
Run: `uv run ruff check src/eventsource/ports/ src/eventsource/__init__.py tests/unit/ports/ tests/unit/test_public_api.py`
Expected: clean.

- [ ] **Step 7: commit**

```bash
git add src/eventsource/ports/store.py src/eventsource/ports/__init__.py src/eventsource/__init__.py tests/unit/ports/ tests/unit/test_public_api.py
git commit -m "feat: add aggregatestore port composing appender and stream reader"
```

---

### Task 2: Retype `AggregateRepository` onto `AggregateStore`

**Files:**
- Modify: `src/eventsource/application/aggregates/repository.py`
- Modify tests: `tests/unit/application/aggregates/test_repository.py` (1000), `tests/unit/application/aggregates/test_repository_snapshot.py` (1212), `tests/unit/application/aggregates/test_repository_tracing.py` (696), `tests/integration/observability/test_tracing_integration.py` (792)
- Create: `tests/unit/application/aggregates/test_repository_store_port.py`
- Not modified (inherits for free): `src/eventsource/multitenancy/repository.py` — `TenantAwareRepository` composes an `AggregateRepository` and calls only `save`/`load`/`exists`/`load_or_create`/`create_new`; it touches no store method directly (verified: `repository.py:125,156,254,282,309,330`).

**Interfaces:**
- Consumes: `eventsource.ports.AggregateStore` (Task 1).
- Produces: `AggregateRepository(event_store: AggregateStore, ...)` — parameter name **unchanged** (`event_store`), type widened structurally; `AggregateRepository.event_store` property returns `AggregateStore`.

**Deltas in play here:** `from_version` +1 shift; `result.success` branch collapse; empty-batch guard; store spans vanish from the integration tracing test.

- [ ] **Step 1 (red): write the port-shaped repository test**

Create `tests/unit/application/aggregates/test_repository_store_port.py`. This is the spec's "small unit suite for `AggregateStore` conformance of the repository's store double" (§7 slice (a)). It drives a real `MemoryEventStore` through the repository and asserts the translation rules directly:

```python
"""The repository's use of the AggregateStore port.

Pins the four translation rules from the legacy ABC (spec §1.1, §1.2):
exact-version append, the +1 from_version shift after a snapshot,
get_stream_version for existence/version probes, and
OptimisticLockError as the only conflict signal.
"""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import MemoryEventStore
from eventsource.domain import StreamId
from eventsource.exceptions import AggregateNotFoundError, OptimisticLockError
from eventsource.ports import AggregateStore, StreamReadOptions, collect
```

Cover, at minimum:

- A `MemoryEventStore` is accepted where `AggregateStore` is annotated (a plain assignment in the test body; mypy is the assertion).
- `save` on a fresh aggregate with 3 events writes stream version 3, readable via `store.get_stream_version(StreamId(aggregate_id=..., category=...))`.
- `save` twice appends with the correct expectation: the second call's expected version is the version before those events, so a stale aggregate raises `OptimisticLockError` (not a falsy `result.success`).
- `save` with no uncommitted events is a no-op and does **not** raise `ValueError` — this is the empty-batch guard the spec requires each slice to re-verify.
- `load` with no events and no snapshot raises `AggregateNotFoundError`.
- `load` after a snapshot at version `n` replays exactly the events with `stream_version > n`: build a 5-event stream, save a snapshot at version 2, and assert `aggregate.version == 5` with 3 events replayed. **This is the +1 shift.** An off-by-one here silently double-applies or drops an event, which is why it gets a dedicated case.
- `exists` is `False` before any save and `True` after.
- `get_version` returns 0 for an unknown aggregate and the true version afterwards.

Then the hypothesis property (the one property test this slice calls for):

```python
from hypothesis import given, settings
from hypothesis import strategies as st


@settings(max_examples=50, deadline=None)
@given(
    batches=st.lists(st.integers(min_value=1, max_value=5), min_size=1, max_size=6),
    snapshot_after=st.integers(min_value=0, max_value=10),
)
@pytest.mark.asyncio
async def test_load_reconstructs_version_regardless_of_snapshot_point(
    batches: list[int], snapshot_after: int
) -> None:
    """Loading yields the same version whether or not a snapshot truncates the replay.

    Random batch shapes, random snapshot points: the snapshot's version plus
    the replayed remainder must always equal the total event count. This is
    the executable form of the from_version translation rule -- an off-by-one
    in either direction breaks it for some (batches, snapshot_after) pair.
    """
```

Build the aggregate through the repository, snapshot at `min(snapshot_after, total)`, construct a second repository over the same store and snapshot store, load, and assert `loaded.version == sum(batches)`.

Run: `uv run pytest tests/unit/application/aggregates/test_repository_store_port.py -q`
Expected: FAIL — `AggregateRepository` still calls `append_events`/`get_events`, which `MemoryEventStore` does not have (`AttributeError`).

- [ ] **Step 2 (green): retype the imports and the constructor**

In `src/eventsource/application/aggregates/repository.py`:

Replace `from eventsource.stores.interface import EventStore` (`:32`) with:

```python
from eventsource.domain.stream_id import StreamId
from eventsource.ports.store import AggregateStore
from eventsource.ports.envelopes import StreamReadOptions
from eventsource.ports.positions import ExpectedVersion
```

(Import from the leaf port modules, not `eventsource.ports`, to keep the application ring's import graph narrow — matching how `ports/bus` is already imported at `:31`. If `eventsource.domain.stream_id` is not the module path in this tree, use whatever `from eventsource.domain import StreamId` resolves to; `ports/store.py:16` uses the package import.)

Change the constructor annotation (`:117`) to `event_store: AggregateStore` and the `event_store` property (`:286`) to return `AggregateStore`. Update the `Args:` docstring line for `event_store` (`:141`) to "Event store port for appending and reading this aggregate's stream".

Add a small private helper beside `_infer_aggregate_type` so the three call sites do not each rebuild the identity:

```python
    def _stream(self, aggregate_id: UUID) -> StreamId:
        """Stream identity for one aggregate of this repository's type."""
        return StreamId(aggregate_id=aggregate_id, category=self._aggregate_type)
```

- [ ] **Step 3 (green): rewrite `load`**

Replace the `get_events` call at `:388-392` with a port read. Legacy `from_version=n` meant "skip the first n"; ports `from_version` is inclusive 1-based, so `n` becomes `n + 1`, and `n == 0` becomes no options at all:

```python
            stream = self._stream(aggregate_id)
            options = (
                StreamReadOptions(from_version=from_version + 1) if from_version > 0 else None
            )
            events = [envelope.event async for envelope in self._event_store.read_stream(stream, options)]
```

Then substitute `events` for `event_stream.events` at every remaining use in the method: the not-found check (`:395`), the snapshot-fallback re-read (`:416-422`, which becomes `read_stream(stream)` with **no options** — full replay from the start), the `load_from_history` call (`:427-428`), and both `len(event_stream.events)` reports (`:431`, `:440`). The method never reads `event_stream.version`, so nothing needs `get_stream_version` here — the restored version comes from `load_from_history` / `_restore_from_snapshot` exactly as today.

- [ ] **Step 4 (green): rewrite `save`**

Replace the `append_events` call at `:515-520`:

```python
            await self._event_store.append(
                self._stream(aggregate.aggregate_id),
                uncommitted_events,
                ExpectedVersion.exact(expected_version),
            )
```

`expected_version` is already computed at `:512` as `aggregate.version - len(uncommitted_events)` and is always `>= 0`, so `ExpectedVersion.exact` is the correct mapping — the by-name sentinel translation in `stores/legacy.py::_expected_from_int` never applies here because the repository never passes a sentinel.

Then **delete the `if result.success:` conditional at `:522` and dedent its whole body** (mark-committed, span attributes, publish, snapshot scheduling). The result is discarded entirely: a failed append raises `OptimisticLockError` rather than returning a falsy result, so everything that used to be guarded now runs unconditionally after a successful `await`. Do not bind the return value to a variable — an unused `result` will trip ruff.

- [ ] **Step 5 (green): rewrite `exists` and `get_version`, and re-verify the empty-batch guard**

`exists` (`:668-672`):

```python
            exists = await self._event_store.get_stream_version(self._stream(aggregate_id)) > 0
```

`get_version` (`:689-693`):

```python
        return await self._event_store.get_stream_version(self._stream(aggregate_id))
```

These are exact replacements. Legacy `EventStream.version` for an unfiltered read was the true stream version, and the in-memory quirk the spec warns about (`version` being the length of the *filtered* list) never bit here because neither call passed `from_version`.

Confirm the empty-batch guard at `:495-499` is intact and still precedes the span and the append — the adapters raise `ValueError` on an empty event sequence where the legacy stores returned a successful result, so this guard is now load-bearing rather than merely an optimization. Say so in a one-line comment only if the existing comment ("No changes to persist") does not already make the guard obvious; do not add a comment explaining the change itself.

- [ ] **Step 6: retarget the three repository test modules**

In `tests/unit/application/aggregates/test_repository.py`, `test_repository_snapshot.py`, and `test_repository_tracing.py`, replace `from eventsource.stores.in_memory import InMemoryEventStore` with `from eventsource.adapters.memory.store import MemoryEventStore` and swap the constructor calls (`InMemoryEventStore()` → `MemoryEventStore()`; the adapter takes no `enable_tracing`, so drop that argument where present — `test_repository_tracing.py:101` constructs the bare form already). Rename local type annotations accordingly.

Rewrite, do not merely retarget, any case that:

- calls `store.append_events(...)` / `store.get_events(...)` directly for setup or assertion — use `store.append(StreamId(...), events, ExpectedVersion.exact(n))` and `collect(store.read_stream(StreamId(...)))`;
- asserts on `AppendResult.success` or `.conflict` — those fields do not exist; assert that `OptimisticLockError` is raised;
- asserts `stored_at == occurred_at` — the memory adapter stamps a real clock;
- uses the legacy store's test helpers `clear()`, `get_all_events()`, `get_event_count()`, `get_aggregate_ids()` — the adapter has none. Per spec §4.4: construct a fresh `MemoryEventStore()` instead of `clear()`, and use `collect(store.read_all())` for all-events. If a case is genuinely awkward without `clear()`, stop and report rather than adding a helper to the adapter.

The repository's own span names (`eventsource.repository.load`, `.save`, `.exists`, `.create_snapshot`, `.snapshot`) and attributes are unchanged, so `test_repository_tracing.py`'s assertions about them stay as they are.

- [ ] **Step 7: fix the integration tracing test — store spans are gone**

`tests/integration/observability/test_tracing_integration.py` is listed as RETARGET in the spec's inventory, but the ADR 0016 amendment means the ports adapters emit **no store-level spans at all**. Cases asserting them cannot be retargeted; they must be deleted with the capability:

- `find_span("inmemory_event_store.append_events")` (`:151`) and `find_span("inmemory_event_store.get_events")` (`:193`) — delete these cases outright.
- The repository-parent/store-child hierarchy cases (`:427`, `:465`, `:492`, and the comment at `:670`) — the child span no longer exists. Narrow each case to what still holds (the `eventsource.repository.save` / `.load` span is created with its documented attributes) and delete the child-span assertions. Where a case's entire subject was the parent/child relationship, delete it.

Retarget everything else in the file to `MemoryEventStore` as in Step 6. Record in the commit body how many cases were deleted; the orchestrator needs that count for the ADR 0025 record in slice (d).

- [ ] **Step 8: verify no legacy store reference survives in the repository**

```bash
grep -rn "stores\.interface\|stores\.in_memory\|append_events\|get_events\b" src/eventsource/application/ src/eventsource/multitenancy/
```

Expected: no matches.

- [ ] **Step 9: run targeted tests**

Run: `uv run pytest tests/unit/application/aggregates/ -q`
Expected: PASS.
Run: `uv run pytest tests/unit/multitenancy/ -q`
Expected: PASS (unmodified — `TenantAwareRepository` inherits the retype).
Run: `uv run pytest tests/integration/observability/test_tracing_integration.py -q`
Expected: PASS.
Run: `uv run mypy src/eventsource/application/ src/eventsource/multitenancy/ --config-file=pyproject.toml`
Expected: clean.
Run: `uv run ruff check src/eventsource/application/ tests/unit/application/ tests/integration/observability/`
Expected: clean.

- [ ] **Step 10: commit**

```bash
git add src/eventsource/application/aggregates/repository.py tests/unit/application/aggregates/ tests/integration/observability/test_tracing_integration.py
git commit -m "refactor: retype aggregate repository onto the aggregatestore port"
```

---

### Task 3: Retarget `testing/harness.py` and the shared test fixtures

**Files:**
- Modify: `src/eventsource/testing/harness.py`
- Modify tests: `tests/unit/testing/test_harness.py` (590), `tests/conftest.py` (the `in_memory_store` / `populated_store` fixtures at `:303-334` and the import at `:31`), `tests/unit/test_fixtures.py` (448)

**Interfaces:**
- Consumes: nothing from earlier tasks (independent of Tasks 1-2, though it lands after them).
- Produces: `InMemoryTestHarness.event_store -> MemoryEventStore`. Every other harness member (`event_bus`, `checkpoint_repo`, `dlq_repo`, `published_events`, `reset`, `clear_published_events`, `get_events_of_type`, `__repr__`) is unchanged.

**Note on fixture leverage:** the spec calls `tests/conftest.py` "the highest-leverage single change" in this slice. Verified against this tree, that is not so: `in_memory_store` and `populated_store` are consumed by exactly one module, `tests/unit/test_fixtures.py`. Retarget them anyway (they are legacy-store references in the root conftest), but size the work accordingly.

- [ ] **Step 1 (red): assert the harness hands out the adapter**

In `tests/unit/testing/test_harness.py`, change the import at `:18` to `from eventsource.adapters.memory.store import MemoryEventStore` and the type assertion at `:63-64` to `assert isinstance(harness.event_store, MemoryEventStore)`.

Run: `uv run pytest tests/unit/testing/test_harness.py -q`
Expected: FAIL on the isinstance assertion.

- [ ] **Step 2 (green): swap the store in the harness**

In `src/eventsource/testing/harness.py`:

- Replace the import at `:31` with `from eventsource.adapters.memory.store import MemoryEventStore`.
- In `__init__` (`:77`) and `reset` (`:189`), replace `InMemoryEventStore(enable_tracing=False)` with `MemoryEventStore()`. **The adapter takes no `enable_tracing` parameter** (spec §4.4); passing it is a `TypeError`. The adapter emits no spans at all, so the intent of `enable_tracing=False` — keep test traces clean — is satisfied by construction.
- Retype the `event_store` property (`:91-107`) to `-> MemoryEventStore` and rewrite its docstring example from `append_events(aggregate_id=..., aggregate_type=..., events=..., expected_version=0)` to the port form:

  ```python
  >>> await harness.event_store.append(
  ...     StreamId(aggregate_id=order_id, category="Order"),
  ...     [order_created],
  ...     ExpectedVersion.no_stream(),
  ... )
  ```

- Update the class docstring's component list (`:46`) from "InMemoryEventStore for persisting events" to "MemoryEventStore (the ports adapter) for persisting events", and the example at `:60`.

- [ ] **Step 3: rewrite the harness tests that drive the store directly**

`tests/unit/testing/test_harness.py` calls `harness.event_store.append_events(...)` at `:246` and `:279` and `harness.event_store.get_events(aggregate_id, "Sample")` at `:257`. Rewrite these onto `append(StreamId(...), events, ExpectedVersion...)` and `collect(store.read_stream(StreamId(...)))`, asserting on `EventEnvelope.event` where the old code asserted on a `DomainEvent` from `EventStream.events`, and on `len(envelopes)` where it asserted on `EventStream.version`.

The `get_events_of_type` / `published_events` cases (`:379-452`) go through the event bus, not the store, and need no change.

- [ ] **Step 4: retarget the root conftest fixtures**

In `tests/conftest.py`: change the import at `:31` to `from eventsource.adapters.memory.store import MemoryEventStore`; rename the fixture's return type and body (`:303-312`) to `MemoryEventStore`; and rewrite `populated_store` (`:315-334`) to append through the port:

```python
    await in_memory_store.append(
        StreamId(aggregate_id=aggregate_id, category="Counter"),
        event_stream,
        ExpectedVersion.no_stream(),
    )
```

Keep the fixture **names** (`in_memory_store`, `populated_store`) — renaming them would churn `tests/unit/test_fixtures.py` for no gain, and the names still describe what they are.

- [ ] **Step 5: retarget `tests/unit/test_fixtures.py`**

The only consumer of those two fixtures. Update its type annotations and any direct `append_events`/`get_events` calls per the same rules as Step 3. Where it asserts a populated store's contents, `collect(store.read_stream(...))` returns envelopes — assert on `.event`.

- [ ] **Step 6: verify**

```bash
grep -rn "stores\.in_memory\|InMemoryEventStore" src/eventsource/testing/ tests/conftest.py tests/unit/test_fixtures.py tests/unit/testing/
```

Expected: no matches.

- [ ] **Step 7: run targeted tests**

Run: `uv run pytest tests/unit/testing/ tests/unit/test_fixtures.py -q`
Expected: PASS.
Run: `uv run mypy src/eventsource/testing/ --config-file=pyproject.toml`
Expected: clean.
Run: `uv run ruff check src/eventsource/testing/ tests/conftest.py tests/unit/test_fixtures.py tests/unit/testing/`
Expected: clean.

- [ ] **Step 8: commit**

```bash
git add src/eventsource/testing/harness.py tests/conftest.py tests/unit/test_fixtures.py tests/unit/testing/test_harness.py
git commit -m "refactor: retarget test harness and root fixtures onto the memory adapter"
```

---

### Task 4: Rewrite `SyncEventStoreAdapter` port-shaped

**Files:**
- Modify: `src/eventsource/sync/adapter.py`, `src/eventsource/sync/__init__.py`, `src/eventsource/testing/sync_facade.py` (docstring cross-reference only)
- Rewrite tests: `tests/unit/sync/test_adapter.py` (563), `tests/unit/sync/test_concurrency.py` (339)

**Interfaces:**
- Consumes: `eventsource.ports.FullEventStore` and the port value objects.
- Produces (exact — the spec's §3 signature block):

```python
class SyncEventStoreAdapter:
    def __init__(self, store: FullEventStore, timeout: float = 30.0) -> None
    def append(self, stream, events, expected, *, timeout=None) -> AppendResult
    def read_stream(self, stream, options=None, *, timeout=None) -> list[EventEnvelope]
    def get_stream_version(self, stream, *, timeout=None) -> int
    def event_exists(self, event_id, *, timeout=None) -> bool
    def read_all(self, from_position=None, options=None, *, timeout=None) -> list[EventEnvelope]
    def read_category(self, category, options=None, *, timeout=None) -> list[EventEnvelope]
    def current_position(self, *, timeout=None) -> Position | None
    @property
    def wrapped_store(self) -> FullEventStore
```

**What must not change:** `_run_sync` (`adapter.py:126-173`) byte-for-byte — the three loop scenarios, the `logger.warning` text when called from a running loop, both `TimeoutError` messages, `asyncio.run(asyncio.wait_for(...))`. Also unchanged: the class-level `_executor` / `_executor_lock`, `_get_executor`, `shutdown_executor`, the `timeout` property, and `__repr__`. The spec explicitly declines a new property test here because this machinery is untouched.

**What changes:** the constructor parameter is renamed `event_store` → `store` and retyped to `FullEventStore`; the `isinstance(event_store, EventStore)` ABC guard (`:95-98`) **is deleted** — protocols are structural, and a `TypeError`/`AttributeError` on a missing method is the honest failure; the `_sync` method-name suffixes are dropped (there is no async twin on this class to disambiguate from); async iterators are drained to lists, as `read_all_sync` already did.

- [ ] **Step 1 (red): rewrite the test module against the new surface**

`tests/unit/sync/test_adapter.py` is rewritten, not retargeted. Replace its imports:

```python
from eventsource.adapters.memory.store import MemoryEventStore
from eventsource.domain import StreamId
from eventsource.exceptions import OptimisticLockError
from eventsource.ports import (
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    Position,
    StreamReadOptions,
)
from eventsource.sync import SyncEventStoreAdapter
```

Case-by-case disposition of the existing suite:

| Existing case | Disposition |
|---|---|
| `test_init_with_valid_store` (`:29`) | keep; `MemoryEventStore()`, assert `wrapped_store is store` |
| `test_init_with_default_timeout` (`:37`) | keep verbatim except the store class |
| `test_init_with_invalid_store_raises_type_error` (`:44`) | **delete** — the `isinstance` guard is gone. Replace with a case asserting that calling a method on an adapter wrapping a non-store object raises `AttributeError`, documenting the structural-typing failure mode |
| `test_repr` (`:49`) | keep; the expected class name in the repr becomes `MemoryEventStore` |
| `TestSyncEventStoreAdapterAppendEvents` (`:60`+) and the rest | rewrite onto the port methods: `append(StreamId(...), [event], ExpectedVersion.no_stream())`, `read_stream(...) -> list[EventEnvelope]` (assert on `envelope.event` / `envelope.stream_version`), `get_stream_version(StreamId(...))`, `read_all(from_position=None, FeedReadOptions(limit=...))`, `current_position()` |

Two new cases the port surface demands:

- `current_position()` returns `None` for an empty store — the legacy `get_global_position_sync()` returned `0`, and `None` is not a comparable floor (spec §1.5).
- `read_all` / `read_stream` return `list`, not an iterator — assert `isinstance(result, list)` so the draining contract is pinned.

`tests/unit/sync/test_concurrency.py` gets the same treatment: it exercises `_run_sync`'s loop scenarios and thread-pool behavior, which are unchanged, so only its call sites and store construction move to the port shape. Do not weaken its concurrency assertions.

Run: `uv run pytest tests/unit/sync/ -q`
Expected: FAIL (the adapter still exposes `*_sync` methods).

- [ ] **Step 2 (green): rewrite the adapter**

Replace the import block at `src/eventsource/sync/adapter.py:19-26` with:

```python
from eventsource.domain import StreamId
from eventsource.events.base import DomainEvent
from eventsource.ports import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    FullEventStore,
    Position,
    StreamReadOptions,
    collect,
)
```

Constructor:

```python
    def __init__(
        self,
        store: FullEventStore,
        timeout: float = 30.0,
    ) -> None:
        """Initialize the sync adapter.

        Args:
            store: The async, port-shaped event store to wrap
            timeout: Default timeout in seconds for all operations (default: 30.0)
        """
        self._store = store
        self._timeout = timeout
```

Methods — each one wraps its async counterpart in `self._run_sync(..., timeout=timeout)`, and the three iterator-returning ports drain through `collect`:

```python
    def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
        *,
        timeout: float | None = None,
    ) -> AppendResult:
        return self._run_sync(self._store.append(stream, events, expected), timeout=timeout)

    def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
        *,
        timeout: float | None = None,
    ) -> list[EventEnvelope]:
        return self._run_sync(collect(self._store.read_stream(stream, options)), timeout=timeout)

    def get_stream_version(
        self, stream: StreamId, *, timeout: float | None = None
    ) -> int:
        return self._run_sync(self._store.get_stream_version(stream), timeout=timeout)

    def event_exists(self, event_id: UUID, *, timeout: float | None = None) -> bool:
        return self._run_sync(self._store.event_exists(event_id), timeout=timeout)

    def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
        *,
        timeout: float | None = None,
    ) -> list[EventEnvelope]:
        return self._run_sync(
            collect(self._store.read_all(from_position, options)), timeout=timeout
        )

    def read_category(
        self,
        category: str,
        options: CategoryReadOptions | None = None,
        *,
        timeout: float | None = None,
    ) -> list[EventEnvelope]:
        return self._run_sync(
            collect(self._store.read_category(category, options)), timeout=timeout
        )

    def current_position(self, *, timeout: float | None = None) -> Position | None:
        return self._run_sync(self._store.current_position(), timeout=timeout)
```

`wrapped_store` retypes to `-> FullEventStore`. Delete the now-unused `datetime` import; keep `Sequence`, `UUID`, `Coroutine`, `Any`, `TypeVar`, `ThreadPoolExecutor`, `threading`, `asyncio`, `logging`.

Note the `read_all` signature difference from the old `read_all_sync(options)`: `from_position` is now the **first positional** parameter and is a `Position | None` where `None` means "from the start" — not a comparable zero floor (spec §1.5). Callers that used to pass `ReadOptions(from_position=0)` pass nothing.

- [ ] **Step 3: rewrite the module and package docstrings**

`adapter.py`'s module docstring and the `SyncEventStoreAdapter` class docstring both carry a legacy example (`PostgreSQLEventStore(database_url)`, `get_events_sync`, `append_events_sync`, `expected_version=events.version`) that is wrong in three ways after this task. Rewrite the class example to:

```python
    Example:
        >>> from sqlalchemy.ext.asyncio import create_async_engine
        >>> from eventsource.adapters.postgresql import PostgreSQLEventStore
        >>> from eventsource.domain import StreamId
        >>> from eventsource.ports import ExpectedVersion
        >>> from eventsource.sync import SyncEventStoreAdapter
        >>>
        >>> engine = create_async_engine(database_url)
        >>> sync_store = SyncEventStoreAdapter(PostgreSQLEventStore(engine), timeout=30.0)
        >>>
        >>> # In a Celery task
        >>> @celery.task
        >>> def process_order(order_id: str):
        ...     stream = StreamId(aggregate_id=UUID(order_id), category="Order")
        ...     envelopes = sync_store.read_stream(stream)
        ...     sync_store.append(
        ...         stream,
        ...         [new_event],
        ...         ExpectedVersion.exact(len(envelopes)),
        ...     )
```

Note the adapter's constructor takes an `AsyncEngine`, not a session factory or a URL (spec §4.4). Apply the same rewrite to `src/eventsource/sync/__init__.py`'s module docstring (`:8-19`), whose example is a shorter copy of the same thing.

- [ ] **Step 4: update the two docstring cross-references**

The spec asks that `sync/adapter.py` and `testing/sync_facade.py` each describe the split, since they are now near-twins over the same port surface.

In `SyncEventStoreAdapter`'s class docstring, add:

```
    Related:
        `eventsource.testing.sync_facade.SyncStoreFacade` is the test-machinery
        counterpart: it owns one private event loop for its lifetime and has no
        timeouts. This adapter is for production sync callers (Celery, Django
        management commands, RQ): per-call `asyncio.run`, a running-loop
        threadpool fallback, and a timeout on every operation.
```

In `sync_facade.py`'s module docstring, replace the two paragraphs that describe `sync/adapter.py` as "the older ABC-oriented adapter" and "targets the `ports` protocols instead of the legacy `EventStore` ABC" (`:6-14`) — both statements are false after this task. The distinction is now loop ownership and timeouts, not the port surface:

```
Both this facade and `eventsource.sync.adapter.SyncEventStoreAdapter` drive
a port-shaped `FullEventStore` synchronously. The split is lifecycle: this
facade owns one private loop for its lifetime and has no timeouts, which
suits test machinery; the adapter runs `asyncio.run` per call, falls back to
a threadpool when a loop is already running, and enforces a timeout, which
suits production sync callers.
```

Do not change any `SyncStoreFacade` code — it is already correct and is used by the hypothesis stateful conformance machine.

- [ ] **Step 5: verify**

```bash
grep -rn "_sync\b\|stores\.interface" src/eventsource/sync/ src/eventsource/testing/sync_facade.py
grep -rn "append_events_sync\|get_events_sync\|get_events_by_type_sync\|get_stream_version_sync\|event_exists_sync\|read_all_sync\|get_global_position_sync" src/ tests/ bench/ --include="*.py"
```

Expected: no matches from either (the first also catches a stray `_run_sync` rename; `_run_sync` itself will match — confirm that is the only hit and that its body is unchanged).

- [ ] **Step 6: run targeted tests**

Run: `uv run pytest tests/unit/sync/ -q`
Expected: PASS.
Run: `uv run mypy src/eventsource/sync/ src/eventsource/testing/ --config-file=pyproject.toml`
Expected: clean.
Run: `uv run ruff check src/eventsource/sync/ src/eventsource/testing/sync_facade.py tests/unit/sync/`
Expected: clean.

- [ ] **Step 7: commit**

```bash
git add src/eventsource/sync/ src/eventsource/testing/sync_facade.py tests/unit/sync/
git commit -m "refactor: retype sync event store adapter onto the store ports"
```

---

### Task 5: Retarget `bench/` onto the ports adapters

**Files:**
- Modify: `bench/adapters/stores.py`, `bench/adapters/e2e.py`, `bench/scenarios/stores.py`, `bench/scenarios/aggregate.py`

**Interfaces:**
- Consumes: `eventsource.ports.FullEventStore` (Task 1 is not required — `FullEventStore` already exists — but Task 2 is, because `bench/scenarios/aggregate.py` drives `AggregateRepository`).
- Produces: `STORE_ADAPTERS: dict[str, type[BenchAdapter[FullEventStore]]]` with the same three keys (`memory`, `postgresql`, `sqlite`) and the same scenario names. Scenario names must not change — `bench/core/runner.py` and the report tooling key on them.

**Comparability note to carry into the commit body:** these benchmarks now exercise a different code path, so numbers will move. The bench harness records the store name per run, so before/after comparability is per-run, not cross-era. Do not attempt to preserve numeric continuity; do preserve scenario names and grid points so the report shape is unchanged.

**Import discipline:** import adapters by their full path (`from eventsource.adapters.memory.store import MemoryEventStore`, `from eventsource.adapters.sqlite import SQLiteEventStore`, `from eventsource.adapters.postgresql import PostgreSQLEventStore`). The top-level `eventsource.SQLiteEventStore` / `eventsource.PostgreSQLEventStore` names are still bound to the **legacy** classes until slice (d); importing them here would silently keep benching the old path.

- [ ] **Step 1: rewrite `bench/adapters/stores.py`**

Replace `from eventsource.stores.interface import EventStore` and `from eventsource import InMemoryEventStore` with the port and the adapters. Then, per adapter:

**Memory** — `async def create(self) -> FullEventStore: return MemoryEventStore()`. The adapter takes no `enable_tracing` (spec §4.4) and emits no spans regardless.

**PostgreSQL** — the adapter takes the **engine**, not the session factory. Delete `self._session_factory` and the `async_sessionmaker` construction in `setup()` (keep `create_async_engine` with its existing pool settings, and keep `ensure_schema` / `truncate` / `ping`):

```python
    async def create(self) -> FullEventStore:
        await truncate(asyncpg_dsn(self._url))
        return PostgreSQLEventStore(self._engine, event_registry=make_registry())
```

`outbox_enabled=False` and `enable_tracing=False` are dropped — the adapter has neither parameter. `create_schema` stays at its default `False`: `ensure_schema` already applies the canonical DDL out of band in `setup()`.

**SQLite** — the adapter connects and applies its schema lazily on first use, so the `await store.initialize()` ceremony is deleted:

```python
    async def create(self) -> FullEventStore:
        assert self._tmpdir is not None
        database = str(Path(self._tmpdir.name) / f"{uuid4().hex}.db")
        return SQLiteEventStore(database, event_registry=make_registry(), wal_mode=True)
```

Keep `destroy()`'s `getattr(resource, "close", None)` shape — all three adapters define `close()` except memory, and the `getattr` guard already handles that.

Retype `STORE_ADAPTERS` to `dict[str, type[BenchAdapter[FullEventStore]]]`. Update the module docstring — "SQL backends are added by later tasks" is stale; say the adapters implement the store ports.

- [ ] **Step 2: rewrite `bench/adapters/e2e.py`**

Purely a retype: `EventStore` → `FullEventStore` in the import, the `BenchAdapter[tuple[...]]` parameter, the `_store` annotation, and both `create`/`destroy` signatures. No logic changes.

- [ ] **Step 3: rewrite `bench/scenarios/stores.py`**

`populate_stream` — the version bookkeeping already tracks the exact current version, so `ExpectedVersion.exact(version)` is a direct translation:

```python
async def populate_stream(
    store: FullEventStore,
    aggregate_id: UUID,
    count: int,
    payload: str = "small",
    chunk: int = 500,
) -> None:
    stream = StreamId(aggregate_id=aggregate_id, category="Bench")
    version = 0
    while version < count:
        n = min(chunk, count - version)
        events = make_events(aggregate_id, n, start_version=version + 1, payload=payload)
        await store.append(stream, cast(list[DomainEvent], events), ExpectedVersion.exact(version))
        version += n
```

`_append_batch` — `expected_version=0` becomes `ExpectedVersion.no_stream()` (the aggregate id is freshly minted each iteration, so "no stream" is the honest expectation and matches the by-name mapping rule for legacy `0`).

`_read_stream` — `get_events` returned an `EventStream` whose `.version` the scenario checked; the port returns an iterator. Drain it and check the count. **Keep the check**: it is a correctness guard that has caught mis-populated cells.

```python
        envelopes = await collect(store.read_stream(stream))
        durations.append(time.perf_counter() - t0)
        if len(envelopes) != stream_length:
            raise RuntimeError(f"expected {stream_length} events, read {len(envelopes)}")
```

Draining into a list is what the legacy call did internally too, so this is not a measurement regression — but note in the commit body that `store.read_stream` timings now include list materialization explicitly.

`_concurrent_append` — `expected_version=version` becomes `ExpectedVersion.exact(version)`.

`_contended_append` — this is the one place slice (a) touches the `result.success`/`.conflict` delta. The ports `AppendResult` has no `conflict` field; `OptimisticLockError` is the only conflict signal:

```python
            version = await store.get_stream_version(stream)
            events = make_events(aggregate_id, 1, start_version=version + 1)
            try:
                await store.append(
                    stream, cast(list[DomainEvent], events), ExpectedVersion.exact(version)
                )
            except OptimisticLockError:
                async with lock:
                    conflicts += 1
                continue
            done += 1
```

The `conflicted` flag variable disappears entirely. Keep the `conflicts` counter and the `counters={"conflicts": conflicts}` measurement — the report depends on it.

Also note `get_stream_version` now takes a `StreamId`, not `(aggregate_id, aggregate_type)`.

- [ ] **Step 4: rewrite `bench/scenarios/aggregate.py`**

Retype only: `from eventsource.stores.interface import EventStore` → `from eventsource.ports import FullEventStore`, and every `EventStore` annotation in `_make_repo`, `_make_decider_repo`, `_prepare_e2e`, `_load_mutate_save`, `_prepare_e2e_decider`, `_load_mutate_save_decider` becomes `FullEventStore`. The repository's parameter is `AggregateStore`, which `FullEventStore` satisfies, so no cast is needed. No `AggregateRepository` constructor argument changes — `enable_tracing`, `snapshot_threshold`, and `snapshot_mode` are repository knobs, not store knobs, and all survive. Both `iteration_cap` lambdas and their explanatory comments stay exactly as they are.

- [ ] **Step 5: verify**

```bash
grep -rn "stores\.interface\|stores\.sqlite\|stores\.postgresql\|stores\.in_memory\|append_events\|get_events\b\|\.conflict\b\|\.success\b" bench/
```

Expected: no matches.

- [ ] **Step 6: run targeted checks**

Bench has no test suite; type and lint checks plus a smoke run are the verification.

Run: `uv run mypy bench/ --config-file=pyproject.toml`
Expected: clean.
Run: `uv run ruff check bench/`
Expected: clean.
Run: `uv run python -c "from bench.adapters.stores import STORE_ADAPTERS; from bench.adapters.e2e import make_e2e_adapters; from bench.scenarios.stores import STORE_SCENARIOS; from bench.scenarios.aggregate import E2E_SCENARIOS; print(sorted(STORE_ADAPTERS), len(STORE_SCENARIOS), len(E2E_SCENARIOS), len(make_e2e_adapters()))"`
Expected: `['memory', 'postgresql', 'sqlite'] 4 2 <n>` — the adapter keys, scenario counts, and e2e pairings are unchanged from before your edit. Run this command on the pre-edit tree first if you want the `<n>` baseline.

Run the memory-backend smoke pass (no Docker needed):

```bash
uv run python -m bench.cli run --interface store --backend memory --scenario store.append_batch --quick 2>/dev/null || uv run python -m bench --help
```

If neither entrypoint exists in this tree, check `Makefile`'s `bench` target for the real invocation and use that with the narrowest possible selection. A memory-only, single-scenario pass is sufficient — do not run the full grid, and do not run the postgres cells (Docker services are the orchestrator's call).

- [ ] **Step 7: commit**

```bash
git add bench/adapters/stores.py bench/adapters/e2e.py bench/scenarios/stores.py bench/scenarios/aggregate.py
git commit -m "refactor: retarget bench store and e2e adapters onto the ports surface"
```

---

### Task 6: Retarget `tests/benchmarks/`

**Files:**
- Modify: `tests/benchmarks/conftest.py` (275), `tests/benchmarks/test_event_store.py` (445)

**Interfaces:**
- Consumes: nothing from earlier tasks (independent; ordered here to keep Task 5's diff bench-only).
- Produces: `benchmark_store` and its populated variants yield a `MemoryEventStore`.

This is the pytest-benchmark suite, distinct from `bench/` (the standalone matrix harness retargeted in Task 5). Both exist; do not conflate them.

- [ ] **Step 1 (red): retarget the fixtures**

In `tests/benchmarks/conftest.py`, replace `from eventsource.stores.in_memory import InMemoryEventStore` (`:19`) with `from eventsource.adapters.memory.store import MemoryEventStore`, and retype `benchmark_store` (`:177-181`) and the two populated fixtures (`:186`, `:208`) accordingly. Their population bodies call `append_events` — rewrite onto `append(StreamId(...), events, ExpectedVersion.exact(n))`, tracking the version across chunks exactly as the existing code tracks it.

The `InMemoryOutboxRepository` import at `:18` and the checkpoint/DLQ imports at `:15-16` are untouched — the outbox ring migration is explicitly out of scope (spec §10).

Run: `uv run pytest tests/benchmarks/ -q --no-cov -p no:randomly --collect-only`
Expected: collection succeeds; the run will fail until Step 2.

- [ ] **Step 2 (green): rewrite `test_event_store.py`**

Every benchmark in this module drives the store directly. Translate call-for-call per the §1 map: `append_events` → `append`, `get_events` → `collect(read_stream(...))`, `get_stream_version(aggregate_id, aggregate_type)` → `get_stream_version(StreamId(...))`, `read_all(ReadOptions(...))` → `read_all(from_position=None, FeedReadOptions(...))`, `get_global_position()` → `current_position()`.

Three deltas will bite here specifically:

- Assertions on `EventStream.version` become `len(envelopes)`.
- Assertions on `StoredEvent.global_position` become `EventEnvelope.position` (a `Position`, not an int — compare positions to each other, never to a number, and never subtract).
- Assertions that an empty store's global position is `0` become `current_position() is None`.

Benchmark names (`benchmark(...)` group and id arguments) should stay stable so historical pytest-benchmark comparisons remain keyed the same way, even though the measured code path changed.

- [ ] **Step 3: verify**

```bash
grep -rn "stores\.in_memory\|stores\.interface\|InMemoryEventStore\|append_events\|get_global_position" tests/benchmarks/
```

Expected: no matches.

- [ ] **Step 4: run targeted tests**

Run: `uv run pytest tests/benchmarks/ -q --no-cov`
Expected: PASS.
Run: `uv run ruff check tests/benchmarks/`
Expected: clean.

- [ ] **Step 5: commit**

```bash
git add tests/benchmarks/
git commit -m "test: retarget benchmark suite onto the memory store adapter"
```

---

### Task 7: Contract hardening and final sweep

**Files:**
- Modify: `pyproject.toml` (import-linter contracts only)

**Interfaces:**
- Consumes: Tasks 1-6.
- Produces: no code surface; two contract edits plus the slice-completion verification.

**Concurrency caveat:** other agents are editing `pyproject.toml` concurrently. Locate the blocks by their TOML anchors — the `[[tool.importlinter.contracts]]` entries whose `name` is `"Infrastructure backends must not import each other"` and `"Tier 0 modules must not import sqlalchemy"` — not by line number. Before editing, re-read the current file; if either edit is already present (another branch may have landed the independence swap), verify rather than duplicate, and say so in the commit body.

- [ ] **Step 1: swap the independence contract to the adapters**

Spec §8: the independence contract still names the legacy store modules. The adapters exist, never import each other, and are what the code now runs on, so the swap can land with this slice. In the contract named `"Infrastructure backends must not import each other"`, replace the three `eventsource.stores.*` entries with the adapter packages, leaving the four bus rows untouched:

```toml
modules = [
    "eventsource.adapters.postgresql",
    "eventsource.adapters.sqlite",
    "eventsource.adapters.memory",
    "eventsource.bus.redis",
    "eventsource.bus.kafka",
    "eventsource.bus.rabbitmq",
    "eventsource.bus.memory",
]
```

Package-level entries cover each backend's store/snapshot submodules. Do **not** add `eventsource.adapters._sql` or `eventsource.adapters.sql`: they are shared dialect-parameterized infrastructure that both SQL backends legitimately import, the same reasoning that keeps `bus/base.py` out of the bus rows.

Note this removes coverage of the legacy `stores/*` modules from the contract. That is intended and safe for one slice: those modules are frozen (nothing in this slice edits them) and they are deleted in slice (d).

- [ ] **Step 2: harden the Tier-0 contract**

Spec §8 calls this a free hardening now that both modules are ports-typed. In the contract named `"Tier 0 modules must not import sqlalchemy"`, add two entries to `source_modules`, next to the other `eventsource.testing.*` rows:

```toml
    "eventsource.sync.adapter",
    "eventsource.testing.sync_facade",
```

`sync_facade` was already sqlalchemy-free; `sync/adapter.py` becomes so as of Task 4. Do not remove the `eventsource.stores.interface` or `eventsource.stores.in_memory` rows — those come out in slice (d) with the modules themselves.

- [ ] **Step 3: run import-linter**

Run: `uv run lint-imports --config pyproject.toml`
Expected: all contracts KEPT.

If the Tier-0 contract now fails on `eventsource.sync.adapter`, an import added in Task 4 is pulling sqlalchemy in transitively — do not relax the contract; find and remove the import (`.claude/rules/architecture.md`, Enforcement).

- [ ] **Step 4: final grep sweep across `src/`, `tests/`, `bench/`, `examples/`**

Confirm slice (a) left no legacy-store reference in the four consumers it owns:

```bash
grep -rn "stores\.interface\|stores\.in_memory" \
  src/eventsource/application/ src/eventsource/sync/ src/eventsource/testing/ \
  src/eventsource/multitenancy/ bench/ tests/unit/application/ tests/unit/sync/ \
  tests/unit/testing/ tests/benchmarks/ tests/conftest.py
```

Expected: no matches.

Confirm the rest of the tree still references the legacy surface — it should, and a *zero* result here would mean this slice overreached into (b), (c), or (d):

```bash
grep -rln "stores\.interface\|stores\.in_memory\|append_events" src/eventsource/subscriptions/ src/eventsource/migration/ src/eventsource/__init__.py
```

Expected: several matches. If this comes back empty, stop and report — scope has leaked.

Confirm `examples/` is untouched and still imports only from top-level `eventsource`, whose bindings this slice did not change apart from the additive `AggregateStore`:

```bash
grep -rn "from eventsource\." examples/ | grep -v "^.*from eventsource import" | head
```

Expected: no deep imports into `stores/`, `ports/`, or `adapters/`.

- [ ] **Step 5: mutation-testing config check (no edit expected)**

Spec §7 states slices (a) and (d) need no `only_mutate` additions — `src/eventsource/{domain,ports,adapters,application}` is already covered, which includes the new `AggregateStore` and the retyped repository. Confirm by reading the `[tool.mutmut]` block's `only_mutate` key and verifying `src/eventsource/application` and `src/eventsource/ports` are present. If they are, make no change and note the confirmation in the commit body. `src/eventsource/sync` is deliberately not added — the retyped adapter is a thin delegation layer whose logic (`_run_sync`) is unchanged.

- [ ] **Step 6: commit**

```bash
git add pyproject.toml
git commit -m "chore: point import-linter contracts at the store adapters"
```

---

## Slice Completion Criteria

The orchestrator runs these; implementers do not.

- [ ] `make check` passes (lint, mypy, import-linter, bandit/pip-audit, full unit suite).
- [ ] Integration suites pass with Docker services up: `uv run pytest tests/integration/ -v`.
- [ ] The full suite is run at least once **in default random order** (do not pass `-p no:randomly`) to shake out `stored_at` assumptions in time-frozen tests — spec §11 risk 6 names this as the risk most likely to surface late and in an unrelated-looking place.
- [ ] `src/eventsource/stores/` is byte-identical to its pre-slice state: `git diff <base>..HEAD --stat -- src/eventsource/stores/` reports no changes.
- [ ] `eventsource.__all__` gained exactly one name (`AggregateStore`) and lost none.
- [ ] No file outside this slice's declared scope was modified — in particular `src/eventsource/subscriptions/`, `src/eventsource/migration/`, and `src/eventsource/testing/conformance.py` are untouched.

## Plan Self-Review

- **Spec coverage, slice (a) only.** Every row of the spec's §6 slice (a) migration table has a task: `application/aggregates/repository.py:32` → Task 2; `multitenancy/repository.py` (inherited) → Task 2, verified no direct store calls; `sync/adapter.py` → Task 4; `sync/__init__.py` docstring → Task 4 Step 3; `testing/harness.py:32,78` → Task 3; the four `bench/` modules → Task 5. The `AggregateStore` port addition (§3) → Task 1. The §7 slice (a) test inventory is fully allocated: RETARGET items to Tasks 2, 3, 6; the two REWRITE sync suites to Task 4; the "small unit suite for `AggregateStore` conformance" to Task 2 Step 1. The §8 contract edits attributed to slice (a) → Task 7. Nothing from slices (b), (c), or (d) appears here.
- **Placeholder scan.** No TODO, TBD, or `???` remains. Every code block is complete or is an explicit, bounded instruction ("translate call-for-call per the §1 map") over code the implementer is reading anyway.
- **Signature consistency.** `AggregateStore` is spelled identically in Tasks 1, 2, 5. `SyncEventStoreAdapter`'s eight method signatures in Task 4 Step 2 match the spec's §3 block exactly, including the keyword-only `timeout` and the `from_position`-first ordering of `read_all`. `MemoryEventStore` (not `InMemoryEventStore`) is used throughout — the rename to `InMemoryEventStore` is slice (d)'s, and using the future name here would not import.

## Spec Gaps Found (report, do not silently deviate)

1. **§7 slice (a) calls `tests/conftest.py` "the highest-leverage single change."** Verified: `in_memory_store` and `populated_store` are consumed by exactly one module, `tests/unit/test_fixtures.py`. The fixtures still need retargeting (Task 3), but the leverage claim is wrong and Task 3 is sized accordingly.
2. **§7 lists `tests/integration/observability/test_tracing_integration.py` as RETARGET, which contradicts the ADR 0016 amendment.** The file asserts `inmemory_event_store.append_events` / `.get_events` spans (`:151`, `:193`, `:492`) and repository-parent/store-child hierarchies (`:427`, `:465`, `:670`). The adapters emit no store spans, so those cases must be DELETED, not retargeted. Task 2 Step 7 handles it and asks the implementer to record the deleted-case count for ADR 0025.
3. **§4.1 and §6 disagree on the public API.** §6 says slice (a) leaves "`eventsource` exports unchanged," then in the same paragraph says `AggregateStore` "is added to `ports` and exported." §4.1's end-state table blesses it at top level. Resolved in favor of the additive top-level export (Task 1 Steps 4-5): it is purely additive, collides with nothing in the legacy surface, and `tests/unit/test_public_api.py` accommodates it with a one-line addition.
4. **§3 says `AggregateRepository.load` needs the restored version computed as `snapshot.version + len(events)`, "replacing `EventStream.version`."** Verified against `repository.py:325-443`: `load` never reads `EventStream.version`. The version comes from `_restore_from_snapshot` plus `load_from_history`, both unchanged. No arithmetic is needed; Task 2 Step 3 says so explicitly to stop an implementer from adding a redundant computation.
5. **`EventAppender` declares a `max_append_batch: int | None` attribute, not only methods.** The spec's port descriptions do not mention it, but any hand-rolled test double or `Mock(spec=...)` standing in for `AggregateStore` must define it or fail mypy. Flagged in Task 1 Step 2.
6. **§3's `bench/adapters/stores.py` description says the postgres adapter "takes the *engine*"** — correct, and the bench adapter already builds one in `setup()`. But it also builds an `async_sessionmaker` that becomes dead once the engine is passed directly; the spec does not say to remove it. Task 5 Step 1 removes it.
