# Outbox Ring Migration (+ Connection-Helper Consolidation)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split `repositories/outbox.py` (1,080 lines) into a pure `ports/outbox.py` and three driver-specific adapters, consolidate the two identical SQL connection helpers onto `adapters/_sql/connection.py::sql_connection`, and delete the `src/eventsource/repositories/` package. After this slice the string `eventsource.repositories` does not appear in any executable file in the repository, and `eventsource.ports.outbox` imports without sqlalchemy.

**Architecture:** `OutboxEntry`, `OutboxStats`, the `OutboxRepository` Protocol, and a new shared `outbox_event_data()` payload builder move to `ports/outbox.py` (Tier 0). The three implementations move to `adapters/memory/outbox.py`, `adapters/postgresql/outbox.py`, and `adapters/sqlite/outbox.py` — split by *driver*, not dialect-parameterized like `adapters/sql/{checkpoints,dlq}.py`, because the SQLite outbox takes a raw `aiosqlite.Connection` where the PostgreSQL one takes a sqlalchemy `AsyncConnection | AsyncEngine`. `outbox_event_data()` becomes the single authority for the six-key payload that `adapters/postgresql/store.py::_write_to_outbox` writes and the outbox repositories read. `repositories/_connection.py` dies; its five non-outbox consumers retarget onto `sql_connection`.

**Tech Stack:** Python 3.13, pydantic v2, sqlalchemy 2 (adapters only), asyncpg, aiosqlite, pytest + pytest-asyncio + hypothesis, mypy strict, ruff, import-linter, mutmut.

**Spec:** `docs/superpowers/specs/2026-07-31-outbox-ring-design.md` (in full)

**Predecessor:** the four legacy-store-retirement slices have ALL LANDED. Verified against the tree at HEAD: `src/eventsource/stores/` is gone, `adapters/postgresql/store.py` already carries `outbox_enabled` and `_write_to_outbox` (slice (d) Task 1), `SQLITE_AVAILABLE` already comes from `adapters.sqlite` rather than from the outbox `try/except`, and ADR 0025 is Accepted. Do not redo any of it.

## Global Constraints

- **Unreleased software — no shims, no back-compat aliases.** No `eventsource.repositories` stub package, no `execute_with_connection = sql_connection` alias, no re-export of the outbox names from their old paths, no deprecation module. A deleted name is deleted. This constraint is what forces Task 3's size — see its preamble.
- **Positions are opaque.** Compare and persist (`to_str`/`from_str`); never subtract, never compare to an int. Nothing in the outbox touches positions; do not introduce any.
- **`migrations/` SQL files are append-only BY FILE.** This slice needs **no** schema change at all — not a new file, not a fragment, not an edit. `event_outbox`'s shipped schema (`templates/outbox.sql`, `templates/sqlite/outbox.sql`) is the contract the adapters already satisfy. If a task appears to need a schema change, stop and report.
- **Public-API changes keep `tests/unit/test_public_api.py` as the single pin.** It is updated in the *same task* as the swap it pins, never before and never after. Do not create a second file that also asserts export identities.
- **Every deletion task ends with a full grep sweep** over `src/ tests/ bench/ examples/ docs/`. Executable references (`.py`, `.toml`, `.yml`) must be **zero**; docs *prose* references are updated in Task 5 and may still be non-zero until then — the sweep reports them, and Task 5's sweep is the one that must come back clean everywhere.
- **Test discipline.** Implementers run ONLY the test commands listed in the task they are executing — never the full suite, never `make check`. The orchestrator runs the full suite after each task and dispatches fixes.
- **`bench/` is in every sweep.** It is executable code that imports the library and is not covered by the unit suite. Grep it alongside `src/ tests/ examples/` in every verification step, even when a task expects zero hits there.
- **Path-scoped `git add` only.** Other agents work concurrently in this worktree. Never `git add -A`, never `git add .`; stage exactly the files the task names, and prefer `git commit --only <paths>`. On `index.lock` contention, wait 5 seconds and retry.
- **No live-src mutation probes.** Do not "try an edit and see what mypy says" on shared source while other agents are active. Reason from the code you read; experiment on a copy under `$CLAUDE_JOB_DIR/tmp`.
- **Implementers do not push.** Commit only. Branch pushes and PRs are the orchestrator's.
- **Commit messages** follow `.claude/rules/commits.md` — `<type>: <lowercase description>` — and every commit ends with the trailer:

  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  ```

- **Red/green TDD.** Every task that changes behavior writes or edits its failing test first, observes the failure, then implements. Steps are ordered so the red step precedes the green one; do not reorder them.
- **Property tests only where §4.2 mandates them.** This slice mandates exactly two, both in Task 4. Do not add speculative hypothesis suites elsewhere, and do not extend the existing ones (`tests/unit/adapters/test_memory_stateful.py`, `test_memory_dlq_properties.py`, `test_memory_checkpoints_properties.py`) — they must stay green untouched.
- **mypy strict:** all new and modified code fully annotated. `uv run mypy src/eventsource/ --config-file=pyproject.toml` is listed per task.
- **import-linter green per task, not only at the end.** `uv run lint-imports` is in every task's verify step. Note that import-linter resolves every module named in a contract, so a contract row naming a deleted module fails the run — which is why Task 3's `pyproject.toml` edit and its package deletion are the same commit.
- Pre-commit hooks run on commit (ruff, mypy, import-linter). A task is not done until its commit succeeds, which means **every task must leave the tree importable and type-clean**.

### Why the split, the deletion, and the public-API swap are one task

Task 3 is large and cannot be subdivided. Three constraints interlock:

1. **The port cannot precede the deletion.** `OutboxEntry` and `OutboxStats` are dataclasses, and dataclass identity is nominal: if `ports/outbox.py` defines `OutboxEntry` while `repositories/outbox.py` still defines its own, an adapter returning one and a test importing the other compare unequal and `isinstance` fails. Two referents for one name is exactly the collision slice (d) spent a task eliminating. Having `repositories/outbox.py` import the VOs from the port during a transitional window would be a shim, which the standing rule forbids.
2. **The `__init__.py` swap cannot precede the deletion.** `eventsource/__init__.py:191` imports six outbox names from `eventsource.repositories`, and `tests/unit/test_public_api.py` pins them. Rebinding them to the new paths while the old module still exists gives two live definitions again.
3. **The deletion cannot precede the swap.** `__init__.py` is a runtime importer of `eventsource.repositories`. Deleting the package first makes `import eventsource` an `ImportError` — nothing collects, no test runs, the commit cannot pass pre-commit. The same is true of the `pyproject.toml` `forbidden_modules` row: import-linter fails on a named module that does not exist, so that edit cannot land early either.

Everything that *can* be lifted out has been. Task 1 (`outbox_event_data()`) and Task 2 (the connection retarget) are green before and after and land first; the conformance suite and property tests (Task 4) are additive and land after; the ADR and docs (Task 5) reference nothing at import time.

Task 2 must precede Task 3 for a smaller reason: it strips `execute_with_connection` down to a single consumer (`repositories/outbox.py`), so Task 3 deletes the helper outright rather than negotiating five other importers mid-deletion.

### Behavior deltas this slice makes visible

Spec §3. Repeated in the task that first encounters each.

| Delta | Before | After | Who feels it |
|---|---|---|---|
| `list_pending_events` | on the Protocol and all three backends | removed; use `get_pending_events` | Task 3; CHANGELOG (Task 5) |
| `OutboxRepositoryProtocol` | importable self-alias | removed | Task 3; CHANGELOG |
| `eventsource.repositories` | importable package | `ModuleNotFoundError` | Task 3; pinned by `test_public_api.py` |
| in-memory `event_data` serialization | orjson `json_dumps`, `{"a":1}` | stdlib `json.dumps`, `{"a": 1}` | Task 3; any exact-string assertion on `event_data` |
| `execute_with_connection` | `eventsource.repositories._connection` | gone; `sql_connection(conn, *, write=)` | Task 2 (consumers), Task 3 (deletion) |

---

### Task 1: The shared payload builder

**Files:**
- Create: `src/eventsource/ports/outbox.py` (the `outbox_event_data` half only — the VOs and Protocol arrive in Task 3)
- Modify: `src/eventsource/ports/__init__.py`
- Modify: `src/eventsource/adapters/postgresql/store.py` (`_write_to_outbox` only)
- Create tests: `tests/unit/ports/test_outbox_payload.py`

**Interfaces:**
- Consumes: nothing.
- Produces (used by Task 3's three adapters and, from this task on, by the store's write path):

```python
def outbox_event_data(event: DomainEvent) -> dict[str, Any]: ...
```

**Why this is lifted out of Task 3.** The six-key payload dict is written out as a literal in four places today — `repositories/outbox.py:258-265`, `:551-558`, `:831-838`, and `adapters/postgresql/store.py:377-384` — and it is a contract between the store's *writer* and the repositories' *reader* that nothing enforces. Landing the shared builder first means Task 3's three adapter moves each delete a literal rather than copying one, and it means the writer/reader alignment the slice was asked to pin is verified by its own test before the move churn starts.

- [ ] **Step 1: read the four literals before writing anything**

Read all four sites listed above. Confirm by reading, not assumption, that they produce the same six keys with the same value types. Two spellings differ and must be reconciled deliberately:

- `adapters/postgresql/store.py:383` uses `event.model_dump(mode="json")` for `payload`; the three repositories use `json.loads(event.model_dump_json())`. These produce equal dicts — `model_dump_json()` is `model_dump(mode="json")` serialized — so `model_dump(mode="json")` is the surviving form (one fewer round-trip). Confirm the equality on a scratch event under `$CLAUDE_JOB_DIR/tmp` before relying on it; if any field differs, stop and report rather than picking one.
- `tenant_id` is `str(event.tenant_id) if event.tenant_id else None` in all four. Preserve the falsy check exactly — do not "improve" it to `is not None`; a nil UUID is not a case this codebase has decided about, and changing it here would be an undeclared delta.

- [ ] **Step 2 (red): test the builder**

Create `tests/unit/ports/test_outbox_payload.py`. Read a neighbouring module in `tests/unit/ports/` (e.g. `test_envelopes.py`) for house style first.

Cases:

- The returned dict has exactly the six keys `event_id`, `aggregate_id`, `aggregate_type`, `tenant_id`, `occurred_at`, `payload` — assert on the key set, so a seventh key added later fails here.
- `event_id`, `aggregate_id` are `str`; `occurred_at` is `event.occurred_at.isoformat()`.
- `tenant_id` is `str(...)` when set and `None` when the event has none.
- `payload == event.model_dump(mode="json")`.
- The result survives `json.dumps` with **no** custom encoder — this is the property the memory adapter's delta 4 rests on, and it is the reason the builder returns a JSON-safe dict rather than raw objects.

Run: `uv run pytest tests/unit/ports/test_outbox_payload.py -q` — Expected: FAIL (`ModuleNotFoundError: eventsource.ports.outbox`).

- [ ] **Step 3 (green): write the builder**

Create `src/eventsource/ports/outbox.py`:

```python
"""Transactional outbox port.

Pure boundary interface: stdlib, typing, uuid, datetime, dataclasses, and
`DomainEvent` only. No sqlalchemy, no driver types.
"""

from typing import Any

from eventsource.events.base import DomainEvent


def outbox_event_data(event: DomainEvent) -> dict[str, Any]:
    """Build the JSON-safe payload stored in `event_outbox.event_data`.

    This is the single authority for that shape. Both sides of the outbox
    depend on it: the same-transaction writer
    (`eventsource.adapters.postgresql.store.PostgreSQLEventStore._write_to_outbox`)
    produces it, and every `OutboxRepository` adapter stores it and hands it
    back on `OutboxEntry.event_data`. A drain worker parses it to rebuild the
    event, so adding or renaming a key is a wire-format change, not a
    refactor.

    The result contains only `str`, `None`, and JSON-native values, so
    `json.dumps` serializes it without a custom encoder.
    """
    return {
        "event_id": str(event.event_id),
        "aggregate_id": str(event.aggregate_id),
        "aggregate_type": event.aggregate_type,
        "tenant_id": str(event.tenant_id) if event.tenant_id else None,
        "occurred_at": event.occurred_at.isoformat(),
        "payload": event.model_dump(mode="json"),
    }


__all__ = ["outbox_event_data"]
```

Add `outbox_event_data` to `ports/__init__.py`'s imports and `__all__`, in the position the file's existing grouping implies (it groups by concept with a comment per group — add an outbox group rather than appending to an unrelated one).

- [ ] **Step 4 (green): retarget the store's write path**

In `src/eventsource/adapters/postgresql/store.py::_write_to_outbox`, replace the inline
`outbox_event_data = {...}` literal (`:377-384`) with a call to the new helper. The local
variable currently shares the helper's name — rename the local or import the function
under its own name so the two do not shadow each other; do not rename the *function* to
avoid the collision.

Keep `json.dumps(...)` at the serialization site exactly as it is, including the comment
at `:368-372` explaining why stdlib `json.dumps` is used rather than the module's orjson
`json_dumps`. Update that comment to note the payload now comes from
`ports.outbox.outbox_event_data`, which guarantees JSON-safety.

Update the docstring at `:358-370`: the sentence pointing at
`eventsource.repositories.outbox` as the reader is about to become false. Point it at
`eventsource.ports.outbox.OutboxRepository` (the contract) — **not** at
`eventsource.adapters.postgresql.outbox`, which does not exist until Task 3. Also update
the `outbox_enabled` property docstring at `:149`, which carries the same stale pointer.

- [ ] **Step 5: verify**

```bash
grep -rn "occurred_at\": \|\"payload\": " src/eventsource/ | grep -v ports/outbox.py
```
Expected: the three literals still in `repositories/outbox.py` (Task 3 removes them) and nothing in `adapters/`.

```bash
grep -rn "eventsource.repositories" src/eventsource/adapters/
```
Expected: no matches.

Run: `uv run pytest tests/unit/ports/ tests/unit/adapters/test_postgresql_outbox.py -q` — Expected: PASS.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/ tests/unit/ports/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean. (`eventsource.ports` is already a Tier-0 `source_modules` row; the new module inherits it and imports no sqlalchemy.)

- [ ] **Step 6: commit**

```bash
git add src/eventsource/ports/outbox.py src/eventsource/ports/__init__.py \
        src/eventsource/adapters/postgresql/store.py tests/unit/ports/test_outbox_payload.py
git commit --only src/eventsource/ports/outbox.py src/eventsource/ports/__init__.py \
        src/eventsource/adapters/postgresql/store.py tests/unit/ports/test_outbox_payload.py \
        -m "refactor: pin the outbox payload shape in one place"
```

Record in the commit body: that four literals become one builder (three of them in Task 3), the `model_dump(mode="json")`-vs-`json.loads(model_dump_json())` equivalence you confirmed in Step 1, and that the falsy `tenant_id` check was preserved deliberately.

---

### Task 2: Retarget the five non-outbox `execute_with_connection` consumers

**Files:**
- Modify: `src/eventsource/readmodels/postgresql.py`, `src/eventsource/migration/repositories/{audit_log,migration,position_mapping,routing}.py`
- Create tests: `tests/unit/adapters/test_sql_connection.py`
- Modify tests: `tests/unit/migration/test_{audit_log,migration,position_mapping,routing}_repository.py` (patch targets only)

**Interfaces:**
- Consumes: nothing.
- Produces: `eventsource.repositories._connection.execute_with_connection` has exactly one importer left — `repositories/outbox.py` — which Task 3 deletes.

**Why `sql_connection` is the survivor** (spec §2.3): no shims are permitted, so the helper cannot move and re-export; `adapters/_sql/` is where the ring map puts shared SQL infrastructure and `repositories/` has no successor; and `write` is keyword-only with no default, so every call site states its intent, where `transactional=True` was a silent default. `adapters/_sql/connection.py`'s own docstring schedules this merge for "the outbox slice".

**The mapping is exact and total:** `execute_with_connection(c, transactional=True)` → `sql_connection(c, write=True)`; `transactional=False` → `write=False`; a bare `execute_with_connection(c)` (which defaulted to transactional) → `sql_connection(c, write=True)`. Both functions branch identically on `isinstance(conn, AsyncEngine)` and both yield a live `AsyncConnection` through without committing. **Check every bare call** — `readmodels/postgresql.py` and `migration/repositories/position_mapping.py` both have call sites that omit the keyword, and reading `write=True` into them is a decision, not a transcription. Confirm each such site is a write (INSERT/UPDATE/DELETE) before assigning `write=True`; if one is a bare `SELECT`, that is a pre-existing bug where a read took a transaction, and you must report it rather than silently changing it to `write=False`.

- [ ] **Step 1 (red): test the surviving helper**

`adapters/_sql/connection.py` has **no** direct test today — it is covered only incidentally through `adapters/sql/{checkpoints,dlq}.py`. Before it becomes the tree's only connection helper it gets its own suite.

Create `tests/unit/adapters/test_sql_connection.py`. `tests/unit/test_connection_helper.py` (316 lines) is the working model for the mock shapes — read it and port its cases, translating `transactional=` to `write=`. Note it patches `"eventsource.repositories._connection.isinstance"` by dotted string at eight sites; the equivalent target is `"eventsource.adapters._sql.connection.isinstance"`. Do **not** delete `tests/unit/test_connection_helper.py` in this task — `execute_with_connection` is still live until Task 3, and deleting its only coverage early would drop the module's line coverage below the gate for one commit.

Cases (all of `test_connection_helper.py`'s, minus any that test only the defaulting behavior `sql_connection` deliberately does not have): engine + `write=True` uses `begin()`; engine + `write=False` uses `connect()`; a live `AsyncConnection` is yielded through unchanged for both values of `write` and is **not** committed; exceptions propagate and the engine context manager still exits; two sequential uses of the same engine each get their own context.

Run: `uv run pytest tests/unit/adapters/test_sql_connection.py -q` — Expected: FAIL initially if you write the cases before checking the mock plumbing; if it passes on the first run, confirm the assertions actually exercise `sql_connection` (a suite that passes against an unmodified module is a suite that asserts nothing about this change — check that a deliberate local inversion of the `write` branch, on a copy under `$CLAUDE_JOB_DIR/tmp`, would fail it).

- [ ] **Step 2 (green): retarget the five source modules**

Per module: change the import line to `from eventsource.adapters._sql.connection import sql_connection`, then rewrite each call site per the mapping above. The import line numbers at HEAD are `readmodels/postgresql.py:29`, `migration/repositories/audit_log.py:64`, `migration/repositories/migration.py:67`, `migration/repositories/position_mapping.py:70`, `migration/repositories/routing.py:72` — re-read each file rather than trusting these; other agents are active.

Do the modules one at a time and count call sites against the expected totals (readmodels 16, position_mapping 11, migration 10, routing 7, audit_log 5) before moving to the next. A count that does not match means either the file changed under you or you missed a site.

Do not reformat, reorder, or otherwise touch anything else in these five files. This is a rename.

- [ ] **Step 3 (green): retarget the ~90 mock-patch strings**

The four migration unit-test modules patch the helper by dotted string on the *consumer* module, not on the helper's own module — e.g.
`"eventsource.migration.repositories.routing.execute_with_connection"`. Each becomes
`"eventsource.migration.repositories.routing.sql_connection"`. Counts at HEAD: routing 23, position_mapping 22, migration 15, audit_log 16.

A stale patch target raises `AttributeError` at test time rather than passing vacuously, so a missed one fails loudly — but verify by count and by grep anyway (Step 4), because a *test* that never runs cannot fail.

Any patched call whose mock asserts on the call's arguments (`assert_called_with(..., transactional=True)`) must have that assertion updated to `write=True`. Grep for `transactional` inside the test files, not only for the patch strings.

`readmodels/postgresql.py`'s tests: locate them (`grep -rln "readmodels" tests/unit/`) and check whether they patch the helper the same way. If they do, they are part of this step; if they use a real engine or a different seam, leave them alone.

- [ ] **Step 4: verify**

```bash
grep -rn "execute_with_connection" src/ tests/ bench/ examples/
```
Expected: only `src/eventsource/repositories/_connection.py` (the definition), `src/eventsource/repositories/outbox.py` (7 call sites + 1 import), and `tests/unit/test_connection_helper.py`. Anything else is a missed retarget.

```bash
grep -rn "transactional=" src/ tests/ bench/ examples/
```
Expected: the same three files only.

Run: `uv run pytest tests/unit/adapters/test_sql_connection.py tests/unit/migration/ tests/unit/test_connection_helper.py -q` — Expected: PASS.
Run: `uv run pytest tests/unit/ -q --co` — Expected: collection succeeds.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/ tests/unit/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

The readmodels and migration integration suites need Docker and are the orchestrator's; do not start services.

- [ ] **Step 5: commit**

```bash
git add src/eventsource/readmodels/postgresql.py src/eventsource/migration/repositories/ \
        tests/unit/adapters/test_sql_connection.py tests/unit/migration/
git commit --only src/eventsource/readmodels/postgresql.py src/eventsource/migration/repositories/ \
        tests/unit/adapters/test_sql_connection.py tests/unit/migration/ \
        -m "refactor: move the sql connection helper's consumers onto sql_connection"
```

Record in the commit body: the per-module call-site counts, the per-module patch-string counts, every bare `execute_with_connection(c)` call you resolved and which way you resolved it, and the accepted debt that four `migration/` modules now name an `adapters/` module by import (spec §2.3 — no import-linter contract covers it, and their real destination is a later slice).

---

### Task 3: The move — port, three adapters, package deletion, public-API swap

**Why this task is one commit.** See "Why the split, the deletion, and the public-API swap are one task" above. In short: two live definitions of `OutboxEntry` is a nominal-identity collision, the `__init__` swap needs the old module gone, and the deletion needs the swap done. Tasks 1 and 2 removed everything that could be removed. The steps are ordered so an implementer can work outside-in, and the orchestrator may dispatch Steps 1-5 and Steps 6-9 as separate work units against the same uncommitted tree — but there is exactly one commit at the end.

**Files:**
- Create: `src/eventsource/adapters/memory/outbox.py`, `src/eventsource/adapters/postgresql/outbox.py`, `src/eventsource/adapters/sqlite/outbox.py`
- Modify: `src/eventsource/ports/outbox.py` (add the VOs and Protocol), `src/eventsource/ports/__init__.py`, `src/eventsource/adapters/{memory,postgresql,sqlite}/__init__.py`, `src/eventsource/__init__.py`, `pyproject.toml`
- Delete: `src/eventsource/repositories/` (entire package: `outbox.py` 1080, `_connection.py` 65, `__init__.py` 57)
- Delete tests: `tests/unit/test_connection_helper.py` (316)
- Move/rewrite tests: `tests/unit/test_outbox_repository.py` (895) → `tests/unit/adapters/`; the outbox half of `tests/repositories/test_sqlite_repos.py` (`:262` onward) → `tests/unit/adapters/`
- Modify tests: `tests/unit/test_public_api.py`, `tests/conftest.py` (`:33`, `:654`), `tests/integration/conftest.py`, `tests/integration/repositories/test_outbox.py`, `tests/benchmarks/{conftest.py,test_repositories.py}`, `tests/unit/test_edge_cases.py` (`:31`), `tests/unit/serialization/test_json.py` (`:652`)

**Interfaces:**
- Consumes: Task 1 (`outbox_event_data`), Task 2 (`sql_connection` with one caller left).
- Produces — `ports/outbox.py` in full:

```python
"""Transactional outbox port.

Pure boundary interface: stdlib, typing, uuid, datetime, dataclasses, and
`DomainEvent` only. No sqlalchemy, no driver types.

The outbox pattern lets events be published reliably even when the broker
is unavailable: the event row and the outbox row commit in one database
transaction, and a drain worker (which this library does not ship — see
`docs/guides/repository-operations.md`) publishes and marks them.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from eventsource.events.base import DomainEvent


@dataclass
class OutboxEntry:
    """One row of the outbox.

    Attributes:
        id: Unique outbox entry identifier. A `UUID` on PostgreSQL and
            in memory; SQLite mints an autoincrement integer, so adapters
            may hand back either.
        event_id: Event ID being published
        event_type: Type of the event
        aggregate_id: Aggregate ID the event belongs to
        aggregate_type: Type of aggregate
        tenant_id: Tenant ID (optional)
        event_data: The `outbox_event_data()` payload, as the JSON string
            the backend stored or as the already-parsed dict
        created_at: When the entry was created
        status: Current status (pending, published, failed)
        published_at: When the event was published (if applicable)
        retry_count: Number of publish retry attempts
        last_error: Last error message (if any)
    """

    id: UUID
    event_id: UUID
    event_type: str
    aggregate_id: UUID
    aggregate_type: str
    tenant_id: UUID | None
    event_data: str | dict[str, Any]
    created_at: datetime
    status: str = "pending"
    published_at: datetime | None = None
    retry_count: int = 0
    last_error: str | None = None


@dataclass(frozen=True)
class OutboxStats:
    """Aggregate statistics for the outbox.

    Attributes:
        pending_count: Number of pending events
        published_count: Number of published events
        failed_count: Number of failed events
        oldest_pending: Timestamp of oldest pending event
        avg_retries: Average retry count for pending events
    """

    pending_count: int = 0
    published_count: int = 0
    failed_count: int = 0
    oldest_pending: datetime | None = None
    avg_retries: float = 0.0


@runtime_checkable
class OutboxRepository(Protocol):
    """Protocol for outbox repositories.

    Implementations store events transactionally with aggregate changes
    and hand them to a drain worker for asynchronous publishing.
    """

    async def add_event(self, event: DomainEvent) -> UUID:
        """Add an event to the outbox for publishing.

        Call this inside the same transaction that persists the event to
        the event store; that atomicity is the point of the pattern.

        Args:
            event: Domain event to publish

        Returns:
            Outbox record ID
        """
        ...

    async def get_pending_events(self, limit: int = 100) -> list[OutboxEntry]:
        """Get pending events that need to be published, oldest first.

        Args:
            limit: Maximum number of events to return

        Returns:
            List of OutboxEntry instances
        """
        ...

    async def mark_published(self, outbox_id: UUID) -> None:
        """Mark an outbox event as successfully published.

        Args:
            outbox_id: Outbox record ID
        """
        ...

    async def mark_failed(self, outbox_id: UUID, error: str) -> None:
        """Mark an outbox event as permanently failed.

        Args:
            outbox_id: Outbox record ID
            error: Error message
        """
        ...

    async def increment_retry(self, outbox_id: UUID, error: str | None = None) -> None:
        """Increment retry count for a failed publishing attempt.

        Args:
            outbox_id: Outbox record ID
            error: Error message (optional)
        """
        ...

    async def cleanup_published(self, days: int = 7) -> int:
        """Delete published events older than `days`.

        Args:
            days: Number of days to retain published events

        Returns:
            Number of records deleted
        """
        ...

    async def get_stats(self) -> OutboxStats:
        """Get outbox statistics.

        Returns:
            OutboxStats with outbox metrics
        """
        ...


def outbox_event_data(event: DomainEvent) -> dict[str, Any]:
    ...  # unchanged from Task 1


__all__ = [
    "OutboxEntry",
    "OutboxRepository",
    "OutboxStats",
    "outbox_event_data",
]
```

- Produces — the end-state export surface of `eventsource`:

| Name | New source |
|---|---|
| `OutboxRepository`, `OutboxEntry`, `OutboxStats`, `outbox_event_data` | `eventsource.ports` |
| `InMemoryOutboxRepository` | `eventsource.adapters.memory` |
| `PostgreSQLOutboxRepository` | `eventsource.adapters.postgresql` |
| `SQLiteOutboxRepository` (behind `if SQLITE_AVAILABLE:`) | `eventsource.adapters.sqlite` |

Names that die from `eventsource` and everywhere: `OutboxRepositoryProtocol`, `OutboxRepository.list_pending_events` (and its three implementations), the module path `eventsource.repositories` and every submodule of it, and the `EventSourceJSONEncoder`/`json_dumps`/`json_loads` re-exports that `repositories/__init__.py` carried (their home, `eventsource.serialization`, is untouched).

- [ ] **Step 1 (red): rewrite `tests/unit/test_public_api.py`**

This file is the acceptance test for the export swap. Read it first; slice (d) rewrote it and it now has a `TestBlessedStoreSurface`-shaped identity section and a `DEAD_NAMES` absence case. Extend both rather than inventing a third idiom:

- Add positive identity cases in the existing style:

```python
def test_top_level_outbox_repository_is_the_port_protocol(self) -> None:
    from eventsource.ports import OutboxRepository
    assert eventsource.OutboxRepository is OutboxRepository

def test_top_level_in_memory_outbox_is_the_memory_adapter(self) -> None:
    from eventsource.adapters.memory.outbox import InMemoryOutboxRepository
    assert eventsource.InMemoryOutboxRepository is InMemoryOutboxRepository
```

  — and the same shape for `OutboxEntry`, `OutboxStats`, and `PostgreSQLOutboxRepository`.
- Add `"OutboxRepositoryProtocol"` to `DEAD_NAMES`.
- Add a case asserting `import eventsource.repositories` raises `ModuleNotFoundError`, alongside the existing `eventsource.stores` case.
- Add a case asserting `not hasattr(eventsource.ports.OutboxRepository, "list_pending_events")`.
- Add `"outbox_event_data"` to whichever export list the file uses to pin `eventsource.__all__` membership.

Run: `uv run pytest tests/unit/test_public_api.py -q` — Expected: FAIL.

- [ ] **Step 2 (green): finish `ports/outbox.py`**

Add the two dataclasses and the Protocol per the Interfaces block, above the
`outbox_event_data` Task 1 landed. Copy the dataclass field lists and docstrings from
`repositories/outbox.py:41-92` — they are correct and reviewed; do not rewrite them,
except for the `id` and `event_data` docstring lines, which the Interfaces block above
improves (the `id` type genuinely varies by backend and the old docstring did not say so).

The Protocol carries **seven** methods, not eight: `list_pending_events` does not move.
Update `ports/__init__.py`'s outbox group and `__all__`.

- [ ] **Step 3 (green): move the three implementations**

Each is a move, not a rewrite. Copy the class verbatim, then apply exactly the changes listed for it and nothing else. Do not reformat, do not rename locals, do not "improve" a query.

**`src/eventsource/adapters/memory/outbox.py`** — from `repositories/outbox.py:497-751`:
- Import `OutboxEntry`, `OutboxStats`, `outbox_event_data` from `eventsource.ports.outbox`.
- Replace the `event_data = {...}` literal (`:551-558`) with `outbox_event_data(event)`.
- Replace `json_dumps(event_data)` (`:568`) with `json.dumps(event_data)` and drop the
  `from eventsource.serialization import json_dumps` import. **This is behavior delta 4**
  (orjson's compact `{"a":1}` becomes stdlib's `{"a": 1}`); it is what lets this module's
  only non-stdlib imports be `eventsource.observability` and `eventsource.ports.outbox`.
  Add a one-line comment saying the payload is JSON-safe by construction so no encoder is
  needed.
- Delete `list_pending_events` (`:740-742`). Keep `clear()` (`:744-751`) — it matches
  `InMemoryDLQRepository.clear()` and is deliberately off the Protocol.
- Move the function-local `from datetime import timedelta` inside `cleanup_published`
  (`:683`) to the module import block; a deferred stdlib import inside a hot method is
  noise the move can drop without changing behavior.
- Keep every tracer span name and attribute exactly (ADR 0016 stands for the outbox).

**`src/eventsource/adapters/postgresql/outbox.py`** — from `repositories/outbox.py:198-494`:
- Import the VOs and `outbox_event_data` from `eventsource.ports.outbox`; import
  `sql_connection` from `eventsource.adapters._sql.connection`.
- Replace the seven `execute_with_connection(self.conn, transactional=X)` calls with
  `sql_connection(self.conn, write=X)` — same mapping as Task 2.
- Replace the `event_data = {...}` literal (`:258-265`) with `outbox_event_data(event)`.
  `json.dumps(event_data, cls=EventSourceJSONEncoder)` (`:282`) **stays as it is**: the
  encoder is now redundant for this payload but the parameter binding is unchanged either
  way, and dropping it here would be a second undeclared serialization delta in the same
  commit. Note in the module docstring that it is redundant and why it was left.
- Delete `list_pending_events` (`:492-494`).
- Module docstring names `src/eventsource/migrations/templates/outbox.sql` as the schema
  this adapter reads and writes, and names
  `adapters/postgresql/store.py::_write_to_outbox` as the in-transaction writer of the
  same rows. That pairing is the table contract; state it once, here.

**`src/eventsource/adapters/sqlite/outbox.py`** — from `repositories/outbox.py:754-1076`:
- Import the VOs and `outbox_event_data` from `eventsource.ports.outbox`.
- Replace the `event_data = {...}` literal (`:831-838`) with `outbox_event_data(event)`;
  leave the `EventSourceJSONEncoder` argument for the same reason as PostgreSQL.
- Delete `list_pending_events` (`:1074-1076`). Keep `_parse_datetime` and the whole
  SQLite-adaptations docstring block (`:760-766`) — that list of dialect differences is
  the module's most valuable prose.
- Keep the `if TYPE_CHECKING: import aiosqlite` guard. This module must import cleanly
  **without** aiosqlite installed, matching `adapters/sqlite/store.py`. Verify by reading
  the import block, not by assumption — if anything imports `aiosqlite` at module scope,
  the `__init__.py` wiring in Step 4 and the `SQLITE_AVAILABLE` guard in Step 5 both
  change shape, and you must report that rather than adding a guard of your own design.
- Module docstring names `src/eventsource/migrations/templates/sqlite/outbox.sql`.

- [ ] **Step 4 (green): wire the three adapter packages**

Add the new class to each of `adapters/{memory,postgresql,sqlite}/__init__.py`'s imports
and `__all__`, following each file's existing alphabetical-within-`__all__` convention.
Update each package docstring's one-line summary: memory's currently reads "implementing
the store, snapshot, checkpoint, and DLQ ports" and now also implements the outbox port;
postgresql's and sqlite's read "implementing the store ports" and now cover snapshots and
the outbox too — fix all three, since the sqlite and postgresql summaries were already
understating their contents before this slice.

- [ ] **Step 5 (green): rewrite the `__init__.py` outbox section**

In `src/eventsource/__init__.py`:

- Delete the `from eventsource.repositories import (...)` block at `:191-197`.
- Bind `OutboxRepository`, `OutboxEntry`, `OutboxStats`, `outbox_event_data` by extending
  the module's **existing** `from eventsource.ports import (...)` block rather than adding
  a second one.
- Bind `InMemoryOutboxRepository` from `eventsource.adapters.memory` and
  `PostgreSQLOutboxRepository` from `eventsource.adapters.postgresql`, extending the
  existing imports from those packages.
- Retarget the guarded import at `:205-208` from
  `eventsource.repositories.outbox.SQLiteOutboxRepository` to
  `eventsource.adapters.sqlite.SQLiteOutboxRepository`. Keep the `try/except ImportError`
  and the `# noqa: F401`. Keep the comment above it, updated for the new path. Slice (d)
  already decoupled `SQLITE_AVAILABLE`'s *definition* from this statement (it comes from
  `adapters.sqlite` at `:43-44`), so this is a path change only — do not restructure the
  guard.
- Keep `if SQLITE_AVAILABLE: __all__.append("SQLiteOutboxRepository")` (`:382-385`) exactly.
- In `__all__`: remove `"OutboxRepositoryProtocol"` if present, add `"outbox_event_data"`,
  and confirm each surviving outbox name appears exactly once. Run the file's own
  duplicate check.
- The module docstring's feature bullet "Transactional Outbox pattern" (`:10`) stays true;
  leave it.

- [ ] **Step 6 (green): delete**

```bash
git status --porcelain src/eventsource/repositories/ tests/unit/test_connection_helper.py
```

Confirm no other agent has uncommitted work under those paths. If any file there is modified, stop and report rather than deleting someone's work. Then:

```bash
git rm -r src/eventsource/repositories/
git rm tests/unit/test_connection_helper.py
```

`tests/unit/test_connection_helper.py` dies with `execute_with_connection`;
`tests/unit/adapters/test_sql_connection.py` (Task 2) is its successor and covers the
same control flow.

**Do not touch `tests/unit/repositories/`.** It holds `test_dialect.py` and
`test_dialect_properties.py`, which test `adapters/_sql/dialect.py` and have nothing to do
with the deleted source package. The mutmut test selection names that directory.

- [ ] **Step 7 (green): move and retarget the tests**

- `tests/unit/test_outbox_repository.py` (895): split by backend into
  `tests/unit/adapters/test_memory_outbox.py` and
  `tests/unit/adapters/test_postgresql_outbox_repository.py` (name it so it does not
  collide with the existing `test_postgresql_outbox.py`, which covers the *store's* write
  path from slice (d)). Delete the `TestOutboxRepositoryProtocol` class at `:296` if it
  asserts on the dead alias; if it asserts `isinstance(repo, OutboxRepository)` against
  the runtime-checkable Protocol, keep it and retarget the import. Delete every
  `list_pending_events` case.
- `tests/repositories/test_sqlite_repos.py`: move the outbox half (`:262` onward, the
  `SQLiteOutboxRepository` classes) to `tests/unit/adapters/test_sqlite_outbox.py`. The
  `SQLCheckpointRepository` half (`:60-261`) **stays in place** — do not move the file.
  Carry the module's pytest markers and its `TestSampleEvent` fixture across as needed;
  read the module header before splitting.
- Import retargets, no other change: `tests/conftest.py:33` and `:654`,
  `tests/integration/conftest.py`, `tests/integration/repositories/test_outbox.py:21`
  (imports `PostgreSQLOutboxRepository` from top-level `eventsource`, which still works —
  check whether the campaign's convention wants it path-explicit and match what the other
  integration modules do), `tests/benchmarks/conftest.py:21`,
  `tests/benchmarks/test_repositories.py:22`, `tests/unit/test_edge_cases.py:31`.
- `tests/unit/serialization/test_json.py:652` does
  `from eventsource.repositories import EventSourceJSONEncoder` to assert the re-export
  exists. That re-export dies. Delete the case — do not retarget it to
  `eventsource.serialization`, which the module already tests directly a few lines above
  (read it and confirm before deleting). `:32`'s docstring reference to
  `eventsource.repositories.outbox` becomes `eventsource.adapters.postgresql.outbox`.
- **Behavior delta 4 sweep:** `grep -rn "event_data" tests/ | grep -v "event_data=" ` and
  inspect every assertion comparing `event_data` to a literal JSON *string*. The in-memory
  adapter's output now has stdlib spacing. Rewrite such an assertion to parse and compare
  dicts, which is what it meant; do not adjust the expected string's whitespace.

- [ ] **Step 8 (green): contracts**

In `pyproject.toml`:

- Tier-0 forbidden contract (`name = "Tier 0 modules must not import sqlalchemy"`): add
  `"eventsource.adapters.memory.outbox"` beside its `snapshots`/`checkpoints`/`dlq`
  siblings. `"eventsource.ports"` is already a row and covers `ports/outbox.py`.
- "Application ring must not import adapters" contract: delete `"eventsource.repositories"`
  from `forbidden_modules`. import-linter resolves every named module and the package no
  longer exists.
- `[tool.mutmut]` needs **no** edit — verify this rather than assuming: `only_mutate`
  already lists `src/eventsource/ports` and `src/eventsource/adapters` wholesale, and
  `pytest_add_cli_args_test_selection` already lists `tests/unit/ports/` and
  `tests/unit/adapters/`. If either is narrower than that at the time you read it, add
  what is missing and say so in the commit body.
- The explanatory comment below the contracts block describes the resolved projections
  Tier-0 blocker. Add a sentence recording that `repositories/` is now deleted outright,
  matching that comment's voice.

**Concurrency caveat:** other agents edit `pyproject.toml`. Locate each contract by its
`name = ` line, not by line number, and re-read the file immediately before editing.

- [ ] **Step 9: sweep and verify**

```bash
grep -rn "eventsource\.repositories\|OutboxRepositoryProtocol\|list_pending_events\|execute_with_connection" \
  src/ tests/ bench/ examples/ pyproject.toml
```
Expected: **zero** matches. (`docs/` still has matches — those are Task 5's.)

```bash
grep -rn "from eventsource.repositories\|import repositories" src/ tests/ bench/ examples/
```
Expected: zero matches.

```bash
git status --porcelain src/eventsource/migrations/
```
Expected: empty. This slice changes no SQL.

Run: `uv run pytest tests/unit/test_public_api.py -q` — Expected: PASS.
Run: `uv run python -c "import eventsource.repositories"` — Expected: `ModuleNotFoundError`.
Run: `uv run python -c "import sys; import eventsource.ports.outbox; assert 'sqlalchemy' not in sys.modules"` — Expected: exits 0. This is the Tier-0 claim, checked at runtime rather than only statically.
Run: `uv run pytest tests/unit/ -q` — Expected: PASS. (This task is the one exception to "targeted tests only": the unit suite is the only thing that proves the deletion did not orphan a consumer. Do **not** run integration or `make check`.)
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/ tests/ bench/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 10: commit**

```bash
git add src/eventsource/ports/ src/eventsource/adapters/ src/eventsource/__init__.py \
        pyproject.toml tests/
git status --porcelain
```

Review that `git status` output line by line before committing — this is the largest commit of the slice and a stray file from another agent must not ride along.

```bash
git commit -m "refactor: split the outbox onto ports and adapters, delete repositories/"
```

Record in the commit body: the four deleted-and-rehomed names (`OutboxRepositoryProtocol`, `list_pending_events` ×3 implementations plus the Protocol method, the `eventsource.repositories` path, the JSON re-exports), behavior delta 4 and every assertion it forced you to rewrite, the two `pyproject.toml` contract edits, confirmation that mutmut needed no edit, and that `migrations/` is untouched.

---

### Task 4: Conformance suite and property tests

**Files:**
- Create: `src/eventsource/testing/conformance_ports/outbox.py`
- Modify: `src/eventsource/testing/conformance_ports/__init__.py`
- Create tests: `tests/unit/adapters/test_memory_outbox_conformance.py`, `tests/unit/adapters/test_memory_outbox_properties.py`
- Modify tests: `tests/unit/adapters/test_sqlite_conformance.py`, `tests/integration/repositories/test_outbox.py`

**Interfaces:**
- Consumes: Task 3.
- Produces: `OutboxRepositoryConformance`, exported from `eventsource.testing.conformance_ports`, run against all three backends.

**Why after the move and not with it.** The suite is additive and cannot be written against two competing `OutboxEntry` definitions. Landing it separately also keeps the gap it may find — an adapter that does not honor the port it now formally claims — reviewable on its own rather than buried in a 2,000-line move.

- [ ] **Step 1 (red): write the suite**

Create `src/eventsource/testing/conformance_ports/outbox.py`. Read
`conformance_ports/dlq.py` first and match it exactly: an ABC with an abstract `store`
pytest fixture, importing only from `eventsource.ports`, `eventsource.events`, and
pytest/stdlib — no sqlalchemy, no adapter imports. Use the shared helpers in
`conformance_ports/_fixtures.py` for event construction; read two neighbouring suites
before writing.

```python
"""Conformance suite for the `OutboxRepository` port.

Subclass and provide a `store` fixture yielding a fresh adapter instance.
Cleanup cutoff semantics differ per backend (PostgreSQL computes
`NOW() - INTERVAL`, SQLite `datetime('now', ...)`, and the in-memory
adapter a Python `timedelta`), so only the status partition is asserted
here -- exact cutoff boundaries belong in the backend-specific modules,
following the pattern `dlq.py` established.
"""
```

Cases, one per behavior, named in the house `test_<subject>_<expectation>` style:

- `add_event` returns an id, and `get_pending_events` hands back one entry whose
  `event_id`, `event_type`, `aggregate_id`, `aggregate_type`, and `tenant_id` match the
  event, with `status == "pending"` and `retry_count == 0`.
- `event_data` round-trips: parsing it (it may already be a dict) yields the six
  `outbox_event_data` keys, with `payload` equal to `event.model_dump(mode="json")`.
- Pending events come back oldest-first.
- `limit` is honored.
- `mark_published` removes the entry from pending and stamps a non-`None` `published_at`.
- `mark_failed` sets `status == "failed"` and records `last_error`.
- `increment_retry` raises `retry_count` by one per call and records the error, leaving
  `status` as `"pending"`.
- `cleanup_published` deletes only published entries, returns the number deleted, and
  leaves pending and failed counts unchanged.
- `get_stats` counts each status, reports the oldest pending `created_at`, and averages
  `retry_count` over pending entries only.

Two things to get right, both learned from the existing suites: an unknown `outbox_id`
passed to `mark_published`/`mark_failed`/`increment_retry` is a **no-op** on the in-memory
adapter (it guards with `if outbox_id in self._entries`) and silently affects zero rows on
both SQL adapters — so a "missing id" case is legitimate conformance and should assert
"does not raise and changes nothing", not an exception. And `OutboxEntry.id` is a `UUID`
on PostgreSQL and in memory but an autoincrement integer on SQLite, so no case may assume
its type; round-trip the value the adapter gave you.

Export the class from `conformance_ports/__init__.py` (import + `__all__`), and update
that package docstring's list of suites.

- [ ] **Step 2 (green): wire the three backends**

- `tests/unit/adapters/test_memory_outbox_conformance.py`: new, modelled on
  `test_memory_dlq_conformance.py` verbatim in shape — a class subclassing the suite with
  a `store` fixture yielding `InMemoryOutboxRepository()`, plus the memory-specific
  extras: `clear()` empties the repository, and whatever cleanup-cutoff behavior the
  in-memory `timedelta` arithmetic gives that the suite deliberately does not assert.
- `tests/unit/adapters/test_sqlite_conformance.py`: add `OutboxRepositoryConformance` to
  the existing multiple-inheritance test class **or** add a new class beside it — read how
  the module already composes `DLQRepositoryConformance` and `CheckpointRepositoryConformance`
  and follow that. The outbox needs the `event_outbox` table; the module already applies
  schemas via `get_schema` — extend that call, do not hand-write DDL.
- `tests/integration/repositories/test_outbox.py`: add the suite as a base class to the
  existing PostgreSQL test class, keeping every case already there. Its fixtures already
  create and clean the table.

Run: `uv run pytest tests/unit/adapters/ -q` — Expected: PASS, with the collected count up by the number of suite cases × 2 backends.

**If a newly added case fails on an adapter, stop and report.** That is a genuine
divergence between an adapter and the port it now formally claims, and deciding whether
the adapter or the suite is wrong is not an implementer's call.

- [ ] **Step 3 (red/green): the two property tests**

Create `tests/unit/adapters/test_memory_outbox_properties.py`, modelled on
`test_memory_dlq_properties.py`. Exactly two properties (spec §4.2):

- **Retry monotonicity.** For a generated sequence of `increment_retry` calls with
  arbitrary `str | None` errors interleaved with `mark_failed`, an entry's final
  `retry_count` equals the number of `increment_retry` calls applied to it, and
  `last_error` is the most recent error argument — including the case where the most
  recent one is `None`, which the current implementation *does* store (read `:642` and
  assert what it does, then say so in the commit body if that surprises you; do not change
  the implementation from a property test).
- **Cleanup partition.** For a generated set of entries in mixed statuses and any
  `days >= 0`, `cleanup_published` returns exactly the number it removed, removes only
  entries whose status is `published`, and leaves `get_stats().pending_count` and
  `.failed_count` unchanged.

Use the project's existing hypothesis settings idiom (read the neighbouring properties
module for the `@settings` decorator and any registered profile). Keep generated sizes
small — these run in the default unit suite.

Do not write property tests for the SQL adapters or for `OutboxEntry` construction.

- [ ] **Step 4: verify**

```bash
grep -rn "sqlalchemy\|aiosqlite\|asyncpg\|eventsource.adapters" src/eventsource/testing/conformance_ports/outbox.py
```
Expected: no matches. The suite is backend-free.

Run: `uv run pytest tests/unit/adapters/ tests/unit/test_conformance.py -q` — Expected: PASS.
Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml` — Expected: clean.
Run: `uv run ruff check src/eventsource/testing/ tests/unit/adapters/` — Expected: clean.
Run: `uv run lint-imports` — Expected: clean. (`eventsource.testing.conformance` is a Tier-0 row; `conformance_ports` inherits the same discipline and the new module must not break it.)

The PostgreSQL conformance run needs Docker and is the orchestrator's; do not start services.

- [ ] **Step 5: commit**

```bash
git add src/eventsource/testing/conformance_ports/ \
        tests/unit/adapters/test_memory_outbox_conformance.py \
        tests/unit/adapters/test_memory_outbox_properties.py \
        tests/unit/adapters/test_sqlite_conformance.py \
        tests/integration/repositories/test_outbox.py
git commit --only <the same paths> -m "test: conformance and property suites for the outbox port"
```

Record in the commit body: the case count, the collected-count delta per backend, the two properties and what each fixes in place, and any divergence you found and reported rather than fixed.

---

### Task 5: ADR 0026, docs, changelog, backlog

**Files:**
- Create: `docs/adrs/0026-outbox-ring-migration.md`
- Modify: `docs/adrs/index.md`
- Modify: `docs/api/repositories.md`, `docs/guides/repository-operations.md`, `docs/core-surface.md`, `docs/architecture.md`, `docs/development/code-structure.md`, plus whatever Step 3's grep finds
- Modify: `CLAUDE.md` (Project Structure block), `CHANGELOG.md`, `BACKLOG.md`

**Interfaces:**
- Consumes: Tasks 1-4.
- Produces: no code surface.

- [ ] **Step 1: write ADR 0026**

`docs/adrs/0026-outbox-ring-migration.md`. Follow the house shape — read
`0024-projection-persistence-ports.md` (the direct precedent, same refactor on the sibling
modules) and `0025-legacy-store-retirement.md` first. **Confirm 0026 is still the next
free number** before writing; other agents are active and ADRs are numbered after the
current highest.

The Decision must record:

1. The split: VOs + Protocol to `ports/outbox.py` (Tier 0), implementations to
   `adapters/{memory,postgresql,sqlite}/outbox.py`. Same operation ADR 0024 applied to
   checkpoint and DLQ; this completes the set of three.
2. **Per-backend, not dialect-parameterized** — and the rule that follows from it: a
   repository belongs in `adapters/sql/` when one sqlalchemy implementation serves both
   dialects, and in `adapters/<technology>/` when the backends take different drivers.
   The SQLite outbox takes a raw `aiosqlite.Connection`; unifying it would mean rewriting
   a working adapter onto sqlalchemy. Rejected alternative, named and not built.
3. `outbox_event_data()` as the single authority for the six-key payload, replacing four
   copies, one of which lived in a different ring from the other three.
4. The connection-helper consolidation onto `sql_connection`, with the accepted debt that
   four `migration/` modules and `readmodels/postgresql.py` now import an `adapters/`
   module — permitted by every contract, and resolved when those modules move to
   `adapters/` in their own slice.
5. Deletion of the `repositories/` package.
6. Death of `OutboxRepositoryProtocol` and `list_pending_events` — no shims, unreleased
   software, one name per thing.
7. The in-memory `event_data` serialization change (orjson → stdlib), and that it buys
   one fewer non-stdlib import in a Tier-0 adapter.
8. That ADR 0016's tracing decision **stands** for the outbox repositories, unlike the
   store adapters that 0025 amended it for — outbox repositories keep their spans.

Include an **ADR Impact** table per `.claude/rules/definition-of-done.md`, rendering spec
§6's verdicts as this record's own impact statement rather than copying the prose: 0015
stands, 0016 stands, 0019 stands, 0024 stands (extended, not amended), 0025 stands.
**No existing ADR is amended by this one**, so no Status pointer is added to any other
record — say so explicitly in the Consequences, because a reviewer checking the
definition-of-done rule will look for the pointers and should find the reason they are
absent.

Add the 0026 entry to `docs/adrs/index.md` in the style of its neighbours: one dense
sentence naming what it decides, its relationships, and "Complete."

- [ ] **Step 2: the docs sweep**

```bash
grep -rn "eventsource\.repositories\|OutboxRepositoryProtocol\|list_pending_events\|execute_with_connection\|repositories/outbox\|repositories/_connection" \
  docs/ examples/ README.md CLAUDE.md | grep -v "docs/superpowers/"
```

Skip `docs/superpowers/` entirely — specs and plans are dated records of what was decided
when, and rewriting them destroys the audit trail. Work the rest:

- **`docs/api/repositories.md`** (423 lines) is the largest single job and needs a
  rewrite, not a patch: it is an API reference *for a package that no longer exists*,
  organized around `from eventsource.repositories import ...`. Three of its sections
  describe things that are now gone — the `OutboxRepositoryProtocol` alias section
  (`:92-104`), the `list_pending_events` compatibility-pair table (`:136-146`), and the
  import-surface section (`:68-90`). The page becomes a reference for the outbox port and
  its three adapters: the port at `eventsource.ports.outbox`, the adapters at their new
  paths, the backend-selection table (`:162-172`, which survives with its constructor
  column intact), and the naming-convention section minus the compatibility pair. Consider
  whether the file should be renamed to `docs/api/outbox.md` now that it documents one
  port rather than a package — check `mkdocs.yml`'s nav before deciding, and if renaming
  means touching nav entries other agents may also be editing, keep the filename and say
  so in the commit body.
- **`docs/guides/repository-operations.md`** (520): `:22-30` imports from
  `eventsource.repositories` and explicitly documents the `*Protocol` aliases as "kept for
  compatibility" — that sentence and the aliases both go. `:271`'s `list_pending_events`
  paragraph goes. The construction examples (`:99-171`) keep their content and change
  their import lines. `:112-142`'s explanation of connection-vs-engine construction is the
  best prose in the file on why `sql_connection`'s two modes exist — keep it, and check it
  still describes `write=`-shaped behavior accurately.
- **`docs/core-surface.md`**: steps 3, 4, and 5 of "Recommended pre-extraction cleanup"
  (`:285-297`) are this slice's charter and are now **Done** — mark them so in the same
  voice steps 1 and 2 use ("**Done (ADR 0024).**" → "**Done (ADR 0026).**"), stating what
  actually landed rather than restating the plan. Step 5's "two viable shapes" discussion
  is moot: the package was deleted rather than narrowed. Also update the fifteen-file
  sqlalchemy grep result (`:322-329`) — re-run the grep rather than editing the list by
  hand, note the new count, and re-derive the front-door import chain, since deleting
  `repositories/` removes one route to sqlalchemy without removing the cost (`engine` and
  `adapters.postgresql` still pull it from `__init__.py`). Say that honestly; the lazy
  front door stays out of scope.
- **`docs/architecture.md`** and **`docs/development/code-structure.md`**: both carry a
  `repositories/` entry in their layout narratives. Rewrite to name `ports/outbox.py` and
  the three adapter modules.
- **`CLAUDE.md`** Project Structure block: delete the `repositories/` row (`:83-84`);
  extend the `ports/` row (`:69`) to name the outbox port and the `adapters/` row
  (`:71-73`) to name the three outbox adapters. Line `:104`'s "Backend-agnostic" bullet
  says interfaces are "defined in `interface.py` or `base.py` files" — that has been false
  since the ports migration; fix it to name `ports/` while you are in the file.
- Everything else the grep finds: import lines and path references only.

- [ ] **Step 3: changelog**

`CHANGELOG.md` `[Unreleased]`. Add:

- **Added:** `eventsource.ports.outbox` (`OutboxRepository`, `OutboxEntry`, `OutboxStats`,
  `outbox_event_data`), the three adapter modules, and
  `OutboxRepositoryConformance` in `eventsource.testing.conformance_ports`.
- **Changed:** the four outbox names' import paths; `sql_connection` as the one SQL
  connection helper.
- **Removed:** `eventsource.repositories` (the whole package, including the
  `EventSourceJSONEncoder`/`json_dumps`/`json_loads` re-exports — point at
  `eventsource.serialization`), `OutboxRepositoryProtocol`,
  `OutboxRepository.list_pending_events`, and
  `eventsource.repositories._connection.execute_with_connection`.

Call behavior delta 4 out explicitly — the in-memory adapter's `event_data` JSON spacing
changed with the move from orjson to stdlib `json.dumps`. It is cosmetic for any consumer
that parses the field and breaking for one that compares the string.

Check whether any existing `[Unreleased]` bullet now describes a state that no longer
holds (the slice-(d) adapters bullet mentions `outbox_enabled` and the outbox reader's
location). `[Unreleased]` has not shipped, so edit such a bullet in place to describe the
end state rather than layering a correction on top — and say in the commit body that you
did.

- [ ] **Step 4: groom the backlog**

`BACKLOG.md`:

- **"Migrate outbox repository to ports/adapters (P2)"** — delete the item; it is done.
  Note that its text predicted `adapters/sql/outbox.py` and the slice landed
  `adapters/{memory,postgresql,sqlite}/outbox.py` instead; that divergence is argued in
  ADR 0026 §2 and does not need to survive in the backlog.
- **"Investigate making sqlalchemy an optional dependency (P3)"** — **carry, with an
  edit.** Its premise list names `stores/postgresql.py`, `stores/sqlite.py`, and
  `repositories/`, none of which exist. Rewrite the item's inventory against a fresh
  `grep -rlE '^(from|import) sqlalchemy' src/eventsource/` and keep the question, which is
  still open and still worth answering. Do not close it: this slice removed one blocker,
  not the investigation.
- **"Add CI boundary check for core surface purity (P2)"** — carry unchanged, but note
  that Task 3 Step 9's `sys.modules` assertion for `ports.outbox` is a working example of
  exactly the check the item proposes, and point at it.
- **"Lazy top-level eventsource/__init__ (P3)"** — carry, with an edit: its parenthetical
  cites the import chain `application/aggregates/repository.py -> stores/__init__ ->
  stores/postgresql`, and `stores/` no longer exists. Replace it with the chain
  `docs/core-surface.md` records after Step 2's re-derivation.

- [ ] **Step 5: final sweep**

```bash
grep -rn "eventsource\.repositories\|OutboxRepositoryProtocol\|list_pending_events\|execute_with_connection" \
  src/ tests/ bench/ examples/ docs/ CLAUDE.md README.md pyproject.toml | grep -v "docs/superpowers/"
```
Expected: **zero** matches. This is the acceptance for the whole slice.

```bash
git status --porcelain src/eventsource/migrations/
```
Expected: empty.

Run: `uv run ruff check examples/` — Expected: clean.
Run: `uv run pytest tests/unit/test_public_api.py -q` — Expected: PASS (docs changes must not move the surface).
Run: `uv run lint-imports` — Expected: clean.

- [ ] **Step 6: commit**

```bash
git add docs/ CLAUDE.md CHANGELOG.md BACKLOG.md examples/
git commit --only docs/ CLAUDE.md CHANGELOG.md BACKLOG.md examples/ \
        -m "docs: adr 0026 and the outbox ring documentation sweep"
```

Record in the commit body: which pages were rewritten wholesale versus patched, whether `docs/api/repositories.md` was renamed and why, the backlog items closed versus carried and what was edited in each, and any `[Unreleased]` changelog entry corrected in place.

---

## Slice Completion Criteria

The orchestrator runs these; implementers do not.

- [ ] `make check` passes (lint, mypy, import-linter, bandit/pip-audit, full unit suite).
- [ ] Integration suites pass with Docker services up: `uv run pytest tests/integration/ -v`, including `tests/integration/repositories/test_outbox.py` with its new conformance base class and `tests/integration/e2e/test_full_flow.py::TestOutboxIntegration`.
- [ ] The full suite runs at least once in default random order (do not pass `-p no:randomly`).
- [ ] `uv run python -c "import eventsource.repositories"` raises `ModuleNotFoundError`.
- [ ] `uv run python -c "import sys, eventsource.ports.outbox; assert 'sqlalchemy' not in sys.modules"` exits 0.
- [ ] `grep -rn "eventsource\.repositories\|OutboxRepositoryProtocol\|list_pending_events\|execute_with_connection" src/ tests/ bench/ examples/ docs/ pyproject.toml | grep -v docs/superpowers/` is empty.
- [ ] `uv run mkdocs build --strict` (or the project's docs command — check the `Makefile`) succeeds, catching dead internal links left by the `docs/api/repositories.md` rewrite.
- [ ] `docs/adrs/0026-outbox-ring-migration.md` has `Status: Accepted`, a Decision covering all eight items, a Consequences section, and an ADR Impact table stating that no existing ADR is amended.
- [ ] `docs/core-surface.md` cleanup steps 3, 4, and 5 are marked Done, and its sqlalchemy-importer file list matches a fresh grep.
- [ ] Benchmarks still run: `make bench-up && make bench` on at least the memory scenarios — `tests/benchmarks/{conftest,test_repositories}.py` both import the in-memory outbox and are the most likely orphan.
- [ ] `git log --stat` review confirms the deletion commit removed `src/eventsource/repositories/` (1,202 lines across three files) and that no unrelated file rode along.

## Plan Self-Review

- **Spec coverage.** Every section of the spec has a task. §2.1's four modules: Task 1 creates `ports/outbox.py` partially and Task 3 completes it plus the three adapters. §2.2's per-backend rationale: Task 3 Step 3's structure and ADR 0026 item 2. §2.3's consolidation decision: Task 2 in full, with the accepted debt recorded in both its commit body and ADR 0026 item 4. §2.4's payload pin: Task 1, with the table-contract half in Task 3 Step 3's module docstrings. §2.5's dying names: Task 3 Steps 2, 3, 5, pinned by `test_public_api.py` in Step 1. §2.6's export table: Task 3's Interfaces block. §2.7's contracts: Task 3 Step 8, including the verified-not-assumed mutmut check. §3's five deltas each appear in the task that first encounters them and again in Task 5's changelog. §4.1's conformance and §4.2's two properties: Task 4. §4.3's test dispositions: Task 3 Step 7 and Task 4 Step 2. §5's out-of-scope list is not touched by any task. §6's ADR Impact and §7's five risks are Task 5 Step 1 and, respectively, Task 2 Step 4 (risk 1), Task 3 Step 7 (risks 2 and 3), Task 3 Step 8 (risk 4), and Task 3 Step 6 (risk 5).
- **Placeholder scan.** No TODO, TBD, or `???` remains. Every code block is complete or is a bounded instruction over code the implementer is reading anyway. The six places where reality may differ from this plan — the `model_dump(mode="json")` equivalence (Task 1 Step 1), the bare `execute_with_connection` call sites (Task 2 preamble), whether `readmodels`' tests patch the same seam (Task 2 Step 3), whether `adapters/sqlite/outbox.py` imports aiosqlite at module scope (Task 3 Step 3), whether `test_json.py:652`'s case is redundant with a nearby one (Task 3 Step 7), and whether 0026 is still the free ADR number (Task 5 Step 1) — each say to read first and what to do if reality differs.
- **Signature consistency.** `outbox_event_data(event: DomainEvent) -> dict[str, Any]` is spelled identically in Task 1's Interfaces block, its Step 3 code, and Task 3's full `ports/outbox.py`. The `OutboxRepository` Protocol in Task 3's Interfaces block carries seven methods and matches `repositories/outbox.py`'s surviving signatures byte-for-byte apart from the deleted eighth. `sql_connection(conn, *, write: bool)` matches `adapters/_sql/connection.py` at HEAD exactly; no new parameter is proposed for it. The three adapter constructors are unchanged from HEAD and are not restated, because nothing about them moves.
- **Every task leaves the tree importable, type-clean, and lint-imports green.** Task 1 is purely additive plus one call-site substitution. Task 2 is a rename across five modules with both helpers live throughout. Task 3 is one commit precisely because no partial ordering of the port, the swap, and the deletion is green — the argument is in the Global Constraints with its three constraints stated separately so a reviewer can check each — and its `pyproject.toml` edit is in the same commit as the deletion it describes, because import-linter fails on a contract row naming a module that does not exist. Task 4 is additive. Task 5 touches no importable code at all.

## Spec Gaps and Contradictions Found (report, do not silently deviate)

1. **The backlog item this slice implements specifies the wrong destination.** `BACKLOG.md`'s "Migrate outbox repository to ports/adapters" says the SQL implementation goes to `adapters/sql/outbox.py`, following checkpoint and DLQ. That is not possible without rewriting `SQLiteOutboxRepository` off aiosqlite and onto sqlalchemy, which is a behavior change to a working adapter with no caller requesting it. The spec (§2.2) and this plan land three driver-specific modules instead. Recorded in ADR 0026 rather than silently diverging, and the backlog item is deleted rather than edited (Task 5 Step 4).
2. **`docs/core-surface.md` step 4 offers a choice this slice cannot take.** It says to pick "either a backend-only subpackage under `repositories/`, or the shared SQL infrastructure directory at `adapters/_sql/`". The first option is unavailable, because the same document's step 5 and this slice both delete `repositories/` entirely. Only one of the two options was ever real once the package deletion was in scope. No deviation — the choice is made for us.
3. **`docs/core-surface.md` step 4 predicts a bare move breaks eight patch strings; the real count is larger and elsewhere.** It flags `tests/unit/test_connection_helper.py`'s eight `"eventsource.repositories._connection.isinstance"` patches. It does not mention the ~90 `"...<consumer>.execute_with_connection"` patches in the four migration test modules, which are the actual bulk of the churn (Task 2 Step 3) — and `test_connection_helper.py` is deleted rather than moved, so its eight strings never need updating at all.
4. **The `[tool.mutmut]` edit the task brief anticipated is not needed.** Verified at HEAD: `only_mutate` lists `src/eventsource/ports` and `src/eventsource/adapters` as whole directories, and `pytest_add_cli_args_test_selection` lists `tests/unit/ports/` and `tests/unit/adapters/`. The new modules and their tests are covered the moment they land in those paths, which is why Task 3 Step 7 and Task 4 place every new test there. Task 3 Step 8 still says to verify rather than assume, since another agent could narrow those entries.
5. **`tests/unit/repositories/` and `tests/repositories/` are both live and neither corresponds to the deleted source package.** The first holds dialect tests (and is named in the mutmut selection); the second holds SQLite checkpoint tests alongside the outbox tests this slice moves out. A sweep that deletes by name would take real coverage with it. Called out in Task 3 Steps 6 and 7.
6. **ADR 0016's tracing decision now means two different things in two rings.** ADR 0025 amended it so store adapters emit no per-operation spans; the outbox repositories keep theirs, unchanged by this slice. Nothing is wrong, but "does this adapter trace?" no longer has one answer, and ADR 0026 item 8 states the outbox side explicitly so the next reader does not infer the store rule applies everywhere under `adapters/`.
