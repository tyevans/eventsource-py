# M0: SQLAlchemy Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move every SQLite code path in the library off raw `aiosqlite` and onto SQLAlchemy async, so that repositories can enlist in a caller-owned transaction and SQLite transaction boundaries behave as the delivery-guarantee work assumes.

**Architecture:** One shared engine factory applies correct SQLite transaction control and PRAGMAs. A small dialect-adapter module absorbs the PostgreSQL/SQLite differences (UUID, timestamp, JSON representation). The three *repositories* that must participate in someone else's unit of work -- checkpoint, DLQ, outbox -- collapse from two backend classes each into a single dialect-aware class. The event store, snapshot store, and read-model repository keep separate SQLite classes but are ported from `aiosqlite` to SQLAlchemy.

**Tech Stack:** Python 3.12+, SQLAlchemy 2.0.44 async, asyncpg, aiosqlite (as a SQLAlchemy driver only), pytest with `asyncio_mode = "auto"`.

**Prerequisite for:** M1-M5 of the projection delivery guarantees work. See
`docs/superpowers/specs/2026-07-27-projection-delivery-guarantees-design.md`
and the milestone breakdown alongside it.

## Global Constraints

- No backward-compatibility burden: the library has no users. Prefer deleting a
  class over aliasing it.
- `py.typed` must keep passing `mypy src/eventsource/ --config-file=pyproject.toml`.
- `migrations/` SQL files are append-only by project convention; edit existing
  files only to correct factually wrong comments.
- Never call `commit()` or `rollback()` inside a repository method. Durability is
  the caller's decision. This rule is the entire point of M0.
- Optional dependencies stay guarded by `try/except ImportError` with
  `*_AVAILABLE` flags, matching existing project convention.
- Tests: `uv run pytest tests/unit/ -v` must stay green after every task.
  Integration tests (`-m postgres`) require Docker and may be deferred to the
  final task, but SQLite integration tests must pass throughout.
- Commit after every task. Use `--no-verify` (the repo's beads pre-commit hook
  fails in this worktree; see the M0 notes at the bottom).

## Verification gate (applies to Tasks 3 onward)

**Rationale, recorded because it drives the sequencing.** Tasks 1 and 2 found four
defects, every one of them caught by the controller hand-probing a subagent's
claim rather than by the test suite. That does not scale to the remaining tasks
(~2,500 lines, seven classes, 20+ test files), and it is a single point of failure
— one of the controller's own probes was itself wrong and reported a clean result
where three divergences existed. Machine-checkable evidence is what makes
delegated work verifiable without the controller re-deriving each claim.

Therefore Tasks 2c (Hypothesis) and 2d (mutmut) run **before Task 3**, and from
Task 3 onward every task's definition of done includes:

1. **A mutation run over the modules the task touched**, with every surviving
   mutant triaged in the task report as: real gap (write the killing test),
   equivalent mutant (record why), or out of scope (record why). A green test
   suite is not sufficient evidence of completion.
2. **Property tests for any pure function** the task introduces or ports —
   round-trips, encoder agreement, dialect symmetry.
3. **Break/restore evidence** for each new hand-written test, as already required.

**Where a full mutation run is impractical** — likely for
`stores/sqlite.py` at 1,098 lines — the requirement narrows to mutating only the
functions the task changed, via targeted `paths_to_mutate` or per-function
selection. Narrower is acceptable; skipping silently is not. If a task cannot
satisfy this, it says so in its report and explains why, rather than omitting it.

---

### Task 1: Shared async engine factory

**Files:**
- Create: `src/eventsource/engine.py`
- Modify: `src/eventsource/__init__.py` (add export)
- Test: `tests/unit/test_engine.py`

> **Amended during execution.** The implementation below shows a Python-version
> branch: native `connect_args={"autocommit": False}` on 3.12+, with an
> `isolation_level = None` + explicit `BEGIN` fallback below that. As shipped,
> the version branch was dropped and the `isolation_level = None` + explicit
> `BEGIN` recipe is used unconditionally. Both approaches pass the isolation
> test; the uniform recipe is SQLAlchemy's documented portable form and removes
> a branch that cannot be exercised on this interpreter. `_HAS_SQLITE_AUTOCOMMIT`
> does not exist in the shipped module. Treat the uniform recipe as the
> requirement.

**Interfaces:**
- Consumes: nothing (first task)
- Produces: `eventsource.engine.create_async_engine(url: str, **kwargs) -> AsyncEngine`
  and `SQLITE_PRAGMAS: dict[str, str | int]`. Every later task uses this factory
  instead of `sqlalchemy.ext.asyncio.create_async_engine` directly.

**Why this exists:** sqlite3's legacy transaction control does not emit `BEGIN`
for `SELECT`, so a read-then-write sequence on one connection is not isolated.
No `connect_args`, `isolation_level`, or `do_begin` hook exists anywhere in the
project today. Without this, the M1 ledger's claim-then-dispatch sequence would
be silently non-transactional on SQLite.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_engine.py
"""Unit tests for the shared async engine factory."""

import pytest
from sqlalchemy import text

from eventsource.engine import create_async_engine


async def test_sqlite_engine_holds_read_write_in_one_transaction(tmp_path):
    """A SELECT then INSERT on one connection must roll back together.

    Under sqlite3's legacy isolation this fails: the SELECT runs outside any
    transaction, so the driver may commit implicitly before the rollback.
    """
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db")
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))

    conn = await engine.connect()
    try:
        await conn.begin()
        await conn.execute(text("SELECT COUNT(*) FROM t"))
        await conn.execute(text("INSERT INTO t (id) VALUES (1)"))
        await conn.rollback()
    finally:
        await conn.close()

    async with engine.connect() as conn:
        count = (await conn.execute(text("SELECT COUNT(*) FROM t"))).scalar_one()
    assert count == 0, "INSERT survived rollback: transaction control is wrong"
    await engine.dispose()


async def test_sqlite_engine_applies_pragmas(tmp_path):
    """WAL mode and foreign keys must be on for every pooled connection."""
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/t.db")
    async with engine.connect() as conn:
        journal = (await conn.execute(text("PRAGMA journal_mode"))).scalar_one()
        fk = (await conn.execute(text("PRAGMA foreign_keys"))).scalar_one()
    assert journal.lower() == "wal"
    assert fk == 1
    await engine.dispose()


async def test_memory_sqlite_shares_one_connection():
    """:memory: must use StaticPool or each pooled connection gets its own DB."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
    async with engine.connect() as conn:
        # Would raise OperationalError: no such table, on a fresh connection.
        await conn.execute(text("SELECT COUNT(*) FROM t"))
    await engine.dispose()


def test_postgres_url_passes_through_without_sqlite_config():
    """Non-SQLite URLs must not get SQLite connect_args."""
    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db")
    assert engine.dialect.name == "postgresql"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_engine.py -v --no-cov`
Expected: FAIL, `ModuleNotFoundError: No module named 'eventsource.engine'`

- [ ] **Step 3: Write the implementation**

```python
# src/eventsource/engine.py
"""
Shared async engine factory.

All SQLAlchemy engines used by this library -- and by its tests -- must come
from here rather than from ``sqlalchemy.ext.asyncio.create_async_engine``
directly. The factory applies the SQLite transaction control and PRAGMA setup
that the library's transactional guarantees depend on.

Why this is not optional for SQLite:

The stdlib ``sqlite3`` driver's legacy transaction control does not emit BEGIN
before a SELECT, so reads are not repeatable inside what the application
believes is a transaction, and it may commit implicitly at points the caller
did not choose. Projections rely on a read-then-write-then-commit sequence
being atomic, so the driver must be put under explicit transaction control.

See https://docs.sqlalchemy.org/en/20/dialects/sqlite.html
"""

import logging
import sys
from typing import Any

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import create_async_engine as _sa_create_async_engine
from sqlalchemy.pool import StaticPool

logger = logging.getLogger(__name__)

# Applied to every SQLite connection as it is checked out of the pool.
SQLITE_PRAGMAS: dict[str, str | int] = {
    "foreign_keys": "ON",
    "journal_mode": "WAL",
    "busy_timeout": 5000,
}

# sqlite3 gained a real ``autocommit`` attribute in Python 3.12. Below that we
# fall back to the portable isolation_level=None + explicit BEGIN pattern.
_HAS_SQLITE_AUTOCOMMIT = sys.version_info >= (3, 12)


def _configure_sqlite(engine: AsyncEngine, *, is_memory: bool) -> None:
    """Attach PRAGMA and transaction-control hooks to a SQLite engine."""

    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragmas(dbapi_connection: Any, _record: Any) -> None:
        cursor = dbapi_connection.cursor()
        try:
            for pragma, value in SQLITE_PRAGMAS.items():
                # :memory: databases cannot use WAL; skip it rather than error.
                if is_memory and pragma == "journal_mode":
                    continue
                cursor.execute(f"PRAGMA {pragma} = {value}")
        finally:
            cursor.close()
        if not _HAS_SQLITE_AUTOCOMMIT:
            dbapi_connection.isolation_level = None

    if not _HAS_SQLITE_AUTOCOMMIT:

        @event.listens_for(engine.sync_engine, "begin")
        def _emit_begin(conn: Any) -> None:
            conn.exec_driver_sql("BEGIN")


def create_async_engine(url: str, **kwargs: Any) -> AsyncEngine:
    """
    Create an AsyncEngine configured for this library's guarantees.

    For SQLite URLs this enables explicit transaction control, WAL mode,
    foreign keys, and a busy timeout, and uses StaticPool for ``:memory:``
    databases so that every checkout sees the same database.

    For all other dialects this is a thin passthrough to SQLAlchemy.

    Args:
        url: SQLAlchemy database URL.
        **kwargs: Passed through to ``create_async_engine``. Caller-supplied
                  ``connect_args`` are merged with, and take precedence over,
                  the SQLite defaults.

    Returns:
        A configured AsyncEngine.
    """
    is_sqlite = url.startswith("sqlite")
    if not is_sqlite:
        return _sa_create_async_engine(url, **kwargs)

    is_memory = ":memory:" in url
    connect_args: dict[str, Any] = dict(kwargs.pop("connect_args", {}))
    if _HAS_SQLITE_AUTOCOMMIT:
        connect_args.setdefault("autocommit", False)

    if is_memory:
        kwargs.setdefault("poolclass", StaticPool)

    engine = _sa_create_async_engine(url, connect_args=connect_args, **kwargs)
    _configure_sqlite(engine, is_memory=is_memory)
    logger.debug(
        "Created SQLite engine (memory=%s, native_autocommit=%s)",
        is_memory,
        _HAS_SQLITE_AUTOCOMMIT,
    )
    return engine


__all__ = ["SQLITE_PRAGMAS", "create_async_engine"]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_engine.py -v --no-cov`
Expected: 4 passed.

If `test_sqlite_engine_applies_pragmas` fails on `journal_mode` for a file
database, check that the `connect` hook is attached to `engine.sync_engine`, not
to the `AsyncEngine`. Event listeners do not fire on the async facade.

- [ ] **Step 5: Export it**

Add to `src/eventsource/__init__.py`, alongside the existing exports:

```python
from eventsource.engine import create_async_engine
```

and add `"create_async_engine"` to `__all__`.

- [ ] **Step 6: Verify the export and type-check**

Run: `uv run python -c "from eventsource import create_async_engine; print(create_async_engine)"`
Expected: prints a function reference.

Run: `uv run mypy src/eventsource/engine.py --config-file=pyproject.toml`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add src/eventsource/engine.py src/eventsource/__init__.py tests/unit/test_engine.py
git commit --no-verify -m "feat: add shared async engine factory with SQLite transaction control"
```

---

### Task 2: Dialect adapter

**Files:**
- Create: `src/eventsource/repositories/_dialect.py`
- Test: `tests/unit/repositories/test_dialect.py`

**Interfaces:**
- Consumes: nothing from Task 1 at runtime; tests use `create_async_engine`.
- Produces:
  - `class Dialect(Enum): POSTGRESQL, SQLITE`
  - `def dialect_of(conn: AsyncConnection) -> Dialect`
  - `def uuid_param(value: UUID | None, dialect: Dialect) -> str | UUID | None`
  - `def uuid_result(value: object) -> UUID | None`
  - `def ts_param(value: datetime, dialect: Dialect) -> str | datetime`
  - `def ts_result(value: object) -> datetime | None`
  - `def json_param(value: object, dialect: Dialect) -> str`
  - `def json_result(value: object) -> Any`
  - `def now_expr(dialect: Dialect) -> str` -- returns `"NOW()"` or `"CURRENT_TIMESTAMP"`

Tasks 3-8 import these instead of branching on backend inline.

**Why this exists:** the two backends differ in exactly four ways across every
repository -- UUID storage (native `UUID` vs 36-char `TEXT`), timestamps
(`TIMESTAMPTZ` vs ISO-8601 `TEXT`), JSON (`JSONB` vs `TEXT`), and the current-time
expression. Centralizing these keeps the merged repositories readable.

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/repositories/test_dialect.py
"""Unit tests for the SQL dialect adapter."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from eventsource.repositories._dialect import (
    Dialect,
    json_param,
    json_result,
    now_expr,
    ts_param,
    ts_result,
    uuid_param,
    uuid_result,
)


def test_uuid_param_postgres_passes_through():
    value = uuid4()
    assert uuid_param(value, Dialect.POSTGRESQL) is value


def test_uuid_param_sqlite_stringifies():
    value = uuid4()
    assert uuid_param(value, Dialect.SQLITE) == str(value)


def test_uuid_param_handles_none():
    assert uuid_param(None, Dialect.SQLITE) is None
    assert uuid_param(None, Dialect.POSTGRESQL) is None


def test_uuid_result_accepts_both_representations():
    value = uuid4()
    assert uuid_result(value) == value
    assert uuid_result(str(value)) == value
    assert uuid_result(None) is None


def test_ts_param_sqlite_is_iso_and_roundtrips():
    value = datetime(2026, 7, 28, 12, 30, tzinfo=UTC)
    encoded = ts_param(value, Dialect.SQLITE)
    assert isinstance(encoded, str)
    assert ts_result(encoded) == value


def test_ts_result_attaches_utc_to_naive_values():
    """SQLite returns naive strings; comparisons must not raise."""
    result = ts_result("2026-07-28T12:30:00")
    assert result is not None
    assert result.tzinfo is UTC


def test_json_roundtrip_sqlite():
    payload = {"a": 1, "b": ["x"]}
    assert json_result(json_param(payload, Dialect.SQLITE)) == payload


def test_now_expr_per_dialect():
    assert now_expr(Dialect.POSTGRESQL) == "NOW()"
    assert now_expr(Dialect.SQLITE) == "CURRENT_TIMESTAMP"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/repositories/test_dialect.py -v --no-cov`
Expected: FAIL, `ModuleNotFoundError: No module named 'eventsource.repositories._dialect'`

- [ ] **Step 3: Write the implementation**

```python
# src/eventsource/repositories/_dialect.py
"""
Dialect differences between PostgreSQL and SQLite.

Repositories in this package serve both backends from a single implementation.
The backends differ in four ways that reach the SQL and the bound parameters:

- UUID: PostgreSQL has a native type; SQLite stores 36-character TEXT.
- Timestamps: PostgreSQL has TIMESTAMPTZ; SQLite stores ISO-8601 TEXT and
  returns it without timezone information.
- JSON: PostgreSQL has JSONB; SQLite stores TEXT.
- Current time: NOW() versus CURRENT_TIMESTAMP.

Everything else in our SQL (ON CONFLICT, RETURNING) is supported by both --
PostgreSQL 9.5+ and SQLite 3.35+ respectively.
"""

import json
from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncConnection


class Dialect(Enum):
    """Supported SQL dialects."""

    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"


def dialect_of(conn: AsyncConnection) -> Dialect:
    """
    Determine the dialect of a live connection.

    Args:
        conn: An active SQLAlchemy async connection.

    Returns:
        The matching Dialect.

    Raises:
        ValueError: If the dialect is not supported by this library.
    """
    name = conn.dialect.name
    try:
        return Dialect(name)
    except ValueError:
        raise ValueError(
            f"Unsupported SQL dialect {name!r}. "
            f"Supported dialects: {[d.value for d in Dialect]}"
        ) from None


def uuid_param(value: UUID | None, dialect: Dialect) -> str | UUID | None:
    """Encode a UUID for binding as a query parameter."""
    if value is None:
        return None
    return str(value) if dialect is Dialect.SQLITE else value


def uuid_result(value: object) -> UUID | None:
    """Decode a UUID from a result row, accepting either representation."""
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def ts_param(value: datetime, dialect: Dialect) -> str | datetime:
    """Encode a datetime for binding as a query parameter."""
    return value.isoformat() if dialect is Dialect.SQLITE else value


def ts_result(value: object) -> datetime | None:
    """
    Decode a timestamp from a result row.

    SQLite returns naive ISO-8601 strings. We attach UTC rather than returning
    a naive datetime, so that callers can always compare results safely.
    """
    if value is None:
        return None
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def json_param(value: object, dialect: Dialect) -> str:
    """Encode a JSON-serializable value for binding as a query parameter."""
    return json.dumps(value)


def json_result(value: object) -> Any:
    """Decode a JSON value from a result row, accepting text or parsed JSON."""
    if value is None:
        return None
    if isinstance(value, str | bytes):
        return json.loads(value)
    return value


def now_expr(dialect: Dialect) -> str:
    """SQL expression for the current timestamp in this dialect."""
    return "NOW()" if dialect is Dialect.POSTGRESQL else "CURRENT_TIMESTAMP"


__all__ = [
    "Dialect",
    "dialect_of",
    "json_param",
    "json_result",
    "now_expr",
    "ts_param",
    "ts_result",
    "uuid_param",
    "uuid_result",
]
```

Note `json_param` ignores `dialect` deliberately: asyncpg's JSONB binding also
accepts a JSON string, so one encoding serves both. The parameter is kept for
call-site symmetry with the other adapters.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/repositories/test_dialect.py -v --no-cov`
Expected: 8 passed.

- [ ] **Step 5: Type-check and commit**

Run: `uv run mypy src/eventsource/repositories/_dialect.py --config-file=pyproject.toml`
Expected: no errors.

```bash
git add src/eventsource/repositories/_dialect.py tests/unit/repositories/test_dialect.py
git commit --no-verify -m "feat: add PostgreSQL/SQLite dialect adapter for repositories"
```

---

### Task 2b: orjson as an optional encoder accelerator

**Added during execution** (user decision, 2026-07-29). Sequenced here because Task 2
made `json_param` delegate to the project encoder, which turns this into a
single-site change instead of a six-repository change.

**Files:**
- Modify: `src/eventsource/serialization/json.py` (100 lines; `json_dumps` at :59, `json_loads` at :79)
- Modify: `src/eventsource/repositories/_dialect.py` (`json_result` call site)
- Modify: `pyproject.toml` (optional extra)
- Test: `tests/unit/serialization/test_json.py`, `tests/unit/repositories/test_dialect.py`

**Interfaces:**
- Consumes: `json_dumps` / `json_loads` from Task 2's usage.
- Produces: unchanged public signatures — `json_dumps(obj: Any) -> str`,
  `json_loads(s: str) -> Any` — plus `ORJSON_AVAILABLE: bool`.

**Design constraints, all load-bearing:**

- **Optional, not core.** orjson is a compiled Rust extension. Core deps are
  deliberately `pydantic` + `sqlalchemy` only, and `docs/core-surface.md` records
  `serialization/` as cleanly Tier 0 (stdlib-only). Add orjson as an extra, guarded
  by `try/except ImportError` with an `ORJSON_AVAILABLE` flag, matching the
  convention already used for redis/asyncpg/aiosqlite/kafka. Tier 0 must still hold
  when orjson is absent.
- **`json_dumps` must keep returning `str`.** `orjson.dumps` returns `bytes`; decode
  at the boundary. Changing the signature would ripple into every repository.
- **The split-decoder trap.** `json_loads` is currently a plain `json.loads` alias, so
  `_dialect.json_result` calls stdlib directly. The moment `json_loads` reroutes to
  orjson, `json_result` must be switched to call the *project* loader, or `_dialect`
  silently keeps decoding with stdlib while everything else moves — one module, two
  decoders. This is invisible to output-equality tests, so it needs a test that
  asserts the routing itself (monkeypatch `eventsource.serialization.json_loads`,
  prove `json_result` calls it).
- **Parity, not just speed.** These payloads persist in `event_outbox` and
  `dead_letter_queue`. Format drift means rows written by different builds disagree.
  orjson is stricter than stdlib: non-`str` dict keys need `OPT_NON_STR_KEYS`, and
  `str`/`int` subclasses are handled differently. It serializes UUID and datetime
  natively, which makes `EventSourceJSONEncoder` largely redundant — "largely" being
  exactly where drift hides.

- [ ] **Step 1: Write parity tests first, before touching the encoder.**

Parametrize over both encoders and assert byte-identical output for: a UUID; a
timezone-aware datetime; a naive datetime; a nested dict/list structure; an empty
dict and empty list; `None`; a non-`str` dict key (int and UUID keys); a `str`
subclass; a `Decimal` if the existing encoder supports one. Any case where the two
disagree is a decision to make explicitly and document — not a test to loosen.

- [ ] **Step 2: Run them against the stdlib encoder alone**

Expected: all pass (both parametrizations resolve to stdlib while orjson is unwired).
This proves the harness works before it has anything to catch.

- [ ] **Step 3: Add the optional dependency**

```toml
# pyproject.toml, in [project.optional-dependencies]
orjson = ["orjson>=3.9"]
```
Add `orjson` to the `all` extra if one exists.

- [ ] **Step 4: Wire the accelerator**

```python
try:
    import orjson

    ORJSON_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised in the no-orjson environment
    ORJSON_AVAILABLE = False
```

`json_dumps` uses `orjson.dumps(obj, default=_fallback).decode()` when available,
else the existing `json.dumps(obj, cls=EventSourceJSONEncoder)`. `_fallback` handles
whatever `EventSourceJSONEncoder` handles that orjson does not. `json_loads` uses
`orjson.loads` when available.

- [ ] **Step 5: Run the parity suite with orjson installed**

Every case must pass. Where a case fails, do not weaken the assertion — either add
the matching `OPT_*` flag / `default=` handler so the output matches stdlib, or, if
matching is genuinely undesirable, record the divergence in the test as an explicit
expected difference with a comment explaining why it is safe for persisted payloads.

- [ ] **Step 6: Fix the split-decoder trap**

Switch `_dialect.json_result` to the project `json_loads`, and add the routing test
described above.

- [ ] **Step 7: Prove the fallback path**

Run the suite with orjson forced unavailable (monkeypatch `ORJSON_AVAILABLE` to
`False`, or use an env marker) and confirm everything still passes. A fallback that
is never exercised is not a fallback.

- [ ] **Step 8: Verify, lint, commit**

```bash
uv run pytest tests/unit/serialization/ tests/unit/repositories/test_dialect.py -v --no-cov
uv run ruff check src/ tests/ --fix && uv run ruff format src/ tests/
uv run mypy src/eventsource/ --config-file=pyproject.toml
git commit --no-verify -m "perf: use orjson as an optional encoder accelerator"
```

**Opportunistic cleanup, if it stays trivial:** `src/eventsource/repositories/_json.py`
is a deprecation shim whose docstring says it "will be removed in version 0.4.0"; the
project is at 0.5.0 and no production code imports it (only four tests asserting the
warning fires). Deleting it and those tests is safe — there are no users. If it turns
out to be entangled, leave it and report rather than expanding this task.

---

### Task 2c: Property-based tests for the encoder and dialect adapter

**Added during execution** (user decision, 2026-07-29). Runs after Task 2b closes.

**Rationale.** Three of the four defects found so far in this milestone were
case-coverage gaps, not logic errors: the parity suite covered UUID and datetime
but not non-ASCII; `uuid_result` covered `str` and `UUID` but not `bytes`. Both
classes are what property-based testing generates for free. Hypothesis emits
unicode, surrogates, and astral-plane codepoints by default — the `ensure_ascii`
drift would have surfaced on the first run.

**Honest limit, recorded so nobody over-trusts this:** Hypothesis would NOT have
caught Task 1's vacuous isolation test, which had a wrong oracle rather than
missing cases. Property tests find coverage gaps; mutation testing finds oracle
errors. The break/restore discipline stays in force alongside this.

**Files:**
- Modify: `pyproject.toml` (test-only dependency + Hypothesis profiles)
- Create: `tests/unit/serialization/test_json_properties.py`
- Create: `tests/unit/repositories/test_dialect_properties.py`
- Create/modify: `tests/conftest.py` (profile registration)

**Interfaces:**
- Consumes: `json_dumps`/`json_loads` and `ORJSON_AVAILABLE` (Task 2b);
  `uuid_param`/`uuid_result`/`ts_param`/`ts_result`/`json_param`/`json_result`
  (Tasks 2, 2b).
- Produces: no library API. Test infrastructure only.

- [ ] **Step 1: Add the dependency and profiles**

Add `hypothesis>=6.100` to the dev/test dependency group (NOT to runtime deps, and
NOT to the `orjson` extra). Register profiles in `tests/conftest.py`:

```python
from hypothesis import HealthCheck, settings

settings.register_profile("default", max_examples=100)
settings.register_profile("ci", max_examples=500, deadline=None)
settings.register_profile(
    "db", max_examples=25, deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
settings.load_profile("default")
```

Commit `.hypothesis/examples/` rather than gitignoring it, so a discovered
counterexample is replayed deterministically on every later run instead of being
rediscovered by luck.

- [ ] **Step 2: Encoder parity property**

```python
json_values = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False, allow_infinity=False) | st.text(),
    lambda children: st.lists(children) | st.dictionaries(st.text(), children),
    max_leaves=20,
)

@given(payload=json_values)
def test_orjson_and_fallback_agree(payload, force_fallback):
    """Both encoder branches must produce byte-identical output."""
    with_orjson = json_dumps(payload)
    with force_fallback():
        without_orjson = json_dumps(payload)
    assert with_orjson == without_orjson
```

`force_fallback` must toggle the real `ORJSON_AVAILABLE` switch. Do NOT
reconstruct what stdlib "would" produce by hand-calling `json.dumps` with the
right flags — that tests a reimplementation rather than the branch, reports zero
divergences, and is exactly the mistake that hid the non-ASCII drift.

Add a second property: `json_loads(json_dumps(x)) == x` for JSON-native values.

- [ ] **Step 3: Dialect adapter round-trip properties**

```python
@given(value=st.uuids())
@pytest.mark.parametrize("dialect", [Dialect.POSTGRESQL, Dialect.SQLITE])
def test_uuid_roundtrip(value, dialect):
    assert uuid_result(uuid_param(value, dialect)) == value

@given(value=st.datetimes(timezones=st.timezones()))
def test_ts_roundtrip_sqlite_preserves_instant(value):
    assert ts_result(ts_param(value, Dialect.SQLITE)) == value
```

Also property-test `uuid_result` across `str`, `UUID`, `bytes`, `bytearray`, and
`memoryview` representations of the same UUID, asserting all five agree.

Naive datetimes are documented as coming back UTC-attached, so either exclude them
from the equality property or assert the documented behavior explicitly — do not
loosen the property to make both pass.

- [ ] **Step 4: Verify the properties can fail**

For each property, break the implementation it covers, confirm Hypothesis reports a
counterexample, restore, confirm it passes. Record the counterexample Hypothesis
found in each case — a property that cannot produce one is as useless as a test
that cannot fail.

- [ ] **Step 5: Verify, lint, commit**

```bash
uv run pytest tests/unit/serialization/ tests/unit/repositories/ -q --no-cov
uv run ruff check src/ tests/ --fix && uv run ruff format src/ tests/
git add .hypothesis/examples
git commit --no-verify -m "test: add property-based tests for encoder parity and dialect round-trips"
```

**Deferred to M2, recorded here so it is not lost:** the delivery-guarantee work's
central claim is itself a property — *applying event sequence E with arbitrary
redeliveries and crash points yields the same read model as applying E once.* That
is a Hypothesis `RuleBasedStateMachine` generating deliver/crash/redeliver
interleavings, and it is a far better test of exactly-once semantics than any
hand-enumerated scenario list, because the dangerous interleavings are precisely
the ones nobody thinks to write down. Use the `db` profile there; each example
costs real transactions.

---

### Task 2d: Mutation testing on the core purity set

**Added during execution** (user decision, 2026-07-29). Runs after Task 2c.

**Rationale.** Property tests find case-coverage gaps; mutation tests find oracle
errors. This milestone has produced one of each, and only the coverage gaps were
caught by adding cases. Task 1's isolation test passed against an engine with none
of its transaction control applied — a mutation run deleting
`conn.exec_driver_sql("BEGIN")` would have reported a surviving mutant immediately.
Every manual break/restore in this milestone's ledger is a hand-run mutation test;
this automates them.

**Scope is the whole design.** Mutation testing is O(mutants x suite runtime). This
repo is ~18k source lines with ~6,000 tests; a whole-repo run is hours and would go
unused. Target a curated set of small, pure, high-consequence modules, each pinned
to its own fast test subset:

| Module | Lines | Test subset |
| --- | --- | --- |
| `src/eventsource/engine.py` | ~110 | `tests/unit/test_engine.py` |
| `src/eventsource/repositories/_dialect.py` | ~130 | `tests/unit/repositories/test_dialect.py` |
| `src/eventsource/serialization/json.py` | ~100 | `tests/unit/serialization/` |

**Files:**
- Modify: `pyproject.toml` (dev dependency + `[tool.mutmut]` config)
- Create: `scripts/mutation.sh` (or a documented `just`/`make` target if the repo
  has one — check before adding a new script convention)
- Create: `docs/development/mutation-testing.md`

**Interfaces:**
- Consumes: the test suites from Tasks 1, 2, 2b, 2c.
- Produces: no library API. A repeatable command and a triage record.

- [ ] **Step 1: Add the dependency**

Add `mutmut>=3.0` to the dev/test dependency group. Not a runtime dep, not in any
published extra.

- [ ] **Step 2: Configure per-module targeting**

```toml
[tool.mutmut]
paths_to_mutate = [
    "src/eventsource/engine.py",
    "src/eventsource/repositories/_dialect.py",
    "src/eventsource/serialization/json.py",
]
tests_dir = "tests/unit/"
```

Verify the installed mutmut version's config schema before writing this — mutmut 3
changed its configuration surface from 2.x, and a config the tool silently ignores
is worse than none. Confirm by running it and checking that it mutates only the
three files.

The runner must invoke pytest with `--no-cov -x -q` and the narrowest test subset
per module. Coverage instrumentation on every mutant is pure waste.

- [ ] **Step 3: Establish and record a baseline**

Run mutation on all three modules. Record in
`docs/development/mutation-testing.md`: total mutants, killed, survived, timeout,
and the wall-clock runtime per module. If any module takes more than a few minutes,
narrow its test subset rather than accepting a run nobody will wait for.

- [ ] **Step 4: Triage every surviving mutant individually**

This is the deliverable, not the score. For each survivor, classify it:

- **Real gap** — the mutant changes behavior and no test notices. Write the test
  that kills it. These are the finds that justify the tool.
- **Equivalent mutant** — semantically identical to the original (e.g. a changed
  constant that no observable behavior depends on). Record it as equivalent with a
  one-line reason. Do NOT contort a test to kill it.
- **Out of scope** — mutating a docstring, a log message, or a defensive branch that
  cannot be reached. Record and move on.

Write the classification into the doc, mutant by mutant. A bare score with no
triage is not a result.

- [ ] **Step 5: Do NOT gate CI on a mutation score**

Explicitly out of scope, and record why in the doc: equivalent mutants make 100%
unreachable, so any threshold is arbitrary, and a slow non-deterministic gate turns
a diagnostic into a flaky blocker that gets disabled within a month. This runs on
demand and when the curated modules change. Revisit only if the survivor count
stays at zero across several milestones.

- [ ] **Step 6: Verify the harness catches a known-vacuous test**

The honest test of this tool is whether it would have caught what we already know
was wrong. Temporarily restore Task 1's original single-connection isolation test
(the vacuous one — see `git show` on the commit that replaced it, and the ledger
entry describing it), run mutation on `engine.py`, and confirm a surviving mutant
appears for the deleted `BEGIN`. Then restore the real test. Record the output.

If it does NOT surface that mutant, the configuration is wrong and everything above
is theatre — fix the config before proceeding.

- [ ] **Step 7: Document and commit**

`docs/development/mutation-testing.md` states: what is in the curated set and why,
how to run it, how to read a survivor, the triage classifications from Step 4, and
the explicit non-goal of a CI score gate.

```bash
uv run ruff check src/ tests/ scripts/ --fix && uv run ruff format src/ tests/
git commit --no-verify -m "test: add scoped mutation testing for the core purity set"
```

**As Tasks 3-8 land**, the merged repositories become candidates for the curated
set — but only with per-module test targeting, and only if a run stays in the tens
of seconds. Add them one at a time, never by widening `paths_to_mutate` to a
directory.

---

### Task 3: Unify the checkpoint repository

**Files:**
- Modify: `src/eventsource/repositories/checkpoint.py` (replace
  `PostgreSQLCheckpointRepository` at line 191 and `SQLiteCheckpointRepository`
  at line 735 with one class; keep `InMemoryCheckpointRepository` unchanged)
- Modify: `src/eventsource/repositories/__init__.py` (exports)
- Modify: `src/eventsource/__init__.py` (exports)
- Test: `tests/unit/test_checkpoint_repository.py`,
  `tests/unit/repositories/test_checkpoint_tracing.py`,
  `tests/integration/repositories/test_checkpoint.py`

**Interfaces:**
- Consumes: `create_async_engine` (Task 1); `Dialect`, `dialect_of`,
  `uuid_param`, `uuid_result`, `ts_param`, `ts_result`, `now_expr` (Task 2).
- Produces: `SQLCheckpointRepository(conn: AsyncConnection | AsyncEngine, tracer=None, enable_tracing=True)`
  implementing the existing `CheckpointRepository` protocol unchanged:
  `get_checkpoint`, `update_checkpoint`, `get_lag_metrics`, `reset_checkpoint`,
  `get_position`, `save_position`, `get_all_checkpoints`.

Method signatures do not change in this task. The `conn=` enlistment kwarg is
M1 Task 6, deliberately separated so this task is a pure port.

**This is the reference task.** Tasks 4-8 follow the same shape; read this one
first even if assigned another.

- [ ] **Step 1: Write the failing test**

Add to `tests/unit/test_checkpoint_repository.py`:

```python
class TestSQLCheckpointRepository:
    """The unified repository must behave identically on both dialects."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        from eventsource.engine import create_async_engine
        from eventsource.migrations import get_schema

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/cp.db")
        async with engine.begin() as conn:
            for statement in get_schema("checkpoints", backend="sqlite").split(";"):
                if statement.strip():
                    await conn.execute(text(statement))
        yield engine
        await engine.dispose()

    async def test_update_and_get_checkpoint(self, sqlite_engine):
        from eventsource.repositories.checkpoint import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        event_id = uuid4()
        await repo.update_checkpoint("Proj", event_id, "Created")
        assert await repo.get_checkpoint("Proj") == event_id

    async def test_repository_does_not_commit(self, sqlite_engine):
        """A repository write must roll back with the caller's transaction.

        This is the regression test for the old SQLiteCheckpointRepository,
        which called connection.commit() inside update_checkpoint.
        """
        from eventsource.repositories.checkpoint import SQLCheckpointRepository

        conn = await sqlite_engine.connect()
        try:
            await conn.begin()
            repo = SQLCheckpointRepository(conn)
            await repo.update_checkpoint("Proj", uuid4(), "Created")
            await conn.rollback()
        finally:
            await conn.close()

        repo = SQLCheckpointRepository(sqlite_engine)
        assert await repo.get_checkpoint("Proj") is None

    async def test_save_and_get_position(self, sqlite_engine):
        from eventsource.repositories.checkpoint import SQLCheckpointRepository

        repo = SQLCheckpointRepository(sqlite_engine)
        await repo.save_position("sub-1", 42, uuid4(), "Created")
        assert await repo.get_position("sub-1") == 42
```

Add `from sqlalchemy import text` to the test module imports.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_checkpoint_repository.py -v --no-cov -k SQLCheckpoint`
Expected: FAIL, `ImportError: cannot import name 'SQLCheckpointRepository'`

- [ ] **Step 3: Write the implementation**

Replace both backend classes with a single `SQLCheckpointRepository`. Take the
existing `PostgreSQLCheckpointRepository` body as the starting point -- it is
already SQLAlchemy Core with `text()` -- and apply these changes:

1. Rename the class to `SQLCheckpointRepository`.
2. At the top of each method, resolve the dialect once:

```python
# Read methods pass write=False; write methods pass write=True.
async with self._connect(write=False) as conn:
    dialect = dialect_of(conn)
```

3. Replace every hardcoded `NOW()` in SQL with `{now_expr(dialect)}` in an
   f-string, and every bound UUID with `uuid_param(value, dialect)`, every bound
   datetime with `ts_param(value, dialect)`.
4. Decode results with `uuid_result` / `ts_result` instead of assuming the
   PostgreSQL representation.
5. Delete `SQLiteCheckpointRepository` entirely, including its
   `aiosqlite` import and `AIOSQLITE_AVAILABLE` guard if nothing else in the
   module uses them.
6. Add the private connection helper, which replaces `execute_with_connection`
   for this repository:

```python
    @asynccontextmanager
    async def _connect(self, *, write: bool) -> AsyncIterator[AsyncConnection]:
        """
        Yield a connection to execute on.

        If this repository was constructed with a live connection, that
        connection is yielded directly and NOT committed -- the caller owns the
        transaction. If constructed with an engine, a connection is opened here
        and, for writes, committed on successful exit.
        """
        if isinstance(self._conn, AsyncEngine):
            if write:
                async with self._conn.begin() as conn:
                    yield conn
            else:
                async with self._conn.connect() as conn:
                    yield conn
        else:
            yield self._conn
```

The `SQLiteCheckpointRepository.update_checkpoint` call to
`await self._connection.commit()` (checkpoint.py:844) must not survive in any
form. That unconditional commit is what this milestone exists to remove.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_checkpoint_repository.py -v --no-cov`
Expected: all pass, including the pre-existing `InMemoryCheckpointRepository`
tests, which must be untouched.

- [ ] **Step 5: Update the remaining references**

Run: `grep -rn "PostgreSQLCheckpointRepository\|SQLiteCheckpointRepository" src/ tests/ docs/ examples/`

Update every hit to `SQLCheckpointRepository`. Where a test constructed the
SQLite repo from an `aiosqlite.Connection`, it now takes an engine from
`create_async_engine`. Where a fixture yielded an `aiosqlite.Connection`, it
yields an `AsyncEngine`.

- [ ] **Step 6: Run the full unit suite**

Run: `uv run pytest tests/unit/ -q --no-cov`
Expected: all pass, no collection errors.

Run: `uv run mypy src/eventsource/repositories/checkpoint.py --config-file=pyproject.toml`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add -A src/eventsource/repositories/checkpoint.py src/eventsource/repositories/__init__.py src/eventsource/__init__.py tests/
git commit --no-verify -m "refactor: unify checkpoint repository on SQLAlchemy, remove internal commit"
```

---

### Task 4: Unify the DLQ repository

**Files:**
- Modify: `src/eventsource/repositories/dlq.py` (replace
  `PostgreSQLDLQRepository` at line 259 and `SQLiteDLQRepository` at line 997
  with one class; keep `InMemoryDLQRepository` at line 642 unchanged)
- Modify: `src/eventsource/repositories/__init__.py`, `src/eventsource/__init__.py`
- Test: `tests/unit/test_dlq_repository.py`

**Interfaces:**
- Consumes: Task 2 adapters; the `_connect` helper pattern from Task 3.
- Produces: `SQLDLQRepository(conn: AsyncConnection | AsyncEngine, tracer=None, enable_tracing=True)`
  implementing the existing `DLQRepository` protocol unchanged, including
  `add_failed_event`.

Follow Task 3 step-for-step. The DLQ-specific differences:

- The error payload and event payload columns are JSON. Bind them with
  `json_param(value, dialect)` and decode with `json_result`.
- `DLQEntry` has more timestamp fields than the checkpoint row; route each
  through `ts_param` / `ts_result`.
- `DLQStats` aggregate queries may use PostgreSQL-only constructs. Check for
  `FILTER (WHERE ...)` and rewrite as `SUM(CASE WHEN ... THEN 1 ELSE 0 END)`,
  which both dialects accept. The existing `SQLiteDLQRepository` already contains
  a working SQLite formulation of these queries -- take its SQL, not the
  PostgreSQL version, wherever the two differ.

- [ ] **Step 1: Write failing tests** mirroring Task 3's three cases against
  `SQLDLQRepository`: roundtrip an entry, prove no internal commit, and query
  stats. Use `get_schema("dlq", backend="sqlite")` for the fixture.
- [ ] **Step 2: Run them, confirm ImportError.**
- [ ] **Step 3: Implement, deleting both backend classes.**
- [ ] **Step 4: Run `uv run pytest tests/unit/test_dlq_repository.py -v --no-cov`.**
- [ ] **Step 5: `grep -rn "PostgreSQLDLQRepository\|SQLiteDLQRepository" src/ tests/ docs/ examples/` and update all hits.**
- [ ] **Step 6: `uv run pytest tests/unit/ -q --no-cov` and mypy.**
- [ ] **Step 7: Commit** with `refactor: unify DLQ repository on SQLAlchemy`.

---

### Task 5: Unify the outbox repository

**Files:**
- Modify: `src/eventsource/repositories/outbox.py` (replace the PostgreSQL class
  and `SQLiteOutboxRepository` at line 754 with one class; keep the in-memory
  implementation)
- Modify: `src/eventsource/repositories/__init__.py`, `src/eventsource/__init__.py`
- Test: `tests/unit/test_outbox_repository.py`

**Interfaces:**
- Consumes: Task 2 adapters; Task 3's `_connect` pattern.
- Produces: `SQLOutboxRepository(conn: AsyncConnection | AsyncEngine, tracer=None, enable_tracing=True)`
  implementing the existing outbox protocol unchanged.

Outbox-specific differences from Task 3:

- The claim/lease query is the risky one. If the PostgreSQL implementation uses
  `FOR UPDATE SKIP LOCKED`, SQLite does not support it. Branch on dialect: keep
  `SKIP LOCKED` for PostgreSQL, and on SQLite rely on the single-writer lock,
  which makes the plain `UPDATE ... WHERE status = 'pending'` safe. Do not
  silently drop `SKIP LOCKED` for both -- that would degrade PostgreSQL
  concurrency.
- Payload columns are JSON; use `json_param` / `json_result`.

- [ ] **Step 1: Write failing tests** for roundtrip, no-internal-commit, and
  claiming a batch. Use `get_schema("outbox", backend="sqlite")`.
- [ ] **Step 2: Run them, confirm ImportError.**
- [ ] **Step 3: Implement, deleting both backend classes.**
- [ ] **Step 4: Run `uv run pytest tests/unit/test_outbox_repository.py -v --no-cov`.**
- [ ] **Step 5: `grep -rn "SQLiteOutboxRepository\|PostgreSQLOutboxRepository" src/ tests/ docs/ examples/` and update.**
- [ ] **Step 6: `uv run pytest tests/unit/ -q --no-cov` and mypy.**
- [ ] **Step 7: Commit** with `refactor: unify outbox repository on SQLAlchemy`.

---

### Task 6: Port the SQLite snapshot store

**Files:**
- Modify: `src/eventsource/snapshots/sqlite.py` (371 lines)
- Test: `tests/unit/snapshots/test_sqlite_snapshot_store.py`,
  `tests/unit/snapshots/test_snapshot_store_tracing.py`

**Interfaces:**
- Consumes: Task 1 `create_async_engine`; Task 2 adapters.
- Produces: `SQLiteSnapshotStore(engine: AsyncEngine, *, tracer=None, enable_tracing=True)`
  -- the class name stays; the constructor takes an `AsyncEngine` instead of a
  `database_path: str`. The `SnapshotStore` interface methods are unchanged.

Unlike Tasks 3-5 this class is **not** merged with its PostgreSQL counterpart.
Snapshot stores are not composed into a caller's unit of work, so the merge buys
nothing and risks the PostgreSQL-specific SQL. Port it to SQLAlchemy and stop.

Specific changes:

- Delete the `aiosqlite.connect` call and the `SQLiteNotAvailableError` raise
  path in `__init__` (line 43 defines the error class; keep the class if it is
  exported, but the store no longer raises it at construction).
- Replace `await self._connection.execute(sql, params)` with
  `await conn.execute(text(sql), params_dict)` -- note SQLAlchemy `text()` uses
  named `:param` placeholders, not `?` positional. Every query needs its
  placeholders converted.
- Replace `await self._connection.commit()` with an `engine.begin()` block.
- UUIDs and timestamps go through the Task 2 adapters.

- [ ] **Step 1: Write a failing test** constructing `SQLiteSnapshotStore` from an
  engine and round-tripping a snapshot.
- [ ] **Step 2: Run it, confirm failure** (`TypeError` on the constructor).
- [ ] **Step 3: Port the implementation.**
- [ ] **Step 4: Run `uv run pytest tests/unit/snapshots/ -v --no-cov`.**
- [ ] **Step 5: `grep -rn "SQLiteSnapshotStore" src/ tests/ docs/ examples/` and update construction sites.**
- [ ] **Step 6: `uv run pytest tests/unit/ -q --no-cov` and mypy.**
- [ ] **Step 7: Commit** with `refactor: port SQLite snapshot store to SQLAlchemy`.

---

### Task 7: Port the SQLite read-model repository

**Files:**
- Modify: `src/eventsource/readmodels/sqlite.py`
- Modify: `src/eventsource/readmodels/projection.py` (3 aiosqlite references)
- Test: `tests/unit/readmodels/test_sqlite.py`, `tests/unit/readmodels/test_schema.py`,
  `tests/unit/readmodels/test_soft_delete.py`,
  `tests/integration/readmodels/test_projection.py`,
  `tests/integration/readmodels/conftest.py`

**Interfaces:**
- Consumes: Task 1, Task 2.
- Produces: `SQLiteReadModelRepository(engine: AsyncEngine, ...)` -- name
  unchanged, constructor takes an engine.

Same shape as Task 6. Two extra hazards:

- `readmodels/projection.py` references `aiosqlite` directly; those must become
  SQLAlchemy connections passed through from the projection.
- `tests/integration/readmodels/test_projection.py:289,318,347` call
  `create_async_engine` from SQLAlchemy directly with no `connect_args`. Switch
  them to `eventsource.engine.create_async_engine`, or these tests will exercise
  the wrong transaction semantics and mask exactly the bug M0 exists to remove.

- [ ] **Step 1: Write a failing test** for engine-based construction and a
  read-model roundtrip.
- [ ] **Step 2: Run it, confirm failure.**
- [ ] **Step 3: Port the implementation and `readmodels/projection.py`.**
- [ ] **Step 4: Run `uv run pytest tests/unit/readmodels/ -v --no-cov`.**
- [ ] **Step 5: Update the integration fixtures and the three direct
  `create_async_engine` calls.**
- [ ] **Step 6: `uv run pytest tests/unit/ -q --no-cov` and mypy.**
- [ ] **Step 7: Commit** with `refactor: port SQLite read-model repository to SQLAlchemy`.

---

### Task 8: Port the SQLite event store

**Files:**
- Modify: `src/eventsource/stores/sqlite.py` (1098 lines -- the largest single
  port in this milestone)
- Test: `tests/unit/stores/test_sqlite_tracing.py`,
  `tests/unit/test_eventstore_global_position.py`, `tests/conftest.py`

**Interfaces:**
- Consumes: Task 1, Task 2.
- Produces: `SQLiteEventStore(engine: AsyncEngine, event_registry=None, *, tracer=None, enable_tracing=True, type_converter=None, uuid_fields=None, string_id_fields=None, auto_detect_uuid=True)`.

Note the constructor change: `database: str`, `wal_mode: bool`, and
`busy_timeout: int` are **removed**. WAL mode and busy timeout now come from
`create_async_engine`'s PRAGMA hook (Task 1), which is the whole point of having
the factory. Callers that passed `":memory:"` now pass
`create_async_engine("sqlite+aiosqlite:///:memory:")`.

`connect()` / `close()` / `__aenter__` / `__aexit__` become no-ops or engine
lifecycle wrappers -- the pool manages connections now. Keep the context-manager
protocol so existing call sites still read naturally, and have `close()` call
`engine.dispose()`.

Hazards specific to this file:

- `aiosqlite.Row` row factory is used for name-based access. SQLAlchemy
  `Result` rows already support `.mappings()`; convert access patterns rather
  than trying to reproduce the row factory.
- Placeholder conversion from `?` to `:name` is mechanical but pervasive; there
  are far more queries here than in Tasks 3-7. Convert one method at a time and
  run the tests between methods rather than converting the whole file blind.
- The append path relies on `INTEGER PRIMARY KEY AUTOINCREMENT` for global
  position and on SQLite's single-writer semantics for optimistic locking. Do not
  change the SQL semantics while porting -- this task is a driver swap, nothing
  more.

- [ ] **Step 1: Write a failing test** constructing `SQLiteEventStore` from an
  engine, appending two events, and reading them back with correct global
  positions.
- [ ] **Step 2: Run it, confirm failure.**
- [ ] **Step 3: Port method by method**, running
  `uv run pytest tests/unit/stores/ -q --no-cov` after each method.
- [ ] **Step 4: Run `uv run pytest tests/unit/stores/ tests/unit/test_eventstore_global_position.py -v --no-cov`.**
- [ ] **Step 5: Update `tests/conftest.py` SQLite fixtures** to yield engines.
- [ ] **Step 6: `uv run pytest tests/unit/ -q --no-cov` and mypy.**
- [ ] **Step 7: Commit** with `refactor: port SQLite event store to SQLAlchemy`.

---

### Task 9: Purge raw aiosqlite and verify

**Files:**
- Modify: `src/eventsource/__init__.py`, `src/eventsource/stores/__init__.py`,
  `src/eventsource/snapshots/__init__.py`, `src/eventsource/migrations/__init__.py`
- Modify: `pyproject.toml` (dependency comment only, if `aiosqlite` is described
  as a direct driver rather than a SQLAlchemy driver)
- Modify: `docs/` and `examples/` construction sites
- Test: whole suite

**Interfaces:**
- Consumes: everything from Tasks 1-8.
- Produces: no new API. This task proves the milestone is actually complete.

- [ ] **Step 1: Find every remaining raw usage**

Run: `grep -rn "import aiosqlite\|aiosqlite.connect\|aiosqlite.Connection\|aiosqlite.Row" src/ tests/ docs/ examples/`

Expected end state: zero hits in `src/`. The only legitimate remaining
references are the `AIOSQLITE_AVAILABLE` feature flag (which now means "the
SQLAlchemy SQLite driver is installed") and its `try/except ImportError` guard.

- [ ] **Step 2: Fix each remaining hit**, then re-run the grep and confirm it is
  empty for `src/`.

- [ ] **Step 3: Run the full unit suite with coverage**

Run: `uv run pytest tests/unit/ -q`
Expected: all pass. Note the coverage number; it must not drop more than 1% from
the pre-M0 baseline. If it drops further, deleted code was carrying tests that
should have been ported rather than removed.

- [ ] **Step 4: Run SQLite integration tests**

Run: `uv run pytest tests/integration/ -m sqlite -v`
Expected: all pass.

- [ ] **Step 5: Run PostgreSQL integration tests**

```bash
docker compose -f docker-compose.test.yml up -d
uv run pytest tests/integration/ -m postgres -v
```
Expected: all pass. This is the first point where the merged repositories are
exercised against a real PostgreSQL, so treat failures here as merge defects,
not flakes.

- [ ] **Step 6: Lint, format, type-check**

```bash
uv run ruff check src/ tests/ --fix
uv run ruff format src/ tests/
uv run mypy src/eventsource/ --config-file=pyproject.toml
```
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit --no-verify -m "refactor: complete SQLAlchemy unification, remove raw aiosqlite usage"
```

---

## Self-review notes

**Spec coverage.** M0 is a prerequisite derived from design-doc revisions 4-6,
not from the original spec. It covers: revision 5 (SQLite transaction control,
Task 1) and the enlistment precondition that revision 6 depends on (Tasks 3-5).
Revision 4 (connection threading) is deliberately **not** here -- it belongs to
M1 because it touches `DatabaseProjection`, not the SQLite layer.

**Not covered here, tracked for M1:** the `conn=` enlistment kwarg, the
`ProcessedEventLedger`, the `processed_events` schema, `DeliveryGuarantee`,
connection threading, and the docstring corrections.

**Known risk.** Task 8 is the largest and least mechanical. If it proves harder
than estimated, it can be deferred past M1 without blocking the delivery
guarantees -- the event store is not enlisted in projection transactions. Tasks
1-5 are the true prerequisites. Do not let Task 8 hold up M1.

**Beads.** The repo's pre-commit hook calls `bd sync --flush-only`, which fails
in this worktree: `beads.db` (Feb 20) is newer than `issues.jsonl` (Feb 14), so
neither `no-db: true` nor `bd init` is safe without a human deciding which is
authoritative. Until then, all commits in this plan use `--no-verify`. The other
pre-commit checks (ruff, mypy, bandit) are run explicitly in Task 9 Step 6.
