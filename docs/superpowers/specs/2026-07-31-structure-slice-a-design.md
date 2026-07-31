# Structure Slice A — Design

**Status:** Design complete, ready for planning
**Source of scope:** Exactly three `BACKLOG.md` entries left over from the
clean-architecture rings campaign (PR #88 line):

1. "Migrate locks/ to ports/adapters (P2)"
2. "Split readmodels/ into port + adapter (P2)"
3. "Decide engine.py's ring placement (P3)"

Everything else is out of scope, explicitly including: the `bus/` ring
migration, `migration/repositories/`, relocating `subscriptions/`, making the
top-level `__init__` lazy, the `SnapshotStore` redesign, and the "Small
ring-consistency cleanups" batch. §3.5 records how the engine ruling changes
the lazy-`__init__` entry's premise without doing that work here.

Each section is self-contained: what the code looks like today with citations,
the ruling with rationale and rejected alternatives, the target layout
precisely enough specified for red/green tasks, the failing test that must
exist first, acceptance criteria, and docs obligations. §4 is the cross-cutting
obligation ledger (ADR, CHANGELOG, import-linter, docs). §5 is the task
grouping and dependency order.

**House rule applied throughout:** this is a *structure* slice. Where a move
would require changing a public name's meaning or an exception's identity to
land cleanly, the move stops at the boundary and the semantic change is filed
as a separate backlog item. Two such stops are recorded (§2.4, §1.3) and both
are called out rather than quietly absorbed.

**Pattern being followed** (established by ADRs 0019/0021/0024/0025/0026, not
reinvented here):

- `ports/` holds small composed Protocols, ISP-split along real consumer
  groups. `ports/store.py` is the exemplar (five capability protocols plus a
  composed union); `ports/checkpoints.py` shows the two-group split.
- `adapters/` colocates by backend: `adapters/postgresql/`,
  `adapters/sqlite/`, `adapters/memory/`, plus `adapters/sql/` for
  dialect-parameterized implementations and `adapters/_sql/` for private
  shared SQL helpers.
- Conformance suites live in `testing/conformance_ports/` as ABCs with an
  abstract `store` pytest fixture, importing only `ports`, `domain`, `events`,
  `exceptions`, pytest and stdlib — never an adapter.
- Public API names are preserved by top-level `__init__` rebinding; a package
  whose *import path* is itself documented public API gets a deprecation shim
  with module-level `__getattr__` + `__dir__`, scheduled for removal in 0.8.0
  (the `bus/` decomposition precedent).

---

## 1. Migrate `locks/` to ports + adapters

### 1.1 Current state

`src/eventsource/locks/` is a two-file package in pre-ring style:

- `locks/__init__.py` — a pure re-export shim over `locks/postgresql.py`,
  exporting `LockAcquisitionError`, `LockInfo`, `LockNotHeldError`,
  `PostgreSQLLockManager`, `migration_lock_key`.
- `locks/postgresql.py` (~500 lines) — mixes, in one module: the `LockInfo`
  frozen dataclass (pure), two exception classes (pure), the
  `migration_lock_key` helper (pure string formatting), and
  `PostgreSQLLockManager` (sqlalchemy `AsyncSession` / `async_sessionmaker`,
  `text()`, one dedicated session per held lock).

There is no interface: `PostgreSQLLockManager` is a concrete class and its two
consumers name it directly.

- `migration/cutover.py:63` imports `LockAcquisitionError` and
  `migration_lock_key` at runtime; `cutover.py:78` and `coordinator.py:103`
  import `PostgreSQLLockManager` under `TYPE_CHECKING` purely for annotation
  (`coordinator.py:202`: `lock_manager: PostgreSQLLockManager | None = None`).
- `docs/core-surface.md:202` lists the whole `locks/` package as
  sqlalchemy-tainted, and `:201` records that `migration/`'s package `__init__`
  is tainted *through* it.
- `pyproject.toml`'s "Application ring must not import adapters" contract names
  `eventsource.locks` alongside `eventsource.adapters` and `eventsource.bus` —
  the package is already treated as an adapter by the build, just not located
  like one.

There are exactly zero top-level `eventsource` re-exports of any lock name, and
`docs/api/locks.md:189` states that deliberately ("They are not reachable via
`eventsource`").

### 1.2 The ADR 0023 tension, and the ruling on it

ADR 0023 decided, in as many words, that `eventsource.locks` provides "exactly
one distributed mutual-exclusion primitive … instead of a backend-agnostic
protocol with several implementations." A naive reading makes this backlog item
a reversal of an accepted ADR. It is not, and the distinction must be stated
precisely because it constrains what the port is allowed to promise.

**Ruling: introduce a port, but the port describes *the shape of the
dependency*, not an equivalence of *distributed* semantics.** ADR 0023's
argument is about the second thing and survives intact.

Concretely, the port promises: acquire/try/release/is-held mutual exclusion
among callers sharing one manager instance, plus the error contract. It does
**not** promise cross-process exclusion, crash release, or fairness — those
remain PostgreSQL-specific guarantees documented on the PostgreSQL adapter and
pinned only by PostgreSQL integration tests. An in-memory adapter that offers
none of them is therefore not a lie about being a "distributed lock"; it is a
conforming implementation of a narrower contract, restricted by its own
docstring and by ADR 0029 to single-process testing.

ADR 0029 **amends** ADR 0023 with exactly this paragraph. ADR 0023 is not
superseded and its "When Not to Use This" section stands.

### 1.3 Target layout

**`src/eventsource/ports/locks.py`** (new, Tier 0: stdlib + dataclasses +
typing only, no pydantic needed):

```python
@dataclass(frozen=True)
class LockInfo:                       # moved verbatim from locks/postgresql.py
    key: str
    lock_id: int
    acquired_at: datetime
    holder_id: str | None = None


def migration_lock_key(...) -> str:   # moved verbatim; pure string formatting


class DistributedLock(Protocol):
    """Acquire/release mutual exclusion on a string key."""

    def acquire(
        self,
        key: str,
        *,
        timeout: float | None = None,
        retry_interval: float = 0.1,
    ) -> AbstractAsyncContextManager[LockInfo]: ...

    async def try_acquire(self, key: str) -> LockInfo | None: ...
    async def release(self, key: str) -> None: ...
    async def is_held(self, key: str) -> bool: ...


class LockRegistry(Protocol):
    """Bulk lifecycle over everything one manager instance holds."""

    async def release_all(self) -> int: ...

    @property
    def held_lock_count(self) -> int: ...


class LockManager(DistributedLock, LockRegistry, Protocol):
    """Composed convenience protocol: both capabilities in one object."""
```

The ISP split is along the two real consumer groups, mirroring
`ports/checkpoints.py`: `migration/cutover.py` and `coordinator.py` use only
`DistributedLock` (acquire / try_acquire / release), while `release_all()` and
`held_lock_count` are shutdown-and-introspection operations for whoever *owns*
the manager, with no in-library caller. Both consumers are annotated with
`DistributedLock`, not `LockManager`.

Note the deliberate signature detail: `acquire` is declared as a **non-async
method returning `AbstractAsyncContextManager[LockInfo]`**, not as an async
generator. `PostgreSQLLockManager.acquire` is decorated with
`@asynccontextmanager`, so its *runtime* type is exactly that; declaring the
port as `async def acquire(...) -> AsyncIterator[LockInfo]` would fail to type
against the decorated implementation. None of these protocols is
`@runtime_checkable` — no consumer does `isinstance` — matching
`ports/store.py`'s stated policy.

**`src/eventsource/adapters/postgresql/locks.py`** (new): `PostgreSQLLockManager`
moved with **no behavior change**. The class body, the 63-bit key hashing, the
per-lock `AsyncSession`, the poll loop, and every tracing attribute are byte-
identical apart from import rewrites. Its `__init__.py` gains the export.

**`src/eventsource/adapters/memory/locks.py`** (new): `InMemoryLockManager`,
stdlib-only (`asyncio`, `dataclasses`, `datetime`, `hashlib` for lock-id
parity). Backed by a `dict[str, LockInfo]` guarded by an `asyncio.Lock`, with
an `asyncio.Condition` for the blocking/timeout path in `acquire`. Its
docstring must state, in its first paragraph, that it excludes only coroutines
in one event loop in one process, offers no crash release, and is for tests.
Added to `adapters/memory/__init__.py`.

**`src/eventsource/locks/`**: `postgresql.py` is **deleted**. `__init__.py` is
replaced by a deprecation shim, because `eventsource.locks` is documented
public API in five places (`docs/api/index.md:69`, `:438`, `:680`, `:796`, and
all of `docs/api/locks.md`):

```python
_MOVED = {
    "LockInfo": "eventsource.ports.locks",
    "migration_lock_key": "eventsource.ports.locks",
    "LockAcquisitionError": "eventsource.locks.postgresql",   # see below
    "LockNotHeldError": "eventsource.locks.postgresql",
    "PostgreSQLLockManager": "eventsource.adapters.postgresql.locks",
}

def __getattr__(name: str) -> object: ...   # resolves + DeprecationWarning
def __dir__() -> list[str]: ...             # required alongside __getattr__
```

The warning message names the new import path per attribute and states removal
in 0.8.0. `__dir__` is not optional: a lazy `__getattr__` without it broke
introspection during the snapshots migration and had to be fixed after the
fact.

**Exceptions — the stop.** `LockAcquisitionError` and `LockNotHeldError`
currently subclass bare `Exception`, not `EventSourceError`. CLAUDE.md says
`exceptions.py` holds all exception types, and the snapshot slice (ADR 0021)
set that precedent. But every class already in `exceptions.py` derives from
`EventSourceError`, so a verbatim move would put two `Exception`-rooted
outliers in a module whose whole contract is "catch `EventSourceError` and you
have caught everything," and rebasing them would newly make them catchable by
`except EventSourceError` — a semantic change to a public exception hierarchy.

**Ruling:** move them to `eventsource/exceptions.py` **and rebase both onto
`EventSourceError`**. This is the one semantic change this slice accepts, and
it is accepted because the alternative is worse in both directions: leaving
them in a deleted module is impossible, and creating a third exception home
contradicts the documented rule. The change is *widening* only — every existing
`except LockAcquisitionError` and `except Exception` still catches; the only
newly-catching clause is `except EventSourceError`, which today catches nothing
lock-related and cannot therefore have been relied on to *not* catch these. It
is CHANGELOG **Changed**, called out explicitly, and pinned by a new test
asserting both are `EventSourceError` subclasses.

### 1.4 Conformance suite

**`src/eventsource/testing/conformance_ports/locks.py`** — `DistributedLockConformance`
(ABC, abstract `store` fixture yielding a fresh manager, matching the eleven
existing suites' shape).

Pinned cases — deliberately only the intersection both backends honestly meet:

1. `acquire` yields a `LockInfo` whose `key` matches the request.
2. Inside `acquire`, `is_held(key)` is `True`; after the block, `False`.
3. `acquire` releases on exception propagating out of the block, and the
   exception propagates unchanged.
4. `try_acquire` on a free key returns a `LockInfo`; on a key already held by
   the same manager returns `None`.
5. `release` after `try_acquire` makes the key acquirable again.
6. `release` of a never-held key raises `LockNotHeldError`.
7. `acquire(key, timeout=<small>)` on a key the same manager already holds
   raises `LockAcquisitionError` with `.timeout` set.
8. Two different keys are independent (holding one does not block the other).
9. `release_all()` returns the number released and drives `held_lock_count` to
   0; `held_lock_count` tracks acquisitions.

Explicitly **not** pinned by the suite, and stated in its module docstring:
cross-process or cross-connection exclusion, release-on-crash, fairness/FIFO
ordering, and the numeric value of `LockInfo.lock_id`. Those stay in
`tests/integration/locks/test_postgresql_locks_integration.py`, which is
untouched by this slice.

Bindings: `tests/unit/adapters/test_memory_locks_conformance.py` (memory) and a
`@pytest.mark.postgres` binding appended to
`tests/integration/locks/test_postgresql_locks_conformance.py` (new file).

### 1.5 Red-first requirements

- **Task A1 red:** write `tests/unit/adapters/test_memory_locks_conformance.py`
  binding `DistributedLockConformance` to `InMemoryLockManager` before either
  exists. Both the suite import and the adapter import must fail
  (`ModuleNotFoundError`) at collection. Green = `ports/locks.py`, the memory
  adapter, and the suite.
- **Task A2 red:** the PostgreSQL conformance binding, plus a static test
  `tests/unit/test_locks_shim.py` asserting (a) each of the five names is
  importable from `eventsource.locks` and (b) each import emits a
  `DeprecationWarning` naming its new path (`pytest.warns`). Both fail before
  the move.
- Both existing lock test files (`tests/locks/test_postgresql_locks.py`,
  `tests/integration/locks/test_postgresql_locks_integration.py`) get their
  imports retargeted to the new paths and must otherwise pass **unmodified** —
  that is the behavior-preservation evidence for the move.

### 1.6 Acceptance criteria

- `grep -rn "eventsource.locks" src/` matches only `src/eventsource/locks/__init__.py`.
- `src/eventsource/locks/postgresql.py` does not exist.
- `DistributedLockConformance` passes against `InMemoryLockManager` (unit) and
  `PostgreSQLLockManager` (integration, `-m postgres`).
- `migration/cutover.py` and `migration/coordinator.py` annotate
  `lock_manager` as `DistributedLock` and import it from
  `eventsource.ports.locks`; `LockAcquisitionError` comes from
  `eventsource.exceptions`.
- `eventsource.adapters.memory.locks` imports no sqlalchemy (enforced by the
  Tier-0 contract addition in §4.3).
- The five legacy names still import from `eventsource.locks`, each with a
  `DeprecationWarning`.
- `tests/locks/test_postgresql_locks.py` passes with only import lines changed.
- `make check` green.

---

## 2. Split `readmodels/` into port + adapter

### 2.1 Correcting the backlog entry's premise

The backlog says `readmodels/postgresql.py` "mixes the ReadModelProjection base
contract with its PostgreSQL implementation (16 `sql_connection` call sites)."
The call-site count is right (16). The rest is not: `ReadModelProjection` lives
in `readmodels/projection.py`, and `readmodels/postgresql.py` contains only
`PostgreSQLReadModelRepository`. The real defect is bigger and differently
shaped than the entry describes — `readmodels/` is a **nine-module package
that splits cleanly down the middle** and has simply never been separated:

| Module | Content | Dependency | Belongs |
|---|---|---|---|
| `base.py` | `ReadModel` (pydantic) | pydantic | port |
| `query.py` | `Query`, `Filter` (dataclasses) | stdlib | port |
| `repository.py` | `ReadModelRepository` Protocol (15 methods) | pure | port |
| `exceptions.py` | `ReadModelError`, `OptimisticLockError`, `ReadModelNotFoundError` | stdlib | port (see §2.4) |
| `schema.py` | `generate_schema`, `POSTGRESQL_TYPE_MAP`, `SQLITE_TYPE_MAP` | stdlib, but emits dialect DDL | adapter |
| `in_memory.py` | `InMemoryReadModelRepository` | stdlib + observability | adapter (memory) |
| `postgresql.py` | `PostgreSQLReadModelRepository` | sqlalchemy, `adapters/_sql/connection` | adapter (postgresql) |
| `sqlite.py` | `SQLiteReadModelRepository` | aiosqlite | adapter (sqlite) |
| `projection.py` | `ReadModelProjection` | `adapters/sql/projection.DatabaseProjection` | adapter (sql) |

`docs/core-surface.md:204` already says exactly this — "`readmodels/` … splits
down the middle: the contract and in-memory halves are Tier 0, while the
backend halves land here" — and `:193` records the `sql_connection` reach as
"accepted debt … resolved when this module itself moves under `adapters/`",
which is what this section does.

### 2.2 Ruling on where the port lives

**Ruling: `eventsource/ports/readmodels/` as a subpackage**, not a flat
`ports/readmodels.py`:

```
ports/readmodels/__init__.py     # re-exports the six public names
ports/readmodels/model.py        # <- readmodels/base.py
ports/readmodels/query.py        # <- readmodels/query.py
ports/readmodels/repository.py   # <- readmodels/repository.py
ports/readmodels/exceptions.py   # <- readmodels/exceptions.py  (see §2.4)
```

Rationale: `ports/` is flat today, and departing from that needs a reason. The
reason is size and cardinality. Every other port is one Protocol plus its value
objects in one file; readmodels is four genuinely distinct pure artifacts — a
user-subclassable pydantic base, a query specification language, a 15-method
repository Protocol, and an exception family — totaling ~35 KB. Flattening
would make `ports/readmodels.py` roughly three times the size of the next
largest port module and would merge four things users import for four different
reasons. The subpackage keeps `eventsource.ports.readmodels` as the import path
users see, which is what matters for the public surface.

*Rejected: flat `ports/readmodels.py`.* Produces the largest file in `ports/`
by a wide margin and buries `Query`/`Filter` — which are useful without the
repository Protocol — inside a module named for the repository.

*Rejected: `domain/readmodel.py` for `ReadModel`.* A read model is not a domain
entity; it is a persistence-shaped projection of one, and `domain/` is
documented as the entities ring. Splitting `ReadModel` from the Protocol that
consumes it across two rings would also make the two halves independently
un-shippable.

### 2.3 Ruling on `schema.py`

**Ruling: `adapters/sql/readmodel_schema.py`.** `schema.py` has no third-party
imports, so a purely mechanical dependency test would let it sit in `ports/`.
It must not: it hardcodes `POSTGRESQL_TYPE_MAP` and `SQLITE_TYPE_MAP` and emits
dialect-specific `CREATE TABLE` text. Dialect knowledge in a port is precisely
the boundary error this campaign exists to remove, and `adapters/sql/` is
already the home for dialect-parameterized code (`checkpoints.py`, `dlq.py`).

Consequence to record rather than hide: `eventsource.readmodels.schema` is
currently listed in the Tier-0 "must not import sqlalchemy" contract, and after
the move it sits under `adapters/sql/`, which is not Tier 0, so it loses that
guard. That is correct — the guard was asserting the wrong property about it —
but §4.3 requires the import-linter delta to be explicit rather than a silent
line deletion.

### 2.4 Ruling on `readmodels/exceptions.py` — the second stop

`readmodels/exceptions.py` defines `OptimisticLockError(ReadModelError)` with
`(model_id, expected_version, actual_version=None)`. `eventsource/exceptions.py`
defines a **different class of the same name**,
`OptimisticLockError(EventSourceError)`, with
`(aggregate_id, expected_version, actual_version)`. They are unrelated types
that do not catch each other. Top-level `eventsource.OptimisticLockError`
resolves to the `exceptions.py` one; `eventsource.readmodels.OptimisticLockError`
resolves to the other.

That collision is a real defect, and it is **not this slice's to fix**: any
resolution requires renaming a shipped public name, which is a semantic change.
Moving the trio into `exceptions.py` is also impossible as a verbatim move —
two classes of the same name cannot coexist in one module.

**Ruling: the trio moves to `ports/readmodels/exceptions.py` and stays a
separate exception home for now.** ADR 0029 records this as a documented,
time-boxed exception to CLAUDE.md's "`exceptions.py` holds all exception types"
rule, with the reason stated. A new P2 backlog entry — "Resolve the duplicate
`OptimisticLockError` name (readmodels vs core)" — is filed by task A7,
proposing `ReadModelVersionConflictError` with a deprecation alias, and noting
that the collision predates this slice.

*Rejected: rename inside this slice.* Breaks the slice's public-API-stability
constraint and mixes a semantic decision into a structural one, which is
exactly the failure mode the house rule at the top of this document guards
against.

### 2.5 Target layout (complete move table)

| From | To | Public name(s) |
|---|---|---|
| `readmodels/base.py` | `ports/readmodels/model.py` | `ReadModel` |
| `readmodels/query.py` | `ports/readmodels/query.py` | `Query`, `Filter` |
| `readmodels/repository.py` | `ports/readmodels/repository.py` | `ReadModelRepository`, `ReadModelRepositoryProtocol` |
| `readmodels/exceptions.py` | `ports/readmodels/exceptions.py` | `ReadModelError`, `OptimisticLockError`, `ReadModelNotFoundError` |
| `readmodels/schema.py` | `adapters/sql/readmodel_schema.py` | `generate_schema`, `generate_indexes`, `generate_full_schema`, `POSTGRESQL_TYPE_MAP`, `SQLITE_TYPE_MAP` |
| `readmodels/in_memory.py` | `adapters/memory/readmodels.py` | `InMemoryReadModelRepository` |
| `readmodels/postgresql.py` | `adapters/postgresql/readmodels.py` | `PostgreSQLReadModelRepository` |
| `readmodels/sqlite.py` | `adapters/sqlite/readmodels.py` | `SQLiteReadModelRepository` |
| `readmodels/projection.py` | `adapters/sql/readmodel_projection.py` | `ReadModelProjection` |

`ReadModelProjection` goes to `adapters/sql/` and not to
`application/projections/` for the same reason `DatabaseProjection` did
(ADR 0024): it subclasses `DatabaseProjection`, takes an
`async_sessionmaker`, and hands handlers a live `AsyncConnection`. A framework
type in the class signature makes it an adapter. `adapters/sql/__init__.py`
gains both new exports.

`src/eventsource/readmodels/__init__.py` becomes a deprecation shim of the same
shape as §1.3's (module `__getattr__` + `__dir__`, per-name new path,
removal in 0.8.0), covering all sixteen names in its current `__all__`.
`eventsource.readmodels` is documented public API at `docs/api/index.md:240`,
`:353`, `:370` and throughout `docs/api/readmodels.md`, so it cannot simply
disappear. All other files under `readmodels/` are deleted.

**No behavior changes anywhere in this section.** Every moved module's body is
unchanged apart from import rewrites and the `sql_connection` import in the
PostgreSQL adapter becoming a sibling-package import.

Top-level `eventsource` keeps re-exporting `ReadModelProjection` (line 199),
now sourced from `eventsource.adapters.sql.readmodel_projection`. No top-level
names are added or removed by this section.

### 2.6 Conformance suite — ruling: yes, readmodels warrants one

Three implementations (memory, PostgreSQL, SQLite) satisfy a 15-method Protocol
with genuinely subtle shared semantics: `save` upsert with automatic
`created_at`/`updated_at`/`version` handling, soft delete excluded from `find`
but visible to `get_deleted`, `restore`, `truncate` removing soft-deleted rows
too, and `save_with_version_check`'s optimistic-locking contract including
`ReadModelNotFoundError` for a nonexistent model. Those semantics are currently
asserted three separate times, in
`tests/unit/readmodels/test_in_memory.py`, `test_postgresql.py`, and
`test_sqlite.py`, plus cross-cutting `test_soft_delete.py` and
`test_optimistic_locking.py` — which is exactly the triplication the
conformance-suite pattern exists to collapse, and the reason
`SQLiteReadModelRepository` can drift from its siblings undetected today.

**`src/eventsource/testing/conformance_ports/readmodels.py`** —
`ReadModelRepositoryConformance` (ABC, abstract `store` fixture). Because the
Protocol is generic over `TModel`, the suite pins its own model type in
`conformance_ports/_fixtures.py`:

```python
class ConformanceReadModel(ReadModel):
    name: str = "conformance"
    count: int = 0
```

and the abstract `store` fixture is documented as yielding a repository already
bound to `ConformanceReadModel` with its table provisioned. Backend bindings
own provisioning (`generate_schema(ConformanceReadModel, dialect=...)` for the
SQL ones), so the suite itself stays adapter-free and dialect-free —
`_fixtures.py`'s "no sqlalchemy" property is preserved, since `ReadModel` is
pydantic-only.

Required case groups, one method each: get/save round-trip; `save` upsert
semantics and `updated_at` advance; `get_many` with missing ids; `save_many`;
`exists`; hard `delete` return value on hit and miss; `soft_delete` then `get`
returns `None` while `get_deleted` returns the row; `restore`; `find_deleted`;
`find` with `Filter.eq` and with ordering and with `limit`; `count` with and
without filters; `truncate` return value and post-state; `save_with_version_check`
success incrementing version, `OptimisticLockError` on stale version, and
`ReadModelNotFoundError` on absent model.

Not pinned: ordering of `get_many` results (the Protocol says order is not
guaranteed), the exact `updated_at` resolution, and any dialect-specific type
coercion.

Bindings: `tests/unit/adapters/test_memory_readmodels_conformance.py`,
`tests/unit/adapters/test_sqlite_readmodels_conformance.py`, and a
`@pytest.mark.postgres` binding under `tests/integration/readmodels/`. Cases
in the three existing per-backend unit files that the suite now covers are
deleted from those files; backend-specific cases (SQLite TEXT/JSON coercion,
PostgreSQL JSONB behavior) stay where they are.

### 2.7 Red-first requirements

- **Task A3 red:** `tests/unit/ports/test_readmodels_port_surface.py` —
  imports `ReadModel`, `Query`, `Filter`, `ReadModelRepository`, and the three
  exceptions from `eventsource.ports.readmodels`, and asserts
  `sqlalchemy not in sys.modules` after importing every
  `eventsource.ports.readmodels` submodule under a stubbed top-level package
  (the same technique `docs/core-surface.md` finding 12 used for `ports/`).
  Fails with `ModuleNotFoundError` before the move.
- **Task A4 red:** `tests/unit/test_readmodels_shim.py` asserting all sixteen
  legacy names import from `eventsource.readmodels` with a `DeprecationWarning`
  naming the new path, and that `eventsource.ReadModelProjection` is
  `eventsource.adapters.sql.readmodel_projection.ReadModelProjection`.
- **Task A5 red:** the three conformance bindings, written before the suite
  module exists.
- `tests/unit/readmodels/test_exports.py` is retargeted rather than deleted: it
  becomes the shim's export-completeness test.

### 2.8 Acceptance criteria

- `src/eventsource/readmodels/` contains exactly one file, `__init__.py`, and
  that file imports no sqlalchemy at module scope (it resolves lazily).
- `eventsource.ports.readmodels` and every submodule import with sqlalchemy
  absent from `sys.modules`.
- `ReadModelRepositoryConformance` passes against all three adapters.
- `grep -rn "eventsource.readmodels" src/` matches only the shim.
- Top-level `eventsource.__all__` is byte-identical to before the slice.
- The 16 `sql_connection` call sites in the PostgreSQL read-model repository
  now sit under `adapters/`, closing the ADR 0026 §4 accepted-debt item for
  this module (the four `migration/repositories/` modules remain open — out of
  scope).
- `make check` green; `tests/integration/readmodels/` green with Docker up.

---

## 3. Ring placement for `engine.py`

### 3.1 Current state

`src/eventsource/engine.py` is a 157-line top-level module holding
`create_async_engine` and `SQLITE_PRAGMAS`, plus three module-level helpers
(`_apply_pragmas`, `_begin_unless_autocommit`, `_driver_is_autocommit`) and
`_configure_sqlite`, which attaches `connect` and `begin` listeners. It exists
because the stdlib `sqlite3` driver's legacy transaction control breaks the
read-then-write-then-commit atomicity projections depend on.

Importers:

- `src/eventsource/__init__.py:103` — the only in-library importer, exporting
  `create_async_engine` as a top-level public name (`__init__.py:234`).
- 33 references across `tests/`, `docs/`, and plans; the runtime ones are
  `tests/conftest.py`, `tests/unit/test_engine.py`, and five adapter/repository
  test modules.
- Mutation-testing configuration in three places:
  `pyproject.toml`'s `[tool.mutmut] only_mutate` (line 204),
  `cosmic-ray/engine.toml`, `scripts/mutation.sh` (lines 9, 24, 48) and
  `scripts/_mutmut_configure.py`; documented in
  `docs/development/mutation-testing.md:25`, `:49`, `:394`.

`docs/core-surface.md:178` lists it in the non-Tier-0 table, and `:254`/`:286`
name it as one of the two remaining module-level sqlalchemy imports in the
front-door `__init__` (the other being `adapters.postgresql`).

### 3.2 Ruling: `adapters/_sql/engine.py`

The module is unambiguously adapter-ring content — it constructs a SQLAlchemy
`AsyncEngine` and registers dialect-specific driver listeners — so the only
real question is *which* SQL package. Three candidates, and the eager-import
chain decides it.

`adapters/_sql/__init__.py` is a one-line docstring with **no imports**.
`adapters/sql/__init__.py` eagerly imports `checkpoints`, `dlq`, and
`projection`, and `projection.py` reaches into `application/projections/`. So
`from eventsource.adapters._sql.engine import create_async_engine` in the
top-level `__init__` executes two package `__init__`s that do nothing, whereas
the `adapters/sql/` placement would execute three module bodies and drag the
application projection ring into the front-door chain for what is a leaf
factory function. Since finding 12 in `docs/core-surface.md` is specifically
about that chain, widening it while relocating the module named in it would be
self-defeating.

The obvious objection — a private-by-name package hosting a public-facing
function — does not survive contact with the facts. `create_async_engine`'s
canonical public name is `eventsource.create_async_engine`; it has been
top-level-exported since it was introduced, `docs/api/index.md` does not list
`eventsource.engine` among the subsystems shipping without top-level re-export
(it lists `eventsource.locks`, `eventsource.gdpr`, `eventsource.config`), and
`docs/core-surface.md:178` describes it by function, not by import path. The
module path was never the advertised surface, and `_sql` marks exactly that.

**Consequently `src/eventsource/engine.py` is deleted outright, with no
deprecation shim** — unlike `locks/` and `readmodels/`, whose package paths
*are* documented public API. The public name `eventsource.create_async_engine`
is unchanged, and the CHANGELOG entry is a **Changed** note stating that the
module moved and that anyone importing `eventsource.engine` directly (which the
docs never told them to do) should import from `eventsource` instead.

*Rejected: dissolve into `adapters/_sql/connection.py`.* Two unrelated
concerns — engine *construction* with driver-level event listeners, versus
`sql_connection`'s 20-line `AsyncConnection | AsyncEngine` normalization — and
merging them costs something concrete: `engine.py` is a first-class mutation-
testing target with its own `cosmic-ray/engine.toml` session, its own
`scripts/mutation.sh engine` selector, and a dedicated
`tests/unit/test_engine.py`. `connection.py` has none of that and is covered by
`tests/unit/adapters/test_sql_connection.py`. Folding them would either
mutation-test the connection helper by accident or force per-function mutation
filtering that the tooling does not express well. Two files, two test files,
two mutation targets is the honest shape.

*Rejected: `adapters/sql/engine.py`.* Widens the front-door eager-import chain,
as argued above.

*Rejected: leave it at top level.* That is the status quo the backlog item
exists to end; a top-level module is not a ring, and it is the last non-ring
sqlalchemy module in the package root.

### 3.3 Target layout

- `src/eventsource/adapters/_sql/engine.py` — moved verbatim, no edits beyond
  the module docstring gaining one sentence on why it sits in `_sql/` rather
  than `sql/` (the eager-chain reason above).
- `src/eventsource/engine.py` — deleted.
- `src/eventsource/__init__.py:103` — `from eventsource.adapters._sql.engine
  import create_async_engine`. `SQLITE_PRAGMAS` is **not** added to the
  top-level surface (it is not exported today).
- `adapters/_sql/__init__.py` stays import-free — deliberately. Adding a
  convenience re-export there would reintroduce the eager cost the ruling was
  chosen to avoid.

### 3.4 Configuration and test updates (all mandatory, all in task A6)

- `pyproject.toml` `[tool.mutmut] only_mutate`: `src/eventsource/engine.py` →
  `src/eventsource/adapters/_sql/engine.py`.
- `cosmic-ray/engine.toml`: module path updated.
- `scripts/mutation.sh` (lines 9, 24, 48) and `scripts/_mutmut_configure.py`:
  path updated; the `engine` selector *name* is kept.
- `docs/development/mutation-testing.md` lines 25, 49, 394: paths updated.
- `tests/unit/test_engine.py` stays at its path (it tests a behavior, not a
  location) with its import retargeted; it must pass **unmodified otherwise**,
  which is the behavior-preservation evidence.
- `tests/conftest.py` and the five other test importers: import retargeted. The
  simplest correct retarget for all of them is `from eventsource import
  create_async_engine`, which is the path the docs recommend; use it rather
  than reaching into `adapters._sql`.
- `docs/core-surface.md:178`, `:254`, `:286`, `:321`: path updated; `:254` and
  `:286` keep their substance (the front-door still imports sqlalchemy) with
  the module renamed.
- `docs/adrs/0008-mutation-testing-tool-selection.md:4` names
  `src/eventsource/engine.py` in a historical list; leave the historical text
  and append a parenthetical "(now
  `src/eventsource/adapters/_sql/engine.py`)". ADRs record what was decided
  then; they are not retro-edited.

### 3.5 Interaction with the lazy-`__init__` backlog entry (noted, not acted on)

The "make the top-level `__init__` lazy" entry cites two module-level
sqlalchemy routes out of `eventsource/__init__.py`: `eventsource.engine` and
`eventsource.adapters.postgresql`. This slice **renames the first without
removing it** — after A6 the chain is
`eventsource/__init__` → `adapters/_sql/engine` → `sqlalchemy`, one module
deeper and no cheaper. Slice A also *adds* nothing to that chain: the locks and
readmodels adapters are reached only through their deprecation shims' lazy
`__getattr__`, and `ReadModelProjection`'s top-level re-export already pulled
sqlalchemy in via `adapters/sql/projection.py` before this slice.

Task A7 updates that backlog entry's import-chain notes to name the new paths.
It does not expand its scope, and making the `__init__` lazy stays out of
slice A.

### 3.6 Red-first and acceptance

Red: a one-line test `tests/unit/test_engine.py::test_module_lives_under_sql_adapters`
asserting `create_async_engine.__module__ == "eventsource.adapters._sql.engine"`,
which fails before the move. (The module has no *behavioral* deficiency to
write a failing test against — this is a pure relocation — so the red test
pins the ruling itself.)

Acceptance:

- `src/eventsource/engine.py` does not exist;
  `grep -rn "eventsource.engine" src/ tests/ scripts/ pyproject.toml cosmic-ray/`
  is empty except for the historical ADR 0008 note.
- `eventsource.create_async_engine` imports and behaves identically;
  `tests/unit/test_engine.py` passes with only its import line changed.
- `scripts/mutation.sh engine` resolves and runs against the new path.
- `make check` green.

---

## 4. Cross-cutting obligations

### 4.1 ADR

**One ADR for the whole slice: `docs/adrs/0029-locks-readmodels-and-engine-rings.md`**
(0027 and 0028 are taken). Required content:

- Status: Accepted. **Amends ADR 0023** with §1.2's paragraph (the port
  describes the dependency shape, not distributed semantics; ADR 0023's
  single-primitive argument stands and the in-memory adapter is test-scoped).
  Sibling of 0024/0026 — the same split applied to the last two pre-ring
  packages.
- The `DistributedLock` / `LockRegistry` / `LockManager` ISP split and why
  `acquire` is typed as returning an async context manager rather than declared
  `async def`.
- The `ports/readmodels/` subpackage decision with its rejected alternatives
  (§2.2), and the `schema.py` → `adapters/sql/` ruling (§2.3).
- The `engine.py` → `adapters/_sql/` ruling with both rejected alternatives
  (§3.2), including the eager-import-chain argument.
- **Two recorded exceptions, both stated as such:** (a) the readmodel exception
  trio staying out of `exceptions.py` while the `OptimisticLockError` name
  collision is unresolved (§2.4), with the follow-up backlog item referenced;
  (b) `LockAcquisitionError`/`LockNotHeldError` being rebased onto
  `EventSourceError` — the one semantic change in the slice (§1.3).
- What each deprecation shim covers and that both are removed in 0.8.0.

`docs/adrs/index.md` gains the 0029 row.

### 4.2 CHANGELOG (`## [Unreleased]`)

**Added:** `eventsource.ports.locks` (`DistributedLock`, `LockRegistry`,
`LockManager`, `LockInfo`, `migration_lock_key`);
`eventsource.adapters.memory.locks.InMemoryLockManager` (test-scoped —
single-process only, no crash release);
`eventsource.ports.readmodels` (`ReadModel`, `Query`, `Filter`,
`ReadModelRepository`, and the read-model exception family);
`DistributedLockConformance` and `ReadModelRepositoryConformance` in
`eventsource.testing.conformance_ports`.

**Changed:** the two re-home paragraphs (locks and readmodels) in the same
shape as the existing "Projection persistence re-homed" and "Outbox ring
migration" entries, each with a from→to table and the statement that top-level
`eventsource` imports are unaffected; the `engine.py` move; and — called out
separately, not buried — `LockAcquisitionError` and `LockNotHeldError` now
subclass `EventSourceError`.

**Deprecated:** `eventsource.locks` and `eventsource.readmodels` as import
paths — every name still resolves, each with a `DeprecationWarning` naming its
replacement; both packages are removed in 0.8.0. No names are removed in this
release.

There is no **Removed** entry: `src/eventsource/engine.py`'s deletion removes
no public name and is covered under Changed.

### 4.3 import-linter (`pyproject.toml`) — exact deltas

**"Tier 0 modules must not import sqlalchemy"** — remove the six now-deleted
`eventsource.readmodels.*` entries (`base`, `query`, `schema`, `repository`,
`in_memory`, `exceptions`) and add:

```
"eventsource.adapters.memory.locks",
"eventsource.adapters.memory.readmodels",
```

`eventsource.ports` is already listed as a whole package, so
`ports/locks.py` and `ports/readmodels/` are covered on arrival — that is the
guard replacing the deleted readmodel contract entries.
`eventsource.readmodels.schema` loses its guard by design (§2.3); the removal
must be accompanied by a comment in the contract block saying so, matching the
existing explanatory comments at the end of the `[tool.importlinter]` section.

**"Application ring must not import adapters"** — `forbidden_modules` keeps
`eventsource.locks` while the shim exists (the shim resolves to adapters, so
the forbid stays correct) and gains nothing. Add a comment noting the entry is
deleted when the shim is removed in 0.8.0.

**New contract — "Ports must not import outward":**

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

This is new because slice A creates the first port with a sibling that moved to
`adapters/` (`ports/readmodels/` and `adapters/sql/readmodel_schema.py`), and
an accidental `from eventsource.adapters.sql.readmodel_schema import
generate_schema` inside `ports/readmodels/model.py` would be a real regression
that the existing sqlalchemy-only contract would not catch (`readmodel_schema.py`
imports no sqlalchemy).

**"Infrastructure backends must not import each other"** needs no change: it
names the `adapters.postgresql` / `adapters.sqlite` / `adapters.memory`
packages, so the new `locks.py` and `readmodels.py` modules inside them are
covered automatically.

### 4.4 Documentation

- `docs/api/locks.md`: rewrite the import-path sections (`:16`, `:133`,
  `:154-168`, `:189`, `:197`, `:210`, and every code sample) onto
  `eventsource.ports.locks` / `eventsource.adapters.postgresql.locks`. The
  "no top-level re-export" statement at `:189` **stays true and stays** — this
  slice adds no top-level lock names. Add a section documenting
  `InMemoryLockManager` and, prominently, what it does not guarantee.
- `docs/api/readmodels.md` and `docs/api/projections.md`: import paths updated;
  the `ReadModelProjection` section notes its new `adapters/sql/` home.
- `docs/api/index.md` lines 69, 240, 353, 370, 438, 680, 796: module table and
  subsystem lists updated for both packages; the deprecation of the old paths
  noted inline.
- `docs/architecture.md`, `docs/guides/distributed-locks.md`,
  `docs/guides/live-migration.md`, `docs/guides/error-handling.md`,
  `docs/explanation/schema-design.md`, `docs/development/testing.md`,
  `docs/index.md`: import-path sweep. `grep -rn "eventsource\.locks\|eventsource\.readmodels\|eventsource\.engine" docs/`
  must return only the ADRs (historical), the migration notes, and the
  deprecation callouts.
- `docs/core-surface.md`: rows `:178` (engine), `:193`-`:195` (readmodels),
  `:201`-`:204` (migration/locks) rewritten; the `readmodels/` "splits down the
  middle" paragraph at `:204` is replaced by a statement that the split has
  been performed. `:240`/`:242` — the ADR 0026 §4 accepted-debt item — is
  updated to record `readmodels/postgresql.py` as resolved and the four
  `migration/repositories/` modules as still open.
- `mkdocs.yml` nav: no new pages are added by this slice, so nav is unchanged.
  A7 still runs the nav-completeness check, since a strict build does not catch
  omissions.
- `BACKLOG.md`: the three entries are struck; two new entries filed —
  "Resolve the duplicate `OptimisticLockError` name (readmodels vs core)" (P2,
  §2.4) and "Remove the `eventsource.locks` and `eventsource.readmodels`
  deprecation shims" (P3, 0.8.0). The lazy-`__init__` entry's import-chain
  notes are updated per §3.5.

---

## 5. Task grouping and dependency order

Seven tasks. Every one is independently green: `make check` passes at each
task's end, and no task leaves a half-moved package.

| Task | Scope | Depends on |
|---|---|---|
| **A1** | `ports/locks.py`; `adapters/memory/locks.py`; `DistributedLockConformance`; memory binding. Red = memory binding written first. | — |
| **A2** | `PostgreSQLLockManager` → `adapters/postgresql/locks.py`; lock exceptions → `exceptions.py` rebased on `EventSourceError`; `locks/` shim; migration consumers retargeted onto `DistributedLock`; pg conformance binding; shim test. | A1 |
| **A3** | `ports/readmodels/` subpackage (model, query, repository, exceptions); the four source modules deleted from `readmodels/`; every remaining `readmodels/` module (`schema`, `in_memory`, `postgresql`, `sqlite`, `projection`, `__init__`) retargeted onto `eventsource.ports.readmodels`, so the package still works unchanged from outside; Tier-0 contract entries swapped; port-surface purity test. | — |
| **A4** | The five adapter moves (memory, postgresql, sqlite, `readmodel_schema`, `readmodel_projection`); `readmodels/` shim; top-level re-export retargeted; shim test. | A3 |
| **A5** | `ReadModelRepositoryConformance` + `ConformanceReadModel` fixture; three bindings; de-duplicate the covered cases out of the three per-backend unit files. | A4 |
| **A6** | `engine.py` → `adapters/_sql/engine.py`; mutation config (pyproject, cosmic-ray, both scripts); test and doc path sweep. | — |
| **A7** | ADR 0029 + index row; CHANGELOG; the new import-linter "Ports must not import outward" contract; `docs/core-surface.md`; `docs/api/*`; doc sweep; `BACKLOG.md` edits including the lazy-`__init__` note. | A1–A6 |

**Dependency order:** A1 → A2 and A3 → A4 → A5 are two independent chains;
A6 is independent of both; A7 is last and depends on everything.

**Parallelism:** the three roots (A1, A3, A6) touch disjoint files and can be
dispatched together. A2 and A4 both edit `pyproject.toml`'s Tier-0 contract
list and both add to `adapters/*/__init__.py`, so they should not run
concurrently — serialize them, or accept a small merge on those two files.

**Suggested reviewer focus per task:** A2 — that
`tests/locks/test_postgresql_locks.py` changed only its import lines; A4 — that
no moved module body changed apart from imports (`git diff -M --stat` should
show pure renames); A5 — that the deleted per-backend cases are genuinely
covered by the suite and not merely dropped; A6 — that all four mutation-config
locations were updated, not just `pyproject.toml`.
