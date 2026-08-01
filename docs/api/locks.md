# Distributed Locks

Technical reference for `eventsource.ports.locks` and
`eventsource.adapters.postgresql.locks`: the `PostgreSQLLockManager`
advisory-lock manager, the `LockInfo` value object, the `migration_lock_key`
helper, and the two lock-specific exceptions.

`eventsource.locks` -- the pre-slice-A import path for all of the above --
**no longer exists** (ADR 0030). Importing it raises `ModuleNotFoundError`,
with no deprecation shim. Update imports to the paths this page documents.

The package provides mutual exclusion *across processes* — one holder at a time
for a given string key, regardless of which application instance asks. It is
built entirely on PostgreSQL session-level advisory locks (`pg_advisory_lock`,
`pg_try_advisory_lock`, `pg_advisory_unlock`), which are application-level locks
independent of table and row locks. A lock persists for the lifetime of the
database session that holds it and is released automatically when that session
ends, including on an unexpected disconnect.

The names split across three modules along ring boundaries (ADR 0029):

| Name | Kind | Lives in |
| --- | --- | --- |
| `PostgreSQLLockManager` | Lock manager class | `eventsource.adapters.postgresql.locks` |
| `LockInfo` | Frozen dataclass describing an acquired lock | `eventsource.ports.locks` |
| `migration_lock_key` | Lock-key naming helper | `eventsource.ports.locks` |
| `LockAcquisitionError` | Raised when a lock cannot be acquired | `eventsource.domain.exceptions` |
| `LockNotHeldError` | Raised when releasing a lock this manager does not hold | `eventsource.domain.exceptions` |

The package's primary in-tree consumer is the live migration tooling
(`eventsource.migration`), where a lock guards cutover so that only one instance
can flip a tenant's event store at a time — hence the `migration_lock_key`
convenience helper. Nothing about the manager is migration-specific, though: any
operation that must not run concurrently across instances can use it.

## Overview

`PostgreSQLLockManager` is the only lock manager in the package. It is
constructed from a SQLAlchemy `async_sessionmaker[AsyncSession]` and exposes two
acquisition styles plus a small set of inspection and cleanup operations:

| Member | Signature | Purpose |
| --- | --- | --- |
| `acquire` | `acquire(key, *, timeout=None, retry_interval=0.1)` | Async context manager; yields `LockInfo`, always releases on exit |
| `try_acquire` | `await try_acquire(key)` | One-shot, non-blocking; returns `LockInfo` or `None` |
| `release` | `await release(key)` | Releases a lock taken with `try_acquire`; raises `LockNotHeldError` if not held |
| `is_held` | `await is_held(key)` | Whether *this* manager currently holds `key` |
| `release_all` | `await release_all()` | Releases every lock this manager holds; returns the count |
| `held_lock_count` | property | Number of locks currently held (plain `int`, not async) |

Locks are named by arbitrary strings. The manager hashes each key with SHA-256
and truncates the digest to 63 bits to produce the `bigint` lock ID that
PostgreSQL's advisory-lock functions actually take, so callers never deal with
numeric IDs directly — `LockInfo.lock_id` exposes the derived value only for
diagnostics.

The two acquisition styles differ in who is responsible for cleanup.
`acquire()` is the preferred form: it is an `@asynccontextmanager`, and its
`finally` block releases the lock whether the body returns normally or raises.
`try_acquire()` returns immediately — `LockInfo` when it won the lock, `None`
when another session holds it — and leaves release entirely to the caller, who
must pair it with `release(key)` in a `try`/`finally`.

Waiting behaviour is controlled by `timeout` on `acquire()`. With the default
`timeout=None` the manager issues a single blocking `pg_advisory_lock` and waits
indefinitely. With a `timeout` set it polls `pg_try_advisory_lock` every
`retry_interval` seconds until the deadline, then raises
`LockAcquisitionError`. `try_acquire()` never waits and never raises for
contention — it simply returns `None`.

Because advisory locks are *session*-scoped in PostgreSQL, the manager checks
out a dedicated `AsyncSession` per held lock and keeps it open for as long as
the lock is held, closing it on release. That is the single most important
operational consequence of this design: N concurrently held locks occupy N
connections from the pool. It also means a crashed process releases its locks
automatically, since the server drops the advisory lock when the backing session
disconnects.

The manager tracks held locks in an in-process dictionary guarded by an
`asyncio.Lock`, so `is_held`, `release`, and `release_all` reflect only what
*this* manager instance holds — not locks held by other processes or by another
manager against the same database. Every acquire and release is wrapped in an
OpenTelemetry span when tracing is enabled, and both emit `DEBUG` log records.

## PostgreSQL-Only Constraint

`eventsource.ports.locks` now defines `DistributedLock` and `LockRegistry` --
small Protocols describing the shape of the dependency (acquire/release/
is_held; bulk release and count), split along the two real consumer groups
(ADR 0029). That is narrower than a backend-agnostic distributed-locking
guarantee: the Protocols say nothing about cross-process exclusion, release
on crash, or fairness, because only one implementation offers those
properties. `PostgreSQLLockManager` is the only production implementation and
requires a PostgreSQL connection; `InMemoryLockManager`
(`eventsource.adapters.memory.locks`) conforms to the same Protocols for
single-process testing only -- see
[`InMemoryLockManager`](#inmemorylockmanager-test-only) below. There is no
SQLite variant.

The constraint is structural, not a matter of configuration.
`PostgreSQLLockManager` issues three PostgreSQL-specific statements directly as
raw SQL through SQLAlchemy's `text()`:

| Operation | Statement |
| --- | --- |
| Blocking acquire (`timeout=None`) | `SELECT pg_advisory_lock(:lock_id)` |
| Non-blocking acquire (`try_acquire`, and each poll when `timeout` is set) | `SELECT pg_try_advisory_lock(:lock_id)` |
| Release | `SELECT pg_advisory_unlock(:lock_id)` |

These functions exist only in PostgreSQL. Nothing in the manager branches on
dialect, and no fallback path exists. Pointing the `session_factory` at a
SQLite or other non-PostgreSQL engine does not degrade gracefully — the first
`acquire()` or `try_acquire()` call fails when the database rejects the unknown
function. In `acquire()` and its timeout loop that failure is caught and
re-raised as `LockAcquisitionError` with a `reason` of `"Database error: ..."`;
`try_acquire()` closes the session, logs at `ERROR`, and re-raises the
underlying driver exception unchanged.

Two further consequences of the PostgreSQL binding are worth stating
explicitly:

- **The 63-bit lock ID exists because of PostgreSQL.** `pg_advisory_lock` and
  friends take a signed `bigint`, so the manager masks its SHA-256 digest to 63
  bits (`& 0x7FFFFFFFFFFFFFFF`). See
  [Key-to-Lock-ID Derivation](#key-to-lock-id-derivation).
- **Lock lifetime is PostgreSQL session lifetime.** The manager holds one
  `AsyncSession` open per lock precisely because advisory locks are
  session-scoped on the server. See
  [Session and Connection Pool Model](#session-and-connection-pool-model).

### No optional-dependency guard

The package follows the library's optional-backend convention only in spirit,
not in mechanics. There is no `try`/`except ImportError` wrapper and no
`POSTGRES_AVAILABLE`-style flag to check at runtime, because the module imports
nothing beyond the standard library, `sqlalchemy`, and
`eventsource.observability` — and `sqlalchemy` is a core dependency. `asyncpg`
is not imported here at all; the driver is whatever the caller's
`async_sessionmaker` was built with. `import eventsource.adapters.postgresql.locks`
therefore always succeeds on a base install, and PostgreSQL availability is
discoverable only by attempting an acquisition.

Note also that the lock names are not re-exported from the top-level
`eventsource` package. **They are not reachable via `eventsource`.** Import
them from `eventsource.ports.locks` / `eventsource.adapters.postgresql.locks`
/ `eventsource.domain.exceptions` (see [Import Surface](#import-surface)).

### Effect on migration cutover

Because `MigrationCoordinator.cutover` delegates coordination to a
`PostgreSQLLockManager`, cutover inherits this constraint: a PostgreSQL database
must be reachable to serialize it across instances, independent of which stores
are being migrated between. The coordinator's `lock_manager` argument is
optional at construction (`lock_manager: PostgreSQLLockManager | None = None`),
so a coordinator can be built for sync-only work without one, but calling
cutover without a lock manager raises `MigrationError` rather than proceeding
unguarded.

If you need mutual exclusion in a deployment with no PostgreSQL instance, this
package will not provide it — use an external coordination service, or, for
single-process cases where cross-instance exclusion is not actually required, an
`asyncio.Lock`.

## `InMemoryLockManager` (test-only)

**Lead with what it does not guarantee.** `InMemoryLockManager`
(`eventsource.adapters.memory.locks`) excludes only coroutines running in one
`asyncio` event loop in one process:

- **No cross-process exclusion.** It is a dict guarded by an
  `asyncio.Condition`, not a database primitive. Two OS processes never
  contend against the same `InMemoryLockManager` instance.
- **No release on crash.** There is no server session to drop a lock when the
  holder disappears. A process that dies mid-critical-section leaves the lock
  held until the manager itself is garbage collected.
- **No fairness.** Waiters are woken in whatever order `asyncio.Condition`
  happens to wake them; there is no FIFO queue.
- **Single event loop, single process.** Construct one per test, per loop.
  Sharing an instance across loops is unsupported, same as
  `PostgreSQLLockManager`.

It conforms to the same `DistributedLock` / `LockRegistry` Protocols as
`PostgreSQLLockManager` -- same `acquire`/`try_acquire`/`release`/`is_held`
signatures, same `LockAcquisitionError` / `LockNotHeldError` on failure, and
the same key-to-lock-id hashing (`LockInfo.lock_id` matches what
`PostgreSQLLockManager` would derive for the same key) -- so code written
against the port can be tested against it without a database:

```python
from eventsource.adapters.memory.locks import InMemoryLockManager

lock_manager = InMemoryLockManager(holder_id="test-worker")

async with lock_manager.acquire("cutover:tenant-123") as lock_info:
    ...  # exercise code that needs a DistributedLock
```

Use it in unit tests that need a `DistributedLock` and nothing more. Use
`PostgreSQLLockManager` anywhere two processes must actually coordinate.

## Import Surface

The five public names split across three modules by ring (ADR 0029):

```python
from eventsource.ports.locks import LockInfo, migration_lock_key
from eventsource.domain.exceptions import LockAcquisitionError, LockNotHeldError
from eventsource.adapters.postgresql.locks import PostgreSQLLockManager
```

`eventsource.locks` (the pre-slice-A path) no longer exists (ADR 0030);
importing it raises `ModuleNotFoundError`.

### Not exported from the top-level package

None of these names appear in the top-level `eventsource` namespace. `from
eventsource import PostgreSQLLockManager` raises `ImportError`. This is a
deliberate departure from the library's usual convention of re-exporting the
public API from `eventsource/__init__.py`, and it is consistent with the
locks' PostgreSQL-only production nature — they are a backend-specific tool
rather than part of the core surface.

### The exceptions live in `eventsource.domain.exceptions` now

`LockAcquisitionError` and `LockNotHeldError` moved to
`eventsource.domain.exceptions` and now subclass `EventSourceError` (ADR 0029, the
one semantic change in the slice A structure work -- widening only). Two
consequences for calling code:

- Import them from `eventsource.domain.exceptions`, not from the locks modules.
- **`except EventSourceError` now catches a lock failure too.** Every
  existing `except LockAcquisitionError` and `except Exception` still catches
  exactly as before; the newly-catching clause is `except EventSourceError`,
  which caught nothing lock-related prior to this change.

In-tree, `eventsource.migration.cutover` follows this pattern, importing
`LockAcquisitionError` from `eventsource.domain.exceptions` and `migration_lock_key`
from `eventsource.ports.locks`.

### Typing-only imports

`PostgreSQLLockManager` is referenced under `if TYPE_CHECKING:` in
`eventsource.migration.cutover` and `eventsource.migration.coordinator`, which
is the right approach for annotating a `lock_manager` parameter without pulling
`eventsource.adapters.postgresql.locks` into module import at runtime. `from
__future__ import annotations` is already in effect in those modules, so the
deferred import costs nothing at runtime.

### What the import costs

`eventsource.ports.locks` is pure: stdlib only (`contextlib`, `dataclasses`,
`datetime`, `typing`, `uuid`), no sqlalchemy. Importing
`eventsource.adapters.postgresql.locks` pulls in the standard library
(`asyncio`, `hashlib`, `logging`, `contextlib`, `datetime`), `sqlalchemy`
(`text`, `AsyncSession`, `async_sessionmaker`), and
`eventsource.observability` for `Tracer` / `create_tracer`. No PostgreSQL
driver is imported — `asyncpg` never appears — so the import succeeds on a base
install regardless of which extras are present. See
[No optional-dependency guard](#no-optional-dependency-guard).

## Quick Reference

Constructor and full public surface at a glance. Import paths per
[Import Surface](#import-surface) above.

```python
PostgreSQLLockManager(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    holder_id: str | None = None,
    tracer: Tracer | None = None,
    enable_tracing: bool = True,
)
```

| Member | Signature | Returns | Raises |
| --- | --- | --- | --- |
| `acquire` | `acquire(key: str, *, timeout: float \| None = None, retry_interval: float = 0.1)` | async context manager yielding `LockInfo` | `LockAcquisitionError` |
| `try_acquire` | `await try_acquire(key: str)` | `LockInfo` on success, `None` if held elsewhere | underlying driver exception on database error |
| `release` | `await release(key: str)` | `None` | `LockNotHeldError` |
| `is_held` | `await is_held(key: str)` | `bool` | — |
| `release_all` | `await release_all()` | `int` (locks released) | — |
| `held_lock_count` | property (sync) | `int` | — |

| Module-level name | Signature | Returns |
| --- | --- | --- |
| `migration_lock_key` | `migration_lock_key(tenant_id: UUID, operation: str = "migration")` | `f"{operation}:{tenant_id}"` |

`LockInfo` is a frozen dataclass with four fields: `key: str`,
`lock_id: int`, `acquired_at: datetime` (UTC), `holder_id: str | None`.

### Context-managed acquisition (preferred)

Blocking — waits indefinitely for the lock, releases on exit either way:

```python
from eventsource.adapters.postgresql.locks import PostgreSQLLockManager

lock_manager = PostgreSQLLockManager(session_factory, holder_id="worker-1")

async with lock_manager.acquire("cutover:tenant-123") as lock_info:
    print(lock_info.lock_id)  # derived bigint, diagnostics only
    await perform_cutover()
```

Bounded — polls `pg_try_advisory_lock` every `retry_interval` seconds until the
deadline, then raises:

```python
from eventsource.domain.exceptions import LockAcquisitionError

try:
    async with lock_manager.acquire("cutover:tenant-123", timeout=5.0):
        await perform_cutover()
except LockAcquisitionError as exc:
    # exc.key, exc.reason ("Timeout after 5.0s"), exc.timeout
    print(f"another instance holds {exc.key}: {exc.reason}")
```

### Manual acquisition

`try_acquire` never waits and never raises on contention — it returns `None`.
Release is the caller's responsibility, so pair it with `try`/`finally`:

```python
lock_info = await lock_manager.try_acquire("cutover:tenant-123")
if lock_info is None:
    return  # someone else has it
try:
    await perform_cutover()
finally:
    await lock_manager.release("cutover:tenant-123")
```

### Inspection and shutdown

```python
await lock_manager.is_held("cutover:tenant-123")  # this manager only
lock_manager.held_lock_count                      # sync property, no await
released = await lock_manager.release_all()       # cleanup on shutdown
```

`is_held`, `held_lock_count`, and `release_all` report only locks held by *this*
manager instance — they say nothing about other processes contending for the
same key.

### Migration key helper

```python
from eventsource.ports.locks import migration_lock_key

key = migration_lock_key(tenant_id, "cutover")  # "cutover:<uuid>"
async with lock_manager.acquire(key):
    await perform_cutover()
```

### Reminders

- PostgreSQL only — the manager issues `pg_advisory_lock`,
  `pg_try_advisory_lock`, and `pg_advisory_unlock` as raw SQL with no fallback.
- One open `AsyncSession` per held lock; N concurrent locks consume N pool
  connections.
- `LockAcquisitionError` and `LockNotHeldError` derive from `Exception`, not
  from any eventsource error base.

## `PostgreSQLLockManager`

```python
class PostgreSQLLockManager:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        holder_id: str | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None: ...
```

The manager owns a set of PostgreSQL advisory locks on behalf of one process.
It is not a connection pool, a store, or a registry of locks held anywhere else
— it is a bookkeeper for the locks *it* took, plus the sessions keeping them
alive.

### Instance state

Construction performs no I/O. It only records the collaborators and initializes
empty bookkeeping:

| Attribute | Type | Role |
| --- | --- | --- |
| `_session_factory` | `async_sessionmaker[AsyncSession]` | Checks out one session per lock acquisition |
| `_holder_id` | `str \| None` | Copied verbatim into every `LockInfo.holder_id` |
| `_tracer` | `Tracer` | Wraps acquire and release in spans |
| `_held_locks` | `dict[str, tuple[AsyncSession, int]]` | Key → (session holding it, derived lock ID) |
| `_lock` | `asyncio.Lock` | Guards mutations of `_held_locks` |

`_held_locks` is the manager's whole notion of "held." `is_held`, `release`,
`release_all`, and `held_lock_count` all read from it, so they answer a
process-local question: *did this manager object take this key and not yet give
it back?* A second `PostgreSQLLockManager` in the same process, or the same
manager in another process, has a separate dictionary; none of these methods
query PostgreSQL to find out who else is contending. Only an acquisition
attempt does that.

### Lifecycle of a lock

Every acquisition follows the same three steps, regardless of which entry point
is used:

1. Derive the `bigint` lock ID from the string key
   (`_key_to_lock_id`, see [Key-to-Lock-ID Derivation](#key-to-lock-id-derivation)).
2. Check out a fresh `AsyncSession` from `session_factory` and issue
   `pg_advisory_lock` or `pg_try_advisory_lock` on it. On failure the session is
   closed before the error propagates, so a failed attempt leaks nothing.
3. On success, record `key -> (session, lock_id)` in `_held_locks` under the
   `asyncio.Lock`, log at `DEBUG`, and hand back a `LockInfo`.

Release is the mirror image, funnelled through a single internal
`_release_lock`: issue `pg_advisory_unlock` on the owning session, drop the key
from `_held_locks`, and close the session. The unlock statement is wrapped in
its own `try`/`except` that logs a `WARNING` rather than raising — a database
error during release does not prevent the bookkeeping entry from being removed
or the session from being closed, since both happen in a `finally`. The
practical upshot is that release never raises for database reasons; the only
exception it can raise is `LockNotHeldError`, and that comes from the public
`release()` wrapper before `_release_lock` is reached.

### Two acquisition styles

| | `acquire()` | `try_acquire()` |
| --- | --- | --- |
| Form | async context manager | plain coroutine |
| Waits? | yes (indefinitely, or up to `timeout`) | never |
| Contention result | raises `LockAcquisitionError` (only when `timeout` set) | returns `None` |
| Release | automatic, in `finally` | caller must call `release(key)` |
| DB error | re-raised as `LockAcquisitionError` | underlying driver exception propagates unchanged |

Prefer `acquire()`. Its `finally` block releases the lock on any exit path
— normal return, exception, or cancellation of the enclosing task — so the lock
cannot outlive the critical section. Reach for `try_acquire()` only when
"someone else has it, move on" is a legitimate outcome and you cannot express
the critical section as a `with` block.

Note the asymmetry in error handling between the two: `acquire()` translates
database failures into `LockAcquisitionError` with a `reason` of
`"Database error: ..."`, while `try_acquire()` logs at `ERROR` and re-raises
the driver's own exception. Code that must handle both uniformly should catch
`Exception` around `try_acquire()`, not just `LockAcquisitionError`.

### Thread- and task-safety

Mutations of `_held_locks` are serialized by an `asyncio.Lock`, which makes
`acquire`, `try_acquire`, `release`, `is_held`, and `release_all` safe to call
concurrently from multiple tasks *in the same event loop*. Two caveats:

- `held_lock_count` is a plain synchronous property that reads `len()` without
  taking the guard. Its docstring flags it as not async-safe; treat it as a
  diagnostic reading, not a value to branch on in concurrent code.
- The `asyncio.Lock` is bound to the loop that created it. A manager is not
  shareable across event loops or OS threads; construct one per loop.

Re-acquiring a key the manager already holds is *not* guarded. PostgreSQL
advisory locks are re-entrant within a session, but each acquisition here uses a
*new* session, so a second `acquire()` of the same key from the same manager
blocks against the first — a self-deadlock when `timeout=None`. The
`_held_locks` entry is also overwritten by the second acquisition, orphaning the
first session's bookkeeping. Do not nest acquisitions of the same key.

### Typical construction

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from eventsource.adapters.postgresql.locks import PostgreSQLLockManager

engine = create_async_engine("postgresql+asyncpg://localhost/app", pool_size=10)
session_factory = async_sessionmaker(engine, expire_on_commit=False)

lock_manager = PostgreSQLLockManager(
    session_factory,
    holder_id=f"worker-{os.getpid()}",
)
```

A single manager instance is intended to be long-lived and shared across the
tasks in a process — it is cheap to construct but holds the bookkeeping that
makes `release_all()` meaningful at shutdown. Wire `await
lock_manager.release_all()` into your shutdown path so locks are given back
deliberately rather than relying on connection teardown.

### Constructor

```python
PostgreSQLLockManager(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    holder_id: str | None = None,
    tracer: Tracer | None = None,
    enable_tracing: bool = True,
) -> None
```

One positional parameter, three keyword-only. Only `session_factory` is
required; the rest affect diagnostics rather than locking behaviour.

| Parameter | Type | Default | Effect |
| --- | --- | --- | --- |
| `session_factory` | `async_sessionmaker[AsyncSession]` | — | Source of the dedicated session opened per held lock |
| `holder_id` | `str \| None` | `None` | Copied into `LockInfo.holder_id`; debugging label only |
| `tracer` | `Tracer \| None` | `None` | Explicit tracer; overrides `enable_tracing` when given |
| `enable_tracing` | `bool` | `True` | Whether to build an OpenTelemetry tracer when `tracer` is omitted |

`__init__` performs no I/O and issues no SQL. It does not connect to
PostgreSQL, does not validate that the engine behind `session_factory` speaks
PostgreSQL, and does not verify that advisory-lock functions exist. A manager
constructed against a SQLite engine builds successfully and fails only on the
first `acquire()` or `try_acquire()`. Construction is therefore cheap and safe
at import or startup time, before the database is reachable.

The body assigns five attributes and nothing else:

```python
self._tracer = tracer or create_tracer(__name__, enable_tracing)
self._enable_tracing = self._tracer.enabled
self._session_factory = session_factory
self._holder_id = holder_id
self._held_locks: dict[str, tuple[AsyncSession, int]] = {}
self._lock = asyncio.Lock()
```

Two of those are worth noting for callers:

- **`_held_locks` starts empty**, so a freshly constructed manager reports
  `held_lock_count == 0` and `is_held(key) is False` for every key — even when
  another process holds that key in PostgreSQL. Held-lock state is per instance
  and never rehydrated from the database.
- **`_lock` is an `asyncio.Lock` created eagerly.** On modern Python it binds to
  the running loop on first use rather than at construction, but the manager is
  still single-loop: construct it inside the event loop that will use it, and do
  not share one instance across loops or threads.

There is no `close()`, `__aenter__`, or `__aexit__` on the manager itself. The
counterpart to construction is `release_all()`, which closes every session still
held; see [`release_all()`](#release_all).

#### Minimal construction

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from eventsource.adapters.postgresql.locks import PostgreSQLLockManager

engine = create_async_engine("postgresql+asyncpg://localhost/app")
session_factory = async_sessionmaker(engine, expire_on_commit=False)

lock_manager = PostgreSQLLockManager(session_factory)
```

Everything else is optional. The following two constructions behave identically
with respect to locking; they differ only in what shows up in `LockInfo` and in
your traces:

```python
plain = PostgreSQLLockManager(session_factory)
labelled = PostgreSQLLockManager(
    session_factory,
    holder_id=f"worker-{os.getpid()}",
    enable_tracing=False,
)
```

#### Keyword-only enforcement

`holder_id`, `tracer`, and `enable_tracing` sit behind a bare `*`, so they
cannot be passed positionally:

```python
PostgreSQLLockManager(session_factory, "worker-1")  # TypeError
PostgreSQLLockManager(session_factory, holder_id="worker-1")  # correct
```

#### How many managers to construct

One per process is the intended shape. The manager is stateful — its
`_held_locks` dictionary is the only record of which sessions must be closed —
so constructing a throwaway manager per operation defeats `release_all()` and
makes `is_held()` answer about a scope narrower than you likely mean. Build one
at application startup alongside the engine, share it across tasks, and release
it during shutdown.

Constructing several managers against the same `session_factory` is legal and
occasionally useful (independent `holder_id` labels, independent
`release_all()` scopes), but each maintains its own bookkeeping, so two managers
in the same process contend against each other for the same key exactly as two
processes would — each acquisition opens its own PostgreSQL session, and
advisory locks are session-scoped.

#### `session_factory`

```python
session_factory: async_sessionmaker[AsyncSession]
```

The only required parameter, and the manager's sole route to the database. It
is stored as `self._session_factory` and called — with no arguments — once per
acquisition attempt:

```python
session = self._session_factory()
```

That call happens in `_acquire_lock` (used by `acquire()`) and again in
`try_acquire()`. Nothing else in the class touches the factory: there is no
long-lived "control" session, no engine reference, and no reuse of a session
across two different keys.

##### What the manager expects of it

| Expectation | Why |
| --- | --- |
| Callable with zero arguments | Invoked as `self._session_factory()` |
| Produces an `AsyncSession` | `await session.execute(...)` and `await session.close()` are called on the result |
| Bound to a PostgreSQL engine | The statements are `pg_advisory_lock` / `pg_try_advisory_lock` / `pg_advisory_unlock` |
| Backed by a pool with spare capacity | One session stays checked out for the whole time a lock is held |

The annotation is `async_sessionmaker[AsyncSession]`, but the runtime
requirement is only the first two rows — any zero-arg callable returning an
object with async `execute` and `close` satisfies the code path, which is what
lets unit tests substitute a mock factory. Type checkers will still demand the
real `async_sessionmaker`, so production code should pass one.

The manager never calls `commit()`, `rollback()`, `begin()`, or `flush()` on the
sessions it creates, and never uses them as async context managers. Advisory
locks are session-scoped rather than transaction-scoped, so no transaction
boundary is needed to make the lock durable for its lifetime. Session
configuration that only affects ORM behaviour — `expire_on_commit`,
`autoflush`, mapped-class registry — is therefore irrelevant here; the factory's
only meaningful attribute is the engine it is bound to.

##### It is not validated at construction

`__init__` stores the factory and returns. It does not call it, does not open a
connection, and does not inspect the engine's dialect. A manager built over a
SQLite or MySQL engine constructs cleanly and fails on first use — see
[PostgreSQL-Only Constraint](#postgresql-only-constraint) for the shape of that
failure, which differs between `acquire()` (`LockAcquisitionError` with
`reason="Database error: ..."`) and `try_acquire()` (driver exception re-raised
after an `ERROR` log).

##### Session lifetime is lock lifetime

Each acquisition holds its session open for exactly as long as it holds the
lock:

1. `session = self._session_factory()` — a connection is checked out of the
   pool.
2. `SELECT pg_advisory_lock(...)` (or the `pg_try_advisory_lock` poll loop) runs
   on that session.
3. On success the session is stored in `_held_locks[key]` and stays open,
   idle, holding the lock.
4. `_release_lock` issues `pg_advisory_unlock` on the *same* session, then
   `await session.close()` in a `finally`, returning the connection to the pool.

Failure paths close the session before propagating: the timeout branch closes it
before raising `LockAcquisitionError`, the generic `except Exception` branch
closes it before re-raising as `LockAcquisitionError`, and `try_acquire()`
closes it both when the lock is already held elsewhere (returning `None`) and
when the statement errors. A failed acquisition therefore does not leak a
connection.

The direct consequence for sizing: **the pool must accommodate one connection
per concurrently held lock, on top of everything else the application is
doing.** Sharing an application's main `session_factory` with the lock manager
is fine as long as that headroom exists; if locks are held for long stretches or
in large numbers, give the manager a factory over its own small engine so a
stuck lock cannot starve request handling. See
[Session and Connection Pool Model](#session-and-connection-pool-model) for the
full treatment.

##### Typical wiring

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from eventsource.adapters.postgresql.locks import PostgreSQLLockManager

engine = create_async_engine(
    "postgresql+asyncpg://localhost/app",
    pool_size=10,
    max_overflow=5,
)
session_factory = async_sessionmaker(engine, expire_on_commit=False)

lock_manager = PostgreSQLLockManager(session_factory)
```

A dedicated engine, when lock traffic should be isolated from application
traffic:

```python
lock_engine = create_async_engine(
    "postgresql+asyncpg://localhost/app",
    pool_size=4,          # ceiling on simultaneously held locks
    max_overflow=0,
)
lock_manager = PostgreSQLLockManager(
    async_sessionmaker(lock_engine, expire_on_commit=False),
    holder_id=f"worker-{os.getpid()}",
)
```

Note that a pool ceiling becomes a *lock* ceiling: with `pool_size=4` and
`max_overflow=0`, a fifth concurrent acquisition blocks waiting for a connection
before it ever reaches `pg_advisory_lock`, and that wait is governed by the
engine's `pool_timeout`, not by the `timeout` argument to `acquire()`.

##### Sharing with the rest of the application

The factory may be the same one used by other session-based PostgreSQL-backed
components — the manager holds no exclusive claim on it and adds no state to
the sessions it borrows. (Note that `PostgreSQLEventStore` itself is
constructed from an `AsyncEngine`, not a session factory, so it is not a
sharing candidate here — but `PostgreSQLSnapshotStore` and other
session-factory-based components are.) What it must *not* be given is a
single already-open `AsyncSession` wrapped in a lambda: every acquisition needs
its own session, because two locks sharing one PostgreSQL session would also
share lock ownership, and closing one would drop the other.

##### Connection reuse after a release failure

`_release_lock` catches any error from `pg_advisory_unlock`, logs a `WARNING`,
and proceeds to close the session regardless. Closing returns the connection to
the pool, and pool checkin rolls back the transaction — but a rollback does not
clear session-level advisory locks. In the rare case where the unlock statement
itself fails yet the connection survives and is recycled, the advisory lock can
outlive the manager's bookkeeping, since it is released only when that backend
connection actually terminates. Treat repeated "Error releasing advisory lock"
warnings as a signal to recycle the engine's pool (`await engine.dispose()`)
rather than as noise.
