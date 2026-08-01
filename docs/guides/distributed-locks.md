# Coordinate work with distributed locks

When several instances of your service run the same code, some operations must
happen exactly once: a tenant cutover, a schema migration, a projection rebuild.
`PostgreSQLLockManager` gives you a mutual-exclusion primitive built on
PostgreSQL *advisory locks* — application-level locks that are independent of
table and row locks, live for the duration of the database session that took
them, and are released automatically when that session's connection closes.

This guide shows you how to:

- Build a `PostgreSQLLockManager` from a SQLAlchemy `async_sessionmaker`, and set
  `holder_id` and tracing options.
- Choose lock keys, including the `migration_lock_key(tenant_id, operation)`
  helper, and understand how a string key becomes a PostgreSQL lock ID.
- Guard a critical section with `async with lock_manager.acquire(key)`, blocking
  indefinitely or bounded by a `timeout`.
- Elect a leader with the non-blocking `try_acquire()` / `release()` pair, and
  inspect ownership with `is_held()` and `held_lock_count`.
- Clean up with `release_all()` on shutdown, and size your connection pool for
  the one pinned session each held lock consumes.
- Pass the manager into `MigrationCoordinator` and cutover, and diagnose the
  errors these APIs raise.

The five names split across three modules by ring (ADR 0029):

```python
from eventsource.ports.locks import LockInfo, migration_lock_key
from eventsource.domain.exceptions import LockAcquisitionError, LockNotHeldError
from eventsource.adapters.postgresql.locks import PostgreSQLLockManager
```

Note that none of these are exported from the top-level `eventsource`
package. `eventsource.locks` (the pre-slice-A path) no longer exists (ADR
0030); importing it raises `ModuleNotFoundError`.

## Before you start: PostgreSQL only

`PostgreSQLLockManager` is the only distributed lock implementation the library
ships, and it is not backend-agnostic. Every acquisition and release issues raw
PostgreSQL advisory-lock SQL:

```sql
SELECT pg_advisory_lock(:lock_id)      -- blocking acquire
SELECT pg_try_advisory_lock(:lock_id)  -- non-blocking attempt
SELECT pg_advisory_unlock(:lock_id)    -- release
```

Those functions exist only in PostgreSQL. Point the manager's
`async_sessionmaker` at SQLite (or any other engine) and the statements fail at
the database, surfacing as a `LockAcquisitionError` whose `reason` carries the
underlying driver error — not as a clean "unsupported backend" message at
construction time.

`eventsource.ports.locks` defines `DistributedLock` and `LockRegistry` --
small Protocols describing the shape of the dependency (ADR 0029), used by
`eventsource.testing.conformance_ports.DistributedLockConformance`. They do
**not** promise a backend-agnostic distributed-locking guarantee: nothing in
the Protocols says anything about cross-process exclusion, release on crash,
or fairness, because `PostgreSQLLockManager` is the only implementation that
offers those properties. `MigrationCoordinator` and `CutoverManager` both
annotate their `lock_manager` parameter as `PostgreSQLLockManager`
specifically, not as the Protocol, because they need its PostgreSQL
guarantees. `eventsource.adapters.memory.locks.InMemoryLockManager` also
conforms to the Protocols, but only for single-process testing -- see
[Testing without PostgreSQL](#testing-without-postgresql) below.

### What to do on SQLite or other backends

You have three practical options, in descending order of preference:

**Use PostgreSQL just for the locks.** The lock manager takes its own
`async_sessionmaker` and never touches your event tables — it only calls the
advisory-lock functions. A small PostgreSQL instance alongside a SQLite event
store is enough to coordinate cutovers, and it keeps the library code paths
you're using on their tested configuration.

**Run a single instance for the guarded operation.** SQLite deployments are
typically single-process anyway. If exactly one process performs migrations,
cutovers, and rebuilds, the lock is redundant — enforce that operationally (a
dedicated worker, a `--role=migrator` flag, a cron job on one host) rather than
in code.

**Coordinate externally.** If you already run Redis, etcd, or ZooKeeper, take
the lock there and call the library's operations inside your own critical
section. Note the consequence for migration: `MigrationCoordinator` raises
`MigrationError` when you ask it to cut over without a lock manager —

> Cannot perform cutover: lock_manager not provided to coordinator. Provide a
> PostgreSQLLockManager when creating the coordinator to enable cutover
> operations.

— so external coordination means driving the cutover steps yourself rather than
through `MigrationCoordinator`, or supplying a duck-typed manager with the
caveats above.

Everything else in this guide assumes PostgreSQL.

## Testing without PostgreSQL

For unit tests that need a `DistributedLock` and nothing more,
`eventsource.adapters.memory.locks.InMemoryLockManager` conforms to the same
Protocols as `PostgreSQLLockManager` -- same `acquire()` / `try_acquire()` /
`release()` / `is_held()` signatures, same exceptions, same key-to-lock-id
hashing:

```python
from eventsource.adapters.memory.locks import InMemoryLockManager

lock_manager = InMemoryLockManager(holder_id="test-worker")

async with lock_manager.acquire("cutover:tenant-123"):
    ...  # exercise code that needs a DistributedLock
```

It is a test double, not a distributed lock: no cross-process exclusion, no
release on crash, no fairness; single event loop, single process. Do not use
it for anything but tests.

## Construct a PostgreSQLLockManager from an async_sessionmaker

`PostgreSQLLockManager` takes one positional argument: a SQLAlchemy
`async_sessionmaker`. Build the engine and factory as usual, then hand the
factory to the manager:

```python
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from eventsource.adapters.postgresql.locks import PostgreSQLLockManager

engine = create_async_engine(
    "postgresql+asyncpg://user:pass@localhost/app",
    pool_size=10,
)
session_factory = async_sessionmaker(engine, expire_on_commit=False)

lock_manager = PostgreSQLLockManager(session_factory)
```

The manager stores the factory and calls it once per lock acquisition — each
held lock gets its own `AsyncSession`, kept open for as long as the lock is
held. Nothing is opened at construction time, so building the manager never
touches the database and cannot fail on a bad connection string; the first
`acquire()` will.

Construct the manager once and share it. Its internal registry of held locks
(`key -> (session, lock_id)`) is guarded by an `asyncio.Lock`, so concurrent
tasks in the same event loop can safely acquire and release through a single
instance. Two managers built from the same factory are two independent holders:
they do not see each other's locks, and because advisory locks are session-
scoped, one will block on a key the other holds.

Only the session factory is required. The remaining parameters are
keyword-only:

```python
lock_manager = PostgreSQLLockManager(
    session_factory,
    holder_id="worker-3",
    enable_tracing=False,
)
```

### Set holder_id for debuggable lock ownership

`holder_id` is a free-form string identifying *this* holder. It defaults to
`None`, is never sent to PostgreSQL, and has no effect on which locks can be
acquired — its single job is to be copied onto every `LockInfo` this manager
produces — both the one yielded by `acquire()` and the one returned by
`try_acquire()`:

```python
lock_manager = PostgreSQLLockManager(session_factory, holder_id="worker-3")

async with lock_manager.acquire("cutover:tenant-abc") as info:
    print(info.holder_id)  # "worker-3"
```

Because PostgreSQL advisory locks carry no application-level owner metadata,
`holder_id` is the only handle you get on "who is holding this". Note that the
manager's own `logger.debug` records for acquire and release log only `key` and
`lock_id` — if you want the holder in your logs, read it off the `LockInfo` and
log it yourself. Give it a value that identifies the process uniquely: a pod
name, a hostname plus PID, a worker index. A constant like `"app"` across fifty
replicas tells you nothing.

### Disable tracing with enable_tracing=False (or inject a Tracer)

The manager emits two spans through its tracer: `eventsource.lock.acquire`
around the blocking/timeout acquisition path, and `eventsource.lock.release`
around every release (including the automatic one at the end of an `acquire()`
block and each release performed by `release_all()`). By default the manager
builds a tracer for itself:

```python
self._tracer = tracer or create_tracer(__name__, enable_tracing)
```

`create_tracer` returns an OpenTelemetry-backed tracer when
`enable_tracing=True` *and* the optional OpenTelemetry dependency is installed;
otherwise it returns a no-op `NullTracer`. So with the default
`enable_tracing=True` and no OpenTelemetry installed, you already get the no-op
path — no configuration needed.

Pass `enable_tracing=False` to force the no-op tracer even where OpenTelemetry
is available. This is what the library's own integration tests do, and it is
the right choice for tests and short-lived scripts where lock spans are noise:

```python
lock_manager = PostgreSQLLockManager(
    session_factory,
    holder_id="test-worker",
    enable_tracing=False,
)
```

To route lock spans through a tracer you already own, pass `tracer=` instead:

```python
from eventsource.observability import create_tracer

tracer = create_tracer("myapp.locks", enable_tracing=True)
lock_manager = PostgreSQLLockManager(session_factory, tracer=tracer)
```

When `tracer` is given, `enable_tracing` is ignored entirely — the injected
tracer decides. The acquire span carries `lock.key`, `lock.id`, and
`lock.timeout` (`-1` when no timeout was given, since span attributes cannot be
`None`); the release span carries `lock.key` and `lock.id`. `try_acquire()` is
not traced — if you build a leader election loop on it (see below), add your own
instrumentation.

Neither span records an error status for a failed unlock: `_release_lock`
catches any exception from `pg_advisory_unlock`, logs a warning, and still drops
the key from the held-lock registry and closes the session. Treat the release
span as "release was attempted", not "release succeeded", and watch the logs for
the warning.

## Choose a lock key convention

A lock key is any string you like. The manager hashes it into the numeric ID
PostgreSQL actually locks on, so `"cutover:tenant-abc"`, an empty string, and a
1000-character key are all valid — the unit tests exercise exactly those cases,
plus Unicode keys.

That freedom is the problem. Advisory lock IDs live in a single flat namespace
per database: there is no schema, table, or tenant scoping. Any two callers that
derive the same ID contend, and any two that derive different IDs do not — even
if they meant to guard the same resource. Your key convention *is* your
mutual-exclusion boundary, so make it explicit rather than ad hoc.

Use a `{operation}:{scope}` shape, mirroring what the library itself does:

```python
f"cutover:{tenant_id}"          # one cutover per tenant
f"migration:{tenant_id}"        # one migration per tenant
f"rebuild:{projection_name}"    # one rebuild per projection
f"leader:orders-subscription"   # one leader per subscription
```

Rules that pay off:

- **Put the operation first.** Keys sort and grep sensibly, and `held_lock_count`
  debugging sessions read better.
- **Scope to the narrowest correct unit.** `f"rebuild:{name}"` lets two
  projections rebuild concurrently; `"rebuild"` serializes them all.
- **Derive keys from a single function, not inline f-strings.** A typo in one
  call site silently produces a *different* lock, and nothing fails loudly —
  both callers just proceed into the critical section.
- **Keep keys stable across releases.** Renaming a key during a rolling deploy
  means old and new pods hold different locks and both run.
- **Do not encode volatile data** (timestamps, attempt counters, PIDs) into the
  key. Every distinct value is a distinct lock, which defeats exclusion.

One caveat on the held-lock registry: the manager tracks locks it holds in a
dict keyed by your *string*, while contention happens on the derived numeric ID.
Two different strings that collide to the same ID would be two registry entries
for one PostgreSQL lock — the second `acquire()` from a *different* manager
would block, but within one manager the entries are independent. With SHA-256
truncated to 63 bits this is not something to plan around; it is a reason to
treat keys as canonical identifiers rather than free text.

### Use migration_lock_key(tenant_id, operation) for migration and cutover locks

For anything migration-related, don't invent a convention — use the helper:

```python
from eventsource.ports.locks import migration_lock_key

key = migration_lock_key(tenant_id)             # "migration:<uuid>"
key = migration_lock_key(tenant_id, "cutover")  # "cutover:<uuid>"
```

The signature is `migration_lock_key(tenant_id: UUID, operation: str = "migration") -> str`,
and the body is a single f-string: `f"{operation}:{tenant_id}"`. The `tenant_id`
is a `UUID`, formatted with `str()`.

Using it matters because `CutoverManager` calls it internally. Both the cutover
execution path and the pre-flight readiness check build their key with
`migration_lock_key(tenant_id, "cutover")` — the first wraps the cutover in
`acquire(lock_key, timeout=self._lock_acquisition_timeout)`, the second uses
`try_acquire()` to test availability and immediately `release()`s it. If your
own code guards the same tenant with a hand-written `f"cutover-{tenant_id}"`,
it hashes to a different ID and provides no exclusion against the library's
cutover at all.

Note the two paths behave differently on contention. The cutover path catches
`LockAcquisitionError` and returns an unsuccessful `CutoverResult` rather than
raising; the readiness check simply reports `(False, "Cutover lock is already
held by another process")`. Because that check releases the lock the instant it
succeeds, a `True` answer means "was free a moment ago", not "is reserved for
you" — the real exclusion happens inside the cutover itself.

Distinct operations produce distinct locks, by design: `migration_lock_key(t,
"migration")` and `migration_lock_key(t, "cutover")` do not contend, so a
long-running backfill for a tenant does not block that tenant's cutover
readiness check. If you want them mutually exclusive, use the same `operation`
string for both.

The helper is tenant-scoped and takes a `UUID`. For non-tenant resources
(projection rebuilds, singleton jobs) write your own small helper following the
same `{operation}:{scope}` shape rather than forcing an unrelated value through
this one.
