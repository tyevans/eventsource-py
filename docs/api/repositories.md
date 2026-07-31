# Repositories API Reference

Technical reference for the `eventsource.repositories` package: the
transactional outbox repository, plus its PostgreSQL, SQLite, and in-memory
backends.

> **Checkpoint and DLQ repositories moved.** They used to live here, but
> mixed a Protocol definition with sqlalchemy-backed implementations in the
> same module — the same defect ADR 0019 had already fixed for the event
> store. [ADR 0024](../adrs/0024-projection-persistence-ports.md) split them
> out: the checkpoint contract (`ProjectionCheckpoints`,
> `SubscriptionPositions`, and the composed `CheckpointRepository`) and the
> DLQ contract (`DLQRepository`) now live in `eventsource.ports.checkpoints`
> and `eventsource.ports.dlq`; their dialect-parameterized SQL
> implementations (`SQLCheckpointRepository`, `SQLDLQRepository`) live in
> `eventsource.adapters.sql`, and the in-memory ones
> (`InMemoryCheckpointRepository`, `InMemoryDLQRepository`) in
> `eventsource.adapters.memory`. All of the above are re-exported from the
> top-level `eventsource` package. The checkpoint and DLQ functions that
> consume them (`record_checkpoint`, `read_checkpoint`, `lag_metrics_dict`,
> `reset_checkpoint`, `send_to_dlq`, `read_failed_events`) are documented on
> [Projections](projections.md).

The package now holds one public source module, plus two private helpers:

| Module | Contains |
| --- | --- |
| `eventsource.repositories.outbox` | `OutboxRepository`, `OutboxRepositoryProtocol`, `PostgreSQLOutboxRepository`, `SQLiteOutboxRepository`, `InMemoryOutboxRepository`, `OutboxEntry`, `OutboxStats` |
| `eventsource.repositories._connection` | Private connection-handling helper shared by the SQL backends |
| `eventsource.repositories._json` | Deprecated JSON shim; import from `eventsource.serialization` instead |

Every name above from the public module is re-exported from the
`eventsource.repositories` barrel, along with three JSON utilities forwarded
from `eventsource.serialization` (`EventSourceJSONEncoder`, `json_dumps`,
`json_loads`).

The family follows the same shape as the checkpoint and DLQ ports it used to
share this file with: a Protocol defining the contract, a PostgreSQL
implementation for production, a SQLite implementation for lightweight
deployments, and an in-memory implementation for tests. Code that depends
only on the Protocol can swap backends without changes; the places where the
SQLite backend behaves differently from PostgreSQL are called out in
[SQLite backend deviations](#sqlite-backend-deviations).

## Overview

### What this module provides

`eventsource.repositories` now covers one piece of supporting
infrastructure: the transactional outbox.

**Transactional outbox** (`outbox.py`) implements the outbox pattern: events
are persisted in the same transaction as the aggregate change, then
published asynchronously by a worker. This gives at-least-once delivery even
when the event bus is temporarily unavailable, and keeps publishing off the
request path. An `OutboxEntry` carries the event ID, type, aggregate ID and
type, optional tenant ID, serialized payload, a status of `pending`,
`published`, or `failed`, publish timestamp, retry count, and last error.
`get_stats()` -> `OutboxStats` summarizes the backlog.

The `OutboxRepository` contract is a `@runtime_checkable` Protocol, so
`isinstance()` checks against it work and any object with matching methods
satisfies it — you are not required to subclass to supply your own backend.

None of the three outbox backends are constructed for you as part of a store
or bus. You instantiate the one you want and pass it to the component that
uses it.

### Import surface: `from eventsource.repositories import ...`

Every public name in the package is re-exported from the package root:

```python
from eventsource.repositories import (
    OutboxRepository,
    PostgreSQLOutboxRepository,
    SQLiteOutboxRepository,
    InMemoryOutboxRepository,
)
```

`__all__` in `eventsource/repositories/__init__.py` lists **9 names**:

| Group | Count | Names |
| --- | --- | --- |
| Outbox | 6 | `OutboxRepository`, `OutboxRepositoryProtocol`, `PostgreSQLOutboxRepository`, `SQLiteOutboxRepository`, `InMemoryOutboxRepository`, `OutboxEntry`, `OutboxStats` |
| JSON utilities | 3 | `EventSourceJSONEncoder`, `json_dumps`, `json_loads` |

The three JSON utilities are not defined here — they are forwarded from
`eventsource.serialization`; see
[Re-exported JSON utilities](#re-exported-json-utilities-eventsourcejsonencoder-json_dumps-json_loads).

#### The `OutboxRepositoryProtocol` alias

`OutboxRepositoryProtocol` is the same object as `OutboxRepository`, assigned
at the bottom of `outbox.py`:

```python
OutboxRepositoryProtocol = OutboxRepository  # outbox.py:1080
```

It exists for readers who prefer the explicit `...Protocol` suffix in
annotations. `OutboxRepositoryProtocol is OutboxRepository` is `True`, so the
two are fully interchangeable in annotations and `isinstance()` checks.

#### Also available from the top-level namespace

Most of these names are additionally re-exported from `eventsource` itself:
`OutboxRepositoryProtocol`, `PostgreSQLOutboxRepository`,
`InMemoryOutboxRepository`, and both data types. `SQLiteOutboxRepository` is
exported too, but only conditionally — `eventsource/__init__.py` imports it
inside a `try/except ImportError` alongside `SQLiteEventStore` and adds it to
`__all__` only when `SQLITE_AVAILABLE` is `True` (that is, when `aiosqlite`
is installed). `OutboxRepositoryProtocol` and the JSON utilities are **not**
re-exported at the top level.

Importing from `eventsource.repositories` is the recommended form for this
package: it works uniformly for every name, including the alias, and does
not depend on whether the optional SQLite extra is installed.

### Method naming convention (`get_` / `list_` / `add_` / `update_` / `delete_` / `save_`)

The package docstring in `eventsource/repositories/__init__.py` declares the
prefix convention that repository methods follow:

| Prefix | Meaning | Examples in this package |
| --- | --- | --- |
| `get_{entity}` | Fetch a single entity by ID | `get_stats()` |
| `list_{entities}` | Fetch multiple entities, with filtering | `list_pending_events()` |
| `add_{entity}` | Create a new entity | `add_event()` |
| `update_{entity}` | Update an existing entity | — |
| `delete_{entity}` | Delete an entity | `cleanup_published()` |
| `save_{entity}` | Upsert — create or update | — |

All repository methods are `async`; every name below is awaited.

#### Compatibility pair

One method exists under two names. The `list_*` form was added later for
consistency with the table above, and is implemented as a one-line
delegation to the original — not as a separate query path. Both names are
declared on the Protocol and defined on all three backends, so either is
safe to call and neither changes behavior.

| Preferred | Original | Delegation site |
| --- | --- | --- |
| `list_pending_events(limit=100)` | `get_pending_events(limit=100)` | `outbox.py:492` (PostgreSQL), also on SQLite and InMemory |

The alias carries the docstring "Alias for `get_pending_events()` -
preferred for naming consistency". Use the preferred column in new code; the
original is not deprecated and emits no warning.

#### Where the convention does not apply

The convention describes the CRUD-shaped methods only. Several methods use
domain verbs instead, and are named for the state transition or computation
they perform rather than for a storage operation: `mark_published()`,
`mark_failed()`, `increment_retry()`, `cleanup_published()`, `get_stats()`.

The two "delete-ish" methods return an `int` count of rows removed rather
than `None`: `cleanup_published(days=7)`.

## Selecting a backend

The outbox family ships three backends. They differ only in what you hand
the constructor and where the data lives; the method surface is the
Protocol's in every case (plus `clear()` on the in-memory one).

| Backend | Constructor argument | Storage | Intended use |
| --- | --- | --- | --- |
| `PostgreSQLOutboxRepository` | `conn: AsyncConnection \| AsyncEngine` | `event_outbox` table | Production |
| `SQLiteOutboxRepository` | `connection: aiosqlite.Connection` | Same table name, SQLite schema | Single-node, embedded, local dev |
| `InMemoryOutboxRepository` | *(none)* | Process memory | Tests |

Because the contract is a `@runtime_checkable` Protocol, annotate call sites
with the Protocol and choose the backend at composition time:

```python
from eventsource.repositories import (
    OutboxRepository,
    InMemoryOutboxRepository,
    PostgreSQLOutboxRepository,
)

def build_outbox(engine=None) -> OutboxRepository:
    if engine is None:
        return InMemoryOutboxRepository()
    return PostgreSQLOutboxRepository(engine)
```

### PostgreSQL (production) — `AsyncConnection | AsyncEngine`

```python
def __init__(
    self,
    conn: AsyncConnection | AsyncEngine,
    tracer: Tracer | None = None,
    enable_tracing: bool = True,
) -> None: ...
```

`conn` is the only required argument and is stored unchanged on the public
attribute `self.conn`. Both accepted types come from `sqlalchemy.ext.asyncio`.
The type is not validated at construction time — passing something else fails on
the first method call, not in `__init__`.

#### Engine vs. connection: the transaction boundary

Every statement runs inside the shared helper
`execute_with_connection(conn, transactional=...)` from
`eventsource/repositories/_connection.py`. That helper is what makes the union
type work, and its two branches differ in who owns the transaction:

| You pass | Write methods (`transactional=True`) | Read methods (`transactional=False`) | Who commits |
| --- | --- | --- | --- |
| `AsyncEngine` | `async with engine.begin()` | `async with engine.connect()` | The repository, per call |
| `AsyncConnection` | connection yielded as-is | connection yielded as-is | **The caller** |

With an `AsyncEngine`, each method is self-contained: it checks out a
connection, runs its statement, commits (for writes), and returns the connection
to the pool. Two calls are two transactions.

With an `AsyncConnection`, the `transactional` argument has no effect — the
helper yields the connection directly and, in its own words, "caller is
responsible for transaction management." The repository issues no `COMMIT` and
no `ROLLBACK`. Build one on a bare `engine.connect()` connection and never
commit, and the writes are discarded.

#### Which mode to use

Pass a **connection** when the repository write must be atomic with other work
in the same transaction. That is the whole point of the outbox pattern: staging
the outbox rows and appending the events either both happen or neither does.

```python
async with engine.begin() as conn:
    await store.append_events(aggregate_id, events, expected_version)
    outbox = PostgreSQLOutboxRepository(conn)
    for event in events:
        await outbox.add_event(event)
    # commit happens when the engine.begin() block exits
```

Constructing the repository inside the block, as above, matches its lifetime: it
holds a reference to that connection, not a factory, so it is only usable while
the connection is open. A repository built on an engine has no such
restriction and can be constructed once at startup and kept for the process
lifetime — pass an **engine** for outbox work that stands on its own, such as
a publisher worker marking rows published.

The class docstring for `PostgreSQLOutboxRepository` shows the connection
form. Both forms are supported; the docstring example is not a restriction.

#### PostgreSQL-specific SQL

This backend emits PostgreSQL-dialect SQL through `sqlalchemy.text()` with
named bind parameters, and is not portable to another server.

Any asyncpg-compatible URL works (`postgresql+asyncpg://…`). The library never
creates the engine for you.

#### Schema prerequisite

The repository does not create its table. The backing table must exist
before the first call, or the statement fails with a database error:

| Repository | Table | DDL template |
| --- | --- | --- |
| `PostgreSQLOutboxRepository` | `event_outbox` | `migrations/templates/outbox.sql` |

Templates live under `src/eventsource/migrations/templates/`; apply them with
your own migration tooling, or use the Alembic scaffold provided alongside them
in `migrations/templates/alembic/`.

### SQLite (lightweight) — `aiosqlite.Connection`

`SQLiteOutboxRepository` has a constructor shaped like the PostgreSQL one, but
the connection argument is named `connection` and must be an open `aiosqlite.Connection`:

```python
def __init__(
    self,
    connection: "aiosqlite.Connection",
    tracer: Tracer | None = None,
    enable_tracing: bool = True,
) -> None: ...
```

The connection is stored on the private attribute `self._connection` — there is
no public `conn` attribute as on the PostgreSQL backend.

A SQLAlchemy engine or a stdlib `sqlite3.Connection` will not work: the code
calls `await connection.execute(...)`, `await cursor.fetchone()` /
`fetchall()`, and `await connection.commit()`, which are the aiosqlite coroutine
forms.

```python
import aiosqlite
from eventsource.repositories import SQLiteOutboxRepository

async with aiosqlite.connect("events.db") as db:
    repo = SQLiteOutboxRepository(db)
    await repo.add_event(event)
```

#### Availability

`aiosqlite` is an optional dependency (`uv sync --extra sqlite`, or install
`aiosqlite` directly). Importing `SQLiteOutboxRepository` from
`eventsource.repositories` always succeeds regardless — `outbox.py` imports
`aiosqlite` only under `if TYPE_CHECKING:`. Only `aiosqlite.connect()` in your
own code needs the package present.

The top-level namespace behaves differently: `eventsource/__init__.py` imports
`SQLiteOutboxRepository` inside a `try/except ImportError` next to
`SQLiteEventStore` and appends it to `__all__` only when `SQLITE_AVAILABLE`
is `True`. `from eventsource import SQLiteOutboxRepository` therefore fails on a
machine without `aiosqlite`, while
`from eventsource.repositories import SQLiteOutboxRepository` succeeds. Prefer
the latter.

#### Transactions: the repository commits, you do not

This is the sharpest difference from the PostgreSQL backend. There is no
engine-versus-connection distinction and no `execute_with_connection()` helper:
the SQLite repository holds the one connection you gave it for its lifetime and
calls `await self._connection.commit()` itself at the end of every write.

| Repository | Methods that commit |
| --- | --- |
| `SQLiteOutboxRepository` | `add_event()`, `mark_published()`, `mark_failed()`, `increment_retry()`, `cleanup_published()` |

Read methods issue no commit.

The consequence: the atomic-outbox pattern shown for PostgreSQL does not carry
over. With PostgreSQL you can construct `PostgreSQLOutboxRepository(conn)`
inside an `engine.begin()` block and have the outbox insert commit together with
the event append. With SQLite, `add_event()` commits on its own, ending whatever
transaction was open on that connection — including work the caller had staged
but not yet committed. If you need the outbox row and the event append to be
atomic on SQLite, write the outbox row with your own SQL inside your own
transaction rather than through this repository.

#### Schema prerequisite

As with PostgreSQL, no table is created for you. The SQLite DDL lives in a
separate directory from the PostgreSQL templates:

| Repository | Table | DDL template |
| --- | --- | --- |
| `SQLiteOutboxRepository` | `event_outbox` | `migrations/templates/sqlite/outbox.sql` |

Templates are under `src/eventsource/migrations/templates/sqlite/`, alongside
`events.sql` and `snapshots.sql`.

#### What differs from PostgreSQL

At the Protocol level this backend is interchangeable with the PostgreSQL
one — the SQLite class passes the same `isinstance()` check against
`OutboxRepository` (`tests/repositories/test_sqlite_repos.py`). Below the
interface, storage and query shape differ:

- UUIDs are stored as 36-character hyphenated TEXT and timestamps as ISO 8601
  TEXT, parsed back on read.
- Statements use `?` positional parameters, not the named binds of the
  PostgreSQL dialect.

Full detail is in [SQLite backend deviations](#sqlite-backend-deviations).

> **Discrepancy worth checking.** `SQLiteOutboxRepository.add_event()` generates
> a `uuid4()` and inserts `str(outbox_id)` into `event_outbox.id`, but
> `migrations/templates/sqlite/outbox.sql` declares that column as
> `id INTEGER PRIMARY KEY AUTOINCREMENT`, which rejects a non-integer value.
> The test fixture in `tests/conftest.py` creates the table as
> `id TEXT PRIMARY KEY` instead, so the test suite does not exercise the shipped
> template. Anyone applying `sqlite/outbox.sql` verbatim should expect a
> datatype-mismatch error on the first `add_event()`.

### InMemory (testing) — no connection, adds `clear()`

`InMemoryOutboxRepository` takes no connection at all:

```python
from eventsource.repositories import InMemoryOutboxRepository

repo = InMemoryOutboxRepository()
```

State is plain dicts guarded by an `asyncio.Lock`, so concurrent tasks in one
event loop are safe; nothing is shared across processes and everything is lost
when the process exits. It adds one method beyond its Protocol:

```python
await repo.clear()  # async, despite being a test helper
```

`clear()` drops all stored state. It is deliberately not on the Protocol, so
code typed against `OutboxRepository` cannot call it; reach for it in fixture
teardown where you hold the concrete type.

### Common constructor keywords

Every one of the three outbox repositories accepts the same two optional keywords after
its connection argument:

| Keyword | Type | Default | Effect |
| --- | --- | --- | --- |
| `tracer` | `Tracer \| None` | `None` | Supply a tracer instead of letting the repository build one |
| `enable_tracing` | `bool` | `True` | Whether to build an OpenTelemetry tracer at all |

Both feed one line in `__init__`:

```python
self._tracer = tracer or create_tracer(__name__, enable_tracing)
```

`create_tracer()` returns an `OpenTelemetryTracer` when tracing is enabled *and*
the optional `opentelemetry` dependency is importable, and a no-op `NullTracer`
otherwise. So leaving `enable_tracing=True` on a deployment without OpenTelemetry
installed costs nothing — the spans are dropped at the tracer, not guarded at
each call site. Pass `enable_tracing=False` to force the null tracer even where
OpenTelemetry is available, or pass an explicit `tracer` to share one instance
across repositories (in which case `enable_tracing` is ignored, and the
repository's own `_enable_tracing` reflects the tracer you supplied).
