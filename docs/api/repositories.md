# Outbox API Reference

Technical reference for the transactional outbox: the `OutboxRepository`
port and its PostgreSQL, SQLite, and in-memory adapters.

> **Checkpoint and DLQ repositories moved.** They used to live in this file
> too, back when it documented the whole `eventsource.repositories`
> package. [ADR 0024](../adrs/0024-projection-persistence-ports.md) split
> them out: the checkpoint contract (`ProjectionCheckpoints`,
> `SubscriptionPositions`, and the composed `CheckpointRepository`) and the
> DLQ contract (`DLQRepository`) live in `eventsource.ports.checkpoints`
> and `eventsource.ports.dlq`; their dialect-parameterized SQL
> implementations (`SQLCheckpointRepository`, `SQLDLQRepository`) live in
> `eventsource.adapters.sql`, and the in-memory ones
> (`InMemoryCheckpointRepository`, `InMemoryDLQRepository`) in
> `eventsource.adapters.memory`. All of the above are re-exported from the
> top-level `eventsource` package. The checkpoint and DLQ functions that
> consume them (`record_checkpoint`, `read_checkpoint`, `lag_metrics_dict`,
> `reset_checkpoint`, `send_to_dlq`, `read_failed_events`) are documented on
> [Projections](projections.md).
>
> **The `eventsource.repositories` package is gone.** [ADR
> 0026](../adrs/0026-outbox-ring-migration.md) completed the same split for
> the outbox — the third and last module that used to mix a Protocol
> definition with sqlalchemy-backed implementations in one file. The
> contract now lives in `eventsource.ports.outbox`, a Tier 0 module with no
> sqlalchemy or driver dependency; the three implementations live in
> `eventsource.adapters.memory`, `eventsource.adapters.postgresql`, and
> `eventsource.adapters.sqlite`. `import eventsource.repositories` raises
> `ModuleNotFoundError`. This page is the reference for the port and its
> adapters; it keeps its original filename (`repositories.md`) rather than
> being renamed to `outbox.md`, since renaming would mean rewriting inbound
> links from several other pages this same sweep touches, for no reader-facing
> benefit — the page's content, not its filename, is what changed.

The outbox contract is one Protocol, two value objects, and one helper
function, all in `eventsource.ports.outbox`:

| Name | Kind | Purpose |
| --- | --- | --- |
| `OutboxRepository` | `@runtime_checkable` Protocol | The seven-method contract every backend implements |
| `OutboxEntry` | dataclass | One row of the outbox |
| `OutboxStats` | frozen dataclass | Aggregate counts over the outbox table |
| `outbox_event_data(event)` | function | The single authority for the JSON-safe payload dict stored in `event_outbox.event_data` |

The three backend implementations live one per technology, per [ADR
0026](../adrs/0026-outbox-ring-migration.md)'s per-backend-not-dialect-
parameterized rule (SQLite takes a raw `aiosqlite.Connection`, not a
sqlalchemy engine or session, so it cannot share code with the PostgreSQL
implementation the way checkpoints and DLQ do):

| Backend | Module |
| --- | --- |
| `PostgreSQLOutboxRepository` | `eventsource.adapters.postgresql` |
| `SQLiteOutboxRepository` | `eventsource.adapters.sqlite` |
| `InMemoryOutboxRepository` | `eventsource.adapters.memory` |

## Overview

### What this module provides

The transactional outbox implements the outbox pattern: events are
persisted in the same transaction as the aggregate change, then published
asynchronously by a worker. This gives at-least-once delivery even when
the event bus is temporarily unavailable, and keeps publishing off the
request path. An `OutboxEntry` carries the event ID, type, aggregate ID and
type, optional tenant ID, serialized payload, a status of `pending`,
`published`, or `failed`, publish timestamp, retry count, and last error.
`get_stats()` -> `OutboxStats` summarizes the backlog.

The `OutboxRepository` contract is a `@runtime_checkable` Protocol, so
`isinstance()` checks against it work and any object with matching methods
satisfies it — you are not required to subclass to supply your own backend.

None of the three outbox backends are constructed for you as part of a
store or bus. You instantiate the one you want and pass it to the
component that uses it.

### `outbox_event_data()`: the single payload authority

Every adapter's `add_event()` builds the JSON-safe `event_data` dict by
calling `outbox_event_data(event)` rather than constructing it inline.
That is also what `PostgreSQLEventStore._write_to_outbox` calls for its
same-transaction outbox write. One function, four call sites, one shape:

```python
def outbox_event_data(event: DomainEvent) -> dict[str, Any]:
    return {
        "event_id": str(event.event_id),
        "aggregate_id": str(event.aggregate_id),
        "aggregate_type": event.aggregate_type,
        "tenant_id": str(event.tenant_id) if event.tenant_id else None,
        "occurred_at": event.occurred_at.isoformat(),
        "payload": event.model_dump(mode="json"),
    }
```

The result contains only `str`, `None`, and JSON-native values, so it
serializes with plain `json.dumps` and needs no custom encoder.

### Import surface

```python
from eventsource.ports.outbox import OutboxRepository, OutboxEntry, OutboxStats, outbox_event_data
from eventsource.adapters.postgresql import PostgreSQLOutboxRepository
from eventsource.adapters.sqlite import SQLiteOutboxRepository
from eventsource.adapters.memory import InMemoryOutboxRepository
```

Most of these names are additionally re-exported from `eventsource` itself:
`OutboxRepository`, `OutboxEntry`, `OutboxStats`, `outbox_event_data`,
`PostgreSQLOutboxRepository`, and `InMemoryOutboxRepository`.
`SQLiteOutboxRepository` is exported too, but only conditionally —
`eventsource/__init__.py` imports it inside a `try/except ImportError`
alongside `SQLiteEventStore` and adds it to `__all__` only when
`SQLITE_AVAILABLE` is `True` (that is, when `aiosqlite` is installed).

### Method naming convention (`get_` / `add_` / `mark_` / `increment_` / `cleanup_`)

| Prefix | Meaning | Examples in this package |
| --- | --- | --- |
| `get_{entity}` | Fetch a single entity, or a filtered list | `get_stats()`, `get_pending_events(limit=100)` |
| `add_{entity}` | Create a new entity | `add_event()` |
| `mark_{state}` | State transition | `mark_published()`, `mark_failed()` |

All repository methods are `async`; every name below is awaited.

Two "delete-ish" or domain-verb methods do not fit the `get_`/`add_`
pattern: `increment_retry()` and `cleanup_published()`. `cleanup_published`
returns an `int` count of rows removed rather than `None`.

`list_pending_events()`, a second name that used to delegate to
`get_pending_events()`, is gone — [ADR 0026](../adrs/0026-outbox-ring-migration.md)
retired it along with the `OutboxRepositoryProtocol` alias: one name per
thing, no compatibility pair.

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
from eventsource.ports.outbox import OutboxRepository
from eventsource.adapters.memory import InMemoryOutboxRepository
from eventsource.adapters.postgresql import PostgreSQLOutboxRepository

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

Every statement runs inside `sql_connection(conn, *, write=...)` from
`eventsource.adapters._sql.connection` — the same connection-normalization
helper `SQLCheckpointRepository` and `SQLDLQRepository` use. That helper is
what makes the union type work, and its two branches differ in who owns
the transaction:

| You pass | Write methods (`write=True`) | Read methods (`write=False`) | Who commits |
| --- | --- | --- | --- |
| `AsyncEngine` | `async with engine.begin()` | `async with engine.connect()` | The repository, per call |
| `AsyncConnection` | connection yielded as-is | connection yielded as-is | **The caller** |

With an `AsyncEngine`, each method is self-contained: it checks out a
connection, runs its statement, commits (for writes), and returns the connection
to the pool. Two calls are two transactions.

With an `AsyncConnection`, the `write` argument has no effect — the
helper yields the connection directly and the caller is responsible for
transaction management. The repository issues no `COMMIT` and
no `ROLLBACK`. Build one on a bare `engine.connect()` connection and never
commit, and the writes are discarded.

#### Which mode to use

Pass a **connection** when the repository write must be atomic with other work
in the same transaction. That is the whole point of the outbox pattern: staging
the outbox rows and appending the events either both happen or neither does.

```python
async with engine.begin() as conn:
    await store.append(stream, events, expected=ExpectedVersion.exact(current_version))
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

Templates live under `src/eventsource/adapters/sql/schemas/templates/`; apply them with
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
from eventsource.adapters.sqlite import SQLiteOutboxRepository

async with aiosqlite.connect("events.db") as db:
    repo = SQLiteOutboxRepository(db)
    await repo.add_event(event)
```

#### Availability

`aiosqlite` is an optional dependency (`uv sync --extra sqlite`, or install
`aiosqlite` directly). Importing `SQLiteOutboxRepository` from
`eventsource.adapters.sqlite` always succeeds regardless — the module
imports `aiosqlite` only under `if TYPE_CHECKING:`. Only
`aiosqlite.connect()` in your own code needs the package present.

The top-level namespace behaves differently: `eventsource/__init__.py` imports
`SQLiteOutboxRepository` inside a `try/except ImportError` next to
`SQLiteEventStore` and appends it to `__all__` only when `SQLITE_AVAILABLE`
is `True`. `from eventsource import SQLiteOutboxRepository` therefore fails on a
machine without `aiosqlite`, while
`from eventsource.adapters.sqlite import SQLiteOutboxRepository` succeeds. Prefer
the latter when you need to import unconditionally.

#### Transactions: the repository commits, you do not

This is the sharpest difference from the PostgreSQL backend. There is no
engine-versus-connection distinction and no connection-normalizing helper:
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

Templates are under `src/eventsource/adapters/sql/schemas/templates/sqlite/`, alongside
`events.sql` and `snapshots.sql`.

#### What differs from PostgreSQL

At the Protocol level this backend is interchangeable with the PostgreSQL
one — the SQLite class passes the same `isinstance()` check against
`OutboxRepository`. Below the interface, storage and query shape differ:

- UUIDs are stored as 36-character hyphenated TEXT and timestamps as ISO 8601
  TEXT, parsed back on read.
- Statements use `?` positional parameters, not the named binds of the
  PostgreSQL dialect.

> **Known bug (backlogged, P1).** `SQLiteOutboxRepository.add_event()`
> generates a `uuid4()` and inserts `str(outbox_id)` into `event_outbox.id`,
> but `migrations/templates/sqlite/outbox.sql` declares that column as
> `id INTEGER PRIMARY KEY AUTOINCREMENT`, which rejects a non-integer value.
> [ADR 0026](../adrs/0026-outbox-ring-migration.md)'s conformance suite
> surfaced this against the real schema; the real-schema conformance
> binding runs `xfail(strict=False)` until the fix lands. See `BACKLOG.md`.

### InMemory (testing) — no connection, adds `clear()`

`InMemoryOutboxRepository` takes no connection at all:

```python
from eventsource.adapters.memory import InMemoryOutboxRepository

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

Its `event_data` field is serialized with stdlib `json.dumps` rather than
the orjson-backed helper used elsewhere in the library — [ADR
0026](../adrs/0026-outbox-ring-migration.md) made this swap to keep
`eventsource.adapters.memory` free of a non-stdlib import. The only
observable effect is JSON spacing (`", "` instead of `","`); anything that
parses the field back into a dict sees no difference.

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
repository's own `_enable_tracing` reflects the tracer you supplied). Per [ADR
0026](../adrs/0026-outbox-ring-migration.md), all three outbox adapters keep
their per-operation tracing spans — this is one of the rings ADR 0025 did
not amend ADR 0016 for.
