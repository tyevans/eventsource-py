# Repositories API Reference

Technical reference for the `eventsource.repositories` package: the three
infrastructure repositories that sit beside the event store — projection
checkpoint tracking, the dead letter queue, and the transactional outbox — plus
their PostgreSQL, SQLite, and in-memory backends.

The package is organized into three public source modules, one per repository
family, with two private helpers:

| Module | Contains |
| --- | --- |
| `eventsource.repositories.checkpoint` | `CheckpointRepository`, `CheckpointRepositoryProtocol`, `PostgreSQLCheckpointRepository`, `SQLiteCheckpointRepository`, `InMemoryCheckpointRepository`, `CheckpointData`, `LagMetrics` |
| `eventsource.repositories.dlq` | `DLQRepository`, `DLQRepositoryProtocol`, `PostgreSQLDLQRepository`, `SQLiteDLQRepository`, `InMemoryDLQRepository`, `DLQEntry`, `DLQStats`, `ProjectionFailureCount` |
| `eventsource.repositories.outbox` | `OutboxRepository`, `OutboxRepositoryProtocol`, `PostgreSQLOutboxRepository`, `SQLiteOutboxRepository`, `InMemoryOutboxRepository`, `OutboxEntry`, `OutboxStats` |
| `eventsource.repositories._connection` | Private connection-handling helper shared by the SQL backends |
| `eventsource.repositories._json` | Deprecated JSON shim; import from `eventsource.serialization` instead |

Every name above from the three public modules is re-exported from the
`eventsource.repositories` barrel, along with three JSON utilities forwarded from
`eventsource.serialization` (`EventSourceJSONEncoder`, `json_dumps`,
`json_loads`). Nothing in this package is re-exported from the top-level
`eventsource` namespace, so import from `eventsource.repositories` directly.

Each family follows the same shape: a Protocol defining the contract, a
PostgreSQL implementation for production, a SQLite implementation for
lightweight deployments, and an in-memory implementation for tests. Code that
depends only on the Protocol can swap backends without changes; the places where
the SQLite backend behaves differently from PostgreSQL are called out in
[SQLite backend deviations](#sqlite-backend-deviations).

## Overview

### What this module provides

`eventsource.repositories` covers three pieces of supporting infrastructure that
event-sourced systems need alongside the event store itself. Each is an
independent family — you can use one without the others.

**Checkpoint tracking** (`checkpoint.py`) records how far each projection has
progressed through the event stream. A checkpoint row carries the projection
name, the last processed event ID and type, the timestamp of that processing,
a running `events_processed` count, and an optional `global_position`
(`CheckpointData`). This is what lets a projection resume after a restart
instead of replaying from zero, and what backs lag reporting via
`get_lag_metrics()` -> `LagMetrics`. The same repository doubles as position
storage for subscriptions through `get_position()` / `save_position()`, keyed by
subscription ID rather than projection name. `reset_checkpoint()` clears a
projection's position so it can be rebuilt.

**Dead letter queue** (`dlq.py`) stores events that failed processing after
retries were exhausted, so a failure is investigable rather than silently lost.
A `DLQEntry` keeps the original event ID, the projection that failed, the event
type and serialized payload, the error message and full stack trace, a retry
count, first/last failure timestamps, and a status of `failed`, `retrying`, or
`resolved` plus resolution metadata. Aggregate views are available without
scanning entries: `get_failure_stats()` -> `DLQStats` and
`get_projection_failure_counts()` -> `list[ProjectionFailureCount]`.

**Transactional outbox** (`outbox.py`) implements the outbox pattern: events are
persisted in the same transaction as the aggregate change, then published
asynchronously by a worker. This gives at-least-once delivery even when the
event bus is temporarily unavailable, and keeps publishing off the request path.
An `OutboxEntry` carries the event ID, type, aggregate ID and type, optional
tenant ID, serialized payload, a status of `pending`, `published`, or `failed`,
publish timestamp, retry count, and last error. `get_stats()` -> `OutboxStats`
summarizes the backlog.

All three contracts are `@runtime_checkable` Protocols, so `isinstance()` checks
against them work and any object with matching methods satisfies them — you are
not required to subclass to supply your own backend.

None of the three are constructed for you as part of a store or bus. You
instantiate the backend you want and pass it to the component that uses it —
`SubscriptionLifecycleManager` takes a required `checkpoint_repo` argument, for
example. The one
convenience default is `DLQManager` in `eventsource.projections.dlq_manager`,
whose `dlq_repo` parameter falls back to `InMemoryDLQRepository()` when omitted;
that default is suitable for tests, not for production.

### Import surface: `from eventsource.repositories import ...`

Every public name in the package is re-exported from the package root, so a
single import site covers all three families:

```python
from eventsource.repositories import (
    CheckpointRepository,
    PostgreSQLCheckpointRepository,
    DLQRepository,
    InMemoryDLQRepository,
    OutboxRepository,
    SQLiteOutboxRepository,
)
```

`__all__` in `eventsource/repositories/__init__.py` lists **25 names**, grouped
as the module's own comments group them:

| Group | Count | Names |
| --- | --- | --- |
| Checkpoint | 7 | `CheckpointRepository`, `CheckpointRepositoryProtocol`, `PostgreSQLCheckpointRepository`, `SQLiteCheckpointRepository`, `InMemoryCheckpointRepository`, `CheckpointData`, `LagMetrics` |
| DLQ | 8 | `DLQRepository`, `DLQRepositoryProtocol`, `PostgreSQLDLQRepository`, `SQLiteDLQRepository`, `InMemoryDLQRepository`, `DLQEntry`, `DLQStats`, `ProjectionFailureCount` |
| Outbox | 7 | `OutboxRepository`, `OutboxRepositoryProtocol`, `PostgreSQLOutboxRepository`, `SQLiteOutboxRepository`, `InMemoryOutboxRepository`, `OutboxEntry`, `OutboxStats` |
| JSON utilities | 3 | `EventSourceJSONEncoder`, `json_dumps`, `json_loads` |

The pattern per family is identical: a Protocol, a `*Protocol` alias, three
backends, and the family's data types. The DLQ contributes one extra name
because it has a third data type, `ProjectionFailureCount`. The three JSON
utilities are not defined here — they are forwarded from
`eventsource.serialization`; see
[Re-exported JSON utilities](#re-exported-json-utilities-eventsourcejsonencoder-json_dumps-json_loads).
See the [full export table](#full-export-table-name---kind---module) for kinds
and defining modules.

#### The `*Protocol` aliases

Each `*Protocol` name is the same object as the name it aliases, assigned at the
bottom of its module:

```python
CheckpointRepositoryProtocol = CheckpointRepository  # checkpoint.py:1102
DLQRepositoryProtocol = DLQRepository                # dlq.py:1401
OutboxRepositoryProtocol = OutboxRepository          # outbox.py:1080
```

They exist for readers who prefer the explicit `...Protocol` suffix in
annotations. `CheckpointRepositoryProtocol is CheckpointRepository` is `True`, so
the two are fully interchangeable in annotations and `isinstance()` checks.

#### Also available from the top-level namespace

Most of these names are additionally re-exported from `eventsource` itself:
the three Protocols, the three `PostgreSQL*` backends, the three `InMemory*`
backends, and all eight data types. The three `SQLite*` backends are exported
too, but only conditionally — `eventsource/__init__.py` imports them inside a
`try/except ImportError` alongside `SQLiteEventStore` and adds them to `__all__`
only when `SQLITE_AVAILABLE` is `True` (that is, when `aiosqlite` is installed).
The three `*Protocol` aliases and the JSON utilities are **not** re-exported at
the top level.

Importing from `eventsource.repositories` is the recommended form for this
package: it works uniformly for every name, including the aliases, and does not
depend on whether the optional SQLite extra is installed.

### Method naming convention (`get_` / `list_` / `add_` / `update_` / `delete_` / `save_`)

The package docstring in `eventsource/repositories/__init__.py` declares the
prefix convention that repository methods follow:

| Prefix | Meaning | Examples in this package |
| --- | --- | --- |
| `get_{entity}` | Fetch a single entity by ID | `get_checkpoint()`, `get_failed_event()`, `get_position()` |
| `list_{entities}` | Fetch multiple entities, with filtering | `list_failed_events()`, `list_pending_events()` |
| `add_{entity}` | Create a new entity | `add_failed_event()`, `add_event()` |
| `update_{entity}` | Update an existing entity | `update_checkpoint()` |
| `delete_{entity}` | Delete an entity | `delete_resolved_events()` |
| `save_{entity}` | Upsert — create or update | `save_position()` |

All repository methods are `async`; every name below is awaited.

#### Compatibility pairs

Three methods exist under two names each. The `list_*` / short form was added
later for consistency with the table above, and is implemented as a one-line
delegation to the original — not as a separate query path. Both names are
declared on the Protocol and defined on all three backends of the family, so
either is safe to call and neither changes behavior.

| Preferred | Original | Delegation site |
| --- | --- | --- |
| `list_failed_events(projection_name=None, status="failed", limit=100)` | `get_failed_events(...)` | `dlq.py:628` (PostgreSQL), also on SQLite and InMemory |
| `get_failed_event(dlq_id)` | `get_failed_event_by_id(dlq_id)` | `dlq.py:637` (PostgreSQL), also on SQLite and InMemory |
| `list_pending_events(limit=100)` | `get_pending_events(limit=100)` | `outbox.py:492` (PostgreSQL), also on SQLite and InMemory |

Each alias carries the docstring "Alias for `…()` - preferred for naming
consistency". Use the preferred column in new code; the originals are not
deprecated and emit no warning.

Note the asymmetry: for the DLQ, the preferred single-entity name is the
*shorter* `get_failed_event()`, because `get_{entity}` already implies "by ID" —
whereas for collections the preferred name is the *longer* `list_*` form.

#### Where the convention does not apply

The convention describes the CRUD-shaped methods only. Several methods use
domain verbs instead, and are named for the state transition or computation they
perform rather than for a storage operation:

- Checkpoint: `reset_checkpoint()`, `get_lag_metrics()`, `get_all_checkpoints()`
- DLQ: `mark_resolved()`, `mark_retrying()`, `get_failure_stats()`,
  `get_projection_failure_counts()`
- Outbox: `mark_published()`, `mark_failed()`, `increment_retry()`,
  `cleanup_published()`, `get_stats()`

Two prefixes are also looser than the table suggests:

- `update_checkpoint(projection_name, event_id, event_type)` is an **upsert**,
  not a strict update — the PostgreSQL implementation issues
  `INSERT … ON CONFLICT (projection_name) DO UPDATE` and increments
  `events_processed`, so calling it for a projection with no existing row
  creates one. By the table it would be a `save_`; the `update_` name predates
  the convention.
- `get_lag_metrics()`, `get_failure_stats()`, and `get_stats()` return computed
  aggregates (`LagMetrics`, `DLQStats`, `OutboxStats`), not stored entities
  fetched by ID.

The two "delete-ish" methods return an `int` count of rows removed rather than
`None`: `delete_resolved_events(older_than_days=30)` and
`cleanup_published(days=7)`. The in-memory backends additionally expose
`clear()`, which is a test helper and not part of any Protocol.

## Selecting a backend

Each of the three families ships the same three backends. They differ only in
what you hand the constructor and where the data lives; the method surface is
the Protocol's in every case (plus `clear()` on the in-memory ones).

| Backend | Constructor argument | Storage | Intended use |
| --- | --- | --- | --- |
| `PostgreSQL*Repository` | `conn: AsyncConnection \| AsyncEngine` | `projection_checkpoints` / `dead_letter_queue` / `event_outbox` tables | Production |
| `SQLite*Repository` | `connection: aiosqlite.Connection` | Same table names, SQLite schema | Single-node, embedded, local dev |
| `InMemory*Repository` | *(none)* | Process memory | Tests |

Because the contracts are `@runtime_checkable` Protocols, annotate call sites
with the Protocol and choose the backend at composition time:

```python
from eventsource.repositories import (
    CheckpointRepository,
    InMemoryCheckpointRepository,
    PostgreSQLCheckpointRepository,
)

def build_checkpoints(engine=None) -> CheckpointRepository:
    if engine is None:
        return InMemoryCheckpointRepository()
    return PostgreSQLCheckpointRepository(engine)
```

### PostgreSQL (production) — `AsyncConnection | AsyncEngine`

`PostgreSQLCheckpointRepository`, `PostgreSQLDLQRepository`, and
`PostgreSQLOutboxRepository` share an identical constructor signature:

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

Every statement in these three backends runs inside the shared helper
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

Pass an **engine** for work that stands on its own — a projection runner
updating checkpoints, an outbox publisher marking rows published, a DLQ writer
recording a failure that must survive the rollback of the work that caused it:

```python
from sqlalchemy.ext.asyncio import create_async_engine
from eventsource.repositories import PostgreSQLCheckpointRepository

engine = create_async_engine("postgresql+asyncpg://localhost/app")
checkpoints = PostgreSQLCheckpointRepository(engine)

await checkpoints.update_checkpoint("OrderProjection", event.event_id, event.event_type)
```

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
the connection is open. Repositories built on an engine have no such
restriction and can be constructed once at startup and kept for the process
lifetime.

The class docstrings for all three PostgreSQL repositories show the connection
form. Both forms are supported; the docstring example is not a restriction.

#### PostgreSQL-specific SQL

These backends emit PostgreSQL-dialect SQL through `sqlalchemy.text()` with
named bind parameters, and are not portable to another server:

- Upserts use `INSERT … ON CONFLICT (…) DO UPDATE` — on `projection_name` for
  checkpoints (`checkpoint.py:280`, `checkpoint.py:457`) and on the
  `(event_id, projection_name)` pair for the DLQ (`dlq.py:335`). This is why
  `update_checkpoint()` creates a row when none exists, and why re-recording the
  same failure for the same projection updates the existing entry instead of
  inserting a duplicate.
- Aggregate queries use `FILTER (WHERE …)` clauses and array operators that the
  SQLite backend has to work around; see
  [SQLite backend deviations](#sqlite-backend-deviations).
- `dlq_id` is a real `UUID` here, not the `int | str` the SQLite backend uses.

Any asyncpg-compatible URL works (`postgresql+asyncpg://…`). The library never
creates the engine for you.

#### Schema prerequisite

None of the three repositories create their table. The backing table must exist
before the first call, or the statement fails with a database error:

| Repository | Table | DDL template |
| --- | --- | --- |
| `PostgreSQLCheckpointRepository` | `projection_checkpoints` | `migrations/templates/checkpoints.sql` |
| `PostgreSQLDLQRepository` | `dead_letter_queue` | `migrations/templates/dlq.sql` |
| `PostgreSQLOutboxRepository` | `event_outbox` | `migrations/templates/outbox.sql` |

Templates live under `src/eventsource/migrations/templates/`; apply them with
your own migration tooling, or use the Alembic scaffold provided alongside them
in `migrations/templates/alembic/`.

### SQLite (lightweight) — `aiosqlite.Connection`

`SQLiteCheckpointRepository`, `SQLiteDLQRepository`, and `SQLiteOutboxRepository`
share a constructor shaped like the PostgreSQL one, but the connection argument
is named `connection` and must be an open `aiosqlite.Connection`:

```python
def __init__(
    self,
    connection: "aiosqlite.Connection",
    tracer: Tracer | None = None,
    enable_tracing: bool = True,
) -> None: ...
```

The connection is stored on the private attribute `self._connection` — there is
no public `conn` attribute as on the PostgreSQL backends. `SQLiteDLQRepository`
annotates the parameter as `Any` rather than `aiosqlite.Connection`; the
requirement is the same, only the annotation is looser.

A SQLAlchemy engine or a stdlib `sqlite3.Connection` will not work: the code
calls `await connection.execute(...)`, `await cursor.fetchone()` /
`fetchall()`, and `await connection.commit()`, which are the aiosqlite coroutine
forms.

```python
import aiosqlite
from eventsource.repositories import SQLiteCheckpointRepository

async with aiosqlite.connect("events.db") as db:
    repo = SQLiteCheckpointRepository(db)
    await repo.update_checkpoint("MyProjection", event.event_id, "MyEvent")
```

#### Availability

`aiosqlite` is an optional dependency (`uv sync --extra sqlite`, or install
`aiosqlite` directly). Importing the `SQLite*` names from
`eventsource.repositories` always succeeds regardless — `checkpoint.py` and
`outbox.py` import `aiosqlite` only under `if TYPE_CHECKING:`, and `dlq.py`
does not import it at all. Only `aiosqlite.connect()` in your own code needs
the package present.

The top-level namespace behaves differently: `eventsource/__init__.py` imports
the three `SQLite*` repositories inside a `try/except ImportError` next to
`SQLiteEventStore` and appends them to `__all__` only when `SQLITE_AVAILABLE`
is `True`. `from eventsource import SQLiteOutboxRepository` therefore fails on a
machine without `aiosqlite`, while
`from eventsource.repositories import SQLiteOutboxRepository` succeeds. Prefer
the latter.

#### Transactions: the repository commits, you do not

This is the sharpest difference from the PostgreSQL backends. There is no
engine-versus-connection distinction and no `execute_with_connection()` helper:
each SQLite repository holds the one connection you gave it for its lifetime and
calls `await self._connection.commit()` itself at the end of every write.

| Repository | Methods that commit |
| --- | --- |
| `SQLiteCheckpointRepository` | `update_checkpoint()`, `reset_checkpoint()`, `save_position()` |
| `SQLiteDLQRepository` | `add_failed_event()`, `mark_resolved()`, `mark_retrying()`, `delete_resolved_events()` |
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

Sharing one `aiosqlite.Connection` across several repositories is fine and is
the normal arrangement (SQLite has a single writer anyway), as long as you
accept that any write through any of them commits the shared connection.

#### Schema prerequisite

As with PostgreSQL, no table is created for you. The SQLite DDL lives in a
separate directory from the PostgreSQL templates:

| Repository | Table | DDL template |
| --- | --- | --- |
| `SQLiteCheckpointRepository` | `projection_checkpoints` | `migrations/templates/sqlite/checkpoints.sql` |
| `SQLiteDLQRepository` | `dead_letter_queue` | `migrations/templates/sqlite/dlq.sql` |
| `SQLiteOutboxRepository` | `event_outbox` | `migrations/templates/sqlite/outbox.sql` |

Templates are under `src/eventsource/migrations/templates/sqlite/`, alongside
`events.sql` and `snapshots.sql`. `SQLiteCheckpointRepository.get_lag_metrics()`
additionally reads the `events` table, so lag reporting requires the event store
schema on the same connection, not just `checkpoints.sql`.

`update_checkpoint()` and `save_position()` use `INSERT … ON CONFLICT
(projection_name) DO UPDATE`, which needs SQLite 3.24 or newer (2018). The DLQ's
`add_failed_event()` upsert targets the `(event_id, projection_name)` pair, so
the unique index in `sqlite/dlq.sql` must be present or the conflict clause has
nothing to match.

#### What differs from PostgreSQL

At the Protocol level these backends are interchangeable with the PostgreSQL
ones — the SQLite classes pass the same `isinstance()` checks against
`CheckpointRepository`, `DLQRepository`, and `OutboxRepository`
(`tests/repositories/test_sqlite_repos.py`). Below the interface, storage and
query shape differ:

- UUIDs are stored as 36-character hyphenated TEXT and timestamps as ISO 8601
  TEXT, parsed back on read.
- Statements use `?` positional parameters, not the named binds of the
  PostgreSQL dialect.
- Aggregates use `SUM(CASE WHEN … THEN 1 ELSE 0 END)` in place of
  `COUNT(*) FILTER (WHERE …)`, and `get_lag_metrics()` uses an `IN (…)` list
  instead of an array operator.
- `dlq_id` is an autoincrementing integer here rather than a UUID, which is why
  the DLQ methods are typed `dlq_id: int | str` across all backends.

Full detail, including the `LagMetrics` differences, is in
[SQLite backend deviations](#sqlite-backend-deviations).

> **Discrepancy worth checking.** `SQLiteOutboxRepository.add_event()` generates
> a `uuid4()` and inserts `str(outbox_id)` into `event_outbox.id`, but
> `migrations/templates/sqlite/outbox.sql` declares that column as
> `id INTEGER PRIMARY KEY AUTOINCREMENT`, which rejects a non-integer value.
> The test fixture in `tests/conftest.py` creates the table as
> `id TEXT PRIMARY KEY` instead, so the test suite does not exercise the shipped
> template. Anyone applying `sqlite/outbox.sql` verbatim should expect a
> datatype-mismatch error on the first `add_event()`. (The DLQ template's
> `id INTEGER PRIMARY KEY AUTOINCREMENT` is consistent with its repository,
> which lets SQLite assign the ID.)

### InMemory (testing) — no connection, adds `clear()`

The in-memory backends take no connection at all:

```python
from eventsource.repositories import InMemoryDLQRepository

repo = InMemoryDLQRepository()
```

State is plain dicts guarded by an `asyncio.Lock`, so concurrent tasks in one
event loop are safe; nothing is shared across processes and everything is lost
when the process exits. Each adds one method beyond its Protocol:

```python
await repo.clear()  # async, despite being a test helper
```

`clear()` drops all stored state — and for `InMemoryDLQRepository`, also resets
the integer ID counter it uses to mint `dlq_id` values. It is deliberately not
on the Protocol, so code typed against `DLQRepository` cannot call it; reach for
it in fixture teardown where you hold the concrete type.

`InMemoryDLQRepository()` is also the fallback that `DLQManager` constructs when
its `dlq_repo` argument is omitted, which is convenient in tests and wrong in
production.

### Common constructor keywords

Every one of the nine repositories accepts the same two optional keywords after
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
