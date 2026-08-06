# Building Projections

In this tutorial you will build the query side of an event-sourced system: a
*projection* that consumes `Order` events and maintains a denormalized view you can
query directly.

An aggregate answers "what is allowed to happen next?"; it is not built for queries. A
projection answers the other half: it subscribes to the events an aggregate produced and
folds them into a shape that reads well -- one row per order, a customer's order history,
a running total. In `eventsource` this starts with a `Projection` subclass whose handler
methods are wired to event types by the `@handles` decorator, and ends with a
`ReadModelProjection[TModel]` that writes typed `ReadModel` rows through a
`ReadModelRepository`.

You will start close to the metal with a `DatabaseProjection` that writes rows with raw
SQL, so you can see exactly what a projection does; then watch it keep its place in the
stream with checkpoints, survive a failing event via the dead letter queue, and be reset
and rebuilt from scratch. From there you will define an `OrderSummary` read model,
create its table with `generate_full_schema`, and rewrite the same projection as a
`ReadModelProjection[OrderSummary]` -- shorter, typed, and backend-agnostic. Finally you
will query the view with `Query`, `Filter`, and ordering, handle concurrent updates with
`save_with_version_check` and `OptimisticLockError`, and swap
`InMemoryReadModelRepository` for the PostgreSQL and SQLite repositories without
touching the projection code.

By the end you will have a projection that is idempotent, resumable, observable, and
testable in memory -- the four properties that make read models safe to run in
production.

## What you'll build

You will build one thing twice: an order-summary view fed by `OrderCreated`,
`OrderShipped`, and `OrderCancelled` events.

The first version is a `DatabaseProjection`. Its handlers receive a live database
connection and write rows with `sqlalchemy.text()` and bound parameters, inside a
transaction the framework opens and commits around each event. This version exists so
that nothing is hidden: you will see the checkpoint written only after the transaction
commits, watch a deliberately poisoned event exhaust its retries and land in the dead
letter queue, and call `reset()` to clear both the checkpoint and the projected rows
before replaying the stream.

The second version is the same view rebuilt as a `ReadModelProjection[OrderSummary]`.
`OrderSummary` is a `ReadModel` -- a Pydantic model that already carries `id`, `version`,
and timestamp fields -- and `generate_full_schema(OrderSummary, ...)` emits its
`CREATE TABLE` and index DDL for you. Handlers now take
`(self, repo: ReadModelRepository[OrderSummary], event)` and speak in models rather than
SQL: `await repo.get(...)`, `await repo.save(...)`,
`await repo.save_with_version_check(...)`. No SQL appears in the projection at all.

With the view populated, you will query it:

```python
recent_shipped = await repo.find(
    Query(
        filters=[Filter.eq("status", "shipped")],
        order_by="created_at",
        order_direction="desc",
        limit=10,
    )
)
```

Then you will make two writers race the same summary row and catch the
`OptimisticLockError` that `save_with_version_check` raises when the stored `version` has
already moved on -- the read-model counterpart to an aggregate's optimistic locking.

The last step costs almost nothing, which is the point: the projection is written
against the `ReadModelRepository` protocol, so the same class runs on
`InMemoryReadModelRepository` in your unit tests, `SQLiteReadModelRepository` on a
laptop, and `PostgreSQLReadModelRepository` in production. Only the repository you
construct changes.

The early steps run in plain Python against in-memory and SQLite backends -- no services
required. The PostgreSQL wiring in the final step is worth running only if you have
Docker available.

## Prerequisites

Before you start you need:

- **Python 3.13 or newer** -- the package declares `requires-python = ">=3.13"`.
- **`eventsource-py` installed, with the SQLite extra.** From a clone of the repository,
  `uv sync --all-extras`; otherwise `pip install "eventsource-py[sqlite]"`. The
  projection classes themselves come from the core install (pydantic + sqlalchemy), but
  every step that actually writes rows uses a `sqlite+aiosqlite://` engine, and that
  driver ships in the `sqlite` extra. Step 12 also mentions
  `PostgreSQLReadModelRepository`, which needs the `postgresql` extra (`asyncpg`) if you
  want to run it.
- **[Your First Event](02-first-event.md) and [Your First Aggregate](03-first-aggregate.md),
  or their equivalent.** You should be able to declare a `DomainEvent` subclass, and know
  that `@handles(EventType)` maps an event type to a handler method -- projections reuse
  the same decorator `DeclarativeAggregate` uses.
- **Working knowledge of `async`/`await`.** Every handler you write is an `async def`,
  and the framework awaits it inside a transaction it opened for you. The snippets are
  driven from an `asyncio.run(main())` entry point.
- **Enough SQLAlchemy to read a `text()` query.** You will call
  `create_async_engine(...)` and wrap it in `async_sessionmaker(...)`, because both
  `DatabaseProjection` and `ReadModelProjection` take a `session_factory`. You do not
  need the ORM: Steps 2-5 execute raw statements with bound parameters against the
  `AsyncConnection` your handler receives, and from Step 8 on there is no SQL in the
  projection at all.

You do **not** need Docker for anything up to Step 11. SQLite gives you a real
transaction boundary in a local file (or `:memory:`), and checkpoints and the dead letter
queue work without extra setup: leave `checkpoint_repo` and `dlq_repo` unset and the
projection falls back to `InMemoryCheckpointRepository` and `InMemoryDLQRepository`.
Swap in `SQLiteCheckpointRepository` / `SQLiteDLQRepository` -- or the PostgreSQL pair --
when you want progress to survive a restart.

Docker only matters if you choose to run the PostgreSQL half of Step 12. The repository
ships a compose file for that:

```bash
docker compose -f docker-compose.test.yml up -d
```

Note that `InMemoryReadModelRepository`, used for the test-oriented variant in Step 12,
is constructed directly (`InMemoryReadModelRepository(OrderSummary)`) and exercised
against your handler logic -- `ReadModelProjection` itself always builds its repository
from the SQLAlchemy connection it is given.

Check your install before you begin:

```bash
python -c "import eventsource; print(eventsource.__version__)"
python -c "from eventsource import ReadModelProjection; from eventsource.adapters.sql.readmodel_schema import generate_full_schema; print('ok')"
```

If both print, you are ready.

## Step 1: Projections and read models -- why events need a second model

Before you write any code, it is worth being precise about what problem a projection
solves. Take two minutes on this step; everything after it is mechanical once the shape
is clear.

### The event store answers one question well

An event store is an append-only log. Look at what an event store actually offers and the
bias is obvious -- its primary read is `read_stream(stream, ...)`, which returns the
ordered stream for *one* aggregate, plus `read_all()` for walking the whole log and
`read_category()` for walking every stream of one type.

That is exactly what an aggregate needs. To decide whether an order may ship, you load
that order's events, fold them into current state, and check an invariant. One stream,
one decision.

Now try to answer a question a user would actually ask:

> Show me every order with status `shipped`, most recently updated first, 20 per page.

There is no aggregate id to load. Answering it from the log alone means reading every
event for every order, folding each stream, filtering the results, then sorting them --
work that grows with your entire history, on every page load. And you cannot index your
way out: the log stores *what happened*, not *what is currently true*.

The log is optimized for writing truth down. Queries need something else.

### The second model

So you keep two models of the same facts:

| | Write model | Read model |
| --- | --- | --- |
| Shape | append-only event streams, one per aggregate | tables shaped for the query you need |
| Source of truth | yes | no -- derived |
| Optimized for | correct decisions, one aggregate at a time | fast reads, across many aggregates |
| Rebuildable | no, it *is* the data | yes, from the events |
| Written by | aggregates, via `append()` | projections |

A **projection** is the thing in the middle: it consumes `DomainEvent` instances and
writes rows. A **read model** is what it writes -- an ordinary table you can `SELECT`
from, index, and paginate, with no event sourcing involved at query time. In the steps
that follow, `OrderProjection` is the projection; the orders table with `id`, `status`,
and `updated_at` is the read model.

This is the query half of CQRS: commands go to aggregates and produce events, queries go
to read models built from those events.

### Derived data is disposable data

The most useful consequence: because the read model is derived, it is *disposable*. Get
the shape wrong, need a new column, discover a bug in a handler -- you do not migrate the
read model, you throw it away and rebuild it from the log.

The framework makes that a first-class operation. Every projection has `reset()`, which
clears the checkpoint and then calls your `_truncate_read_models()`:

```python
await projection.reset()   # checkpoint cleared, your tables truncated
# ...then replay events through projection.handle() to rebuild
```

Nothing about your domain history is at risk in that operation, because none of it lives
in the read model. This is why projections can afford to be denormalized, duplicated, and
tailored: you can have three projections over the same events, each shaped for a
different screen, and each independently rebuildable.

### Reading is eventually consistent

The trade is consistency. An event is appended to the store first; a projection processes
it afterwards. Between those two moments, the read model is stale.

For most read models -- dashboards, lists, search results -- a short lag is fine. Where
it is not, the answer is not "make the projection synchronous"; it is to have the
decision read from the aggregate instead. Aggregates enforce invariants; read models
report.

Because the lag is real, the framework gives you a way to see it. Each projection tracks
its own position via a checkpoint:

```python
await projection.get_checkpoint()    # last processed event id, or None
await projection.get_lag_metrics()   # how far behind this projection is
```

The checkpoint is per-projection, which is what lets projections run independently: a
slow one falling behind does not hold up the others, and a rebuilt one can catch up on
its own.

### What you are actually writing

Given all that, the job of the class you are about to write is narrow:

1. Declare which event types it cares about.
2. For each one, apply the change to the read model.

Everything around that -- the transaction, commit and rollback, the checkpoint, retries,
the dead letter queue -- belongs to the framework. That is the division of labor to keep
in mind: **you write the "apply this event to a row" step, and nothing else.**

With that in place, start with the smallest possible version -- routing events to
handlers, no database at all.

## Step 2: Your first projection with `DeclarativeProjection` and `@handles`

Now write one. This first version has no database in it at all -- it keeps its "read
model" in a plain dictionary -- so that the only thing you are watching is routing: an
event goes into `handle()`, and the right method runs.

### The events

Create `order_events.py`:

```python
from eventsource import DomainEvent


class OrderCreated(DomainEvent):
    customer_id: str
    total: float


class OrderShipped(DomainEvent):
    carrier: str
```

Both are ordinary `DomainEvent` subclasses -- Pydantic models with the framework's
envelope fields already on them. The two you must supply when constructing one are
`aggregate_id` and `aggregate_type`; `event_id`, `event_type`, `timestamp`, and `version`
are filled in for you.

### The projection

Create `order_projection.py`:

```python
from eventsource import DeclarativeProjection, handles

from order_events import OrderCreated, OrderShipped


class OrderProjection(DeclarativeProjection):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.orders: dict[str, dict] = {}   # stand-in for a table

    @handles(OrderCreated)
    async def _handle_order_created(self, event: OrderCreated) -> None:
        self.orders[str(event.aggregate_id)] = {
            "status": "created",
            "total": event.total,
        }

    @handles(OrderShipped)
    async def _handle_order_shipped(self, event: OrderShipped) -> None:
        self.orders[str(event.aggregate_id)]["status"] = "shipped"

    async def _truncate_read_models(self) -> None:
        self.orders.clear()
```

Four things are worth naming here.

**`@handles(EventType)` is the whole registration mechanism.** The decorator attaches the
event type to the function; `DeclarativeProjection.__init__` then scans the instance for
decorated methods and builds a routing table from them. There is no separate list of
event types to keep in sync -- `subscribed_to()` is generated from what it found:

```python
>>> OrderProjection().subscribed_to()
[<class 'order_events.OrderCreated'>, <class 'order_events.OrderShipped'>]
```

**Handlers must be `async def`.** `DeclarativeProjection` builds its registry with
`require_async=True`, so a plain `def` handler fails at construction time, not at the
first event:

```
ValueError: Handler '_handle_order_created' in OrderProjection must be async.
```

**Handlers take one or two parameters after `self`.** This one takes one -- just the
event -- because it needs no database. The two-parameter form `(self, conn, event)` is
what you will switch to in Step 4, once there is a real connection to hand over. Anything
else is rejected at construction time with a `HandlerSignatureError` that spells out both
valid shapes:

```
HandlerSignatureError: Handler '_handle_order_created' in OrderProjection has invalid
signature for @handles(OrderCreated).
...
Got: 3 parameter(s) (excluding self)

Hint: Ensure your handler has exactly 1 or 2 parameters after 'self'.
```

(If you write a two-parameter handler on a plain `DeclarativeProjection`, it is called
with `None` as the context. That is legal but useless -- the connection only appears once
`DatabaseProjection` is supplying it.)

**Handler methods are private by convention.** The `_handle_` prefix is a naming habit,
not a rule; discovery looks for the decorator, not the name. Nothing calls these methods
directly -- you call `handle()`.

### Run it

```python
import asyncio
from uuid import uuid4

from order_events import OrderCreated, OrderShipped
from order_projection import OrderProjection


async def main() -> None:
    projection = OrderProjection()
    order_id = uuid4()

    await projection.handle(
        OrderCreated(
            aggregate_id=order_id,
            aggregate_type="Order",
            customer_id="cust-1",
            total=42.0,
        )
    )
    await projection.handle(
        OrderShipped(
            aggregate_id=order_id,
            aggregate_type="Order",
            carrier="dhl",
        )
    )

    print(projection.orders)
    print(await projection.get_checkpoint())


asyncio.run(main())
```

```
{'0d71c46d-9056-4792-8f6f-fd65acb4d5a7': {'status': 'shipped', 'total': 42.0}}
036ff817-6218-4768-afa3-498d32531bbe
```

Two events, two handlers, one row -- and a checkpoint holding the id of the last event
that was processed successfully. You did not construct a checkpoint repository: with
`checkpoint_repo` left unset, the projection falls back to
`InMemoryCheckpointRepository` (and `InMemoryDLQRepository` for the DLQ), which is why
this runs with no services and nothing to clean up.

Call `handle()`, never a handler method and never `_process_event()`. `handle()` is
where the retry loop, the checkpoint update, and -- from Step 4 on -- the transaction
live. Step 12 shows exactly what breaks when you go around it.

### Unhandled events

Send an event no handler is registered for and, by default, nothing happens:

```python
await projection.handle(SomeOtherEvent(aggregate_id=uuid4(), aggregate_type="Order"))
# ...no handler, no error, no checkpoint concerns
```

That default is deliberate. A projection subscribed to a live stream will see event types
it does not care about, and new ones will be added after it was written; silently
ignoring them keeps it forward-compatible. When you would rather be told, set the
class attribute:

```python
class OrderProjection(DeclarativeProjection):
    unregistered_event_handling = "warn"    # or "error", or the default "ignore"
```

`"warn"` logs a message naming the missing event type and the handlers that do exist.
`"error"` raises `UnhandledEventError`:

```
UnhandledEventError: No handler registered for event type 'OrderCancelled' in
OrderProjection. Available handlers: OrderCreated, OrderShipped. Add
@handles(OrderCancelled) decorator or set unregistered_event_handling='ignore' or 'warn'.
```

Be aware that in `"error"` mode the raise is a failure like any other, so it goes through
the retry machinery first: three attempts with backoff between them, then the DLQ, then
the exception reaches you. It is a useful setting for tests and strict internal
projections, and a slow one to hit repeatedly in production.

### Resetting

`_truncate_read_models()` is the one other method you implemented, and it is what makes
`reset()` mean something:

```python
await projection.reset()
print(projection.orders)                 # {}
print(await projection.get_checkpoint()) # None
```

`reset()` clears the checkpoint and then calls your `_truncate_read_models()`. It takes
no arguments on `DeclarativeProjection` -- clearing the dictionary is entirely your code.
The base implementation does nothing, so a projection that skips it will happily replay
events on top of stale rows. Implement it from the start.

### If your read model is a store rather than a table

The projection above owns a dictionary. Real ones usually own something with a port
behind it -- a repository, a graph store, a vector index. `StoreProjection[TStore]` is
`DeclarativeProjection` with that store held for you as `self._store`:

```python
from eventsource.application.projections import StoreProjection, handles


class OrderProjection(StoreProjection[OrderStore]):
    @handles(OrderCreated)
    async def _on_created(self, _context: object, event: OrderCreated) -> None:
        await self._store.upsert(event.order)


projection = OrderProjection(order_store)
```

The value is in what you do not write. If you later need a constructor parameter of your
own, you declare that one and forward the rest -- `def __init__(self, store, batch_size=100,
**options: Unpack[ProjectionOptions])` -- and `retry_policy`, `tracer`, and
`tenant_filter` still reach the base. Spelling the parent's parameters out by hand is how
projections have historically dropped them by accident. See the projections API reference
for the full constructor.

If the read model is a SQL table, keep reading: `DatabaseProjection` in Step 4 is the
case where the "store" is a session and a transaction.

You now have routing, checkpoints, and rebuild in about twenty lines. What is missing is
somewhere real to write. The next step defines that table.

## Step 3: Defining the read-model table

The dictionary in Step 2 was a placeholder for the thing you actually need: a table. This
step designs it. No projection code changes yet -- get the shape right first, because
every handler you write from Step 4 on is just "apply this event to a row in here".

### The table

The integration suite for `DatabaseProjection` uses a table called `test_orders`, and it
is a good template because it was written to exercise exactly the behaviors this tutorial
walks through. Here it is, as PostgreSQL DDL:

```sql
CREATE TABLE IF NOT EXISTS orders_read_model (
    id               UUID PRIMARY KEY,
    order_number     VARCHAR(50)  NOT NULL,
    amount           DECIMAL(10, 2) NOT NULL DEFAULT 0,
    status           VARCHAR(50)  NOT NULL DEFAULT 'created',
    tracking_number  VARCHAR(100),
    cancelled_reason TEXT,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
```

Three columns carry the weight, and they are the ones to understand before the rest.

**`id UUID PRIMARY KEY`** -- this is the event's `aggregate_id`, stored directly. One
order, one row. Making the aggregate id the primary key is not a convenience; it is what
lets a handler locate the row it must change from the event alone. `OrderShipped` carries
a tracking number and nothing else identifying, so `WHERE id = :id` bound to
`event.aggregate_id` is the only way it finds its target.

Store it as a real `UUID` column, not text. `event.aggregate_id` is a `uuid.UUID`, asyncpg
binds it natively, and the integration tests pass it straight through:

```python
{"id": event.aggregate_id}    # no str() needed against a UUID column
```

If you declare the column `TEXT` instead, you must `str()` it on every write *and* every
read, and the first place you forget is a query that silently returns zero rows.

**`status VARCHAR(50) NOT NULL DEFAULT 'created'`** -- the denormalized current state.
This is the column that justifies the whole read model. The log knows an order is shipped
because an `OrderShipped` exists somewhere in its stream; this column knows it because a
handler wrote the string `'shipped'` into it, so `WHERE status = 'shipped'` is an index
lookup instead of a replay.

Status lives here as a plain string set by handlers -- `'created'` on `OrderCreated`,
`'shipped'` on `OrderShipped`, `'cancelled'` on `OrderCancelled`. Resist the urge to add a
`CHECK` constraint listing the values while that set is still changing: a new event type
would then need a migration before the projection could be deployed, and read models are
meant to be the cheap half of the system.

**`updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`** -- when this row last changed. It is
the natural sort key for "most recently updated first", and the fastest way to notice a
projection has stalled.

There is a real decision hiding in this column, and it is the most common way to get a
read model subtly wrong:

```sql
-- Two candidate meanings for updated_at:
SET status = 'shipped', updated_at = NOW()          -- when the row was written
SET status = 'shipped', updated_at = :occurred_at   -- when the event happened
```

`NOW()` is server time at write time. On a live stream the two are within milliseconds of
each other, so the difference never shows -- until you rebuild. Replay a year of history
through a projection that uses `NOW()` and every row claims it was last updated this
afternoon. Bind `event.occurred_at` instead and the rebuilt table matches the one you
threw away, which is the property that makes rebuilds safe to do casually.

Use the column default only for rows inserted outside the projection, and have handlers
pass the event's own timestamp explicitly:

```python
{
    "id": event.aggregate_id,
    "updated_at": event.occurred_at,   # not datetime.now()
}
```

`occurred_at` is a `DomainEvent` envelope field, defaulted to `datetime.now(UTC)` when the
event is constructed -- so it is the moment the fact was recorded, not the moment your
projection got around to it. The same argument applies to `created_at`: set it from the
`occurred_at` of the event that created the row.

### The rest of the columns

`order_number`, `amount`, `tracking_number`, and `cancelled_reason` are payload copied out
of events as they arrive. Two things about them look like mistakes if you come from a
normalized-schema habit, and are not:

**Nullable columns are normal here.** `tracking_number` is NULL until an `OrderShipped`
arrives; `cancelled_reason` is NULL for every order that is never cancelled. A read-model
row is built up incrementally by a sequence of events, so any column contributed by a
later event must be nullable -- or have a default -- for the insert done by the first
event to succeed.

**Duplication is the point.** `order_number` and `amount` also live in the event store.
Copying them here means the list screen is one `SELECT` with no joins and no replay. A
projection is allowed to be as denormalized as its query needs; correctness lives in the
write model.

**There are no foreign keys** to anything in the event store, and there should not be. The
read model is derived and disposable -- `reset()` truncates it -- and a foreign key would
tie its lifecycle to tables it must be droppable independently of. Between projections, no
FKs either: two projections that reference each other's tables can no longer be rebuilt
one at a time.

### Indexes

The primary key already covers every write your handlers do (`INSERT ... VALUES (:id, ...)`
and `UPDATE ... WHERE id = :id`). Indexes exist for the queries the read model was built
to serve, so add them to match the screen:

```sql
CREATE INDEX IF NOT EXISTS ix_orders_status_updated
    ON orders_read_model (status, updated_at DESC);
```

That one composite index answers "every shipped order, most recently updated first" -- the
question from Step 1 that the event log could not answer without reading everything.
Column order matters: the equality column (`status`) first, the sort column
(`updated_at`) second.

Every index is also a cost on the write side, and a projection catching up on a rebuild is
write-heavy. For large rebuilds it is normal to create the table, replay everything, and
add the indexes afterwards.

### Where the DDL lives

The projection does not create this table. `DatabaseProjection` runs your handlers' SQL
inside a transaction; it never runs DDL, and nothing in the framework will create a
read-model table for you.

That is your schema to manage, with whatever tool you already use -- Alembic, plain SQL
files, or an `engine.begin()` block in a test fixture, which is what the integration tests
do:

```python
async with postgres_engine.begin() as conn:
    await conn.execute(text(ORDERS_SCHEMA))
```

Keep it clearly separate from `src/eventsource/adapters/sql/schemas/`, which holds the framework's
own schema (events, checkpoints, outbox, DLQ) and is append-only by design. Your read
models are yours, and unlike the framework's tables they can be dropped and recreated at
will.

### SQLite

To follow the next few steps without Docker, the same table in SQLite:

```sql
CREATE TABLE IF NOT EXISTS orders_read_model (
    id               TEXT PRIMARY KEY,
    order_number     TEXT NOT NULL,
    amount           TEXT NOT NULL DEFAULT '0',
    status           TEXT NOT NULL DEFAULT 'created',
    tracking_number  TEXT,
    cancelled_reason TEXT,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
```

Three substitutions, each of which changes what your handlers must bind:

| PostgreSQL | SQLite | Consequence for handlers |
| --- | --- | --- |
| `UUID` | `TEXT` | bind `str(event.aggregate_id)`, and `str()` it in queries too |
| `DECIMAL(10, 2)` | `TEXT` | SQLite has no decimal type, and `REAL` rounds money; store `str(amount)` and parse back to `Decimal` on read |
| `TIMESTAMPTZ` | `TEXT` | bind `event.occurred_at.isoformat()` -- ISO-8601 in UTC sorts correctly as text |

SQLite is fine for learning the mechanics and for fast tests. It cannot show you the
retry behavior in Step 10, though: that depends on PostgreSQL aborting a transaction after
an error, which is why Step 14 asks for the real thing.

### Creating it

For the steps that follow, create the table once against the compose-provided database:

```python
import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

DSN = "postgresql+asyncpg://test:test@localhost:5433/eventsource_test"

ORDERS_SCHEMA = """
CREATE TABLE IF NOT EXISTS orders_read_model (
    id               UUID PRIMARY KEY,
    order_number     VARCHAR(50) NOT NULL,
    amount           DECIMAL(10, 2) NOT NULL DEFAULT 0,
    status           VARCHAR(50) NOT NULL DEFAULT 'created',
    tracking_number  VARCHAR(100),
    cancelled_reason TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


async def main() -> None:
    engine = create_async_engine(DSN)
    async with engine.begin() as conn:
        await conn.execute(text(ORDERS_SCHEMA))
    await engine.dispose()


asyncio.run(main())
```

You now have a table, and a projection that writes to a dictionary. The next step connects
them, by handing the projection an `async_sessionmaker` and changing its base class.

## Step 4: Switching to `DatabaseProjection`

You have a table and a projection that writes to a dictionary. Connecting them is a
three-line change: import a different base class, hand it a `session_factory`, and let
your handlers take a connection.

### Build the session factory

`DatabaseProjection` does not take a URL, an engine, or a connection. It takes a
*factory* -- something it can call once per event to get a fresh session:

```python
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

DSN = "postgresql+asyncpg://test:test@localhost:5433/eventsource_test"

engine = create_async_engine(DSN, pool_size=5, max_overflow=10)

session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)
```

Three ordinary SQLAlchemy objects, in a fixed relationship:

| Object | Lifetime | Who creates it |
| --- | --- | --- |
| `AsyncEngine` | one per process -- it owns the connection pool | you, once at startup |
| `async_sessionmaker` | one per process -- a callable bound to that engine | you, once at startup |
| `AsyncSession` | one per event | the framework, by calling the factory |

That last row is the reason the constructor wants a factory rather than a session. Look
at what `_execute_in_transaction()` does for every single event:

```python
async with self._session_factory() as session, session.begin():
    conn = await session.connection()
    self._current_connection = conn
    ...
```

A new session, a new transaction, then a connection pulled out of it and stashed where
your handler can reach it. If you passed in one long-lived session, every event would
share a transaction and a failure anywhere would poison all of them. Passing the factory
is what makes per-event isolation possible.

`expire_on_commit=False` matches what the integration fixtures use. It only affects ORM
objects loaded through the session, and your handlers use `text()` against the raw
connection, so it changes nothing here -- but it costs nothing and avoids surprise
refresh queries if you later mix in ORM reads. `pool_size` and `max_overflow` are
whatever your deployment needs; the values above come from the test conftest.

Create the engine **once** and keep it. A fresh `create_async_engine` per event means a
fresh pool per event, which is how you turn a projection into a connection-exhaustion
incident.

### Change the base class

Now the projection itself:

```python
from sqlalchemy import text

from eventsource import DatabaseProjection, handles

from order_events import OrderCreated, OrderShipped


class OrderProjection(DatabaseProjection):
    @handles(OrderCreated)
    async def _handle_order_created(self, conn, event: OrderCreated) -> None:
        await conn.execute(
            text(
                """
                INSERT INTO orders_read_model
                    (id, order_number, amount, status, created_at, updated_at)
                VALUES (:id, :order_number, :amount, 'created', :ts, :ts)
                """
            ),
            {
                "id": event.aggregate_id,
                "order_number": event.order_number,
                "amount": event.amount,
                "ts": event.occurred_at,
            },
        )

    @handles(OrderShipped)
    async def _handle_order_shipped(self, conn, event: OrderShipped) -> None:
        await conn.execute(
            text(
                """
                UPDATE orders_read_model
                   SET status = 'shipped',
                       tracking_number = :tracking_number,
                       updated_at = :ts
                 WHERE id = :id
                """
            ),
            {
                "id": event.aggregate_id,
                "tracking_number": event.tracking_number,
                "ts": event.occurred_at,
            },
        )

    async def _truncate_read_models(self) -> None:
        async with self._session_factory() as session, session.begin():
            await session.execute(text("TRUNCATE TABLE orders_read_model"))
```

Compare it to Step 2 and exactly three things moved:

1. **The base class** is `DatabaseProjection` instead of `DeclarativeProjection`.
2. **Handlers gained a first parameter, `conn`.** That is an `AsyncConnection`, taken
   from the session the framework just opened. The `@handles` decorator is unchanged --
   the registry counts parameters and passes a connection only to the two-parameter ones,
   which is why Step 5 can mix both shapes in one class.
3. **`__init__` is gone.** There is no dictionary to set up anymore, and the base
   constructor already accepts everything you need.

`_truncate_read_models()` still takes no arguments -- `reset()` calls it outside any
transaction, so it opens its own session. (The class docstring in the source shows a
`conn` parameter here; that is stale, and a handler-style signature will fail.)

### Construct it

```python
projection = OrderProjection(session_factory=session_factory)
```

`session_factory` is the only required argument. Everything else --
`checkpoint_repo`, `dlq_repo`, `enable_tracing`, `tenant_filter` -- defaults, and Step 13
covers them. With the repositories left unset you get `InMemoryCheckpointRepository` and
`InMemoryDLQRepository`, exactly as in Step 2: your *read model* is now in PostgreSQL, but
the projection's own bookkeeping is still in process memory and disappears on restart.
That is fine for this tutorial and wrong for production.

Pass it as a keyword argument. It is the first positional parameter, so
`OrderProjection(session_factory)` works, but the keyword form reads better next to the
optional arguments you will add later.

### Run it

```python
import asyncio
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import text


async def main() -> None:
    projection = OrderProjection(session_factory=session_factory)
    order_id = uuid4()

    await projection.handle(
        OrderCreated(
            aggregate_id=order_id,
            aggregate_type="Order",
            order_number="ORD-001",
            amount=Decimal("42.00"),
        )
    )
    await projection.handle(
        OrderShipped(
            aggregate_id=order_id,
            aggregate_type="Order",
            tracking_number="TRK-9",
        )
    )

    async with engine.connect() as conn:
        rows = await conn.execute(
            text("SELECT id, status, updated_at FROM orders_read_model")
        )
        for row in rows:
            print(row)

    await engine.dispose()


asyncio.run(main())
```

You call `handle()` exactly as before. The change is what happens underneath it: per
event, the framework opens a session, begins a transaction, hands your handler the
connection, commits if the handler returns, rolls back if it raises, and only then writes
the checkpoint. Steps 7 through 9 take that sequence apart.

One thing to notice now, because it explains an error you will meet in Step 12: the
connection is not a parameter the framework threads through your call stack. It is
assigned to `self._current_connection` at the top of the transaction and set back to
`None` in a `finally` block when the transaction ends. A two-parameter handler reached
while that attribute is `None` -- which is what happens if you call `_process_event()`
yourself -- raises a `RuntimeError` rather than silently writing nowhere.

Your read model now lives in a real table. The next step looks more closely at the two
handler signatures and when to prefer each.

## Step 1: The events we're projecting

A projection is only as good as the events feeding it, so start by pinning them down.
Create `order_events.py`:

```python
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from eventsource import DomainEvent


class OrderCreated(DomainEvent):
    aggregate_type: str = "Order"

    order_number: str
    customer_id: UUID
    total: Decimal


class OrderShipped(DomainEvent):
    aggregate_type: str = "Order"

    tracking_number: str
    shipped_at: datetime


class OrderCancelled(DomainEvent):
    aggregate_type: str = "Order"

    reason: str
```

Three classes, three transitions in an order's life. Each carries only the data its own
transition introduces: `OrderShipped` does not repeat the customer or the total, because
the projection already saw those on `OrderCreated` and will have them in the row it is
maintaining. That is the same "narrow events" discipline from
[Your First Event](02-first-event.md), and it matters more on the read side than the
write side -- a projection that needs a field the event does not carry has no way to get
it.

### What every event already gives you

You did not declare `event_type`, and you do not have to.
`DomainEvent.__init_subclass__` sets the default to the class name at class-definition
time, so `OrderCreated().event_type == "OrderCreated"`. This is what `@handles` keys on
in Step 3, and it is the name written to the store -- which is why renaming an event
class is a schema change, not a refactor.

Beyond your three payload fields, `DomainEvent` contributes a fixed envelope that the
projection will lean on constantly:

| Field | Why the projection cares |
| --- | --- |
| `aggregate_id: UUID` | The natural primary key of the summary row -- one order, one row. |
| `aggregate_version: int` | Monotonic per aggregate, starting at 1. Step 11 stores it and compares against it to make handlers idempotent under replay. |
| `occurred_at: datetime` | UTC, defaulted at construction. Use this for "when did it happen", never `datetime.now()` inside a handler -- a replay would rewrite history to today. |
| `event_id: UUID` | Unique per event instance. Useful as a dedupe key and as the identifier the dead letter queue records in Step 5. |
| `aggregate_type: str` | Required by `DomainEvent`, which is why each class above defaults it to `"Order"`. |
| `correlation_id` / `causation_id` / `actor_id` / `metadata` | Not used by the view itself, but they ride along and are worth copying into the row when you want to trace where a change came from. |

Note the two required fields with no default: `aggregate_id` and `aggregate_type`. The
`aggregate_type: str = "Order"` line in each class supplies the second one so that
construction only needs the first.

### Events are frozen

`DomainEvent` sets `model_config = ConfigDict(frozen=True)`. A handler cannot normalize
an event in place -- `event.total = event.total.quantize(...)` raises a
`ValidationError`. Do the conversion when building the row instead. This is not a
restriction the projection has to work around; it is the guarantee that lets you replay
the same stream twice and get the same view.

### A stream to project

Everything from Step 2 on assumes a handful of these events for a single order. Build
them once so the versions line up the way a real aggregate would have written them:

```python
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

order_id = uuid4()
customer_id = uuid4()

events = [
    OrderCreated(
        aggregate_id=order_id,
        aggregate_version=1,
        order_number="ORD-001",
        customer_id=customer_id,
        total=Decimal("129.99"),
    ),
    OrderShipped(
        aggregate_id=order_id,
        aggregate_version=2,
        tracking_number="1Z999AA10123456784",
        shipped_at=datetime.now(UTC),
    ),
]
```

In production you would not hand-build these -- they come out of the event store as
`EventEnvelope` wrappers carrying `stream_version` and `position` alongside the
event, and the subscription machinery in Step 4 feeds them to the projection in global
order. Constructing them directly keeps the next few steps runnable in a single file, and
the handler code is identical either way: a handler only ever sees the `DomainEvent`.

With the input defined, you can write something that consumes it.

## Step 2: A first projection with DatabaseProjection

The simplest useful projection base class is `DatabaseProjection`. It hands each handler
a live `AsyncConnection`, wrapped in a transaction it opens and commits for you, so your
handler body is nothing but SQL. Everything else -- routing, retries, checkpoints, the
dead letter queue -- is inherited.

### The table

A projection owns its table. Nothing else writes to it, so design it for the query you
want to serve, not for normalization. Create `schema.sql` or just keep the DDL inline:

```python
SCHEMA = """
CREATE TABLE IF NOT EXISTS order_summary (
    id              TEXT PRIMARY KEY,
    order_number    TEXT NOT NULL,
    customer_id     TEXT NOT NULL,
    total           TEXT NOT NULL,
    status          TEXT NOT NULL,
    tracking_number TEXT,
    version         INTEGER NOT NULL,
    created_at      TEXT NOT NULL
)
"""
```

`id` is the order's `aggregate_id` -- one order, one row. `version` will hold the
`aggregate_version` of the last event folded in; Step 11 uses it for optimistic
concurrency, and you will want it well before that for debugging. `total` is `TEXT`
because SQLite has no decimal type and rounding money through a float is how you get
support tickets.

### The projection

```python
from sqlalchemy import text

from eventsource import DatabaseProjection, handles


class OrderSummaryProjection(DatabaseProjection):
    @handles(OrderCreated)
    async def _on_order_created(self, conn, event: OrderCreated) -> None:
        await conn.execute(
            text(
                "INSERT INTO order_summary "
                "(id, order_number, customer_id, total, status, version, created_at) "
                "VALUES (:id, :number, :customer, :total, 'created', :version, :at)"
            ),
            {
                "id": str(event.aggregate_id),
                "number": event.order_number,
                "customer": str(event.customer_id),
                "total": str(event.total),
                "version": event.aggregate_version,
                "at": event.occurred_at.isoformat(),
            },
        )

    @handles(OrderShipped)
    async def _on_order_shipped(self, conn, event: OrderShipped) -> None:
        await conn.execute(
            text(
                "UPDATE order_summary "
                "SET status = 'shipped', tracking_number = :tracking, version = :version "
                "WHERE id = :id"
            ),
            {
                "tracking": event.tracking_number,
                "version": event.aggregate_version,
                "id": str(event.aggregate_id),
            },
        )
```

Four things are worth pausing on.

**`handles` comes from `eventsource.domain.decorators`** (re-exported from the top-level
`eventsource` package, and also from `eventsource.application.projections`). It
is the same decorator `DeclarativeAggregate` uses.

**The handler signature is `(self, conn, event)`.** `DatabaseProjection` inspects each
decorated method's parameter count. Two parameters means "give me the connection"; one
parameter means "just the event", and that handler runs without touching the database --
useful for a handler that only logs or fires a notification. Both forms can coexist in
one projection.

**Always bind parameters.** `conn.execute(text(...), {...})` is not just about SQL
injection; the driver also handles quoting and typing for you. Note the explicit
`str(...)` on the two UUIDs and on the `Decimal` -- SQLAlchemy will not guess how you
want those stored.

**You did not write `subscribed_to()`.** `DeclarativeProjection` generates it from the
`@handles` decorators, so `projection.subscribed_to()` returns
`[OrderCreated, OrderShipped]` with no duplication to keep in sync.

There is no `OrderCancelled` handler yet. That is deliberate: by default
`unregistered_event_handling = "ignore"`, so an `OrderCancelled` arriving now is silently
skipped -- and, importantly, still checkpointed as processed. Step 3 covers changing that
default to `"warn"` or `"error"`.

### Clearing the view

`reset()` is inherited and clears the checkpoint, but only your projection knows which
tables to empty. Override `_truncate_read_models()`:

```python
    async def _truncate_read_models(self) -> None:
        async with self._session_factory() as session, session.begin():
            conn = await session.connection()
            await conn.execute(text("DELETE FROM order_summary"))
```

Note the signature: `_truncate_read_models(self)` takes no connection argument. `reset()`
runs outside the per-event transaction, so there is no ambient connection to hand you --
open your own from `self._session_factory`. Step 5 puts this to work.

### Running it

`DatabaseProjection` takes a SQLAlchemy `async_sessionmaker`, not an engine or a
connection:

```python
import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


async def main() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with engine.begin() as conn:
        await conn.execute(text(SCHEMA))

    projection = OrderSummaryProjection(session_factory=session_factory)

    for event in events:  # the two events from Step 1
        await projection.handle(event)

    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT order_number, status, tracking_number, version "
                    "FROM order_summary"
                )
            )
        ).all()
    print(rows)

    await engine.dispose()


asyncio.run(main())
```

```
[('ORD-001', 'shipped', '1Z999AA10123456784', 2)]
```

One row, folded from two events, with `status` and `version` advanced by the second. You
never opened a transaction, never called `commit()`, and never wrote a routing
`if isinstance(...)` chain.

### What `handle()` actually did

Each `await projection.handle(event)` ran this sequence:

1. Open a fresh session from the factory and begin a transaction
   (`async with self._session_factory() as session, session.begin()`).
2. Take the `AsyncConnection` from that session and look up the handler registered for
   `type(event)`.
3. Await your handler with that connection.
4. Commit on clean return -- or roll back if the handler raised.
5. **Only after the commit**, write the checkpoint recording `event.event_id` as
   processed.

Steps 4 and 5 in that order are the load-bearing part. The checkpoint advances after the
data is durable, so a crash between the two replays one event rather than skipping it --
at-least-once delivery, which is why your handlers need to tolerate seeing the same event
twice.

Failures get a fresh session per retry attempt, not a reused one. That matters on
PostgreSQL, where any error poisons the surrounding transaction and every subsequent
statement fails with "current transaction is aborted". By default the retry policy allows
two retries (three attempts total) with a 2-second exponential backoff; after that the
event goes to the dead letter queue and the exception is re-raised. Step 5 walks through
that path deliberately.

Right now both the checkpoint and the DLQ are in-memory -- you passed neither
`checkpoint_repo` nor `dlq_repo`, so the constructor defaulted to
`InMemoryCheckpointRepository` and `InMemoryDLQRepository`, and all progress vanishes
when the process exits. That is fine for a tutorial and wrong for production. The next
steps fix it.

## Step 3: Routing events with @handles

You have used `@handles` twice now without looking at what it does. This step opens it
up, because everything that surprises people about projections -- an event silently
skipped, a handler that never fires, a `HandlerSignatureError` at import time -- comes
from the routing layer, and it is small enough to understand completely.

### What the decorator stores

`handles` does almost nothing at decoration time:

```python
def handles(event_type: type[DomainEvent]) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        func._handles_event_type = event_type
        return func
    return decorator
```

It tags the function with an attribute and hands it back unchanged. It does not wrap the
function, so a decorated handler is still directly callable in a test, and it does not
register anything globally.

The real work happens when you *construct* the projection. `DeclarativeProjection.__init__`
builds a `HandlerRegistry` over `self`, which walks `dir(self)`, picks out every attribute
carrying `_handles_event_type`, records each one's name, whether it is a coroutine
function, and its parameter count, and stores the result in a dict keyed by event type.
`subscribed_to()` is just that dict's keys.

### Adding the third handler

Step 2 left `OrderCancelled` unhandled. Add it, and add a second handler that does not
touch the database at all:

```python
class OrderSummaryProjection(DatabaseProjection):
    unregistered_event_handling = "warn"

    @handles(OrderCreated)
    async def _on_order_created(self, conn, event: OrderCreated) -> None:
        ...  # as in Step 2

    @handles(OrderShipped)
    async def _on_order_shipped(self, conn, event: OrderShipped) -> None:
        ...  # as in Step 2

    @handles(OrderCancelled)
    async def _on_order_cancelled(self, conn, event: OrderCancelled) -> None:
        await conn.execute(
            text(
                "UPDATE order_summary "
                "SET status = 'cancelled', version = :version "
                "WHERE id = :id"
            ),
            {"version": event.aggregate_version, "id": str(event.aggregate_id)},
        )
```

```python
>>> sorted(e.__name__ for e in projection.subscribed_to())
['OrderCancelled', 'OrderCreated', 'OrderShipped']
```

You never edited a list. That is the point of generating `subscribed_to()` from the
decorators: the subscription and the routing cannot drift apart, because they are the
same data structure.

### Two legal signatures

The registry validates every handler at construction time and accepts exactly two shapes:

| Signature | What the handler gets |
| --- | --- |
| `async def h(self, event: E) -> None` | The event only. No database access. |
| `async def h(self, conn, event: E) -> None` | The transaction's `AsyncConnection`, then the event. |

Both can live in the same class. `DatabaseProjection._process_event` checks
`handler_info.param_count` per event and passes the connection only to the two-parameter
form:

```python
    @handles(OrderShipped)
    async def _notify_customer(self, event: OrderShipped) -> None:
        # No conn parameter -- no database work, no reason to ask for one.
        print(f"Shipped: {event.tracking_number}")
```

The transaction is still opened around it -- `DatabaseProjection` always begins a session
before dispatching -- but this handler simply never uses it.

Anything else fails loudly, and it fails when you *instantiate* the projection, not when
the first event arrives. Three parameters:

```
HandlerSignatureError: Handler '_h' in Bad has invalid signature for @handles(OrderCreated).

Expected one of:
  async def _h(self, event: OrderCreated) -> None
  async def _h(self, context, event: OrderCreated) -> None

Got: 3 parameter(s) (excluding self)
```

A non-`async` handler:

```
ValueError: Handler '_h' in Sync must be async.
```

`DeclarativeProjection` builds its registry with `require_async=True`, so a `def` where
you meant `async def` is rejected outright rather than silently returning a coroutine
nobody awaits. (`DeclarativeAggregate` uses the same decorator with `require_async=False`
-- aggregate handlers are sync. Same `@handles`, different contract.)

One check is advisory rather than fatal: if the annotation on the event parameter does
not match the type in the decorator, the registry logs a warning and carries on. The
decorator wins; the annotation is documentation. Keep them in step anyway -- a mismatch
almost always means a copy-paste slip.

### Routing is by exact type

`dispatch()` looks up `type(event)` in the dict. There is no walk up the MRO:

```python
class PriorityOrderCreated(OrderCreated):
    """Subclass of an event you already handle."""

# The OrderCreated handler will NOT run for this.
```

If you subclass an event, register the subclass explicitly. This is deliberate -- a
projection maintains a specific shape, and inheriting a handler you did not think about
is a worse failure than not receiving the event.

Two consequences of the dict follow directly:

**One handler per event type per projection.** Decorating two methods with
`@handles(OrderCreated)` does not run both; discovery iterates `dir(self)` in alphabetical
order and each registration overwrites the previous, so `_zeta` silently beats `_alpha`
and `handler_count` stays at 1. If you need two effects, call two private methods from one
handler.

**Subclasses override by method name.** Redefine `_on_order_created` in a subclass, keep
the `@handles(OrderCreated)` decorator on the override, and the child's version is the one
found -- ordinary Python attribute lookup, because the registry inspects the bound
attributes of the live instance.

### What happens to an event with no handler

This is the setting worth choosing consciously. `unregistered_event_handling` is a
class attribute with three values:

- **`"ignore"` (the default)** -- the event is dropped, no log line, and the checkpoint
  still advances past it. Good for a projection deliberately watching a slice of a busy
  stream; dangerous when you forgot a handler, because there is nothing to notice.
- **`"warn"`** -- same behavior, plus a warning naming the missing type and listing the
  handlers you do have:

  ```
  No handler registered for event type OrderRefunded in OrderSummaryProjection.
  Available handlers: OrderCreated, OrderShipped, OrderCancelled.
  Add @handles(OrderRefunded) decorator to handle this event.
  ```

- **`"error"`** -- raises `UnhandledEventError`.

Setting `unregistered_event_handling = "warn"` on the class above is the recommendation
for a tutorial and for most services: you keep forward compatibility with event types
added later, and you find out that you are ignoring them.

Be clear-eyed about `"error"` before reaching for it. The exception is raised inside
`_process_event`, which sits *inside* the retry loop -- so an unhandled event is retried
three times with exponential backoff (roughly four seconds of sleeping), lands in the dead
letter queue, and only then propagates. That is the machinery of Step 5 firing on what is
really a programming error, and in a live subscription it stalls the stream on every event
of that type. Use `"error"` in tests, where it turns a forgotten handler into an immediate
failure; prefer `"warn"` in production.

Note the asymmetry with the two permissive modes: under `"ignore"` and `"warn"` the
unhandled event is treated as successfully processed, so the checkpoint moves past it. Turn
`"warn"` into a real handler later and the events you skipped will not come back on their
own -- you have to reset and replay, which is exactly what Step 5 covers.

### Filtering by tenant

One more routing knob, if you use the multitenancy support: `DeclarativeProjection` takes
a `tenant_filter` in its constructor, checked before handler lookup.

```python
from eventsource import get_current_tenant

# Static: only ever this tenant's events.
projection = OrderSummaryProjection(session_factory=sf, tenant_filter=tenant_uuid)

# Dynamic: resolved per event from the ambient context.
projection = OrderSummaryProjection(session_factory=sf, tenant_filter=get_current_tenant)
```

A non-matching event is skipped with a debug log -- and, as with `"ignore"`, checkpointed.
Events with no `tenant_id` at all are always processed, so legacy and system events are not
accidentally filtered out.

With routing understood, the remaining question is who calls `handle()` in the first place,
and how the projection knows where it left off.

## Step 4: Running the projection and checking progress (checkpoints)

So far you have called `await projection.handle(event)` by hand. In a real service nobody
does that. A `SubscriptionManager` does it: it reads the backlog out of the `EventStore`,
hands each event to your projection, then switches to the `EventBus` for events arriving
live -- and it records where it got to, so a restart resumes instead of starting over.

### Wiring the manager

`SubscriptionManager` needs three collaborators: a store for history, a bus for live
events, and a checkpoint repository for position tracking.

```python
import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource import InMemoryCheckpointRepository
from eventsource.adapters.memory import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.application.subscriptions import SubscriptionConfig, SubscriptionManager


async def main() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.execute(text(SCHEMA))

    store = InMemoryEventStore()
    bus = InMemoryEventBus()
    checkpoints = InMemoryCheckpointRepository()

    # A backlog for the projection to catch up on.
    order_stream = StreamId(aggregate_id=order_id, category="Order")
    await store.append(
        order_stream,
        events,          # the OrderCreated + OrderShipped pair from Step 1
        ExpectedVersion.no_stream(),
    )

    projection = OrderSummaryProjection(
        session_factory=session_factory,
        checkpoint_repo=checkpoints,
    )

    manager = SubscriptionManager(
        event_store=store,
        event_bus=bus,
        checkpoint_repo=checkpoints,
    )
    subscription = await manager.subscribe(
        projection,
        SubscriptionConfig(start_from="beginning"),
        name="order-summary",
    )

    await manager.start()
    await asyncio.sleep(0.5)  # let catch-up drain; see below for the real version
```

`subscribe()` returns a `Subscription` you can query; `start()` launches the catch-up
runner in the background and returns immediately, which is why the snippet sleeps.
`manager.stop()` shuts it down, and `SubscriptionManager` is an async context manager
(`async with manager:`) if you would rather not pair the calls yourself.

`SubscriptionConfig.start_from` takes four kinds of value:

| Value | Meaning |
| --- | --- |
| `"checkpoint"` (default) | Resume from the stored position for this subscription name. |
| `"beginning"` | Start of the feed -- a full replay. |
| `"end"` | Live only; ignore everything already in the store. |
| a `Position` | Start from that specific opaque position token. |

The rest of the config is throughput and safety knobs -- `batch_size` (100),
`max_in_flight` (1000), `processing_timeout` (30s), `continue_on_error` (True), plus
event/aggregate/tenant filters. `event_types` on the config *overrides* the projection's
`subscribed_to()`, which is occasionally what you want and more often a surprise; leave
it unset and let the `@handles` decorators speak.

### Two different checkpoints

This is the part that repays five minutes of attention: **two independent things are
recording progress, and they record different things.**

1. **The projection's checkpoint** -- written by `CheckpointTrackingProjection` after
   every successful `_process_event`, via
   `checkpoint_repo.update_checkpoint(projection_name, event_id, event_type)`. It is
   keyed by the *projection class name* and stores the last event **id**. This is the one
   you read with `await projection.get_checkpoint()`.
2. **The subscription's position** -- written by the catch-up and live runners via
   `checkpoint_repo.save_position(subscription_id, position, event_id, event_type)`,
   keyed by the *subscription name* and storing an opaque **`Position`** token (rendered
   into the `position_token` column). This is what `start_from="checkpoint"` reads on the
   next boot.

Both live in the same `projection_checkpoints` table, in a row keyed by name -- so if the
subscription name equals the projection class name, they collide on one row. That is the
default, because `subscribe()` falls back to `subscriber.__class__.__name__`. The
collision is not harmless: `update_checkpoint` rewrites the row without a
`position_token`, so the projection's own write erases the position the subscription
saved, `events_processed` counts both writers, and the next `start_from="checkpoint"`
boot reads that row as no-position and replays the entire stream.

Pass an explicit `name=` -- as the snippet above does with `"order-summary"` -- and the
two live in separate rows. (Giving the projection its own `CheckpointRepository` instance
works equally well.) With that in place:

```python
    print(await projection.get_checkpoint())
    print(await checkpoints.get_position("order-summary"))
```

```
67adb3f2-d8e8-425d-a6af-f2562b5490f6
2
```

An event id from the projection, a global position from the subscription. Both are now
correct, and they answer different questions: *which event did my handler last apply*
versus *where do I resume reading*.

### Watching it work

Three views, from coarse to fine.

`subscription.get_status()` is the per-subscription snapshot:

```python
    print(subscription.get_status())
```

```
SubscriptionStatus(name='order-summary', state='live', position=2, lag_events=0,
                   events_processed=2, events_failed=0, last_processed_at='...',
                   started_at='...', uptime_seconds=0.4, error=None,
                   events_dlq=0, recent_errors_count=0)
```

`state='live'` means catch-up finished and the subscription has switched to the bus.
`events_dlq` and `events_failed` are the numbers to alert on -- Step 5 makes them move.

`manager.get_health()` rolls every subscription into one dict, and the manager also
exposes `readiness_check()` and `liveness_check()` for probe endpoints.

`projection.get_lag_metrics()` reports from the projection's side:

```python
    print(await projection.get_lag_metrics())
```

```
{'projection_name': 'OrderSummaryProjection',
 'last_event_id': '50d3e716-0725-4908-b858-7c8ffeea6e54',
 'latest_event_id': None,
 'lag_seconds': 0.0,
 'events_processed': 2,
 'last_processed_at': '2026-07-28T04:27:55.016398+00:00'}
```

Note `latest_event_id: None` and `lag_seconds: 0.0`. Real lag means comparing your
checkpoint against the newest relevant event in the store, and
`InMemoryCheckpointRepository` has no store to compare against -- it says so in its own
docstring and returns placeholders. The SQLite and PostgreSQL repositories query the
`events` table (filtered to the event types from `subscribed_to()`) and give you honest
numbers. Do not build a dashboard on in-memory lag.

### Live events

Once the subscription is `live`, publishing to the bus reaches the projection. Note the
list -- `EventBus.publish` takes a batch, not a single event:

```python
    cancelled = OrderCancelled(
        aggregate_id=order_id,
        aggregate_version=3,
        reason="customer request",
    )
    await store.append(order_stream, [cancelled], ExpectedVersion.exact(2))
    await bus.publish([cancelled])
    await asyncio.sleep(0.3)
```

```
[('cancelled', 3)]
```

Append to the store *and* publish to the bus. The store is the record; the bus is the
notification. Publishing without appending gives you a view that a rebuild cannot
reproduce -- in production you get both from one transaction with the outbox pattern
rather than two calls in sequence.

### Restarting

Tear the manager down and stand a fresh one up with `start_from="checkpoint"` and the
same subscription name:

```python
    await manager.stop()

    projection2 = OrderSummaryProjection(
        session_factory=session_factory, checkpoint_repo=checkpoints
    )
    manager2 = SubscriptionManager(
        event_store=store, event_bus=bus, checkpoint_repo=checkpoints
    )
    await manager2.subscribe(
        projection2, SubscriptionConfig(start_from="checkpoint"), name="order-summary"
    )
    await manager2.start()
    await asyncio.sleep(0.5)
    await manager2.stop()
```

The second run replays exactly one event: `OrderCancelled`. Not all three -- the stored
position skipped the two it had already caught up on. But not zero either, even though
`OrderCancelled` was processed before the restart.

That is worth understanding rather than working around. Live events arriving over the bus
do not carry a global position -- `LiveEventRunner._get_event_position` looks for a
`_global_position` attribute and finds none on a plain `DomainEvent` -- so live processing
cannot advance the stored position. It stays at the catch-up high-water mark, and
everything published live since then is re-read from the store on the next boot.

This is at-least-once delivery, deliberately. Combined with the ordering from Step 2 --
data committed first, checkpoint written second -- the framework's failure mode is always
"see it twice", never "miss it". Your handlers absorb that: `INSERT OR REPLACE` (or
`ON CONFLICT DO UPDATE`) instead of bare `INSERT`, and guard non-idempotent handlers with
the stored `version` column:

```sql
UPDATE order_summary SET status = 'shipped', version = :version
WHERE id = :id AND version < :version
```

Step 11 turns that comparison into `save_with_version_check` once the view is a typed read
model.

### Making the checkpoint survive the process

Everything above used `InMemoryCheckpointRepository`, so "restart" only meant a new
manager in the same process. For real durability, swap the repository. The table is part
of the shipped schema:

```python
from eventsource.adapters.sql.schemas import get_schema

ddl = get_schema("checkpoints", backend="sqlite")  # or backend="postgresql"
```

`SQLCheckpointRepository` (`eventsource.adapters.sql`) is dialect-parameterized -- the
same class serves both backends -- and always takes a SQLAlchemy `AsyncConnection` or
`AsyncEngine`, never a raw driver connection:

```python
from eventsource import SQLCheckpointRepository

# SQLite: a SQLAlchemy AsyncEngine created against sqlite+aiosqlite://...
checkpoints = SQLCheckpointRepository(sqlite_engine)

# PostgreSQL: a SQLAlchemy AsyncConnection or AsyncEngine.
checkpoints = SQLCheckpointRepository(engine)
```

Nothing else changes. The projection and the manager both take `checkpoint_repo` as a
constructor argument and speak only to the `CheckpointRepository` protocol, so the same
`OrderSummaryProjection` you ran in memory now resumes across process restarts.

Progress tracking is in place. The next question is what happens when a handler raises --
and how you throw the whole view away and rebuild it.
