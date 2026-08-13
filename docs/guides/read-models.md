# How to Work with Read Models

A *read model* is a denormalized, directly persisted view of aggregate state,
shaped for the queries your application actually runs. Where an aggregate is
rebuilt by replaying its events, a read model row is written once by a
projection handler and then read back cheaply. `eventsource.ports.readmodels`
and its adapters give you the pieces for that: a `ReadModel` base class, three repository backends
behind one `ReadModelRepository` protocol, a backend-agnostic `Query` / `Filter`
pair, DDL generation from the model definition, and `ReadModelProjection` to
drive the whole thing from events.

This guide shows you how to:

- Define a `ReadModel` subclass, control its table name, and declare indexes.
- Pick between the in-memory, SQLite, and PostgreSQL repositories.
- Generate the `CREATE TABLE` / `CREATE INDEX` DDL for a model with
  `generate_full_schema()` and apply it at startup or from a migration.
- Save, fetch, filter, order, paginate, and count rows.
- Handle concurrent writers with `save_with_version_check()` and the
  `OptimisticLockError` it raises, or fall back to last-write-wins `save()`.
- Use the soft-delete lifecycle (`soft_delete`, `restore`, `get_deleted`,
  `find_deleted`, `Query.with_deleted()`) and know which read paths hide
  deleted rows.
- Wire a `ReadModelProjection` so event handlers receive a repository scoped to
  the projection's transaction.

Everything here assumes async code: every repository method is a coroutine.

Two behaviors are worth internalizing before you start, because they explain
most surprises later:

- **`ReadModel` is mutable, `DomainEvent` is not.** Events are frozen
  (`ConfigDict(frozen=True)`); read models deliberately are not, because
  projection handlers load a row, mutate fields, and save it back. Every
  `ReadModel` carries `id`, `created_at`, `updated_at`, `version`, and
  `deleted_at` on top of your domain fields, and the repository maintains the
  last four for you.
- **Soft-deleted rows are invisible by default.** `get`, `get_many`, `find`,
  `count`, and `exists` all skip rows whose `deleted_at` is set. That is
  usually what you want, and it is the first thing to check when a row you
  know you wrote does not come back.

## Before you start

A `ReadModel` is a Pydantic `BaseModel` subclass — but a deliberately *mutable*
one. Its `model_config` sets `from_attributes=True` and `populate_by_name=True`
and pointedly does **not** set `frozen=True`, with the reason spelled out in the
source: projection handlers need to load a row, assign to fields, and save it
back. This is the opposite of `DomainEvent`, which declares
`model_config = ConfigDict(frozen=True)` so that a recorded fact can never be
edited after the fact. Events are the immutable log; read models are the
scratch surface you rebuild from it.

Every `ReadModel` subclass inherits five fields before you add any of your own:

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `id` | `UUID` | required | Primary key. You supply it — usually the aggregate id. |
| `created_at` | `datetime` | `datetime.now(UTC)` | When the instance was first constructed. |
| `updated_at` | `datetime` | `datetime.now(UTC)` | Stamped by the repository on every `save`. |
| `version` | `int` (`ge=1`) | `1` | Optimistic-locking counter, incremented by the repository on update. |
| `deleted_at` | `datetime \| None` | `None` | Soft-delete marker; `None` means live. |

Your subclass adds domain fields on top:

```python
from decimal import Decimal
from eventsource.ports.readmodels import ReadModel


class OrderSummary(ReadModel):
    order_number: str
    customer_name: str
    status: str
    total_amount: Decimal
    item_count: int = 0
```

Three consequences follow from that layout, and they are worth holding onto
because they explain most of the surprises later in this guide.

**The repository owns `updated_at` and `version`, not you.** `save()` mutates
the instance you hand it: on an insert it stamps `updated_at`; on an update it
sets `version = existing.version + 1` and stamps `updated_at` again. Assigning
to `version` yourself before a plain `save()` has no effect — the repository
overwrites it. `created_at` is *not* maintained by the repository; it comes from
the field default at construction time, so a model you rebuild from scratch and
re-save will carry a new `created_at` unless you copy the old one across.

**Soft-deleted rows are invisible by default.** `get`, `get_many`, `find`,
`count`, and `exists` all skip rows whose `deleted_at` is set. Use
`get_deleted` / `find_deleted` to see only the deleted set, or
`Query(include_deleted=True)` to see both. When a row you are certain you wrote
does not come back, check `deleted_at` first.

**Base-field names are reserved.** `ReadModel.custom_field_names()` returns your
fields by filtering out exactly
`{"id", "created_at", "updated_at", "version", "deleted_at"}`. The schema
generator emits a column for every entry in `model_fields`, so naming a domain
field after one of those five will collide rather than shadow.

Two helpers on the class are useful while you are setting things up:
`ReadModel.table_name()` returns the resolved table name (derived snake_case
plural, or `__table_name__` if you set it), and `ReadModel.field_names()`
returns all fields including the base five, in declaration order.

You will also want, before going further:

- An async runtime. Every repository method is a coroutine; there is no sync
  facade for read models.
- A backend decision. `InMemoryReadModelRepository` needs nothing;
  `SQLiteReadModelRepository` needs an `aiosqlite.Connection`;
  `PostgreSQLReadModelRepository` needs a SQLAlchemy `AsyncConnection` or
  `AsyncEngine`. Install the matching extra (`aiosqlite`, `asyncpg`).
- A place to run DDL. Read model tables are not created for you — you generate
  the SQL from the model class and apply it yourself.

## Define a read model

### Subclass ReadModel and add domain fields

Declare the fields your queries need, with normal Pydantic annotations and
defaults. Nothing else is required — identity, timestamps, version, and the
soft-delete marker come from the base class.

```python
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from eventsource.ports.readmodels import ReadModel


class OrderSummary(ReadModel):
    order_number: str
    customer_id: UUID
    customer_name: str
    status: str
    total_amount: Decimal
    item_count: int = 0
    shipped_at: datetime | None = None
```

Construct one by passing `id` (the only required base field) plus your fields:

```python
from uuid import uuid4

summary = OrderSummary(
    id=uuid4(),
    order_number="ORD-001",
    customer_id=customer_id,
    customer_name="Alice Smith",
    status="pending",
    total_amount=Decimal("99.99"),
)
```

Keep the field set flat and query-shaped. Every entry in `model_fields` becomes
a column when you generate DDL, and any annotation the dialect type map does not
recognize falls back to `TEXT`. Nested models and containers therefore land as
`JSONB` (PostgreSQL) or `TEXT` (SQLite) rather than something you can index or
filter on precisely. Prefer scalar columns for anything a `Filter` will target,
and reserve `list` / `dict` fields for payloads you only ever read back whole.

Your annotations also decide nullability. A field typed `T | None` is emitted as
a nullable column; anything else — required or with a default — gets `NOT NULL`.
So `shipped_at: datetime | None = None` above is the right shape for a value
that genuinely may be absent, while `item_count: int = 0` becomes
`item_count INTEGER NOT NULL DEFAULT 0`. Making a field optional after the table
exists is a migration, so decide before you generate DDL.

Remember the five reserved names — `id`, `created_at`, `updated_at`, `version`,
`deleted_at`. A domain field with one of those names collides with the base
field rather than shadowing it. To check what you ended up with,
`OrderSummary.custom_field_names()` returns just your fields
(`['order_number', 'customer_id', ...]`) and `OrderSummary.field_names()`
returns all of them, base five first, in declaration order.

### Table naming: derived snake_case plural vs. explicit `__table_name__`

By default `table_name()` derives the table from the class name: CamelCase is
converted to snake_case, then pluralized with a small set of English rules.

| Class | Derived table |
| --- | --- |
| `OrderSummary` | `order_summaries` |
| `UserProfile` | `user_profiles` |
| `Address` | `addresses` |
| `Batch` | `batches` |
| `HTTPResponse` | `http_responses` |

The pluralizer handles the common shapes only: a trailing consonant + `y`
becomes `ies`; a trailing `s`, `x`, `z`, `ch`, or `sh` gets `es`; everything else
gets `s`. Irregular plurals are not handled — a `Person` read model becomes
`persons`, not `people`.

When the derived name is wrong, or when the table already exists under a name
you do not control, set `__table_name__` and it wins outright:

```python
class Invoice(ReadModel):
    __table_name__ = "billing_invoices"

    amount: Decimal


Invoice.table_name()  # 'billing_invoices'
```

`__table_name__` is declared on the base class as `ClassVar[str | None] = None`,
so Pydantic treats it as class metadata rather than a field, and the double
trailing underscore keeps it clear of Python's private-name mangling. The
override is checked for truthiness, not for `None` — setting it to `""` falls
back to the derived name rather than producing an empty table name.

Set it whenever the derived name would be ambiguous across bounded contexts,
and always when you are mapping onto a table you did not create. Changing it
later is a migration: the same name feeds both the DDL generator and every
repository, so a rename with no `ALTER TABLE` behind it points a working
repository at a table that does not exist.

Everything downstream reads the resolved value through `table_name()`. The DDL
generators interpolate it into `CREATE TABLE` / `CREATE INDEX`, and each
repository captures it once in its constructor
(`self._table_name = model_class.table_name()`) and interpolates it into every
statement it builds. Filter values are always bound as parameters, but the
table name, field names, and index names are formatted straight into the SQL
with no quoting or escaping — the generators trust them because they come from
the class definition. Keep all three to plain unquoted identifiers: lowercase
letters, digits, and underscores, and not a reserved word.

Two quick checks while wiring things up:

```python
OrderSummary.table_name()  # 'order_summaries'
Invoice.table_name()       # 'billing_invoices'
```

Use `table_name()` rather than re-deriving the name yourself when you write
migrations or ad-hoc queries, so the doc, the DDL, and the repository can never
disagree.

### Declare custom indexes with `__indexes__` (fields, name, unique, where)

`generate_indexes()` always emits one index on `deleted_at` so the soft-delete
filter on every read path has support. Anything else you want, you declare on
the model as `__indexes__` — a list of dicts, each describing one index:

```python
from typing import Any, ClassVar
from uuid import UUID


class OrderSummary(ReadModel):
    __indexes__: ClassVar[list[dict[str, Any]]] = [
        {"fields": ["status"], "where": "deleted_at IS NULL"},
        {"fields": ["customer_id", "created_at"]},
        {"fields": ["priority"], "name": "idx_priority_custom"},
    ]

    status: str
    customer_id: UUID
    priority: int = 0
```

Unlike `__table_name__`, `__indexes__` is not declared on `ReadModel` — the
generator looks for it with `getattr(model_class, "__indexes__", [])` and falls
back to an empty list. Annotate it as `ClassVar` so Pydantic and your type
checker treat it as class metadata rather than a field; an unannotated
assignment reaches the generator too, but Pydantic will try to interpret it.
Because it is ordinary class-level state, a subclass that sets `__indexes__`
replaces its parent's list wholesale rather than adding to it.

Recognized keys:

| Key | Required | Effect |
| --- | --- | --- |
| `fields` | yes | Column list, in index order. A spec with an empty or missing `fields` list is skipped silently. |
| `name` | no | Index name. Defaults to `idx_{table}_{field1}_{field2}...`. |
| `where` | no | Partial-index predicate, emitted verbatim as `WHERE <clause>`. **PostgreSQL only** — on the `sqlite` dialect the clause is dropped and the index is created unfiltered. |

For the model above, `generate_indexes(OrderSummary, dialect="postgresql")`
produces:

```sql
CREATE INDEX IF NOT EXISTS idx_order_summaries_deleted ON order_summaries(deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_order_summaries_status ON order_summaries(status) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_order_summaries_customer_id_created_at ON order_summaries(customer_id, created_at);
CREATE INDEX IF NOT EXISTS idx_priority_custom ON order_summaries(priority);
```

The same call with `dialect="sqlite"` emits an unfiltered
`idx_order_summaries_deleted` on `deleted_at`, and drops the `WHERE
deleted_at IS NULL` from the `status` index. `generate_full_schema()` appends
these same statements after the `CREATE TABLE`, so you rarely call
`generate_indexes()` directly except to inspect what you are about to run.

Three limits to plan around:

- **There is no `unique` key.** The generator only ever emits `CREATE INDEX`,
  never `CREATE UNIQUE INDEX`, and an extra `"unique": True` in a spec is
  ignored. If a read model needs a uniqueness constraint — a natural key such
  as `order_number`, say — add the `CREATE UNIQUE INDEX` by hand in the
  migration that creates the table, alongside the generated DDL.
- **Every statement uses `IF NOT EXISTS`.** Re-running the generated index DDL
  is safe, but changing an existing index's definition is not picked up. Drop
  and recreate it in a migration instead.
- **Nothing is validated.** Field names, index names, and the `where` clause
  are interpolated straight into the SQL string; the generator never checks
  that a listed field exists on the model or that the predicate parses. A typo
  surfaces as a database error when you apply the DDL, not at import time.

Index the columns your `Filter`s and `order_by` clauses actually use. Because
the default read paths append a `deleted_at IS NULL` predicate, a partial index
with `{"where": "deleted_at IS NULL"}` on PostgreSQL is usually the better shape
for hot query columns than a plain index over the whole table.

### Add a column to a table that already exists

`generate_schema()` emits `CREATE TABLE IF NOT EXISTS`, and against a database
that already has the table, that statement does nothing whatsoever. So adding a
field to a `ReadModel` does **not** add a column to any database created before
the field existed — and nothing tells you, because your tests build their
tables from nothing, where the `CREATE` is always complete. The failure shows
up the first time production writes the new field.

`reconcile_read_model_schema()` closes the additive half of that gap:

```python
from eventsource.adapters.sql.readmodel_reconcile import reconcile_read_model_schema

applied = await reconcile_read_model_schema(engine, OrderSummary)
for statement in applied:
    logger.info("read model schema: %s", statement)
```

It reads the columns the table currently has, adds the ones the model declares
and the table lacks, and returns the statements it ran — empty when nothing was
needed, so it is safe to call on every startup. If the table does not exist it
creates it with its indexes. The dialect comes from the connection; there is no
dialect argument to get wrong. Pass an `AsyncConnection` to keep the DDL in
your own transaction, or an `AsyncEngine` to have one opened and committed.

Nothing in the library calls it for you. It is a function you call, at a point
you choose, and it is deliberately not a replacement for Alembic — it handles
the one schema change that is safe to make unattended and refuses everything
else:

- **Additive only.** No column is dropped, retyped, renamed, or reordered, and
  a column the model no longer declares is left in place.
- **A required field with no default is refused**, with
  `ReadModelSchemaMismatchError`, because it cannot be added to a table that
  may already have rows. Give the field a default or make it optional — or
  write the real migration, which is what the refusal is telling you the change
  needs. The check runs before any statement executes, so a refusal leaves the
  table untouched.
- **Indexes on an existing table are not reconciled.** Only `CREATE TABLE`
  brings the generated index DDL with it.

To inspect before executing, or to feed the statements into a migration tool
instead, use the pure function underneath — it does no I/O and takes the
current columns as an argument:

```python
from eventsource.adapters.sql.readmodel_schema import generate_additive_migration

statements = generate_additive_migration(OrderSummary, existing_columns, dialect="postgresql")
```

## Choose a repository backend

Three implementations ship with the library, all satisfying the same
`ReadModelRepository` protocol: `InMemoryReadModelRepository`,
`SQLiteReadModelRepository`, and `PostgreSQLReadModelRepository`. They expose an
identical method set — `get`, `get_many`, `save`, `save_many`,
`save_with_version_check`, `find`, `count`, `exists`, `delete`, `soft_delete`,
`restore`, `get_deleted`, `find_deleted`, `truncate` — so projection code
written against the protocol runs unchanged on any of them. Each is imported
from its own adapter module.

```python
from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.adapters.postgresql.readmodels import PostgreSQLReadModelRepository
from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository
from eventsource.ports.readmodels import (
    ReadModelRepository,
)
```

Type your own code against the protocol, not the concrete class, so swapping
backends is a constructor change:

```python
repo: ReadModelRepository[OrderSummary] = InMemoryReadModelRepository(OrderSummary)
```

What differs between them is the constructor, the transaction story, and how
faithfully your Python types survive a round trip.

### InMemoryReadModelRepository — tests and prototypes

Use this backend for unit tests, examples, and prototypes. It is the only one
that needs no database, no connection, and no DDL — construct it with the model
class and start saving:

```python
from uuid import uuid4

from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository

repo = InMemoryReadModelRepository(OrderSummary)

await repo.save(
    OrderSummary(
        id=uuid4(),
        order_number="ORD-001",
        customer_id=customer_id,
        customer_name="Alice Smith",
        status="pending",
        total_amount=Decimal("99.99"),
    )
)
```

`model_class` is the only positional argument. The constructor also accepts two
optional tracing knobs shared by all three backends — `tracer` and
`enable_tracing=True` — which you can leave alone unless you are wiring
OpenTelemetry spans in tests.

Rows live in a plain `dict[UUID, TModel]` on the instance, guarded by a single
`asyncio.Lock` that every method acquires for the duration of its work. That
makes concurrent coroutines on one event loop serialize correctly, which is
what lets you exercise async projections and the optimistic-locking paths
without a database. It is *not* a cross-thread or cross-process guarantee:
`asyncio.Lock` protects one event loop only, and the dict dies with the
instance, so nothing survives a restart.

Behaviorally it is a full `ReadModelRepository` — same soft-delete rules, same
version increments, same `OptimisticLockError` and `ReadModelNotFoundError`
from `save_with_version_check()`. Four differences matter when you write tests
against it.

**ID lookups are O(1); every query is O(n).** `get`, `get_many`, `exists`,
`soft_delete`, and `restore` are dict lookups. `find`, `count`, and
`find_deleted` build a list of every stored model and then apply each `Filter`
in Python, one pass per filter, before ordering and slicing. For the tens or
hundreds of rows a test creates that is free; it is not a query engine, and a
prototype that grows past a few thousand rows will feel it.

**Filters are evaluated in Python, so type coercion differs.** Comparisons run
against the raw Python objects — `Decimal` stays `Decimal`, a `datetime` stays
a `datetime`, and `in` / `not_in` use Python containment. SQL backends compare
after a round trip through the database's type system. An `eq` filter on a
`Decimal` that passes here can behave differently against SQLite, where
`Decimal` lands in a `REAL` column.

*Operator* semantics, on the other hand, do not differ: every backend dispatches
through one shared table, so which operators exist, how they treat a `None`
field, and the fact that an unknown field name or operator raises `ValueError`
are the same everywhere. See `ReadModelRepository.find` for the rules and
`ReadModelRepositoryConformance` for the matrix that pins them.

**Ordering has no null handling.** `find` sorts with
`getattr(model, query.order_by)` as the key, so ordering by a nullable field
whose values mix `None` with real values raises `TypeError` rather than sorting
nulls first or last the way SQL would. Order by a non-nullable field, or filter
the nulls out first.

**Models are stored by reference, not copied.** `save()` puts *your* instance
into the dict and `get()` hands the same object back:

```python
saved = await repo.get(order_id)
saved.status = "shipped"          # the stored row changed already
assert (await repo.get(order_id)).status == "shipped"  # passes, with no save()
```

The SQL backends serialize on write and reconstruct on read, so a test that
leans on this aliasing passes in memory and fails against Postgres. Where the
distinction matters, mutate a `model_copy()` and save that, or assert against a
freshly fetched instance.

Two conveniences exist here that the SQL backends do not offer:

| Helper | Behavior |
| --- | --- |
| `await repo.clear()` | Test-teardown alias for `truncate()`. Drops every model, soft-deleted included, and returns `None` (`truncate()` returns the count). |
| `len(repo)` | Synchronous count of every stored model, including soft-deleted ones — unlike `await repo.count()`, which excludes them. |

`len(repo)` is the quickest way to assert that a soft delete kept the row:
after `await repo.soft_delete(order_id)`, `await repo.count()` drops to zero
while `len(repo)` stays at one.

Because of the aliasing, `Decimal` handling, and O(n) query differences, treat
green in-memory tests as necessary but not sufficient. Keep at least one
integration test per read model running against the backend you actually
deploy.

### SQLiteReadModelRepository — embedded and development

Takes an open `aiosqlite.Connection` plus the model class:

```python
import aiosqlite

from eventsource.adapters.sql.readmodel_schema import generate_indexes, generate_schema
from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository

async with aiosqlite.connect("readmodels.db") as db:
    await db.execute(generate_schema(OrderSummary, dialect="sqlite"))
    for index_sql in generate_indexes(OrderSummary, dialect="sqlite"):
        await db.execute(index_sql)
    await db.commit()

    repo = SQLiteReadModelRepository(db, OrderSummary)
    await repo.save(summary)
```

`aiosqlite` is an optional dependency, imported only under `TYPE_CHECKING` in
the module, so importing `eventsource.adapters.sqlite.readmodels` does not require it — but
constructing the connection does. Install the `aiosqlite` extra.

Notable characteristics:

- **The repository commits for you.** Every write method calls
  `connection.commit()` after executing. You cannot batch several repository
  calls into one atomic unit through this API; each `save`, `soft_delete`, and
  so on lands independently.
- **Upserts need SQLite 3.24+**, because writes are
  `INSERT ... ON CONFLICT(id) DO UPDATE`.
- **Types are lossy in both directions.** Values are written from
  `model.model_dump(mode="json")`: UUIDs become 36-character text, datetimes
  ISO 8601 text, `dict`/`list` fields JSON strings. On read, only `id` and the
  three timestamp columns get explicit conversion back; every other field is
  handed to `model_validate`, which coerces what it can from the text. The
  sharp edge is `Decimal`, which the SQLite type map sends to `REAL` — a float
  column. Money columns will drift. Store amounts as integer minor units if you
  need SQLite to be exact — see [Money and Precision](money-and-precision.md) for
  the trade-off against `Decimal`, which is exact on the event itself regardless of
  which read-model backend you use.

Good for embedded deployments, local development, and integration tests that
want real SQL semantics without a server.

### PostgreSQLReadModelRepository — production

Takes a SQLAlchemy `AsyncConnection` **or** an `AsyncEngine`, plus the model
class:

```python
from eventsource.adapters.postgresql.readmodels import PostgreSQLReadModelRepository

async with engine.begin() as conn:
    repo = PostgreSQLReadModelRepository(conn, OrderSummary)
    await repo.save(summary)
    await repo.save(other_summary)  # same transaction
```

Which one you pass decides who owns the transaction, and this is the single
most important thing to get right:

- **Pass an `AsyncConnection`** and the repository uses it directly for every
  statement and never commits. Your surrounding `engine.begin()` block (or the
  projection's transaction) commits or rolls back, so several repository calls
  are one atomic unit. This is what you want inside a projection handler.
- **Pass an `AsyncEngine`** and the repository opens its own scope per call:
  `engine.begin()` for writes (committed when the call returns) and
  `engine.connect()` for reads. Convenient for a standalone query service;
  useless for grouping writes, since each one commits on its own.

The backend uses native PostgreSQL types — `UUID`, `TIMESTAMP WITH TIME ZONE`,
`DECIMAL(18, 6)`, `JSONB` — so values round-trip without the string encoding SQLite
needs; rows come back straight through `model_validate`. Writes are
`INSERT ... ON CONFLICT (id) DO UPDATE`, `get_many` uses `id = ANY(:ids)`, and
`truncate` reports the number of rows removed. `asyncpg` is an optional
dependency; install that extra along with a `postgresql+asyncpg://` URL.

### Backend comparison

| | InMemory | SQLite | PostgreSQL |
| --- | --- | --- | --- |
| Constructor | `(model_class)` | `(aiosqlite.Connection, model_class)` | `(AsyncConnection \| AsyncEngine, model_class)` |
| Extra dependency | none | `aiosqlite` | `asyncpg` |
| Setup cost | none | create the file, apply DDL | provision a server, apply DDL |
| Needs generated DDL | no | yes | yes |
| Transaction control | n/a | none — commits per call | caller's, when you pass a connection |
| Concurrency | one event loop, `asyncio.Lock` | single-writer file locking | full MVCC, multi-process |
| Query cost | O(n) Python scan | indexed SQL | indexed SQL, partial indexes honored |
| Type fidelity | exact Python objects | lossy (`Decimal` → `REAL`, JSON as text) | native types, `DECIMAL`/`JSONB` |
| Partial `__indexes__` `where` | n/a | dropped | applied |
| Data survives restart | no | yes | yes |
| Use it for | unit tests, prototypes | dev, embedded, single-node | production |

A practical default: `InMemoryReadModelRepository` in unit tests,
`PostgreSQLReadModelRepository` everywhere else, with SQLite when the deployment
genuinely has nowhere to run a server. Because the in-memory backend differs on
reference aliasing, `Decimal` handling, and O(n) queries, keep at least one
integration test per read model running against the backend you actually ship.
