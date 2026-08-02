# Read Models API Reference

Reference documentation for `eventsource.ports.readmodels` and its adapters,
which together persist and query denormalized projection state: the
`ReadModel` Pydantic base class, the `Query`/`Filter` specification objects,
the `ReadModelRepository` protocol and its in-memory, PostgreSQL, and SQLite
implementations, the `ReadModelProjection` bridge between events and stored
rows, and the DDL generation helpers.

`eventsource.readmodels` -- the pre-slice-A import path for all sixteen names
below -- **no longer exists** (ADR 0030). Importing it raises
`ModuleNotFoundError`, with no deprecation shim. Update imports to the paths
this page documents.

Public names covered here (everything the old `eventsource.readmodels.__all__`
listed, now split across `eventsource.ports.readmodels` and three adapter
modules):

| Name | Kind | Purpose |
| --- | --- | --- |
| `ReadModel` | class | Pydantic base for read model definitions, with `id`, timestamps, `version`, and `deleted_at` |
| `ReadModelRepository` | protocol | Persistence contract: read, write, delete, and soft-delete operations |
| `ReadModelProjection` | class | Projection base that routes events to a repository-backed read model |
| `Query` / `Filter` | classes | Filter, ordering, and pagination specification passed to `find()` and `count()` |
| `InMemoryReadModelRepository` | class | In-process repository for tests and development |
| `PostgreSQLReadModelRepository` | class | Production repository over an asyncpg/SQLAlchemy async connection |
| `SQLiteReadModelRepository` | class | Embedded repository over an `aiosqlite` connection |
| `generate_schema` / `generate_indexes` / `generate_full_schema` | functions | DDL generation from a `ReadModel` subclass |
| `POSTGRESQL_TYPE_MAP` / `SQLITE_TYPE_MAP` | dicts | Python-to-SQL column type mappings used by schema generation |
| `ReadModelError` / `OptimisticLockError` / `ReadModelNotFoundError` | exceptions | Error hierarchy for read model operations |

The pure ones import from the port; the backends from their adapter modules:

```python
from eventsource.ports.readmodels import ReadModel, Query, Filter
from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
```

## Overview

Read models are the query side of the library's CQRS split. Where an
`AggregateRoot` is rebuilt from its event stream on every load, a `ReadModel` is
a denormalized row that is written once by a projection and then read directly —
no replay, no aggregation at query time.

The package divides into five concerns:

- **Model definition.** `ReadModel` is a Pydantic `BaseModel` (mutable, unlike
  the frozen `DomainEvent`) supplying `id`, `created_at`, `updated_at`,
  `version`, and `deleted_at`. Subclasses add domain fields; the table name is
  derived from the class name (`OrderSummary` → `order_summaries`) unless
  `__table_name__` overrides it.
- **Query specification.** `Filter` and `Query` are frozen dataclasses that
  describe filters, ordering, pagination, and soft-delete inclusion in a
  backend-neutral way. Each repository translates them into its own dialect.
- **Persistence contract.** `ReadModelRepository` is a `@runtime_checkable`
  `Protocol[TModel]` covering reads (`get`, `get_many`, `exists`, `find`,
  `count`), writes (`save`, `save_many`, `save_with_version_check`), deletes
  (`delete`, `soft_delete`, `restore`, `truncate`), and soft-delete reads
  (`get_deleted`, `find_deleted`). Every method is async, per the library's
  async-first design.
- **Projection integration.** `ReadModelProjection[TModel]` extends
  `DatabaseProjection` and hands each `@handles`-decorated handler a repository
  rather than a raw connection, picking the PostgreSQL or SQLite repository
  based on the configured session factory.
- **Schema tooling.** `generate_schema`, `generate_indexes`, and
  `generate_full_schema` emit `CREATE TABLE` / `CREATE INDEX` DDL from a
  `ReadModel` subclass using `POSTGRESQL_TYPE_MAP` or `SQLITE_TYPE_MAP`.

A few behaviors are worth internalizing before reading the per-symbol sections:

- `save()` is an **upsert** and increments `version` when a row already exists;
  `save_with_version_check()` is the concurrency-safe alternative and raises
  `OptimisticLockError` on a version mismatch.
- `find()` and `count()` **exclude soft-deleted rows by default**. Set
  `Query.include_deleted=True`, or use `find_deleted()` / `get_deleted()`, to
  see them.
- Filters combine with **AND only** — there is no `OR` in the query model. Push
  disjunctions into the projection's denormalized shape instead.
- A `Query` with no `limit` is unbounded. Repositories will not cap results for
  you.
- Timestamps are repository-managed: `updated_at` is set on every write, and
  `version` is maintained by the repository rather than by your handler code.

The three shipped implementations are interchangeable behind the protocol:
`InMemoryReadModelRepository` for tests, `PostgreSQLReadModelRepository` for
production, and `SQLiteReadModelRepository` for embedded or development use —
with the SQLite type-fidelity caveats noted under [Type Maps](#type-maps).

## Import Surface

`eventsource.ports.readmodels` is a subpackage (ADR 0029) rather than a flat
module, because it groups four genuinely distinct pure artifacts users import
for four different reasons: a subclassable pydantic base, a query
specification language, a 15-method repository Protocol, and an exception
family. Its `__all__` contains eight names; the three backend repositories and
three schema-generation names live in their adapter modules instead:

```python
__all__ = [
    "Filter",
    "OptimisticLockError",
    "Query",
    "ReadModel",
    "ReadModelError",
    "ReadModelNotFoundError",
    "ReadModelRepository",
    "ReadModelRepositoryProtocol",
]
```

### Names by defining module

| Exported name | Defined in | Kind |
| --- | --- | --- |
| `ReadModel` | `eventsource.ports.readmodels.model` | Pydantic `BaseModel` subclass |
| `ReadModelRepository` | `eventsource.ports.readmodels.repository` | `@runtime_checkable` `Protocol[TModel]` |
| `Query`, `Filter` | `eventsource.ports.readmodels.query` | query specification classes |
| `ReadModelError`, `OptimisticLockError`, `ReadModelNotFoundError` | `eventsource.ports.readmodels.exceptions` | exceptions |
| `ReadModelProjection` | `eventsource.adapters.sql.readmodel_projection` | class (extends `DatabaseProjection`) |
| `InMemoryReadModelRepository` | `eventsource.adapters.memory.readmodels` | `class InMemoryReadModelRepository[TModel: ReadModel]` |
| `PostgreSQLReadModelRepository` | `eventsource.adapters.postgresql.readmodels` | `class PostgreSQLReadModelRepository[TModel: ReadModel]` |
| `SQLiteReadModelRepository` | `eventsource.adapters.sqlite.readmodels` | `class SQLiteReadModelRepository[TModel: _BaseReadModel]` |
| `generate_schema`, `generate_indexes`, `generate_full_schema` | `eventsource.adapters.sql.readmodel_schema` | functions |
| `POSTGRESQL_TYPE_MAP`, `SQLITE_TYPE_MAP` | `eventsource.adapters.sql.readmodel_schema` | `dict` constants |

Import the pure names from the port package root — its submodule layout is an
implementation detail — and the backend names from their adapter modules:

```python
from eventsource.ports.readmodels import (
    Filter,
    OptimisticLockError,
    Query,
    ReadModel,
    ReadModelRepository,
)
from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.adapters.sql.readmodel_schema import generate_full_schema
```

### Backend imports are per-adapter, not eager on one namespace

Unlike the pre-slice-A `eventsource.readmodels` package, which imported all
three repository implementations eagerly at one package's import time,
`eventsource.ports.readmodels` pulls in none of them — importing it costs no
sqlalchemy and no aiosqlite. Each backend is its own adapter module,
imported only when you need that backend:

- `eventsource.adapters.postgresql.readmodels` depends on `sqlalchemy`
  (`AsyncConnection` / `AsyncEngine`), which is a core dependency.
- `eventsource.adapters.sqlite.readmodels` type-hints `aiosqlite.Connection`
  under `TYPE_CHECKING` only, so importing it does not require `aiosqlite` to
  be installed. You need `aiosqlite` at runtime only when you actually
  construct the repository and pass it a connection.

So `from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository`
succeeds in a bare install; the missing-dependency error, if any, surfaces
where you open the connection.

### Relationship to the top-level `eventsource` namespace

Of these names, only `ReadModelProjection` is re-exported from the top-level
`eventsource` package — unchanged by this move. Everything else must be
imported from `eventsource.ports.readmodels` or the relevant adapter module:

```python
from eventsource import ReadModelProjection               # available
from eventsource.ports.readmodels import ReadModel, Query  # required form
```

### Name collision: two `OptimisticLockError` classes

The library defines **two distinct** exceptions with this name, and they are
not related by inheritance. **This collision predates the slice A structure
work** — it is not something the port split introduced — and is tracked in
`BACKLOG.md` for resolution (proposed: rename the read-model one to
`ReadModelVersionConflictError` with a deprecation alias):

| Symbol | Base | Raised by |
| --- | --- | --- |
| `eventsource.OptimisticLockError` (from `eventsource.domain.exceptions`) | `EventSourceError` | aggregate/event-store concurrency conflicts |
| `eventsource.ports.readmodels.OptimisticLockError` | `ReadModelError` | `save_with_version_check()` on a read model repository |

An `except eventsource.OptimisticLockError` block will **not** catch a read
model version conflict. When both are in scope, alias one at the import site:

```python
from eventsource import OptimisticLockError
from eventsource.ports.readmodels import OptimisticLockError as ReadModelOptimisticLockError
```

See [Exceptions](#exceptions) for the attributes each carries.

## ReadModel

```python
class ReadModel(BaseModel)
```

Defined in `eventsource.ports.readmodels.model`. The Pydantic base class for
every persisted projection row. Subclass it, add domain fields, and the
repository and schema-generation helpers take care of the rest.

```python
from decimal import Decimal
from uuid import uuid4

from eventsource.ports.readmodels import ReadModel


class OrderSummary(ReadModel):
    order_number: str
    customer_name: str
    status: str
    total_amount: Decimal
    item_count: int = 0


summary = OrderSummary(
    id=uuid4(),
    order_number="ORD-001",
    customer_name="Alice Smith",
    status="pending",
    total_amount=Decimal("99.99"),
)
```

`id` is the only required field — everything else on the base has a default.

### Model configuration (mutable, `from_attributes`, `populate_by_name`)

```python
model_config = ConfigDict(
    from_attributes=True,
    populate_by_name=True,
)
```

| Setting | Value | Consequence |
| --- | --- | --- |
| `frozen` | *not set* (i.e. `False`) | Instances are **mutable**. This is the deliberate contrast with `DomainEvent`, which is frozen — projection handlers mutate a loaded model and save it back. |
| `from_attributes` | `True` | `Model.model_validate(obj)` accepts any object with matching attributes, not just a mapping. Used to hydrate models from ORM rows and `Row` objects. |
| `populate_by_name` | `True` | Fields declared with an alias can be populated by either the alias or the Python field name. |

Everything else is Pydantic v2 defaults: `extra="ignore"` (unknown keys are
dropped rather than rejected) and `validate_assignment=False` (see the caveat
below).

Mutability is what makes the read/modify/save cycle work:

```python
model = await repo.get(order_id)
model.status = "shipped"      # no copy, no model_copy(update=...)
await repo.save(model)
```

`from_attributes=True` is what lets a model be hydrated from a database row or
any other attribute-bearing object, not just a `dict`:

```python
class SourceObject:
    def __init__(self) -> None:
        self.id = uuid4()
        self.name = "from_attrs"
        self.created_at = datetime.now(UTC)
        self.updated_at = datetime.now(UTC)
        self.version = 1
        self.deleted_at = None


class SimpleModel(ReadModel):
    name: str


model = SimpleModel.model_validate(SourceObject())
model.name    # 'from_attrs'
```

Validation still runs on construction and on `model_validate` — the field
constraints on the base fields (notably `version >= 1`) are enforced there.
Because `validate_assignment` is *not* enabled, those constraints are **not**
re-checked on attribute assignment:

```python
model = SimpleModel(id=uuid4(), name="x")
SimpleModel(id=uuid4(), name="x", version=0)   # ValidationError
model.version = 0                              # accepted silently
```

In practice this does not bite, because `version` and `updated_at` are
repository-managed and overwritten on `save()` — but do not rely on assignment
to catch a bad value in your own domain fields.

### Base fields: `id`, `created_at`, `updated_at`, `version`, `deleted_at`

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `id` | `UUID` | *required* | Primary key. Usually the aggregate id the projection is tracking. |
| `created_at` | `datetime` | `datetime.now(UTC)` | Set once when the model is first constructed; timezone-aware UTC. |
| `updated_at` | `datetime` | `datetime.now(UTC)` | Refreshed by the repository on every write. |
| `version` | `int` | `1`, constrained `ge=1` | Optimistic-locking counter. `version=0` raises a Pydantic `ValidationError`. |
| `deleted_at` | `datetime \| None` | `None` | Soft-delete marker. Non-`None` means the row is excluded from default reads. |

Only `id` is required. A minimal construction therefore looks like:

```python
model = SimpleModel(id=uuid4(), name="test")
model.version        # 1
model.deleted_at     # None
model.created_at     # timezone-aware datetime, tzinfo is not None
```

#### `id`

Declared `id: UUID` with no default, so it must be supplied explicitly — read
models do not mint their own identity. In practice this is the id of the
aggregate (or other natural key) the projection tracks, which is what makes
`get(aggregate_id)` the normal lookup. It is the `PRIMARY KEY` in generated
schemas and the conflict target for the upsert in `save()`.

#### `created_at` and `updated_at`

Both use `default_factory=lambda: datetime.now(UTC)`, so both are always
timezone-aware UTC and both are set at construction time — a freshly built
model has `created_at == updated_at` in effect, not `None`.

After the first write the two diverge, and the split is enforced by the
repositories:

- `created_at` is **write-once**. The SQL backends explicitly exclude it from
  the `ON CONFLICT DO UPDATE` set (`f not in ("id", "created_at", "version")`),
  so an update never overwrites the stored creation timestamp even if the
  in-memory model carries a different one.
- `updated_at` is **repository-managed on every write**. `save()`,
  `save_many()`, and `save_with_version_check()` all stamp
  `datetime.now(UTC)` over whatever value the model holds. `soft_delete()`
  sets `updated_at` to the same instant as `deleted_at`, and `restore()`
  stamps it too.

Assigning `updated_at` in a projection handler is therefore pointless: it is
discarded on the next save.

#### `version`

`version: int = Field(default=1, ge=1)` — the optimistic-locking counter, and
the only base field with a validation constraint. Construction with `version=0`
raises a Pydantic `ValidationError`; the counter starts at 1 for a
newly-created model and is incremented by the repository, never by the caller:

- `save()` inserts at the model's version, and on conflict sets
  `version = <table>.version + 1`. The increment is computed from the
  **stored** row, not the in-memory one, so a stale model still advances the
  row by exactly one.
- `save_with_version_check()` updates only `WHERE id = :id AND version =
  :expected_version`, incrementing on success and raising
  `OptimisticLockError` (or `ReadModelNotFoundError`) otherwise. See
  [save() vs save_with_version_check()](#save-vs-save_with_version_check).

Because `validate_assignment` is off, `model.version = 0` after construction is
accepted silently; the constraint only guards construction and
`model_validate`.

#### `deleted_at`

`datetime | None`, default `None`. Non-`None` marks the row soft-deleted:
`is_deleted()` returns `True`, and `find()` / `count()` skip the row unless
`Query.include_deleted=True`. `soft_delete()` sets it, `restore()` clears it,
and `delete()` removes the row outright. Nothing in the library prunes
soft-deleted rows — retention is your policy to implement.

#### Whether a save mutates your instance is backend-dependent

`InMemoryReadModelRepository.save()` assigns the new `version` and
`updated_at` onto the model object you passed in. The SQL backends build a
parameter dict from `model_dump()` and let the database compute the new
version; they do **not** write it back. After a PostgreSQL or SQLite `save()`,
the in-memory instance still holds the pre-save `version` and `updated_at`.

```python
await repo.save(model)
model.version   # in-memory: incremented. postgres/sqlite: unchanged.
```

Do not read `model.version` after a save to learn the stored version — re-`get()`
the row, or drive concurrency through `save_with_version_check()`, which tells
you about a mismatch by raising rather than by leaving you to compare numbers.

### `__table_name__` and `table_name()` derivation (snake_case + pluralization rules)

```python
__table_name__: ClassVar[str | None] = None

@classmethod
def table_name(cls) -> str: ...
```

`table_name()` returns `__table_name__` when it is set, otherwise
`_pluralize(_camel_to_snake(cls.__name__))`.

```python
class CustomerView(ReadModel):
    name: str

CustomerView.table_name()          # 'customer_views'


class Invoice(ReadModel):
    __table_name__ = "billing_invoices"
    amount: Decimal

Invoice.table_name()               # 'billing_invoices'
```

**snake_case step.** Two regex passes insert underscores, then the result is
lowercased. Acronyms are handled reasonably:

| Class name | snake_case |
| --- | --- |
| `OrderSummary` | `order_summary` |
| `UserProfileView` | `user_profile_view` |
| `HTTPResponse` | `http_response` |
| `UserProfileDTO` | `user_profile_dto` |
| `Item2View` | `item2_view` |
| `ABC` | `abc` |

**Pluralization step.** `_pluralize` implements three rules, in order:

1. Ends in `y` preceded by a consonant → `y` becomes `ies`
   (`order_summary` → `order_summaries`, `entity` → `entities`).
   A vowel before the `y` skips this rule (`day` → `days`, `key` → `keys`).
2. Ends in `s`, `x`, `z`, `ch`, or `sh` → append `es`
   (`address` → `addresses`, `box` → `boxes`, `batch` → `batches`,
   `dash` → `dashes`).
3. Otherwise → append `s` (`order` → `orders`).

This is deliberately simple English pluralization with no irregular-plural
table. Two consequences to know about:

- Irregular plurals are wrong: a `Person` read model maps to `persons`, not
  `people`; `Status` maps to `statuses` (correct), but `Quiz` maps to
  `quizes`, not `quizzes` (no consonant doubling).
- `__table_name__` is a `ClassVar`, so it is **inherited**. If you set it on a
  base read model and subclass that base, the subclass silently reuses the same
  table name. Set `__table_name__` on each concrete model, or leave it `None`
  and let derivation run.

The override check is `if cls.__table_name__:` — a **truthiness** test, not an
`is not None` test. `__table_name__ = ""` therefore falls through to derivation
(a class `D` would land on `ds`, not on an error). Use `None` to mean "derive",
never the empty string.

### Who consumes `table_name()`

It is the single source of truth for the table a read model lives in, and every
SQL-touching component calls it rather than accepting a caller-supplied string:

- `PostgreSQLReadModelRepository` and `SQLiteReadModelRepository` set
  `self._table_name = model_class.table_name()` in their constructors and
  interpolate it into every statement they build. Because the value comes from
  a class definition and never from user input, it is a trusted identifier —
  the repositories' own tests assert exactly this
  (`test_table_name_from_trusted_source`).
- The [schema helpers](#schema-generation) emit
  `CREATE TABLE <table_name()>` and index names derived from the same value.

The practical consequence: the derived name is part of your database contract.
Renaming `OrderSummary` to `OrderProjection` silently repoints the repository
at `order_projections` and every existing row disappears from view. Whenever
the derived name is wrong, irregular, or merely something you would rather not
couple to a Python class name, pin it explicitly:

```python
class OrderSummary(ReadModel):
    __table_name__ = "order_summaries"   # pinned, survives class renames
```

Whenever the derived name is wrong or unstable, pin it explicitly — the table
name is part of your database contract, and renaming a class should not
silently repoint a projection at a new table.

### `field_names()` / `custom_field_names()`

```python
@classmethod
def field_names(cls) -> list[str]: ...

@classmethod
def custom_field_names(cls) -> list[str]: ...
```

Two classmethods over `cls.model_fields`, used to build SQL column lists
without hard-coding field names.

- `field_names()` is `list(cls.model_fields.keys())` — every field in Pydantic
  declaration order: the five base fields first (inherited fields come first in
  `model_fields`), then subclass fields in the order they are declared.
- `custom_field_names()` is the same list filtered against the literal set
  `{"id", "created_at", "updated_at", "version", "deleted_at"}`.

```python
class ProductView(ReadModel):
    sku: str
    name: str
    price: Decimal

ProductView.field_names()
# ['id', 'created_at', 'updated_at', 'version', 'deleted_at', 'sku', 'name', 'price']

ProductView.custom_field_names()
# ['sku', 'name', 'price']
```

Both are classmethods, so no instance is required.

### Declaration order is a wire contract in the SQL backends

`PostgreSQLReadModelRepository` and `SQLiteReadModelRepository` snapshot the
list once in their constructors:

```python
self._field_names = model_class.field_names()
```

and then use it for both halves of every round trip — the projected column
list (`SELECT {", ".join(self._field_names)} FROM ...`), the `INSERT` column
list and its placeholders, and the `ON CONFLICT DO UPDATE` set (which
subtracts `id`, `created_at`, and, on the version-managing paths, `version`).
Rows come back as positional tuples and are zipped straight back against the
same list:

```python
data = dict(zip(self._field_names, row, strict=True))
```

Because the `SELECT` list and the unzip list are both derived from
`field_names()`, order is internally consistent and reordering your field
declarations is safe — the generated SQL moves with them. What is *not* safe is
a table whose physical column set has drifted from the model: a field added to
the subclass without a matching `ALTER TABLE` produces a `SELECT` naming a
column that does not exist, and `strict=True` in the zip turns any
length mismatch into a hard error rather than silent misalignment.

Note that `field_names()` is snapshotted at construction time. Adding fields to
the model class after a repository has been built will not be picked up by that
repository instance.

### `custom_field_names()` has no in-tree consumer

Despite its docstring ("useful for generating SQL column lists that exclude
standard fields"), nothing in the library currently calls it — the schema
helpers iterate `model_class.model_fields` directly, and the repositories use
the full `field_names()` list. It is a public convenience for your own tooling:
diffing a model against `information_schema`, building a projection-specific
`UPDATE` set, or rendering domain fields in an admin view.

The base-field exclusion is a literal name set, not a check against
`ReadModel.model_fields`. A subclass field that happened to be named `version`
would be filtered out — but there is no reason to shadow a base field, and
doing so would also break the repositories' version handling.

### `is_deleted()` / `to_dict()` / `__str__` / `__repr__`

```python
def is_deleted(self) -> bool: ...
def to_dict(self) -> dict[str, Any]: ...
```

**`is_deleted()`** is `self.deleted_at is not None`. It reports on the
in-memory instance only — it does not consult the repository, so a model loaded
before another writer soft-deleted the row still returns `False`.

**`to_dict()`** is `self.model_dump(mode="json")`: a dictionary in which every
value is JSON-serializable. `UUID` becomes `str`, `datetime` becomes an ISO
8601 `str`, `Decimal` becomes a JSON number/string per Pydantic's JSON mode.

```python
model.to_dict()
# {'id': '3f2a...', 'created_at': '2026-07-27T18:04:11.123456Z',
#  'updated_at': '...', 'version': 1, 'deleted_at': None,
#  'name': 'test', 'amount': '99.99'}
```

Use `to_dict()` when handing a model to an API response or a JSON column; use
plain `model_dump()` when you want native Python types back.

**`__str__`** is a one-line identity summary:

```
OrderSummary(id=3f2a49c8-..., version=1)
```

**`__repr__`** adds both timestamps, and appends `deleted_at` only when the
model is soft-deleted:

```
OrderSummary(id=UUID('3f2a49c8-...'), version=1,
             created_at=datetime.datetime(...), updated_at=datetime.datetime(...))

OrderSummary(id=UUID('3f2a49c8-...'), version=3,
             created_at=..., updated_at=..., deleted_at=datetime.datetime(...))
```

Neither includes custom fields, so neither leaks domain data into logs — but
neither is a substitute for `to_dict()` when you actually need the contents.
