# Migrations API Reference

Technical reference for the `eventsource.migrations` package: the loader
functions that return SQL text, the schema-name and backend type aliases, the
tables each schema creates, and the Alembic templates shipped alongside them.

The package ships no migration runner. It is a collection of `.sql` files plus
seven functions that read them off disk and hand you the text; applying that
text — through SQLAlchemy, `aiosqlite`, `psql`, or an Alembic revision you
generate from a template — is the caller's job. Nothing in this package opens a
database connection.

Three directories inside `src/eventsource/migrations/` hold the shipped SQL:

| Directory | Contains |
| --- | --- |
| `templates/` | Per-table PostgreSQL schemas, plus `templates/sqlite/` for the SQLite variants and `templates/alembic/` for `.py.template` migration stubs |
| `schemas/` | Pre-combined `all.sql` (PostgreSQL) and `sqlite_all.sql` |
| `updates/` | Incremental change scripts for existing deployments |

The SQL files are append-only by convention: published schema files are never
edited in place, so a deployment that applied an earlier release's `events.sql`
stays compatible.

## Overview

Every public function in `eventsource.migrations` resolves a path under the
package directory and returns what it finds there. Two of them return SQL text
(`get_schema`, `get_all_schemas`), one returns a `Path` (`get_template_path`),
one returns Alembic template text (`get_alembic_template`), and three enumerate
what is available (`list_schemas`, `list_backends`, `list_alembic_templates`).
There is no state, no caching, and no connection handling.

Selection happens on two axes: the schema name and the backend.

- **Schema name** — one of the `SchemaName` literals: `events`,
  `events_partitioned`, `outbox`, `checkpoints`, `dlq`, `snapshots`,
  `migration`, or `all`. Each name maps to `<name>.sql`, except `all`, which
  maps to the pre-combined file in `schemas/`.
- **Backend** — one of the `BackendName` literals: `postgresql` (the default)
  or `sqlite`. PostgreSQL templates live directly in `templates/`; every other
  backend gets a subdirectory, so SQLite resolves to `templates/sqlite/`.
  Combined schemas resolve to `schemas/all.sql` for PostgreSQL and
  `schemas/<backend>_all.sql` otherwise.

The two axes are not fully populated. `events_partitioned` and `migration` are
PostgreSQL-only — they use declarative partitioning, `plpgsql` functions, and
partial unique indexes that SQLite has no equivalent for. Asking for them with
`backend="sqlite"` raises `ValueError`, not `FileNotFoundError`; the error text
lists what that backend does have. A missing PostgreSQL file, by contrast,
raises `FileNotFoundError`, because it indicates a broken installation rather
than an unsupported combination.

The SQL itself is written to be applied more than once. Tables use
`CREATE TABLE IF NOT EXISTS`, indexes use `CREATE INDEX IF NOT EXISTS`, and
PostgreSQL routines use `CREATE OR REPLACE FUNCTION`, so re-running a schema
against a database that already has it is a no-op rather than an error. That
makes `get_all_schemas()` safe to run at application startup or in test
fixtures. It does not make the SQL a substitute for real migration
tracking — for versioned, reversible changes, generate an Alembic revision from
`templates/alembic/` instead and let Alembic own the history table.

Applying the text is always the caller's step, and the mechanism differs by
backend: PostgreSQL text goes through `conn.execute(text(...))` on a SQLAlchemy
connection, while SQLite text contains multiple statements and needs
`db.executescript(...)` on an `aiosqlite` connection. Both patterns are shown
under [Usage Examples](#usage-examples).

## Import Surface (`from eventsource.migrations import ...`)

Everything public lives in `eventsource.migrations/__init__.py` and is listed in
its `__all__`. The package is not re-exported from the top-level `eventsource`
namespace — import from `eventsource.migrations` directly.

```python
from eventsource.migrations import (
    # Type aliases
    SchemaName,
    BackendName,
    # Functions
    get_schema,
    get_all_schemas,
    get_template_path,
    list_schemas,
    list_backends,
    get_alembic_template,
    list_alembic_templates,
    # Schema name constants
    EVENTS_SCHEMA,
    EVENTS_PARTITIONED_SCHEMA,
    OUTBOX_SCHEMA,
    CHECKPOINTS_SCHEMA,
    DLQ_SCHEMA,
    SNAPSHOTS_SCHEMA,
    MIGRATION_SCHEMA,
)
```

| Name | Kind | Summary |
| --- | --- | --- |
| `SchemaName` | `Literal` alias | The eight accepted schema names |
| `BackendName` | `Literal` alias | `"postgresql"` or `"sqlite"` |
| `get_schema` | function | Returns SQL text for one schema (or `"all"`) |
| `get_all_schemas` | function | Returns the combined SQL text; thin wrapper over `get_schema("all", ...)` |
| `get_template_path` | function | Returns the on-disk `Path` of a per-table `.sql` template |
| `list_schemas` | function | Sorted schema names available for a backend |
| `list_backends` | function | Sorted backend names that have templates |
| `get_alembic_template` | function | Returns the text of an Alembic `.py.template` |
| `list_alembic_templates` | function | Sorted Alembic template names |
| `EVENTS_SCHEMA` … `MIGRATION_SCHEMA` | `str` constants | String literals matching the seven per-table schema names |

The seven constants are plain `str` values equal to their own schema names
(`EVENTS_SCHEMA == "events"`), provided so call sites can reference a symbol
instead of a bare string. There is no `ALL_SCHEMA` constant — pass `"all"`, or
call `get_all_schemas()`.

Module-level names prefixed with an underscore — `_PACKAGE_DIR`,
`_TEMPLATES_DIR`, `_SCHEMAS_DIR`, `_get_backend_templates_dir`, and
`_get_backend_schema_file` — resolve directory locations and are private. Use
`get_template_path` when you need a filesystem path.

The only imports the module itself pulls in are `pathlib.Path` and
`typing.Literal`, so `eventsource.migrations` is importable without SQLAlchemy,
`asyncpg`, or `aiosqlite` installed. The database driver is only needed at the
point where you apply the returned text.

## Type Aliases

Both aliases are `typing.Literal` definitions declared at module scope in
`eventsource/migrations/__init__.py`. They exist for static checking and
editor completion only: nothing in the module validates arguments against
them at runtime, so an out-of-range string reaches the path lookup and fails
there with `ValueError` or `FileNotFoundError` rather than a `TypeError`.

### `SchemaName`

```python
SchemaName = Literal[
    "events",
    "events_partitioned",
    "outbox",
    "checkpoints",
    "dlq",
    "snapshots",
    "migration",
    "all",
]
```

The `name` parameter type for `get_schema` and `get_template_path`. Seven of
the eight members name a per-table template and resolve to `<name>.sql` inside
the backend's templates directory. The eighth, `"all"`, is special-cased in
`get_schema`: it resolves into `schemas/` instead of `templates/`, picking up
the pre-combined file for the backend.

| Member | Resolves to (PostgreSQL) | Creates |
| --- | --- | --- |
| `"events"` | `templates/events.sql` | `events` |
| `"events_partitioned"` | `templates/events_partitioned.sql` | `events` (declaratively partitioned) |
| `"outbox"` | `templates/outbox.sql` | `event_outbox` |
| `"checkpoints"` | `templates/checkpoints.sql` | `projection_checkpoints` |
| `"dlq"` | `templates/dlq.sql` | `dead_letter_queue` |
| `"snapshots"` | `templates/snapshots.sql` | `snapshots` |
| `"migration"` | `templates/migration.sql` | tenant migration tables |
| `"all"` | `schemas/all.sql` | the combined set |

Two members are constrained by backend rather than by the alias.
`"events_partitioned"` and `"migration"` have no file under
`templates/sqlite/`, so passing them with `backend="sqlite"` raises
`ValueError`. The alias itself does not encode this — it is the union across
all backends, and the per-backend subset is what
[`list_schemas`](#list_schemasbackendpostgresql---liststr) reports.

`"all"` is likewise absent from `list_schemas` output, because that function
globs the templates directory and the combined files live in `schemas/`. A
value returned by `list_schemas` is always a valid `SchemaName`; the converse
does not hold.

The seven per-table members each have a matching `str` constant
(`EVENTS_SCHEMA`, `EVENTS_PARTITIONED_SCHEMA`, `OUTBOX_SCHEMA`,
`CHECKPOINTS_SCHEMA`, `DLQ_SCHEMA`, `SNAPSHOTS_SCHEMA`, `MIGRATION_SCHEMA`).
Those constants are annotated as plain `str`, not as `SchemaName`, so a type
checker will not narrow them back to the literal type — passing
`EVENTS_SCHEMA` where `SchemaName` is expected can draw a complaint under
strict settings. Passing the bare literal `"events"` always checks cleanly.

### `BackendName`

```python
BackendName = Literal["postgresql", "sqlite"]
```

The `backend` parameter type for `get_schema`, `get_all_schemas`,
`get_template_path`, and `list_schemas`. Every one of those functions defaults
it to `"postgresql"`.

The value drives two path resolutions, both in private helpers:

- `_get_backend_templates_dir` returns `templates/` for `"postgresql"` and
  `templates/<backend>/` for anything else — so SQLite templates come from
  `templates/sqlite/`.
- `_get_backend_schema_file` returns `schemas/all.sql` for `"postgresql"` and
  `schemas/<backend>_all.sql` otherwise — so the SQLite combined schema is
  `schemas/sqlite_all.sql`.

Because both helpers branch on `postgresql` versus everything-else rather than
enumerating members, PostgreSQL is the layout's special case: it is the only
backend whose templates sit at the top level and whose combined file is
unprefixed. Adding a backend means adding `templates/<name>/` and
`schemas/<name>_all.sql`, with no change to the resolution code — only the
alias needs widening.

The alias is also what makes the backend branch in the error handling
meaningful. When a template is missing and `backend != "postgresql"`, the
functions treat it as an unsupported combination (`ValueError`, with the
backend's available schemas listed); when the backend *is* `"postgresql"`, a
missing file means a broken installation and raises `FileNotFoundError`. See
[Error Behavior](#error-behavior-valueerror-vs-filenotfounderror). The one
exception is `get_schema("all", ...)`: a missing combined file raises
`FileNotFoundError` for every backend, since `"all"` is not a per-table
template and listing per-table schemas would not help.

What each backend actually ships:

| Backend | Templates directory | Templates present | Combined file |
| --- | --- | --- | --- |
| `"postgresql"` | `templates/` | `checkpoints`, `dlq`, `events`, `events_partitioned`, `migration`, `outbox`, `snapshots` | `schemas/all.sql` |
| `"sqlite"` | `templates/sqlite/` | `checkpoints`, `dlq`, `events`, `outbox`, `snapshots` | `schemas/sqlite_all.sql` |

`list_backends` returns the backends actually present on disk rather than the
alias members: it seeds the list with `"postgresql"` and appends every
subdirectory of `templates/` that is not `alembic` and contains at least one
`.sql` file. In the shipped package that yields `["postgresql", "sqlite"]`,
matching `BackendName` exactly. Its return type is `list[str]`, not
`list[BackendName]`, so values from it need a cast or a literal before they
type-check as a `backend` argument.

`get_alembic_template` and `list_alembic_templates` take no backend at all —
the Alembic stubs under `templates/alembic/` emit PostgreSQL DDL only.

## Schema Name Constants

### `EVENTS_SCHEMA`, `EVENTS_PARTITIONED_SCHEMA`, `OUTBOX_SCHEMA`, `CHECKPOINTS_SCHEMA`, `DLQ_SCHEMA`, `SNAPSHOTS_SCHEMA`, `MIGRATION_SCHEMA`

Seven module-level `str` constants, defined at the bottom of
`eventsource/migrations/__init__.py` under a `# Convenience exports` comment
and listed in `__all__`. Each holds the schema name it is named after:

```python
EVENTS_SCHEMA = "events"
EVENTS_PARTITIONED_SCHEMA = "events_partitioned"
OUTBOX_SCHEMA = "outbox"
CHECKPOINTS_SCHEMA = "checkpoints"
DLQ_SCHEMA = "dlq"
SNAPSHOTS_SCHEMA = "snapshots"
MIGRATION_SCHEMA = "migration"
```

| Constant | Value | Backends | Template file |
| --- | --- | --- | --- |
| `EVENTS_SCHEMA` | `"events"` | postgresql, sqlite | `events.sql` |
| `EVENTS_PARTITIONED_SCHEMA` | `"events_partitioned"` | postgresql only | `events_partitioned.sql` |
| `OUTBOX_SCHEMA` | `"outbox"` | postgresql, sqlite | `outbox.sql` |
| `CHECKPOINTS_SCHEMA` | `"checkpoints"` | postgresql, sqlite | `checkpoints.sql` |
| `DLQ_SCHEMA` | `"dlq"` | postgresql, sqlite | `dlq.sql` |
| `SNAPSHOTS_SCHEMA` | `"snapshots"` | postgresql, sqlite | `snapshots.sql` |
| `MIGRATION_SCHEMA` | `"migration"` | postgresql only | `migration.sql` |

They are pure aliases for string literals — no enum, no wrapper type, no
validation. Nothing inside the package consults them; `get_schema`,
`get_template_path`, and the rest take the raw string and build a path from it.
Their only purpose is to let a call site name a schema with an importable
symbol, so a typo becomes an `ImportError` at module load instead of a
`ValueError` or `FileNotFoundError` at call time:

```python
from eventsource.migrations import DLQ_SCHEMA, SNAPSHOTS_SCHEMA, get_schema

for schema in (DLQ_SCHEMA, SNAPSHOTS_SCHEMA):
    sql = get_schema(schema, backend="sqlite")
```

Two caveats apply when using them.

**They are annotated as `str`, not `SchemaName`.** The assignments carry no
annotation, so a type checker infers `str` and will not narrow back to the
literal type. Passing `EVENTS_SCHEMA` where a `SchemaName` parameter is
expected can be flagged under strict settings; the bare literal `"events"`
always checks cleanly. If you prefer symbols and strict typing both, declare
your own `Final[SchemaName]` bindings or cast at the call site.

**There is no constant for `"all"`.** The combined schema is not a per-table
template, and the convenience block covers only the seven templates. Pass the
literal `"all"` to `get_schema`, or call `get_all_schemas()`, which exists for
exactly that case.

Constant availability does not imply backend availability.
`EVENTS_PARTITIONED_SCHEMA` and `MIGRATION_SCHEMA` import fine but have no file
under `templates/sqlite/`, so `get_schema(MIGRATION_SCHEMA, backend="sqlite")`
raises `ValueError`. The per-backend subset is what `list_schemas` reports —
see the [Schema Availability Matrix](#schema-availability-matrix-schema-x-backend).
