# Set Up the Database Schema

The PostgreSQL and SQLite backends do not create their own tables. Before
`PostgreSQLEventStore`, `SQLiteEventStore`, or any of the outbox, checkpoint, DLQ,
and snapshot repositories will work, the tables they read and write have to
exist. `eventsource.adapters.sql.schemas` ships the SQL that creates them, plus helpers for
loading that SQL from the installed package instead of copying it into your repo.

This guide shows you how to:

- Apply the bundled combined schema with `psql` or the `sqlite3` CLI.
- Apply it from Python with `get_all_schemas()` over a SQLAlchemy async engine
  or an `aiosqlite` connection.
- Create only the tables you need with `get_schema(name, backend=...)`, and see
  what exists with `list_schemas()` and `list_backends()`.
- Generate an Alembic revision from the bundled templates with
  `get_alembic_template()` and fold it into an existing migration chain.
- Opt into the partitioned `events` table on PostgreSQL.
- Apply the versioned SQL in `updates/` to a database created by an earlier
  version of the library.

Pick exactly one of the three application options. They all produce the same
tables, so running two of them against the same database gives you either
duplicate-object errors or an Alembic chain that disagrees with what is actually
in the database.

Everything here is per-backend: `postgresql` is the default for every helper, and
`sqlite` is the only other backend with bundled templates. Some tables
(`events_partitioned`, `migration`) are PostgreSQL-only, so a SQLite setup is a
strict subset.

## Before you start

You need:

- **Python 3.13 or newer** with `eventsource-py` installed. The SQL and Alembic
  templates ship inside the `eventsource.adapters.sql.schemas` package, so the helpers
  read them out of your site-packages -- there is nothing to download or vendor.
- **A running database you can create tables in.** PostgreSQL 12 or newer for
  the PostgreSQL schema, SQLite 3.8 or newer for the SQLite schema.
- **A driver for the backend you are targeting**, if you intend to apply the
  schema from Python: `pip install "eventsource-py[postgresql]"` for `asyncpg`,
  or `pip install "eventsource-py[sqlite]"` for `aiosqlite`. The core install
  brings in `pydantic` and `sqlalchemy` only. If you are applying the SQL with
  `psql` or the `sqlite3` CLI instead, you do not need either driver yet -- but
  the event store will need one at runtime.
- **Alembic**, only if you are taking Option 3. It is not a dependency of this
  library; install it yourself.

On PostgreSQL, check two things before you run anything:

- The combined schema starts with `CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`.
  Creating an extension requires elevated privileges on most managed
  PostgreSQL services. If the extension already exists in the database, the
  `IF NOT EXISTS` guard makes the statement a no-op and an unprivileged role can
  run the rest of the file; if it does not, have a superuser or the service's
  extension-management console install it first.
- The whole combined file runs inside a single `BEGIN; ... COMMIT;`, so it either
  applies completely or not at all. Every `CREATE` in it is `IF NOT EXISTS`
  guarded, which makes a full re-run safe but also means it will silently leave
  an existing, differently-shaped table alone rather than correcting it.

Decide up front which tables you actually need. The combined schema creates all
five (`events`, `event_outbox`, `projection_checkpoints`, `dead_letter_queue`,
`snapshots`). If you are only using an event store with no outbox, projections,
or snapshots, skip ahead to `get_schema(name, backend=...)` and create just
`events`.

Finally, know which application option you are committing to. If your project
already has an Alembic chain, take Option 3 -- applying the raw SQL first and
adding an Alembic revision later leaves Alembic believing it created tables it
did not.

## Which tables eventsource needs

Nothing in the library creates tables on the fly, and no component checks for a
table it does not use. Create only the tables whose components you actually
construct.

| Table | Created by | Needed by | Backends |
| --- | --- | --- | --- |
| `events` | `get_schema("events")` | `PostgreSQLEventStore`, `SQLiteEventStore` | postgresql, sqlite |
| `event_outbox` | `get_schema("outbox")` | `PostgreSQLOutboxRepository`, `SQLiteOutboxRepository`, and the event stores when you append and enqueue in one transaction | postgresql, sqlite |
| `projection_checkpoints` | `get_schema("checkpoints")` | `PostgreSQLCheckpointRepository`, `SQLiteCheckpointRepository` -- resumable projections and subscriptions | postgresql, sqlite |
| `dead_letter_queue` | `get_schema("dlq")` | `PostgreSQLDLQRepository`, `SQLiteDLQRepository` | postgresql, sqlite |
| `snapshots` | `get_schema("snapshots")` | `PostgreSQLSnapshotStore`; `SQLiteSnapshotStore` applies the sqlite variant itself on connect | postgresql, sqlite |
| `events` (partitioned) | `get_schema("events_partitioned")` | drop-in replacement for `events` on high-volume PostgreSQL | postgresql only |
| `tenant_migrations`, `tenant_routing`, `migration_position_mappings`, `migration_audit_log` | `get_schema("migration")` | `PostgreSQLMigrationRepository` -- live store-to-store migration tooling | postgresql only |

The combined schema (`get_all_schemas()`, `schemas/all.sql`,
`schemas/sqlite_all.sql`) creates the first five rows only. The partitioned
`events` variant and the four migration tables are not in it -- if you want
either, apply their templates yourself.

### What each table carries

- **`events`** is the source of truth. It is keyed by a monotonic
  `global_position` (`BIGSERIAL` on PostgreSQL, `INTEGER PRIMARY KEY
  AUTOINCREMENT` on SQLite) and enforces `UNIQUE (aggregate_id,
  aggregate_type, version)`. That constraint is what turns a concurrent append
  into an `OptimisticLockError` rather than a corrupt stream, so do not drop it.
  `tenant_id` and `actor_id` are nullable -- leave `tenant_id` NULL for
  single-tenant deployments.
- **`event_outbox`** holds rows written in the same transaction as the append
  so a publisher can deliver them afterwards. `status` is constrained to
  `pending`, `published`, or `failed`, and most of its indexes are partial ones
  over `status = 'pending'`.
- **`projection_checkpoints`** is one row per projection name, tracking
  `global_position`, `last_event_id`, and `events_processed`. Without it a
  projection restarts from the beginning of the stream every time. On
  PostgreSQL the schema also installs an `update_checkpoint_timestamp()`
  trigger function that maintains `updated_at`.
- **`dead_letter_queue`** stores events a projection gave up on, unique per
  `(event_id, projection_name)`, with `status` in `failed`, `retrying`, or
  `resolved`.
- **`snapshots`** stores at most one row per `(aggregate_id, aggregate_type)`
  -- it is a cache in front of stream replay, not history. Deleting its
  contents is always safe; aggregates simply rebuild from `events`.

### Backend differences worth knowing

The SQLite schema is a straight subset of the PostgreSQL one, with the same
table and column names but different types: `TEXT` in place of `UUID` and
`VARCHAR`, `TEXT` timestamps in place of `TIMESTAMP WITH TIME ZONE`, and `TEXT`
payloads in place of `JSONB`. SQLite also gets no `CHECK` constraints on
`status`, no `COMMENT ON TABLE`, and no trigger -- application code enforces
what the PostgreSQL schema enforces in the database.

Two schemas have no SQLite counterpart at all: `events_partitioned` and
`migration`. Asking for them with `backend="sqlite"` raises `ValueError`, which
is the same error you get for any name outside `list_schemas("sqlite")`.

### If you are unsure, start with the event store only

A first cut needs `events` and nothing else. Add `projection_checkpoints` when
you start running projections, `dead_letter_queue` when those projections need
somewhere to park poison events, `event_outbox` when you move to transactional
publishing, and `snapshots` when replay latency becomes a problem. Each is a
separate template, so adding one later means applying one more file -- there is
no ordering dependency between them and no foreign keys tying them together.

## Option 1: Apply the bundled schema with a CLI client

This is the option to take when the database is set up once, by hand or by a
provisioning script, and your project has no Alembic chain to fold into. You run
one file and you are done.

The two combined files ship inside the installed package, at
`eventsource/adapters/sql/schemas/schemas/all.sql` (PostgreSQL) and
`eventsource/adapters/sql/schemas/schemas/sqlite_all.sql` (SQLite). You do not have to
guess where site-packages put them -- ask Python:

```bash
python -c "from importlib.resources import files; print(files('eventsource.adapters.sql.schemas').joinpath('schemas/all.sql'))"
```

That prints an absolute path you can hand to `psql -f`. If you would rather not
touch the filesystem at all, print the SQL to stdout with `get_all_schemas()`
and pipe it into the client; both approaches are shown below.

Both files create the same five tables -- `events`, `event_outbox`,
`projection_checkpoints`, `dead_letter_queue`, `snapshots` -- and nothing else.
The partitioned `events` variant and the migration tables are separate
templates; see the partitioning section below if you need them.

### PostgreSQL: psql and schemas/all.sql

Point `psql` at the packaged file. Always pass `-v ON_ERROR_STOP=1`: without it
`psql` reports an error and keeps going, and because the file wraps everything
in `BEGIN; ... COMMIT;` you can end up watching a long cascade of "current
transaction is aborted" messages scroll past a failure you have already missed.

```bash
SCHEMA=$(python -c "from importlib.resources import files; print(files('eventsource.adapters.sql.schemas').joinpath('schemas/all.sql'))")

psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$SCHEMA"
```

Or pipe the SQL straight from the helper, which avoids resolving a path and
works identically when the package is installed inside a container image:

```bash
python -c "from eventsource.adapters.sql.schemas import get_all_schemas; print(get_all_schemas())" \
  | psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f -
```

Expect output ending in `COMMIT`, with `CREATE TABLE` / `CREATE INDEX` /
`COMMENT` lines above it.

The file is safe to re-run end to end. Every `CREATE TABLE` and `CREATE INDEX`
is `IF NOT EXISTS` guarded, `update_checkpoint_timestamp()` is `CREATE OR
REPLACE FUNCTION`, and the one statement PostgreSQL will not let you guard --
`CREATE TRIGGER trg_checkpoint_updated_at` on `projection_checkpoints` -- is
preceded by an explicit `DROP TRIGGER IF EXISTS trg_checkpoint_updated_at ON
projection_checkpoints;`. A second pass re-creates the trigger and changes
nothing else.

That idempotence is about object *existence*, not object *shape*. If an `events`
table already exists with different columns, `CREATE TABLE IF NOT EXISTS` skips
it silently and you get no error at all -- the mismatch surfaces much later as a
runtime failure from the event store. When you are applying the schema to a
database that is not empty, inspect what is already there before you run the
file.

The one thing that commonly fails on a first run is the extension:

```
ERROR:  permission denied to create extension "uuid-ossp"
HINT:  Must be superuser to create this extension.
```

`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";` is the first statement in the
file and it sits *before* the `BEGIN;`, so it is the only thing that fails --
nothing else has been applied yet, and with `ON_ERROR_STOP=1` psql exits right
there. Have a superuser, or your provider's extension console, install the
extension once; then re-run the file as your normal role and the `IF NOT
EXISTS` guard turns that statement into a no-op.

Everything after the extension runs inside a single `BEGIN; ... COMMIT;`. If
any statement in that block fails, the whole transaction rolls back and you are
left with no partial schema to clean up.

The tail of `all.sql` carries a commented-out verification query. Run it to
confirm all five tables landed:

```bash
psql "$DATABASE_URL" -c "
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN ('events', 'event_outbox', 'projection_checkpoints', 'dead_letter_queue', 'snapshots')
ORDER BY table_name;"
```

Five rows, all `BASE TABLE`. Fewer than five means the transaction rolled back
or you ran a per-table template instead of the combined file.

### SQLite: sqlite3 and schemas/sqlite_all.sql

Same shape, different file. The SQLite schema has no transaction wrapper of its
own -- no `BEGIN;`/`COMMIT;` -- so `sqlite3` applies each statement as it reads
it. A failure partway through leaves the tables created up to that point.

```bash
SCHEMA=$(python -c "from importlib.resources import files; print(files('eventsource.adapters.sql.schemas').joinpath('schemas/sqlite_all.sql'))")

sqlite3 events.db < "$SCHEMA"
```

Or pipe the SQL from the helper, passing the backend explicitly -- every
migrations helper defaults to `postgresql`, so omitting `backend="sqlite"` gets
you the PostgreSQL file:

```bash
python -c "from eventsource.adapters.sql.schemas import get_all_schemas; print(get_all_schemas(backend='sqlite'))" \
  | sqlite3 events.db
```

The `sqlite3` CLI reads a whole multi-statement script from stdin without
complaint -- the one-statement-per-call limitation belongs to `aiosqlite`'s
`execute()`, not to the CLI, and is covered in the troubleshooting section.
Redirecting into `sqlite3 events.db` creates the database file if it does not
exist, so there is no separate "create the database" step.

Successful output is silence. Confirm with:

```bash
sqlite3 events.db ".tables"
```

which should list `dead_letter_queue`, `event_outbox`, `events`,
`projection_checkpoints`, `snapshots`, and the `sqlite_sequence` table SQLite
maintains for the `AUTOINCREMENT` primary keys.

Like the PostgreSQL file, this one is safe to re-run: every `CREATE TABLE` and
`CREATE INDEX` is `IF NOT EXISTS` guarded, and the `updated_at` trigger on
`projection_checkpoints` is written as `CREATE TRIGGER IF NOT EXISTS
trg_checkpoint_updated_at`. And as on PostgreSQL, that idempotence is about
existence, not shape -- an existing `events` table with different columns is
skipped silently, and the mismatch only shows up later as a runtime error.

The tail of `sqlite_all.sql` carries a commented-out verification query if you
want more than `.tables`:

```bash
sqlite3 events.db "
SELECT name, type
FROM sqlite_master
WHERE type = 'table'
  AND name IN ('events', 'event_outbox', 'projection_checkpoints', 'dead_letter_queue', 'snapshots')
ORDER BY name;"
```

Five rows. Fewer means a statement failed partway through -- scroll back for the
error, since without a surrounding transaction nothing rolled back.

Two differences from the PostgreSQL run are worth knowing before you go further:

- There is no `CREATE EXTENSION` step, so none of the privilege problems from
  the PostgreSQL section apply. SQLite generates its own row ids through
  `INTEGER PRIMARY KEY AUTOINCREMENT`; UUIDs come from application code.
- Two schemas have no SQLite file at all: `events_partitioned` and `migration`.
  If you need partitioned events or the live-migration tables, you need
  PostgreSQL.

Do not run the PostgreSQL file against SQLite or the reverse. `sqlite3` rejects
`BIGSERIAL`, `JSONB`, and `COMMENT ON`; `psql` rejects `AUTOINCREMENT` and
`datetime('now')` defaults.

## Option 2: Apply the schema programmatically

Take this option when the schema should be created by your own code rather than
by a person at a shell: test fixtures that need a throwaway database, a
container entrypoint that runs before the app serves traffic, or a
`bootstrap`/`init-db` subcommand you ship with the service. Everything Option 1
does with `psql` and `sqlite3` is reachable from Python through
`eventsource.adapters.sql.schemas`, which reads the same SQL files out of the installed
package.

The helpers only *load* SQL -- none of them touch a database. You choose the
driver and the transaction. That split is why the rest of this section is mostly
about how each driver wants to be handed a multi-statement script, which is the
one place this gets fiddly.

### Load the combined schema with get_all_schemas()

`get_all_schemas()` returns the contents of the combined file as a string:

```python
from eventsource.adapters.sql.schemas import get_all_schemas

pg_sql = get_all_schemas()                    # schemas/all.sql
sqlite_sql = get_all_schemas(backend="sqlite")  # schemas/sqlite_all.sql
```

`backend` defaults to `"postgresql"` on this and every other helper in the
module, so the SQLite call must pass it explicitly. It is a thin wrapper around
`get_schema("all", backend=backend)` -- same file, same string.

What you get back is the same five-table script described in Option 1:
`events`, `event_outbox`, `projection_checkpoints`, `dead_letter_queue`,
`snapshots`. The PostgreSQL string leads with `CREATE EXTENSION IF NOT EXISTS
"uuid-ossp";` and wraps the rest in `BEGIN; ... COMMIT;`. Keep that in mind
before you wrap the call in a transaction of your own: on PostgreSQL the script
already manages one.

### Apply it over a SQLAlchemy async engine (PostgreSQL)

This is where the obvious code does not work. The `asyncpg` dialect always
routes statements through a prepared statement, and PostgreSQL refuses to
prepare more than one command at a time. So the example that looks right --

```python
# Does NOT work with the asyncpg driver.
async with engine.begin() as conn:
    await conn.execute(text(get_all_schemas()))
```

-- fails with `asyncpg.exceptions.PostgresSyntaxError: cannot insert multiple
commands into a prepared statement`. The combined schema is forty-odd
statements.

Reach past SQLAlchemy to the raw `asyncpg` connection, whose `execute()` uses
the simple query protocol and accepts a whole script:

```python
from sqlalchemy.ext.asyncio import create_async_engine

from eventsource.adapters.sql.schemas import get_all_schemas


async def create_schema(dsn: str) -> None:
    engine = create_async_engine(dsn)
    try:
        async with engine.begin() as conn:
            raw = await conn.get_raw_connection()
            await raw.driver_connection.execute(get_all_schemas())
    finally:
        await engine.dispose()
```

`get_raw_connection()` gives you SQLAlchemy's pooled wrapper;
`.driver_connection` is the underlying `asyncpg.Connection`. Because the script
carries its own `BEGIN; ... COMMIT;`, the surrounding `engine.begin()` block is
there for connection handling rather than atomicity -- the script's own
transaction is what makes the schema all-or-nothing.

If you have no engine to begin with, skip SQLAlchemy entirely:

```python
import asyncpg

from eventsource.adapters.sql.schemas import get_all_schemas

conn = await asyncpg.connect(dsn)
try:
    await conn.execute(get_all_schemas())
finally:
    await conn.close()
```

Do not try to work around the prepared-statement limit by splitting the script
on `;`. The PostgreSQL templates define PL/pgSQL functions whose bodies are
dollar-quoted and contain their own `BEGIN`, `END;`, and internal semicolons --
`update_checkpoint_timestamp()` in `checkpoints.sql`, and a dozen more in
`outbox.sql` and `dlq.sql`. A naive split cuts those function bodies in half.
If you genuinely need statement-at-a-time execution (some connection poolers in
transaction mode will force this on you), keep a hand-maintained list of
statements the way `tests/integration/conftest.py` does rather than deriving one
from the packaged SQL.

Errors surface as they do in Option 1: a privilege error on `CREATE EXTENSION`
arrives as `asyncpg.exceptions.InsufficientPrivilegeError` before any table is
created, and re-running against a database that already has the schema is a
no-op, because every `CREATE` is `IF NOT EXISTS` guarded and the one trigger is
preceded by `DROP TRIGGER IF EXISTS`.

### Apply it with aiosqlite (SQLite)

`aiosqlite` has the mirror-image restriction and a purpose-built escape hatch.
`execute()` raises `sqlite3.ProgrammingError: You can only execute one statement
at a time.`; `executescript()` takes the whole file:

```python
import aiosqlite

from eventsource.adapters.sql.schemas import get_all_schemas


async def create_schema(path: str = "events.db") -> None:
    async with aiosqlite.connect(path) as db:
        await db.executescript(get_all_schemas(backend="sqlite"))
        await db.commit()
```

`connect()` creates the file if it is missing, and `":memory:"` gives you a
fresh schema per test with no cleanup. The SQLite script has no `BEGIN;`/
`COMMIT;` of its own -- `executescript()` issues an implicit `COMMIT` before it
starts and then runs the statements, so a failure partway through leaves the
tables created up to that point in place. Since every statement is `IF NOT
EXISTS` guarded (including `CREATE TRIGGER IF NOT EXISTS
trg_checkpoint_updated_at`), the fix for a partial apply is simply to run it
again.

Over a SQLAlchemy `aiosqlite` engine, the same driver rule applies -- go to the
raw connection and call `executescript`:

```python
async with engine.begin() as conn:
    raw = await conn.get_raw_connection()
    await raw.driver_connection.executescript(get_all_schemas(backend="sqlite"))
```

Verify the result from the same connection rather than shelling out:

```python
async with db.execute(
    "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
) as cursor:
    print([row[0] for row in await cursor.fetchall()])
# ['dead_letter_queue', 'event_outbox', 'events',
#  'projection_checkpoints', 'snapshots', 'sqlite_sequence']
```

`sqlite_sequence` is SQLite's own bookkeeping table for the `AUTOINCREMENT`
primary keys, not one of ours.

### Create tables selectively with get_schema(name, backend=...)

`get_schema()` loads one template instead of the combined file. Use it when you
want an event store and nothing else, or when you want a table the combined
schema does not include:

```python
from eventsource.adapters.sql.schemas import get_schema

events_sql = get_schema("events")
events_sql_lite = get_schema("events", backend="sqlite")
partitioned = get_schema("events_partitioned")   # PostgreSQL only
migration_tables = get_schema("migration")       # PostgreSQL only
```

Valid names are `events`, `events_partitioned`, `outbox`, `checkpoints`, `dlq`,
`snapshots`, `migration`, and `all`. Passing `"all"` routes to the combined file,
which is exactly what `get_all_schemas()` does.

Apply the strings the same way as the combined script -- one `asyncpg.execute()`
or one `executescript()` per template. There are no foreign keys between the
tables and no ordering constraints, so apply only what you need, in any order:

```python
for name in ("events", "checkpoints", "dlq"):
    await conn.execute(get_schema(name))
```

Two differences from the combined file are worth planning around:

- **No `CREATE EXTENSION`.** The `uuid-ossp` line lives only in `schemas/all.sql`.
  Per-table templates never create extensions, which is convenient on managed
  PostgreSQL where you may not have the privilege -- the library generates UUIDs
  in Python, so nothing at runtime depends on the extension.
- **No transaction wrapper.** The `BEGIN;`/`COMMIT;` pair is also only in the
  combined file. If you want several per-table templates to land atomically,
  open the transaction yourself, or concatenate the templates and send them as
  one script (asyncpg's simple-query protocol wraps a multi-statement script in
  an implicit transaction).

Beyond the five tables in the combined schema, `get_schema()` is the only way to
reach `events_partitioned` (a drop-in replacement for `events`, covered below)
and `migration` (the four tables behind `PostgreSQLMigrationRepository`). Both
are PostgreSQL-only.

If you would rather hand a path to an external tool than pass a string around,
`get_template_path(name, backend=...)` returns the `Path` to the same file
without reading it.

### Discover what is available: list_schemas() and list_backends()

Two helpers let you enumerate rather than hard-code:

```python
from eventsource.adapters.sql.schemas import list_backends, list_schemas

list_backends()
# ['postgresql', 'sqlite']

list_schemas()
# ['checkpoints', 'dlq', 'events', 'events_partitioned',
#  'migration', 'outbox', 'snapshots']

list_schemas("sqlite")
# ['checkpoints', 'dlq', 'events', 'outbox', 'snapshots']
```

Both return sorted lists derived from the `.sql` files actually present in the
installed package, so they cannot drift from what `get_schema()` will accept.
The difference between the two `list_schemas()` results is the whole story of
the backend gap: `events_partitioned` and `migration` are PostgreSQL-only.
Neither list includes `all`, even though `get_schema("all")` works -- the
combined files live in `schemas/`, not `templates/`, and these helpers only scan
`templates/`.

`list_schemas()` is what makes the `ValueError` for a missing SQLite template
actionable, since the error message embeds the available list:

```python
get_schema("events_partitioned", backend="sqlite")
# ValueError: Schema 'events_partitioned' is not available for backend
# 'sqlite'. Available schemas: ['checkpoints', 'dlq', 'events', 'outbox',
# 'snapshots']
```

A misspelled name under `backend="postgresql"` raises `FileNotFoundError`
instead -- there is no fallback backend to compare against. See the
troubleshooting section for both.

A useful pattern in setup code that supports both backends is to intersect what
you want with what the backend has:

```python
wanted = ["events", "checkpoints", "dlq", "snapshots"]
available = set(list_schemas(backend))

for name in wanted:
    if name in available:
        sql = get_schema(name, backend=backend)
        ...
```
