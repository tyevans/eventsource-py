# Tutorial 11: Running EventSource on PostgreSQL

So far you have been running everything in memory. Events vanished when the
process exited, and that was fine — you were learning the shapes of the API,
not operating a system. In this tutorial you will move the same code onto
PostgreSQL, so that appended events survive restarts.

The important idea here is that you never write the schema by hand, and you
never copy SQL out of the repository. EventSource ships its table definitions
*inside the installed package*, under `eventsource.migrations`. You ask the
library for the SQL and execute it against your database:

```python
from sqlalchemy import text
from eventsource.migrations import get_all_schemas

async with engine.begin() as conn:
    await conn.execute(text(get_all_schemas()))
```

That one call creates every table the library needs — `events`,
`event_outbox`, `projection_checkpoints`, `dead_letter_queue`, and
`snapshots` — for the PostgreSQL backend, which is the default. You will run
exactly that, then look at what it built, then point a
`PostgreSQLEventStore` at the result and append your first durable event.

By the end you will also know how to pull a single table's SQL with
`get_schema(name)`, how to discover what is available with `list_schemas()`
and `list_backends()`, and how to hand the whole thing over to Alembic using
the packaged migration templates from `get_alembic_template()`.

Work through the steps in order — each one builds on the database you set up
in the previous one.

## What you'll build

By the end of this tutorial you will have a running PostgreSQL database with
the complete EventSource schema installed from the packaged SQL, and a
`PostgreSQLEventStore` writing to it. You will append an event, restart your
Python process, read the event back, and see that it is still there.

Concretely, you will:

1. Start a PostgreSQL container using the `docker-compose.test.yml` that ships
   with the repository.
2. Apply the packaged schema with a single `get_all_schemas()` call, creating
   the five tables the library uses: `events`, `event_outbox`,
   `projection_checkpoints`, `dead_letter_queue`, and `snapshots`.
3. Look at each of those tables so you know what the library is storing and
   why — the core event log, the transactional outbox, projection positions,
   failed events, and aggregate snapshots.
4. Pull a single table's SQL with `get_schema("events")` when you do not want
   all five, and discover what else is on offer with `list_schemas()` and
   `list_backends()`.
5. Wire a `PostgreSQLEventStore` to the new tables and verify it with an
   append followed by a read.
6. Hand schema management to Alembic using `get_alembic_template("all_tables")`,
   substituting `${revision_id}` into a generated migration file.

Along the way you will meet a few options you may want later but do not need
today: `get_schema("events_partitioned")` for high-volume PostgreSQL
deployments, and `get_schema("migration")` for tracking a live event store
migration.

The whole thing takes about twenty minutes. You need Docker running and a
PostgreSQL 12 or newer image; everything else comes from the package.

## Prerequisites

Before you start, make sure the following are in place. If you have been
following the earlier tutorials, only the last two items are new.

**Python 3.11 or newer.** The package declares `requires-python = ">=3.11"`,
so anything older will not install.

**Docker (with Compose).** Step 1 uses the `docker-compose.test.yml` that
ships in the repository to start the database. If you already have a
PostgreSQL server you can point at, you can skip that step — you just need a
database you can create tables in.

**PostgreSQL 12 or newer.** The packaged `events` schema notes 12+ as the
floor; the repository's compose file pins `postgres:15`, which is what this
tutorial was written against. One thing to know if you are on an older
server: the combined schema uses `gen_random_uuid()` for the outbox table's
primary key, which is only built in from PostgreSQL 13 onward. On 12 you
would need `CREATE EXTENSION IF NOT EXISTS pgcrypto;` first. Sticking with 15
avoids the question entirely.

**The PostgreSQL extra installed.** The core package depends only on pydantic
and SQLAlchemy. The async PostgreSQL driver, `asyncpg`, comes from the
`postgresql` extra:

```bash
uv sync --extra postgresql
```

Note the spelling — the extra is `postgresql`, not `postgres`. With pip it is
`pip install "eventsource-py[postgresql]"`. If you would rather install both
SQL backends at once (Tutorial 12 covers SQLite), `--extra all-backends`
pulls in `asyncpg` and `aiosqlite` together.

That gives you the two pieces the store actually talks to:

- **SQLAlchemy 2.x** — a core dependency. `PostgreSQLEventStore` does not
  open its own connections; you hand it an
  `async_sessionmaker[AsyncSession]`, which you build from a SQLAlchemy async
  engine in Step 5.
- **asyncpg** — the driver SQLAlchemy uses behind that engine. It is selected
  by the URL scheme `postgresql+asyncpg://`, not `postgresql://`. Getting
  this wrong is the single most common first error, and it surfaces as
  SQLAlchemy trying to load the synchronous psycopg driver instead.

You can confirm the extra landed before going any further:

```bash
uv run python -c "import asyncpg, sqlalchemy; print(asyncpg.__version__, sqlalchemy.__version__)"
```

If that prints two version numbers, you are ready. If it raises
`ModuleNotFoundError: No module named 'asyncpg'`, the extra did not install —
re-run `uv sync --extra postgresql`.

Finally, you need **a database user that can create tables** — and, if you
apply the combined schema, one permitted to run
`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`, which the `all` schema issues at
the top. On a managed PostgreSQL service that statement may require elevated
privileges; the test container in Step 1 runs as a superuser, so it is a
non-issue locally. Step 4 shows how to pull individual tables if you need to
sidestep the extension line in production.

No prior knowledge of Alembic is assumed. Step 7 introduces it from scratch,
and it is optional — everything up to Step 6 works without it.

## Step 1: Start a PostgreSQL instance

The repository ships a Compose file, `docker-compose.test.yml`, whose whole
job is to give you the databases the integration tests expect. It is a
perfectly good tutorial database too, so use it rather than installing
PostgreSQL onto your machine.

From the repository root, start just the PostgreSQL service:

```bash
docker-compose -f docker-compose.test.yml up -d postgres
```

Naming `postgres` at the end keeps the Redis container out of it — the file
also defines `redis:7`, which you do not need until you reach the event bus
backends. If you leave the name off, both start; that is harmless, just
slower.

The service the file defines looks like this:

```yaml
services:
  postgres:
    image: postgres:15
    container_name: eventsource-test-postgres
    environment:
      POSTGRES_DB: eventsource_test
      POSTGRES_USER: test
      POSTGRES_PASSWORD: test
    ports:
      - "${POSTGRES_PORT:-5433}:5432"
```

Four things there matter for the rest of the tutorial:

- **The image is `postgres:15`.** That is comfortably past the 12+ floor and
  past the 13 cutoff for built-in `gen_random_uuid()`, so the packaged schema
  applies without any extension juggling.
- **The database is `eventsource_test`, and the user and password are both
  `test`.** These are throwaway credentials for a local container. Do not
  reuse this pattern anywhere real.
- **The published port is 5433, not 5432.** The container listens on 5432
  internally, but Compose maps it to 5433 on your host, deliberately, so it
  does not collide with a PostgreSQL you may already be running. Every
  connection string in this tutorial uses 5433. Override it by setting
  `POSTGRES_PORT` before bringing the stack up if 5433 is taken.
- **The data lives in a named volume,** `postgres_test_data`. That is what
  makes the durability check in Step 6 meaningful: the container can stop and
  start and your events are still there.

Put the connection details somewhere you can reuse them:

```bash
export DATABASE_URL="postgresql+asyncpg://test:test@localhost:5433/eventsource_test"
```

Note the `+asyncpg` again. This URL is for SQLAlchemy, and it is the one you
will pass to `create_async_engine` in Step 5.

### Wait for it to be ready

`up -d` returns as soon as the container is *created*, which is a moment or
two before PostgreSQL is *accepting connections*. Connecting too early gives
you a confusing `ConnectionRefusedError` on your very first command. The
Compose file defines a healthcheck (`pg_isready -U test -d eventsource_test`,
polled every 5 seconds), so you can just watch for it to flip to healthy:

```bash
docker-compose -f docker-compose.test.yml ps
```

Wait until the `STATUS` column for `eventsource-test-postgres` reads
`Up (healthy)` rather than `Up (health: starting)`. On a first run, when the
image still has to be pulled and the data directory initialised, this can take
ten or twenty seconds.

### Confirm you can actually connect

Before adding EventSource to the picture, prove that Python can reach the
database. This uses only what the `postgresql` extra already installed:

```python
import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

DATABASE_URL = "postgresql+asyncpg://test:test@localhost:5433/eventsource_test"


async def main() -> None:
    engine = create_async_engine(DATABASE_URL)
    async with engine.connect() as conn:
        result = await conn.execute(text("SELECT version()"))
        print(result.scalar_one())
    await engine.dispose()


asyncio.run(main())
```

Save it as `check_db.py` and run `uv run python check_db.py`. You should see
something like:

```
PostgreSQL 15.x on x86_64-pc-linux-gnu, compiled by gcc ...
```

Keep this engine-creation shape in mind — Step 2 reuses it almost verbatim,
swapping `engine.connect()` for `engine.begin()` and the `SELECT version()`
for the packaged schema SQL.

If instead you get `ConnectionRefusedError` or `[Errno 111] Connect call
failed`, the container is not up yet or the port is wrong: re-check
`docker-compose ... ps` and confirm the mapping says `0.0.0.0:5433->5432/tcp`.
If you get `InvalidPasswordError`, you likely have an *older*
`eventsource-test-postgres` volume from a previous project with different
credentials — `docker-compose -f docker-compose.test.yml down -v` removes the
volume so the next `up` re-initialises it. Note that `-v` throws the data
away, which is exactly what you want right now and exactly what you do not
want after Step 6.

The database is running and empty — no tables at all. Step 2 fills it.

## Step 2: Load the packaged schema with `get_all_schemas()`

The SQL that creates EventSource's tables is installed alongside the Python
code, in `eventsource/migrations/`. You never author it and you never copy it;
you ask the library for it as a string and execute that string.

Start by looking at what you are about to run:

```bash
uv run python -c "from eventsource.migrations import get_all_schemas; print(get_all_schemas())" | head -30
```

You should see a comment banner, then:

```sql
-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";  -- For UUID generation

BEGIN;
```

That is the shape of the whole file: one extension statement, then a
transaction containing five `CREATE TABLE IF NOT EXISTS` blocks and their
indexes, then `COMMIT`. `get_all_schemas()` takes a `backend` argument that
defaults to `"postgresql"`, so the bare call is the PostgreSQL combined
schema. (Tutorial 12 uses `get_all_schemas(backend="sqlite")`, which returns
the SQLite equivalent.)

Two details in that header matter:

- **The `CREATE EXTENSION` line sits *outside* the `BEGIN;`.** If your role
  cannot create extensions, that one statement fails on its own and nothing
  else has been applied yet. Your `test` superuser in the container can, so
  this will not bite you today.
- **Everything is `IF NOT EXISTS` guarded.** Applying the schema twice is a
  no-op, not an error. That matters more than it sounds: you can re-run Step 2
  freely while you are experimenting.

### Why not `psql -f` a repo path or `wget` from GitHub

It is tempting to find `all.sql` in a checkout and feed it to `psql`, or to
fetch it from the project's GitHub page. Both work exactly once, on your
laptop, and then rot.

The problem is that the SQL and the code that reads the tables are versioned
together. `PostgreSQLEventStore` expects a `global_position BIGSERIAL`, a
unique index on `(aggregate_id, aggregate_type, version)`, and a `JSONB`
payload column. When a future release changes any of that, the packaged SQL
changes in the same release you installed. A repo path points at whatever
branch happens to be checked out; a GitHub URL points at whatever `main` looks
like today. Neither is pinned to the version in your virtualenv, and the
mismatch surfaces later as a confusing runtime error rather than an install
failure.

Asking the installed package removes the question. Whatever
`uv sync` resolved is what `get_all_schemas()` returns. It also means the SQL
is available inside a container image with no repository checkout, which is
where you will actually need it.

If you do want the file on disk — to hand to `psql -f`, or to commit into a
provisioning script — resolve its real location rather than guessing at
site-packages. For a single table, the package hands you the path directly:

```bash
uv run python -c "from eventsource.migrations import get_template_path; print(get_template_path('events'))"
```

The combined schema is the one exception: `get_template_path` looks in the
package's `templates/` directory, and `all.sql` lives next door in
`schemas/`, so `get_template_path("all")` raises `FileNotFoundError`. Resolve
that one through `importlib.resources`:

```bash
uv run python -c "from importlib.resources import files; print(files('eventsource.migrations').joinpath('schemas/all.sql'))"
```

Both print a path inside your virtualenv, which is the point — it is the SQL
belonging to the version you installed, not to a branch. For this tutorial,
though, stay in Python and let `get_all_schemas()` hand you the string.

### Applying the SQL

Here is the part to be careful about. The combined schema is a *multi-statement
script*, and SQLAlchemy's asyncpg dialect sends statements through asyncpg's
prepared-statement path, which accepts exactly one command at a time. Passing
the whole script to `conn.execute(text(...))` raises:

```
asyncpg.exceptions.PostgresSyntaxError: cannot insert multiple commands into a prepared statement
```

The fix is to reach past the dialect to the raw asyncpg connection, whose
`execute()` does accept a script. This is the same technique the library's own
integration tests use to apply packaged schemas (see
`tests/integration/migrations/test_migration_schema_postgresql.py`):

```python
import asyncio

from sqlalchemy.ext.asyncio import create_async_engine

from eventsource.migrations import get_all_schemas

DATABASE_URL = "postgresql+asyncpg://test:test@localhost:5433/eventsource_test"


async def main() -> None:
    engine = create_async_engine(DATABASE_URL)
    schema_sql = get_all_schemas()

    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        await raw_conn.driver_connection.execute(schema_sql)

    print("schema applied")
    await engine.dispose()


asyncio.run(main())
```

Save that as `apply_schema.py` and run it:

```bash
uv run python apply_schema.py
```

It should print `schema applied` and nothing else. Run it a second time — it
should print the same thing, because of the `IF NOT EXISTS` guards.

Note the shape compared with Step 1's connection check: `engine.begin()`
instead of `engine.connect()`, because this writes; and the script's own
`BEGIN; ... COMMIT;` handles the DDL transaction internally.

If you prefer to keep Python out of the deployment path entirely, the
equivalent one-liner pipes the same string into `psql`:

```bash
uv run python -c "from eventsource.migrations import get_all_schemas; print(get_all_schemas())" \
  | psql "postgresql://test:test@localhost:5433/eventsource_test" -v ON_ERROR_STOP=1 -f -
```

Note the URL has no `+asyncpg` there — that suffix is SQLAlchemy's way of
choosing a driver, and `psql` does not understand it. `-v ON_ERROR_STOP=1`
matters because the script is wrapped in a transaction: without it, one failed
statement leaves `psql` cheerfully printing "current transaction is aborted"
for every remaining line.

### Confirm the tables exist

The bottom of `all.sql` carries a commented-out verification query. Run it for
real:

```python
import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

DATABASE_URL = "postgresql+asyncpg://test:test@localhost:5433/eventsource_test"

QUERY = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name
"""


async def main() -> None:
    engine = create_async_engine(DATABASE_URL)
    async with engine.connect() as conn:
        result = await conn.execute(text(QUERY))
        for (name,) in result:
            print(name)
    await engine.dispose()


asyncio.run(main())
```

Expected output:

```
dead_letter_queue
event_outbox
events
projection_checkpoints
snapshots
```

Five tables, in alphabetical order. That is a single statement with no
parameters, so plain `conn.execute(text(...))` is fine here — the
multi-statement restriction only applied to the schema script.

If a table is missing, the most likely cause is that you ran the SQL against a
different database than the one you are querying: check that both connection
strings say port 5433 and database `eventsource_test`.

Step 3 walks through what each of these five tables is for.

## Step 3: What the schema creates

You now have five tables and none of them are yours. Before you write to any
of them, it is worth spending a few minutes on what each one holds — partly so
the columns are not a surprise later, and partly because two of the five are
optional in the sense that you can run for a long time without ever looking at
them.

If you want to follow along in the database, `psql` into the container:

```bash
docker exec -it eventsource-test-postgres psql -U test -d eventsource_test
```

and use `\d <table>` on each table as you read. The rest of this step assumes
you have that open, but you can just read it.

### `events` — the core event store

This is the one that matters. Everything else in the library exists to support
it.

```sql
CREATE TABLE IF NOT EXISTS events (
    global_position BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    tenant_id UUID,
    actor_id VARCHAR(255),
    version INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_events_aggregate_version
        UNIQUE (aggregate_id, aggregate_type, version)
);
```

Read it as three groups.

**Ordering.** `global_position` is a `BIGSERIAL`: a single, database-assigned,
monotonically increasing number across every event in the store, regardless of
which aggregate produced it. This is what a subscription means when it says
"resume from position 4711". `version`, by contrast, is per-aggregate — the
1, 2, 3 sequence you saw on your in-memory aggregates in earlier tutorials.
Two events from different aggregates can share a `version` but never a
`global_position`.

**Identity and concurrency.** `event_id` is unique, so a retried append cannot
silently duplicate a row. The more interesting constraint is
`uq_events_aggregate_version`: at most one event per
`(aggregate_id, aggregate_type, version)`. That single unique index *is* the
optimistic locking you have been relying on. When two writers both load an
aggregate at version 7 and both try to append version 8, PostgreSQL rejects
the second insert, and `PostgreSQLEventStore` turns that rejection into the
`OptimisticLockError` you already know. Nothing in Python is holding a lock;
the constraint does the work.

**The event itself.** `event_type` is the registered name your `DomainEvent`
subclass carries, and `payload` is its fields as `JSONB` — the whole event
body in one column, which is why adding a field to an event class needs no
migration. `timestamp` is when the event happened (your event's own value);
`created_at` is when the row landed, defaulted by the database. They are
usually within milliseconds of each other and occasionally are not, which is
the point of keeping both.

`tenant_id` and `actor_id` are nullable and stay `NULL` unless you are using
them. `tenant_id` is populated by `TenantDomainEvent` (Tutorial 16);
`actor_id` records who caused the event. Both are written on every append
regardless, so you can start using them later without touching the schema.

The indexes follow the queries the store actually issues: lookups by
`aggregate_id` when rehydrating an aggregate, by `event_type` and
`aggregate_type` when a projection subscribes to a slice of the stream, by
`timestamp` for time-range reads, and a partial index on `tenant_id` that only
covers rows where it is non-null — no cost for single-tenant deployments.

### `event_outbox` — transactional outbox

The problem this solves: you have appended an event to `events` and now you
want to publish it to your event bus. If you publish and then the transaction
rolls back, you have announced something that never happened. If you commit
and then the publish fails, subscribers never hear about it. There is no
ordering of those two operations that is safe, because one of them is not
transactional.

The outbox makes the publish transactional by turning it into a second insert
in the *same* transaction:

```sql
CREATE TABLE IF NOT EXISTS event_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    tenant_id UUID,
    event_data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP WITH TIME ZONE,
    retry_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    CONSTRAINT chk_outbox_status CHECK (status IN ('pending', 'published', 'failed'))
);
```

Rows arrive as `pending`. A separate relay process reads pending rows oldest
first, publishes each to the bus, and marks it `published` with a
`published_at`. Failures bump `retry_count` and record `last_error`; after
enough of them the row goes to `failed`, where a human can find it. The
`CHECK` constraint means those three values are the only states that can ever
exist — the database will not let a typo'd status through.

Notice how narrow the indexes are. `idx_outbox_pending` covers only
`created_at`, and only `WHERE status = 'pending'`. That is deliberate: the
relay's hot query is "oldest unpublished rows", and a partial index on that
predicate stays small even when the table has millions of published rows
awaiting cleanup. `idx_outbox_failed` and `idx_outbox_published_at` are the
same trick for the other two states.

This is also the one table whose primary key default, `gen_random_uuid()`,
needs PostgreSQL 13 or newer — the point from the prerequisites. On `postgres:15`
it is built in.

You do not have to use the outbox. If you publish directly to an in-process
bus you can leave the table empty forever; it costs you nothing but the
`CREATE TABLE`.

### `projection_checkpoints` — projection position tracking

A projection needs to know where it stopped so that a restart resumes rather
than replays from zero. That bookmark lives here, one row per projection:

```sql
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name VARCHAR(255) PRIMARY KEY,
    last_event_id UUID,
    last_event_type VARCHAR(255),
    last_processed_at TIMESTAMP WITH TIME ZONE,
    global_position BIGINT,
    events_processed BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
```

`projection_name` is the primary key, which is the whole design in one line:
a projection is identified by its name, and it has exactly one position. Pick
those names carefully and do not rename a live projection unless you intend it
to start over.

`global_position` is the field that does the resuming — it points into the
`events` table's `BIGSERIAL`, so restarting means "read everything after this
number". `last_event_id` and `last_event_type` are there for operators
debugging a stuck projection; `events_processed` is a running count that makes
"is this thing moving?" answerable with one query.

`updated_at` maintains itself. The schema installs a trigger function,
`update_checkpoint_timestamp()`, wired to a `BEFORE UPDATE` trigger on the
table, so every checkpoint write refreshes the timestamp without the
application setting it. This is the only piece of procedural code the schema
creates — if you ever wonder why `\df` shows a function you did not write,
that is it.

To find a lagging projection, compare its checkpoint against the head of the
log:

```sql
SELECT c.projection_name,
       c.global_position,
       (SELECT MAX(global_position) FROM events) - c.global_position AS lag
FROM projection_checkpoints c;
```

### `dead_letter_queue` — failed event storage

Some events cannot be processed no matter how many times you try — a
malformed payload, a bug in a handler, a downstream service that is gone. If a
projection retries such an event forever, it stops making progress on
everything behind it. The dead letter queue is the escape hatch: park the
event, move on, deal with it later.

```sql
CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL,
    projection_name VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    event_data JSONB NOT NULL,
    error_message TEXT NOT NULL,
    error_stacktrace TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    first_failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    last_failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    status VARCHAR(20) NOT NULL DEFAULT 'failed',
    resolved_at TIMESTAMP WITH TIME ZONE,
    resolved_by VARCHAR(255),
    CONSTRAINT chk_dlq_status CHECK (status IN ('failed', 'retrying', 'resolved')),
    CONSTRAINT uq_dlq_event_projection UNIQUE (event_id, projection_name)
);
```

The uniqueness is on `(event_id, projection_name)`, not on `event_id` alone,
and that pairing is the important detail. One bad event can fail in three
different projections, and each of those is a separate problem with a separate
error and a separate resolution. Conversely, the same event failing twice in
the same projection updates the existing row — `retry_count` and
`last_failed_at` climb while `first_failed_at` stays put, so you can see both
how long a failure has been going and how hard it has been retried.

`error_message` is required and `error_stacktrace` is optional, so a row is
never silently useless. `resolved_at` and `resolved_by` are the audit trail
for the fix: someone reprocessed it, and here is who and when.

The full event body is copied into `event_data`, which means the DLQ is
self-contained — you can inspect and replay from it without joining back to
`events`.

An empty `dead_letter_queue` is the healthy state. A non-empty one is a
monitoring alert:

```sql
SELECT projection_name, event_type, COUNT(*)
FROM dead_letter_queue
WHERE status IN ('failed', 'retrying')
GROUP BY projection_name, event_type;
```

### `snapshots` — aggregate snapshots for fast loading

Rehydrating an aggregate means replaying every one of its events. At twenty
events that is instant; at twenty thousand it is not. A snapshot stores the
aggregate's state at a version so that loading becomes "read the snapshot,
then replay only what came after".

```sql
CREATE TABLE IF NOT EXISTS snapshots (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    version INTEGER NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    state JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_snapshots_aggregate UNIQUE (aggregate_id, aggregate_type)
);
```

The constraint to read carefully is `uq_snapshots_aggregate`: unique on
`(aggregate_id, aggregate_type)` — *not* including `version`. This table keeps
one snapshot per aggregate, the latest. Writing a new snapshot replaces the
old one rather than accumulating history. That is a deliberate trade: you
cannot use this table to inspect an aggregate as of last Tuesday, but you also
never have to write a cleanup job. History lives in `events`, which is
authoritative; snapshots are a cache and can be deleted wholesale without
losing anything but speed.

`schema_version` exists because a snapshot is a serialization of your
aggregate's internal state, and that shape changes when you refactor. Bumping
`schema_version` lets the loader recognise that a stored snapshot predates the
current layout and fall back to a full replay instead of deserializing
nonsense into your aggregate. This is the one column here that will save you
from a genuinely nasty production bug.

Like the outbox, snapshots are opt-in. The table sits empty until you
configure a snapshot store; Tutorial 14 covers when that is worth doing.

### Taking stock

Of the five tables, exactly one is mandatory. `events` is the system of
record — delete it and your data is gone. The other four are derived or
operational: `event_outbox` is in-flight publishing state,
`projection_checkpoints` is bookmarks, `dead_letter_queue` is a problem
inbox, and `snapshots` is a cache. You could truncate all four and rebuild
everything from `events`, which is the property that makes event sourcing
worth the trouble in the first place.

Step 4 shows how to install these tables individually, in case you do not want
all five.
