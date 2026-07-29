# Schema Design

Event sourcing puts an unusual amount of weight on the database schema. In a
CRUD system the tables are an implementation detail that can be reshaped at
will; here they are the system of record, the ordering authority, the
concurrency arbiter, and the contract that every replay, projection rebuild and
tenant migration depends on. A column added carelessly is inconvenient. A
primary key chosen carelessly is a rewrite.

This page explains *why* the schemas shipped in `eventsource.migrations` look
the way they do — why the events table carries two identities rather than one,
why optimistic locking is a unique constraint instead of application logic, why
the payload is opaque JSON, and what each of those choices costs. It is not a
setup guide and not a column-by-column reference; it is the reasoning behind the
DDL, so that when you extend it, partition it, or port it to another backend you
know which properties are load-bearing and which are conveniences.

Throughout, the schemas under discussion are the ones you get from
`get_schema()` and `get_all_schemas()`: `events`, `events_partitioned`,
`outbox`, `checkpoints`, `dlq`, `snapshots`, and `migration`, in their
PostgreSQL and SQLite forms.

## Why this page exists

The library has carried an in-tree design note at
`src/eventsource/migrations/SCHEMA_DESIGN.md` since the schemas were first
written. It sat next to the SQL, which made it easy to update while writing DDL
and easy to forget afterwards — and it drifted. It still describes the events
table as having `event_id` for a primary key, while
`src/eventsource/migrations/schemas/events.sql` has used
`global_position BIGSERIAL PRIMARY KEY` with `event_id UUID NOT NULL UNIQUE`
for some time. That is not a cosmetic difference: it is the difference between
a store that can be replayed in a total order and one that cannot. A design
document that is wrong about the primary key is worse than no document, because
readers trust it.

This page is the published counterpart of that note, and the one that is kept
current. The in-tree file becomes a pointer here. Keeping the prose in `docs/`
means it is built, reviewed and cross-linked alongside the rest of the
documentation rather than reviewed only by whoever happens to be editing SQL.

The split of responsibilities is deliberate:

- **The SQL files** in `src/eventsource/migrations/schemas/` (and
  `templates/`, `updates/`) are the authority on *what* the schema is. If this
  page and a `.sql` file disagree, the `.sql` file is right and this page is a
  bug.
- **The reference documentation** covers the API surface — `get_schema()`,
  `get_all_schemas()`, `get_alembic_template()` — and the per-backend store
  implementations.
- **This page** covers *why*: the constraints that are load-bearing, the ones
  that are convenience, and the costs each choice imposes on operations.

Read it before you change the DDL, before you add an index, and before you
assume a constraint is safe to relax. Most of what follows exists because some
property of the system — total replay order, optimistic locking, at-least-once
delivery — depends on it in a way that is not visible from the DDL alone.

## Scope

This page covers the SQL that ships inside the library — the seven schema names
exposed by `eventsource.migrations` and nothing else:

| Schema name | Objects it creates | Backends |
| --- | --- | --- |
| `events` | `events` (non-partitioned) plus its indexes | postgresql, sqlite |
| `events_partitioned` | `events` declaratively partitioned by `timestamp`, the `events_global_position_seq` sequence, monthly partitions and lifecycle helpers | postgresql |
| `outbox` | `event_outbox` | postgresql, sqlite |
| `checkpoints` | `projection_checkpoints` | postgresql, sqlite |
| `dlq` | `dead_letter_queue` | postgresql, sqlite |
| `snapshots` | `snapshots` | postgresql, sqlite |
| `migration` | `tenant_migrations`, `tenant_routing`, `migration_position_mappings`, `migration_audit_log` | postgresql |

These names are not documentation labels; they are the values of the
`SchemaName` literal in `src/eventsource/migrations/__init__.py`, and they are
what you pass to `get_schema(name, backend=...)`. There is one further name,
`"all"`, which is not a table but a pre-combined file — and its contents are
narrower than the list above, which is a distinction worth holding onto:
`schemas/all.sql` bundles the *non-partitioned* `events`, `event_outbox`,
`projection_checkpoints`, `dead_letter_queue` and `snapshots`. It does not
include `events_partitioned` (which defines a conflicting `events` table) and
it does not include the tenant-migration tables (which most deployments never
need). `get_all_schemas()` is the quick path to a working store, not a
superset.

Two backends are in scope, because two backends are supported:
`postgresql` and `sqlite`. PostgreSQL is the production target and carries the
full set — partitioning, partial indexes, JSONB, triggers, the migration
tables. SQLite exists for tests and small single-process deployments and
carries five of the seven schemas; `events_partitioned` and `migration` have
no SQLite form at all. That gap is expressed in the layout rather than in a
compatibility shim: PostgreSQL templates live directly in
`src/eventsource/migrations/templates/`, SQLite templates in
`templates/sqlite/`, and a schema simply has no file under the backend that
does not support it. Asking for one you do not get a degraded substitute; you
get a `ValueError` naming what is available.

Also in scope, though it sits slightly outside the `get_schema()` surface, is
`src/eventsource/migrations/updates/` — currently the pair of
`001_add_global_position` files that retrofit position-based resume onto an
existing `projection_checkpoints` table. New installs get that column from the
shipped template already; the update files exist for databases created before
it landed. They explain the append-only convention discussed later on this
page, so they are discussed here too.

Deliberately **out of scope**:

- **Your tables.** Read-model and projection tables are yours to design. The
  library tracks *where* a projection is (`projection_checkpoints`) and *what
  broke* (`dead_letter_queue`); it has no opinion about the shape of what the
  projection writes.
- **Non-SQL backends.** The in-memory and Redis implementations have no DDL.
  Nothing on this page constrains them, though the invariants — total order,
  per-aggregate versioning, event identity — are the same ones their
  conformance tests assert.
- **Advisory locks.** Distributed locking uses PostgreSQL advisory locks, which
  are a runtime facility, not a table. There is no schema to describe.
- **Runtime behaviour.** How the outbox poller drains rows, how subscriptions
  retry, how a tenant migration cuts over — those belong to the components
  that use these tables. This page stops at the storage shape and the
  invariants the DDL enforces.

## Design goals

Four properties shape every table on this page. None of them is fully
achievable by DDL alone, and being precise about where the schema's
responsibility ends is the point of this section: a constraint the database
enforces is a guarantee, a convention the application upholds is a risk, and
the two are easy to confuse when you are reading SQL.

### Immutability

Events are facts. Once `events` has a row, that row is not meant to change and
not meant to disappear — replay must produce the same aggregate state today
that it produced last month, and an audit trail that can be edited is not an
audit trail.

The schema leans into that shape. `events` has no `updated_at`, no soft-delete
flag, no status column, nothing that implies a lifecycle. Contrast it with
`event_outbox`, `projection_checkpoints` and `dead_letter_queue`, which all
carry mutable status or counters and, in the checkpoints case, a
`BEFORE UPDATE` trigger to maintain `updated_at`. The absence of those columns
in `events` is the design statement. The library reinforces it in Python:
`DomainEvent` sets `model_config = ConfigDict(frozen=True)`, so an event
instance cannot be mutated after construction.

What the schema does *not* do is enforce it. There is no rule, no trigger, and
no `REVOKE` in `templates/events.sql` that stops `UPDATE events SET payload =
...` or a `DELETE`. Immutability here is a discipline backed by the absence of
any API that would violate it, not a database-level prohibition. If you need it
enforced — and in a regulated environment you probably do — that is a
deployment-level concern: grant the application role `SELECT, INSERT` on
`events` and withhold `UPDATE, DELETE`. The schema is compatible with that
grant; it does not apply it for you.

The one place this collides with reality is data protection, since "never
delete" and "erase this subject's data on request" are direct opposites. That
tension is real, has no clean schema-level answer, and is taken up later on
this page.

### Ordered replay

Replay only means something if it has an order, and the order has to be one the
database can hand back deterministically. That requirement is why `events`
carries two identities rather than one. `global_position BIGSERIAL PRIMARY KEY`
is the total order across every stream; `version INTEGER` under
`UNIQUE (aggregate_id, aggregate_type, version)` is the per-aggregate order.
The first is what a subscription resumes from; the second is what an aggregate
replays along.

Neither of those is `timestamp`. The `timestamp` column is domain time — when
the event happened in the business sense — and domain time is supplied by the
writer, can be backdated, can arrive out of order under clock skew, and can
collide exactly between two events. It is indexed and useful for queries and
auditing. It is not an ordering authority, and a checkpoint stored as a
timestamp would silently skip events written by a lagging clock. This is why
`projection_checkpoints` has a `global_position BIGINT` column at all, and why
resume is expressed as "everything after position N".

The per-aggregate half of the ordering is where the *write* side gets its
guarantee, and the design decision worth noticing is that the guarantee is
delegated to the database. The unique constraint on
`(aggregate_id, aggregate_type, version)` means two concurrent writers who both
believe an aggregate is at version 7 cannot both write version 8; one of them
gets a constraint violation. `PostgreSQLEventStore.append_events` catches the
`IntegrityError`, matches `uq_events_aggregate_version` in the message,
re-reads `MAX(version)` for the aggregate and raises `OptimisticLockError` with
the actual version. The SQLite store does the same against
`aiosqlite.IntegrityError`. The application never has to hold a lock or
serialize writers to be correct — the concurrency check *is* the constraint,
and correctness survives multiple processes, restarts and races it never sees.

`event_id UUID NOT NULL UNIQUE` is the third identity, and it is separate from
both orders. It is client-generated, travels with the event through the bus and
into the outbox, DLQ and checkpoints, and gives consumers a stable key to
deduplicate on. Note what its uniqueness does and does not do on the write
path: re-appending an already-stored event raises rather than silently
succeeding — the store has no `ON CONFLICT (event_id) DO NOTHING`. It is a
guard against double-writes, not an upsert.

### At-least-once delivery with idempotent handlers

The delivery guarantee is at-least-once, deliberately, and everything
downstream of the event store is designed on the assumption that handlers will
see duplicates.

Exactly-once across a database and a message broker requires distributed
transactions, which is a cost most systems should not pay. The library takes
the standard trade instead: `event_outbox` is written in the *same transaction*
as the event, so a state change and the intent to publish it commit or fail
together. A background poller then drains pending rows —
`get_pending_outbox_events()` selects `WHERE status = 'pending'` ordered by
`created_at` with `FOR UPDATE SKIP LOCKED`, so concurrent workers do not
collide — and marks them published afterwards. If the process dies between
publishing to the broker and marking the row published, the row is still
pending and the event is published again. That is not a bug in the design; it
is the design. `retry_count` and `last_error` on the outbox row exist because
publishing is expected to fail sometimes and be retried.

The consequence is a contract with your code: **event handlers must be
idempotent.** Processing the same event twice must produce the same result as
processing it once. The schema helps where it can — `event_id` is a stable
deduplication key, `projection_checkpoints` records where a projection got to,
and `dead_letter_queue` has `UNIQUE (event_id, projection_name)` so a repeated
failure updates one row rather than accumulating many — but no arrangement of
tables can make a non-idempotent handler safe. "Exactly-once processing" is a
property of idempotent handlers *plus* checkpoints, never of the schema alone.

The at-least-once choice also explains two operational obligations that appear
later: `event_outbox` and `dead_letter_queue` are working tables that grow
without bound unless something prunes them, which is why the shipped SQL
includes `cleanup_published_outbox_events()` and `cleanup_resolved_dlq()`
rather than leaving cleanup entirely to you.

### Optional multi-tenancy

One schema serves both single-tenant and multi-tenant deployments. There is no
`events_multitenant.sql`, no build flag, no separate template. The mechanism is
a nullable column: `tenant_id UUID` on `events` and on `event_outbox`, left
`NULL` when you have no tenants and populated when you do.

Nullable rather than required, because requiring it would tax every
single-tenant install with a column it must invent a value for, and because
`NULL` is an honest representation of "this deployment has no tenant concept".
`actor_id VARCHAR(255)` is nullable for the same reason — some events are
caused by a system or a scheduler, not a user.

The cost is paid in indexing, and the schema is explicit about it. Multi-tenant
lookups use a *partial* index, `idx_events_tenant_id ... WHERE tenant_id IS NOT
NULL`, so a single-tenant install with a hundred million all-`NULL` rows does
not carry a hundred million useless index entries. The same partial-index
pattern appears on `event_outbox.tenant_id`. That is the general principle
behind "optional" here: optional features should be free when unused, and the
place that shows up is index size and write amplification rather than the table
definition.

Where the DDL stops is filtering. Nothing in these schemas prevents a query
from reading across tenants — there is no row-level security policy, no
mandatory predicate, no foreign key to a tenants table. Tenant isolation is an
application obligation, upheld at runtime by the `contextvars`-based tenant
context, `TenantDomainEvent` (which makes `tenant_id` required rather than
optional at the event level), and the tenant-scoped repository, which validates
that every uncommitted event carries the expected `tenant_id` before it saves
and raises `TenantMismatchError` otherwise. The column makes isolation
*possible* and makes it auditable after the fact; the runtime layer is what
makes it happen.

## The events table

Everything else in this library is derived data. Projections can be rebuilt,
snapshots discarded, checkpoints reset, outbox rows drained and deleted. The
`events` table cannot be regenerated from anything, so its shape is the one
place where a wrong decision is permanent. Eleven columns, one primary key, one
unique constraint, and one additional unique index carry the whole design.

### Two identities, two jobs

The table has two independent identifiers, and confusing them is the most
common way to get event sourcing wrong:

```sql
global_position BIGSERIAL PRIMARY KEY,
event_id        UUID NOT NULL UNIQUE,
```

`global_position` is **assigned by the database at insert time**, from a
sequence, in insertion order. It answers "where am I in the stream of everything?" — it is a
monotonic cursor over the whole store, and it is the primary key because that
is also the order the table is most usefully clustered and scanned in.
Subscriptions resume from it, `get_global_position()` reports the current high
water mark, and `append_events` returns the last one it wrote so a caller knows
where its own writes landed.

`event_id` is **assigned by the client, before the write** — it is
`uuid4()` by default on `DomainEvent`. It answers "which event is this?" and it
is the identity that survives leaving the database: it travels through the bus,
into `event_outbox`, into `dead_letter_queue`, and into whatever deduplication
table a downstream consumer keeps. It is `UNIQUE` but not the primary key,
because a UUID makes a poor ordering key and no ordering at all.

That separation is what lets the store be idempotent on append without being
lossy on order. `PostgreSQLEventStore._do_append_events` checks
`SELECT 1 FROM events WHERE event_id = :event_id` before each insert and skips
events that are already stored, so a retried append does not duplicate or
error. The `UNIQUE` constraint on `event_id` is the backstop for the race that
check cannot cover — two processes appending the same event concurrently — and
means the invariant holds even when the pre-check loses.

A single identifier cannot do both jobs. A UUID primary key gives identity but
no replay order. A bare sequence gives order but no identity that means
anything outside this one database — restore into a new database and the
positions change, while the `event_id` values do not.

### Why global_position exists at all

The obvious alternative to a sequence is `timestamp`, and it does not work.

`timestamp` in this schema is **domain time**: `append_events` writes
`event.occurred_at` into it, which is a value the application supplies. It can
be backdated deliberately when importing history. It can go backwards under
clock skew between two writers. Two events can share it exactly. None of that
is a defect — domain time is supposed to describe when something happened in
the business, not when a row was inserted — but every one of those properties
breaks a cursor.

Consider a projection that has processed everything up to `12:00:00` and stores
that timestamp. A writer with a slightly slow clock commits an event stamped
`11:59:59` a moment later. The projection resumes "after 12:00:00" and never
sees it. Nothing errors; the read model is just quietly wrong, and the only way
to discover it is to notice the missing data. With `global_position` the same
event gets a higher position than everything already committed, and "everything
after position N" cannot skip it.

This is why `projection_checkpoints` grew a `global_position BIGINT` column,
and why resume is expressed in positions throughout. The checkpoint repository
exposes exactly that pair — `save_position(name, position)` and
`get_position(name)` — and the subscription machinery uses them: the manager
saves a position after processing, and transition logic reads it back to decide
where a catch-up subscription should start. The store side matches: reading
forward is `WHERE global_position > :from_position ... ORDER BY global_position
ASC`. Nothing in that loop consults a clock.

The distinction is not that timestamps are useless — it is that they are a
*query* facility rather than a *resume* facility. `get_events_by_type()` takes
a `from_timestamp` and orders by `timestamp ASC`, which is the right shape for
"show me what happened to Orders since Tuesday" and the wrong shape for a
cursor, precisely because a later-arriving backdated event would never be
returned to a caller that had already advanced past it. Timestamp filters are
also what enable partition pruning on `events_partitioned`, so they earn their
place — just not as the thing a consumer stores.

The `events` table does also keep `created_at TIMESTAMPTZ NOT NULL DEFAULT
NOW()`, which is wall-clock insert time — useful for auditing the gap between
when something happened and when it was recorded, and specifically *not* used
for ordering either. Two timestamp columns, neither an ordering authority, is a
deliberate arrangement: the moment one of them becomes a cursor, the guarantee
above is gone.

One caveat worth knowing before you build tooling on it: `global_position`
values are allocated from a sequence, so they are monotonic but not gap-free
(rolled-back transactions burn values), and under concurrent writers a
transaction that grabs a lower position may commit after one with a higher
position. A reader polling the tail can therefore see position 105 before 104
becomes visible. For catch-up reads over committed history — the case the
checkpoints are for — this does not arise.

### Why the unique constraint is the optimistic lock

```sql
CONSTRAINT uq_events_aggregate_version
    UNIQUE (aggregate_id, aggregate_type, version)
```

This is the single most important line in the schema. It is not a data-quality
check; it is the concurrency control mechanism for the entire library. The
database — not the application — arbitrates concurrent appends to an aggregate.

The append path *starts* in Python. `_do_append_events` reads
`SELECT COALESCE(MAX(version), 0)` for the `(aggregate_id, aggregate_type)`
pair, compares the result against the caller's `expected_version`, and raises
`OptimisticLockError(aggregate_id, expected_version, current_version)` on
mismatch. The sentinels in `ExpectedVersion` shape that comparison:
`NO_STREAM` requires the current version to be 0, `STREAM_EXISTS` requires it
not to be, and `ANY` skips the check entirely. It is a useful check — it fails
fast, before any insert, with a clear error — but on its own it is a textbook
read-then-write race. Nothing between the `SELECT` and the `INSERT` locks the
aggregate: no `SELECT ... FOR UPDATE`, no advisory lock, no serializable
isolation requirement. Two processes can both read version 7, both conclude
they may write version 8, and both proceed. Under `ExpectedVersion.ANY` there
is no application-side check at all.

The constraint is what makes that safe. One insert of version 8 commits; the
other violates `uq_events_aggregate_version` and the driver raises
`IntegrityError`. `_do_append_events` catches it, rolls the transaction back,
confirms the failure was this constraint by matching
`"uq_events_aggregate_version"` against the lowercased exception text, re-reads
the actual `MAX(version)`, and raises `OptimisticLockError` with the true
current version. An `IntegrityError` from anything else — a duplicate
`event_id`, say — is re-raised untouched.

The result is that both paths converge on the same exception. A caller cannot
tell whether its conflict was caught by the pre-check or by the constraint, and
does not need to: it reloads the aggregate and retries either way. The
pre-check is an optimisation that avoids a doomed insert in the common
uncontended case; the constraint is the guarantee.

`SQLiteEventStore.append_events` follows the same structure against
`aiosqlite.IntegrityError`, with one difference worth knowing: the SQLite
schema declares the constraint inline and unnamed —
`UNIQUE (aggregate_id, aggregate_type, version)` — so there is no name to match
on. The store instead checks that the lowercased message contains `"unique"`
along with `"aggregate_id"` or `"version"`, which is what SQLite's
`UNIQUE constraint failed: events.aggregate_id, events.aggregate_type,
events.version` produces. It is a string heuristic over an error message, and
it is broader than the PostgreSQL match — a unique violation on some *other*
column named `version` in a future table would be misclassified. Nothing today
triggers that, but it is a reason to prefer PostgreSQL where concurrency
actually matters.

The consequence of pushing the check into the database is that **correctness
does not depend on the application's deployment topology**. There is no lock to
acquire, no leader to elect, no requirement that writers for a given aggregate
be routed to the same process. Deploy twenty replicas across three machines and
the guarantee holds, because the arbiter is the one component all of them
already share. This is why the PostgreSQL advisory locks in `eventsource.locks`
exist for coordinating *operations* — tenant cutover, migration exclusivity —
and not for guarding aggregate writes. Aggregate writes need no lock.

The costs are real and worth stating:

- **Callers must handle conflicts.** `OptimisticLockError` is a normal
  outcome, not an exceptional one. The usual response is to reload the
  aggregate, re-run the command against fresh state, and re-append — retry
  logic you have to write, and which must be safe to run more than once.
- **Contention degrades into wasted work.** Under heavy concurrent writes to a
  single aggregate, each loser has done its full command execution before being
  rejected, and retries pile up. That is a signal the aggregate is too
  coarse-grained, not a defect in the schema; the fix is smaller aggregates,
  not a bigger lock.
- **Rejection happens late.** The losing writer discovers the conflict only at
  insert time, after serializing every event in the batch. The batch is
  all-or-nothing: the transaction rolls back, so a partially-written stream is
  never left behind.
- **The constraint name is load-bearing on PostgreSQL.** The error mapping
  matches on `uq_events_aggregate_version` by name. Rename it in your own
  migration and version conflicts stop surfacing as `OptimisticLockError` and
  start surfacing as raw `IntegrityError` — a silent behavioural regression
  with no test in your codebase to catch it. If you must rename, change the
  store's matcher in the same commit.

### Why aggregate_type is part of the key

The key is `(aggregate_id, aggregate_type, version)` rather than
`(aggregate_id, version)`, which looks redundant if aggregate IDs are UUIDs —
they are globally unique, so the type adds no distinguishing power.

It is not there for uniqueness. It is there because the *store* treats an
aggregate stream as `(id, type)`, not `id` alone: every version query in
`postgresql.py` filters on both columns. That makes the type part of the stream
address, which allows the same identifier to carry independent event streams for
different aggregate types — the pattern where a `User` aggregate and a
`UserPreferences` aggregate share a subject ID and version independently. With
`aggregate_id` alone they would share a version counter and constantly conflict
over nothing.

Including the type in the constraint also makes the constraint's index directly
usable for the store's actual predicate. And the ordering matters: `aggregate_id`
leads because it is the selective column, so the index is useful for
`WHERE aggregate_id = ?` even when the type is not specified — which is exactly
what the store's fallback lookup, `SELECT aggregate_type, MAX(version) ... WHERE
aggregate_id = :aggregate_id`, needs.

### Why version is a per-aggregate counter

`version` starts at 1 for each aggregate and increments by one. It is not a
global sequence, and it is not shared between aggregate types.

Per-aggregate numbering is what makes the version *meaningful to the domain*.
"Order 123 is at version 7" is a statement a command handler can act on: load
the aggregate, assert it is still at 7, append 8. A global sequence would give
each event a number, but "this order is at global number 4,812,003" tells a
writer nothing about whether another writer has touched the same order, so it
could not serve as the concurrency check at all.

The dense, gap-free property is what makes partial replay work. Loading an
aggregate from a snapshot is `WHERE aggregate_id = ? AND version > ?` — with a
per-aggregate counter that is a contiguous range read; with a global sequence
it would be a sparse scatter across the table.

Note that the column is assigned by the *store*, not taken from the event: the
append loop computes `new_version = current_version + 1` and writes that.
`DomainEvent.aggregate_version` is the event's own copy of the same idea and is
serialized into the payload, but the column is the authority for locking and
ordering. Starting at 1 rather than 0 leaves 0 free to mean "this stream does
not exist", which is exactly how `COALESCE(MAX(version), 0)` and
`ExpectedVersion.NO_STREAM` use it.

### Why payload is JSONB (or TEXT)

The event's data lives in one opaque column — `JSONB NOT NULL` on PostgreSQL,
`TEXT NOT NULL` on SQLite — rather than in typed columns per event type.

The reason is that **schema evolution belongs in the pydantic event classes,
not in DDL**. A store holds every version of every event type that has ever
been written, forever. Typed columns would mean a migration for each new event
type, a nullable column for each new field, and a table whose column count
grows without bound — and no way at all to have two versions of the same event
type coexist. `DomainEvent` carries `event_version` precisely so that evolution
is a Python concern: add an optional field with a default and old rows validate
fine; make a breaking change and bump `event_version` and branch in your
upcasting logic. Neither requires touching the database.

What is written is the *whole* event: `_serialize_event` is
`event.model_dump(mode="json")`, so `payload` contains `event_id`,
`aggregate_id`, `timestamp`, `metadata` and every domain field. The typed
columns alongside it are denormalized copies extracted for indexing. Read back,
`_deserialize_event` looks up the class by `event_type` in the `EventRegistry`
and calls `model_validate` on the payload alone — the columns are not consulted
to rebuild the event. The columns exist so the *database* can filter and order;
the payload exists so *Python* can reconstruct. That is also why the columns
must not be edited independently of the payload: they would diverge silently,
with the payload winning on read.

The costs: the database cannot validate payload structure, so a malformed write
is only caught by pydantic on read, potentially long after the fact.
Type-correctness depends entirely on the event class still being importable and
registered — deleting an event class makes those rows undeserializable, which
is why `EventTypeNotFoundError` exists. JSONB is more verbose on disk than
packed typed columns. And querying inside the payload means JSONB operators on
unindexed data, which is fine for occasional investigation and wrong as a
projection strategy. On SQLite the payload is plain `TEXT`, so even those
operators are unavailable — read models, not payload queries, are the supported
way to ask questions of event data.

### Nullable tenant_id and actor_id

```sql
tenant_id UUID,
actor_id  VARCHAR(255),
```

Both are nullable, and both are nullable for the same reason: one schema has to
serve deployments that use the feature and deployments that do not.

For `tenant_id`, the alternative designs are worse. A separate multi-tenant
schema would double the DDL, double the store implementations, and force an
irreversible decision at install time — the single-tenant deployment that later
needs tenants would face a migration rather than a configuration change. A
`NOT NULL` column with a sentinel tenant would tax every single-tenant install
with a fictional value to invent, propagate, and filter on. `NULL` is the
honest encoding of "this deployment has no tenant concept", and the shape
matches `DomainEvent.tenant_id: UUID | None`. Deployments that do want the
column mandatory get that from `TenantDomainEvent`, which makes `tenant_id`
required at the event level — the strictness is opt-in at the type layer rather
than baked into DDL everyone shares.

`actor_id` is nullable because plenty of events have no user behind them:
scheduler ticks, system reconciliation, imports, events emitted by other
events. It is `VARCHAR(255)` rather than `UUID` deliberately — an actor may be
a user ID, a service name, or a job identifier, and constraining it to a UUID
would exclude the latter two.

The cost of nullability is index efficiency, and the schema pays it explicitly:
`idx_events_tenant_id` is declared `WHERE tenant_id IS NOT NULL`, so a
single-tenant store with a hundred million all-`NULL` rows carries an empty
index rather than a hundred million useless entries. The same pattern appears
on `event_outbox`. That is the general principle behind "optional" in this
schema — optional features should cost nothing when unused, and the place the
cost would otherwise show up is index size and write amplification. The other
cost is enforcement: nothing in the DDL stops a query reading across tenants,
so isolation remains an application obligation, discussed at the end of this
page.
