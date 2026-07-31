# Adapters

Interface adapters (Clean Architecture "adapters" ring) implementing the boundary
protocols defined in `eventsource.ports`. This is the sole home for event store
implementations -- `InMemoryEventStore`, `SQLiteEventStore`, and `PostgreSQLEventStore`
all live here, alongside their matching snapshot stores and (for memory/sql) the
checkpoint and DLQ repositories.

## Key Interfaces

- `eventsource.ports.store.FullEventStore` -- the union of the five store capability
  protocols (`EventAppender`, `StreamReader`, `EventLookup`, `GlobalEventFeed`,
  `CategoryQuery`) that every event store adapter here implements structurally (no ABC
  inheritance -- conformance is duck-typed).
- `eventsource.ports.snapshots.SnapshotStore` -- the snapshot storage contract.
- `eventsource.ports.checkpoints` / `eventsource.ports.dlq` -- checkpoint and
  dead-letter-queue repository contracts implemented by the memory and SQL backends.

## Module Map

- `memory/` -- `InMemoryEventStore`, `InMemorySnapshotStore`, `InMemoryCheckpointRepository`,
  `InMemoryDLQRepository`. No external dependencies; all state lives in process memory.
- `sqlite/` -- `SQLiteEventStore`, `SQLiteSnapshotStore`. Requires `aiosqlite`.
- `postgresql/` -- `PostgreSQLEventStore`, `PostgreSQLSnapshotStore`. Requires `asyncpg`.
- `sql/` and `_sql/` -- dialect-parameterized SQL building blocks shared by the SQLite
  and PostgreSQL backends (checkpoint/DLQ/`DatabaseProjection` SQL, and position codecs
  such as `IntPositionCodec`).

## Choosing a Backend

- **`InMemoryEventStore`** -- tests, examples, and prototypes. No dependencies beyond
  the core package. Constructed with `InMemoryEventStore(store_id: str = "memory", *,
  event_registry: EventRegistry | None = None)`. Nothing to connect, initialize, or
  close -- construct it, use it, drop the reference.
- **`SQLiteEventStore`** -- embedded and single-node deployments, local development, or
  any setting where a single writer is acceptable. Self-initializing: there is no
  `async with store:` context manager and no `.initialize()` call -- construct it with
  `SQLiteEventStore(database: str, event_registry: EventRegistry | None = None, *,
  store_id: str | None = None, wal_mode: bool = True, busy_timeout: int = 5000)` and use
  it directly. Because SQLite writes are serialized to a single connection, the adapter's
  global feed needs no "safe horizon" handling -- a reader can never observe a lower
  `global_position` commit after a higher one.
- **`PostgreSQLEventStore`** -- production, multi-writer deployments. Takes a plain
  SQLAlchemy `AsyncEngine` (not a `session_factory`/`async_sessionmaker` -- the adapter
  builds and owns its own session factory internally): `PostgreSQLEventStore(engine:
  AsyncEngine, event_registry: EventRegistry | None = None, *, store_id: str | None =
  None, create_schema: bool = False, outbox_enabled: bool = False)`. Because PostgreSQL
  commits can become visible out of order under concurrent transactions, `read_all()` and
  `current_position()` apply a safe-horizon predicate so an in-flight lower position is
  never skipped.

## Outbox

`PostgreSQLEventStore` accepts `outbox_enabled: bool = False` at construction. When
`True`, every `append()` also writes a row to the `event_outbox` table inside the same
database transaction as the event append, giving same-transaction, at-least-once outbox
semantics without a separate two-phase write. The flag is read back via the read-only
`outbox_enabled` property. SQLite and the in-memory adapter have no outbox support.

## Optional-Dependency Availability Flags

Backends that need an optional driver guard the import with `try/except ImportError` and
expose a module-level boolean so callers can branch without triggering the exception
themselves:

- `AIOSQLITE_AVAILABLE` (`eventsource.adapters.sqlite.store`, re-exported from
  `eventsource.adapters.sqlite`) -- whether `aiosqlite` is importable; gates
  `SQLiteEventStore`.
- `SQLITE_AVAILABLE` (`eventsource.adapters.sqlite.snapshots`, re-exported from
  `eventsource.adapters.sqlite`) -- whether `aiosqlite` is importable, checked from the
  snapshot module; gates `SQLiteSnapshotStore`. `SQLiteSnapshotStore` itself still
  guards `aiosqlite` internally and raises `SQLiteNotAvailableError` (an `ImportError`
  subclass) from its constructor if the driver is missing, rather than failing at import
  time.
- `ASYNCPG_AVAILABLE` (`eventsource.adapters.postgresql.store`, re-exported from
  `eventsource.adapters.postgresql`) -- whether `asyncpg` is importable; gates
  `PostgreSQLEventStore`. `PostgreSQLSnapshotStore` has no separate flag of its own.

`InMemoryEventStore` and `InMemorySnapshotStore` have no availability flag -- they are
always importable, since they depend only on the standard library.

## Invariants

- **Structural conformance, not inheritance.** Store adapters satisfy
  `eventsource.ports.store.FullEventStore` (and the narrower `AggregateStore` used by
  `AggregateRepository`) by implementing the right methods, not by subclassing a shared
  base class.
- **Optimistic concurrency via `ExpectedVersion`, enforced by raising.** `append()` takes
  an `ExpectedVersion` (constructed via `.any_()`, `.no_stream()`, `.stream_exists()`, or
  `.exact(n)`) and raises `OptimisticLockError` on a mismatch -- it never returns a
  result object with a "conflict" flag.
- **Positions are opaque per-store tokens.** `Position(store_id, key)` is comparable only
  against another `Position` from the same `store_id`; consumers persist and compare it,
  never do arithmetic on it.
- **Optional dependencies fail at construction, not at import.** Importing
  `eventsource.adapters.sqlite` or `eventsource.adapters.postgresql` never raises even
  without the driver installed; only constructing the store/snapshot class does.
