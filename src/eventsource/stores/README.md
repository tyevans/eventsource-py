# Event Stores

`eventsource.stores` contains the event store interface and its backends. The event store is the
source of truth in an event-sourced system: it appends immutable domain events and reads them back
for aggregate reconstruction, projections, and subscriptions. All operations are async.

## Key Interfaces

### Core Types (EventStore, StoredEvent, EventStream, AppendResult)

`EventStore` (ABC, `interface.py`) defines the store contract.

Abstract methods (must be implemented by a backend):

| Method | Signature | Returns |
| --- | --- | --- |
| `append_events` | `(aggregate_id: UUID, aggregate_type: str, events: list[DomainEvent], expected_version: int)` | `AppendResult` |
| `get_events` | `(aggregate_id: UUID, aggregate_type: str \| None = None, from_version: int = 0, from_timestamp: datetime \| None = None, to_timestamp: datetime \| None = None)` | `EventStream` |
| `get_events_by_type` | `(aggregate_type: str, tenant_id: UUID \| None = None, from_timestamp: datetime \| None = None)` | `list[DomainEvent]` |
| `event_exists` | `(event_id: UUID)` | `bool` |
| `get_global_position` | `()` | `int` (max global position, `0` when empty) |

Concrete methods with default implementations (overridable):

| Method | Default behavior |
| --- | --- |
| `get_stream_version(aggregate_id, aggregate_type)` | Calls `get_events()` and returns `stream.version` |
| `read_stream(stream_id, options=None)` | Async iterator; parses `stream_id`, delegates to `get_events()`, applies direction/limit, yields `StoredEvent` with `global_position=0` |
| `read_all(options=None)` | Raises `NotImplementedError` — global ordering is opt-in per backend |

`StoredEvent` (frozen dataclass) wraps a persisted event with position metadata: `event`,
`stream_id`, `stream_position` (1-based), `global_position` (1-based), `stored_at`. It proxies
`event_id`, `event_type`, `aggregate_id`, and `aggregate_type` from the underlying event as
properties.

`EventStream` (frozen dataclass) is the result of `get_events()`: `aggregate_id`, `aggregate_type`,
`events` (chronological, oldest first), `version`. Helpers: `is_empty`, `latest_event`, and the
`EventStream.empty(aggregate_id, aggregate_type)` classmethod (version 0).

`AppendResult` (frozen dataclass) reports `success`, `new_version`, `global_position` (default `0`),
and `conflict`. Construct via `AppendResult.successful(new_version, global_position=0)` or
`AppendResult.conflicted(current_version)`.

Note that the shipped backends raise `OptimisticLockError` on a version mismatch rather than
returning `AppendResult.conflicted(...)`; the `conflict` field exists for implementations that
prefer to signal conflicts as a value.

### ReadOptions and ReadDirection

`ReadDirection` is an enum with `FORWARD` and `BACKWARD`.

`ReadOptions` (frozen dataclass) fields and defaults:

| Field | Default | Meaning |
| --- | --- | --- |
| `direction` | `ReadDirection.FORWARD` | Read order |
| `from_position` | `0` | Exclusive starting position; `0` means "from the beginning". `-1` is accepted by validation as "from the end" but no shipped backend gives it special meaning |
| `limit` | `None` | Max events; `None` for unlimited |
| `from_timestamp` | `None` | Only events at or after this timestamp (`>=`) |
| `to_timestamp` | `None` | Only events at or before this timestamp (`<=`) |
| `tenant_id` | `None` | Tenant filter for `read_all()`; `None` means all tenants |

`__post_init__` raises `ValueError` if `from_position < 0` and is not `-1`, or if `limit` is
negative. `limit=0` is valid and yields nothing.

How the fields are applied:

| Field | `read_stream()` | `read_all()` |
| --- | --- | --- |
| `from_position` | Stream version, exclusive (`version > from_position`) | Global position, exclusive (`global_position > from_position`) |
| `direction` | Orders by version `ASC` / `DESC` | Orders by global position `ASC` / `DESC` |
| `limit` | Applied after ordering | Applied after ordering |
| `from_timestamp` / `to_timestamp` | Filters on the event timestamp | Filters on the event timestamp |
| `tenant_id` | Ignored — the stream is already one aggregate | Adds a `tenant_id` predicate |

`BACKWARD` reverses ordering only; it does not flip the meaning of `from_position`, which remains a
lower bound in both directions. Combine `BACKWARD` with `limit` to read the most recent events:

```python
options = ReadOptions(direction=ReadDirection.BACKWARD, limit=10)
async for stored in store.read_stream(f"{order_id}:Order", options):
    ...  # newest first
```

The ABC's default `read_stream()` behaves slightly differently from the SQL backends: it passes
`from_position` through to `get_events(from_version=...)`, reverses and truncates the resulting list
in Python, and reports `global_position=0` because it has no global ordering. `InMemoryEventStore`,
`PostgreSQLEventStore`, and `SQLiteEventStore` all override it and report real global positions.

`ReadOptions` is frozen — build a new instance (or `dataclasses.replace`) to change a field. Passing
`options=None` is the same as passing `ReadOptions()`.

### ExpectedVersion (optimistic-locking sentinels)

`ExpectedVersion` is a plain namespace class of `int` class attributes (not an `Enum`), passed as
the `expected_version` argument of `append_events()`:

| Constant | Value | Check performed | Raises when |
| --- | --- | --- | --- |
| `ANY` | `-1` | None — optimistic locking disabled | never |
| `NO_STREAM` | `0` | `current_version == 0` | the stream already has events |
| `STREAM_EXISTS` | `-2` | `current_version > 0` | the stream is empty |
| any other int | — | `current_version == expected_version` | the versions differ |

`current_version` is the number of events already stored for that `(aggregate_id, aggregate_type)`
pair — the store's per-stream version, not a global position. A failed check raises
`OptimisticLockError(aggregate_id, expected_version, current_version)`; the shipped backends raise
rather than returning `AppendResult.conflicted(...)`.

Because the constants are plain ints, they are interchangeable with literals: `NO_STREAM` is `0`, so
`expected_version=0` for a brand-new aggregate performs the identical check. Values other than `-1`
and `-2` that happen to be negative are *not* sentinels — they fall through to the exact-match
branch and can never succeed, since `current_version` is never negative.

```python
from eventsource.stores import ExpectedVersion

# New aggregate: fail if someone else created this stream first
await store.append_events(order_id, "Order", [created], ExpectedVersion.NO_STREAM)

# Update: fail if the stream vanished (or was never created)
await store.append_events(order_id, "Order", [shipped], ExpectedVersion.STREAM_EXISTS)

# Last-write-wins ingestion, no concurrency guard
await store.append_events(order_id, "Order", batch, ExpectedVersion.ANY)

# Normal aggregate save: version read at load time
await store.append_events(order_id, "Order", new_events, expected_version=loaded_version)
```

Two behaviors worth knowing:

- **Empty appends short-circuit.** All three backends return
  `AppendResult.successful(expected_version)` for an empty `events` list *before* running the
  version check, so an empty append never raises `OptimisticLockError` — and the returned
  `new_version` echoes the sentinel value you passed, which for `ANY` is `-1`.
- **The check is per aggregate type.** Two aggregate types sharing one `aggregate_id` keep separate
  versions, so `NO_STREAM` only asserts that *this* `(id, type)` stream is empty.

`InMemoryEventStore`, `PostgreSQLEventStore`, and `SQLiteEventStore` implement these semantics
identically, and the shared behavior is pinned by the `EventStore` conformance suite in
`eventsource.testing.conformance`.

### EventPublisher (Protocol)

`EventPublisher` is a one-method `typing.Protocol` for pushing already-persisted events to
downstream systems — notifications, search indices, an event bus. It is defined in
`eventsource.ports.bus` and re-exported from `interface.py` for backward compatibility:

```python
class EventPublisher(Protocol):
    async def publish(self, events: list[DomainEvent]) -> None: ...
```

Structural typing: any object with a matching async `publish` satisfies it — no base class, no
registration. It is *not* `@runtime_checkable`, so `isinstance(obj, EventPublisher)` raises
`TypeError`; conformance is a static (mypy) check only.

```python
class NotificationPublisher:
    async def publish(self, events: list[DomainEvent]) -> None:
        for event in events:
            await send_notification(event)

publisher: EventPublisher = NotificationPublisher()  # type-checks
```

The protocol declares no exception contract: `publish()` may raise, and failures are
implementation-specific.

**Where it is consumed.** `AggregateRepository(..., event_publisher=...)` is the main use. Inside
`save()`, the repository appends to the store first and only calls
`await self._event_publisher.publish(uncommitted_events)` after `AppendResult.success` — and after
`mark_events_as_committed()`. Publication is therefore in the same `await` path as the save: a
raising publisher propagates out of `save()` *after* the events are already durable. There is no
retry and no rollback. Use the outbox (`PostgreSQLEventStore(outbox_enabled=True)`) when you need
at-least-once delivery rather than best-effort. The configured publisher is readable via the
repository's `event_publisher` property.

**Relation to `EventBus`.** The bus backends in `../bus/` are not declared as `EventPublisher`
subclasses, but `EventBus.publish(events, background=False)` — with `background` defaulting — is
call-compatible with `publish(events)`, so a bus can be passed as the repository's publisher. The
event store itself never publishes; it only persists.

### TypeConverter / DefaultTypeConverter (Protocol + default impl)

Event payloads are persisted as JSON, so UUIDs and datetimes come back as strings. Before an event
is rebuilt with `event_class.model_validate(data, strict=False)`, the SQL backends run the decoded
payload through a `TypeConverter` to restore those Python types.

`TypeConverter` (`_type_converter.py`) is a `@runtime_checkable` `Protocol` — unlike
`EventPublisher`, `isinstance(obj, TypeConverter)` works (a method-name check only; signatures are
not verified):

| Method | Contract |
| --- | --- |
| `convert_types(data: Any) -> Any` | Recurses through dicts and lists, returning a new structure; the input is never mutated. Scalars pass through unchanged. Malformed UUID/datetime strings are preserved as-is rather than raising |
| `is_uuid_field(key: str) -> bool` | Should this key's string value become a `UUID`? |
| `is_datetime_field(key: str) -> bool` | Should this key's string value become a `datetime`? |

Only `str` values are conversion candidates, and each key is tested as a UUID field first, then as a
datetime field. Datetime parsing is `datetime.fromisoformat(value.replace("Z", "+00:00"))`, so a
trailing `Z` is accepted.

#### DefaultTypeConverter

```python
DefaultTypeConverter(
    *,
    uuid_fields: set[str] | None = None,
    string_id_fields: set[str] | None = None,
    auto_detect_uuid: bool = True,
    use_defaults: bool = True,
)
```

- `use_defaults=True` (the default) unions your sets with `DEFAULT_UUID_FIELDS` and
  `DEFAULT_STRING_ID_FIELDS`; `use_defaults=False` uses only what you pass.
- `DefaultTypeConverter.strict(uuid_fields)` is shorthand for
  `use_defaults=False, auto_detect_uuid=False, string_id_fields=set()` — exactly the listed fields
  become UUIDs and nothing else.

`is_uuid_field()` decides in this fixed order:

1. key in `uuid_fields` → `True`
2. key in `string_id_fields` → `False`
3. `auto_detect_uuid` and key ends with `_id` → `True`
4. otherwise `False`

Because step 1 precedes step 2, a name in both sets is treated as a UUID. With defaults merged in
that means listing, say, `user_id` in `string_id_fields` has no effect — it is already in
`DEFAULT_UUID_FIELDS`. Use `use_defaults=False` (or `strict()`) when you need to override a default.

`is_datetime_field()` is simply `key == "occurred_at" or key.endswith("_at")`.

```python
from eventsource.stores import DefaultTypeConverter

converter = DefaultTypeConverter(string_id_fields={"stripe_customer_id"})
converter.convert_types(
    {
        "aggregate_id": "550e8400-e29b-41d4-a716-446655440000",  # -> UUID
        "occurred_at": "2026-07-27T12:00:00Z",                   # -> datetime (UTC)
        "stripe_customer_id": "cus_abc123",                      # -> unchanged str
        "lines": [{"product_id": "..."}],                        # recursed
    }
)
```

Detection is name-based, not value-based: under the default settings an opaque external identifier
stored under a key ending in `_id` becomes a `UUID` if it happens to parse as one. Add it to
`string_id_fields`, or turn `auto_detect_uuid` off, when that is wrong.

#### Wiring it into a store

`PostgreSQLEventStore` and `SQLiteEventStore` each accept either a ready-made converter or the
constructor keywords that build one:

```python
# Option 1: pass a converter
store = PostgreSQLEventStore(session_factory, type_converter=my_converter)

# Option 2: let the store build a DefaultTypeConverter (use_defaults stays True)
store = SQLiteEventStore(":memory:", string_id_fields={"stripe_customer_id"})

# Option 3: strict mode via the classmethod
store = PostgreSQLEventStore.with_strict_uuid_detection(
    session_factory, uuid_fields={"event_id", "aggregate_id", "tenant_id"}
)
```

Passing `type_converter=` makes the `uuid_fields` / `string_id_fields` / `auto_detect_uuid` keywords
inert — the supplied converter is used verbatim. The converter is a private attribute; it is not
exposed as a property.

`InMemoryEventStore` holds live `DomainEvent` objects and never round-trips through JSON, so it has
no type converter and ignores these settings. Keep converter configuration in mind when in-memory
tests are meant to mirror production deserialization.

## Public Surface (`stores/__init__.py` exports)

### Always exported

`from eventsource.stores import ...` gives the following, all listed in `__all__`:

| Group | Names | Defined in |
| --- | --- | --- |
| Data structures | `AppendResult`, `EventStream`, `StoredEvent`, `ReadOptions`, `ReadDirection`, `ExpectedVersion` | `interface.py` |
| Abstract base class | `EventStore` | `interface.py` |
| Concrete implementations | `InMemoryEventStore`, `PostgreSQLEventStore` | `in_memory.py`, `postgresql.py` |
| Protocols | `EventPublisher` | `ports/bus.py` (re-exported via `interface.py`) |
| Type conversion | `TypeConverter`, `DefaultTypeConverter`, `DEFAULT_UUID_FIELDS`, `DEFAULT_STRING_ID_FIELDS` | `_type_converter.py` |

`PostgreSQLEventStore` is unconditional: SQLAlchemy is a core dependency, and the store creates
sessions from a `session_factory` you supply, so importing it does not require a live database or
the `asyncpg` driver.

Modules prefixed with `_` (`_type_converter.py`, `_compat.py`) are internal. `_type_converter.py` is
the exception that re-exports through the package: its four public names above are part of the
supported surface, while `_compat.validate_timestamp()` is not.

Most of this surface is also re-exported from the top-level package, which is the import path the
rest of the docs use:

```python
from eventsource import EventStore, StoredEvent, ReadOptions, PostgreSQLEventStore
```

Two exceptions: `TypeConverter`, `DefaultTypeConverter`, `DEFAULT_UUID_FIELDS`, and
`DEFAULT_STRING_ID_FIELDS` are *not* re-exported from `eventsource` — import them from
`eventsource.stores`.

### Conditionally exported: SQLiteEventStore (requires aiosqlite)

`SQLiteEventStore` is imported inside a `try/except ImportError`. When `aiosqlite` is installed the
name is bound and appended to `__all__`; when it is missing the import is skipped, the
module-private `_SQLITE_AVAILABLE` flag stays `False`, and `SQLiteEventStore` is absent from both
the module namespace and `__all__` — so `from eventsource.stores import SQLiteEventStore` raises
`ImportError`.

`_SQLITE_AVAILABLE` is private to `eventsource.stores` and not exported. To branch on availability,
use the top-level public flag instead, which is set by the same pattern in `eventsource/__init__.py`
(that block also covers `SQLiteCheckpointRepository`, `SQLiteOutboxRepository`, and
`SQLiteDLQRepository`):

```python
from eventsource import SQLITE_AVAILABLE

if SQLITE_AVAILABLE:
    from eventsource import SQLiteEventStore
```

`SQLITE_AVAILABLE` itself is only added to the top-level `__all__` when SQLite support is present,
so guard with `try/except ImportError` if you cannot assume the extra is installed. Install it with
the `sqlite` extra (`uv sync --extra sqlite`, or `--all-extras` for development).

## Module Map

### `interface.py` — EventStore ABC, StoredEvent, EventStream, AppendResult, ReadOptions, ReadDirection, ExpectedVersion (+ re-exported EventPublisher)

Backend-agnostic contracts and data structures. Imports only stdlib plus `DomainEvent`.

### `in_memory.py` — InMemoryEventStore (testing / no external deps)

`InMemoryEventStore(*, tracer=None, enable_tracing=True)`. Keeps events in dicts keyed by
`aggregate_id`, an event-id set for idempotency, and a global list for `read_all()`. Guarded by an
`asyncio.Lock`. Beyond the interface it offers `clear()`, `get_all_events()`, `get_event_count()`,
and `get_aggregate_ids()`. Not for production persistence or multi-process use.

### `postgresql.py` — PostgreSQLEventStore

```python
PostgreSQLEventStore(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    event_registry: EventRegistry | None = None,
    outbox_enabled: bool = False,
    tracer: Tracer | None = None,
    enable_tracing: bool = True,
    type_converter: TypeConverter | None = None,
    uuid_fields: set[str] | None = None,
    string_id_fields: set[str] | None = None,
    auto_detect_uuid: bool = True,
)
```

Uses SQLAlchemy async sessions against the `events` table. With `outbox_enabled=True`, appends also
write each event to the outbox table in the same transaction. Implements `read_stream()` and
`read_all()` with real global positions. Read-only properties: `session_factory`, `event_registry`,
`outbox_enabled`.

### `sqlite.py` — SQLiteEventStore (optional import)

```python
SQLiteEventStore(
    database: str,
    event_registry: EventRegistry | None = None,
    *,
    wal_mode: bool = True,
    busy_timeout: int = 5000,
    tracer=None, enable_tracing=True,
    type_converter=None, uuid_fields=None, string_id_fields=None, auto_detect_uuid=True,
)
```

Backed by `aiosqlite`; `database` is a file path or `":memory:"`. Manages its own connection —
`initialize()`, `close()`, and async context-manager support (`async with SQLiteEventStore(...)`).
Implements `read_stream()` and `read_all()`. Read-only properties: `database`, `event_registry`,
`is_connected`, `wal_mode`, `busy_timeout`.

### `_type_converter.py` — TypeConverter protocol, DefaultTypeConverter, DEFAULT_UUID_FIELDS, DEFAULT_STRING_ID_FIELDS

`DEFAULT_UUID_FIELDS` (frozenset): `event_id`, `aggregate_id`, `tenant_id`, `correlation_id`,
`causation_id`, `template_id`, `issuance_id`, `user_id`.

`DEFAULT_STRING_ID_FIELDS` (frozenset, never coerced to UUID even though they look like ids):
`actor_id`, `issuer_id`, `recipient_id`, `invited_by`, `assigned_by`, `revoked_by`,
`deactivated_by`, `reactivated_by`, `removed_by`.

### `_compat.py` — `validate_timestamp()` parameter validation helper

`validate_timestamp(value: datetime | None, param_name: str = "timestamp") -> datetime | None`
passes `datetime` and `None` through unchanged and raises `TypeError` for anything else. All three
backends call it on the `from_timestamp` argument of `get_events_by_type()`.

## Tenant Filtering

### `ReadOptions.tenant_id` on `read_all()`

Set `ReadOptions(tenant_id=...)` to restrict a global read to one tenant. The SQL backends add a
`tenant_id = ?` predicate; `InMemoryEventStore` filters the global list by `event.tenant_id`.

```python
options = ReadOptions(tenant_id=my_tenant_uuid)
async for stored in store.read_all(options):
    migrate_event(stored.event)
```

### `tenant_id` parameter on `get_events_by_type()`

`get_events_by_type()` takes `tenant_id` directly (it does not accept `ReadOptions`):

```python
events = await store.get_events_by_type("Order", tenant_id=my_tenant_id)
```

### Semantics: `None` means all tenants

`tenant_id=None` — the default in both places — applies no filter and returns events across all
tenants. There is no separate "events with no tenant" filter; to get only untenanted events, filter
in application code.

`read_stream()` and `get_events()` have no tenant parameter: they are already scoped to a single
aggregate.

## Choosing an Implementation

| Use case | Implementation |
| --- | --- |
| Unit tests, prototypes, single-process ephemeral state | `InMemoryEventStore` — no external dependency, no persistence |
| Production, concurrent writers, outbox pattern, global ordering | `PostgreSQLEventStore` |
| Embedded / single-node deployments, local development with durability | `SQLiteEventStore` (extra dependency: `aiosqlite`) |

Only backends that override `read_all()` support global reads; the ABC's default raises
`NotImplementedError`. All three shipped backends implement it.

## Invariants

- **Append-only.** Events are never updated or deleted, only appended.
- **Optimistic locking.** `append_events()` checks `expected_version` and raises
  `OptimisticLockError` on mismatch. Use `ExpectedVersion.ANY` to opt out.
- **Empty appends.** The `append_events()` contract documents `ValueError` for an empty events
  list; the shipped backends do not currently raise it, so do not rely on the check.
- **Idempotency.** `event_exists(event_id)` lets callers skip already-processed events; the SQL
  backends also skip events whose `event_id` is already present during an append.
- **Ordered streams.** `stream_position` within a stream is 1-based; `EventStream.events` is oldest
  first.
- **Global ordering.** `global_position` is 1-based across all streams. `get_global_position()`
  returns the current max, or `0` for an empty store.
- **Stream ID format.** `"{aggregate_id}:{aggregate_type}"` (e.g. `"abc-123:Order"`). The default
  `read_stream()` splits on the last `:`; a bare UUID is treated as an aggregate id with no type.
- **Frozen data structures.** `StoredEvent`, `ReadOptions`, `EventStream`, and `AppendResult` are
  frozen dataclasses.

## Related Documentation

- `docs/core-surface.md` — Tier 0 boundary and dependency tiers
- `../subscriptions/README.md` — catch-up subscriptions built on `read_all()` / global positions
- `../bus/README.md` — event bus backends, which satisfy the `EventPublisher` protocol
- `../aggregates/README.md` — how repositories use the store for load and save
- `../migrations/` — append-only SQL schema for the `events` table
