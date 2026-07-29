# Event Store Protocol

Reference for the event store contract defined in `eventsource.stores.interface`: the
`EventStore` abstract base class, the `EventPublisher` protocol, and the data types they
exchange (`StoredEvent`, `ReadOptions`, `EventStream`, `AppendResult`, `ReadDirection`,
and the `ExpectedVersion` sentinels).

The event store is the source of truth in this library. Every other component --
`AggregateRepository`, projections, subscriptions, migration tooling -- reads and writes
through this contract, so any backend that satisfies it can be swapped in without
touching domain code. The library ships three implementations: `InMemoryEventStore`,
`PostgreSQLEventStore`, and `SQLiteEventStore` (optional, requires `aiosqlite`).

`EventStore` declares five abstract methods that every backend must implement
(`append_events`, `get_events`, `get_events_by_type`, `event_exists`,
`get_global_position`) and three concrete methods with overridable defaults
(`get_stream_version`, `read_stream`, `read_all`). All methods are async.

Everything on this page is re-exported from the top-level `eventsource` package as well
as from `eventsource.stores`.

## Overview

The contract splits into a write path and a read path.

**Write path.** `append_events(aggregate_id, aggregate_type, events, expected_version)`
is the only write operation. It appends a non-empty list of `DomainEvent` atomically,
guarded by optimistic locking on `expected_version`, and returns an `AppendResult`
carrying `success`, `new_version`, `global_position`, and `conflict`. Callers pass either
a concrete version number or one of the `ExpectedVersion` sentinels (`ANY = -1`,
`NO_STREAM = 0`, `STREAM_EXISTS = -2`).

**Read path.** Three shapes of read are available:

| Method | Returns | Use |
| --- | --- | --- |
| `get_events()` | `EventStream` | Rehydrate one aggregate from its full (or partial) history |
| `get_events_by_type()` | `list[DomainEvent]` | Fetch every event for an aggregate type, optionally tenant- and timestamp-filtered |
| `read_stream()` / `read_all()` | `AsyncIterator[StoredEvent]` | Stream events lazily, with position metadata, for projections and subscriptions |

Two supporting reads round out the surface: `event_exists(event_id)` for idempotency
checks, and `get_global_position()` for the store-wide high-water mark that
`SubscriptionManager` uses to decide when catch-up is complete.

**Two position spaces.** Every persisted event has a *stream position* (its 1-based index
within one aggregate's stream, which is also what `EventStream.version` counts) and a
*global position* (its 1-based index across all events in the store). `StoredEvent`
carries both. Aggregate reconstruction and optimistic locking use stream positions;
catch-up subscriptions, projection checkpoints, and migration cutover use global
positions.

**Plain data types.** `StoredEvent`, `ReadOptions`, `EventStream`, and `AppendResult` are
all frozen dataclasses with no backend coupling. `ReadOptions` validates itself in
`__post_init__`, and `EventStream` / `AppendResult` provide constructor helpers
(`EventStream.empty()`, `AppendResult.successful()`, `AppendResult.conflicted()`).

**What a backend must actually write.** Only the five abstract methods are mandatory. The
three concrete methods have working defaults: `get_stream_version()` loads the stream and
returns its version, and `read_stream()` parses `stream_id` as
`"<aggregate_id>:<aggregate_type>"` and delegates to `get_events()`. Both defaults are
correct but unoptimized -- a backend that can answer them with a targeted query should
override them. `read_all()` is different: its default raises `NotImplementedError`, so a
backend that cannot provide a total global ordering may simply leave it alone, at the
cost of not supporting catch-up subscriptions or global projections.

**Multi-tenancy.** Tenant filtering is a read-side concern expressed through the
`tenant_id` field on `ReadOptions` (honoured by `read_all()`) and the `tenant_id`
parameter on `get_events_by_type()`. `append_events()` takes no tenant argument -- tenancy
travels on the event itself via `TenantDomainEvent`.

**`EventPublisher`** is a separate, single-method `Protocol` (`publish(events)`) for
pushing events onward to external systems. It is structurally typed, so event bus
implementations satisfy it without inheriting from anything.

## Import Paths

Everything in this reference lives in `eventsource.stores.interface` and is re-exported
from both `eventsource.stores` and the top-level `eventsource` package. Prefer the
top-level import in application code:

```python
from eventsource import (
    AppendResult,
    EventPublisher,
    EventStore,
    EventStream,
    ExpectedVersion,
    ReadDirection,
    ReadOptions,
    StoredEvent,
)
```

The bundled implementations are imported the same way:

```python
from eventsource import InMemoryEventStore, PostgreSQLEventStore
```

| Name | Kind | Defining module |
| --- | --- | --- |
| `EventStore` | Abstract base class | `eventsource.stores.interface` |
| `EventPublisher` | `Protocol` | `eventsource.stores.interface` |
| `StoredEvent` | Frozen dataclass | `eventsource.stores.interface` |
| `ReadOptions` | Frozen dataclass | `eventsource.stores.interface` |
| `EventStream` | Frozen dataclass | `eventsource.stores.interface` |
| `AppendResult` | Frozen dataclass | `eventsource.stores.interface` |
| `ReadDirection` | `Enum` | `eventsource.stores.interface` |
| `ExpectedVersion` | Constant holder class | `eventsource.stores.interface` |
| `InMemoryEventStore` | Implementation | `eventsource.stores.in_memory` |
| `PostgreSQLEventStore` | Implementation | `eventsource.stores.postgresql` |
| `SQLiteEventStore` | Implementation (optional) | `eventsource.stores.sqlite` |

### SQLiteEventStore is conditional

`SQLiteEventStore` requires the optional `aiosqlite` dependency. Both `eventsource` and
`eventsource.stores` import it inside a `try/except ImportError`, so the name is only
present -- and only appears in `__all__` -- when `aiosqlite` is installed. Guard on the
public flag rather than catching `ImportError` yourself:

```python
from eventsource import SQLITE_AVAILABLE

if SQLITE_AVAILABLE:
    from eventsource import SQLiteEventStore
```

`PostgreSQLEventStore` is imported unconditionally, but using it requires `asyncpg`;
`InMemoryEventStore` has no extra dependencies and is the store used throughout the
testing helpers.

### Importing from the defining module

Importing directly from `eventsource.stores.interface` is supported and is what the
library itself does internally, but the top-level re-exports are the stable public
surface -- module layout below `eventsource.stores` is an implementation detail.

## Data Types

Five plain data types travel across the event store boundary. Four are frozen
dataclasses (`StoredEvent`, `ReadOptions`, `EventStream`, `AppendResult`), one is an
`Enum` (`ReadDirection`), and `ExpectedVersion` is a bare class used only as a namespace
for integer constants. None of them depend on a backend, so they can be constructed
freely in tests and in domain code.

### ReadDirection

`Enum` with two string-valued members controlling stream traversal order:

| Member | Value | Meaning |
| --- | --- | --- |
| `ReadDirection.FORWARD` | `"forward"` | Oldest event first (chronological) |
| `ReadDirection.BACKWARD` | `"backward"` | Newest event first (reverse chronological) |

`ReadDirection` is a plain `Enum` (not `StrEnum`), so members do not compare equal to
their string values -- compare against the member, as every backend does
(`if options.direction == ReadDirection.BACKWARD:`).

`FORWARD` is the default for `ReadOptions.direction`. Direction is only reachable through
`ReadOptions`, so it applies to `read_stream()` and `read_all()` only; `get_events()` and
`get_events_by_type()` have no direction parameter and always return events oldest-first.

Direction is applied *before* `limit`, so `ReadDirection.BACKWARD` with `limit=10` yields
the 10 newest events, not the 10 oldest in reverse:

```python
options = ReadOptions(direction=ReadDirection.BACKWARD, limit=10)
async for stored in store.read_stream(f"{order_id}:Order", options):
    print(stored.stream_position)  # counts down from the stream's current version
```

Direction also changes how `stream_position` is computed on the yielded `StoredEvent`.
Reading forward, positions ascend as `from_position + i + 1`; reading backward, they
descend from the stream version as `version - i`. In-memory and default implementations
reverse an already-materialized list, while the SQL backends push the ordering into the
query (`ORDER BY version DESC` / `ASC`); the observable ordering is the same either way.

### StoredEvent

A frozen dataclass wrapping a persisted `DomainEvent` with the position metadata the
store assigned to it. The read iterators (`read_stream()`, `read_all()`) yield
`StoredEvent`; the eager reads (`get_events()`, `get_events_by_type()`) return bare
`DomainEvent` objects instead.

```python
stored = StoredEvent(
    event=order_created_event,
    stream_id=f"{order_id}:Order",
    stream_position=1,
    global_position=1000,
    stored_at=datetime.now(UTC),
)
```

The dataclass is declared `@dataclass(frozen=True)`, so assigning to any field raises
`AttributeError`. It carries both position spaces at once -- `stream_position` for
aggregate-level reasoning and `global_position` for checkpoints, catch-up subscriptions,
and migration cutover -- which is the reason projections and subscriptions consume the
iterator forms of the read API rather than the eager ones.

Backends construct `StoredEvent` themselves from their own storage rows, so the
faithfulness of `global_position` and `stored_at` depends on the backend: the SQL stores
populate them from the event table's global sequence and persisted timestamp column,
while `EventStore`'s default `read_stream()` has neither available and fills in
`global_position=0` and `stored_at=event.occurred_at`. Do not rely on `global_position`
from a backend that has not overridden `read_stream()`.

#### Fields

| Field | Type | Description |
| --- | --- | --- |
| `event` | `DomainEvent` | The underlying domain event |
| `stream_id` | `str` | Identifier of the stream, conventionally `"<aggregate_id>:<aggregate_type>"` |
| `stream_position` | `int` | 1-based position within that aggregate's stream |
| `global_position` | `int` | 1-based position across all events in the store |
| `stored_at` | `datetime` | When the event was persisted |

All five fields are required -- none has a default, so every `StoredEvent` must be
constructed with the full set, keyword or positional, in the order above.

**`event`** is the `DomainEvent` itself, unmodified. The wrapper adds metadata around it
rather than copying anything out of it; the four derived properties documented below
read straight through to this object.

**`stream_id`** is a `str`, not a `UUID`. The convention throughout the library is
`"<aggregate_id>:<aggregate_type>"` -- that is the format `read_stream()` parses, and
what the backends echo back into the events they yield. Nothing in the dataclass
validates the format or cross-checks it against `event.aggregate_id`.

**`stream_position`** is the 1-based index of the event within its aggregate's stream,
the same number `EventStream.version` counts up to. Reading forward it ascends; reading
backward the backends compute it descending from the stream's current version, so it
always identifies the same event regardless of `ReadDirection`.

**`global_position`** is the 1-based index across all events in the store, and is the
value projections and subscriptions persist as a checkpoint. Its trustworthiness is
backend-dependent: `PostgreSQLEventStore` reads it from the event table's global
sequence column, `InMemoryEventStore` tracks it on its own append counter, and the
default `read_stream()` in `EventStore` hardcodes `global_position=0` with the comment
`# Not available in default implementation`. Treat `0` from an unoverridden backend as
"unknown", not as "the first event".

**`stored_at`** is nominally the persistence timestamp, distinct from the event's own
`occurred_at`. Only `PostgreSQLEventStore` supplies a true persistence time (the row's
`created_at` column); both `InMemoryEventStore` and the default `read_stream()` fill it
from `event.occurred_at`. Code that needs to distinguish "when it happened" from "when
it was written" should not assume the two differ on every backend.

`__str__` renders as `StoredEvent(<event_type>, stream_pos=<n>, global_pos=<n>)` -- note
that it shows the event *type*, not the event ID, which is what appears in log lines and
test failure output. There is no custom `__repr__`; the dataclass-generated one is used
for `repr()` and shows all five fields.

#### Derived Properties (event_id, event_type, aggregate_id, aggregate_type)

Four read-only `@property` accessors delegate straight to the wrapped event, so callers
routing or filtering a `StoredEvent` do not have to reach through `.event`:

| Property | Type | Returns | Source field on `DomainEvent` |
| --- | --- | --- | --- |
| `event_id` | `UUID` | `self.event.event_id` | `event_id`, defaults to `uuid4()` |
| `event_type` | `str` | `self.event.event_type` | `event_type`, auto-derived from the class name |
| `aggregate_id` | `UUID` | `self.event.aggregate_id` | `aggregate_id`, required |
| `aggregate_type` | `str` | `self.event.aggregate_type` | `aggregate_type`, required |

```python
async for stored in store.read_all():
    if stored.event_type == "OrderShipped":
        await projection.handle(stored.event)
```

They are one-line delegations with no caching, no coercion, and no fallback -- reading
`stored.event_type` and `stored.event.event_type` are interchangeable. Because
`StoredEvent` is frozen and they are properties without setters, all four are read-only:
assignment raises `AttributeError`.

**They read the event, never the metadata.** `aggregate_id` and `aggregate_type` come
from the event payload, not from parsing `stream_id`. The two are conventionally the same
information (`stream_id` is `"<aggregate_id>:<aggregate_type>"`), but nothing in the
dataclass cross-checks them -- a backend that writes a `stream_id` disagreeing with the
event's own `aggregate_id` produces a `StoredEvent` that silently reports both. When the
two could diverge, `aggregate_id` is the authoritative one, since it is what the event
itself was constructed with and what `AggregateRepository` replays against.

**`event_type` is the auto-derived class name.** `DomainEvent.__init_subclass__` sets
`event_type` to the subclass's `__name__` unless the class explicitly declares its own
value, so string comparisons like `stored.event_type == "OrderShipped"` match the class
name by default. A class that overrides `event_type` (for example
`event_type: str = "order_created_v2"`) changes what this property returns -- match on
the declared value, not the class name, for such events.

**`event_id` is what idempotency keys off.** It is the same UUID that
`event_exists(event_id)` checks and that the append path uses to skip already-persisted
events, which makes `stored.event_id` the natural deduplication key for a projection or
subscription handler processing a `StoredEvent`.

The only other member of `StoredEvent` derived from the event is `__str__`, which uses
`self.event_type` -- so a log line reading `StoredEvent(OrderShipped, ...)` is showing
this property, not the `stream_id`.

### ReadOptions

A frozen dataclass bundling the read parameters for `read_stream()` and `read_all()`.
Every field has a default, so `ReadOptions()` is a valid "read everything forward from
the beginning" request. Passing `options=None` to either method is equivalent.

```python
# First 100 events, forward
ReadOptions(limit=100)

# Last 10 events, newest first
ReadOptions(direction=ReadDirection.BACKWARD, limit=10)

# From position 50 onward
ReadOptions(from_position=50, limit=100)

# Everything belonging to one tenant
ReadOptions(tenant_id=my_tenant_uuid)
```

#### Fields

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `direction` | `ReadDirection` | `FORWARD` | Traversal order |
| `from_position` | `int` | `0` | Starting position; `0` means the beginning, `-1` means the end |
| `limit` | `int \| None` | `None` | Maximum events to return; `None` means unlimited |
| `from_timestamp` | `datetime \| None` | `None` | Only events at or after this time |
| `to_timestamp` | `datetime \| None` | `None` | Only events at or before this time |
| `tenant_id` | `UUID \| None` | `None` | Restrict to one tenant; `None` means all tenants |

Which position space `from_position` refers to depends on the method: `read_stream()`
passes it through as `from_version` (a stream position), while `read_all()` implementations
interpret it as a global position.

#### Validation Rules (from_position, limit)

`__post_init__` enforces two rules and raises `ValueError` otherwise:

- `from_position` must be `>= 0`, with `-1` allowed as the "from the end" sentinel. Any
  other negative value raises `ValueError("from_position must be >= 0 or -1 (for end)")`.
- `limit`, when not `None`, must be `>= 0`. A negative limit raises
  `ValueError("limit must be >= 0")`. `limit=0` is accepted and yields nothing.

Nothing else is validated: the timestamps are not checked for ordering, and an
inconsistent `from_timestamp`/`to_timestamp` pair simply produces an empty result.

#### tenant_id Read Filter

`tenant_id` is purely a read-side filter. It exists because tenancy travels on the event
itself -- `TenantDomainEvent` populates the `tenant_id` field that `DomainEvent` declares
as optional -- rather than being a parameter of the write path. `append_events()` takes
no tenant argument at all.

Only `read_all()` consumes `ReadOptions.tenant_id`; `read_stream()`'s default
implementation ignores it, since a stream already belongs to exactly one aggregate. The
parallel filter for the eager read path is the separate `tenant_id` parameter on
`get_events_by_type()`, which backends translate into an `AND tenant_id = ...` clause.

Its main uses are tenant-scoped migrations and per-tenant projection rebuilds.

### EventStream

A frozen dataclass holding one aggregate's events plus its resulting version. This is
what `get_events()` returns and what `AggregateRepository` replays to rehydrate an
aggregate.

```python
stream = await event_store.get_events(order_id, "Order")
for event in stream.events:
    aggregate.apply_event(event, is_new=False)
```

#### Fields

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `aggregate_id` | `UUID` | -- | Identifier of the aggregate |
| `aggregate_type` | `str` | -- | Type name, e.g. `"Order"` |
| `events` | `list[DomainEvent]` | `[]` (per-instance) | Events in chronological order, oldest first |
| `version` | `int` | `0` | Aggregate version after applying all events |

`events` uses `field(default_factory=list)`, so each instance gets its own list.
`__post_init__` normalizes an explicitly-passed `None` back to `[]` via
`object.__setattr__`. The dataclass is frozen, but the `events` list itself is mutable --
treat it as read-only.

`version` is a separate field, not computed from `len(events)`. For a full read of the
stream the two coincide, but a partial read (`from_version=5`) returns the aggregate's
true current version alongside a shorter event list, which is exactly what optimistic
locking needs. Nothing enforces the relationship, so a hand-constructed `EventStream` can
carry any version you give it.

#### Properties (is_empty, latest_event)

| Property | Type | Behavior |
| --- | --- | --- |
| `is_empty` | `bool` | `True` when `events` has no entries |
| `latest_event` | `DomainEvent \| None` | Last element of `events`, or `None` when empty |

`latest_event` is the last event *in the returned list*, which for a partial read is not
necessarily the newest event in the underlying stream.

#### EventStream.empty()

```python
stream = EventStream.empty(aggregate_id, "Order")
assert stream.is_empty
assert stream.version == 0
```

Classmethod constructing an `EventStream` with no events and `version=0`. Backends return
it when an aggregate has no history, so callers can distinguish "nothing stored yet"
without a `None` check. `AggregateRepository` treats it as the signal to build a fresh
aggregate rather than raise.

### AppendResult

A frozen dataclass describing the outcome of `append_events()`.

```python
result = await event_store.append_events(...)
if result.success:
    print(f"appended, new version {result.new_version}")
elif result.conflict:
    print("concurrent modification, retry")
```

#### Fields

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `success` | `bool` | -- | Whether the append committed |
| `new_version` | `int` | -- | Aggregate version after the append |
| `global_position` | `int` | `0` | Global position of the last appended event |
| `conflict` | `bool` | `False` | Whether the failure was an optimistic-lock conflict |

Because the bundled backends *raise* `OptimisticLockError` on a version mismatch rather
than returning a value, in practice a returned `AppendResult` is a success. The
`conflict` field exists for backends that prefer to report conflicts as values, and for
callers that want a uniform shape. Handle both: catch `OptimisticLockError` and check
`result.success`.

Two edge cases in the shipped implementations are worth knowing. An empty `events` list
short-circuits to `AppendResult.successful(expected_version)` -- no write, and
`new_version` echoes what you passed in. And appends are idempotent by `event_id`: events
whose ID already exists are skipped, so `new_version` can advance by fewer than
`len(events)`.

#### AppendResult.successful()

```python
AppendResult.successful(new_version=3, global_position=1042)
```

Classmethod building `success=True, conflict=False` with the given version and
`global_position` (defaulting to `0`). This is the constructor backends use on the happy
path.

#### AppendResult.conflicted()

```python
AppendResult.conflicted(current_version=5)
```

Classmethod building `success=False, conflict=True, global_position=0`. Note the
parameter name: the aggregate's *actual* current version is stored in the `new_version`
field, so a caller recovering from a conflict reads `result.new_version` to learn what the
version really is before retrying.

### ExpectedVersion Sentinels

`ExpectedVersion` is not an enum or a dataclass -- it is a plain class holding three
`int` class attributes, used as a namespace. The values are ordinary integers passed as
the `expected_version` argument to `append_events()`, and backends distinguish them from
real version numbers by their negative or zero value.

| Constant | Value | Precondition |
| --- | --- | --- |
| `ExpectedVersion.ANY` | `-1` | None -- version check disabled |
| `ExpectedVersion.NO_STREAM` | `0` | Stream must not exist |
| `ExpectedVersion.STREAM_EXISTS` | `-2` | Stream must exist |

Any other value is treated as a concrete expected version and must equal the stream's
current version exactly. In all three failing cases the backend raises
`OptimisticLockError(aggregate_id, expected_version, current_version)`.

#### ANY (-1)

Skips the version check entirely; the append always proceeds. Use it for append-only
streams where no invariant depends on prior state (audit logs, telemetry, imports), and
for backfills where concurrent writers are acceptable. It gives up the only concurrency
protection the store offers, so it is the wrong default for aggregate writes.

The conformance suite asserts this behavior: consecutive appends with
`ExpectedVersion.ANY` succeed regardless of the stream's current version.

#### NO_STREAM (0)

Requires the stream to be empty -- current version must be `0`. This is the creation
guard: it fails if another writer already created the aggregate.

`NO_STREAM` and the literal `0` are the same value, which is why "pass `expected_version=0`
for a new aggregate" and "pass `ExpectedVersion.NO_STREAM`" describe the same call. The
concrete-version branch would compare `0 != current_version` and reach the identical
conclusion, so the sentinel is a readability choice rather than a behavioral one.

#### STREAM_EXISTS (-2)

Requires the stream to be non-empty -- current version must be greater than `0` -- but
does not care which version it is. Use it for updates that must apply to an existing
aggregate yet tolerate concurrent appends, such as adding an annotation or a
compensating event. It rejects writes to a non-existent aggregate while providing no
protection against lost updates.
