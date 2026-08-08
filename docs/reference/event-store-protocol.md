# Event Store Ports

Reference for the store contract defined in `eventsource.ports.store` and
`eventsource.ports.envelopes` / `eventsource.ports.positions`: the five segregated
capability ports (`EventAppender`, `StreamReader`, `EventLookup`, `GlobalEventFeed`,
`CategoryQuery`), the two convenience unions (`AggregateStore`, `FullEventStore`), and
the value objects that cross the boundary (`EventEnvelope`, `AppendResult`, `Position`,
`ExpectedVersion`, `ReadDirection`, and the three read-options records).

There is no `EventStore` base class. A backend implements whichever ports it can
honestly satisfy -- an append-only log with no total ordering simply does not implement
`GlobalEventFeed`, and that is a static (mypy-checked) fact about it, not a method that
raises `NotImplementedError` at runtime. See ADR 0019 for the rationale and ADR 0025 for
the retirement of the previous `EventStore` ABC that this page used to document.

Everything else in the library reads and writes through these ports:
`AggregateRepository` depends on `AggregateStore`, projections and subscriptions depend
on `GlobalEventFeed` and `CategoryQuery`, migration tooling depends on `FullEventStore`.
The library ships three adapters -- `InMemoryEventStore`, `PostgreSQLEventStore`
(requires `asyncpg`), and `SQLiteEventStore` (optional, requires `aiosqlite`) -- and all
three implement all five ports.

## Import Paths

```python
from eventsource import (
    AppendResult,
    CategoryReadOptions,
    EventEnvelope,
    ExpectedVersion,
    FeedReadOptions,
    Position,
    ReadDirection,
    StreamReadOptions,
)
```

The ports themselves are typing-only constructs (`Protocol` classes) -- import them when
you need to annotate a parameter, not to instantiate anything:

```python
from eventsource.ports.store import (
    AggregateStore,
    CategoryQuery,
    EventAppender,
    EventLookup,
    FullEventStore,
    GlobalEventFeed,
    StreamReader,
)
```

The bundled adapters:

```python
from eventsource import InMemoryEventStore, PostgreSQLEventStore

from eventsource import SQLITE_AVAILABLE

if SQLITE_AVAILABLE:
    from eventsource import SQLiteEventStore
```

Guard the `SQLiteEventStore` import on `SQLITE_AVAILABLE` since `aiosqlite` is an
optional dependency; `PostgreSQLEventStore` is importable unconditionally but needs
`asyncpg` at call time, gated by `ASYNCPG_AVAILABLE`.

| Name | Kind | Defining module |
| --- | --- | --- |
| `EventAppender` | `Protocol` | `eventsource.ports.store` |
| `StreamReader` | `Protocol` | `eventsource.ports.store` |
| `EventLookup` | `Protocol` | `eventsource.ports.store` |
| `GlobalEventFeed` | `Protocol` | `eventsource.ports.store` |
| `CategoryQuery` | `Protocol` | `eventsource.ports.store` |
| `AggregateStore` | `Protocol` (union) | `eventsource.ports.store` |
| `FullEventStore` | `Protocol` (union) | `eventsource.ports.store` |
| `EventEnvelope` | Frozen dataclass | `eventsource.ports.envelopes` |
| `AppendResult` | Frozen dataclass | `eventsource.ports.envelopes` |
| `StreamReadOptions` | Frozen dataclass | `eventsource.ports.envelopes` |
| `FeedReadOptions` | Frozen dataclass | `eventsource.ports.envelopes` |
| `CategoryReadOptions` | Frozen dataclass | `eventsource.ports.envelopes` |
| `ReadDirection` | `Enum` | `eventsource.ports.envelopes` |
| `Position` | Frozen dataclass | `eventsource.ports.positions` |
| `ExpectedVersion` | Frozen dataclass | `eventsource.ports.positions` |
| `InMemoryEventStore` | Adapter | `eventsource.adapters.memory` |
| `PostgreSQLEventStore` | Adapter | `eventsource.adapters.postgresql` |
| `SQLiteEventStore` | Adapter (optional) | `eventsource.adapters.sqlite` |

The "Defining module" column is also the deepest import path that works. Each of
these modules declares an explicit `__all__`, so each name is importable from the
module that defines it -- and every port name is additionally importable from
`eventsource.ports` and from the `eventsource` barrel. What does not work is
importing a name from a neighbouring module that merely imports it to write its
own annotations. `from eventsource.ports.store import FeedReadOptions`
fails under a strict type checker (`--no-implicit-reexport`) for that reason:
`store.py` uses that record without owning it. Import it from
`eventsource.ports.envelopes`, `eventsource.ports`, or `eventsource`.

None of the ports is `@runtime_checkable` -- `isinstance(store, StreamReader)` does not
work out of the box. That is a deliberate default (see the module docstring in
`ports/store.py`); add the decorator only where a consumer genuinely needs the
`isinstance` check.

## Overview

The contract is Interface Segregation applied to a store: five narrow `Protocol`
classes, each one capability, so a consumer's type annotation states exactly what it
needs and nothing more.

| Port | Capability | Consumers |
| --- | --- | --- |
| `EventAppender` | Append events to a stream, optimistic-locked | Every writer |
| `StreamReader` | Read one stream's events, forward or backward; fetch its version | `AggregateRepository` |
| `EventLookup` | Check whether an `event_id` has been stored | Idempotency checks |
| `GlobalEventFeed` | Read the store-wide ordered feed; fetch the current position | Subscriptions, catch-up, live migration |
| `CategoryQuery` | Read every stream in a category (aggregate type), storage-time filtered | Projection rebuilds, tenant-scoped migrations |

Two convenience unions are provided rather than left for every caller to spell out:

- **`AggregateStore = EventAppender & StreamReader`** -- what an aggregate repository
  needs: write plus single-stream read/version. Deliberately narrower than
  `FullEventStore`; a repository never touches the global feed, never queries a
  category, and never probes for an individual event id, so it must not type-require
  those capabilities.
- **`FullEventStore`** -- the union of all five ports. Reach for it only when a consumer
  (migration tooling is the one real example) genuinely needs every capability at once;
  everything else should type-hint the narrowest union it uses.

**Write path.** `EventAppender.append(stream, events, expected)` is the only write
operation. It appends a non-empty sequence of `DomainEvent` atomically, guarded by
`expected` (an `ExpectedVersion` value object, not a bare int), and returns an
`AppendResult` carrying the stream identity, the new version, and the position of the
first appended event.

**Read path.** Three shapes of read, one per port:

| Method | Port | Returns | Use |
| --- | --- | --- | --- |
| `read_stream()` | `StreamReader` | `AsyncIterator[EventEnvelope]` | Rehydrate one aggregate |
| `read_all()` | `GlobalEventFeed` | `AsyncIterator[EventEnvelope]` | Catch-up subscriptions, global projections |
| `read_category()` | `CategoryQuery` | `AsyncIterator[EventEnvelope]` | Rebuild a projection over every stream of one type |

All three are async iterators of `EventEnvelope`, not eager lists and not a
"stream vs. envelope" split -- there is no separate eager `get_events()` shape in the
ports surface. Two supporting reads round out the capability set:
`StreamReader.get_stream_version(stream)` for the current version without materializing
events, and `EventLookup.event_exists(event_id)` for idempotency checks.
`GlobalEventFeed.current_position()` returns the store's high-water `Position`, or
`None` for an empty store -- not `0`, which was the legacy behavior.

**Two position spaces.** Every `EventEnvelope` carries a *stream version*
(`stream_version`, a 1-based integer counting events within one `StreamId`) and an
optional *global-feed position* (`position`, an opaque `Position` token, `None` for a
feedless store). Aggregate reconstruction and optimistic locking use stream versions;
catch-up subscriptions, projection checkpoints, and migration cutover use `Position`.

**Value objects.** `EventEnvelope`, `AppendResult`, `StreamReadOptions`,
`FeedReadOptions`, `CategoryReadOptions`, and `Position` are all frozen dataclasses
(`slots=True`) with no backend coupling; `ExpectedVersion` is a frozen dataclass too,
constructed only through its four classmethods. None of them import a driver type, so
they can be constructed freely in tests and in application code.

**Multi-tenancy.** Tenant filtering is a read-side concern expressed through
`tenant_id` on `FeedReadOptions` and `CategoryReadOptions`. `EventAppender.append()`
takes no tenant argument -- tenancy travels on the event itself via `TenantDomainEvent`.

**Duplicate appends.** Appending an event whose `event_id` already exists in the store
raises `DuplicateEventError`. This replaces the legacy stores' silent skip-and-continue
behavior (ADR 0025, Decision 5) -- migration tooling and any other idempotency-sensitive
caller should catch `DuplicateEventError` explicitly rather than assume a duplicate
append is a no-op.

## The Ports

### EventAppender

```python
class EventAppender(Protocol):
    async def append(
        self,
        stream: StreamId,
        events: Sequence[DomainEvent],
        expected: ExpectedVersion,
    ) -> AppendResult: ...
```

The only write operation in the ports surface. `stream` identifies the target stream;
`events` is the non-empty sequence of new `DomainEvent`s to append; `expected` is one of
the four `ExpectedVersion` constructions and gates the write with optimistic
concurrency.

Guarantees:

- The append is atomic: either every event in `events` is committed, or none are.
- A version mismatch against `expected` raises `OptimisticLockError`.
- A duplicate `event_id` anywhere in `events` raises `DuplicateEventError`.
- An event whose `event_type` is not registered in the store's `EventRegistry` raises
  `EventTypeNotFoundError`. The serializing adapters (PostgreSQL, SQLite) cannot
  reconstruct the class on read, so they raise there; the in-memory adapters hold live
  objects and would never notice, so they validate on `append` instead. Either way a
  missing `@register_event` fails in every backend rather than only in production.
- An empty `events` sequence raises `ValueError` (not a silent successful no-op, which
  was the legacy behavior).
- `AppendResult.position` is the position of the **first** appended event, not the
  last -- the reverse of the retired `EventStore.append_events()` contract (ADR 0025,
  Decision 4).

Implemented by: `InMemoryEventStore`, `PostgreSQLEventStore`, `SQLiteEventStore`.

### StreamReader

```python
class StreamReader(Protocol):
    def read_stream(
        self,
        stream: StreamId,
        options: StreamReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]: ...

    async def get_stream_version(self, stream: StreamId) -> int: ...
```

`read_stream()` yields `EventEnvelope`s for one stream, oldest-first by default;
`options` (a `StreamReadOptions`) controls direction, version range, and limit.
`get_stream_version()` returns the stream's current version (`0` for a stream with no
events) without materializing any events -- the cheap call for an existence or
version check.

Implemented by: `InMemoryEventStore`, `PostgreSQLEventStore`, `SQLiteEventStore`. This
is also the port `AggregateStore` composes alongside `EventAppender`, so it is what
`AggregateRepository` depends on.

### EventLookup

```python
class EventLookup(Protocol):
    async def event_exists(self, event_id: UUID) -> bool: ...
```

A single-method port for idempotency checks -- "has this exact event already been
persisted." Distinct from the `append()`-time `DuplicateEventError` guard: this lets a
caller check *before* attempting a write.

Implemented by: `InMemoryEventStore`, `PostgreSQLEventStore`, `SQLiteEventStore`.

### GlobalEventFeed

```python
class GlobalEventFeed(Protocol):
    def read_all(
        self,
        from_position: Position | None = None,
        options: FeedReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]: ...

    async def current_position(self) -> Position | None: ...
```

The store-wide ordered feed. `read_all()` yields every `EventEnvelope` in the store in
total order (subject to `options`' `tenant_id` and `limit` filters), resuming strictly
after `from_position` when given. `current_position()` returns the store's current
high-water `Position`, or `None` for an empty store.

This port carries the strongest guarantee in the surface: **exclusive resumption with
no-skip delivery.** Resuming a read strictly after a feed-produced `Position` must never
permanently skip a committed event, even one committed concurrently with the read. The
PostgreSQL adapter honors this by bounding feed reads to a transaction-safe horizon
rather than reading right up to the latest commit.

Only a backend with a genuine total ordering across all its streams implements this
port -- there is no default, no `NotImplementedError` fallback, and no way to call
`read_all()` against a store that does not support it (mypy rejects the call statically
against anything typed narrower than `GlobalEventFeed`). Catch-up subscriptions and live
migration both type-require `GlobalEventFeed` for exactly this reason.

There is no `ReadDirection.BACKWARD` equivalent for feed reads, and no
`from_timestamp`/`to_timestamp` filter on `FeedReadOptions` -- both existed on the
retired `ReadOptions` and were dropped rather than ported (ADR 0025 Consequences).

Implemented by: `InMemoryEventStore`, `PostgreSQLEventStore`, `SQLiteEventStore`.

### CategoryQuery

```python
class CategoryQuery(Protocol):
    def read_category(
        self,
        category: str,
        options: CategoryReadOptions | None = None,
    ) -> AsyncIterator[EventEnvelope]: ...
```

Reads every stream belonging to one category (today's aggregate type) as a single
merged, ordered feed -- the shape a projection rebuild or a tenant-scoped migration
needs, where the caller does not know or care about individual stream identities ahead
of time.

**Filtering and ordering are on storage time, inclusive, with position as tie-break.**
`read_category()` filters and orders on `EventEnvelope.stored_at` -- when the row was
written -- not on the event's own `occurred_at`, and the `from_timestamp` bound in
`CategoryReadOptions` is inclusive (`>=`). Where two envelopes share the same
`stored_at`, `Position` breaks the tie deterministically. This is a reversal of the
retired `get_events_by_type()`, which filtered/ordered on `occurred_at` and used an
exclusive (`>`) bound (ADR 0025, Decision 6).

**Naive datetimes are rejected.** `from_timestamp` must be timezone-aware; a naive
`datetime` raises `ValueError` rather than being silently compared against
timezone-aware `stored_at` values.

Implemented by: `InMemoryEventStore`, `PostgreSQLEventStore`, `SQLiteEventStore`.

### AggregateStore and FullEventStore

```python
class AggregateStore(EventAppender, StreamReader, Protocol):
    """What an aggregate repository needs: append plus stream read/version."""


class FullEventStore(
    EventAppender, StreamReader, EventLookup, GlobalEventFeed, CategoryQuery, Protocol
):
    """Union of all five store capability ports."""
```

Both are pure type-level compositions -- no new methods, no implementation. Use
`AggregateStore` for anything shaped like `AggregateRepository`; use `FullEventStore`
only for a consumer that genuinely spans all five capabilities. Do not default to
`FullEventStore` out of convenience -- it defeats the segregation the other four ports
exist to provide (see `.claude/rules/architecture.md`).

### collect()

```python
async def collect(it: AsyncIterator[EventEnvelope]) -> list[EventEnvelope]:
    """Drain an async iterator into a list."""
```

A small helper for tests and scripts that want the eager list a legacy caller might
have expected from `get_events()`. Production code should prefer consuming the async
iterator lazily.

## Value Objects

### ReadDirection

`Enum` with two string-valued members, defined in `eventsource.ports.envelopes`:

| Member | Value | Meaning |
| --- | --- | --- |
| `ReadDirection.FORWARD` | `"forward"` | Oldest event first (chronological) |
| `ReadDirection.BACKWARD` | `"backward"` | Newest event first (reverse chronological) |

`ReadDirection` is a plain `Enum`, not a `StrEnum` -- members do not compare equal to
their string values; compare against the member.

`FORWARD` is the default for `StreamReadOptions.direction`. **`BACKWARD` is meaningful
for stream reads only** -- `StreamReadOptions` has a `direction` field, but
`FeedReadOptions` does not. There is no backward-feed-read equivalent in the ports
surface (see `GlobalEventFeed` above); a caller that needs the newest events from the
global feed must read forward and take the tail, or maintain its own reverse index.

### EventEnvelope

```python
@dataclass(frozen=True, slots=True)
class EventEnvelope:
    event: DomainEvent
    stream_id: StreamId
    stream_version: int
    position: Position | None
    stored_at: datetime
```

The single container type yielded by all three read methods (`read_stream()`,
`read_all()`, `read_category()`) -- there is no separate eager/bare-event return shape
in the ports surface.

| Field | Type | Description |
| --- | --- | --- |
| `event` | `DomainEvent` | The underlying domain event, unmodified |
| `stream_id` | `StreamId` | Identity of the stream (`aggregate_id` + `category`) |
| `stream_version` | `int` | 1-based position within that stream |
| `position` | `Position \| None` | Opaque global-feed position; `None` for a feedless store |
| `stored_at` | `datetime` | When the event was persisted (UTC) |

`stream_id` is a `StreamId` value object (see below), not a raw string. `position` is
`None` only for a store that does not implement `GlobalEventFeed`; every bundled
adapter implements it, so in practice `position` is always populated for
`InMemoryEventStore`, `PostgreSQLEventStore`, and `SQLiteEventStore`.

There are no derived `event_id`/`event_type`/`aggregate_id`/`aggregate_type` convenience
properties on `EventEnvelope` -- read those straight off `.event` and `.stream_id`:

```python
async for envelope in store.read_all():
    if envelope.event.event_type == "OrderShipped":
        await projection.handle(envelope.event)
    aggregate_id = envelope.stream_id.aggregate_id
```

### StreamId

`StreamId` is a domain value object (`eventsource.domain.StreamId`), not part of the
ports module, but it is the identity every port method keys reads and writes on:

```python
@dataclass(frozen=True)
class StreamId:
    aggregate_id: UUID
    category: str
```

`category` must match `[A-Za-z0-9_.-]+` -- `:` is banned because `render()` produces
`"{aggregate_id}:{category}"`, the same wire format the legacy `stream_id` string used.
`StreamId.parse(raw)` is the inverse. Construct a `StreamId` to call `read_stream()` or
`append()`; do not build the `"<uuid>:<category>"` string by hand.

### AppendResult

```python
@dataclass(frozen=True, slots=True)
class AppendResult:
    stream: StreamId
    new_version: int
    position: Position | None
```

Returned by `EventAppender.append()` on success (a failed append raises rather than
returning a falsy result -- see `OptimisticLockError` and `DuplicateEventError` above).
`position` is the position of the **first** event appended in this call, `None` for a
feedless store.

```python
result = await store.append(stream, events, ExpectedVersion.no_stream())
print(f"stream now at version {result.new_version}, first event at {result.position}")
```

### StreamReadOptions

```python
@dataclass(frozen=True, slots=True)
class StreamReadOptions:
    direction: ReadDirection = ReadDirection.FORWARD
    from_version: int | None = None
    to_version: int | None = None
    limit: int | None = None
```

| Field | Default | Description |
| --- | --- | --- |
| `direction` | `FORWARD` | Traversal order for this stream |
| `from_version` | `None` | Starting version (inclusive); `None` means the stream start |
| `to_version` | `None` | Ending version (inclusive); `None` means the stream end |
| `limit` | `None` | Maximum events to return; `None` means unlimited |

```python
# Last 10 events of a stream, newest first
options = StreamReadOptions(direction=ReadDirection.BACKWARD, limit=10)
async for envelope in store.read_stream(stream, options):
    print(envelope.stream_version)  # counts down
```

### FeedReadOptions

```python
@dataclass(frozen=True, slots=True)
class FeedReadOptions:
    tenant_id: UUID | None = None
    aggregate_type: str | None = None
    limit: int | None = None
```

| Field | Default | Description |
| --- | --- | --- |
| `tenant_id` | `None` | Restrict to one tenant; `None` means all tenants |
| `aggregate_type` | `None` | Restrict to one aggregate type (stream category); `None` means all types |
| `limit` | `None` | Maximum events to return; `None` means unlimited |

`aggregate_type` is the same fact as `StreamId.category` and the stored
`aggregate_type` column, and the SQL adapters push it into the `WHERE` clause --
so a consumer interested in one type should pass it rather than reading the whole
feed and discarding the rest. Note the difference from `read_category`, which
filters on the same column but orders by storage time; a feed read stays ordered
by global position, so a position taken from one page resumes the next.

Deliberately narrower than the retired `ReadOptions`: no `direction` (feed reads are
always forward -- see `ReadDirection` above) and no `from_timestamp`/`to_timestamp`
(dropped, not ported, per ADR 0025 Consequences). Resumption is via the
`from_position` parameter on `read_all()` itself, not a field on this record.

### CategoryReadOptions

```python
@dataclass(frozen=True, slots=True)
class CategoryReadOptions:
    tenant_id: UUID | None = None
    from_timestamp: datetime | None = None
    limit: int | None = None
```

| Field | Default | Description |
| --- | --- | --- |
| `tenant_id` | `None` | Restrict to one tenant; `None` means all tenants |
| `from_timestamp` | `None` | Minimum `stored_at` (inclusive); `None` means no minimum |
| `limit` | `None` | Maximum events to return; `None` means unlimited |

`from_timestamp` filters on storage time (`EventEnvelope.stored_at`), inclusive, not on
the event's own `occurred_at` -- see `CategoryQuery` above for the full inclusive/
tie-break/naive-datetime rules. There is no `to_timestamp` field.

### Position

```python
@dataclass(frozen=True, slots=True)
class Position:
    store_id: str
    key: tuple[int | str, ...]
```

An opaque, ordered, serializable token identifying a point in one store's global feed.
Produced only by `GlobalEventFeed` implementers (`current_position()`, and via
`EventEnvelope.position` / `AppendResult.position`), and consumed by passing it back
into `read_all(from_position=...)` or persisting it as a checkpoint.

**Totally ordered within one store; not comparable across stores.** `<`, `<=`, `>`,
`>=` compare `key` lexicographically once both operands share `store_id`; comparing
`Position`s from two different `store_id`s raises `PositionForeignError`. Equality
(`==`) is a plain dataclass comparison of both fields together, so positions from
different stores are simply unequal rather than raising.

**No arithmetic.** `Position` deliberately exposes no subtraction, no distance, no way
to compute "how far behind." Lag metrics built on positions became count-behind or
wall-clock lag as of ADR 0019/0025 (amending ADR 0014) -- see `key` as an opaque tuple
whose shape is adapter-defined, not a number to do math on.

```python
pos = await store.current_position()
if pos is not None:
    serialized = pos.to_str()          # '{"s":"pg-main","k":[1042]}'
    restored = Position.from_str(serialized)
    assert restored == pos
```

`to_str()` / `from_str()` round-trip through JSON for checkpoint persistence.
`from_str()` raises `PositionDecodeError` on malformed input (not valid JSON, wrong
shape, or a `key` element that is not an `int`/`str` -- `bool` is explicitly excluded
even though `bool` is an `int` subclass in Python).

### ExpectedVersion

```python
@dataclass(frozen=True, slots=True)
class ExpectedVersion:
    kind: str
    version: int | None = None
```

Optimistic-concurrency expectation for `EventAppender.append()`. Not an int, not an
enum member -- a frozen dataclass constructed only through its four classmethods; the
raw constructor validates `kind` against a known set and enforces that `version` is
present if and only if `kind == "exact"`.

| Classmethod | `kind` | Precondition |
| --- | --- | --- |
| `ExpectedVersion.any_()` | `"any"` | None -- version check disabled |
| `ExpectedVersion.no_stream()` | `"no_stream"` | Stream must not exist (version 0) |
| `ExpectedVersion.stream_exists()` | `"stream_exists"` | Stream must exist (version > 0) |
| `ExpectedVersion.exact(n)` | `"exact"` | Stream's current version must equal exactly `n` |

Any mismatch raises `OptimisticLockError(aggregate_id, expected_version, current_version)`.

**Adapters translate by name, never by numeric coincidence.** The retired
`ExpectedVersion` sentinels were bare ints (`ANY = -1`, `NO_STREAM = 0`,
`STREAM_EXISTS = -2`); a caller migrating old integer call sites maps `-1 -> any_()`,
`0 -> no_stream()`, `-2 -> stream_exists()`, and any other non-negative int
`n -> exact(n)`. `OptimisticLockError` itself still carries the legacy sentinel ints in
its `expected_version` field for message fidelity -- that is a private adapter-internal
formatting detail, not part of the ports contract (ADR 0025, Decision 11), so do not
infer a VO's `kind` from `OptimisticLockError.expected_version`.

#### any_()

Skips the version check entirely; the append always proceeds. Use it for append-only
streams where no invariant depends on prior state (audit logs, telemetry, imports), and
for backfills where concurrent writers are acceptable. It gives up the only concurrency
protection the store offers -- the wrong default for aggregate writes.

#### no_stream()

Requires the stream to be empty (current version `0`). This is the creation guard: it
fails if another writer already created the aggregate.

#### stream_exists()

Requires the stream to be non-empty (current version `> 0`) but does not care which
version. Use it for updates that must apply to an existing aggregate yet tolerate
concurrent appends, such as adding an annotation or a compensating event. It rejects
writes to a non-existent aggregate while providing no protection against lost updates.

#### exact(n)

Requires the stream's current version to equal `n` precisely -- the strict form used by
`AggregateRepository` on every normal save, where `n` is the version the aggregate was
loaded at. Raises `ValueError` at construction time if `n < 0`.

## Errors

| Exception | Raised by | When |
| --- | --- | --- |
| `OptimisticLockError` | `EventAppender.append()` | `expected` does not match the stream's current version |
| `DuplicateEventError` | `EventAppender.append()` | An event in the batch has an `event_id` already stored |
| `PositionForeignError` | `Position` comparison operators | Comparing positions from two different `store_id`s |
| `PositionDecodeError` | `Position.from_str()` | Input is not valid, well-shaped JSON for a `Position` |
| `ValueError` | `EventAppender.append()`, `CategoryQuery.read_category()`, `ExpectedVersion.exact()` | Empty `events` sequence; naive `from_timestamp`; negative `exact()` version |

All of these live in `eventsource.domain.exceptions` and are re-exported from the top-level
`eventsource` package.

## What Changed From the Retired `EventStore` ABC

For anyone migrating call sites off the legacy surface (`eventsource.stores`, deleted
per ADR 0025):

- One `EventStore` ABC with five abstract + three overridable-default methods becomes
  five segregated `Protocol` ports; a backend implements only what it can honor, with
  mypy enforcing the boundary instead of `NotImplementedError` at runtime.
- `StoredEvent` becomes `EventEnvelope`; its four derived properties
  (`event_id`/`event_type`/`aggregate_id`/`aggregate_type`) are gone -- read `.event`
  and `.stream_id` directly.
- `stream_id: str` (`"<uuid>:<category>"`) becomes `stream_id: StreamId`, a proper value
  object with `.render()` / `.parse()`.
- `ReadOptions` splits into three narrower records (`StreamReadOptions`,
  `FeedReadOptions`, `CategoryReadOptions`); `direction` and timestamp filters no longer
  apply to feed reads.
- Integer `global_position` becomes the opaque `Position` value object: comparable and
  serializable, never arithmetic.
- `int` `expected_version` (with `-1`/`0`/`-2` sentinels) becomes the `ExpectedVersion`
  value object, constructed via `.any_()` / `.no_stream()` / `.stream_exists()` /
  `.exact(n)`.
- `AppendResult.global_position` (last event) becomes `AppendResult.position` (first
  event) -- the ordering flips.
- Silent duplicate-append skipping becomes `DuplicateEventError`.
- `get_events_by_type()` (occurred_at, exclusive) becomes `read_category()` (stored_at,
  inclusive, position tie-break) and drops the cross-type (`aggregate_type=None`) mode.
- `get_global_position()` returning `0` for an empty store becomes `current_position()`
  returning `None`.
- `EventPublisher` is gone from this surface entirely; it was never a store capability.
