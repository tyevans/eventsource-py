# Snapshots

Technical reference for the snapshot machinery: the `Snapshot` value object and
`SnapshotStore` interface (ports ring), its three backend adapters, the
snapshot-specific exception hierarchy, and the `SnapshotPolicy` /
`SnapshotScheduler` collaborators (application ring) that decide when and how
snapshots get written.

A snapshot is a point-in-time capture of an aggregate's serialized state at a
known version. It is an optimization artifact, never the source of truth — the
event stream is. Any snapshot can be deleted and regenerated from events, and a
missing, unreadable, or schema-mismatched snapshot always degrades to a full
event replay rather than an error surfaced to the caller.

The machinery spans five source modules, across three rings:

| Module | Ring | Contains |
| --- | --- | --- |
| `eventsource.ports.snapshots` | Ports | `Snapshot`, `SnapshotStore` |
| `eventsource.domain.exceptions` | Entities | `SnapshotError`, `SnapshotDeserializationError`, `SnapshotSchemaVersionError`, `SnapshotNotFoundError` |
| `eventsource.adapters.memory.snapshots` | Adapters | `InMemorySnapshotStore` |
| `eventsource.adapters.postgresql.snapshots` | Adapters | `PostgreSQLSnapshotStore` |
| `eventsource.adapters.sqlite.snapshots` | Adapters | `SQLiteSnapshotStore`, `SQLITE_AVAILABLE`, `SQLiteNotAvailableError` |
| `eventsource.application.aggregates.snapshotting` | Application | `SnapshotPolicy`, `EveryNEvents`, `Never`, `SnapshotScheduler`, `ImmediateScheduler`, `BackgroundScheduler`, `take_snapshot()`, `read_valid_snapshot()` |

Import paths differ by name. `Snapshot`, `SnapshotStore`,
`InMemorySnapshotStore`, and the four exception types are re-exported from the
top-level `eventsource` namespace. `PostgreSQLSnapshotStore` and
`SQLiteSnapshotStore` (plus its own `SQLITE_AVAILABLE` /
`SQLiteNotAvailableError`) are exported only from their respective adapter
packages — `eventsource.adapters.postgresql` /
`eventsource.adapters.sqlite`. The policy and scheduler classes and
`take_snapshot()` / `read_valid_snapshot()` are exported from
`eventsource.application.aggregates` but not from the top-level barrel —
import them from `eventsource.application.aggregates.snapshotting` directly.

Snapshots are usually not driven by hand: `AggregateRepository` accepts a
`snapshot_store` and a threshold, composes a `SnapshotPolicy` and
`SnapshotScheduler` from them, and restores from the stored state on load. The
manual API documented below is what that machinery is built on, and what you
use when you want snapshots taken at business milestones instead of on an
event count. See [ADR 0021](../adrs/0021-snapshot-policy-scheduler-composition.md)
for why the design is split this way and what it replaced.

## Overview

Three collaborating pieces make up the machinery.

`Snapshot` is a frozen dataclass holding six fields: `aggregate_id`,
`aggregate_type`, `version`, `state`, `schema_version`, and `created_at`. The
`state` field is a JSON-compatible `dict[str, Any]` — produced by the
aggregate's `_serialize_state()`, which uses Pydantic's `model_dump(mode="json")`
— so a snapshot carries no live objects and can be persisted by any backend
that can store JSON.

`SnapshotStore` is an `ABC` with five abstract coroutines:
`save_snapshot`, `get_snapshot`, `delete_snapshot`, `snapshot_exists`, and
`delete_snapshots_by_type`. Every store keeps at most one snapshot per
`(aggregate_id, aggregate_type)` pair; `save_snapshot` upserts rather than
appending, so reading back always yields the latest capture. Three backends
ship in-tree: `InMemorySnapshotStore` (dict-backed, for tests and single-process
use), `PostgreSQLSnapshotStore`, and `SQLiteSnapshotStore` (gated behind its
adapter package's own `SQLITE_AVAILABLE`, raising `SQLiteNotAvailableError`
when `aiosqlite` is not installed).

A `SnapshotPolicy` decides *when* a snapshot is written; a `SnapshotScheduler`
decides *how* the write is executed — two independent `@runtime_checkable`
`Protocol`s rather than the single merged `SnapshotStrategy` protocol ADR 0017
originally defined. `SnapshotPolicy` has one member,
`should_snapshot(aggregate, events_since_snapshot) -> bool`; the built-in
`EveryNEvents(n)` fires when a save carries the version across a multiple of `n`,
and `Never()` never fires (manual mode). `SnapshotScheduler` has
`async schedule(write, *, aggregate_type, aggregate_id) -> Snapshot | None`
plus `pending_count` and `await_pending()`; the built-in `ImmediateScheduler`
awaits the write inline, and `BackgroundScheduler` hands it to a
`BackgroundTaskManager` and returns `None` immediately. `take_snapshot()` is
the single free function that builds and saves a `Snapshot` from a live
aggregate — reading `schema_version` off the aggregate class (defaulting to
`1`), serializing state, and stamping `created_at` with `datetime.now(UTC)` —
and it is what both schedulers wrap.

Failure handling is deliberately lenient at the automatic write path. Both
`ImmediateScheduler.schedule` and `BackgroundScheduler`'s background task
catch every `Exception` raised by the write they were given, log a warning
with a traceback, and return `None` — a snapshot that cannot be written never
fails the save that triggered it. `take_snapshot()` itself does not catch
anything: the manual path (`AggregateRepository.create_snapshot()`) calls it
directly, with no scheduler in between, so it propagates errors. The exception
types in `eventsource.domain.exceptions` (`SnapshotError` and its three subclasses —
note they do *not* derive from `EventSourceError`) name problems on the
*read* path — corrupt state, a schema version the code no longer understands,
or a snapshot expected to exist but absent. No in-tree code currently raises
them: they are a published vocabulary for custom stores and restore logic,
not exceptions the shipped backends throw.

## `Snapshot`

```python
from eventsource import Snapshot
```

Defined in `eventsource.ports.snapshots`. A frozen dataclass
(`@dataclass(frozen=True)`) that captures one aggregate's serialized state at
one version. All six fields are required positional-or-keyword parameters with
no defaults; the constructor performs no validation beyond dataclass field
assignment.

```python
from datetime import UTC, datetime
from uuid import UUID

snapshot = Snapshot(
    aggregate_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
    aggregate_type="Order",
    version=100,
    state={"order_id": "550e8400-...", "status": "shipped", "items": []},
    schema_version=1,
    created_at=datetime.now(UTC),
)
```

### Fields

The six fields are declared in the order below, which is also the positional
constructor order. None has a default, so all six must be supplied.

| Field | Type | Meaning |
| --- | --- | --- |
| `aggregate_id` | `UUID` | Identifier of the aggregate instance |
| `aggregate_type` | `str` | Type name of the aggregate |
| `version` | `int` | Aggregate version at capture time |
| `state` | `dict[str, Any]` | JSON-compatible serialized state |
| `schema_version` | `int` | Version of the state schema |
| `created_at` | `datetime` | When the snapshot was created |

Together, `aggregate_id` and `aggregate_type` form the storage key: a
`SnapshotStore` holds at most one `Snapshot` per pair.

### `aggregate_id`

`UUID` — the aggregate instance this snapshot belongs to. It is the first half
of the storage key and the first argument of `get_snapshot`,
`delete_snapshot`, and `snapshot_exists`.

The type is `uuid.UUID`, not `str`. Strategies take it from
`aggregate.aggregate_id`, the read-only property on `AggregateRoot` backed by
the `UUID` passed to the aggregate's constructor. Passing a string where a
`UUID` is expected is not validated by `Snapshot.__init__` — the frozen
dataclass assigns whatever you give it — but it will not compare equal to the
`UUID` a store reads back, so lookups miss.

How the value is stored differs by backend, and all three round-trip a `UUID`
unchanged:

- `InMemorySnapshotStore` keys a dict on the `(aggregate_id, aggregate_type)`
  tuple, holding the `UUID` object itself.
- `PostgreSQLSnapshotStore` binds the `UUID` to a `uuid` column, with the
  upsert conflict target `(aggregate_id, aggregate_type)`.
- `SQLiteSnapshotStore` writes `str(aggregate_id)` and rehydrates with
  `UUID(row["aggregate_id"])` on read.

Stores treat the id as an opaque key. Nothing verifies that a matching
aggregate exists in the event store, that the snapshot's `version` is a version
that stream actually reached, or that the id belongs to the named
`aggregate_type` — a snapshot saved under the wrong pair is simply invisible to
the load path, which falls back to a full event replay.

### `aggregate_type`

`str` — the aggregate's type name, e.g. `"Order"` or `"User"`. It is the second
half of the storage key and the second argument of `get_snapshot`,
`delete_snapshot`, and `snapshot_exists`, and the only argument of
`delete_snapshots_by_type`.

The value is a plain string, not a class object, and nothing derives it from
`type(aggregate).__name__` at snapshot time. Strategies receive it as the
`aggregate_type` parameter of `execute_snapshot(aggregate, snapshot_store,
aggregate_type)` and copy it verbatim into the `Snapshot`. In the normal path
that parameter originates from `AggregateRepository`, which either takes the
`aggregate_type=` constructor argument or infers it from the aggregate class's
`aggregate_type` class attribute. `AggregateRoot` declares that attribute as
`ClassVar[str]` with no default — an aggregate class that never sets it fails
even earlier, at aggregate construction, with `AggregateTypeNotSetError`.
Inference itself explicitly rejects `""`, so an aggregate class that manages
to leave it empty still makes `AggregateRepository.__init__` raise
`ValueError` with instructions to declare the attribute or pass the argument:

```python
class OrderAggregate(DeclarativeAggregate[OrderState]):
    aggregate_type = "Order"  # without this, construction raises AggregateTypeNotSetError
```

Note that the guard lives in the repository, not in `Snapshot` or the stores. A
hand-built snapshot may carry `"Unknown"`, an empty string, or any other value;
it will be stored and retrieved under exactly that key.

Backends persist the string as-is: `InMemorySnapshotStore` uses it as the second
element of its `(aggregate_id, aggregate_type)` dict key,
`PostgreSQLSnapshotStore` binds it to the `aggregate_type` column and names it
in the upsert's `ON CONFLICT (aggregate_id, aggregate_type)` target, and
`SQLiteSnapshotStore` compares it with `AND aggregate_type = ?`. Matching is
therefore exact and case-sensitive everywhere — `"Order"` and `"order"` are two
separate keys.

Because the type name participates in the key, changing an aggregate's
`aggregate_type` orphans every snapshot written under the old name: lookups
under the new name miss, loads fall back to a full event replay, and the old
rows stay until you remove them with `delete_snapshots_by_type(old_name)`.
Renaming the Python class alone is harmless — only the attribute's value
matters.

### `version`

`int` — the aggregate version at the moment of capture. Every event with
version `<= version` is already folded into `state`; nothing at or below it
needs replaying.

The value comes from `AggregateRoot.version`, a read-only property returning
the internal `_version` counter, which is the number of events applied to the
aggregate. `take_snapshot()` — the single function both the manual
(`AggregateRepository.create_snapshot()`) and automatic (scheduler-wrapped)
paths call — builds the `Snapshot` with `version=aggregate.version` at the
moment it runs, after the repository has appended the pending events, so a
library-produced snapshot always matches the version just persisted to the
event store.

The load path is what gives the field its meaning. `AggregateRepository.load()`
starts with `from_version = 0`, and when a valid snapshot is found it sets
`from_version = snapshot.version`, fetches events with that bound, restores via
`aggregate._restore_from_snapshot(snapshot.state, snapshot.version)` — which
assigns `self._version = version` directly — and then replays the fetched
events on top. The store's `from_version` filter is *exclusive*
(`WHERE version > :from_version`), which is exactly what makes "events at or
below `version` are already applied" the correct reading.

Two consequences for hand-built snapshots, since neither `Snapshot.__init__`
nor any store validates the number against the actual stream:

- **Too high** — events between the real capture point and `version` are never
  fetched, and the aggregate silently comes back missing them.
- **Too low** — events already reflected in `state` are replayed on top of it,
  double-applying their effects.

Restoration failure is caught, but a *wrong* version is not. If
`_restore_from_snapshot` raises (state that no longer validates against the
state model), the repository logs a warning, re-fetches from version `0`, and
rebuilds from a fresh aggregate instance. A version that is merely inaccurate
raises nothing and produces a wrong aggregate.

`version` also drives the default snapshot cadence: `EveryNEvents(n).should_snapshot()`
returns true when the save moved the version out of one block of `n` and into a
later one, so with `snapshot_threshold=100` (mapped internally to
`EveryNEvents(100)`) a snapshot is written by whichever save first carries the
version past 100, then past 200, and so on. The version it lands on need not be
a multiple: an aggregate emitting several events per command advances in
strides, and a stride need never coincide with a multiple at all. See
[ADR 0049](../adrs/0049-snapshot-boundary-crossing.md).

The field is not a store-level ordering key. Because each
`(aggregate_id, aggregate_type)` pair holds at most one row, `save_snapshot`
overwrites unconditionally: `InMemorySnapshotStore` replaces the dict entry
(logging `v%d -> v%d` at debug level) and `PostgreSQLSnapshotStore`'s upsert
sets `version = EXCLUDED.version`. Neither compares the incoming version
against the stored one, so saving an older snapshot over a newer one succeeds.

### `state`

`dict[str, Any]` — the aggregate's serialized state at `version`, as a
JSON-compatible dictionary. This is the payload the snapshot exists to carry;
every other field describes it.

Library-produced snapshots fill it from `AggregateRoot._serialize_state()`,
which `take_snapshot()` calls. That method is a thin wrapper
over Pydantic:

```python
def _serialize_state(self) -> dict[str, Any]:
    if self._state is None:
        return {}
    return self._state.model_dump(mode="json")
```

`mode="json"` is what makes the result storable: nested models flatten to
dicts, and `UUID`s, `datetime`s, `Decimal`s and enums come out as JSON-native
values rather than Python objects. Note the `None` branch — an aggregate whose
state model has not been initialized snapshots as `{}`, not as a failure.

The reverse trip is `_restore_from_snapshot(state_dict, version)`, called by
`AggregateRepository.load()`. An empty dict is treated as "no state": the
version is assigned and the aggregate is left at its initial state. Otherwise
the dict is validated back into the aggregate's state type:

```python
state_type = self._get_state_type()
self._state = state_type.model_validate(state_dict)
self._version = version
```

Round-tripping therefore goes through Pydantic validation, not raw assignment —
a stored dict that no longer satisfies the current state model raises
`ValidationError`. The repository catches it, logs a warning with a traceback,
re-fetches events from version `0`, and rebuilds from a fresh aggregate
instance, so a stale or corrupt payload costs a full replay rather than an
error.

**Keep the dictionary JSON-serializable.** The three backends do not agree on
how much they check:

- `InMemorySnapshotStore` stores the `Snapshot` object itself in a
  `dict[tuple[UUID, str], Snapshot]`. Nothing is serialized, so any Python
  object in `state` survives — including ones the other backends cannot store.
- `PostgreSQLSnapshotStore` binds `json.dumps(snapshot.state)` on write, and on
  read parses `row.state` with `json.loads` when the driver hands back a `str`
  (leaving it as-is when the driver has already decoded the JSON column).
- `SQLiteSnapshotStore` writes `json.dumps(snapshot.state)` into a text column
  and reads it back with `json.loads(row["state"])`.

The practical consequence: a hand-built snapshot carrying non-JSON values passes
in tests against `InMemorySnapshotStore` and raises `TypeError` from
`json.dumps` the first time it hits Postgres or SQLite. Serialize with
`model_dump(mode="json")` — or an equivalent — rather than `model_dump()`.

Note also that `state` is the reason `Snapshot` is only shallowly immutable: the
dataclass is frozen, but the dict it holds is not, and it is what makes
`hash(snapshot)` raise `TypeError`. See
[Immutability and string representation](#immutability-and-string-representation).
`__repr__` deliberately prints `state_keys=[...]` instead of the values, so
logging a snapshot does not spill state contents into logs.

### `schema_version`

`int` — the version of the state schema that produced `state`. Strategies read
it from the aggregate class attribute `AggregateRoot.schema_version`, which
defaults to `1`. Increment it on the aggregate whenever a change to the state
model makes previously written snapshots unreadable; a snapshot whose
`schema_version` no longer matches the aggregate's is treated as invalid and
the aggregate is rebuilt from events instead.

`delete_snapshots_by_type(aggregate_type, schema_version_below=N)` is the bulk
counterpart, purging snapshots left behind by earlier schema versions.

### `created_at`

`datetime` — when the snapshot was taken. Strategies stamp it with
`datetime.now(UTC)`, so library-produced snapshots are always timezone-aware.
The field is informational: no store or load path compares it, expires
snapshots on age, or uses it for ordering. Supply an aware datetime in
hand-built snapshots to stay consistent with what the backends read back.

### Immutability and string representation

`frozen=True` means field assignment raises `dataclasses.FrozenInstanceError`:

```python
snapshot.version = 101  # FrozenInstanceError: cannot assign to field 'version'
```

The freeze is shallow. `state` is an ordinary `dict` and remains mutable —
`snapshot.state["extra"] = 1` succeeds and mutates the snapshot's contents.
Treat `state` as read-only, and copy it before modifying if you need a variant.

Two consequences of the generated dataclass methods:

- **Equality** is field-by-field, so two snapshots with identical fields compare
  equal even if constructed separately.
- **Hashing fails.** `frozen=True` generates `__hash__`, but it hashes the field
  tuple, and `state` is a `dict` — `hash(snapshot)` raises `TypeError:
  unhashable type: 'dict'`. Snapshots cannot be used as dict keys or set
  members.

Both `__str__` and `__repr__` are hand-written. `__str__` is a compact
identity line:

```python
>>> print(snapshot)
Snapshot(Order/550e8400-e29b-41d4-a716-446655440000, v100, schema_v1)
```

`__repr__` adds `created_at` and, instead of dumping the whole payload, lists
only `state_keys` — so logging a snapshot at debug level will not spill state
values into logs:

```python
>>> repr(snapshot)
"Snapshot(aggregate_id=UUID('550e8400-...'), aggregate_type='Order', version=100, schema_version=1, state_keys=['order_id', 'status', 'items'], created_at=datetime.datetime(...))"
```
