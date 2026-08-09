# Aggregates API Reference

Technical reference for the aggregate root base classes (entities ring,
`eventsource.domain.aggregate`) and the repository that loads and persists
them (use-case ring, `eventsource.application.aggregates.repository`).

## Overview

The package exposes three public classes:

| Name | Kind | Defined in |
| --- | --- | --- |
| `AggregateRoot[TState: BaseModel]` | Abstract base class with an inline PEP 695 type parameter | `eventsource.domain.aggregate` |
| `DeclarativeAggregate[TState: BaseModel]` | Abstract base class (subclass of `AggregateRoot`), same inline parameter | `eventsource.domain.aggregate` |
| `AggregateRepository[TAggregate: AggregateRoot[Any]]` | Concrete class with an inline PEP 695 type parameter | `eventsource.application.aggregates.repository` |

`AggregateRoot` is declared `class AggregateRoot[TState: BaseModel](ABC)`:
`TState` is the Pydantic model that holds the aggregate's state, scoped to the
class itself rather than a module-level `TypeVar`, and subclasses must
implement the abstract methods `_apply()` and `_get_initial_state()`.
`DeclarativeAggregate` replaces the
hand-written `_apply()` dispatch with handlers registered via the `@handles`
decorator from `eventsource.domain.decorators`. `AggregateRepository` is not generic over
state — it is parameterized by the aggregate class itself and mediates between
an aggregate and an `AggregateStore` (the `EventAppender` + `StreamReader`
ports), an optional `EventPublisher`, and an optional `SnapshotStore`.

Every method, property, and class attribute documented below is described as it
behaves in the current source. Sections marked with a leading underscore
(`_apply`, `_serialize_state`, and similar) are documented because subclasses or
the repository rely on them; they are not part of the stable public surface for
application code unless explicitly noted as a subclass hook.

Two further modules back the repository's snapshot behaviour:
`eventsource.application.aggregates.snapshotting` (`SnapshotPolicy`,
`SnapshotScheduler`, and the `take_snapshot()` / `read_valid_snapshot()`
functions — see [ADR 0021](../adrs/0021-snapshot-policy-scheduler-composition.md))
and, for background scheduling, the
`eventsource.application.background_tasks` module. Neither is re-exported from
`eventsource.application.aggregates` except where noted, and both are
documented here only through the repository methods and properties that
expose their effects; the [Snapshots reference](snapshots.md) covers
`snapshotting.py` directly.

For task-oriented material — modelling a new aggregate, wiring snapshots, or
choosing between the declarative and imperative styles — see the guides under
`docs/guides/`, in particular
[Repository Operations](../guides/repository-operations.md) and
[Snapshotting](../guides/snapshotting.md). This page assumes you already know
what you want to call and need the exact signature and semantics.

## Import Surface

`AggregateRoot` and `DeclarativeAggregate` live in the entities ring:

```python
from eventsource.domain.aggregate import AggregateRoot, DeclarativeAggregate
```

`eventsource.domain.__init__.py` re-exports both, plus `StreamId` and
`CATEGORY_PATTERN` (unrelated to aggregates). `AggregateRepository` and the
snapshotting collaborators live one ring out, in `eventsource.application.aggregates`,
whose `__all__` lists exactly `AggregateRepository`,
`BackgroundScheduler`, `EveryNEvents`, `ImmediateScheduler`, `Never`,
`SnapshotPolicy`, `SnapshotScheduler`, `read_valid_snapshot`, `take_snapshot`:

```python
from eventsource.application.aggregates import AggregateRepository
```

Their defining modules are:

| Exported name | Defined in |
| --- | --- |
| `AggregateRoot` | `eventsource.domain.aggregate` |
| `DeclarativeAggregate` | `eventsource.domain.aggregate` |
| `AggregateRepository` | `eventsource.application.aggregates.repository` |

`TState` is not an importable name anywhere in the library. It is declared
inline, once per class, as a PEP 695 type parameter:
`class AggregateRoot[TState: BaseModel](ABC)` and `class
DeclarativeAggregate[TState: BaseModel](AggregateRoot[TState], ABC)`. Code
that needs its own generic helper over aggregate state declares its own
parameter, e.g. `def f[T: BaseModel](a: AggregateRoot[T]) -> None: ...`.

The same is true of `TAggregate`: the repository is declared
`class AggregateRepository[TAggregate: AggregateRoot[Any]]`, so the parameter
is scoped to that class and is not importable either.

### Preferred import path

The three classes are also re-exported from the top-level package, and that is
the intended import path for application code:

```python
from eventsource import AggregateRoot, AggregateRepository, DeclarativeAggregate
```

Neither `TState` nor `TAggregate` is part of the public surface — both are
class-scoped PEP 695 type parameters, so there is nothing to import for
either. A helper that needs to be generic over a repository's aggregate type
declares its own parameter:

```python
def f[A: AggregateRoot[Any]](repo: AggregateRepository[A]) -> None: ...
```

### Names not exported at the top level

`SnapshotPolicy`, `SnapshotScheduler`, `EveryNEvents`, `Never`,
`ImmediateScheduler`, `BackgroundScheduler`, `take_snapshot`, and
`read_valid_snapshot` are exported from `eventsource.application.aggregates`
but not from the top-level package — import them path-only if you need to
pass a custom policy or scheduler to `AggregateRepository`. Their behaviour
reaches application code by default only through `AggregateRepository`'s
snapshot properties and methods, documented under
[Snapshot Surface](#snapshot-surface); see the
[Snapshots reference](snapshots.md) for the collaborators themselves.

Symbols that support these classes but live elsewhere — the `@handles`
decorator (`eventsource.domain.decorators`), `DomainEvent` (`eventsource.domain.event`), the
`AggregateStore` / `EventPublisher` / `SnapshotStore` contracts
(`eventsource.ports.store`, `eventsource.ports.bus`, `eventsource.ports.snapshots`),
and the exceptions raised by this package (`eventsource.domain.exceptions`) — are all
re-exported from the top level and are referenced by their public names
throughout this page.

## `AggregateRoot[TState]`

```python
class AggregateRoot[TState: BaseModel](ABC):
    def __init__(self, aggregate_id: UUID) -> None: ...
```

Defined in `eventsource.domain.aggregate`. The abstract base for every
event-sourced aggregate. An instance owns three pieces of mutable state — the
current version, the current `TState` model (or `None`), and a list of
uncommitted `DomainEvent`s — and mutates all three through a single entry
point, `apply_event()`.

`TState` is scoped to the class via the inline `[TState: BaseModel]` type
parameter, so the state model must be a Pydantic model. That bound is what
makes `_serialize_state()` and
`_restore_from_snapshot()` work: they call `model_dump(mode="json")` and
`model_validate()` on the concrete type recovered by `_get_state_type()`.

### Required subclass implementations

`AggregateRoot` declares exactly two `@abstractmethod`s:

| Method | Signature | Purpose |
| --- | --- | --- |
| `_apply` | `(self, event: DomainEvent) -> None` | Mutate `self._state` in response to one event |
| `_get_initial_state` | `(self) -> TState \| None` | Produce the starting state for a new aggregate |

A subclass that leaves either unimplemented cannot be instantiated.
`DeclarativeAggregate` supplies both — `_apply()` as a registry lookup and
`_get_initial_state()` as a `requires_creation_event`-dependent default — which
is why declarative subclasses only need `@handles` methods.

### The state lifecycle

Three operations move an aggregate through its lifecycle, and all of them
funnel into `apply_event()`:

1. **Command methods** you write call `create_event()` (or `_raise_event()`)
   with `is_new=True`. The event is version-validated, applied, and appended to
   `_uncommitted_events`.
2. **Rehydration** calls `load_from_history()`, which replays each event with
   `is_new=False` — no version validation, no uncommitted tracking.
3. **Commit** calls `mark_events_as_committed()` or
   `clear_uncommitted_events()` after the repository has persisted the events.

`_state` starts as `None` in `__init__()`; nothing in `AggregateRoot` calls
`_get_initial_state()` automatically. Whether an imperative subclass seeds
state eagerly or lets the first event handler build it is the subclass's
choice, and `AggregateRoot.state` is typed `TState | None` to reflect that.
`DeclarativeAggregate` narrows the property to `TState` and raises instead of
returning `None`.

### Class-level configuration

`AggregateRoot` defines four class attributes that subclasses override to
change behaviour: `aggregate_type`, `schema_version`, `validate_versions`, and
`_event_handlers`. They are covered in
[Class Attributes](#class-attributes). The most consequential is
`aggregate_type` — it has no default and must be declared by every concrete
subclass (construction raises `AggregateTypeNotSetError` otherwise), is
stamped onto every event produced by `create_event()`, and causes
`AggregateRepository` to raise `ValueError` if left empty (see
[`aggregate_type` Inference](#aggregate_type-inference)).

### Identity semantics

`__eq__` and `__hash__` are defined on `aggregate_id` alone. Two instances of
different aggregate classes that happen to share an ID compare equal, and an
aggregate's hash does not change as events are applied — deliberate, since an
aggregate is an entity identified by its ID rather than by its contents. See
[Identity](#identity-__repr__-__eq__-__hash__).

### Module-level names

Besides the two classes, `eventsource.domain.aggregate` exports the type alias
`UnregisteredEventHandling = str` (the `"ignore" | "warn" | "error"` values
accepted by `DeclarativeAggregate.unregistered_event_handling`). It is in that
module's `__all__` but is not re-exported from `eventsource.domain` or the
top-level package. The module also defines, without exporting, the alias
`EventHandler = Callable[[DomainEvent], None]` and the type variable
`TEvent = TypeVar("TEvent", bound=DomainEvent)` used by `create_event()`.

### Class Attributes

`AggregateRoot` declares four class-level attributes. `aggregate_type` is
annotated `ClassVar[str]` with no default; the other three are plain class
attributes, so an instance assignment such as `aggregate.validate_versions =
False` shadows the class value for that instance only.

| Attribute | Declared type | Default | Read by |
| --- | --- | --- | --- |
| `aggregate_type` | `ClassVar[str]` | *none — required* | `create_event()`, `AggregateRepository`, snapshot storage |
| `schema_version` | `int` | `1` | Snapshot write and load paths |
| `validate_versions` | `bool` | `True` | `apply_event()` |
| `_event_handlers` | `dict[type[DomainEvent], str]` | `{}` | `DeclarativeAggregate` only |

#### `aggregate_type`

The string identifier stamped onto every event the aggregate produces and used
as the partition key for snapshots. `create_event()` copies it into the event's
`aggregate_type` field, and `AggregateRepository` uses it to scope event-store
and snapshot-store reads.

There is no default, and it is the one class attribute every concrete
subclass must declare — `AggregateRoot.__init__` raises
`AggregateTypeNotSetError` if it's unset. `AggregateRepository._infer_aggregate_type()`
reads `factory.aggregate_type` and raises `ValueError` when the value is
empty, with a message naming both remedies — declare the class
attribute, or pass `aggregate_type=` to the repository constructor. See
[`aggregate_type` Inference](#aggregate_type-inference).

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"
```

#### `schema_version`

An integer describing the shape of `TState`, used solely for snapshot
compatibility. It has no effect on events or on replay from the event store.

On write, `take_snapshot()` reads `getattr(type(aggregate), "schema_version", 1)`
and stores it on the `Snapshot` record (the `snapshots` table has a
`schema_version` column with an index on `(aggregate_type, schema_version)`).
On load, `read_valid_snapshot()` compares the stored value against the
aggregate class's current value; on mismatch it logs at INFO level, discards
the snapshot, and falls back to a full event replay. **Nothing raises** —
`SnapshotSchemaVersionError` exists in `eventsource.domain.exceptions` but
is not raised by this comparison; see [who raises the snapshot
exceptions](../guides/snapshotting.md#who-raises-the-snapshot-exceptions-and-how-to-opt-into-strictness)
for what that type is for. The fallback is counted as
`eventsource.snapshot.miss{reason="schema_mismatch"}`, which is how a
mismatch is observable without reading logs — see [count the
degradation](../guides/snapshotting.md#count-the-degradation-eventsourcesnapshotmiss). See
[ADR 0021](../adrs/0021-snapshot-policy-scheduler-composition.md) for the
collaborators that replaced `AggregateSnapshotManager`.

Increment `schema_version` whenever a change to `TState` would make an existing
serialized snapshot invalid — a new required field, a renamed field, a changed
type. Additive changes with defaults do not require an increment, since
`model_validate()` will accept the older payload. Stale snapshots left behind
by an increment can be reclaimed with the snapshot store's
`delete_snapshots(..., schema_version_below=N)` argument.

#### `validate_versions`

Controls what `apply_event()` does when a new event's `aggregate_version` does
not equal `self.version + 1`. The check runs only for `is_new=True`; historical
replay through `load_from_history()` is never version-validated.

| Value | Behaviour on mismatch |
| --- | --- |
| `True` (default) | Raises `EventVersionError` carrying `expected_version`, `actual_version`, `event_id`, and `aggregate_id` |
| `False` | Logs a warning with the same fields in `extra`, then applies the event anyway |

Leave this at `True`. Setting it to `False` is a testing and migration
affordance — it lets a fixture apply an out-of-sequence event without
constructing a full history — and it removes the guard that keeps an
aggregate's in-memory version aligned with the event stream it will be written
to.

#### `_event_handlers`

The registry mapping event classes to the *name* of the method that handles
them. On `AggregateRoot` it is an empty dict that the base class never reads;
nothing in `AggregateRoot` consults it, because imperative subclasses dispatch
inside their own `_apply()`.

`DeclarativeAggregate.__init_subclass__` is what populates it: for each
subclass it assigns a **fresh** `cls._event_handlers = {}`, then walks `dir(cls)`
and records every attribute carrying a `_handles_event_type` marker (set by the
`@handles` decorator). Because `dir()` includes inherited members, a subclass
picks up its parent's handlers as well — but the dict object itself is not
shared, so registering a handler on a subclass never mutates the parent's
registry. `DeclarativeAggregate._apply()` looks the event type up here and
`_handle_unregistered_event()` reports the registered event names on a miss.

Treat the attribute as read-only introspection. Registration is the decorator's
job; see [`__init_subclass__` Handler Registration](#__init_subclass__-handler-registration).

#### `DeclarativeAggregate` additions

`DeclarativeAggregate` adds two attributes of its own, both genuine
`ClassVar`s: `requires_creation_event: ClassVar[bool] = False` and
`unregistered_event_handling: ClassVar[UnregisteredEventHandling] = "ignore"`.
They are documented under
[`requires_creation_event`](#requires_creation_event-classvar) and
[Event Routing](#event-routing-_apply-_handle_unregistered_event-handles).

### Constructor

```python
def __init__(self, aggregate_id: UUID) -> None
```

The only constructor parameter is the aggregate's identity. There is no
overload that accepts state, a version, or a list of events — an aggregate is
always built empty and then advanced by applying events.

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `aggregate_id` | `UUID` | *required* | Unique identifier for this aggregate instance |

`aggregate_id` is positional-or-keyword and has no default. The declared type
is `uuid.UUID`; nothing in `__init__()` coerces or validates it, so passing a
string leaves a string on the instance and the failure surfaces later — when an
event is constructed and Pydantic validates its `aggregate_id` field, or when
the event store is queried.

#### Instance attributes established

The body assigns exactly four attributes and calls nothing else — no hooks, no
state seeding, no registry lookups:

| Attribute | Initial value | Exposed by |
| --- | --- | --- |
| `_aggregate_id` | the `aggregate_id` argument | `aggregate_id` property |
| `_version` | `0` | `version` property |
| `_uncommitted_events` | `[]` | `uncommitted_events` (returns a copy) |
| `_state` | `None` | `state` property |

Note in particular that **`_get_initial_state()` is not called**. A freshly
constructed aggregate has `state is None` and `version == 0` regardless of
whether the subclass implements a meaningful initial state. State appears when
the first event is applied — either by a command method that calls
`create_event()`, or by `load_from_history()` during rehydration.

`DeclarativeAggregate` does not override `__init__()`; it inherits this one
unchanged. Its `state` property raises `AggregateNotCreatedError` rather than
returning `None`, so on a newly constructed declarative aggregate `state`
raises while `state_or_none` returns `None`. See
[Properties: `state`, `state_or_none`, `is_created`](#properties-state-state_or_none-is_created).

#### Constructing a new aggregate

```python
from uuid import uuid4

order = OrderAggregate(uuid4())
assert order.version == 0
assert not order.has_uncommitted_events

order.create(customer_id=customer_id)   # your command method raises the event
await repo.save(order)
```

Generate the identifier yourself (`uuid4()`) — the constructor will not invent
one. The same call is what `AggregateRepository.create_new()` performs on your
behalf: it returns `self._aggregate_factory(aggregate_id)` and nothing more, so
it is a convenience wrapper, not a different construction path.

#### Subclassing and `__init__()`

Most subclasses do not define `__init__()` at all. When one does — to accept
injected services, for example — it must forward the identifier:

```python
class OrderAggregate(AggregateRoot[OrderState]):
    aggregate_type = "Order"

    def __init__(self, aggregate_id: UUID, pricing: PricingPolicy | None = None) -> None:
        super().__init__(aggregate_id)
        self._pricing = pricing or DefaultPricingPolicy()
```

Any extra parameters must be optional. `AggregateRepository` instantiates the
class with a single positional argument in `load()`, `load_or_create()`, and
`create_new()`, so a subclass whose `__init__()` demands a second required
argument cannot be loaded through the repository.

Do not assign to `_state` or `_version` in `__init__()` to pre-seed an
aggregate. Doing so desynchronises the in-memory version from the event stream
and makes `apply_event()`'s version check reject the first legitimate event.
Seed state from an event handler instead.

### Properties: `aggregate_id`, `version`, `state`, `uncommitted_events`, `has_uncommitted_events`

`AggregateRoot` exposes five read-only properties. All five are getters with no
setter — assigning to any of them raises `AttributeError`. Each reads one of
the four instance attributes established by the constructor.

| Property | Return type | Backing attribute | Notes |
| --- | --- | --- | --- |
| `aggregate_id` | `UUID` | `_aggregate_id` | Fixed for the instance's lifetime |
| `version` | `int` | `_version` | `0` until the first event is applied |
| `state` | `TState \| None` | `_state` | `None` before any event is applied |
| `uncommitted_events` | `list[DomainEvent]` | `_uncommitted_events` | Returns a shallow copy |
| `has_uncommitted_events` | `bool` | `_uncommitted_events` | `len(...) > 0` |

#### `aggregate_id`

```python
@property
def aggregate_id(self) -> UUID
```

Returns the identifier passed to the constructor, unchanged. Nothing in
`AggregateRoot` ever reassigns `_aggregate_id`, so the value is stable across
the whole lifecycle — construction, replay, command handling, and commit. It is
the value `create_event()` stamps onto every event's `aggregate_id` field, the
value `EventVersionError` carries on a version mismatch, and the value `__eq__`
and `__hash__` are computed from.

#### `version`

```python
@property
def version(self) -> int
```

The version of the last event applied — not a count maintained by the
aggregate. `apply_event()` assigns `self._version = event.aggregate_version`
unconditionally, for both new and replayed events, so `version` always mirrors
the `aggregate_version` of the most recent event rather than being incremented
locally. For a well-formed stream starting at version 1 the two coincide, and
`version` reads as "number of events applied"; if `validate_versions` is
`False` and a gapped event is applied, `version` jumps to that event's value.

A freshly constructed aggregate has `version == 0`. Use `get_next_version()`
(which returns `self._version + 1`) rather than `version + 1` when building an
event by hand.

`AggregateRepository.save()` relies on this property for optimistic
concurrency: it computes `expected_version = aggregate.version -
len(uncommitted_events)` and passes that to the event store. This is why you
must not adjust `_version` outside `apply_event()` — the subtraction assumes
every uncommitted event advanced the version by exactly one.

#### `state`

```python
@property
def state(self) -> TState | None
```

The current state model, or `None` if no event has established one. The
constructor sets `_state = None` and does **not** call `_get_initial_state()`,
so `state is None` on every newly constructed `AggregateRoot`. The only code
that assigns `_state` is your `_apply()` implementation (or, for declarative
aggregates, a `@handles` method), plus `_restore_from_snapshot()` on the
snapshot path.

The `TState | None` union means callers must narrow before use:

```python
order = OrderAggregate(order_id)
assert order.state is None

order.create(customer_id=customer_id)
if order.state is not None:
    print(order.state.status)
```

`DeclarativeAggregate` overrides this property to return `TState` and to raise
`AggregateNotCreatedError` when `requires_creation_event` is `True` and
`_state` is still `None`; its `state_or_none` property preserves the
nullable-returning behaviour. See
[Properties: `state`, `state_or_none`, `is_created`](#properties-state-state_or_none-is_created).

Because `TState` is bound to `BaseModel` and events are frozen, the conventional
mutation in `_apply()` is `self._state = self._state.model_copy(update={...})`
rather than in-place attribute assignment.

#### `uncommitted_events`

```python
@property
def uncommitted_events(self) -> list[DomainEvent]
```

The events applied with `is_new=True` since the last commit, in application
order. The getter returns `self._uncommitted_events.copy()` — a new list each
call, so appending to or clearing the returned list has no effect on the
aggregate. The `DomainEvent` objects themselves are shared, not copied, but
`DomainEvent` is `frozen=True`, so they cannot be mutated through the copy
either.

Two consequences follow from the fresh-list-per-access behaviour. Identity
comparison across calls fails (`a.uncommitted_events is a.uncommitted_events`
is `False`), and repeated access in a loop allocates repeatedly — bind it once
if you need it more than once. The copy is also what lets
`eventsource.testing.bdd` capture "events raised by this command" by slicing
`aggregate.uncommitted_events[before_count:]` around a call.

Events land here only via `apply_event(..., is_new=True)`, which is what
`create_event()` and `_raise_event()` use. `load_from_history()` replays with
`is_new=False`, so a rehydrated aggregate has an empty list regardless of how
many events were replayed.

#### `has_uncommitted_events`

```python
@property
def has_uncommitted_events(self) -> bool
```

`True` when at least one event is pending persistence. Implemented as
`len(self._uncommitted_events) > 0`, evaluated against the backing list
directly — it does not build the copy that `uncommitted_events` returns, so
prefer it to `len(aggregate.uncommitted_events) > 0` or
`bool(aggregate.uncommitted_events)` for a pure emptiness check.

Both `mark_events_as_committed()` and `clear_uncommitted_events()` clear the
backing list, so this property reads `False` after either. `save()` short-
circuits to a no-op when there is nothing uncommitted, which makes the
post-save assertion a reliable way to confirm a command actually raised
something:

```python
order.ship()
assert order.has_uncommitted_events

await repo.save(order)
assert not order.has_uncommitted_events
```

### Event Application: `apply_event()`, `load_from_history()`, `get_next_version()`

These three methods are the whole of the state-mutation surface.
`apply_event()` is the single point through which `_version`, `_state`, and
`_uncommitted_events` change; `load_from_history()` is a loop over it; and
`get_next_version()` is the helper that supplies the version an event must
carry to pass its validation.

#### `apply_event()`

```python
def apply_event(self, event: DomainEvent, is_new: bool = True) -> None
```

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `event` | `DomainEvent` | *required* | The event to apply |
| `is_new` | `bool` | `True` | `True` for a freshly raised event; `False` for replay from history |

Returns `None`. Raises `EventVersionError` when `is_new=True`,
`validate_versions` is `True`, and `event.aggregate_version != self.version + 1`.
Any exception raised by the subclass's `_apply()` propagates unchanged —
including `UnhandledEventError` from a declarative aggregate configured with
`unregistered_event_handling = "error"`.

The body performs four steps in a fixed order:

1. **Version check** — only when `is_new=True`. `expected_version` is computed
   as `self._version + 1`. On mismatch, either raise `EventVersionError`
   (carrying `expected_version`, `actual_version`, `event_id`, and
   `aggregate_id`) or, when `validate_versions` is `False`, log a warning
   with those same fields in `extra` and continue.
2. **Version assignment** — `self._version = event.aggregate_version`,
   unconditionally, for both new and replayed events. The aggregate adopts the
   event's version rather than incrementing a counter of its own.
3. **State mutation** — `self._apply(event)` is called to update `self._state`.
4. **Tracking** — when `is_new=True`, the event is appended to
   `_uncommitted_events`.

Two consequences of that ordering are worth stating plainly. The version is
assigned **before** `_apply()` runs, so a handler that reads `self.version`
sees the version of the event it is currently handling, not the previous one.
And if `_apply()` raises, the version has already advanced while the event was
never appended to `_uncommitted_events` — the aggregate is left inconsistent.
There is no rollback. Discard the instance and reload it rather than continuing
to use an aggregate whose `_apply()` threw.

Historical replay skips step 1 entirely. `is_new=False` means no version
validation at all, so a stream with gaps or out-of-order versions replays
without complaint and leaves `version` equal to the last event's
`aggregate_version`. Ordering and contiguity are the event store's
responsibility, not the aggregate's.

```python
# New event: validated, applied, tracked for persistence
aggregate.apply_event(order_created, is_new=True)

# Replayed event: applied only
aggregate.apply_event(historic_event, is_new=False)
```

Application code rarely calls `apply_event()` directly for new events —
`create_event()` and `_raise_event()` both funnel into
`apply_event(event, is_new=True)`, and `create_event()` additionally fills in
`aggregate_version` for you. Call it directly only when you have constructed
the event by hand.

#### `load_from_history()`

```python
def load_from_history(self, events: list[DomainEvent]) -> None
```

| Parameter | Type | Default | Description |
| --- | --- | --- | --- |
| `events` | `list[DomainEvent]` | *required* | Historical events in chronological order |

Returns `None`. The implementation is a single loop:

```python
for event in events:
    self.apply_event(event, is_new=False)
```

Because every event goes in with `is_new=False`, replay never validates
versions and never populates `_uncommitted_events`. A rehydrated aggregate
therefore reports `has_uncommitted_events == False` no matter how long its
history, and `version` equals the `aggregate_version` of the final event in the
list. An empty list is a no-op: nothing is applied, `version` stays at its
current value, and `state` stays `None` on a fresh instance.

The method **does not reset** the aggregate before replaying. It applies events
on top of whatever state the instance already holds. That is deliberate — it is
what makes snapshot loading work, where state is first restored via
`_restore_from_snapshot()` and only the events recorded after the snapshot are
replayed on top:

```python
aggregate.load_from_history(events_since_snapshot)
```

The corollary is that calling `load_from_history()` twice with the same events
replays them twice. To rebuild from scratch, construct a new instance.

```python
stream = await event_store.get_events(aggregate_id, "Order")
aggregate = OrderAggregate(aggregate_id)
aggregate.load_from_history(stream.events)
assert aggregate.version == len(stream.events)   # for a gapless stream from v1
assert not aggregate.has_uncommitted_events
```

`AggregateRepository.load()` does exactly this on your behalf, including the
snapshot fast path. Call `load_from_history()` yourself only when driving an
aggregate from events you already hold — in tests, in migration tooling, or
when reading through a custom store path.

#### `get_next_version()`

```python
def get_next_version(self) -> int
```

Returns `self._version + 1` — the value `apply_event()` will expect in the next
new event's `aggregate_version` field. Takes no arguments, mutates nothing, and
can be called any number of times without effect. On a freshly constructed
aggregate it returns `1`.

It is the correct way to stamp a hand-built event:

```python
def ship(self) -> None:
    event = OrderShipped(
        aggregate_id=self.aggregate_id,
        aggregate_type=self.aggregate_type,
        event_type="OrderShipped",
        aggregate_version=self.get_next_version(),
    )
    self.apply_event(event)
```

Prefer it over writing `self.version + 1` inline: it keeps the "next version"
rule in one place, and it reads as the counterpart to the check inside
`apply_event()`. When raising several events from one command, call it once per
event and apply each before building the next — `apply_event()` advances
`_version`, so the second call returns the correct successor. Capturing the
value once and reusing it for two events produces an `EventVersionError` on the
second.

`create_event()` calls `get_next_version()` internally, so events built through
it never need the field supplied. See
[Event Creation: `create_event()`](#event-creation-create_event).
