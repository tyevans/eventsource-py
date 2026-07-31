# Types and Protocols

Reference for the type vocabulary of `eventsource`: the semantic aliases in
`eventsource.types` and the handler/subscriber contracts in
`eventsource.protocols`.

Two modules cover two different concerns:

- **`eventsource.types`** — plain aliases (`AggregateId`, `EventId`,
  `Version`, ...) and the `TState` type variable. These name intent in
  signatures; they add no runtime behavior.
- **`eventsource.protocols`** — the canonical definitions of every handler and
  subscriber contract in the library: three `runtime_checkable` Protocols
  (`EventHandler`, `SyncEventHandler`, `FlexibleEventHandler`), one Protocol
  subscriber (`FlexibleEventSubscriber`), and two abstract base classes
  (`EventSubscriber`, `AsyncEventHandler`).

Both modules are re-exported from the package root, so
`from eventsource import EventHandler, AggregateId` works. `eventsource.protocols`
is the canonical import location for the protocol names; the aliases `Version`,
`StreamPosition`, and `GlobalPosition` are *not* re-exported at the root and
must be imported from `eventsource.types`.

The sections below list each name, its definition, and its required members.

## Overview

`eventsource` keeps its type vocabulary in two small, dependency-light modules.
Neither defines runtime machinery: `eventsource.types` is a flat list of
assignments, and `eventsource.protocols` contains only `Protocol` classes and
ABCs whose sole import from the library is `DomainEvent`.

Together they answer two questions:

- **What does this `UUID`/`int` mean?** — answered by the aliases and the
  `TState` type variable in `eventsource.types`.
- **What must my class provide to be used as a handler or subscriber?** —
  answered by the six contracts in `eventsource.protocols`.

### Module layout: `eventsource.types` vs `eventsource.protocols`

| | `eventsource.types` | `eventsource.protocols` |
| --- | --- | --- |
| Contents | `TState` plus the aliases `AggregateId`, `EventId`, `TenantId`, `CorrelationId`, `CausationId`, `Version`, `StreamPosition`, `GlobalPosition` | `EventHandler`, `SyncEventHandler`, `FlexibleEventHandler`, `FlexibleEventSubscriber`, `EventSubscriber`, `AsyncEventHandler` |
| Kind | Type aliases and one `TypeVar` | Four `Protocol`s (all `@runtime_checkable`) and two ABCs |
| Runtime effect | None — the aliases *are* `UUID`, `UUID \| None`, and `int` | Protocols support `isinstance()`; ABCs enforce abstract methods at instantiation |
| `__all__` | Not defined | Defined — this module is the canonical import location for the contracts |
| Library imports | `pydantic.BaseModel` only (for the `TState` bound) | `eventsource.events.base.DomainEvent` only |

Most names in both modules are re-exported from the package root, so
`from eventsource import EventHandler, AggregateId, TState` works. The three
ordering aliases — `Version`, `StreamPosition`, and `GlobalPosition` — are the
exception: they are not in the root `__all__` and must be imported from
`eventsource.types` directly.

## Type Aliases (`eventsource.types`)

`eventsource.types` contains nine names: eight plain assignments and one
`TypeVar`. Every alias resolves to a stdlib type (`UUID`, `UUID | None`, or
`int`); the module's only third-party import is `pydantic.BaseModel`, used as
the bound for `TState`.

| Name | Definition | Used for |
| --- | --- | --- |
| `AggregateId` | `UUID` | The identity of an aggregate / event stream |
| `EventId` | `UUID` | The identity of a single event instance |
| `TenantId` | `UUID \| None` | Multi-tenancy scope; `None` means "no tenant" |
| `CorrelationId` | `UUID` | Groups events belonging to one logical flow |
| `CausationId` | `UUID \| None` | The `event_id` of the event that caused this one |
| `Version` | `int` | Aggregate version, used for optimistic locking |
| `StreamPosition` | `int` | Position within one aggregate's stream |
| `GlobalPosition` | `int` | Position across all events in the store |
| `TState` | `TypeVar("TState", bound=BaseModel)` | Aggregate state model parameter |

### Identity aliases

#### `AggregateId` — `UUID`

Identifies the aggregate an event belongs to, and therefore the stream it is
appended to. It appears as `DomainEvent.aggregate_id` and as the `aggregate_id`
argument on `EventStore` methods such as `append_events()` and `get_events()`,
which pair it with `aggregate_type` to form the stream key
(`"aggregate_id:aggregate_type"`).

#### `EventId` — `UUID`

Identifies one event instance. `DomainEvent.event_id` is a
`UUID` field with `default_factory=uuid4`, so every event is uniquely
identified from construction — you never assign it yourself.

It is the value another event's `causation_id` points at:
`with_causation(causing_event)` copies `causing_event.event_id` into the new
event's `causation_id`, and `is_caused_by(other)` compares
`self.causation_id == other.event_id`.

On the store side, `StoredEvent.event_id` is a read-only property delegating to
`self.event.event_id`, and `EventStore.event_exists(event_id: UUID) -> bool`
looks an event up by this identity — the basis for idempotent appends.

#### `TenantId` — `UUID | None`

The tenant an event belongs to. `DomainEvent.tenant_id` is declared
`UUID | None` with `default=None`, so a plain `DomainEvent` subclass is
untenanted unless you pass a tenant explicitly — hence the optional alias.

Two ways to make the tenant non-optional in practice:

- **`TenantDomainEvent`** (`eventsource.multitenancy`) narrows the field to a
  required `tenant_id: UUID`. Construction fails validation if no tenant is
  supplied.
- **Tenant context** — `set_current_tenant()`, `tenant_scope()`, and
  `tenant_scope_sync()` hold the current tenant in a `contextvars` variable
  typed `UUID | None`. `TenantDomainEvent.with_tenant_context(**kwargs)` reads
  it and populates `tenant_id` for you, raising `TenantContextNotSetError` when
  the context is unset and no explicit `tenant_id` was passed. `get_current_tenant()`
  returns `UUID | None`; `get_required_tenant()` returns `UUID` or raises.

Downstream, the optionality survives into storage and querying: the PostgreSQL
store writes `str(event.tenant_id) if event.tenant_id else None`, and
`ReadOptions(tenant_id=...)` filters a read to one tenant when set, leaving
results unfiltered when `None`. So `None` consistently means "no tenant
scoping", never "unknown tenant".

#### `CorrelationId` — `UUID`

Links events that belong to the same logical flow — a request, a saga, a
command handler run — potentially spanning several aggregates.
`DomainEvent.correlation_id` is declared `UUID` with `default_factory=uuid4`,
so it is never `None`: an event that inherits no correlation simply starts its
own group.

Two methods work with it:

- `with_causation(causing_event)` returns a `model_copy` that adopts *both*
  `causation_id=causing_event.event_id` and
  `correlation_id=causing_event.correlation_id` — this is how a correlation
  propagates down an event chain.
- `is_correlated_with(other)` returns `self.correlation_id == other.correlation_id`.

To set one explicitly, pass `correlation_id=` at construction, or use
`EventBuilder.with_correlation_id(correlation_id)` in tests.

The field survives transport and storage: `correlation_id` is in the store
layer's `DEFAULT_UUID_FIELDS`, so it round-trips as a `UUID` rather than a
string, and the Kafka and RabbitMQ buses emit it as a message header
(`str(event.correlation_id)`) for cross-service tracing.

#### `CausationId` — `UUID | None`

Points at the `event_id` of the event that directly caused this one, and
defaults to `None` for an event with no in-system cause.
`with_causation(causing_event)` sets it to `causing_event.event_id`;
`is_caused_by(other)` checks `self.causation_id == other.event_id`.

Correlation and causation differ in shape for that reason: a correlation always
exists (hence `UUID`), a cause may not (hence `UUID | None`).

### Ordering and concurrency aliases

All three are `int`. They are distinguished only by what the number counts.

#### `Version` — `int` (optimistic locking)

The aggregate's version: the number of events applied to it. `DomainEvent`
carries the post-event value as `aggregate_version`, and `EventStream.version`
reports the current version of a loaded stream (`0` for an empty one).

`EventStore.append_events()` takes an `expected_version` argument and raises
`OptimisticLockError` when it does not match the aggregate's current version.
On the store's `AppendResult`, `new_version` is the version after a successful
append; for a conflict, `AppendResult.conflicted(current_version)` reports the
actual current version instead.

#### `StreamPosition` — `int`

The 1-based position of an event within its own aggregate's stream, exposed as
`StoredEvent.stream_position`.

#### `GlobalPosition` — `int`

The 1-based position of an event across all events in the store, exposed as
`StoredEvent.global_position` and as `AppendResult.global_position` (the
position of the last appended event). This is the ordering projections and
subscriptions checkpoint against, since it is total across streams while
`StreamPosition` is only meaningful within one aggregate.

### Type variables

#### `TState` — `TypeVar` bound to `pydantic.BaseModel`

The single type variable in the module, declared as
`TypeVar("TState", bound=BaseModel)`. It parameterizes aggregate state:
`AggregateRoot` is declared `class AggregateRoot(Generic[TState], ABC)`, its
`state` property is typed `TState | None`, and subclasses bind it to their own
pydantic model. The `BaseModel` bound is what lets the framework validate,
copy, and snapshot state generically.

`TState` is re-exported from `eventsource`. It is declared in `eventsource.types`
and imported by `eventsource.domain.aggregate` (`AggregateRoot`); the
`aggregates` package it used to also be re-exported from no longer exists.

### Note: aliases are transparent, not distinct types (no runtime enforcement)

These are assignments, not `NewType` declarations and not subclasses. At
runtime `AggregateId is UUID` and `Version is int` — nothing distinguishes an
`AggregateId` from an `EventId`, or a `StreamPosition` from a `GlobalPosition`.
Consequences:

- A type checker will not flag passing an `EventId` where an `AggregateId` is
  expected. The aliases document intent; they do not police it.
- `isinstance(x, AggregateId)` is just `isinstance(x, UUID)`, and
  `isinstance(x, TenantId)` fails the way `isinstance(x, UUID | None)` does —
  do not use the aliases for runtime validation logic.
- Because they are transparent, you can pass plain `UUID` and `int` values
  everywhere the aliases appear; adopting them is a documentation choice.

Import the identity aliases and `TState` from the package root
(`from eventsource import AggregateId, EventId, TenantId, CorrelationId,
CausationId, TState`). Import `Version`, `StreamPosition`, and
`GlobalPosition` from `eventsource.types` — they are not in the root `__all__`.
