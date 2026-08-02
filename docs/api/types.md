# Types and Protocols

Reference for the type vocabulary of `eventsource`: the semantic aliases in
`eventsource.domain.types` and the handler/subscriber contracts in
`eventsource.ports.handlers`.

Two modules cover two different concerns:

- **`eventsource.domain.types`** — plain aliases for domain identities (`AggregateId`,
  `EventId`, `TenantId`, `CorrelationId`, `CausationId`). These name intent in
  signatures; they add no runtime behavior. The aggregate state parameter
  (formerly a module-level `TState` `TypeVar`) is now declared inline on
  `AggregateRoot[TState: BaseModel]` and `DeciderAggregate[TState: BaseModel,
  TCommand = object]` using native PEP 695 type-parameter syntax — it is no
  longer an importable name.
- **`eventsource.ports.handlers`** — the canonical definitions of every handler and
  subscriber contract in the library: three `runtime_checkable` Protocols
  (`EventHandler`, `SyncEventHandler`, `FlexibleEventHandler`), one Protocol
  subscriber (`FlexibleEventSubscriber`), and two abstract base classes
  (`EventSubscriber`, `AsyncEventHandler`).

Both modules are re-exported from the package root, so
`from eventsource import EventHandler, AggregateId` works. `eventsource.ports.handlers`
is the canonical import location for the protocol names.

The sections below list each name, its definition, and its required members.

## Overview

`eventsource` keeps its type vocabulary in two small, dependency-light modules.
Neither defines runtime machinery: `eventsource.domain.types` is a flat list of
assignments, and `eventsource.ports.handlers` contains only `Protocol` classes and
ABCs whose sole import from the library is `DomainEvent`.

Together they answer two questions:

- **What does this `UUID`/`int` mean?** — answered by the aliases in
  `eventsource.domain.types`.
- **What must my class provide to be used as a handler or subscriber?** —
  answered by the six contracts in `eventsource.ports.handlers`.

### Module layout: `eventsource.domain.types` vs `eventsource.ports.handlers`

| | `eventsource.domain.types` | `eventsource.ports.handlers` |
| --- | --- | --- |
| Contents | The identity aliases `AggregateId`, `EventId`, `TenantId`, `CorrelationId`, `CausationId` | `EventHandler`, `SyncEventHandler`, `FlexibleEventHandler`, `FlexibleEventSubscriber`, `EventSubscriber`, `AsyncEventHandler` |
| Kind | Type aliases | Four `Protocol`s (all `@runtime_checkable`) and two ABCs |
| Runtime effect | None — the aliases *are* `UUID` | Protocols support `isinstance()`; ABCs enforce abstract methods at instantiation |
| `__all__` | Not defined | Defined — this module is the canonical import location for the contracts |
| Library imports | None | `eventsource.domain.event.DomainEvent` only |

All names in both modules are re-exported from the package root, so
`from eventsource import EventHandler, AggregateId` works.

## Type Aliases (`eventsource.domain.types`)

`eventsource.domain.types` contains five identity aliases. Every alias
resolves to a stdlib type (`UUID`); the module has no third-party imports.

| Name | Definition | Used for |
| --- | --- | --- |
| `AggregateId` | `UUID` | The identity of an aggregate / event stream |
| `EventId` | `UUID` | The identity of a single event instance |
| `TenantId` | `UUID` | Multi-tenancy scope (optionality on the referencing field, not the type) |
| `CorrelationId` | `UUID` | Groups events belonging to one logical flow |
| `CausationId` | `UUID` | The `event_id` of the event that caused this one (optionality on the referencing field) |

### Identity aliases

#### `AggregateId` — `UUID`

Identifies the aggregate an event belongs to, and therefore the stream it is
appended to. It appears as `DomainEvent.aggregate_id` and as a component of
the `StreamId` passed to store methods such as `append()` and
`read_stream()`, which pair it with the aggregate/category type to form the
stream identity.

#### `EventId` — `UUID`

Identifies one event instance. `DomainEvent.event_id` is a
`UUID` field with `default_factory=uuid4`, so every event is uniquely
identified from construction — you never assign it yourself.

It is the value another event's `causation_id` points at:
`with_causation(causing_event)` copies `causing_event.event_id` into the new
event's `causation_id`, and `is_caused_by(other)` compares
`self.causation_id == other.event_id`.

On the store side, an `EventEnvelope`'s identity is accessed via
`envelope.event.event_id`, and `EventLookup.event_exists(event_id: UUID) ->
bool` looks an event up by this identity — the basis for idempotent appends.

#### `TenantId` — `UUID`

The tenant an event belongs to. `DomainEvent.tenant_id` is declared
`TenantId | None` (the identity is plain `UUID`; optionality belongs to the
referencing field, not the type). A plain `DomainEvent` subclass is
untenanted unless you pass a tenant explicitly.

Two ways to make the tenant non-optional in practice:

- **`TenantDomainEvent`** (`eventsource.domain.tenant_events`) narrows the field to a
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
`FeedReadOptions(tenant_id=...)` / `CategoryReadOptions(tenant_id=...)` filter
a read to one tenant when set, leaving results unfiltered when `None`. So
`None` consistently means "no tenant scoping", never "unknown tenant".

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

#### `CausationId` — `UUID`

Points at the `event_id` of the event that directly caused this one. The identity
is a plain `UUID`; optionality belongs to the referencing field (`DomainEvent.causation_id`
is `CausationId | None`), so an event can have no in-system cause.
`with_causation(causing_event)` sets it to `causing_event.event_id`;
`is_caused_by(other)` checks `self.causation_id == other.event_id`.

Correlation and causation differ in shape for that reason: a correlation always
exists in a field (hence `UUID`), a cause may not (hence `UUID | None`).

### Version and Position Information

**Aggregate versioning** — `DomainEvent` carries the post-event value as `aggregate_version`
(an `int`), and `StreamReader.get_stream_version()` reports the current version of a stream
(`0` for an empty one). `EventAppender.append()` takes an `expected: ExpectedVersion` argument
(built via `ExpectedVersion.any_()`, `.no_stream()`, `.stream_exists()`, or `.exact(version)`)
and raises `OptimisticLockError` when the actual version does not match.

**Global and stream positions** — Global positions (the ordered position across all events in
the store) are opaque, adapter-defined tokens represented by the `Position` value object
(see `eventsource.ports.positions`). Consumers may compare and persist a `Position` but must
not do arithmetic on it. This is what projections and subscriptions checkpoint against.
Stream positions (the position within one aggregate's stream) are exposed as `EventEnvelope.stream_version`.

Note: `Version`, `StreamPosition`, and `GlobalPosition` type aliases have been removed from
`eventsource.domain.types` to emphasize that positions are opaque adapter-owned tokens, not
plain integers — use the `Position` type from `eventsource.ports.positions` when working
with global feed positions.

### The aggregate state type parameter

`eventsource.domain.types` no longer declares a module-level `TState`
`TypeVar`. `AggregateRoot` and `DeclarativeAggregate` each declare their own
inline PEP 695 type parameter — `class AggregateRoot[TState: BaseModel](ABC)`
— and `DeciderAggregate` adds a second, defaulted one — `class
DeciderAggregate[TState: BaseModel, TCommand = object](AggregateRoot[TState])`.
The `BaseModel` bound is what lets the framework validate, copy, and snapshot
state generically; `TCommand`'s `object` default (PEP 696) means
`DeciderAggregate[OrderState]` still works without naming a command type.

`TState` is not an importable name — it is not re-exported from
`eventsource`, `eventsource.domain`, or anywhere else. Code that needs to
write a helper generic over aggregate state declares its own parameter, e.g.
`def f[T: BaseModel](a: AggregateRoot[T]) -> None: ...`.

### Note: aliases are transparent, not distinct types (no runtime enforcement)

These are assignments, not `NewType` declarations and not subclasses. At
runtime `AggregateId is UUID`, `TenantId is UUID`, etc. — nothing distinguishes
an `AggregateId` from an `EventId`. Consequences:

- A type checker will not flag passing an `EventId` where an `AggregateId` is
  expected. The aliases document intent; they do not police it.
- `isinstance(x, AggregateId)` is just `isinstance(x, UUID)`, and
  `isinstance(x, TenantId)` fails the way `isinstance(x, UUID | None)` does —
  do not use the aliases for runtime validation logic.
- Because they are transparent, you can pass plain `UUID` values everywhere
  the aliases appear; adopting them is a documentation choice.

Import the identity aliases from the package root:
`from eventsource import AggregateId, EventId, TenantId, CorrelationId, CausationId`.
