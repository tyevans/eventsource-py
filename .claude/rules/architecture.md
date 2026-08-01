---
paths:
  - "src/eventsource/**/*.py"
---

# Architecture Rules

**The library follows Clean Architecture, hard DDD, and SOLID.**
Target layout is being migrated to in phases (see `docs/superpowers/specs/` for the
redesign specs); these rules govern all new and modified code now.

## The Dependency Rule

Source-code dependencies point only inward, toward higher-level policy. Nothing in an
inner ring may know anything about an outer ring — no names, no types, no imports.
The rings, innermost first:

1. **Entities** (`domain/` — during transition also `events/`): Enterprise business
   rules — domain events, the event registry, domain value objects, domain
   exceptions. Pure: stdlib + pydantic only. No I/O. `domain/aggregate.py`
   (`AggregateRoot`, `DeclarativeAggregate`) lives here now; `aggregates/` is no
   longer a transitional location for it. `domain/types.py` (type aliases:
   `AggregateId`, `EventId`, `TenantId`, `CorrelationId`, `CausationId`, `Version`,
   `StreamPosition`, `GlobalPosition`, `TState`), `domain/exceptions.py` (the full
   exception hierarchy, including the `SnapshotError` family and the lock
   exceptions), and `domain/command.py` (`DomainCommand`) are settled, not
   transitional; `types.py`, `exceptions.py`, and `commands/` at the top level are
   no longer transitional locations for any of it (ADR 0030).
2. **Use cases** (`application/` — during transition `subscriptions/`,
   `migration/`, `handlers/`): Application business rules —
   aggregate repositories, projection engines, subscription lifecycle, migration
   orchestration. Depends on entities and on the boundary ports it owns. Never on a
   concrete adapter, driver, or framework. `application/aggregates/`
   (`AggregateRepository` plus the `SnapshotPolicy`/`SnapshotScheduler`
   collaborators) is settled, not transitional; so is `application/projections/`
   (`Projection`/`CheckpointTrackingProjection`/`DeclarativeProjection`, the
   `ProjectionCoordinator`/`ProjectionRegistry`/`SubscriberRegistry` collaborators,
   the checkpoint and DLQ functions, and the retry policies) — `projections/` is no
   longer a transitional location for any of it.
3. **Interface adapters** (`adapters/` — during transition `stores/`,
   `bus/` backend modules): Gateways that
   implement the ports for a specific technology, converting between the use-case
   format (value objects, domain events) and the storage/wire format (rows, JSON,
   frames). Snapshot store backends (`InMemorySnapshotStore`,
   `PostgreSQLSnapshotStore`, `SQLiteSnapshotStore`) live under `adapters/memory/`,
   `adapters/postgresql/`, `adapters/sqlite/`; `snapshots/` is no longer a
   transitional adapter location. Checkpoint and DLQ adapters (dialect-parameterized
   for PostgreSQL and SQLite, plus `DatabaseProjection`) live under `adapters/sql/`;
   the in-memory checkpoint and DLQ adapters live under `adapters/memory/`.
   Outbox adapters are per-technology rather than dialect-parameterized — one
   module each under `adapters/memory/`, `adapters/postgresql/`, and
   `adapters/sqlite/`, since the SQLite implementation takes a raw
   `aiosqlite.Connection` rather than a sqlalchemy engine or session. The
   `repositories/` package no longer exists — it is not a transitional
   location for anything. `adapters/sync/` (`SyncEventStoreAdapter`) and
   `adapters/serialization/` (`EventSourceJSONEncoder`, `json_dumps`,
   `json_loads`) are settled, not transitional; top-level `sync/` and
   `serialization/` no longer exist (ADR 0030). Distributed lock adapters
   (`PostgreSQLLockManager`, `InMemoryLockManager`) live under
   `adapters/postgresql/` and `adapters/memory/`; read-model adapters live
   under `adapters/{memory,postgresql,sqlite}/`; top-level `locks/` and
   `readmodels/` no longer exist — there is no shim at either path (ADR 0030).
4. **Frameworks & drivers**: sqlalchemy, asyncpg, aiosqlite, redis, aiokafka,
   aio-pika. Imported only inside the adapter that needs them, always guarded
   (see below). Driver types never appear in port signatures.

The **public API** (`__init__.py`) is the delivery mechanism's front door: it
re-exports from all rings and is the only module users import from.

## Ports (Boundary Interfaces)

Ports are owned by the inner rings and implemented by adapters — dependencies are
inverted at every boundary crossing (the D in SOLID).

- Ports live in `ports/` (during transition also `*/interface.py`), depend only on
  entities, and contain no implementation code, ever. `ports/handlers.py`
  (`EventHandler`, `SyncEventHandler`, `FlexibleEventHandler`, `EventSubscriber`,
  `AsyncEventHandler`, `FlexibleEventSubscriber`) is settled, not transitional;
  top-level `protocols.py` is no longer a transitional location for it (ADR 0030).
- Our store/repository/bus ports are **output ports** (gateways) in Clean
  Architecture terms: the use-case ring calls them; adapters implement them.
- Ports are small, composed `Protocol` classes — one capability per port
  (e.g., appending, stream reading, global feed reading, type querying). Never grow a
  god-interface; add a new port instead (the I in SOLID).
- Consumers type-hint the narrowest port union they need, never a concrete adapter
  and never a wider port than they use.
- Optional capabilities are expressed by a backend *not implementing* a port — never
  by raising `NotImplementedError` from a method it claims to support.
- Mark ports `@runtime_checkable` only when a consumer genuinely needs isinstance
  checks.

## Value Objects (Boundary Crossing)

Data crosses ring boundaries as immutable value objects (frozen pydantic models or
frozen dataclasses), never as primitives, dicts, or driver types: positions, stream
identities, expected versions, event envelopes.

- Positions in the global feed are **opaque ordered tokens** defined by the adapter —
  consumers may compare and persist them, never do arithmetic on them. Per-stream
  versions are integers (required for optimistic concurrency).
- Value objects live with the ring that owns the concept: domain concepts in
  entities, port payloads in `ports/`.

## Adapters

- One directory (or module) per technology; implements whichever ports it honestly
  can.
- Never import from another adapter or another backend.
- Backend-specific dependencies imported inside the adapter only, guarded:

```python
try:
    from some_optional import lib
    BACKEND_AVAILABLE = True
except ImportError:
    BACKEND_AVAILABLE = False
```

Export an `*_AVAILABLE` flag so users can check at runtime.

- Every adapter must pass the conformance suite for each port it implements
  (`testing/conformance.py`).

## Event Model Rules

- All events subclass `DomainEvent` (pydantic BaseModel), frozen.
- `__init_subclass__` auto-derives `event_type` from the class name — never declare it by
  hand. Registry membership is explicit: decorate with `@register_event` (deserialization
  needs it); there is no auto-registration.
- Event types are immutable after creation — never modify event schema, add new event
  types instead.

## Enforcement

- Ring boundaries are enforced by import-linter contracts (`pyproject.toml`); when you
  add or move a module, update the contracts to match the ring map above — never relax
  them to make an import work.
- If a change requires an outward dependency (entities → ports, ports → adapter,
  use case → driver), the design is wrong: introduce or extend a port instead.
