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

1. **Entities** (`domain/` — during transition also `events/`, `types.py`,
   `exceptions.py`): Enterprise business rules — domain events, the event registry,
   domain value objects, domain exceptions. Pure: stdlib + pydantic only. No I/O.
2. **Use cases** (`application/` — during transition `aggregates/`, `projections/`,
   `subscriptions/`, `migration/`, `handlers/`): Application business rules —
   aggregate repositories, projection engines, subscription lifecycle, migration
   orchestration. Depends on entities and on the boundary ports it owns. Never on a
   concrete adapter, driver, or framework.
3. **Interface adapters** (`adapters/` — during transition `stores/`,
   `repositories/`, `bus/`, `snapshots/`, `locks/` backend modules): Gateways that
   implement the ports for a specific technology, converting between the use-case
   format (value objects, domain events) and the storage/wire format (rows, JSON,
   frames).
4. **Frameworks & drivers**: sqlalchemy, asyncpg, aiosqlite, redis, aiokafka,
   aio-pika. Imported only inside the adapter that needs them, always guarded
   (see below). Driver types never appear in port signatures.

The **public API** (`__init__.py`) is the delivery mechanism's front door: it
re-exports from all rings and is the only module users import from.

## Ports (Boundary Interfaces)

Ports are owned by the inner rings and implemented by adapters — dependencies are
inverted at every boundary crossing (the D in SOLID).

- Ports live in `ports/` (during transition also `protocols.py`, `*/interface.py`),
  depend only on entities, and contain no implementation code, ever.
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
- Events auto-register via `__init_subclass__` — do not manually register.
- Event types are immutable after creation — never modify event schema, add new event
  types instead.

## Enforcement

- Ring boundaries are enforced by import-linter contracts (`pyproject.toml`); when you
  add or move a module, update the contracts to match the ring map above — never relax
  them to make an import work.
- If a change requires an outward dependency (entities → ports, ports → adapter,
  use case → driver), the design is wrong: introduce or extend a port instead.
