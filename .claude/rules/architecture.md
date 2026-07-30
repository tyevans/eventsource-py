---
paths:
  - "src/eventsource/**/*.py"
---

# Architecture Rules

**The library follows hexagonal architecture (ports & adapters), hard DDD, and SOLID.**
Target layout is being migrated to in phases (see `docs/superpowers/specs/` for the
redesign specs); these rules govern all new and modified code now.

## Layers and the Dependency Rule

Dependencies point inward only. A layer may import from layers above it in this list,
never below:

1. **Domain** (`domain/` — during transition also `events/`, `types.py`, `exceptions.py`):
   Domain events, value objects, exceptions. Pure: stdlib + pydantic only. No I/O, no
   infrastructure imports, no imports from any other layer.
2. **Ports** (`ports/` — during transition also `protocols.py`, `*/interface.py`):
   Capability protocols and the value objects they exchange. Depends on domain only.
   No implementation code lives here, ever.
3. **Application** (`aggregates/`, `projections/`, `subscriptions/`, `migration/`,
   `handlers/`): Orchestration and use cases. Depends on domain + ports exclusively —
   never on a concrete adapter.
4. **Adapters** (`stores/`, `repositories/`, `bus/`, `snapshots/`, `locks/` backend
   modules): Implement ports for a specific technology. Import their own driver;
   never import from another adapter or another backend.
5. **Public API** (`__init__.py`): Re-exports. The only module users import from.

## Ports (Interface Segregation)

- Ports are small, composed `Protocol` classes — one capability per port
  (e.g., appending, stream reading, global feed reading, type querying). Never grow a
  god-interface; add a new port instead.
- Consumers type-hint the narrowest port union they need, never a concrete adapter and
  never a wider port than they use.
- Optional capabilities are expressed by a backend *not implementing* a port — never by
  raising `NotImplementedError` from a method it claims to support.
- Mark ports `@runtime_checkable` only when a consumer genuinely needs isinstance checks.

## Value Objects

- Cross-boundary data is carried by immutable value objects (frozen pydantic models or
  frozen dataclasses), not primitives or dicts: positions, stream identities, expected
  versions, event envelopes.
- Positions in the global feed are **opaque ordered tokens** defined by the adapter —
  consumers may compare and persist them, never do arithmetic on them. Per-stream
  versions are integers (required for optimistic concurrency).
- Value objects live with the layer that owns the concept: domain concepts in domain,
  port payloads in ports.

## Adapters

- One directory (or module) per technology; implements whichever ports it honestly can.
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

- Layer boundaries are enforced by import-linter contracts (`pyproject.toml`); when you
  add or move a module, update the contracts to match the layer map above — never relax
  them to make an import work.
- If a change requires an outward dependency (domain → port, port → adapter), the design
  is wrong: introduce or extend a port instead.
