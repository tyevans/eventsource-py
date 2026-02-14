---
paths:
  - "src/eventsource/**/*.py"
---

# Architecture Rules

## Layer Boundaries

1. **Core domain** (`events/`, `aggregates/`, `projections/`, `protocols.py`): No infrastructure imports. Depend only on stdlib, pydantic, and other core modules.
2. **Infrastructure** (`infrastructure/`, `stores/`, `repositories/`, `bus/`): May import core domain. Never import from other infrastructure backends.
3. **Public API** (`__init__.py`): Re-exports from all layers. This is the only module users import from.

## Interface Pattern

- Define interface as Protocol or ABC in a dedicated `interface.py` or `base.py`
- Implementations in separate modules (e.g., `stores/interface.py` + `stores/postgresql.py`)
- Backend-specific dependencies imported inside the implementation module only

## Optional Dependencies

Guard optional backends with try/except ImportError:
```python
try:
    from some_optional import lib
    BACKEND_AVAILABLE = True
except ImportError:
    BACKEND_AVAILABLE = False
```

Export an `*_AVAILABLE` flag so users can check at runtime.

## Event Model Rules

- All events subclass `DomainEvent` (pydantic BaseModel)
- Events auto-register via `__init_subclass__` -- do not manually register
- Event types are immutable after creation -- never modify event schema, add new event types instead
