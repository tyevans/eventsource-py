# ADR-0007: Remove SyncEventStore Interface

**Status:** Accepted

**Date:** 2025-12-07

**Deciders:** Tyler Evans

---

## Context

The eventsource-py library contained a `SyncEventStore` abstract base class in `src/eventsource/stores/interface.py` that defined a synchronous interface for event stores. This class was introduced as a potential "escape hatch" for contexts where async is not available.

The `SyncEventStore` class:
- Defined abstract methods: `append_events()`, `get_events()`, `get_events_by_type()`, `event_exists()`
- Provided a default `get_stream_version()` implementation
- Mirrored the async `EventStore` interface but without `async`/`await`

### Problem

Despite being part of the public API:
1. **No implementations existed**: Neither `InMemoryEventStore` nor `PostgreSQLEventStore` provided synchronous implementations
2. **No concrete use cases identified**: No users or internal code was using the sync interface
3. **Maintenance burden**: The class added code that needed to be maintained and tested
4. **Misleading API surface**: Users might expect sync implementations to exist and be supported

### Forces at Play

1. **API cleanliness**: Dead code confuses users and suggests incomplete functionality
2. **Maintenance cost**: Unused abstractions add review and testing overhead
3. **Modern Python patterns**: The async ecosystem in Python 3.13 is mature and well-supported
4. **Escape hatch availability**: `asyncio.run()` provides a simple way to use async code from sync contexts

## Decision

We **remove the `SyncEventStore` abstract class** from the codebase.

### Rationale

1. **No demand**: No implementations exist, indicating no user need
2. **Async is standard**: Modern Python I/O libraries are async-first (SQLAlchemy 2.0, asyncpg, FastAPI)
3. **Easy workaround**: `asyncio.run(store.get_events(...))` works for sync contexts
4. **Reduced maintenance**: Less code to maintain, test, and document

### Migration Path

For users who need synchronous access:

```python
import asyncio
from eventsource import InMemoryEventStore

store = InMemoryEventStore()

# Wrap async calls with asyncio.run()
result = asyncio.run(store.append_events(
    aggregate_id=order_id,
    aggregate_type="Order",
    events=[order_created],
    expected_version=0,
))

stream = asyncio.run(store.get_events(order_id, "Order"))
```

For more complex scenarios where running an event loop is inconvenient:

```python
import asyncio

# Create a helper function
def run_sync(coro):
    """Run an async coroutine synchronously."""
    return asyncio.run(coro)

# Use it throughout your code
stream = run_sync(store.get_events(order_id, "Order"))
```

## Consequences

### Positive

- **Smaller API surface**: Fewer classes to document and maintain
- **Clearer intent**: Library explicitly commits to async-first design
- **Reduced confusion**: No more abstract class without implementations
- **Lower maintenance burden**: Less code to review, test, and update

### Negative

- **Breaking change**: Any code importing `SyncEventStore` will break
  - Mitigated: Since no implementations existed, actual usage is unlikely
- **No built-in sync support**: Users must wrap async calls themselves
  - Mitigated: `asyncio.run()` is simple and well-documented

### Neutral

- **Documentation updates**: ADR-0001 (Async-First Design) updated to reflect this decision
- **Consistency**: Aligns with decision not to provide `SyncProjection` or other sync interfaces

## Alternatives Considered

### Option A: Implement SyncInMemoryEventStore

Create a concrete synchronous implementation:

```python
class SyncInMemoryEventStore(SyncEventStore):
    def append_events(self, ...) -> AppendResult:
        # Synchronous implementation
        ...
```

**Why rejected:**
- Adds maintenance burden to keep sync and async implementations in sync
- Testing burden increases
- No clear demand for this feature
- `asyncio.run()` wrapper provides equivalent functionality

### Option B: Keep SyncEventStore but deprecate

Mark as deprecated and remove in future version:

```python
import warnings

class SyncEventStore(ABC):
    def __init__(self):
        warnings.warn(
            "SyncEventStore is deprecated. Use EventStore with asyncio.run().",
            DeprecationWarning,
        )
```

**Why rejected:**
- Prolongs the maintenance burden
- No implementations exist to deprecate gracefully
- This is pre-1.0 software; breaking changes are acceptable

## Related Decisions

- **ADR-0001: Async-First Design**: Establishes async as the primary API pattern
- **TD-008**: Technical debt task that triggered this decision

## References

### Code Changes

- Removed `SyncEventStore` class from `src/eventsource/stores/interface.py`
- Removed exports from `src/eventsource/__init__.py` and `src/eventsource/stores/__init__.py`
- Updated documentation to remove `SyncEventStore` and `SyncProjection` references
- Updated ADR-0001 to reflect the `asyncio.run()` approach for sync contexts

### External References

- [Python asyncio.run() documentation](https://docs.python.org/3/library/asyncio-runner.html#asyncio.run)
- [ADR-0001: Async-First Design](./0001-async-first-design.md)
