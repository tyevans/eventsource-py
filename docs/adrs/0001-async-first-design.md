# ADR-0001: Async-First Design

**Status:** Accepted

**Date:** 2025-12-06

**Deciders:** Tyler Evans

---

## Context

Event sourcing systems are inherently I/O-bound. The core operations of an event store involve:

1. **Database operations**: Reading and writing events to persistent storage
2. **Event publishing**: Distributing events to subscribers via message buses
3. **Projection updates**: Processing events to build and update read models
4. **External integrations**: Communicating with external services (notifications, webhooks, etc.)

These operations spend most of their time waiting for I/O completion rather than performing CPU-intensive computations. Traditional synchronous approaches to handling I/O-bound workloads suffer from poor resource utilization:

- **Thread-per-request models** consume significant memory (typically 1-8MB per thread) and introduce context-switching overhead
- **Blocking I/O** prevents the application from handling other requests while waiting for database responses
- **Connection pooling limitations** become bottlenecks under high concurrency

### Python Async Ecosystem Maturity (2024-2025)

The Python async ecosystem has reached production maturity:

- **asyncio** is now a stable, well-documented part of the standard library (since Python 3.4, significantly improved in 3.7+)
- **SQLAlchemy 2.0** provides first-class async support with `async_sessionmaker` and native async query execution
- **asyncpg** is a high-performance, PostgreSQL-specific async driver that outperforms synchronous alternatives
- **Pydantic 2.0** is async-friendly and provides high-performance data validation
- Modern web frameworks (FastAPI, Starlette, Litestar) are async-native

### Forces at Play

1. **Scalability**: Event sourcing systems often need to handle high throughput of events
2. **Resource efficiency**: Minimizing memory and CPU overhead per concurrent operation
3. **Ecosystem compatibility**: Integration with modern async Python frameworks
4. **Developer experience**: Learning curve and debugging considerations
5. **Testing complexity**: Async code requires async-aware testing infrastructure

## Decision

We adopt **async/await as the primary API pattern** for the eventsource library. All public interfaces that perform I/O operations will be async by default.

### Core Choices

1. **asyncio** as the concurrency framework
   - Standard library support ensures stability and broad compatibility
   - Well-understood execution model with explicit yield points
   - Rich ecosystem of compatible libraries

2. **SQLAlchemy 2.0 async** for ORM and database operations
   - `async_sessionmaker` for session management
   - Native async query execution
   - Transaction support with async context managers

3. **asyncpg** as the PostgreSQL driver
   - High-performance async driver specifically designed for PostgreSQL
   - Supports advanced PostgreSQL features (LISTEN/NOTIFY, prepared statements)
   - Significantly faster than psycopg2 for async workloads

4. **AsyncIterator** for streaming operations
   - Memory-efficient processing of large event streams
   - Natural backpressure handling
   - Composable with async comprehensions and `async for` loops

### Implementation Patterns

#### Primary Async Interface

The `EventStore` abstract base class defines the async contract:

```python
# src/eventsource/stores/interface.py (lines 289-586)
class EventStore(ABC):
    @abstractmethod
    async def append_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> AppendResult:
        """Append events to an aggregate's event stream."""
        pass

    @abstractmethod
    async def get_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str | None = None,
        from_version: int = 0,
    ) -> EventStream:
        """Get all events for an aggregate."""
        pass

    async def read_stream(
        self,
        stream_id: str,
        options: ReadOptions | None = None,
    ) -> AsyncIterator[StoredEvent]:
        """Read events as an async iterator for memory efficiency."""
        ...
```

#### Sync Usage via asyncio.run()

For contexts where async is not available or desired, users can wrap async calls:

```python
import asyncio
from eventsource import InMemoryEventStore

store = InMemoryEventStore()

# Synchronous usage via asyncio.run()
result = asyncio.run(store.append_events(
    aggregate_id=order_id,
    aggregate_type="Order",
    events=[order_created],
    expected_version=0,
))

stream = asyncio.run(store.get_events(order_id, "Order"))
```

This approach was chosen over a separate `SyncEventStore` ABC because:
- No concrete implementations existed for the sync interface
- Modern Python I/O libraries are async-first
- `asyncio.run()` provides a simple escape hatch when needed
- Maintaining both interfaces adds maintenance burden without clear benefit

#### Repository Pattern

The `AggregateRepository` follows the async-first pattern:

```python
# src/eventsource/aggregates/repository.py
class AggregateRepository(Generic[TAggregate]):
    async def load(self, aggregate_id: UUID) -> TAggregate:
        """Load an aggregate from its event history."""
        event_stream = await self._event_store.get_events(...)
        ...

    async def save(self, aggregate: TAggregate) -> None:
        """Save an aggregate by persisting its uncommitted events."""
        result = await self._event_store.append_events(...)
        if self._event_publisher:
            await self._event_publisher.publish(uncommitted_events)
```

#### Projections and Event Handlers

All projection and handler interfaces are async:

```python
# src/eventsource/projections/base.py
class Projection(ABC):
    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """Handle a domain event."""
        pass

    @abstractmethod
    async def reset(self) -> None:
        """Reset the projection."""
        pass
```

#### Event Bus

The event bus uses async for publishing:

```python
# src/eventsource/bus/interface.py
class EventBus(ABC):
    @abstractmethod
    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """Publish events to all registered subscribers."""
        pass
```

### When to Use Async vs asyncio.run()

| Use Case | Recommended Approach |
|----------|---------------------|
| Web applications (FastAPI, Starlette) | Async `EventStore` |
| Background workers | Async `EventStore` |
| CLI tools | `asyncio.run()` wrapper for simplicity |
| Django (without async views) | `asyncio.run()` wrapper |
| Testing | Async (with pytest-asyncio) |
| Jupyter notebooks | Async with `await` |

## Consequences

### Positive

- **Excellent scalability for I/O-bound workloads**: A single process can handle thousands of concurrent operations without proportional memory growth
- **Efficient resource utilization**: No thread pools needed for I/O concurrency; the event loop manages scheduling efficiently
- **Natural fit with modern Python async ecosystem**: Seamless integration with FastAPI, httpx, aiohttp, and other async libraries
- **Memory-efficient streaming via AsyncIterator**: Processing millions of events without loading them all into memory
- **Natural backpressure**: Async iterators automatically handle flow control
- **Composability**: Async code composes well with async context managers, comprehensions, and higher-order functions

### Negative

- **Learning curve for developers unfamiliar with async**: Concepts like event loops, coroutines, and await points require understanding
- **All consuming code must be async-aware**: Calling async code from sync contexts requires explicit handling (e.g., `asyncio.run()`)
- **Debugging can be more complex**: Stack traces in async code can be harder to follow
- **Some testing frameworks require async support**: Need pytest-asyncio or similar tools
- **Accidental blocking**: Calling sync I/O from async code blocks the entire event loop (requires vigilance)

### Neutral

- **Framework integration varies**: FastAPI and Starlette are async-native; Flask and Django require `asyncio.run()` wrappers
- **Ecosystem dependency on async-capable drivers**: Must use asyncpg (not psycopg2) for PostgreSQL

## References

### Code References

- `src/eventsource/stores/interface.py` - EventStore ABC
- `src/eventsource/stores/postgresql.py` - Async PostgreSQL implementation
- `src/eventsource/stores/in_memory.py` - Async in-memory implementation
- `src/eventsource/aggregates/repository.py` - Async repository pattern
- `src/eventsource/projections/base.py` - Async projection base classes
- `src/eventsource/bus/interface.py` - Async event bus interface
- `src/eventsource/repositories/outbox.py` - Async outbox pattern
- `src/eventsource/repositories/checkpoint.py` - Async checkpoint tracking

### External Documentation

- [SQLAlchemy 2.0 Async Documentation](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html)
- [asyncpg Documentation](https://magicstack.github.io/asyncpg/)
- [Python asyncio Documentation](https://docs.python.org/3/library/asyncio.html)
- [PEP 492 - Coroutines with async and await syntax](https://peps.python.org/pep-0492/)

## Notes

### Alternatives Considered

1. **Threading with ThreadPoolExecutor**
   - **Why rejected**: The GIL limits true parallelism for Python code. Thread pools add memory overhead (MB per thread) and context-switching costs. Managing shared state across threads introduces complexity and potential race conditions. For I/O-bound workloads, async provides better resource utilization.

2. **Multiprocessing**
   - **Why rejected**: Excessive overhead for I/O-bound operations. Multiprocessing is designed for CPU-bound parallelism, not I/O concurrency. Sharing state between processes is complex and slow (requires serialization). Memory usage scales linearly with process count.

3. **Sync-only API**
   - **Why rejected**: Poor scalability under high concurrency. A sync-only API would limit the library to thread-per-request models, which don't scale well for event sourcing workloads that involve many concurrent I/O operations.

4. **Sync-first with async wrappers**
   - **Why rejected**: This inverts the natural model. Wrapping sync code in async doesn't provide the efficiency benefits of true async I/O. It would require thread pools under the hood, negating the advantages of async. The modern Python ecosystem expects async-first libraries for I/O-bound operations.

5. **Trio instead of asyncio**
   - **Why rejected**: While Trio has elegant APIs and better structured concurrency, asyncio is the standard library solution with broader ecosystem support. Most async libraries target asyncio first. Using Trio would limit compatibility with other async libraries and frameworks.

### Implementation Notes

- All async methods that might block should use `await` at natural yield points
- Database connections should be acquired and released within async context managers
- Long-running sync operations (if unavoidable) should be run in an executor: `await loop.run_in_executor(None, sync_func)`
- The `InMemoryEventStore` uses `Lock` for thread safety but still exposes an async interface for API consistency

### Future Considerations

- Consider adding `anyio` support for backend-agnostic async if Trio compatibility becomes important
- Structured concurrency patterns (TaskGroups) from Python 3.11+ could improve projection coordinator reliability
- Async context managers for transaction handling could be enhanced with `async with` patterns
