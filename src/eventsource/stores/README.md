# Event Stores

Event store implementations for persisting and retrieving domain events. The event store is the source of truth in event sourcing architecture.

## Key Interfaces

- `EventStore` -- Abstract base class defining store operations (append, read stream, read all, etc.)
- `StoredEvent` -- Wrapper for persisted events with position metadata (stream_position, global_position)
- `EventStream` -- Container for events belonging to an aggregate
- `AppendResult` -- Result of appending events (new version, global positions)
- `ReadOptions` -- Configuration for reading events (from_version, max_count, direction)

## Module Map

- `interface.py` -- `EventStore` ABC, `StoredEvent`, `EventStream`, `AppendResult`, `ReadOptions`
- `postgresql.py` -- `PostgreSQLEventStore` using asyncpg
- `sqlite.py` -- `SQLiteEventStore` using aiosqlite
- `in_memory.py` -- `InMemoryEventStore` for testing
- `_compat.py` -- Compatibility shims for SQLAlchemy 1.x / 2.x
- `_type_converter.py` -- Type conversion helpers for SQL backends

## Invariants

- **Optimistic locking**: Append operations use `expected_version` to prevent concurrent writes
- **Immutable events**: Events are never updated or deleted, only appended
- **Ordered streams**: Events in a stream are ordered by `stream_position` (1-based)
- **Global ordering**: Events across all streams are ordered by `global_position` (1-based)
- **Stream ID format**: `{aggregate_id}:{aggregate_type}` (e.g., `abc-123:Order`)
