"""
Snapshot storage for aggregate state caching.

This module provides snapshot functionality to optimize aggregate loading
by capturing point-in-time state. Instead of replaying all events,
aggregates can be restored from a snapshot and only replay events
since the snapshot was taken.

Key Components:
    Snapshot: Immutable data structure representing captured aggregate state.
    SnapshotStore: Abstract interface for snapshot persistence.
    InMemorySnapshotStore: Testing/development implementation.

Exceptions:
    SnapshotError: Base exception for all snapshot errors.
    SnapshotDeserializationError: Raised when snapshot state cannot be deserialized.
    SnapshotSchemaVersionError: Raised when schema version doesn't match.
    SnapshotNotFoundError: Raised when snapshot is required but not found.

Backend Implementations:
    PostgreSQLSnapshotStore: Production PostgreSQL implementation (requires sqlalchemy).
    SQLiteSnapshotStore: Embedded SQLite implementation (requires aiosqlite).

Example:
    Basic usage with AggregateRepository::

        from eventsource import AggregateRepository, InMemoryEventStore
        from eventsource.snapshots import InMemorySnapshotStore

        event_store = InMemoryEventStore()
        snapshot_store = InMemorySnapshotStore()

        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
            snapshot_store=snapshot_store,
            snapshot_threshold=100,  # Create snapshot every 100 events
        )

        # Load aggregate - uses snapshot if available
        order = await repo.load(order_id)

    Manual snapshot creation::

        from eventsource.snapshots import InMemorySnapshotStore, Snapshot
        from datetime import datetime, UTC
        from uuid import uuid4

        store = InMemorySnapshotStore()

        snapshot = Snapshot(
            aggregate_id=uuid4(),
            aggregate_type="Order",
            version=100,
            state={"status": "shipped", "items": [...]},
            schema_version=1,
            created_at=datetime.now(UTC),
        )

        await store.save_snapshot(snapshot)
        loaded = await store.get_snapshot(snapshot.aggregate_id, "Order")

Performance Benefits:
    Without snapshots, loading an aggregate with 10,000 events requires
    replaying all events (~5 seconds). With snapshots, only events since
    the last snapshot are replayed (~50ms for 100 events).

    Typical improvements:
    - 100 events: ~50ms -> ~15ms (70% faster)
    - 1,000 events: ~500ms -> ~20ms (96% faster)
    - 10,000 events: ~5s -> ~60ms (99% faster)

See Also:
    - :class:`eventsource.AggregateRepository`: Snapshot-aware repository.
    - :class:`eventsource.AggregateRoot`: Base class with snapshot methods.
"""

from eventsource.snapshots.exceptions import (
    SnapshotDeserializationError,
    SnapshotError,
    SnapshotNotFoundError,
    SnapshotSchemaVersionError,
)
from eventsource.snapshots.interface import Snapshot, SnapshotStore

# NOTE: the backend implementations below live in eventsource.adapters.* as
# of the core-rings re-home (Task 14). They are re-exported here lazily
# (PEP 562 module __getattr__) rather than with eager top-of-module
# imports: eventsource.ports (an inner ring) re-exports Snapshot/
# SnapshotStore from this package for the `SnapshotStore` port, and
# eventsource.adapters._sql (used by the PostgreSQL/SQLite adapters' own
# position codec) in turn depends on eventsource.ports -- an eager import
# of eventsource.adapters.postgresql/sqlite here would close that loop into
# a circular import the first time anything touches eventsource.ports.
# Lazy attribute access defers the adapter import until this module's
# public API is actually used, by which point eventsource.ports has always
# finished initializing.
__all__ = [
    # Core types
    "Snapshot",
    "SnapshotStore",
    # Implementations
    "InMemorySnapshotStore",
    "PostgreSQLSnapshotStore",
    "SQLiteSnapshotStore",
    "SQLITE_AVAILABLE",
    # Exceptions
    "SnapshotError",
    "SnapshotDeserializationError",
    "SnapshotSchemaVersionError",
    "SnapshotNotFoundError",
    "SQLiteNotAvailableError",
]


def __getattr__(name: str) -> object:
    if name == "InMemorySnapshotStore":
        from eventsource.adapters.memory.snapshots import InMemorySnapshotStore

        return InMemorySnapshotStore
    if name == "PostgreSQLSnapshotStore":
        from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore

        return PostgreSQLSnapshotStore
    if name in ("SQLiteSnapshotStore", "SQLiteNotAvailableError", "SQLITE_AVAILABLE"):
        try:
            from eventsource.adapters.sqlite.snapshots import (
                SQLITE_AVAILABLE,
                SQLiteNotAvailableError,
                SQLiteSnapshotStore,
            )
        except ImportError:
            if name == "SQLITE_AVAILABLE":
                return False
            return None
        return {
            "SQLiteSnapshotStore": SQLiteSnapshotStore,
            "SQLiteNotAvailableError": SQLiteNotAvailableError,
            "SQLITE_AVAILABLE": SQLITE_AVAILABLE,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
