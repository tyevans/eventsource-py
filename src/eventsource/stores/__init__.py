"""Event store implementations for the eventsource library."""

from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.stores.interface import (
    AppendResult,
    EventPublisher,
    EventStore,
    EventStream,
    ExpectedVersion,
    ReadDirection,
    ReadOptions,
    StoredEvent,
    SyncEventStore,
)
from eventsource.stores.postgresql import PostgreSQLEventStore
from eventsource.stores.sqlite import SQLiteEventStore

__all__ = [
    # Data structures
    "AppendResult",
    "EventStream",
    "StoredEvent",
    "ReadOptions",
    "ReadDirection",
    "ExpectedVersion",
    # Abstract base classes
    "EventStore",
    "SyncEventStore",
    # Concrete implementations
    "InMemoryEventStore",
    "PostgreSQLEventStore",
    "SQLiteEventStore",
    # Protocols
    "EventPublisher",
]
