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
)
from eventsource.stores.postgresql import PostgreSQLEventStore

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
    # Concrete implementations
    "InMemoryEventStore",
    "PostgreSQLEventStore",
    # Protocols
    "EventPublisher",
]
