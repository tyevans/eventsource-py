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

# SQLite support is optional - only import if aiosqlite is available
try:
    from eventsource.stores.sqlite import SQLiteEventStore  # noqa: F401

    _SQLITE_AVAILABLE = True
except ImportError:
    _SQLITE_AVAILABLE = False

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

# Add SQLiteEventStore to __all__ only if available
if _SQLITE_AVAILABLE:
    __all__.append("SQLiteEventStore")
