"""Event store implementations for the eventsource library."""

from eventsource.stores._type_converter import (
    DEFAULT_STRING_ID_FIELDS,
    DEFAULT_UUID_FIELDS,
    DefaultTypeConverter,
    TypeConverter,
)
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
    # Type conversion
    "TypeConverter",
    "DefaultTypeConverter",
    "DEFAULT_UUID_FIELDS",
    "DEFAULT_STRING_ID_FIELDS",
]

# Add SQLiteEventStore to __all__ only if available
if _SQLITE_AVAILABLE:
    __all__.append("SQLiteEventStore")
