"""SQLite adapter implementing the store, snapshot, and outbox ports."""

from eventsource.adapters.sqlite.outbox import SQLiteOutboxRepository
from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository
from eventsource.adapters.sqlite.snapshots import (
    SQLITE_AVAILABLE,
    SQLiteNotAvailableError,
    SQLiteSnapshotStore,
)
from eventsource.adapters.sqlite.store import AIOSQLITE_AVAILABLE, SQLiteEventStore

__all__ = [
    "AIOSQLITE_AVAILABLE",
    "SQLITE_AVAILABLE",
    "SQLiteEventStore",
    "SQLiteNotAvailableError",
    "SQLiteOutboxRepository",
    "SQLiteReadModelRepository",
    "SQLiteSnapshotStore",
]
