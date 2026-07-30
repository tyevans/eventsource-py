"""
TRANSITION: SQLite snapshot store moved to adapters.sqlite.snapshots.

This module re-exports from the new location for backward compatibility.
Imports should migrate to eventsource.adapters.sqlite.snapshots over time.
"""

from eventsource.adapters.sqlite.snapshots import (
    SQLITE_AVAILABLE,
    SQLiteNotAvailableError,
    SQLiteSnapshotStore,
)

__all__ = [
    "SQLITE_AVAILABLE",
    "SQLiteNotAvailableError",
    "SQLiteSnapshotStore",
]
