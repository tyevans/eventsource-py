"""SQLite adapter implementing the store, snapshot, and outbox ports.

``AIOSQLITE_AVAILABLE`` is the one public availability flag for this
package. ``snapshots.py`` and ``store.py`` each guard their own
``import aiosqlite`` (every module doing a runtime-guarded import needs its
own try/except) and both define a module-local ``AIOSQLITE_AVAILABLE``, but
those two copies guard the identical import and are therefore always equal
-- re-exporting only one of them (store.py's) avoids two names for one fact
(recurring-defects §2). If a future aiosqlite-guarded module's availability
could ever legitimately diverge from this one, that is a new fact and
deserves its own name, not reuse of this one.
"""

from eventsource.adapters.sqlite.outbox import SQLiteOutboxRepository
from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository
from eventsource.adapters.sqlite.snapshots import (
    SQLiteNotAvailableError,
    SQLiteSnapshotStore,
)
from eventsource.adapters.sqlite.store import AIOSQLITE_AVAILABLE, SQLiteEventStore

__all__ = [
    "AIOSQLITE_AVAILABLE",
    "SQLiteEventStore",
    "SQLiteNotAvailableError",
    "SQLiteOutboxRepository",
    "SQLiteReadModelRepository",
    "SQLiteSnapshotStore",
]
