"""PostgreSQL adapter implementing the store, snapshot, and outbox ports."""

from eventsource.adapters.postgresql.outbox import PostgreSQLOutboxRepository
from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore
from eventsource.adapters.postgresql.store import ASYNCPG_AVAILABLE, PostgreSQLEventStore

__all__ = [
    "ASYNCPG_AVAILABLE",
    "PostgreSQLEventStore",
    "PostgreSQLOutboxRepository",
    "PostgreSQLSnapshotStore",
]
