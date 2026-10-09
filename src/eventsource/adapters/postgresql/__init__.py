"""PostgreSQL adapter implementing the store, snapshot, outbox, and lock ports."""

from eventsource.adapters.postgresql.locks import (
    PostgreSQLAdvisoryLock,
    PostgreSQLLockManager,
)
from eventsource.adapters.postgresql.outbox import PostgreSQLOutboxRepository
from eventsource.adapters.postgresql.readmodels import PostgreSQLReadModelRepository
from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore
from eventsource.adapters.postgresql.store import ASYNCPG_AVAILABLE, PostgreSQLEventStore

__all__ = [
    "ASYNCPG_AVAILABLE",
    "PostgreSQLEventStore",
    "PostgreSQLAdvisoryLock",
    "PostgreSQLLockManager",
    "PostgreSQLOutboxRepository",
    "PostgreSQLReadModelRepository",
    "PostgreSQLSnapshotStore",
]
