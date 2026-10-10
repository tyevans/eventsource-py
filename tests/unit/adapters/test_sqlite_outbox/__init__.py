"""Unit tests for SQLiteOutboxRepository."""

from __future__ import annotations

from tests.unit.adapters.test_sqlite_outbox._events import (
    SampleEvent,
    TestSampleEvent,
)
from tests.unit.adapters.test_sqlite_outbox.test_sqlite_outbox_repository import (
    TestSQLiteOutboxRepositoryAddEvent,
    TestSQLiteOutboxRepositoryCleanupPublished,
    TestSQLiteOutboxRepositoryGetPendingEvents,
    TestSQLiteOutboxRepositoryGetStats,
    TestSQLiteOutboxRepositoryIncrementRetry,
    TestSQLiteOutboxRepositoryMarkFailed,
    TestSQLiteOutboxRepositoryMarkPublished,
    TestSQLiteOutboxRepositoryMultipleEvents,
    TestSQLiteOutboxRepositoryProtocol,
)
from tests.unit.adapters.test_sqlite_outbox.test_sqlite_outbox_standalone import (
    TestSQLiteOutboxRepositoryStandalone,
    TestSQLiteOutboxRepositoryStandaloneProtocol,
)

__all__ = [
    "SampleEvent",
    "TestSampleEvent",
    "TestSQLiteOutboxRepositoryAddEvent",  # pragma: allowlist secret
    "TestSQLiteOutboxRepositoryCleanupPublished",  # pragma: allowlist secret
    "TestSQLiteOutboxRepositoryGetPendingEvents",  # pragma: allowlist secret
    "TestSQLiteOutboxRepositoryGetStats",  # pragma: allowlist secret
    "TestSQLiteOutboxRepositoryIncrementRetry",  # pragma: allowlist secret
    "TestSQLiteOutboxRepositoryMarkFailed",  # pragma: allowlist secret
    "TestSQLiteOutboxRepositoryMarkPublished",  # pragma: allowlist secret
    "TestSQLiteOutboxRepositoryMultipleEvents",  # pragma: allowlist secret
    "TestSQLiteOutboxRepositoryProtocol",  # pragma: allowlist secret
    "TestSQLiteOutboxRepositoryStandalone",  # pragma: allowlist secret
    "TestSQLiteOutboxRepositoryStandaloneProtocol",  # pragma: allowlist secret
]
