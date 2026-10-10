"""SQLite repository integration tests package."""

from __future__ import annotations

from .helpers import TestSampleEvent
from .test_checkpoint_repository import (
    TestSQLCheckpointRepositoryGetAllCheckpoints,
    TestSQLCheckpointRepositoryGetCheckpoint,
    TestSQLCheckpointRepositoryGetLagMetrics,
    TestSQLCheckpointRepositoryProtocol,
    TestSQLCheckpointRepositoryResetCheckpoint,
    TestSQLCheckpointRepositoryUpdateCheckpoint,
)
from .test_dlq_crud import (
    TestSQLDLQRepositoryAddFailedEvent,
    TestSQLDLQRepositoryGetFailedEventById,
    TestSQLDLQRepositoryGetFailedEvents,
    TestSQLDLQRepositoryMarkResolved,
    TestSQLDLQRepositoryMarkRetrying,
    TestSQLDLQRepositoryProtocol,
)
from .test_dlq_maintenance import (
    TestSQLDLQRepositoryDeleteResolvedEvents,
    TestSQLDLQRepositoryEventDataSerialization,
    TestSQLDLQRepositoryGetFailureStats,
    TestSQLDLQRepositoryGetProjectionFailureCounts,
)

__all__ = [
    "TestSampleEvent",
    "TestSQLCheckpointRepositoryGetAllCheckpoints",
    "TestSQLCheckpointRepositoryGetCheckpoint",
    "TestSQLCheckpointRepositoryGetLagMetrics",
    "TestSQLCheckpointRepositoryProtocol",
    "TestSQLCheckpointRepositoryResetCheckpoint",
    "TestSQLCheckpointRepositoryUpdateCheckpoint",
    "TestSQLDLQRepositoryAddFailedEvent",  # pragma: allowlist secret
    "TestSQLDLQRepositoryDeleteResolvedEvents",
    "TestSQLDLQRepositoryEventDataSerialization",
    "TestSQLDLQRepositoryGetFailedEventById",  # pragma: allowlist secret
    "TestSQLDLQRepositoryGetFailedEvents",  # pragma: allowlist secret
    "TestSQLDLQRepositoryGetFailureStats",
    "TestSQLDLQRepositoryGetProjectionFailureCounts",  # pragma: allowlist secret
    "TestSQLDLQRepositoryMarkResolved",
    "TestSQLDLQRepositoryMarkRetrying",
    "TestSQLDLQRepositoryProtocol",
]
