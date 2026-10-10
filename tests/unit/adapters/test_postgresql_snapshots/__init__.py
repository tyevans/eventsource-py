"""Unit tests for PostgreSQLSnapshotStore package."""

from __future__ import annotations

from .conftest import (
    aggregate_id,
    mock_session,
    mock_session_factory,
    sample_snapshot,
    store,
)
from .test_postgresql_snapshots_basic import (
    TestPostgreSQLSnapshotStoreBasic,
    TestUpsertSemantics,
)
from .test_postgresql_snapshots_create import (
    create_mock_result,
    create_scalar_result,
    create_snapshot_row,
)
from .test_postgresql_snapshots_operations import (
    TestBulkDelete,
    TestComplexState,
    TestImports,
    TestProperties,
)
from .test_postgresql_snapshots_tracing import (
    TestOpenTelemetryTracing,
)

__all__ = [
    "TestBulkDelete",
    "TestComplexState",
    "TestImports",
    "TestOpenTelemetryTracing",  # pragma: allowlist secret
    "TestPostgreSQLSnapshotStoreBasic",  # pragma: allowlist secret
    "TestProperties",
    "TestUpsertSemantics",  # pragma: allowlist secret
    "aggregate_id",
    "create_mock_result",
    "create_scalar_result",
    "create_snapshot_row",
    "mock_session",
    "mock_session_factory",
    "sample_snapshot",
    "store",
]
