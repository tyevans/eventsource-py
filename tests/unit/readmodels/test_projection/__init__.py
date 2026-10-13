"""Unit tests for ReadModelProjection package."""

from tests.unit.readmodels.test_projection._fixtures import (
    CustomTableModel,
    OrderCancelled,
    OrderCreated,
    OrderShipped,
    OrderSummary,
)
from tests.unit.readmodels.test_projection.conftest import (
    checkpoint_repo,
    dlq_repo,
    mock_postgresql_session_factory,
    mock_sqlite_session_factory,
)
from tests.unit.readmodels.test_projection.test_projection_behavior import (
    TestCheckpointBehavior,
    TestImports,
    TestRepositoryCleanup,
    TestRuntimeErrors,
    TestTracingConfiguration,
    TestUnregisteredEventHandling,
)
from tests.unit.readmodels.test_projection.test_projection_construction import (
    TestReadModelProjectionConstruction,
    TestReadModelProjectionHandlerRouting,
)
from tests.unit.readmodels.test_projection.test_projection_dialect import (
    TestDialectDetection,
    TestTruncateReadModels,
)

__all__ = [  # pragma: allowlist secret
    "CustomTableModel",
    "OrderCancelled",
    "OrderCreated",
    "OrderShipped",
    "OrderSummary",
    "TestCheckpointBehavior",
    "TestDialectDetection",
    "TestImports",
    "TestReadModelProjectionConstruction",
    "TestReadModelProjectionHandlerRouting",
    "TestRepositoryCleanup",
    "TestRuntimeErrors",
    "TestTracingConfiguration",
    "TestTruncateReadModels",
    "TestUnregisteredEventHandling",
    "checkpoint_repo",
    "dlq_repo",
    "mock_postgresql_session_factory",
    "mock_sqlite_session_factory",
]
