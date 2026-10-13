"""Unit tests for projection base classes package."""

from tests.unit.application.projections.test_projection_base._fixtures import (
    OrderCancelled,
    OrderCreated,
    OrderShipped,
)
from tests.unit.application.projections.test_projection_base.test_projection_abstract_and_sync import (
    TestEventHandlerBase,
    TestProjectionAbstract,
    TestSyncProjection,
)
from tests.unit.application.projections.test_projection_base.test_projection_checkpoints_and_declarative import (
    TestCheckpointTrackingProjection,
    TestDeclarativeProjection,
    TestDeclarativeProjectionWithDLQ,
    TestLagMetrics,
)
from tests.unit.application.projections.test_projection_base.test_projection_database import (
    TestDatabaseProjection,
)
from tests.unit.application.projections.test_projection_base.test_projection_unregistered_events import (
    TestDatabaseProjectionUnregisteredEventHandling,
    TestProjectionUnregisteredEventHandling,
)

__all__ = [  # pragma: allowlist secret
    "OrderCancelled",
    "OrderCreated",
    "OrderShipped",
    "TestCheckpointTrackingProjection",
    "TestDatabaseProjection",
    "TestDatabaseProjectionUnregisteredEventHandling",
    "TestDeclarativeProjection",
    "TestDeclarativeProjectionWithDLQ",
    "TestEventHandlerBase",
    "TestLagMetrics",
    "TestProjectionAbstract",
    "TestProjectionUnregisteredEventHandling",
    "TestSyncProjection",
]
