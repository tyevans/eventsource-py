"""Unit tests for projection coordinator package."""

from tests.unit.application.projections.test_projection_coordinator._fixtures import (
    OrderCreated,
    OrderShipped,
)
from tests.unit.application.projections.test_projection_coordinator.test_coordinator_lifecycle import (
    TestProjectionCoordinator,
)
from tests.unit.application.projections.test_projection_coordinator.test_projection_registry import (
    TestProjectionRegistry,
)
from tests.unit.application.projections.test_projection_coordinator.test_subscriber_registry_and_concurrency import (
    TestConcurrentExecution,
    TestSubscriberRegistry,
)

__all__ = [  # pragma: allowlist secret
    "OrderCreated",
    "OrderShipped",
    "TestConcurrentExecution",
    "TestProjectionCoordinator",
    "TestProjectionRegistry",
    "TestSubscriberRegistry",
]
