"""WritePauseManager unit test package facade."""

from tests.unit.application.migration.test_write_pause._fixtures import (
    manager,
    tenant_id,
)
from tests.unit.application.migration.test_write_pause.test_write_pause_init_and_state import (
    TestBasicPauseResume,
    TestIdempotentOperations,
    TestPauseMetrics,
    TestPauseState,
    TestWritePausedError,
    TestWritePauseManagerInit,
)
from tests.unit.application.migration.test_write_pause.test_write_pause_operations import (
    TestConcurrentOperationsSafety,
    TestEdgeCases,
    TestForceResumeAll,
    TestGetAllPaused,
    TestGetPauseState,
    TestMetricsHistory,
    TestRouterIntegration,
)
from tests.unit.application.migration.test_write_pause.test_write_pause_waiting import (
    TestMultipleWaiters,
    TestWaitForNoWaiters,
    TestWaitIfPaused,
)

__all__ = [  # pragma: allowlist secret
    "TestBasicPauseResume",
    "TestConcurrentOperationsSafety",
    "TestEdgeCases",
    "TestForceResumeAll",
    "TestGetAllPaused",
    "TestGetPauseState",
    "TestIdempotentOperations",
    "TestMetricsHistory",
    "TestMultipleWaiters",
    "TestPauseMetrics",
    "TestPauseState",
    "TestRouterIntegration",
    "TestWaitForNoWaiters",
    "TestWaitIfPaused",
    "TestWritePausedError",
    "TestWritePauseManagerInit",
    "manager",
    "tenant_id",
]
