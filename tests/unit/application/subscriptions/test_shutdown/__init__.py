"""Unit tests for subscription graceful shutdown package."""

from tests.unit.application.subscriptions.test_shutdown._fixtures import (
    coordinator,
    dummy_async_hook,
    mock_subscriber,
    signal_mock,
)
from tests.unit.application.subscriptions.test_shutdown.test_post_shutdown_hooks import (
    TestPostShutdownHookImports,
    TestPostShutdownHooks,
)
from tests.unit.application.subscriptions.test_shutdown.test_pre_shutdown_hooks import (
    TestPreShutdownHookImports,
    TestPreShutdownHooks,
)
from tests.unit.application.subscriptions.test_shutdown.test_shutdown_coordinator_lifecycle import (
    TestRequestShutdown,
    TestShutdownCallbacks,
    TestShutdownCoordinatorCreation,
    TestShutdownCoordinatorProperties,
    TestShutdownExecution,
)
from tests.unit.application.subscriptions.test_shutdown.test_shutdown_deadline import (
    TestShutdownDeadline,
)
from tests.unit.application.subscriptions.test_shutdown.test_shutdown_deadline_integration import (
    TestShutdownDeadlineIntegration,
)
from tests.unit.application.subscriptions.test_shutdown.test_shutdown_metrics import (
    TestMetricsModuleImports,
    TestShutdownCoordinatorMetrics,
    TestShutdownMetricsFunctions,
    TestShutdownMetricsSnapshot,
)
from tests.unit.application.subscriptions.test_shutdown.test_shutdown_models import (
    TestModuleImports,
    TestShutdownPhaseEnum,
    TestShutdownReasonEnum,
    TestShutdownResult,
    TestShutdownResultNewFields,
)
from tests.unit.application.subscriptions.test_shutdown.test_shutdown_periodic_checkpoint import (
    TestPeriodicCheckpointDuringDrain,
)
from tests.unit.application.subscriptions.test_shutdown.test_shutdown_reason_tracking import (
    TestShutdownReasonTracking,
)
from tests.unit.application.subscriptions.test_shutdown.test_shutdown_signals import (
    TestReset,
    TestSignalHandling,
    TestWaitForShutdown,
)
from tests.unit.application.subscriptions.test_shutdown.test_shutdown_timeouts_and_errors import (
    TestErrorHandling,
    TestIntegration,
    TestTimeoutHandling,
)

__all__ = [  # pragma: allowlist secret
    "TestErrorHandling",
    "TestIntegration",
    "TestMetricsModuleImports",
    "TestModuleImports",
    "TestPeriodicCheckpointDuringDrain",
    "TestPostShutdownHookImports",
    "TestPostShutdownHooks",
    "TestPreShutdownHookImports",
    "TestPreShutdownHooks",
    "TestRequestShutdown",
    "TestReset",
    "TestShutdownCallbacks",
    "TestShutdownCoordinatorCreation",
    "TestShutdownCoordinatorMetrics",
    "TestShutdownCoordinatorProperties",
    "TestShutdownDeadline",
    "TestShutdownDeadlineIntegration",
    "TestShutdownExecution",
    "TestShutdownMetricsFunctions",
    "TestShutdownMetricsSnapshot",
    "TestShutdownPhaseEnum",
    "TestShutdownReasonEnum",
    "TestShutdownReasonTracking",
    "TestShutdownResult",
    "TestShutdownResultNewFields",
    "TestSignalHandling",
    "TestTimeoutHandling",
    "TestWaitForShutdown",
    "coordinator",
    "dummy_async_hook",
    "mock_subscriber",
    "signal_mock",
]
