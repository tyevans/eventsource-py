"""CutoverManager unit test package facade."""

from tests.unit.application.migration.test_cutover_manager._fixtures import (
    config,
    create_failing_lock_context_manager,
    create_lock_context_manager,
    cutover_manager,
    migration_id,
    mock_lag_tracker,
    mock_lock_manager,
    mock_router,
    mock_routing_repo,
    strict_config,
    target_store_id,
    tenant_id,
)
from tests.unit.application.migration.test_cutover_manager.test_cutover_manager_init_and_flow import (
    TestCutoverManagerInit,
    TestStrictZeroLagDefault,
    TestSuccessfulCutover,
)
from tests.unit.application.migration.test_cutover_manager.test_cutover_manager_locks_and_lag import (
    TestLockAcquisition,
    TestSyncLagValidation,
    TestTimeoutEnforcement,
)
from tests.unit.application.migration.test_cutover_manager.test_cutover_manager_rollback_and_pause import (
    TestAutomaticRollback,
    TestTargetStoreVerification,
    TestWritePauseResume,
)
from tests.unit.application.migration.test_cutover_manager.test_cutover_manager_state_and_readiness import (
    TestCutoverReadinessValidation,
    TestCutoverResult,
    TestEdgeCases,
    TestRoutingStateTransitions,
)

__all__ = [  # pragma: allowlist secret
    "TestAutomaticRollback",
    "TestCutoverManagerInit",
    "TestCutoverReadinessValidation",
    "TestCutoverResult",
    "TestEdgeCases",
    "TestLockAcquisition",
    "TestRoutingStateTransitions",
    "TestStrictZeroLagDefault",
    "TestSuccessfulCutover",
    "TestSyncLagValidation",
    "TestTargetStoreVerification",
    "TestTimeoutEnforcement",
    "TestWritePauseResume",
    "config",
    "create_failing_lock_context_manager",
    "create_lock_context_manager",
    "cutover_manager",
    "migration_id",
    "mock_lag_tracker",
    "mock_lock_manager",
    "mock_router",
    "mock_routing_repo",
    "strict_config",
    "target_store_id",
    "tenant_id",
]
