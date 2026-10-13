"""Phase 2 migration integration test package."""

from __future__ import annotations

from tests.unit.application.migration.test_phase2_integration._events import (
    _REGISTRY,
    OrderConfirmed,
    OrderCreated,
    SampleTestEvent,
)
from tests.unit.application.migration.test_phase2_integration._fixtures import (
    coordinator,
    lock_manager,
    migration_repo,
    router,
    routing_repo,
    source_store,
    target_store,
    tenant_id,
    write_pause_manager,
)
from tests.unit.application.migration.test_phase2_integration._helpers import (
    create_test_events,
    drive_dual_writes,
    get_all_tenant_events,
    seed_copied_prefix,
)
from tests.unit.application.migration.test_phase2_integration._mocks import (
    FlakyTarget,
    GatedFeedStore,
    MockLockInfo,
    MockLockManager,
)
from tests.unit.application.migration.test_phase2_integration._repositories import (
    InMemoryMigrationRepository,
    InMemoryRoutingRepository,
)
from tests.unit.application.migration.test_phase2_integration.test_phase2_abort import (
    TestAbortDuringDifferentPhases,
)
from tests.unit.application.migration.test_phase2_integration.test_phase2_anchor import (
    TestSyncLagAnchorOnWriteActiveTenant,
)
from tests.unit.application.migration.test_phase2_integration.test_phase2_catchup import (
    TestCatchUpRoundsCap,
)
from tests.unit.application.migration.test_phase2_integration.test_phase2_concurrent import (
    TestConcurrentOperations,
)
from tests.unit.application.migration.test_phase2_integration.test_phase2_cutover import (
    TestCutoverFailureAndRollback,
    TestCutoverSuccess,
)
from tests.unit.application.migration.test_phase2_integration.test_phase2_dual_write import (
    TestDualWriteBehavior,
)
from tests.unit.application.migration.test_phase2_integration.test_phase2_error_handling import (
    TestErrorHandlingAndRecovery,
)
from tests.unit.application.migration.test_phase2_integration.test_phase2_first_pass import (
    TestWriteActiveTenantFirstPassCutover,
)
from tests.unit.application.migration.test_phase2_integration.test_phase2_lifecycle import (
    TestFullMigrationLifecycle,
)
from tests.unit.application.migration.test_phase2_integration.test_phase2_pause_resume import (
    TestPauseResumeDuringMigration,
)
from tests.unit.application.migration.test_phase2_integration.test_phase2_sync_lag import (
    TestSyncLagTracking,
)
from tests.unit.application.migration.test_phase2_integration.test_phase2_write_pause import (
    TestWritePauseManager,
)

__all__ = [  # pragma: allowlist secret
    "FlakyTarget",
    "GatedFeedStore",
    "InMemoryMigrationRepository",
    "InMemoryRoutingRepository",
    "MockLockInfo",
    "MockLockManager",
    "OrderConfirmed",
    "OrderCreated",
    "SampleTestEvent",
    "TestAbortDuringDifferentPhases",
    "TestCatchUpRoundsCap",
    "TestConcurrentOperations",
    "TestCutoverFailureAndRollback",
    "TestCutoverSuccess",
    "TestDualWriteBehavior",
    "TestErrorHandlingAndRecovery",
    "TestFullMigrationLifecycle",
    "TestPauseResumeDuringMigration",
    "TestSyncLagAnchorOnWriteActiveTenant",
    "TestSyncLagTracking",
    "TestWriteActiveTenantFirstPassCutover",
    "TestWritePauseManager",
    "_REGISTRY",
    "coordinator",
    "create_test_events",
    "drive_dual_writes",
    "get_all_tenant_events",
    "lock_manager",
    "migration_repo",
    "router",
    "routing_repo",
    "seed_copied_prefix",
    "source_store",
    "target_store",
    "tenant_id",
    "write_pause_manager",
]
