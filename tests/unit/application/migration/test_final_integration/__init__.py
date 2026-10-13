"""Final integration test package facade."""

from __future__ import annotations

from tests.unit.application.migration.test_final_integration._events import (
    _REGISTRY,
    SOURCE_STORE_ID,
    TARGET_STORE_ID,
    OrderCreated,
    OrderUpdated,
    SampleTestEvent,
    src_pos,
    tgt_pos,
)
from tests.unit.application.migration.test_final_integration._fixtures import (
    audit_log_repo,
    checkpoint_repo,
    cleanup_metrics,
    lock_manager,
    migration_id,
    migration_repo,
    position_mapper,
    position_mapping_repo,
    router,
    routing_repo,
    source_store,
    target_store,
    tenant_id,
    write_pause_manager,
)
from tests.unit.application.migration.test_final_integration._helpers import (
    copy_events_between_stores,
    create_test_events,
)
from tests.unit.application.migration.test_final_integration._mocks import (
    InMemoryAuditLogRepository,
    InMemoryPositionMappingRepository,
    MockLockInfo,
    MockLockManager,
)
from tests.unit.application.migration.test_final_integration._repositories import (
    InMemoryCheckpointRepository,
    InMemoryMigrationRepository,
    InMemoryRoutingRepository,
)
from tests.unit.application.migration.test_final_integration.test_audit_logging import (
    TestAuditLoggingThroughoutMigration,
)
from tests.unit.application.migration.test_final_integration.test_error_handling import (
    TestErrorHandlingAndClassification,
)
from tests.unit.application.migration.test_final_integration.test_lifecycle import (
    TestCompleteMigrationLifecycleWithOperationalFeatures,
)
from tests.unit.application.migration.test_final_integration.test_metrics_and_status import (
    TestMetricsCollectionDuringMigration,
    TestStatusStreamingDuringMigration,
)
from tests.unit.application.migration.test_final_integration.test_phase4_components import (
    TestIntegrationOfAllPhase4Components,
)
from tests.unit.application.migration.test_final_integration.test_recovery import (
    TestRecoveryAfterSimulatedFailures,
)

__all__ = [  # pragma: allowlist secret
    "InMemoryAuditLogRepository",
    "InMemoryCheckpointRepository",
    "InMemoryMigrationRepository",
    "InMemoryPositionMappingRepository",
    "InMemoryRoutingRepository",
    "MockLockInfo",
    "MockLockManager",
    "OrderCreated",
    "OrderUpdated",
    "SOURCE_STORE_ID",
    "SampleTestEvent",
    "TARGET_STORE_ID",
    "TestAuditLoggingThroughoutMigration",
    "TestCompleteMigrationLifecycleWithOperationalFeatures",
    "TestErrorHandlingAndClassification",
    "TestIntegrationOfAllPhase4Components",
    "TestMetricsCollectionDuringMigration",
    "TestRecoveryAfterSimulatedFailures",
    "TestStatusStreamingDuringMigration",
    "_REGISTRY",
    "audit_log_repo",
    "checkpoint_repo",
    "cleanup_metrics",
    "copy_events_between_stores",
    "create_test_events",
    "lock_manager",
    "migration_id",
    "migration_repo",
    "position_mapper",
    "position_mapping_repo",
    "router",
    "routing_repo",
    "source_store",
    "src_pos",
    "target_store",
    "tenant_id",
    "tgt_pos",
    "write_pause_manager",
]
