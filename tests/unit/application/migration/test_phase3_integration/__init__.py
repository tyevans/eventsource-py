"""Phase 3 integration test package facade."""

from __future__ import annotations

from tests.unit.application.migration.test_phase3_integration._events import (
    SOURCE_STORE_ID,
    TARGET_STORE_ID,
    OrderCreated,
    OrderUpdated,
    SampleTestEvent,
    source_pos,
    src_pos,
    target_pos,
    tgt_pos,
)
from tests.unit.application.migration.test_phase3_integration._fixtures import (
    checkpoint_repo,
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
from tests.unit.application.migration.test_phase3_integration._helpers import (
    copy_events_with_position_mapping,
    create_test_events,
    get_all_tenant_events,
)
from tests.unit.application.migration.test_phase3_integration._mocks import (
    InMemoryPositionMappingRepository,
    MockLockInfo,
    MockLockManager,
)
from tests.unit.application.migration.test_phase3_integration._repositories import (
    InMemoryCheckpointRepository,
    InMemoryMigrationRepository,
    InMemoryRoutingRepository,
)
from tests.unit.application.migration.test_phase3_integration.test_phase3_consistency import (
    TestConsistencyVerificationLevels,
    TestConsistencyVerificationWithSampling,
)
from tests.unit.application.migration.test_phase3_integration.test_phase3_edge_cases import (
    TestCoordinatorErrorHandling,
    TestMissingMappingErrors,
    TestPositionMapperEdgeCases,
)
from tests.unit.application.migration.test_phase3_integration.test_phase3_full_migration import (
    TestFullMigrationWithVerificationAndSubscriptions,
    TestMigrationSummary,
)
from tests.unit.application.migration.test_phase3_integration.test_phase3_position_mapping import (
    TestPositionMappingRecording,
)
from tests.unit.application.migration.test_phase3_integration.test_phase3_position_translation import (
    TestPositionTranslation,
)
from tests.unit.application.migration.test_phase3_integration.test_phase3_subscriptions import (
    TestDryRunSubscriptionMigration,
    TestSubscriptionCheckpointMigration,
)
from tests.unit.application.migration.test_phase3_integration.test_phase3_verification import (
    TestVerificationFailures,
    TestVerificationReport,
)

__all__ = [  # pragma: allowlist secret
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
    "TestConsistencyVerificationLevels",
    "TestConsistencyVerificationWithSampling",
    "TestCoordinatorErrorHandling",
    "TestDryRunSubscriptionMigration",
    "TestFullMigrationWithVerificationAndSubscriptions",  # pragma: allowlist secret
    "TestMigrationSummary",
    "TestMissingMappingErrors",
    "TestPositionMapperEdgeCases",
    "TestPositionMappingRecording",
    "TestPositionTranslation",
    "TestSubscriptionCheckpointMigration",
    "TestVerificationFailures",
    "TestVerificationReport",
    "checkpoint_repo",
    "copy_events_with_position_mapping",
    "create_test_events",
    "get_all_tenant_events",
    "lock_manager",
    "migration_id",
    "migration_repo",
    "position_mapper",
    "position_mapping_repo",
    "router",
    "routing_repo",
    "source_pos",
    "source_store",
    "src_pos",
    "target_pos",
    "target_store",
    "tenant_id",
    "tgt_pos",
    "write_pause_manager",
]
