"""Migration coordinator unit test package facade."""

from tests.unit.application.migration.test_coordinator._fixtures import (
    TestEvent,
    coordinator_deps,
    create_coordinator,
    create_coordinator_deps,
    create_test_migration,
    setup_start_migration,
)
from tests.unit.application.migration.test_coordinator.test_coordinator_lifecycle import (
    TestAbortMigration,
    TestFailMigration,
    TestMigrationCoordinatorInit,
    TestPauseMigration,
    TestResumeMigration,
    TestStartMigration,
)
from tests.unit.application.migration.test_coordinator.test_coordinator_queries import (
    TestCalculateDuration,
    TestGetMigration,
    TestGetMigrationForTenant,
    TestListActiveMigrations,
    TestWaitForPhase,
)
from tests.unit.application.migration.test_coordinator.test_coordinator_status import (
    TestBuildStatus,
    TestGetStatus,
    TestStatusQueueManagement,
)

__all__ = [  # pragma: allowlist secret
    "TestAbortMigration",
    "TestBuildStatus",
    "TestCalculateDuration",
    "TestEvent",
    "TestFailMigration",
    "TestGetMigration",
    "TestGetMigrationForTenant",
    "TestGetStatus",
    "TestListActiveMigrations",
    "TestMigrationCoordinatorInit",
    "TestPauseMigration",
    "TestResumeMigration",
    "TestStartMigration",
    "TestStatusQueueManagement",
    "TestWaitForPhase",
    "coordinator_deps",
    "create_coordinator",
    "create_coordinator_deps",
    "create_test_migration",
    "setup_start_migration",
]
