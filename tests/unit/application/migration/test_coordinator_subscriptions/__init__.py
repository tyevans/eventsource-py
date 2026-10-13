"""Migration coordinator subscriptions test package facade."""

from tests.unit.application.migration.test_coordinator_subscriptions._fixtures import (
    coordinator_deps,
    create_coordinator,
    create_coordinator_deps,
    make_migration_summary,
    make_verification_report,
    source_pos,
    target_pos,
)
from tests.unit.application.migration.test_coordinator_subscriptions.test_coordinator_subscriptions_cutover import (
    TestCleanupMigrationResourcesP3005,
    TestCompleteCutoverWithP3005,
)
from tests.unit.application.migration.test_coordinator_subscriptions.test_coordinator_subscriptions_migrate import (
    TestGetSubscriptionSummary,
    TestMigrateSubscriptions,
)
from tests.unit.application.migration.test_coordinator_subscriptions.test_coordinator_subscriptions_verify import (
    TestGetConsistencyReport,
    TestVerificationLevel,
    TestVerifyConsistency,
)

__all__ = [  # pragma: allowlist secret
    "TestCleanupMigrationResourcesP3005",  # pragma: allowlist secret
    "TestCompleteCutoverWithP3005",  # pragma: allowlist secret
    "TestGetConsistencyReport",  # pragma: allowlist secret
    "TestGetSubscriptionSummary",  # pragma: allowlist secret
    "TestMigrateSubscriptions",  # pragma: allowlist secret
    "TestVerificationLevel",  # pragma: allowlist secret
    "TestVerifyConsistency",  # pragma: allowlist secret
    "coordinator_deps",  # pragma: allowlist secret
    "create_coordinator",  # pragma: allowlist secret
    "create_coordinator_deps",  # pragma: allowlist secret
    "make_migration_summary",  # pragma: allowlist secret
    "make_verification_report",  # pragma: allowlist secret
    "source_pos",  # pragma: allowlist secret
    "target_pos",  # pragma: allowlist secret
]
