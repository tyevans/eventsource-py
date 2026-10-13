"""Modular tests package for SubscriptionMigrator."""

from tests.unit.application.migration.test_subscription_migrator._fixtures import (
    SOURCE_STORE_ID,
    TARGET_STORE_ID,
    SubscriptionMigratorTestBase,
    exact_translation,
    make_checkpoint_data,
    source_pos,
    target_pos,
)
from tests.unit.application.migration.test_subscription_migrator.test_subscription_migrator_init_and_plan import (
    TestSubscriptionMigratorInit,
    TestSubscriptionMigratorPlanMigration,
)
from tests.unit.application.migration.test_subscription_migrator.test_subscription_migrator_migrate import (
    TestSubscriptionMigratorMigrateSubscriptions,
    TestSubscriptionMigratorMigrateTenantSubscriptions,
    TestSubscriptionMigratorVerifyMigration,
)
from tests.unit.application.migration.test_subscription_migrator.test_subscription_migrator_models_and_workflows import (
    TestMigrationPlanDataclass,
    TestMigrationSummaryDataclass,
    TestPlannedMigrationDataclass,
    TestSubscriptionMigrationErrorException,
    TestSubscriptionMigrationResultDataclass,
    TestSubscriptionMigratorWorkflows,
)

__all__ = [  # pragma: allowlist secret
    "SOURCE_STORE_ID",
    "SubscriptionMigratorTestBase",
    "TARGET_STORE_ID",
    "TestMigrationPlanDataclass",
    "TestMigrationSummaryDataclass",
    "TestPlannedMigrationDataclass",
    "TestSubscriptionMigrationErrorException",
    "TestSubscriptionMigrationResultDataclass",
    "TestSubscriptionMigratorInit",
    "TestSubscriptionMigratorMigrateSubscriptions",
    "TestSubscriptionMigratorMigrateTenantSubscriptions",
    "TestSubscriptionMigratorPlanMigration",
    "TestSubscriptionMigratorVerifyMigration",
    "TestSubscriptionMigratorWorkflows",
    "exact_translation",
    "make_checkpoint_data",
    "source_pos",
    "target_pos",
]
