"""Chaos tests package facade for migration resilience tests."""

from __future__ import annotations

from ._fixtures import (
    ChaosTestEvent,
    FailureConfig,
    FailureInjectableStore,
    FailureMode,
    InjectedFailureError,
    InMemoryMigrationRepository,
    InMemoryRoutingRepository,
    MockLockInfo,
    MockLockManager,
    count_tenant_events,
    create_test_events,
    lock_manager,
    migration_repo,
    router,
    routing_repo,
    source_store,
    target_store,
    tenant_id,
    write_pause_manager,
)
from .test_locks_and_timeouts import (
    TestLockContentionScenarios,
    TestTimeoutScenarios,
)
from .test_network_and_crashes import (
    TestNetworkPartitionSimulation,
    TestProcessCrashDuringDualWrite,
)
from .test_recovery_and_scenarios import (
    TestComprehensiveFailureScenarios,
    TestRecoveryAfterFailures,
)
from .test_stores_and_infrastructure import (
    TestFailureInjectionInfrastructure,
    TestTargetStoreFailures,
)

__all__ = [  # pragma: allowlist secret
    "ChaosTestEvent",
    "FailureConfig",
    "FailureInjectableStore",
    "FailureMode",
    "InMemoryMigrationRepository",
    "InMemoryRoutingRepository",
    "InjectedFailureError",
    "MockLockInfo",
    "MockLockManager",
    "TestComprehensiveFailureScenarios",
    "TestFailureInjectionInfrastructure",
    "TestLockContentionScenarios",
    "TestNetworkPartitionSimulation",
    "TestProcessCrashDuringDualWrite",
    "TestRecoveryAfterFailures",
    "TestTargetStoreFailures",
    "TestTimeoutScenarios",
    "count_tenant_events",
    "create_test_events",
    "lock_manager",
    "migration_repo",
    "router",
    "routing_repo",
    "source_store",
    "target_store",
    "tenant_id",
    "write_pause_manager",
]
