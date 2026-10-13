"""SyncLagTracker unit test package facade."""

from ._fixtures import (
    LagTestEvent,
    config,
    seed_events,
    sid,
    source_store,
    strict_config,
    target_store,
    tracker,
)
from .test_sync_lag_tracker_calculate import (
    TestCalculateLag,
    TestConvergenceDetection,
)
from .test_sync_lag_tracker_models_and_init import (
    TestLagSample,
    TestLagStats,
    TestSyncLagTrackerInit,
)
from .test_sync_lag_tracker_scenarios import (
    TestEdgeCases,
    TestIntegrationScenarios,
    TestWithTenantId,
)
from .test_sync_lag_tracker_stats_and_history import (
    TestManualLagRecording,
    TestSampleHistoryManagement,
    TestTrackerLagStats,
)

__all__ = [  # pragma: allowlist secret
    "LagTestEvent",
    "TestCalculateLag",
    "TestConvergenceDetection",
    "TestEdgeCases",
    "TestIntegrationScenarios",
    "TestLagSample",
    "TestLagStats",
    "TestManualLagRecording",
    "TestSampleHistoryManagement",
    "TestSyncLagTrackerInit",
    "TestTrackerLagStats",
    "TestWithTenantId",
    "config",
    "seed_events",
    "sid",
    "source_store",
    "strict_config",
    "target_store",
    "tracker",
]
