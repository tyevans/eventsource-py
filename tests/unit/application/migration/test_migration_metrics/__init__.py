"""Unit tests for migration metrics decomposed package."""

from ._fixtures import (
    clean_metrics_registry,
    mock_counter,
    mock_histogram,
)
from .test_metrics_core import (
    TestMigrationMetrics,
    TestMigrationMetricSnapshot,
)
from .test_metrics_noop_and_otel import (
    TestMetricsWithMockedOTel,
    TestMigrationMetricsNoOTel,
    TestNoOpInstruments,
    TestOTELMetricsAvailable,
)
from .test_metrics_trackers_and_timers import (
    TestActiveMigrationsTracker,
    TestCutoverTimer,
    TestImports,
    TestMetricsRegistry,
    TestObservableGaugeCallbacks,
    TestPhaseTimer,
)

__all__ = [  # pragma: allowlist secret
    "TestActiveMigrationsTracker",
    "TestCutoverTimer",
    "TestImports",
    "TestMetricsRegistry",
    "TestMetricsWithMockedOTel",
    "TestMigrationMetricSnapshot",
    "TestMigrationMetrics",
    "TestMigrationMetricsNoOTel",
    "TestNoOpInstruments",
    "TestOTELMetricsAvailable",
    "TestObservableGaugeCallbacks",
    "TestPhaseTimer",
    "clean_metrics_registry",
    "mock_counter",
    "mock_histogram",
]
