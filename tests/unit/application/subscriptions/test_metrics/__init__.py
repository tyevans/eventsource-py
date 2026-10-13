"""Unit tests for subscription metrics package."""

from __future__ import annotations

from ._fixtures import (
    mock_counter,
    mock_histogram,
    reset_meter,
    reset_metrics_registry,
)
from .test_metrics_instruments import (
    TestImports,
    TestMetricSnapshot,
    TestNoOpInstruments,
    TestOTELMetricsAvailable,
    TestStateValue,
)
from .test_metrics_registry_and_callbacks import (
    TestMetricsRegistry,
    TestObservableGaugeCallbacks,
    TestTimer,
)
from .test_subscription_metrics import (
    TestMetricsWithMockedOTel,
    TestSubscriptionMetrics,
    TestSubscriptionMetricsNoOTel,
)

__all__ = [  # pragma: allowlist secret
    "TestImports",
    "TestMetricSnapshot",
    "TestMetricsRegistry",
    "TestMetricsWithMockedOTel",
    "TestNoOpInstruments",
    "TestOTELMetricsAvailable",
    "TestObservableGaugeCallbacks",
    "TestStateValue",
    "TestSubscriptionMetrics",
    "TestSubscriptionMetricsNoOTel",
    "TestTimer",
    "mock_counter",
    "mock_histogram",
    "reset_meter",
    "reset_metrics_registry",
]
