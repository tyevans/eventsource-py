"""OpenTelemetry Metrics test helpers and fixtures for Kafka integration tests."""

from __future__ import annotations

from typing import Any

import pytest

from eventsource import EventRegistry
from tests.integration.conftest import (
    TestItemCreated,
    TestItemUpdated,
    TestOrderCreated,
)

OTEL_METRICS_AVAILABLE = False
try:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    OTEL_METRICS_AVAILABLE = True
except ImportError:
    otel_metrics = None  # type: ignore[assignment]
    MeterProvider = None  # type: ignore[assignment, misc]
    InMemoryMetricReader = None  # type: ignore[assignment, misc]


skip_if_no_otel_metrics = pytest.mark.skipif(
    not OTEL_METRICS_AVAILABLE,
    reason="opentelemetry-sdk not installed",
)


def _get_metric_value(metrics_data: Any, metric_name: str) -> int:
    """Get the sum of a counter metric."""
    if not metrics_data or not metrics_data.resource_metrics:
        return 0

    for resource_metric in metrics_data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if metric.name == metric_name:
                    total = 0
                    if hasattr(metric, "data") and hasattr(metric.data, "data_points"):
                        for dp in metric.data.data_points:
                            total += dp.value
                    return total
    return 0


def _get_metric_attributes(metrics_data: Any, metric_name: str) -> dict[str, Any]:
    """Get attributes from the first data point of a metric."""
    if not metrics_data or not metrics_data.resource_metrics:
        return {}

    for resource_metric in metrics_data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if (
                    metric.name == metric_name
                    and hasattr(metric, "data")
                    and hasattr(metric.data, "data_points")
                ):
                    for dp in metric.data.data_points:
                        return dict(dp.attributes)
    return {}


def _has_histogram_data(metrics_data: Any, metric_name: str) -> bool:
    """Check if a histogram has recorded data."""
    if not metrics_data or not metrics_data.resource_metrics:
        return False

    for resource_metric in metrics_data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if (
                    metric.name == metric_name
                    and hasattr(metric, "data")
                    and hasattr(metric.data, "data_points")
                ):
                    return len(metric.data.data_points) > 0
    return False


def _get_histogram_sum(metrics_data: Any, metric_name: str) -> float:
    """Get the sum value from a histogram metric."""
    if not metrics_data or not metrics_data.resource_metrics:
        return 0.0

    for resource_metric in metrics_data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if (
                    metric.name == metric_name
                    and hasattr(metric, "data")
                    and hasattr(metric.data, "data_points")
                ):
                    for dp in metric.data.data_points:
                        return float(dp.sum)
    return 0.0


def _get_gauge_value(metrics_data: Any, metric_name: str) -> float | None:
    """Get the value from a gauge metric."""
    if not metrics_data or not metrics_data.resource_metrics:
        return None

    for resource_metric in metrics_data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if (
                    metric.name == metric_name
                    and hasattr(metric, "data")
                    and hasattr(metric.data, "data_points")
                ):
                    for dp in metric.data.data_points:
                        return float(dp.value)
    return None


_TEST_METER_PROVIDER: Any = None
_TEST_METRIC_READER: Any = None


@pytest.fixture(scope="module")
def metrics_provider() -> Any:
    """Module-scoped fixture that sets up the OTel MeterProvider once."""
    global _TEST_METER_PROVIDER, _TEST_METRIC_READER

    if not OTEL_METRICS_AVAILABLE:
        pytest.skip("opentelemetry-sdk not installed")

    import eventsource.adapters.kafka.bus as kafka_module

    if _TEST_METER_PROVIDER is None:
        kafka_module._meter = None
        _TEST_METRIC_READER = InMemoryMetricReader()
        _TEST_METER_PROVIDER = MeterProvider(metric_readers=[_TEST_METRIC_READER])
        otel_metrics.set_meter_provider(_TEST_METER_PROVIDER)

    yield _TEST_METER_PROVIDER


@pytest.fixture
def metrics_setup(metrics_provider: Any) -> Any:
    """Per-test fixture that provides a fresh view of metrics."""
    global _TEST_METRIC_READER

    if not OTEL_METRICS_AVAILABLE:
        pytest.skip("opentelemetry-sdk not installed")

    import eventsource.adapters.kafka.bus as kafka_module

    kafka_module._meter = None
    yield _TEST_METRIC_READER
    kafka_module._meter = None


@pytest.fixture
def event_registry() -> EventRegistry:
    """Create an event registry with test events."""
    registry = EventRegistry()
    registry.register(TestItemCreated)
    registry.register(TestItemUpdated)
    registry.register(TestOrderCreated)
    return registry


__all__ = [
    "OTEL_METRICS_AVAILABLE",
    "_get_gauge_value",
    "_get_histogram_sum",
    "_get_metric_attributes",
    "_get_metric_value",
    "_has_histogram_data",
    "event_registry",
    "metrics_provider",
    "metrics_setup",
    "skip_if_no_otel_metrics",
]
