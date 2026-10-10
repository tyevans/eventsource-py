"""
OpenTelemetry metrics for subscription management.

This module provides metrics instrumentation for the subscription system,
tracking events processed, failures, processing duration, and lag.

The metrics gracefully degrade when OpenTelemetry is not installed -
all operations become no-ops without raising errors.

Example:
    >>> from eventsource.application.subscriptions.metrics import SubscriptionMetrics
    >>>
    >>> metrics = SubscriptionMetrics("OrderProjection")
    >>> metrics.record_event_processed("OrderCreated", 15.5)
    >>> metrics.record_event_failed("OrderCreated", "ValidationError")
    >>> metrics.record_lag(100)
    >>> metrics.record_state("live")

Metrics Exposed:
    - subscription.events.processed (Counter): Total events processed
    - subscription.events.failed (Counter): Total events failed
    - subscription.processing.duration (Histogram): Processing time in milliseconds
    - subscription.lag (Gauge): Current event lag
    - subscription.state (Gauge): Current subscription state (numeric)

All metrics include the 'subscription' attribute for filtering by subscription name.
"""

from __future__ import annotations

from eventsource.application.subscriptions.metrics_registry import (
    _metrics_registry,
    clear_metrics_registry,
    get_metrics,
)
from eventsource.application.subscriptions.metrics_subscription import (
    SubscriptionMetrics,
)
from eventsource.application.subscriptions.metrics_types import (
    OTEL_METRICS_AVAILABLE,
    STATE_MAPPING,
    MetricSnapshot,
    NoOpCounter,
    NoOpGauge,
    NoOpHistogram,
    StateValue,
    _get_meter,
    _Timer,
    metrics,
    reset_meter,
)

__all__ = [
    # Constants
    "OTEL_METRICS_AVAILABLE",
    "STATE_MAPPING",
    "StateValue",
    # Classes
    "SubscriptionMetrics",
    "MetricSnapshot",
    "NoOpCounter",
    "NoOpHistogram",
    "NoOpGauge",
    "_Timer",
    # Functions
    "get_metrics",
    "clear_metrics_registry",
    "reset_meter",
    "_get_meter",
    "_metrics_registry",
    "metrics",
]
