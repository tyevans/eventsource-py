"""Shared fixtures, sample events, handlers, and metric helpers for KafkaEventBus tests."""

from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry

__all__ = [
    "InMemoryMetricReader",
    "MeterProvider",
    "OTEL_METRICS_AVAILABLE",
    "OrderHandler",
    "SampleKafkaEvent",
    "SampleOrderCreated",
    "SampleOrderShipped",
    "SamplePaymentReceived",
    "SampleSubscriber",
    "SyncHandler",
    "_get_gauge_value",
    "_get_histogram_sum",
    "_get_metric_attributes",
    "_get_metric_value",
    "_has_histogram_data",
    "event_registry",
    "mock_producer",
    "otel_metrics",
    "sample_event",
    "skip_if_no_otel_metrics",
]


class SampleOrderCreated(DomainEvent):
    aggregate_type: str = "Order"
    order_number: str
    customer_id: UUID


class SampleOrderShipped(DomainEvent):
    aggregate_type: str = "Order"
    tracking_number: str


class SamplePaymentReceived(DomainEvent):
    aggregate_type: str = "Payment"
    amount: float


class SampleKafkaEvent(DomainEvent):
    aggregate_type: str = "SampleAggregate"


class OrderHandler:
    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []

    async def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


class SyncHandler:
    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []

    def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


class SampleSubscriber:
    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SampleOrderCreated, SampleOrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


@pytest.fixture
def event_registry() -> EventRegistry:
    registry = EventRegistry()
    registry.register(SampleOrderCreated)
    registry.register(SampleOrderShipped)
    registry.register(SamplePaymentReceived)
    return registry


@pytest.fixture
def sample_event() -> SampleOrderCreated:
    return SampleOrderCreated(
        aggregate_id=uuid4(),
        aggregate_version=1,
        order_number="ORD-001",
        customer_id=uuid4(),
    )


@pytest.fixture
def mock_producer() -> AsyncMock:
    producer = AsyncMock()

    async def default_send(*args: Any, **kwargs: Any) -> Any:
        future: AsyncMock = AsyncMock()
        return future

    producer.send = default_send
    return producer


try:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    OTEL_METRICS_AVAILABLE = True
except ImportError:
    otel_metrics = None  # type: ignore[assignment]
    MeterProvider = None  # type: ignore[assignment, misc]
    InMemoryMetricReader = None  # type: ignore[assignment, misc]
    OTEL_METRICS_AVAILABLE = False

skip_if_no_otel_metrics = pytest.mark.skipif(
    not OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed"
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
