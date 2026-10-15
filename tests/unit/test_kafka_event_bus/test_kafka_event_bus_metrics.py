"""Tests for KafkaEventBus OpenTelemetry metrics infrastructure, config, counters, and histograms."""

import asyncio
from collections.abc import Generator
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

import eventsource.adapters.kafka.bus as kafka_module
from eventsource.adapters.kafka import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
    KafkaEventBusMetrics,
)
from eventsource.adapters.kafka.bus import _get_meter
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerDispatchError

from . import _fixtures as fx


@pytest.fixture(autouse=True)
def _reset_kafka_meter() -> Generator[None]:
    kafka_module._meter = None
    yield
    kafka_module._meter = None


def _sample_event() -> fx.SampleOrderCreated:
    return fx.SampleOrderCreated(
        aggregate_id=uuid4(),
        aggregate_version=1,
        order_number="ORD-001",
        customer_id=uuid4(),
    )


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
class TestKafkaEventBusMetricsInfrastructure:
    """Tests for metrics infrastructure."""

    @fx.skip_if_no_otel_metrics
    def test_get_meter_returns_meter(self) -> None:
        assert _get_meter() is not None

    @fx.skip_if_no_otel_metrics
    def test_get_meter_cached(self) -> None:
        assert _get_meter() is _get_meter()

    def test_get_meter_none_when_otel_unavailable(self) -> None:
        with patch.object(kafka_module, "OTEL_AVAILABLE", False):
            assert _get_meter() is None

    @fx.skip_if_no_otel_metrics
    def test_metrics_class_creates_all_counters(self) -> None:
        metrics = KafkaEventBusMetrics(_get_meter())
        for attr in (
            "messages_published",
            "messages_consumed",
            "handler_invocations",
            "handler_errors",
            "dlq_messages",
            "connection_errors",
            "reconnections",
            "rebalances",
            "publish_errors",
        ):
            assert hasattr(metrics, attr)

    @fx.skip_if_no_otel_metrics
    def test_metrics_class_creates_all_histograms(self) -> None:
        metrics = KafkaEventBusMetrics(_get_meter())
        for attr in (
            "publish_duration",
            "consume_duration",
            "handler_duration",
            "batch_publish_size",
        ):
            assert hasattr(metrics, attr)


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
class TestKafkaEventBusMetricsConfig:
    """Tests for metrics configuration."""

    @fx.skip_if_no_otel_metrics
    def test_metrics_enabled_by_default(self) -> None:
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            bus = KafkaEventBus(config=KafkaEventBusConfig())
        assert bus._connection_manager.metrics is not None

    @fx.skip_if_no_otel_metrics
    def test_metrics_disabled_when_config_false(self) -> None:
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            bus = KafkaEventBus(config=KafkaEventBusConfig(enable_metrics=False))
        assert bus._connection_manager.metrics is None

    def test_metrics_none_when_otel_unavailable(self) -> None:
        with (
            patch.object(kafka_module, "OTEL_AVAILABLE", False),
            patch.object(KafkaEventBusConfig, "_validate_security_config"),
        ):
            bus = KafkaEventBus(config=KafkaEventBusConfig(enable_metrics=True))
        assert bus._connection_manager.metrics is None


class _MetricsTestBase:
    """Shared metrics setup for counter and histogram tests."""

    @pytest.fixture(autouse=True)
    def setup_metrics(self) -> Generator[None]:
        self.reader = fx.InMemoryMetricReader()
        self.provider = fx.MeterProvider(metric_readers=[self.reader])
        self.meter = self.provider.get_meter("test.kafka.eventbus")
        yield

    def _create_bus_with_test_metrics(self) -> KafkaEventBus:
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            bus = KafkaEventBus()
        test_metrics = KafkaEventBusMetrics(self.meter)
        bus._connection_manager.metrics = test_metrics
        bus._publisher._metrics = test_metrics
        bus._consumer_loop._metrics = test_metrics
        bus._meter = self.meter
        return bus


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
@pytest.mark.skipif(not fx.OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed")
class TestKafkaEventBusCounterMetrics(_MetricsTestBase):
    """Tests for counter metrics."""

    @pytest.mark.asyncio
    async def test_handler_invocation_counter(self) -> None:
        bus = self._create_bus_with_test_metrics()
        bus.subscribe(fx.SampleOrderCreated, fx.OrderHandler())
        await bus._consumer_loop._dispatch_to_handlers(
            _sample_event(), bus._handlers_for(fx.SampleOrderCreated)
        )
        metrics_data = self.reader.get_metrics_data()
        assert fx._get_metric_value(metrics_data, "kafka.eventbus.handler.invocations") == 1

    @pytest.mark.asyncio
    async def test_handler_error_counter(self) -> None:
        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Test error")

        bus = self._create_bus_with_test_metrics()
        bus.subscribe(fx.SampleOrderCreated, failing_handler)
        with pytest.raises(HandlerDispatchError):
            await bus._consumer_loop._dispatch_to_handlers(
                _sample_event(), bus._handlers_for(fx.SampleOrderCreated)
            )
        metrics_data = self.reader.get_metrics_data()
        assert fx._get_metric_value(metrics_data, "kafka.eventbus.handler.errors") == 1
        attributes = fx._get_metric_attributes(metrics_data, "kafka.eventbus.handler.errors")
        assert attributes.get("error.type") == "ValueError"

    @pytest.mark.asyncio
    async def test_reconnection_counter(self) -> None:
        bus = self._create_bus_with_test_metrics()
        bus._connection_manager.record_reconnection()
        bus._connection_manager.record_reconnection()
        metrics_data = self.reader.get_metrics_data()
        assert fx._get_metric_value(metrics_data, "kafka.eventbus.reconnections") == 2

    @pytest.mark.asyncio
    async def test_rebalance_counter(self) -> None:
        bus = self._create_bus_with_test_metrics()
        bus._connection_manager.record_rebalance()
        metrics_data = self.reader.get_metrics_data()
        assert fx._get_metric_value(metrics_data, "kafka.eventbus.rebalances") == 1

    @pytest.mark.asyncio
    async def test_connection_error_counter(self) -> None:
        mock_producer = AsyncMock()
        mock_producer.start.side_effect = Exception("Connection failed")
        bus = self._create_bus_with_test_metrics()
        conn_mod = "eventsource.adapters.kafka.connection"
        with (
            patch(f"{conn_mod}.AIOKafkaProducer", return_value=mock_producer),
            patch(f"{conn_mod}.AIOKafkaConsumer", return_value=AsyncMock()),
        ):
            with pytest.raises(Exception, match="Connection failed"):
                await bus.connect()
            metrics_data = self.reader.get_metrics_data()
            assert fx._get_metric_value(metrics_data, "kafka.eventbus.connection.errors") == 1


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
@pytest.mark.skipif(not fx.OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed")
class TestKafkaEventBusHistogramMetrics(_MetricsTestBase):
    """Tests for histogram metrics."""

    @pytest.mark.asyncio
    async def test_handler_duration_recorded(self) -> None:
        async def slow_handler(event: DomainEvent) -> None:
            await asyncio.sleep(0.01)

        bus = self._create_bus_with_test_metrics()
        bus.subscribe(fx.SampleOrderCreated, slow_handler)
        await bus._consumer_loop._dispatch_to_handlers(
            _sample_event(), bus._handlers_for(fx.SampleOrderCreated)
        )
        metrics_data = self.reader.get_metrics_data()
        assert fx._has_histogram_data(metrics_data, "kafka.eventbus.handler.duration")
        assert fx._get_histogram_sum(metrics_data, "kafka.eventbus.handler.duration") >= 10
