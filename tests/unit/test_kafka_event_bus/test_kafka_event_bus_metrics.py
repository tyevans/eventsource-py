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

from ._fixtures import (
    OTEL_METRICS_AVAILABLE,
    InMemoryMetricReader,
    MeterProvider,
    OrderHandler,
    SampleOrderCreated,
    _get_histogram_sum,
    _get_metric_attributes,
    _get_metric_value,
    _has_histogram_data,
    skip_if_no_otel_metrics,
)


@pytest.fixture(autouse=True)
def _reset_kafka_meter() -> Generator[None]:
    """Reset cached meter before and after each test."""
    kafka_module._meter = None
    yield
    kafka_module._meter = None


def _sample_event() -> SampleOrderCreated:
    return SampleOrderCreated(
        aggregate_id=uuid4(),
        aggregate_version=1,
        order_number="ORD-001",
        customer_id=uuid4(),
    )


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
class TestKafkaEventBusMetricsInfrastructure:
    """Tests for metrics infrastructure."""

    @skip_if_no_otel_metrics
    def test_get_meter_returns_meter(self) -> None:
        """Test _get_meter returns a meter instance."""
        assert _get_meter() is not None

    @skip_if_no_otel_metrics
    def test_get_meter_cached(self) -> None:
        """Test _get_meter returns cached meter on subsequent calls."""
        meter1 = _get_meter()
        meter2 = _get_meter()
        assert meter1 is meter2

    def test_get_meter_none_when_otel_unavailable(self) -> None:
        """Test _get_meter returns None when OpenTelemetry not installed."""
        with patch.object(kafka_module, "OTEL_AVAILABLE", False):
            assert _get_meter() is None

    @skip_if_no_otel_metrics
    def test_metrics_class_creates_all_counters(self) -> None:
        """Test KafkaEventBusMetrics creates all counter instruments."""
        metrics = KafkaEventBusMetrics(_get_meter())
        assert hasattr(metrics, "messages_published")
        assert hasattr(metrics, "messages_consumed")
        assert hasattr(metrics, "handler_invocations")
        assert hasattr(metrics, "handler_errors")
        assert hasattr(metrics, "dlq_messages")
        assert hasattr(metrics, "connection_errors")
        assert hasattr(metrics, "reconnections")
        assert hasattr(metrics, "rebalances")
        assert hasattr(metrics, "publish_errors")

    @skip_if_no_otel_metrics
    def test_metrics_class_creates_all_histograms(self) -> None:
        """Test KafkaEventBusMetrics creates all histogram instruments."""
        metrics = KafkaEventBusMetrics(_get_meter())
        assert hasattr(metrics, "publish_duration")
        assert hasattr(metrics, "consume_duration")
        assert hasattr(metrics, "handler_duration")
        assert hasattr(metrics, "batch_publish_size")


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
class TestKafkaEventBusMetricsConfig:
    """Tests for metrics configuration."""

    @skip_if_no_otel_metrics
    def test_metrics_enabled_by_default(self) -> None:
        """Test metrics are enabled by default."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            bus = KafkaEventBus(config=KafkaEventBusConfig())
        assert bus._connection_manager.metrics is not None

    @skip_if_no_otel_metrics
    def test_metrics_disabled_when_config_false(self) -> None:
        """Test metrics disabled when enable_metrics=False."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            bus = KafkaEventBus(config=KafkaEventBusConfig(enable_metrics=False))
        assert bus._connection_manager.metrics is None

    def test_metrics_none_when_otel_unavailable(self) -> None:
        """Test metrics is None when OpenTelemetry not installed."""
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
        """Set up metrics for each test."""
        self.reader = InMemoryMetricReader()
        self.provider = MeterProvider(metric_readers=[self.reader])
        self.meter = self.provider.get_meter("test.kafka.eventbus")
        yield

    def _create_bus_with_test_metrics(self) -> KafkaEventBus:
        """Create a bus and set up metrics with our test provider."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            bus = KafkaEventBus()
        test_metrics = KafkaEventBusMetrics(self.meter)
        bus._connection_manager.metrics = test_metrics
        bus._publisher._metrics = test_metrics
        bus._consumer_loop._metrics = test_metrics
        bus._meter = self.meter
        return bus


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
@pytest.mark.skipif(not OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed")
class TestKafkaEventBusCounterMetrics(_MetricsTestBase):
    """Tests for counter metrics."""

    @pytest.mark.asyncio
    async def test_handler_invocation_counter(self) -> None:
        """Test handler invocation increments counter."""
        bus = self._create_bus_with_test_metrics()
        bus.subscribe(SampleOrderCreated, OrderHandler())
        await bus._consumer_loop._dispatch_to_handlers(
            _sample_event(), bus._handlers_for(SampleOrderCreated)
        )
        metrics_data = self.reader.get_metrics_data()
        assert _get_metric_value(metrics_data, "kafka.eventbus.handler.invocations") == 1

    @pytest.mark.asyncio
    async def test_handler_error_counter(self) -> None:
        """Test handler error increments error counter."""

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Test error")

        bus = self._create_bus_with_test_metrics()
        bus.subscribe(SampleOrderCreated, failing_handler)
        with pytest.raises(HandlerDispatchError):
            await bus._consumer_loop._dispatch_to_handlers(
                _sample_event(), bus._handlers_for(SampleOrderCreated)
            )
        metrics_data = self.reader.get_metrics_data()
        assert _get_metric_value(metrics_data, "kafka.eventbus.handler.errors") == 1
        attributes = _get_metric_attributes(metrics_data, "kafka.eventbus.handler.errors")
        assert attributes.get("error.type") == "ValueError"

    @pytest.mark.asyncio
    async def test_reconnection_counter(self) -> None:
        """Test reconnection counter increments end-to-end through OTel."""
        bus = self._create_bus_with_test_metrics()
        bus._connection_manager.record_reconnection()
        bus._connection_manager.record_reconnection()
        metrics_data = self.reader.get_metrics_data()
        assert _get_metric_value(metrics_data, "kafka.eventbus.reconnections") == 2

    @pytest.mark.asyncio
    async def test_rebalance_counter(self) -> None:
        """Test rebalance counter increments end-to-end through OTel."""
        bus = self._create_bus_with_test_metrics()
        bus._connection_manager.record_rebalance()
        metrics_data = self.reader.get_metrics_data()
        assert _get_metric_value(metrics_data, "kafka.eventbus.rebalances") == 1

    @pytest.mark.asyncio
    async def test_connection_error_counter(self) -> None:
        """Test connection error counter on connect failure."""
        mock_producer = AsyncMock()
        mock_producer.start.side_effect = Exception("Connection failed")
        bus = self._create_bus_with_test_metrics()
        with (
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaProducer", return_value=mock_producer
            ),
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaConsumer", return_value=AsyncMock()
            ),
        ):
            with pytest.raises(Exception, match="Connection failed"):
                await bus.connect()
            metrics_data = self.reader.get_metrics_data()
            assert _get_metric_value(metrics_data, "kafka.eventbus.connection.errors") == 1


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
@pytest.mark.skipif(not OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed")
class TestKafkaEventBusHistogramMetrics(_MetricsTestBase):
    """Tests for histogram metrics."""

    @pytest.mark.asyncio
    async def test_handler_duration_recorded(self) -> None:
        """Test handler duration is recorded."""

        async def slow_handler(event: DomainEvent) -> None:
            await asyncio.sleep(0.01)

        bus = self._create_bus_with_test_metrics()
        bus.subscribe(SampleOrderCreated, slow_handler)
        await bus._consumer_loop._dispatch_to_handlers(
            _sample_event(), bus._handlers_for(SampleOrderCreated)
        )
        metrics_data = self.reader.get_metrics_data()
        assert _has_histogram_data(metrics_data, "kafka.eventbus.handler.duration")
        assert _get_histogram_sum(metrics_data, "kafka.eventbus.handler.duration") >= 10
