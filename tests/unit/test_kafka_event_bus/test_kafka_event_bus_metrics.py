"""Tests for KafkaEventBus OpenTelemetry metrics infrastructure, config, counters, and histograms."""

from collections.abc import Generator
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.kafka import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
)
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


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
class TestKafkaEventBusMetricsInfrastructure:
    """Tests for metrics infrastructure."""

    @pytest.fixture(autouse=True)
    def reset_meter(self) -> Generator[None]:
        """Reset cached meter before each test."""
        import eventsource.adapters.kafka.bus as kafka_module

        kafka_module._meter = None
        yield
        kafka_module._meter = None

    @skip_if_no_otel_metrics
    def test_get_meter_returns_meter(self) -> None:
        """Test _get_meter returns a meter instance."""
        from eventsource.adapters.kafka.bus import _get_meter

        meter = _get_meter()
        assert meter is not None

    @skip_if_no_otel_metrics
    def test_get_meter_cached(self) -> None:
        """Test _get_meter returns cached meter on subsequent calls."""
        from eventsource.adapters.kafka.bus import _get_meter

        meter1 = _get_meter()
        meter2 = _get_meter()
        assert meter1 is meter2

    def test_get_meter_none_when_otel_unavailable(self) -> None:
        """Test _get_meter returns None when OpenTelemetry not installed."""
        import eventsource.adapters.kafka.bus as kafka_module

        kafka_module._meter = None

        with patch.object(kafka_module, "OTEL_AVAILABLE", False):
            from eventsource.adapters.kafka.bus import _get_meter

            meter = _get_meter()
            assert meter is None

    @skip_if_no_otel_metrics
    def test_metrics_class_creates_all_counters(self) -> None:
        """Test KafkaEventBusMetrics creates all counter instruments."""
        from eventsource.adapters.kafka.bus import KafkaEventBusMetrics, _get_meter

        meter = _get_meter()
        metrics = KafkaEventBusMetrics(meter)

        # Verify all 9 counters exist
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
        from eventsource.adapters.kafka.bus import KafkaEventBusMetrics, _get_meter

        meter = _get_meter()
        metrics = KafkaEventBusMetrics(meter)

        # Verify all 4 histograms exist
        assert hasattr(metrics, "publish_duration")
        assert hasattr(metrics, "consume_duration")
        assert hasattr(metrics, "handler_duration")
        assert hasattr(metrics, "batch_publish_size")


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
class TestKafkaEventBusMetricsConfig:
    """Tests for metrics configuration."""

    @pytest.fixture(autouse=True)
    def reset_meter(self) -> Generator[None]:
        """Reset cached meter before each test."""
        import eventsource.adapters.kafka.bus as kafka_module

        kafka_module._meter = None
        yield
        kafka_module._meter = None

    @skip_if_no_otel_metrics
    def test_metrics_enabled_by_default(self) -> None:
        """Test metrics are enabled by default."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig()
            bus = KafkaEventBus(config=config)

        assert bus._connection_manager.metrics is not None

    @skip_if_no_otel_metrics
    def test_metrics_disabled_when_config_false(self) -> None:
        """Test metrics disabled when enable_metrics=False."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig(enable_metrics=False)
            bus = KafkaEventBus(config=config)

        assert bus._connection_manager.metrics is None

    def test_metrics_none_when_otel_unavailable(self) -> None:
        """Test metrics is None when OpenTelemetry not installed."""
        import eventsource.adapters.kafka.bus as kafka_module

        with (
            patch.object(kafka_module, "OTEL_AVAILABLE", False),
            patch.object(KafkaEventBusConfig, "_validate_security_config"),
        ):
            kafka_module._meter = None
            config = KafkaEventBusConfig(enable_metrics=True)
            bus = KafkaEventBus(config=config)

        assert bus._connection_manager.metrics is None


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
@pytest.mark.skipif(not OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed")
class TestKafkaEventBusCounterMetrics:
    """Tests for counter metrics."""

    @pytest.fixture(autouse=True)
    def setup_metrics(self) -> Generator[None]:
        """Set up metrics for each test."""
        import eventsource.adapters.kafka.bus as kafka_module

        kafka_module._meter = None

        self.reader = InMemoryMetricReader()
        self.provider = MeterProvider(metric_readers=[self.reader])
        self.meter = self.provider.get_meter("test.kafka.eventbus")

        yield

        kafka_module._meter = None

    def _create_bus_with_test_metrics(self) -> KafkaEventBus:
        """Create a bus and set up metrics with our test provider."""
        from eventsource.adapters.kafka import KafkaEventBusMetrics

        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            bus = KafkaEventBus()

        test_metrics = KafkaEventBusMetrics(self.meter)
        bus._connection_manager.metrics = test_metrics
        bus._publisher._metrics = test_metrics
        bus._consumer_loop._metrics = test_metrics
        bus._meter = self.meter

        return bus

    @pytest.mark.asyncio
    async def test_handler_invocation_counter(self) -> None:
        """Test handler invocation increments counter."""
        handler = OrderHandler()
        bus = self._create_bus_with_test_metrics()

        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        handlers = bus._handlers_for(SampleOrderCreated)
        await bus._consumer_loop._dispatch_to_handlers(event, handlers)

        metrics_data = self.reader.get_metrics_data()
        invocation_count = _get_metric_value(metrics_data, "kafka.eventbus.handler.invocations")
        assert invocation_count == 1

    @pytest.mark.asyncio
    async def test_handler_error_counter(self) -> None:
        """Test handler error increments error counter."""

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Test error")

        bus = self._create_bus_with_test_metrics()

        bus.subscribe(SampleOrderCreated, failing_handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        handlers = bus._handlers_for(SampleOrderCreated)

        with pytest.raises(HandlerDispatchError):
            await bus._consumer_loop._dispatch_to_handlers(event, handlers)

        metrics_data = self.reader.get_metrics_data()
        error_count = _get_metric_value(metrics_data, "kafka.eventbus.handler.errors")
        assert error_count == 1

        attributes = _get_metric_attributes(metrics_data, "kafka.eventbus.handler.errors")
        assert attributes.get("error.type") == "ValueError"

    @pytest.mark.asyncio
    async def test_reconnection_counter(self) -> None:
        """Test reconnection counter increments end-to-end through OTel."""
        bus = self._create_bus_with_test_metrics()

        bus._connection_manager.record_reconnection()
        bus._connection_manager.record_reconnection()

        metrics_data = self.reader.get_metrics_data()
        reconnection_count = _get_metric_value(metrics_data, "kafka.eventbus.reconnections")
        assert reconnection_count == 2

    @pytest.mark.asyncio
    async def test_rebalance_counter(self) -> None:
        """Test rebalance counter increments end-to-end through OTel."""
        bus = self._create_bus_with_test_metrics()

        bus._connection_manager.record_rebalance()

        metrics_data = self.reader.get_metrics_data()
        rebalance_count = _get_metric_value(metrics_data, "kafka.eventbus.rebalances")
        assert rebalance_count == 1

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
            error_count = _get_metric_value(metrics_data, "kafka.eventbus.connection.errors")
            assert error_count == 1


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
@pytest.mark.skipif(not OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed")
class TestKafkaEventBusHistogramMetrics:
    """Tests for histogram metrics."""

    @pytest.fixture(autouse=True)
    def setup_metrics(self) -> Generator[None]:
        """Set up metrics for each test."""
        import eventsource.adapters.kafka.bus as kafka_module

        kafka_module._meter = None

        self.reader = InMemoryMetricReader()
        self.provider = MeterProvider(metric_readers=[self.reader])
        self.meter = self.provider.get_meter("test.kafka.eventbus")

        yield

        kafka_module._meter = None

    def _create_bus_with_test_metrics(self) -> KafkaEventBus:
        """Create a bus and set up metrics with our test provider."""
        from eventsource.adapters.kafka import KafkaEventBusMetrics

        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            bus = KafkaEventBus()

        test_metrics = KafkaEventBusMetrics(self.meter)
        bus._connection_manager.metrics = test_metrics
        bus._publisher._metrics = test_metrics
        bus._consumer_loop._metrics = test_metrics
        bus._meter = self.meter

        return bus

    @pytest.mark.asyncio
    async def test_handler_duration_recorded(self) -> None:
        """Test handler duration is recorded."""
        import asyncio

        async def slow_handler(event: DomainEvent) -> None:
            await asyncio.sleep(0.01)  # 10ms

        bus = self._create_bus_with_test_metrics()

        bus.subscribe(SampleOrderCreated, slow_handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        handlers = bus._handlers_for(SampleOrderCreated)
        await bus._consumer_loop._dispatch_to_handlers(event, handlers)

        metrics_data = self.reader.get_metrics_data()
        duration_recorded = _has_histogram_data(metrics_data, "kafka.eventbus.handler.duration")
        assert duration_recorded

        duration_sum = _get_histogram_sum(metrics_data, "kafka.eventbus.handler.duration")
        assert duration_sum >= 10  # At least 10ms
