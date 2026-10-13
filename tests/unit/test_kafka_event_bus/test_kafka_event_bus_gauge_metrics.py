"""Tests for KafkaEventBus observable gauge metrics."""

from collections.abc import Generator
from unittest.mock import AsyncMock, patch

import pytest

from eventsource.adapters.kafka import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
)

from ._fixtures import (
    OTEL_METRICS_AVAILABLE,
    InMemoryMetricReader,
    MeterProvider,
    _get_gauge_value,
)


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
@pytest.mark.skipif(not OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed")
class TestKafkaEventBusGaugeMetrics:
    """Tests for observable gauge metrics."""

    @pytest.fixture(autouse=True)
    def setup_metrics(self) -> Generator[None]:
        """Set up metrics for each test."""
        import eventsource.adapters.kafka.bus as kafka_module

        kafka_module._meter = None

        # Private provider/reader per test. Never set the global meter
        # provider here: it is set-once per process, and the first setter
        # inherits stale _ProxyMeter instruments (e.g. connection gauges
        # from earlier plain-bus tests) materialized onto its provider.
        self.reader = InMemoryMetricReader()
        self.provider = MeterProvider(metric_readers=[self.reader])

        # Create a meter from the new provider
        self.meter = self.provider.get_meter("test.kafka.eventbus")

        yield

        # Reset
        kafka_module._meter = None

    def _create_bus_with_test_metrics(self) -> KafkaEventBus:
        """Create a bus and set up metrics with our test provider."""
        from eventsource.adapters.kafka import KafkaEventBusMetrics

        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            bus = KafkaEventBus()

        # Replace the bus's metrics with one using our test meter
        bus._metrics = KafkaEventBusMetrics(self.meter)
        bus._meter = self.meter

        return bus

    @pytest.mark.asyncio
    async def test_connection_gauge_registered_on_connect(self) -> None:
        """Test connection gauge is registered when connected."""
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        bus = self._create_bus_with_test_metrics()

        with (
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaProducer", return_value=mock_producer
            ),
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaConsumer", return_value=mock_consumer
            ),
        ):
            await bus.connect()

            # Verify gauge registered flag is set
            assert bus._connection_gauge_registered is True

            await bus.disconnect()

    @pytest.mark.asyncio
    async def test_connection_gauge_reports_connected(self) -> None:
        """Test connection gauge reports 1 when connected."""
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        bus = self._create_bus_with_test_metrics()

        with (
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaProducer", return_value=mock_producer
            ),
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaConsumer", return_value=mock_consumer
            ),
        ):
            await bus.connect()

            # Force metric collection
            metrics_data = self.reader.get_metrics_data()

            connection_status = _get_gauge_value(metrics_data, "kafka.eventbus.connections.active")
            assert connection_status == 1

            await bus.disconnect()

    @pytest.mark.asyncio
    async def test_connection_gauge_reports_disconnected(self) -> None:
        """Test connection gauge reports 0 when disconnected."""
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        bus = self._create_bus_with_test_metrics()

        with (
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaProducer", return_value=mock_producer
            ),
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaConsumer", return_value=mock_consumer
            ),
        ):
            await bus.connect()
            await bus.disconnect()

            # Force metric collection
            metrics_data = self.reader.get_metrics_data()

            connection_status = _get_gauge_value(metrics_data, "kafka.eventbus.connections.active")
            assert connection_status == 0

    @pytest.mark.asyncio
    async def test_lag_gauge_registered_on_start_consuming(self) -> None:
        """Test consumer lag gauge is registered when consuming starts."""
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()
        mock_consumer.__aiter__ = AsyncMock(return_value=iter([]))

        bus = self._create_bus_with_test_metrics()

        with (
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaProducer", return_value=mock_producer
            ),
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaConsumer", return_value=mock_consumer
            ),
        ):
            await bus.connect()

            # Manually set consuming to simulate start
            bus._consuming = True
            bus._register_consumer_lag_gauge()

            # Verify gauge registered flag is set
            assert bus._lag_gauge_registered is True

            await bus.disconnect()

    @pytest.mark.asyncio
    async def test_gauge_registration_idempotent(self) -> None:
        """Test gauge registration is idempotent (safe to call multiple times)."""
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        bus = self._create_bus_with_test_metrics()

        with (
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaProducer", return_value=mock_producer
            ),
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaConsumer", return_value=mock_consumer
            ),
        ):
            await bus.connect()

            # Try to register again (should be a no-op)
            bus._register_connection_gauge()
            bus._register_connection_gauge()

            # Should still be registered only once (no error)
            assert bus._connection_gauge_registered is True

            await bus.disconnect()
