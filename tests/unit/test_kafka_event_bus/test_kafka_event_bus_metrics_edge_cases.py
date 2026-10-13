"""Tests for edge cases in KafkaEventBus metrics handling."""

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
    InMemoryMetricReader,
    MeterProvider,
    OrderHandler,
    SampleOrderCreated,
    _get_metric_value,
    _has_histogram_data,
    skip_if_no_otel_metrics,
)


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
class TestKafkaEventBusMetricsEdgeCases:
    """Tests for edge cases in metrics handling."""

    @pytest.fixture(autouse=True)
    def reset_meter(self) -> Generator[None]:
        """Reset cached meter before each test."""
        import eventsource.adapters.kafka.bus as kafka_module

        kafka_module._meter = None
        yield
        kafka_module._meter = None

    @pytest.mark.asyncio
    async def test_no_error_when_metrics_disabled(self) -> None:
        """Test no errors occur when metrics are disabled."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig(enable_metrics=False)
            bus = KafkaEventBus(config=config)

        # All operations should work without errors
        # (No need to connect/publish since we're testing that metrics being None doesn't crash)

        # Record methods should not raise even with no metrics
        bus._connection_manager.record_reconnection()
        bus._connection_manager.record_rebalance()

        # Dispatch should work without error even with no metrics
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        handlers = bus._handlers_for(SampleOrderCreated)
        await bus._dispatch_to_handlers(event, handlers)

    @pytest.mark.asyncio
    async def test_no_error_when_otel_unavailable(self) -> None:
        """Test no errors occur when OpenTelemetry not installed."""
        import eventsource.adapters.kafka.bus as kafka_module

        with (
            patch.object(kafka_module, "OTEL_AVAILABLE", False),
            patch.object(KafkaEventBusConfig, "_validate_security_config"),
        ):
            kafka_module._meter = None
            config = KafkaEventBusConfig(enable_metrics=True)
            bus = KafkaEventBus(config=config)

        # Bus should be created successfully, just without metrics
        assert bus._metrics is None

        # Operations should not raise
        bus._connection_manager.record_reconnection()
        bus._connection_manager.record_rebalance()

    @skip_if_no_otel_metrics
    @pytest.mark.asyncio
    async def test_handler_metrics_on_error_still_records_duration(self) -> None:
        """Test handler duration is recorded even when handler raises."""
        import eventsource.adapters.kafka.bus as kafka_module
        from eventsource.adapters.kafka import KafkaEventBusMetrics

        kafka_module._meter = None

        # Private provider/reader for this test; the global meter provider
        # is deliberately left untouched (set-once, see setup_metrics note).
        reader = InMemoryMetricReader()
        provider = MeterProvider(metric_readers=[reader])
        meter = provider.get_meter("test.kafka.eventbus")

        try:

            async def failing_handler(event: DomainEvent) -> None:
                raise ValueError("Test error")

            with patch.object(KafkaEventBusConfig, "_validate_security_config"):
                bus = KafkaEventBus()

            # Set up test metrics
            bus._metrics = KafkaEventBusMetrics(meter)
            bus._meter = meter

            bus.subscribe(SampleOrderCreated, failing_handler)

            event = SampleOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                order_number="ORD-001",
                customer_id=uuid4(),
            )

            handlers = bus._handlers_for(SampleOrderCreated)

            with pytest.raises(HandlerDispatchError):
                await bus._dispatch_to_handlers(event, handlers)

            metrics_data = reader.get_metrics_data()

            # Duration should still be recorded on error path
            duration_recorded = _has_histogram_data(metrics_data, "kafka.eventbus.handler.duration")
            assert duration_recorded

        finally:
            kafka_module._meter = None

    @skip_if_no_otel_metrics
    @pytest.mark.asyncio
    async def test_multiple_handlers_increment_counter_correctly(self) -> None:
        """Test that multiple handlers each increment the counter."""
        import eventsource.adapters.kafka.bus as kafka_module
        from eventsource.adapters.kafka import KafkaEventBusMetrics

        kafka_module._meter = None

        # Private provider/reader for this test; the global meter provider
        # is deliberately left untouched (set-once, see setup_metrics note).
        reader = InMemoryMetricReader()
        provider = MeterProvider(metric_readers=[reader])
        meter = provider.get_meter("test.kafka.eventbus")

        try:
            handler1 = OrderHandler()
            handler2 = OrderHandler()
            handler3 = OrderHandler()

            with patch.object(KafkaEventBusConfig, "_validate_security_config"):
                bus = KafkaEventBus()

            # Set up test metrics
            bus._metrics = KafkaEventBusMetrics(meter)
            bus._meter = meter

            bus.subscribe(SampleOrderCreated, handler1)
            bus.subscribe(SampleOrderCreated, handler2)
            bus.subscribe(SampleOrderCreated, handler3)

            event = SampleOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                order_number="ORD-001",
                customer_id=uuid4(),
            )

            handlers = bus._handlers_for(SampleOrderCreated)
            await bus._dispatch_to_handlers(event, handlers)

            metrics_data = reader.get_metrics_data()
            invocation_count = _get_metric_value(metrics_data, "kafka.eventbus.handler.invocations")
            assert invocation_count == 3

        finally:
            kafka_module._meter = None

    @skip_if_no_otel_metrics
    @pytest.mark.asyncio
    async def test_publish_error_counter(self) -> None:
        """Test publish error increments publish_errors counter."""
        import eventsource.adapters.kafka.bus as kafka_module
        from eventsource.adapters.kafka import KafkaEventBusMetrics

        kafka_module._meter = None

        # Private provider/reader for this test; the global meter provider
        # is deliberately left untouched (set-once, see setup_metrics note).
        reader = InMemoryMetricReader()
        provider = MeterProvider(metric_readers=[reader])
        meter = provider.get_meter("test.kafka.eventbus")

        try:
            mock_producer = AsyncMock()
            mock_producer.send.side_effect = Exception("Publish failed")

            with patch.object(KafkaEventBusConfig, "_validate_security_config"):
                bus = KafkaEventBus()

            # Set up test metrics
            bus._metrics = KafkaEventBusMetrics(meter)
            bus._meter = meter

            with (
                patch(
                    "eventsource.adapters.kafka.connection.AIOKafkaProducer",
                    return_value=mock_producer,
                ),
                patch(
                    "eventsource.adapters.kafka.connection.AIOKafkaConsumer",
                    return_value=AsyncMock(),
                ),
            ):
                await bus.connect()

                event = SampleOrderCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    order_number="ORD-001",
                    customer_id=uuid4(),
                )

                with pytest.raises(Exception, match="Publish failed"):
                    await bus.publish([event])

                metrics_data = reader.get_metrics_data()
                error_count = _get_metric_value(metrics_data, "kafka.eventbus.publish.errors")
                assert error_count == 1

                await bus.disconnect()

        finally:
            kafka_module._meter = None
