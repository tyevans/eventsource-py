"""Tests for edge cases in KafkaEventBus metrics handling."""

from collections.abc import Generator
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.kafka import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
    KafkaEventBusMetrics,
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


def _sample_event() -> SampleOrderCreated:
    return SampleOrderCreated(
        aggregate_id=uuid4(),
        aggregate_version=1,
        order_number="ORD-001",
        customer_id=uuid4(),
    )


def _bus_with_metrics() -> tuple[KafkaEventBus, InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("test.kafka.eventbus")
    with patch.object(KafkaEventBusConfig, "_validate_security_config"):
        bus = KafkaEventBus()
    test_metrics = KafkaEventBusMetrics(meter)
    bus._connection_manager.metrics = test_metrics
    bus._publisher._metrics = test_metrics
    bus._consumer_loop._metrics = test_metrics
    bus._meter = meter
    return bus, reader


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
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig(enable_metrics=False)
            bus = KafkaEventBus(config=config)

        bus._connection_manager.record_reconnection()
        bus._connection_manager.record_rebalance()

        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = _sample_event()
        handlers = bus._handlers_for(SampleOrderCreated)
        await bus._consumer_loop._dispatch_to_handlers(event, handlers)

    @pytest.mark.asyncio
    async def test_no_error_when_otel_unavailable(self) -> None:
        import eventsource.adapters.kafka.bus as kafka_module

        with (
            patch.object(kafka_module, "OTEL_AVAILABLE", False),
            patch.object(KafkaEventBusConfig, "_validate_security_config"),
        ):
            kafka_module._meter = None
            config = KafkaEventBusConfig(enable_metrics=True)
            bus = KafkaEventBus(config=config)

        assert bus._connection_manager.metrics is None
        bus._connection_manager.record_reconnection()
        bus._connection_manager.record_rebalance()

    @skip_if_no_otel_metrics
    @pytest.mark.asyncio
    async def test_handler_metrics_on_error_still_records_duration(self) -> None:
        import eventsource.adapters.kafka.bus as kafka_module

        kafka_module._meter = None
        try:

            async def failing_handler(event: DomainEvent) -> None:
                raise ValueError("Test error")

            bus, reader = _bus_with_metrics()
            bus.subscribe(SampleOrderCreated, failing_handler)

            event = _sample_event()
            handlers = bus._handlers_for(SampleOrderCreated)

            with pytest.raises(HandlerDispatchError):
                await bus._consumer_loop._dispatch_to_handlers(event, handlers)

            metrics_data = reader.get_metrics_data()
            duration_recorded = _has_histogram_data(metrics_data, "kafka.eventbus.handler.duration")
            assert duration_recorded
        finally:
            kafka_module._meter = None

    @skip_if_no_otel_metrics
    @pytest.mark.asyncio
    async def test_multiple_handlers_increment_counter_correctly(self) -> None:
        import eventsource.adapters.kafka.bus as kafka_module

        kafka_module._meter = None
        try:
            bus, reader = _bus_with_metrics()
            for _ in range(3):
                bus.subscribe(SampleOrderCreated, OrderHandler())

            event = _sample_event()
            handlers = bus._handlers_for(SampleOrderCreated)
            await bus._consumer_loop._dispatch_to_handlers(event, handlers)

            metrics_data = reader.get_metrics_data()
            invocation_count = _get_metric_value(metrics_data, "kafka.eventbus.handler.invocations")
            assert invocation_count == 3
        finally:
            kafka_module._meter = None

    @skip_if_no_otel_metrics
    @pytest.mark.asyncio
    async def test_publish_error_counter(self) -> None:
        import eventsource.adapters.kafka.bus as kafka_module

        kafka_module._meter = None
        try:
            mock_producer = AsyncMock()
            mock_producer.send.side_effect = Exception("Publish failed")
            bus, reader = _bus_with_metrics()

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
                with pytest.raises(Exception, match="Publish failed"):
                    await bus.publish([_sample_event()])

                metrics_data = reader.get_metrics_data()
                error_count = _get_metric_value(metrics_data, "kafka.eventbus.publish.errors")
                assert error_count == 1
                await bus.disconnect()
        finally:
            kafka_module._meter = None
