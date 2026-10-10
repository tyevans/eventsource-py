"""Integration tests for Kafka event bus OpenTelemetry metrics."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from eventsource import (
    KAFKA_AVAILABLE,
    DomainEvent,
    EventRegistry,
    KafkaEventBus,
    KafkaEventBusConfig,
)
from tests.integration.conftest import TestOrderCreated

from .conftest import skip_if_no_kafka_infra
from .helpers_metrics import (
    _get_metric_attributes,
    _get_metric_value,
    _has_histogram_data,
    skip_if_no_otel_metrics,
)

if not KAFKA_AVAILABLE:
    pytest.skip("Kafka (aiokafka) not available", allow_module_level=True)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.kafka,
    skip_if_no_kafka_infra,
    skip_if_no_otel_metrics,
]


class TestKafkaMetricsIntegration:
    """Integration tests for Kafka event bus OpenTelemetry metrics.

    Note: Because OpenTelemetry only allows setting the MeterProvider once,
    these tests share a single InMemoryMetricReader across the module.
    Metrics are cumulative, so tests record baseline values before
    performing operations and verify the delta.
    """

    async def test_publish_metrics_recorded(
        self,
        kafka_container: Any,
        event_registry: EventRegistry,
        metrics_setup: Any,
    ) -> None:
        """Test that publish operations record metrics to OTLP."""
        bootstrap_servers = kafka_container.get_bootstrap_server()

        # Record baseline metrics before test
        baseline_data = metrics_setup.get_metrics_data()
        baseline_published = _get_metric_value(baseline_data, "kafka.eventbus.messages.published")

        config = KafkaEventBusConfig(
            bootstrap_servers=bootstrap_servers,
            topic_prefix=f"test-metrics-publish-{uuid4().hex[:8]}",
            consumer_group=f"test-metrics-group-{uuid4().hex[:8]}",
            enable_metrics=True,
            enable_tracing=False,
        )

        async with KafkaEventBus(config=config, event_registry=event_registry) as bus:
            # Publish events
            events: list[DomainEvent] = [
                TestOrderCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    customer_id=uuid4(),
                    total_amount=100.0 * i,
                )
                for i in range(10)
            ]

            await bus.publish(events)

        # Collect and verify metrics
        metrics_data = metrics_setup.get_metrics_data()

        # Verify publish counter (check delta from baseline)
        published_count = _get_metric_value(metrics_data, "kafka.eventbus.messages.published")
        delta = published_count - baseline_published
        assert delta >= 10, f"Expected at least 10 new published events, got delta={delta}"

        # Verify publish duration histogram has data
        assert _has_histogram_data(metrics_data, "kafka.eventbus.publish.duration"), (
            "Publish duration histogram should have data"
        )

    async def test_consume_metrics_recorded(
        self,
        kafka_container: Any,
        event_registry: EventRegistry,
        metrics_setup: Any,
    ) -> None:
        """Test that consume operations record metrics."""
        bootstrap_servers = kafka_container.get_bootstrap_server()

        # Record baseline metrics before test
        baseline_data = metrics_setup.get_metrics_data()
        baseline_consumed = _get_metric_value(baseline_data, "kafka.eventbus.messages.consumed")
        baseline_handler_invocations = _get_metric_value(
            baseline_data, "kafka.eventbus.handler.invocations"
        )

        config = KafkaEventBusConfig(
            bootstrap_servers=bootstrap_servers,
            topic_prefix=f"test-metrics-consume-{uuid4().hex[:8]}",
            consumer_group=f"test-metrics-group-{uuid4().hex[:8]}",
            enable_metrics=True,
            enable_tracing=False,
        )

        handled_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            handled_events.append(event)

        async with KafkaEventBus(config=config, event_registry=event_registry) as bus:
            bus.subscribe(TestOrderCreated, handler)

            # Publish events
            events: list[DomainEvent] = [
                TestOrderCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    customer_id=uuid4(),
                    total_amount=100.0 * i,
                )
                for i in range(5)
            ]
            await bus.publish(events)

            # Start consuming in background
            consume_task = asyncio.create_task(bus.start_consuming())

            # Wait for events to be consumed
            try:
                for _ in range(100):  # 10 second timeout
                    if len(handled_events) >= 5:
                        break
                    await asyncio.sleep(0.1)
            finally:
                await bus.stop_consuming()
                try:
                    await asyncio.wait_for(consume_task, timeout=5.0)
                except TimeoutError:
                    consume_task.cancel()

        # Verify consume metrics
        metrics_data = metrics_setup.get_metrics_data()

        consumed_count = _get_metric_value(metrics_data, "kafka.eventbus.messages.consumed")
        consumed_delta = consumed_count - baseline_consumed
        assert consumed_delta >= 5, (
            f"Expected at least 5 new consumed events, got delta={consumed_delta}"
        )

        # Verify handler metrics
        handler_invocations = _get_metric_value(metrics_data, "kafka.eventbus.handler.invocations")
        handler_delta = handler_invocations - baseline_handler_invocations
        assert handler_delta >= 5, (
            f"Expected at least 5 new handler invocations, got delta={handler_delta}"
        )

        # Verify consume duration histogram
        assert _has_histogram_data(metrics_data, "kafka.eventbus.consume.duration"), (
            "Consume duration histogram should have data"
        )

        # Verify handler duration histogram
        assert _has_histogram_data(metrics_data, "kafka.eventbus.handler.duration"), (
            "Handler duration histogram should have data"
        )

    async def test_handler_error_metrics(
        self,
        kafka_container: Any,
        event_registry: EventRegistry,
        metrics_setup: Any,
    ) -> None:
        """Test handler errors are recorded in metrics."""
        bootstrap_servers = kafka_container.get_bootstrap_server()

        # Record baseline metrics before test
        baseline_data = metrics_setup.get_metrics_data()
        baseline_errors = _get_metric_value(baseline_data, "kafka.eventbus.handler.errors")

        config = KafkaEventBusConfig(
            bootstrap_servers=bootstrap_servers,
            topic_prefix=f"test-metrics-error-{uuid4().hex[:8]}",
            consumer_group=f"test-error-group-{uuid4().hex[:8]}",
            enable_metrics=True,
            enable_tracing=False,
            max_retries=0,  # Don't retry, go straight to DLQ
            enable_dlq=True,
        )

        error_count = 0

        async def failing_handler(event: DomainEvent) -> None:
            nonlocal error_count
            error_count += 1
            raise ValueError("Test handler error")

        async with KafkaEventBus(config=config, event_registry=event_registry) as bus:
            bus.subscribe(TestOrderCreated, failing_handler)

            # Publish event
            event = TestOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                customer_id=uuid4(),
                total_amount=100.0,
            )
            await bus.publish([event])

            # Start consuming
            consume_task = asyncio.create_task(bus.start_consuming())

            # Wait for error processing
            try:
                for _ in range(50):  # 5 second timeout
                    if error_count >= 1:
                        break
                    await asyncio.sleep(0.1)
            finally:
                await bus.stop_consuming()
                try:
                    await asyncio.wait_for(consume_task, timeout=5.0)
                except TimeoutError:
                    consume_task.cancel()

        metrics_data = metrics_setup.get_metrics_data()

        # Verify handler error counter (check delta from baseline)
        handler_errors = _get_metric_value(metrics_data, "kafka.eventbus.handler.errors")
        error_delta = handler_errors - baseline_errors
        assert error_delta >= 1, f"Expected at least 1 new handler error, got delta={error_delta}"

        # Verify error.type attribute is present
        attributes = _get_metric_attributes(metrics_data, "kafka.eventbus.handler.errors")
        if attributes:
            assert "error.type" in attributes, f"Expected error.type attribute, got {attributes}"
