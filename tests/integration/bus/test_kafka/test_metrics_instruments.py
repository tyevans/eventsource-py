"""Integration tests for Kafka OpenTelemetry histograms and gauges."""

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
    _get_gauge_value,
    _get_histogram_sum,
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


def _make_config(kafka_container: Any, prefix: str, group: str) -> KafkaEventBusConfig:
    return KafkaEventBusConfig(
        bootstrap_servers=kafka_container.get_bootstrap_server(),
        topic_prefix=f"{prefix}-{uuid4().hex[:8]}",
        consumer_group=f"{group}-{uuid4().hex[:8]}",
        enable_metrics=True,
        enable_tracing=False,
    )


async def _consume_until(bus: KafkaEventBus, handled: list[DomainEvent], target: int) -> None:
    consume_task = asyncio.create_task(bus.start_consuming())
    try:
        for _ in range(50):
            if len(handled) >= target:
                break
            await asyncio.sleep(0.1)
    finally:
        await bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=5.0)
        except TimeoutError:
            consume_task.cancel()


class TestKafkaHistogramIntegration:
    """Integration tests for histogram metrics with real Kafka."""

    async def test_publish_duration_recorded(
        self,
        kafka_container: Any,
        event_registry: EventRegistry,
        metrics_setup: Any,
    ) -> None:
        config = _make_config(kafka_container, "test-histogram-publish", "test-histogram-group")
        async with KafkaEventBus(config=config, event_registry=event_registry) as bus:
            for _ in range(3):
                events: list[DomainEvent] = [
                    TestOrderCreated(
                        aggregate_id=uuid4(),
                        aggregate_version=1,
                        customer_id=uuid4(),
                        total_amount=100.0,
                    )
                    for _ in range(5)
                ]
                await bus.publish(events)

        metrics_data = metrics_setup.get_metrics_data()
        assert _has_histogram_data(metrics_data, "kafka.eventbus.publish.duration")
        duration_sum = _get_histogram_sum(metrics_data, "kafka.eventbus.publish.duration")
        assert duration_sum > 0

    async def test_handler_duration_recorded(
        self,
        kafka_container: Any,
        event_registry: EventRegistry,
        metrics_setup: Any,
    ) -> None:
        config = _make_config(kafka_container, "test-histogram-handler", "test-histogram-group")
        handled_events: list[DomainEvent] = []

        async def slow_handler(event: DomainEvent) -> None:
            await asyncio.sleep(0.01)
            handled_events.append(event)

        async with KafkaEventBus(config=config, event_registry=event_registry) as bus:
            bus.subscribe(TestOrderCreated, slow_handler)
            await bus.publish(
                [
                    TestOrderCreated(
                        aggregate_id=uuid4(),
                        aggregate_version=1,
                        customer_id=uuid4(),
                        total_amount=100.0,
                    )
                ]
            )
            await _consume_until(bus, handled_events, 1)

        assert len(handled_events) >= 1
        metrics_data = metrics_setup.get_metrics_data()
        assert _has_histogram_data(metrics_data, "kafka.eventbus.handler.duration")
        duration_sum = _get_histogram_sum(metrics_data, "kafka.eventbus.handler.duration")
        assert duration_sum > 0


class TestKafkaGaugeIntegration:
    """Integration tests for observable gauge metrics with real Kafka."""

    async def test_connection_gauge_registered(
        self,
        kafka_container: Any,
        event_registry: EventRegistry,
        metrics_setup: Any,
    ) -> None:
        config = _make_config(kafka_container, "test-gauge-conn", "test-gauge-group")
        bus = KafkaEventBus(config=config, event_registry=event_registry)
        assert not bus.is_connected
        assert not bus._connection_gauge_registered

        await bus.connect()
        assert bus.is_connected
        assert bus._connection_gauge_registered

        metrics_data = metrics_setup.get_metrics_data()
        conn_status = _get_gauge_value(metrics_data, "kafka.eventbus.connections.active")
        assert conn_status is not None
        assert conn_status in (0, 1)

        await bus.disconnect()
        assert not bus.is_connected

    async def test_consumer_lag_gauge_registered(
        self,
        kafka_container: Any,
        event_registry: EventRegistry,
        metrics_setup: Any,
    ) -> None:
        config = _make_config(kafka_container, "test-gauge-lag", "test-lag-group")
        handled_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            handled_events.append(event)

        async with KafkaEventBus(config=config, event_registry=event_registry) as bus:
            bus.subscribe(TestOrderCreated, handler)
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
            await _consume_until(bus, handled_events, 5)
            assert bus._lag_gauge_registered

        _ = metrics_setup.get_metrics_data()
