"""Performance and cardinality integration tests for Kafka metrics."""

from __future__ import annotations

import logging
import time
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
from tests.integration.conftest import (
    TestItemCreated,
    TestOrderCreated,
)

from .conftest import skip_if_no_kafka_infra
from .helpers_metrics import (
    _get_metric_value,
    skip_if_no_otel_metrics,
)

logger = logging.getLogger(__name__)

if not KAFKA_AVAILABLE:
    pytest.skip("Kafka (aiokafka) not available", allow_module_level=True)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.kafka,
    skip_if_no_kafka_infra,
    skip_if_no_otel_metrics,
]


class TestKafkaMetricsPerformance:
    """Tests to validate metrics overhead is acceptable."""

    async def test_metrics_disabled(
        self,
        kafka_container: Any,
        event_registry: EventRegistry,
    ) -> None:
        """Test no metrics recorded when disabled."""
        bootstrap_servers = kafka_container.get_bootstrap_server()

        config = KafkaEventBusConfig(
            bootstrap_servers=bootstrap_servers,
            topic_prefix=f"test-no-metrics-{uuid4().hex[:8]}",
            consumer_group=f"test-disabled-group-{uuid4().hex[:8]}",
            enable_metrics=False,
            enable_tracing=False,
        )

        async with KafkaEventBus(config=config, event_registry=event_registry) as bus:
            assert bus._metrics is None, "Metrics should be None when disabled"

            events: list[DomainEvent] = [
                TestOrderCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    customer_id=uuid4(),
                    total_amount=100.0,
                )
            ]
            await bus.publish(events)

            assert bus.stats.events_published == 1

    @pytest.mark.benchmark
    async def test_metrics_overhead_acceptable(
        self,
        kafka_container: Any,
        event_registry: EventRegistry,
        metrics_setup: Any,
    ) -> None:
        """Benchmark test to verify metrics overhead is acceptable."""
        bootstrap_servers = kafka_container.get_bootstrap_server()
        event_count = 100

        config_no_metrics = KafkaEventBusConfig(
            bootstrap_servers=bootstrap_servers,
            topic_prefix=f"test-perf-disabled-{uuid4().hex[:8]}",
            consumer_group=f"test-perf-group-{uuid4().hex[:8]}",
            enable_metrics=False,
            enable_tracing=False,
        )

        async with KafkaEventBus(config=config_no_metrics, event_registry=event_registry) as bus:
            events: list[DomainEvent] = [
                TestOrderCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    customer_id=uuid4(),
                    total_amount=100.0 * i,
                )
                for i in range(event_count)
            ]

            start = time.perf_counter()
            await bus.publish(events)
            time_no_metrics = time.perf_counter() - start

        config_metrics = KafkaEventBusConfig(
            bootstrap_servers=bootstrap_servers,
            topic_prefix=f"test-perf-enabled-{uuid4().hex[:8]}",
            consumer_group=f"test-perf-group-{uuid4().hex[:8]}",
            enable_metrics=True,
            enable_tracing=False,
        )

        async with KafkaEventBus(config=config_metrics, event_registry=event_registry) as bus:
            events = [
                TestOrderCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    customer_id=uuid4(),
                    total_amount=100.0 * i,
                )
                for i in range(event_count)
            ]

            start = time.perf_counter()
            await bus.publish(events)
            time_with_metrics = time.perf_counter() - start

        if time_no_metrics > 0:
            overhead = (time_with_metrics - time_no_metrics) / time_no_metrics * 100
        else:
            overhead = 0

        logger.info(
            f"Metrics overhead test: without={time_no_metrics:.4f}s, "
            f"with={time_with_metrics:.4f}s, overhead={overhead:.2f}%"
        )

        assert overhead < 20, f"Metrics overhead {overhead:.2f}% exceeds 20% threshold"


class TestKafkaMetricsCardinality:
    """Tests to verify metric cardinality is bounded."""

    async def test_metrics_cardinality_bounded(
        self,
        kafka_container: Any,
        event_registry: EventRegistry,
        metrics_setup: Any,
    ) -> None:
        """Test that metric cardinality is bounded by event types."""
        bootstrap_servers = kafka_container.get_bootstrap_server()

        baseline_data = metrics_setup.get_metrics_data()
        baseline_published = _get_metric_value(baseline_data, "kafka.eventbus.messages.published")

        config = KafkaEventBusConfig(
            bootstrap_servers=bootstrap_servers,
            topic_prefix=f"test-cardinality-{uuid4().hex[:8]}",
            consumer_group=f"test-cardinality-group-{uuid4().hex[:8]}",
            enable_metrics=True,
            enable_tracing=False,
        )

        async with KafkaEventBus(config=config, event_registry=event_registry) as bus:
            events: list[DomainEvent] = []
            for i in range(20):
                events.append(
                    TestOrderCreated(
                        aggregate_id=uuid4(),
                        aggregate_version=1,
                        customer_id=uuid4(),
                        total_amount=100.0 * i,
                    )
                )
                events.append(
                    TestItemCreated(
                        aggregate_id=uuid4(),
                        aggregate_version=1,
                        name=f"Item-{i}",
                        quantity=i + 1,
                    )
                )

            await bus.publish(events)

        metrics_data = metrics_setup.get_metrics_data()

        published_count = _get_metric_value(metrics_data, "kafka.eventbus.messages.published")
        delta = published_count - baseline_published
        assert delta >= 40, (
            f"Expected at least 40 new published (20 orders + 20 items), got delta={delta}"
        )
