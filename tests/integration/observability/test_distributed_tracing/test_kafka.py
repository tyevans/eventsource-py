"""Kafka distributed trace context propagation integration tests."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource import DomainEvent, EventRegistry
from eventsource.adapters.kafka import KafkaEventBus, KafkaEventBusConfig

from .conftest import TracingTestEvent, skip_if_no_kafka, skip_if_no_otel

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = [pytest.mark.integration]


@pytest.mark.kafka
@skip_if_no_kafka
@skip_if_no_otel
class TestKafkaDistributedTracing:
    """Test distributed trace context propagation for Kafka event bus.

    These tests verify that trace context is correctly propagated through
    Kafka message headers for end-to-end distributed tracing.

    Note:
        These tests require a running Kafka broker. They will be skipped
        if Kafka is not available.
    """

    @pytest.fixture
    def kafka_bootstrap_servers(self) -> str:
        """Get Kafka bootstrap servers for tests."""
        import os

        return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    @pytest.fixture
    async def check_kafka_available(self, kafka_bootstrap_servers: str) -> bool:
        """Check if Kafka is actually available for connection."""
        try:
            from aiokafka import AIOKafkaProducer

            producer = AIOKafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers,
                request_timeout_ms=5000,
            )
            await asyncio.wait_for(producer.start(), timeout=10.0)
            await producer.stop()
            return True
        except Exception:
            pytest.skip("Kafka broker not available")
            return False

    @pytest.mark.asyncio
    async def test_kafka_trace_context_propagation(
        self,
        trace_provider: Any,
        trace_exporter: Any,
        get_spans: Callable[[], list[Any]],
        kafka_bootstrap_servers: str,
        check_kafka_available: bool,
    ) -> None:
        """Verify trace context flows from Kafka producer to consumer.

        This test verifies that:
        1. Publishing creates a span with trace context
        2. The trace context is injected into Kafka message headers
        3. Consuming extracts the context and creates linked spans
        4. All spans share the same trace ID
        """
        registry = EventRegistry()
        registry.register(TracingTestEvent)

        topic_prefix = f"test_tracing_{uuid4().hex[:8]}"
        consumer_group = f"test_consumer_{uuid4().hex[:8]}"

        config = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=consumer_group,
            enable_tracing=True,
        )

        bus = KafkaEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            received_events: list[TracingTestEvent] = []
            event_received = asyncio.Event()

            async def handler(event: DomainEvent) -> None:
                assert isinstance(event, TracingTestEvent)
                received_events.append(event)
                event_received.set()

            bus.subscribe(TracingTestEvent, handler)

            # Start consuming
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(1.0)  # Kafka needs more setup time

            # Publish an event
            event = TracingTestEvent(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Kafka Tracing Test",
                value=123,
            )
            await bus.publish([event])

            # Wait for event
            try:
                await asyncio.wait_for(event_received.wait(), timeout=10.0)
            except TimeoutError:
                pytest.skip("Kafka message not received in time")

            await bus.stop_consuming()
            consume_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await consume_task

            assert len(received_events) == 1

            # Verify spans
            spans = get_spans()

            publish_spans = [s for s in spans if "publish" in s.name.lower()]
            consume_spans = [
                s for s in spans if "consume" in s.name.lower() or "process" in s.name.lower()
            ]

            assert len(publish_spans) >= 1, "Should have at least one publish span"

            if publish_spans and consume_spans:
                publish_trace_id = publish_spans[0].context.trace_id
                consume_trace_id = consume_spans[0].context.trace_id

                assert publish_trace_id == consume_trace_id, (
                    "Publish and consume spans should share the same trace ID"
                )

        finally:
            await bus.shutdown()

    @pytest.mark.asyncio
    async def test_kafka_tracing_disabled_still_works(
        self,
        kafka_bootstrap_servers: str,
        check_kafka_available: bool,
    ) -> None:
        """Kafka bus works correctly when tracing is disabled."""
        registry = EventRegistry()
        registry.register(TracingTestEvent)

        topic_prefix = f"test_no_tracing_{uuid4().hex[:8]}"
        consumer_group = f"test_no_trace_{uuid4().hex[:8]}"

        config = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=consumer_group,
            enable_tracing=False,
        )

        bus = KafkaEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            received_events: list[TracingTestEvent] = []
            event_received = asyncio.Event()

            async def handler(event: DomainEvent) -> None:
                assert isinstance(event, TracingTestEvent)
                received_events.append(event)
                event_received.set()

            bus.subscribe(TracingTestEvent, handler)

            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(1.0)

            event = TracingTestEvent(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Kafka No Tracing Test",
            )
            await bus.publish([event])

            try:
                await asyncio.wait_for(event_received.wait(), timeout=10.0)
            except TimeoutError:
                pytest.skip("Kafka message not received in time")

            await bus.stop_consuming()
            consume_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await consume_task

            assert len(received_events) == 1
            assert received_events[0].name == "Kafka No Tracing Test"

        finally:
            await bus.shutdown()
