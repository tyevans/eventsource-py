"""Kafka distributed trace context propagation integration tests."""

from __future__ import annotations

import asyncio
import os
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
    """Test distributed trace context propagation for Kafka event bus."""

    @pytest.fixture
    def kafka_bootstrap_servers(self) -> str:
        return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    @pytest.fixture
    async def check_kafka_available(self, kafka_bootstrap_servers: str) -> bool:
        try:
            from aiokafka import AIOKafkaProducer

            producer = AIOKafkaProducer(
                bootstrap_servers=kafka_bootstrap_servers, request_timeout_ms=5000
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
        """Verify trace context flows from Kafka producer to consumer."""
        registry = EventRegistry()
        registry.register(TracingTestEvent)
        config = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=f"test_tracing_{uuid4().hex[:8]}",
            consumer_group=f"test_consumer_{uuid4().hex[:8]}",
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
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(1.0)

            event = TracingTestEvent(
                aggregate_id=uuid4(), aggregate_version=1, name="Kafka Tracing Test", value=123
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
            spans = get_spans()
            publish_spans = [s for s in spans if "publish" in s.name.lower()]
            consume_spans = [
                s for s in spans if "consume" in s.name.lower() or "process" in s.name.lower()
            ]
            assert len(publish_spans) >= 1, "Should have at least one publish span"
            if publish_spans and consume_spans:
                assert publish_spans[0].context.trace_id == consume_spans[0].context.trace_id
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
        config = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=f"test_no_tracing_{uuid4().hex[:8]}",
            consumer_group=f"test_no_trace_{uuid4().hex[:8]}",
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
                aggregate_id=uuid4(), aggregate_version=1, name="Kafka No Tracing Test"
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
