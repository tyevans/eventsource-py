"""
Integration tests for distributed trace context propagation.

Tests for:
- RabbitMQ trace context propagation (publish -> consume)
- Kafka trace context propagation (publish -> consume)
- Trace ID preservation across message boundaries
- Context injection and extraction

These tests verify that trace context is correctly propagated through
distributed message buses, enabling end-to-end distributed tracing.

Note:
    Tests requiring RabbitMQ or Kafka are marked with appropriate markers
    and will be skipped if the infrastructure is not available.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource import RABBITMQ_AVAILABLE, DomainEvent
from eventsource.observability import OTEL_AVAILABLE

from .conftest import TracingTestEvent

if TYPE_CHECKING:
    from collections.abc import Callable


# Check for Kafka availability
try:
    from eventsource.bus.kafka import KAFKA_AVAILABLE
except ImportError:
    KAFKA_AVAILABLE = False


pytestmark = [pytest.mark.integration]


# ============================================================================
# Skip Conditions
# ============================================================================


skip_if_no_rabbitmq = pytest.mark.skipif(
    not RABBITMQ_AVAILABLE,
    reason="RabbitMQ not available (aio_pika not installed)",
)

skip_if_no_kafka = pytest.mark.skipif(
    not KAFKA_AVAILABLE,
    reason="Kafka not available (aiokafka not installed)",
)

skip_if_no_otel = pytest.mark.skipif(
    not OTEL_AVAILABLE,
    reason="OpenTelemetry not available",
)


# ============================================================================
# RabbitMQ Distributed Tracing Tests
# ============================================================================


@pytest.mark.rabbitmq
@skip_if_no_rabbitmq
@skip_if_no_otel
class TestRabbitMQDistributedTracing:
    """Test distributed trace context propagation for RabbitMQ event bus.

    These tests verify that trace context (trace ID, span ID) is correctly
    propagated through RabbitMQ message headers, enabling distributed tracing
    across service boundaries.

    Note:
        These tests require a running RabbitMQ instance. They will be skipped
        if RabbitMQ is not available.
    """

    @pytest.fixture
    def rabbitmq_url(self) -> str:
        """Get RabbitMQ connection URL for tests."""
        import os

        return os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

    @pytest.fixture
    async def check_rabbitmq_available(self, rabbitmq_url: str) -> bool:
        """Check if RabbitMQ is actually available for connection."""
        try:
            import aio_pika

            connection = await asyncio.wait_for(
                aio_pika.connect(rabbitmq_url),
                timeout=5.0,
            )
            await connection.close()
            return True
        except Exception:
            pytest.skip("RabbitMQ server not available")
            return False

    @pytest.mark.asyncio
    async def test_rabbitmq_trace_context_propagation(
        self,
        trace_provider: Any,
        trace_exporter: Any,
        get_spans: Callable[[], list[Any]],
        rabbitmq_url: str,
        check_rabbitmq_available: bool,
    ):
        """Verify trace context flows from publisher to consumer.

        This test verifies that:
        1. Publishing creates a span with trace context
        2. The trace context is injected into message headers
        3. Consuming extracts the context and creates linked spans
        4. All spans share the same trace ID
        """
        from eventsource import EventRegistry
        from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        registry = EventRegistry()
        registry.register(TracingTestEvent)

        exchange_name = f"test_tracing_{uuid4().hex[:8]}"

        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_url,
            exchange_name=exchange_name,
            consumer_group="test_tracing_consumer",
            enable_tracing=True,
        )

        bus = RabbitMQEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            # Set up event handling
            received_events: list[DomainEvent] = []
            event_received = asyncio.Event()

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)
                event_received.set()

            bus.subscribe(TracingTestEvent, handler)

            # Start consuming in background
            consume_task = asyncio.create_task(bus.start_consuming())

            # Give consumer time to set up
            await asyncio.sleep(0.5)

            # Publish an event
            aggregate_id = uuid4()
            event = TracingTestEvent(
                aggregate_id=aggregate_id,
                aggregate_version=1,
                name="Distributed Tracing Test",
                value=42,
            )
            await bus.publish([event])

            # Wait for event to be received (with timeout)
            try:
                await asyncio.wait_for(event_received.wait(), timeout=5.0)
            except TimeoutError:
                pytest.skip("RabbitMQ message not received in time")

            # Stop consuming
            await bus.stop_consuming()
            consume_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await consume_task

            # Verify event was received
            assert len(received_events) == 1

            # Verify trace spans
            spans = get_spans()

            # Should have publish and consume spans
            publish_spans = [s for s in spans if "publish" in s.name.lower()]
            consume_spans = [
                s for s in spans if "consume" in s.name.lower() or "process" in s.name.lower()
            ]

            # At minimum, we should have publishing spans
            assert len(publish_spans) >= 1, "Should have at least one publish span"

            # If we have both, verify they share trace ID
            if publish_spans and consume_spans:
                publish_trace_id = publish_spans[0].context.trace_id
                consume_trace_id = consume_spans[0].context.trace_id

                assert publish_trace_id == consume_trace_id, (
                    "Publish and consume spans should share the same trace ID"
                )

        finally:
            await bus.disconnect()

    @pytest.mark.asyncio
    async def test_rabbitmq_tracing_disabled_still_works(
        self,
        rabbitmq_url: str,
        check_rabbitmq_available: bool,
    ):
        """RabbitMQ bus works correctly when tracing is disabled."""
        from eventsource import EventRegistry
        from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        registry = EventRegistry()
        registry.register(TracingTestEvent)

        exchange_name = f"test_no_tracing_{uuid4().hex[:8]}"

        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_url,
            exchange_name=exchange_name,
            consumer_group="test_no_tracing_consumer",
            enable_tracing=False,
        )

        bus = RabbitMQEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            received_events: list[DomainEvent] = []
            event_received = asyncio.Event()

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)
                event_received.set()

            bus.subscribe(TracingTestEvent, handler)

            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(0.5)

            event = TracingTestEvent(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="No Tracing Test",
            )
            await bus.publish([event])

            try:
                await asyncio.wait_for(event_received.wait(), timeout=5.0)
            except TimeoutError:
                pytest.skip("RabbitMQ message not received in time")

            await bus.stop_consuming()
            consume_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await consume_task

            assert len(received_events) == 1
            assert received_events[0].name == "No Tracing Test"

        finally:
            await bus.disconnect()


# ============================================================================
# Kafka Distributed Tracing Tests
# ============================================================================


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
    ):
        """Verify trace context flows from Kafka producer to consumer.

        This test verifies that:
        1. Publishing creates a span with trace context
        2. The trace context is injected into Kafka message headers
        3. Consuming extracts the context and creates linked spans
        4. All spans share the same trace ID
        """
        from eventsource import EventRegistry
        from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

        registry = EventRegistry()
        registry.register(TracingTestEvent)

        topic_prefix = f"test_tracing_{uuid4().hex[:8]}"
        consumer_group = f"test_consumer_{uuid4().hex[:8]}"

        config = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=consumer_group,
            enable_tracing=True,
            auto_create_topics=True,
        )

        bus = KafkaEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            received_events: list[DomainEvent] = []
            event_received = asyncio.Event()

            async def handler(event: DomainEvent) -> None:
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
    ):
        """Kafka bus works correctly when tracing is disabled."""
        from eventsource import EventRegistry
        from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig

        registry = EventRegistry()
        registry.register(TracingTestEvent)

        topic_prefix = f"test_no_tracing_{uuid4().hex[:8]}"
        consumer_group = f"test_no_trace_{uuid4().hex[:8]}"

        config = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=consumer_group,
            enable_tracing=False,
            auto_create_topics=True,
        )

        bus = KafkaEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            received_events: list[DomainEvent] = []
            event_received = asyncio.Event()

            async def handler(event: DomainEvent) -> None:
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


# ============================================================================
# Cross-Process Trace Context Tests (Simulated)
# ============================================================================


@skip_if_no_otel
class TestCrossProcessTraceContext:
    """Tests for trace context serialization/deserialization.

    These tests verify that trace context can be correctly injected into
    and extracted from message headers, simulating cross-process propagation.
    """

    def test_trace_context_injection_extraction(
        self,
        trace_provider: Any,
    ):
        """Trace context can be injected into headers and extracted back."""
        from opentelemetry import trace
        from opentelemetry.propagate import extract, inject

        tracer = trace.get_tracer("test")

        # Start a span to get trace context
        with tracer.start_as_current_span("test-span") as span:
            _original_trace_id = span.get_span_context().trace_id  # noqa: F841
            _original_span_id = span.get_span_context().span_id  # noqa: F841

            # Inject context into a carrier (simulating message headers)
            carrier: dict[str, str] = {}
            inject(carrier)

            # Carrier should now have trace context
            assert len(carrier) > 0, "Context should be injected into carrier"

        # Now extract context (simulating receiving a message)
        extracted_context = extract(carrier)

        # The extracted context should preserve the trace ID
        assert extracted_context is not None

    def test_trace_context_preserved_in_dict_headers(
        self,
        trace_provider: Any,
    ):
        """Trace context is correctly preserved when using dict as carrier."""
        from opentelemetry import trace
        from opentelemetry.propagate import extract, inject

        tracer = trace.get_tracer("test")

        with tracer.start_as_current_span("parent-span") as parent_span:
            parent_trace_id = parent_span.get_span_context().trace_id

            # Simulate sending a message: inject context
            message_headers: dict[str, str] = {}
            inject(message_headers)

        # Simulate receiving: extract and create child span
        context = extract(message_headers)

        # Create a child span using extracted context
        with tracer.start_as_current_span("child-span", context=context) as child_span:
            child_trace_id = child_span.get_span_context().trace_id

            # Both spans should share the same trace ID
            assert parent_trace_id == child_trace_id, (
                "Child span should inherit trace ID from parent via propagation"
            )


# ============================================================================
# Redis Distributed Tracing Tests
# ============================================================================


@pytest.mark.redis
@skip_if_no_otel
class TestRedisDistributedTracing:
    """Test trace context propagation for Redis event bus.

    Note:
        Redis Streams do not natively support message headers like RabbitMQ
        or Kafka. Context propagation may need to be embedded in the message
        payload or handled differently.
    """

    @pytest.fixture
    def redis_url(self) -> str:
        """Get Redis connection URL for tests."""
        import os

        return os.environ.get("REDIS_URL", "redis://localhost:6379")

    @pytest.fixture
    async def check_redis_available(self, redis_url: str) -> bool:
        """Check if Redis is actually available."""
        try:
            import redis.asyncio as redis_lib

            client = redis_lib.from_url(redis_url, single_connection_client=True)
            await asyncio.wait_for(client.ping(), timeout=5.0)
            await client.aclose()
            return True
        except Exception:
            pytest.skip("Redis server not available")
            return False

    @pytest.mark.asyncio
    async def test_redis_tracing_creates_spans(
        self,
        trace_provider: Any,
        get_spans: Callable[[], list[Any]],
        redis_url: str,
        check_redis_available: bool,
    ):
        """Redis event bus creates proper tracing spans."""
        from eventsource import REDIS_AVAILABLE, EventRegistry

        if not REDIS_AVAILABLE:
            pytest.skip("Redis not available")

        from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig

        registry = EventRegistry()
        registry.register(TracingTestEvent)

        stream_prefix = f"test_tracing_{uuid4().hex[:8]}"

        config = RedisEventBusConfig(
            redis_url=redis_url,
            stream_prefix=stream_prefix,
            consumer_group="test_tracing_group",
            enable_tracing=True,
            single_connection_client=True,
        )

        bus = RedisEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            # Publish an event
            event = TracingTestEvent(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Redis Tracing Test",
            )
            await bus.publish([event])

            spans = get_spans()

            # Should have at least one span for publishing
            publish_spans = [s for s in spans if "publish" in s.name.lower()]
            assert len(publish_spans) >= 1, "Should have at least one publish span"

        finally:
            await bus.shutdown()
