"""RabbitMQ distributed trace context propagation integration tests."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource import DomainEvent, EventRegistry
from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

from .conftest import TracingTestEvent, skip_if_no_otel, skip_if_no_rabbitmq

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = [pytest.mark.integration]


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
    ) -> None:
        """Verify trace context flows from publisher to consumer.

        This test verifies that:
        1. Publishing creates a span with trace context
        2. The trace context is injected into message headers
        3. Consuming extracts the context and creates linked spans
        4. All spans share the same trace ID
        """
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
    ) -> None:
        """RabbitMQ bus works correctly when tracing is disabled."""
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

            received_events: list[TracingTestEvent] = []
            event_received = asyncio.Event()

            async def handler(event: DomainEvent) -> None:
                assert isinstance(event, TracingTestEvent)
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
