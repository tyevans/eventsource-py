"""Tests for batch publishing functionality, chunking, and ordering in RabbitMQ."""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource import (
    DomainEvent,
    EventRegistry,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from tests.integration.conftest import TestItemCreated

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [
    pytest.mark.integration,
    pytest.mark.rabbitmq,
    skip_if_no_rabbitmq_infra,
]


class TestAdvancedBatchPublishing:
    """Tests for batch publishing functionality."""

    @pytest.mark.benchmark
    async def test_large_batch_publish_performance(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test publishing a large batch of events completes efficiently."""
        batch_size = 500
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Large Batch Item {i}",
                quantity=i % 100,
            )
            for i in range(batch_size)
        ]

        start_time = datetime.now(UTC)
        result = await rabbitmq_event_bus.publish_batch(events)
        end_time = datetime.now(UTC)

        duration = (end_time - start_time).total_seconds()
        assert result["total"] == batch_size
        assert result["published"] == batch_size
        assert result["failed"] == 0
        assert result["chunks"] > 0
        assert duration < 30.0, f"Batch took {duration}s, expected < 30s"

        throughput = batch_size / duration
        print(f"Batch publish: {batch_size} events in {duration:.2f}s ({throughput:.1f} events/s)")

        assert rabbitmq_event_bus.stats.batch_publishes >= 1
        assert rabbitmq_event_bus.stats.batch_events_published >= batch_size

    async def test_batch_publish_with_order_preservation(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test batch publishing with order preservation enabled."""
        batch_size = 50
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Ordered Item {i:04d}",
                quantity=i,
            )
            for i in range(batch_size)
        ]

        result = await rabbitmq_event_bus.publish_batch(events, preserve_order=True)
        assert result["total"] == batch_size
        assert result["published"] == batch_size
        assert result["failed"] == 0
        assert rabbitmq_event_bus.stats.events_published >= batch_size

    async def test_batch_publish_concurrent_mode(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test batch publishing in concurrent mode (default)."""
        batch_size = 100
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Concurrent Item {i}",
                quantity=i,
            )
            for i in range(batch_size)
        ]

        start_time = datetime.now(UTC)
        result = await rabbitmq_event_bus.publish_batch(events, preserve_order=False)
        concurrent_duration = (datetime.now(UTC) - start_time).total_seconds()

        assert result["total"] == batch_size
        assert result["published"] == batch_size
        assert result["failed"] == 0
        print(f"Concurrent batch: {batch_size} events in {concurrent_duration:.2f}s")

    async def test_batch_publish_empty_batch(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test batch publishing with empty list returns immediately."""
        result = await rabbitmq_event_bus.publish_batch([])
        assert result["total"] == 0
        assert result["published"] == 0
        assert result["failed"] == 0
        assert result["chunks"] == 0

    async def test_batch_publish_single_event(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test batch publishing with a single event works correctly."""
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Single Batch Item",
            quantity=1,
        )
        result = await rabbitmq_event_bus.publish_batch([event])
        assert result["total"] == 1
        assert result["published"] == 1
        assert result["failed"] == 0

    async def test_batch_publish_verify_all_received(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that all batch-published events are received by consumer."""
        batch_size = 50
        received_events: list[DomainEvent] = []
        all_received = asyncio.Event()

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)
            if len(received_events) >= batch_size:
                all_received.set()

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())
        await asyncio.sleep(0.5)

        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Batch Receive Test {i}",
                quantity=i,
            )
            for i in range(batch_size)
        ]
        published_ids = {e.event_id for e in events}
        result = await rabbitmq_event_bus.publish_batch(events)

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(all_received.wait(), timeout=30.0)

        await rabbitmq_event_bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        assert len(received_events) == batch_size, (
            f"Expected {batch_size} events, received {len(received_events)}"
        )
        received_ids = {e.event_id for e in received_events}
        assert received_ids == published_ids, "Mismatch between published and received event IDs"
        assert result["published"] == batch_size

    async def test_batch_chunking_behavior(
        self,
        rabbitmq_connection_url: str,
    ) -> None:
        """Test that large batches are chunked according to config."""
        unique_suffix = uuid4().hex[:8]
        registry = EventRegistry()
        registry.register(TestItemCreated)

        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=f"test_chunking_{unique_suffix}",
            consumer_group=f"chunk_test_{unique_suffix}",
            publish_chunk_size=10,
            max_concurrent_publishes=5,
            durable=False,
            auto_delete=True,
        )
        bus = RabbitMQEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()
            batch_size = 55
            events: list[DomainEvent] = [
                TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"Chunk Test Item {i}",
                    quantity=i,
                )
                for i in range(batch_size)
            ]

            result = await bus.publish_batch(events)
            assert result["total"] == batch_size
            assert result["published"] == batch_size
            assert result["chunks"] == 6
        finally:
            await bus.disconnect()
