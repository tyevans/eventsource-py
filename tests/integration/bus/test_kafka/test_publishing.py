"""Integration tests for Kafka publishing and round-trip consume."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from eventsource import KAFKA_AVAILABLE, DomainEvent, KafkaEventBus
from tests.integration.conftest import (
    TestItemCreated,
    TestOrderCreated,
)

from .conftest import skip_if_no_kafka_infra

if not KAFKA_AVAILABLE:
    pytest.skip("Kafka (aiokafka) not available", allow_module_level=True)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.kafka,
    skip_if_no_kafka_infra,
]


class TestKafkaEventBusPublishing:
    """Tests for event publishing."""

    async def test_publish_single_event(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test publishing a single event."""
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )

        await kafka_event_bus.publish([event])

        # Check stats
        assert kafka_event_bus.stats.events_published == 1
        assert kafka_event_bus.stats.last_publish_at is not None

    async def test_publish_multiple_events(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test publishing multiple events."""
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Item {i}",
                quantity=i,
            )
            for i in range(5)
        ]

        await kafka_event_bus.publish(events)

        # Check stats
        assert kafka_event_bus.stats.events_published == 5

    async def test_publish_empty_list(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that publishing empty list does nothing."""
        initial_published = kafka_event_bus.stats.events_published

        await kafka_event_bus.publish([])

        assert kafka_event_bus.stats.events_published == initial_published

    async def test_publish_different_event_types(
        self,
        kafka_event_bus: KafkaEventBus,
        sample_customer_id: Any,
    ) -> None:
        """Test publishing different event types."""
        events = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Test Item",
                quantity=10,
            ),
            TestOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                customer_id=sample_customer_id,
                total_amount=100.0,
            ),
        ]

        await kafka_event_bus.publish(events)

        assert kafka_event_bus.stats.events_published == 2


class TestKafkaPublishConsumeRoundTrip:
    """Tests for publish-consume round trip with multiple events."""

    async def test_round_trip_multiple_events(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test publishing multiple events and consuming them all."""
        received_events: list[DomainEvent] = []
        event_count = 10

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

        # Publish multiple events
        published_ids = []
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Item {i}",
                quantity=i + 1,
            )
            published_ids.append(event.event_id)
            await kafka_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                self._wait_for_events(received_events, event_count),
                timeout=30.0,
            )
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        # Verify all events were received
        assert len(received_events) == event_count
        received_ids = {e.event_id for e in received_events}
        assert received_ids == set(published_ids)

        # Verify stats
        assert kafka_event_bus.stats.events_published == event_count
        assert kafka_event_bus.stats.events_consumed == event_count
        assert kafka_event_bus.stats.events_processed_success == event_count

    async def _wait_for_events(
        self,
        events: list[DomainEvent],
        count: int,
    ) -> None:
        """Helper to wait for events to be received."""
        while len(events) < count:
            await asyncio.sleep(0.1)

    async def test_round_trip_preserves_event_data(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that event data is preserved through publish-consume cycle."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

        # Create event with specific data
        original_event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=42,
            name="Preserved Item",
            quantity=99,
        )
        await kafka_event_bus.publish([original_event])

        # Consume
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                self._wait_for_events(received_events, 1),
                timeout=15.0,
            )
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        # Verify data preserved
        assert len(received_events) == 1
        received = received_events[0]
        assert isinstance(received, TestItemCreated)
        assert received.event_id == original_event.event_id
        assert received.aggregate_id == original_event.aggregate_id
        assert received.aggregate_version == original_event.aggregate_version
        assert received.name == original_event.name
        assert received.quantity == original_event.quantity

    async def test_round_trip_batch_publish(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test batch publishing and consuming."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

        # Batch publish
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=i + 1,
                name=f"Batch Item {i}",
                quantity=i * 10,
            )
            for i in range(20)
        ]
        await kafka_event_bus.publish(events)

        # Consume
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                self._wait_for_events(received_events, 20),
                timeout=30.0,
            )
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(received_events) == 20
