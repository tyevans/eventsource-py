"""Integration tests for Kafka publishing and round-trip consume."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from eventsource import KAFKA_AVAILABLE, DomainEvent, KafkaEventBus
from tests.integration.conftest import TestItemCreated, TestOrderCreated

from .conftest import skip_if_no_kafka_infra

if not KAFKA_AVAILABLE:
    pytest.skip("Kafka (aiokafka) not available", allow_module_level=True)

pytestmark = [pytest.mark.integration, pytest.mark.kafka, skip_if_no_kafka_infra]


class TestKafkaEventBusPublishing:
    async def test_publish_single_event(self, kafka_event_bus: KafkaEventBus) -> None:
        event = TestItemCreated(
            aggregate_id=uuid4(), aggregate_version=1, name="Test Item", quantity=10
        )
        await kafka_event_bus.publish([event])
        assert kafka_event_bus.stats.events_published == 1
        assert kafka_event_bus.stats.last_publish_at is not None

    async def test_publish_multiple_events(self, kafka_event_bus: KafkaEventBus) -> None:
        events: list[DomainEvent] = [
            TestItemCreated(aggregate_id=uuid4(), aggregate_version=1, name=f"Item {i}", quantity=i)
            for i in range(5)
        ]
        await kafka_event_bus.publish(events)
        assert kafka_event_bus.stats.events_published == 5

    async def test_publish_empty_list(self, kafka_event_bus: KafkaEventBus) -> None:
        initial_published = kafka_event_bus.stats.events_published
        await kafka_event_bus.publish([])
        assert kafka_event_bus.stats.events_published == initial_published

    async def test_publish_different_event_types(
        self, kafka_event_bus: KafkaEventBus, sample_customer_id: Any
    ) -> None:
        events = [
            TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name="Test Item", quantity=10
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
    async def _wait_for_events(self, events: list[DomainEvent], count: int) -> None:
        while len(events) < count:
            await asyncio.sleep(0.1)

    async def _consume_expected(
        self, bus: KafkaEventBus, received: list[DomainEvent], count: int, timeout: float
    ) -> None:
        consume_task = asyncio.create_task(bus.start_consuming())
        try:
            await asyncio.wait_for(self._wait_for_events(received, count), timeout=timeout)
        finally:
            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

    async def test_round_trip_multiple_events(self, kafka_event_bus: KafkaEventBus) -> None:
        received_events: list[DomainEvent] = []
        event_count = 10

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)
        published_ids = []
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name=f"Item {i}", quantity=i + 1
            )
            published_ids.append(event.event_id)
            await kafka_event_bus.publish([event])

        await self._consume_expected(kafka_event_bus, received_events, event_count, 30.0)

        assert len(received_events) == event_count
        assert {e.event_id for e in received_events} == set(published_ids)
        assert kafka_event_bus.stats.events_published == event_count
        assert kafka_event_bus.stats.events_consumed == event_count
        assert kafka_event_bus.stats.events_processed_success == event_count

    async def test_round_trip_preserves_event_data(self, kafka_event_bus: KafkaEventBus) -> None:
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)
        original_event = TestItemCreated(
            aggregate_id=uuid4(), aggregate_version=42, name="Preserved Item", quantity=99
        )
        await kafka_event_bus.publish([original_event])
        await self._consume_expected(kafka_event_bus, received_events, 1, 15.0)

        assert len(received_events) == 1
        received = received_events[0]
        assert isinstance(received, TestItemCreated)
        assert received.event_id == original_event.event_id
        assert received.aggregate_id == original_event.aggregate_id
        assert received.aggregate_version == original_event.aggregate_version
        assert received.name == original_event.name
        assert received.quantity == original_event.quantity

    async def test_round_trip_batch_publish(self, kafka_event_bus: KafkaEventBus) -> None:
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)
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
        await self._consume_expected(kafka_event_bus, received_events, 20, 30.0)
        assert len(received_events) == 20
