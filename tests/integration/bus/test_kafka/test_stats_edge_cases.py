"""Integration tests for Kafka statistics, topic info, and edge cases."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from eventsource import KAFKA_AVAILABLE, DomainEvent, EventRegistry, KafkaEventBus
from tests.integration.conftest import (
    TestItemCreated,
    TestItemUpdated,
    TestOrderCreated,
)

from .conftest import skip_if_no_kafka_infra

if not KAFKA_AVAILABLE:
    pytest.skip("Kafka (aiokafka) not available", allow_module_level=True)

pytestmark = [pytest.mark.integration, pytest.mark.kafka, skip_if_no_kafka_infra]


async def _consume_until(
    bus: KafkaEventBus, received: list[DomainEvent], count: int, timeout: float = 15.0
) -> None:
    consume_task = asyncio.create_task(bus.start_consuming())

    async def wait_for_events() -> None:
        while len(received) < count:
            await asyncio.sleep(0.1)

    try:
        await asyncio.wait_for(wait_for_events(), timeout=timeout)
    finally:
        await bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=5.0)
        except TimeoutError:
            consume_task.cancel()


class TestKafkaEventBusStats:
    async def test_stats_tracking_publish(self, kafka_event_bus: KafkaEventBus) -> None:
        assert kafka_event_bus.stats.events_published == 0
        for i in range(5):
            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name=f"Stats Item {i}", quantity=i
            )
            await kafka_event_bus.publish([event])

        assert kafka_event_bus.stats.events_published == 5
        assert kafka_event_bus.stats.last_publish_at is not None

    async def test_stats_tracking_consume(self, kafka_event_bus: KafkaEventBus) -> None:
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)
        event_count = 3
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Stats Consume Item {i}",
                quantity=i,
            )
            await kafka_event_bus.publish([event])

        await _consume_until(kafka_event_bus, received_events, event_count, timeout=15.0)
        assert kafka_event_bus.stats.events_consumed == event_count
        assert kafka_event_bus.stats.events_processed_success == event_count

    async def test_subscriber_counts(self, kafka_event_bus: KafkaEventBus) -> None:
        kafka_event_bus.clear_subscribers()
        assert kafka_event_bus.get_subscriber_count() == 0
        assert kafka_event_bus.get_wildcard_subscriber_count() == 0

        async def handler1(e: DomainEvent) -> None:
            pass

        async def handler2(e: DomainEvent) -> None:
            pass

        kafka_event_bus.subscribe(TestItemCreated, handler1)
        kafka_event_bus.subscribe(TestItemCreated, handler2)
        kafka_event_bus.subscribe(TestOrderCreated, handler1)

        assert kafka_event_bus.get_subscriber_count() == 3
        assert kafka_event_bus.get_subscriber_count(TestItemCreated) == 2
        assert kafka_event_bus.get_subscriber_count(TestOrderCreated) == 1

        kafka_event_bus.subscribe_to_all_events(handler1)
        assert kafka_event_bus.get_wildcard_subscriber_count() == 1

        kafka_event_bus.clear_subscribers()
        assert kafka_event_bus.get_subscriber_count() == 0
        assert kafka_event_bus.get_wildcard_subscriber_count() == 0

    async def test_get_stats_dict(self, kafka_event_bus: KafkaEventBus) -> None:
        stats_dict = kafka_event_bus.get_stats_dict()
        assert "events_published" in stats_dict
        assert "events_consumed" in stats_dict
        assert "events_processed_success" in stats_dict
        assert "events_processed_failed" in stats_dict
        assert "messages_sent_to_dlq" in stats_dict
        assert "handler_errors" in stats_dict
        assert "connected_at" in stats_dict


class TestKafkaEventBusTopicInfo:
    async def test_get_topic_info(self, kafka_event_bus: KafkaEventBus) -> None:
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name=f"Info Item {i}", quantity=i
            )
            for i in range(3)
        ]
        await kafka_event_bus.publish(events)
        info = await kafka_event_bus.get_topic_info()
        assert info["connected"] is True
        assert "topic" in info
        assert "partitions" in info
        assert "consumer_group" in info


class TestKafkaEventBusEdgeCases:
    async def test_consume_unknown_event_type(self, kafka_event_bus_factory: Any) -> None:
        bus = kafka_event_bus_factory()
        bus._event_registry = EventRegistry()
        bus._event_registry.register(TestItemCreated)
        await bus.connect()

        try:
            received_events: list[DomainEvent] = []

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)

            bus.subscribe(TestItemCreated, handler)
            known_event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name="Known Event", quantity=1
            )
            await bus.publish([known_event])
            await _consume_until(bus, received_events, 1, timeout=15.0)
            assert len(received_events) == 1
            assert getattr(received_events[0], "name", None) == "Known Event"
        finally:
            await bus.disconnect()

    async def test_handler_with_slow_processing(self, kafka_event_bus: KafkaEventBus) -> None:
        received_events: list[DomainEvent] = []

        async def slow_handler(event: DomainEvent) -> None:
            await asyncio.sleep(0.5)
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, slow_handler)
        for i in range(3):
            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name=f"Slow Item {i}", quantity=i
            )
            await kafka_event_bus.publish([event])

        await _consume_until(kafka_event_bus, received_events, 3, timeout=30.0)
        assert len(received_events) == 3

    async def test_clear_subscribers(self, kafka_event_bus: KafkaEventBus) -> None:
        kafka_event_bus.subscribe(TestItemCreated, lambda e: None)
        kafka_event_bus.subscribe(TestItemUpdated, lambda e: None)
        kafka_event_bus.subscribe_to_all_events(lambda e: None)

        assert kafka_event_bus.get_subscriber_count() > 0
        assert kafka_event_bus.get_wildcard_subscriber_count() > 0

        kafka_event_bus.clear_subscribers()
        assert kafka_event_bus.get_subscriber_count() == 0
        assert kafka_event_bus.get_wildcard_subscriber_count() == 0
