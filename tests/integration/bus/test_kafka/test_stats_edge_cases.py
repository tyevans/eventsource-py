"""Integration tests for Kafka statistics, topic info, and edge cases."""

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
)
from tests.integration.conftest import (
    TestItemCreated,
    TestItemUpdated,
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


class TestKafkaEventBusStats:
    """Tests for statistics tracking accuracy."""

    async def test_stats_tracking_publish(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that publishing updates statistics correctly."""
        initial_stats = kafka_event_bus.stats

        # Verify initial state
        assert initial_stats.events_published == 0

        # Publish events
        for i in range(5):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Stats Item {i}",
                quantity=i,
            )
            await kafka_event_bus.publish([event])

        assert kafka_event_bus.stats.events_published == 5
        assert kafka_event_bus.stats.last_publish_at is not None

    async def test_stats_tracking_consume(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that consuming updates statistics correctly."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

        # Publish events
        event_count = 3
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Stats Consume Item {i}",
                quantity=i,
            )
            await kafka_event_bus.publish([event])

        # Consume events
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        async def wait_for_events() -> None:
            while len(received_events) < event_count:
                await asyncio.sleep(0.1)

        try:
            await asyncio.wait_for(wait_for_events(), timeout=15.0)
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        # Verify stats
        assert kafka_event_bus.stats.events_consumed == event_count
        assert kafka_event_bus.stats.events_processed_success == event_count

    async def test_subscriber_counts(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that subscriber counts are tracked correctly."""
        # Initially no subscribers
        kafka_event_bus.clear_subscribers()
        assert kafka_event_bus.get_subscriber_count() == 0
        assert kafka_event_bus.get_wildcard_subscriber_count() == 0

        # Add subscribers
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

        # Add wildcard subscriber
        kafka_event_bus.subscribe_to_all_events(handler1)
        assert kafka_event_bus.get_wildcard_subscriber_count() == 1

        # Clear all
        kafka_event_bus.clear_subscribers()
        assert kafka_event_bus.get_subscriber_count() == 0
        assert kafka_event_bus.get_wildcard_subscriber_count() == 0

    async def test_get_stats_dict(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test getting statistics as dictionary."""
        stats_dict = kafka_event_bus.get_stats_dict()

        assert "events_published" in stats_dict
        assert "events_consumed" in stats_dict
        assert "events_processed_success" in stats_dict
        assert "events_processed_failed" in stats_dict
        assert "messages_sent_to_dlq" in stats_dict
        assert "handler_errors" in stats_dict
        assert "connected_at" in stats_dict


class TestKafkaEventBusTopicInfo:
    """Tests for topic information retrieval."""

    async def test_get_topic_info(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test getting topic information."""
        # Publish some events first
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Info Item {i}",
                quantity=i,
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
    """Tests for edge cases and error handling."""

    async def test_consume_unknown_event_type(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test that unknown event types are handled gracefully."""
        bus = kafka_event_bus_factory()
        # Override registry to only include TestItemCreated
        bus._event_registry = EventRegistry()
        bus._event_registry.register(TestItemCreated)

        await bus.connect()

        try:
            received_events: list[DomainEvent] = []

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)

            bus.subscribe(TestItemCreated, handler)

            # Publish a known event
            known_event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Known Event",
                quantity=1,
            )
            await bus.publish([known_event])

            # Start consuming
            consume_task = asyncio.create_task(bus.start_consuming())

            # Wait for known event
            async def wait_for_event() -> None:
                while len(received_events) < 1:
                    await asyncio.sleep(0.1)

            try:
                await asyncio.wait_for(wait_for_event(), timeout=15.0)
            finally:
                await bus.stop_consuming()
                try:
                    await asyncio.wait_for(consume_task, timeout=5.0)
                except TimeoutError:
                    consume_task.cancel()

            # Should have received the known event
            assert len(received_events) == 1
            assert getattr(received_events[0], "name", None) == "Known Event"

        finally:
            await bus.disconnect()

    async def test_handler_with_slow_processing(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test handling of slow event processing."""
        received_events: list[DomainEvent] = []

        async def slow_handler(event: DomainEvent) -> None:
            await asyncio.sleep(0.5)  # Simulate slow processing
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, slow_handler)

        # Publish multiple events
        for i in range(3):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Slow Item {i}",
                quantity=i,
            )
            await kafka_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        # Wait for events to be processed (should take about 1.5 seconds)
        async def wait_for_events() -> None:
            while len(received_events) < 3:
                await asyncio.sleep(0.1)

        try:
            await asyncio.wait_for(wait_for_events(), timeout=30.0)
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(received_events) == 3

    async def test_clear_subscribers(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test clearing all subscribers."""
        # Add some subscribers
        kafka_event_bus.subscribe(TestItemCreated, lambda e: None)
        kafka_event_bus.subscribe(TestItemUpdated, lambda e: None)
        kafka_event_bus.subscribe_to_all_events(lambda e: None)

        assert kafka_event_bus.get_subscriber_count() > 0
        assert kafka_event_bus.get_wildcard_subscriber_count() > 0

        # Clear
        kafka_event_bus.clear_subscribers()

        assert kafka_event_bus.get_subscriber_count() == 0
        assert kafka_event_bus.get_wildcard_subscriber_count() == 0
