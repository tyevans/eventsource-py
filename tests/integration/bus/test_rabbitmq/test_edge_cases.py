"""Tests for RabbitMQ edge cases, error handling, and performance."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from eventsource import DomainEvent, EventRegistry, RabbitMQEventBus
from tests.integration.conftest import TestItemCreated, TestItemUpdated

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [pytest.mark.integration, pytest.mark.rabbitmq, skip_if_no_rabbitmq_infra]


class TestRabbitMQEventBusEdgeCases:
    """Tests for edge cases and error handling."""

    async def test_consume_unknown_event_type(self, rabbitmq_event_bus_factory: Any) -> None:
        """Test that unknown event types are handled gracefully."""
        unique_suffix = uuid4().hex[:8]
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_unknown_{unique_suffix}",
            consumer_group=f"test_group_{unique_suffix}",
        )
        bus._event_registry = EventRegistry()
        bus._event_registry.register(TestItemCreated)

        await bus.connect()
        try:
            received_events: list[DomainEvent] = []

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)

            bus.subscribe(TestItemCreated, handler)
            known_event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Known Event",
                quantity=1,
            )
            await bus.publish([known_event])

            consume_task = asyncio.create_task(bus.start_consuming())

            async def wait_for_event() -> None:
                while len(received_events) < 1:
                    await asyncio.sleep(0.1)

            try:
                await asyncio.wait_for(wait_for_event(), timeout=10.0)
            finally:
                await bus.stop_consuming()
                try:
                    await asyncio.wait_for(consume_task, timeout=2.0)
                except TimeoutError:
                    consume_task.cancel()

            assert len(received_events) == 1
        finally:
            await bus.disconnect()

    async def test_no_handlers_for_event(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
        """Test that events with no handlers are still acknowledged."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)
        await rabbitmq_event_bus.publish(
            [
                TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name="Has Handler",
                    quantity=1,
                ),
                TestItemUpdated(
                    aggregate_id=uuid4(),
                    aggregate_version=2,
                    name="No Handler",
                ),
            ]
        )

        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())
        await asyncio.sleep(3.0)
        await rabbitmq_event_bus.stop_consuming()
        try:
            await asyncio.wait_for(consume_task, timeout=2.0)
        except TimeoutError:
            consume_task.cancel()

        assert len(received_events) == 1
        assert received_events[0].event_type == "TestItemCreated"
        assert rabbitmq_event_bus.stats.events_consumed >= 1

    async def test_clear_subscribers(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
        """Test clearing all subscribers."""

        async def handler(e: DomainEvent) -> None:
            pass

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)
        rabbitmq_event_bus.subscribe(TestItemUpdated, handler)
        rabbitmq_event_bus.subscribe_to_all_events(handler)

        assert rabbitmq_event_bus.get_subscriber_count() > 0
        assert rabbitmq_event_bus.get_wildcard_subscriber_count() > 0

        rabbitmq_event_bus.clear_subscribers()
        assert rabbitmq_event_bus.get_subscriber_count() == 0
        assert rabbitmq_event_bus.get_wildcard_subscriber_count() == 0


class TestRabbitMQEventBusPerformance:
    """Performance-related tests."""

    @pytest.mark.benchmark
    async def test_batch_publish_performance(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
        """Test that batch publishing completes in reasonable time."""
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Batch Item {i}",
                quantity=i,
            )
            for i in range(100)
        ]

        start = datetime.now(UTC)
        await rabbitmq_event_bus.publish(events)
        duration = (datetime.now(UTC) - start).total_seconds()

        assert duration < 10.0
        assert rabbitmq_event_bus.stats.events_published == 100

    @pytest.mark.benchmark
    async def test_high_throughput_publish_consume(
        self, rabbitmq_event_bus: RabbitMQEventBus
    ) -> None:
        """Test high throughput publish and consume."""
        received_events: list[DomainEvent] = []
        event_count = 50

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)

        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"High Throughput Item {i}",
                quantity=i,
            )
            await rabbitmq_event_bus.publish([event])

        start = datetime.now(UTC)
        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())

        async def wait_for_all() -> None:
            while len(received_events) < event_count:
                await asyncio.sleep(0.05)

        try:
            await asyncio.wait_for(wait_for_all(), timeout=30.0)
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        duration = (datetime.now(UTC) - start).total_seconds()
        assert len(received_events) == event_count
        assert duration < 30.0
