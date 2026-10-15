"""Tests for event publishing and publish-consume round trips."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from eventsource import DomainEvent, RabbitMQEventBus
from tests.integration.conftest import TestItemCreated, TestOrderCreated

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [pytest.mark.integration, pytest.mark.rabbitmq, skip_if_no_rabbitmq_infra]


class TestRabbitMQEventBusPublishing:
    """Tests for event publishing."""

    async def test_publish_single_event(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
        """Test publishing a single event."""
        event = TestItemCreated(
            aggregate_id=uuid4(), aggregate_version=1, name="Test Item", quantity=10
        )
        await rabbitmq_event_bus.publish([event])
        assert rabbitmq_event_bus.stats.events_published == 1
        assert rabbitmq_event_bus.stats.publish_confirms == 1

    async def test_publish_multiple_events(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
        """Test publishing multiple events."""
        events: list[DomainEvent] = [
            TestItemCreated(aggregate_id=uuid4(), aggregate_version=1, name=f"Item {i}", quantity=i)
            for i in range(5)
        ]
        await rabbitmq_event_bus.publish(events)
        assert rabbitmq_event_bus.stats.events_published == 5
        assert rabbitmq_event_bus.stats.publish_confirms == 5

    async def test_publish_empty_list(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
        """Test that publishing empty list does nothing."""
        initial_published = rabbitmq_event_bus.stats.events_published
        await rabbitmq_event_bus.publish([])
        assert rabbitmq_event_bus.stats.events_published == initial_published

    async def test_publish_different_event_types(
        self, rabbitmq_event_bus: RabbitMQEventBus, sample_customer_id: Any
    ) -> None:
        """Test publishing different event types."""
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
        await rabbitmq_event_bus.publish(events)
        assert rabbitmq_event_bus.stats.events_published == 2

    async def test_publish_with_auto_connect(self, rabbitmq_event_bus_factory: Any) -> None:
        """Test that publishing auto-connects if not connected."""
        bus = rabbitmq_event_bus_factory(exchange_name=f"test_auto_connect_{uuid4().hex[:8]}")
        assert not bus.is_connected
        event = TestItemCreated(
            aggregate_id=uuid4(), aggregate_version=1, name="Auto Connect Test", quantity=1
        )
        try:
            await bus.publish([event])
            assert bus.is_connected
            assert bus.stats.events_published == 1
        finally:
            await bus.disconnect()


class TestRabbitMQPublishConsumeRoundTrip:
    """Tests for publish-consume round trip with multiple events."""

    async def test_round_trip_multiple_events(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
        """Test publishing multiple events and consuming them all."""
        received_events: list[DomainEvent] = []
        event_count = 10

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)
        published_ids = []
        for i in range(event_count):
            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name=f"Item {i}", quantity=i + 1
            )
            published_ids.append(event.event_id)
            await rabbitmq_event_bus.publish([event])

        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())
        try:
            await asyncio.wait_for(
                self._wait_for_events(received_events, event_count), timeout=30.0
            )
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(received_events) == event_count
        assert {e.event_id for e in received_events} == set(published_ids)
        assert rabbitmq_event_bus.stats.events_published == event_count
        assert rabbitmq_event_bus.stats.events_consumed == event_count
        assert rabbitmq_event_bus.stats.events_processed_success == event_count

    async def _wait_for_events(self, events: list[DomainEvent], count: int) -> None:
        """Helper to wait for events to be received."""
        while len(events) < count:
            await asyncio.sleep(0.1)

    async def test_round_trip_preserves_event_data(
        self, rabbitmq_event_bus: RabbitMQEventBus
    ) -> None:
        """Test that event data is preserved through publish-consume cycle."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)
        original_event = TestItemCreated(
            aggregate_id=uuid4(), aggregate_version=42, name="Preserved Item", quantity=99
        )
        await rabbitmq_event_bus.publish([original_event])

        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())
        try:
            await asyncio.wait_for(self._wait_for_events(received_events, 1), timeout=10.0)
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(received_events) == 1
        received = received_events[0]
        assert isinstance(received, TestItemCreated)
        assert received.event_id == original_event.event_id
        assert received.aggregate_id == original_event.aggregate_id
        assert received.aggregate_version == original_event.aggregate_version
        assert received.name == original_event.name
        assert received.quantity == original_event.quantity

    async def test_round_trip_batch_publish(self, rabbitmq_event_bus: RabbitMQEventBus) -> None:
        """Test batch publishing and consuming."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        rabbitmq_event_bus.subscribe(TestItemCreated, handler)
        events: list[DomainEvent] = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=i + 1,
                name=f"Batch Item {i}",
                quantity=i * 10,
            )
            for i in range(20)
        ]
        await rabbitmq_event_bus.publish(events)

        consume_task = asyncio.create_task(rabbitmq_event_bus.start_consuming())
        try:
            await asyncio.wait_for(self._wait_for_events(received_events, 20), timeout=30.0)
        finally:
            await rabbitmq_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(received_events) == 20
