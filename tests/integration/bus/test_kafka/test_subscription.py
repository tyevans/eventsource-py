"""Integration tests for Kafka event subscriptions."""

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


class TestKafkaEventBusSubscription:
    """Tests for event subscription."""

    async def test_subscribe_and_receive(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test subscribing to events and receiving them."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

        # Publish an event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Test Item",
            quantity=10,
        )
        await kafka_event_bus.publish([event])

        # Start consuming in background
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        # Wait for event to be processed
        try:
            await asyncio.wait_for(
                asyncio.create_task(self._wait_for_events(received_events, 1)),
                timeout=15.0,
            )
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(received_events) == 1
        assert received_events[0].event_type == "TestItemCreated"
        assert kafka_event_bus.stats.events_consumed >= 1
        assert kafka_event_bus.stats.events_processed_success >= 1

    async def _wait_for_events(
        self,
        events: list[DomainEvent],
        count: int,
    ) -> None:
        """Helper to wait for events to be received."""
        while len(events) < count:
            await asyncio.sleep(0.1)

    async def test_subscribe_multiple_handlers(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test multiple handlers for same event type."""
        handler1_events: list[DomainEvent] = []
        handler2_events: list[DomainEvent] = []

        async def handler1(event: DomainEvent) -> None:
            handler1_events.append(event)

        async def handler2(event: DomainEvent) -> None:
            handler2_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler1)
        kafka_event_bus.subscribe(TestItemCreated, handler2)

        # Publish an event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Multi Handler Test",
            quantity=5,
        )
        await kafka_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                asyncio.create_task(self._wait_for_events(handler1_events, 1)),
                timeout=15.0,
            )
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        # Both handlers should have received the event
        assert len(handler1_events) == 1
        assert len(handler2_events) == 1

    async def test_subscribe_to_all_events(
        self,
        kafka_event_bus: KafkaEventBus,
        sample_customer_id: Any,
    ) -> None:
        """Test wildcard subscription to all events."""
        all_events: list[DomainEvent] = []

        async def wildcard_handler(event: DomainEvent) -> None:
            all_events.append(event)

        kafka_event_bus.subscribe_to_all_events(wildcard_handler)

        # Publish different event types
        events = [
            TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Item",
                quantity=1,
            ),
            TestOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                customer_id=sample_customer_id,
                total_amount=50.0,
            ),
        ]
        await kafka_event_bus.publish(events)

        # Start consuming
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        try:
            await asyncio.wait_for(
                asyncio.create_task(self._wait_for_events(all_events, 2)),
                timeout=15.0,
            )
        finally:
            await kafka_event_bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=5.0)
            except TimeoutError:
                consume_task.cancel()

        assert len(all_events) == 2

    async def test_unsubscribe(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test unsubscribing a handler."""
        events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

        # Unsubscribe
        result = kafka_event_bus.unsubscribe(TestItemCreated, handler)
        assert result is True

        # Second unsubscribe should return False
        result = kafka_event_bus.unsubscribe(TestItemCreated, handler)
        assert result is False
