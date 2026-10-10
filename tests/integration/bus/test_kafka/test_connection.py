"""Integration tests for Kafka connection lifecycle and graceful shutdown."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from eventsource import KAFKA_AVAILABLE, DomainEvent, KafkaEventBus
from tests.integration.conftest import TestItemCreated

from .conftest import skip_if_no_kafka_infra

if not KAFKA_AVAILABLE:
    pytest.skip("Kafka (aiokafka) not available", allow_module_level=True)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.kafka,
    skip_if_no_kafka_infra,
]


class TestKafkaEventBusConnection:
    """Tests for Kafka connection management."""

    async def test_connect_and_disconnect(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test connecting and disconnecting from Kafka."""
        bus = kafka_event_bus_factory()

        assert not bus.is_connected

        await bus.connect()
        assert bus.is_connected

        await bus.disconnect()
        assert not bus.is_connected

    async def test_context_manager(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test using event bus as async context manager."""
        bus = kafka_event_bus_factory()

        assert not bus.is_connected

        async with bus:
            assert bus.is_connected

        assert not bus.is_connected

    async def test_double_connect_is_safe(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that connecting when already connected is a no-op."""
        # Bus is already connected via fixture
        assert kafka_event_bus.is_connected

        # Second connect should not raise
        await kafka_event_bus.connect()
        assert kafka_event_bus.is_connected

    async def test_double_disconnect_is_safe(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test that disconnect can be called multiple times safely."""
        await kafka_event_bus.disconnect()
        assert not kafka_event_bus.is_connected

        # Second disconnect should not raise
        await kafka_event_bus.disconnect()
        assert not kafka_event_bus.is_connected


class TestKafkaEventBusShutdown:
    """Tests for graceful shutdown functionality."""

    async def test_graceful_shutdown(
        self,
        kafka_event_bus_factory: Any,
    ) -> None:
        """Test graceful shutdown of event bus."""
        bus = kafka_event_bus_factory()
        await bus.connect()

        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)
            await asyncio.sleep(0.1)  # Simulate work

        bus.subscribe(TestItemCreated, handler)

        # Publish events
        for i in range(5):
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name=f"Shutdown Item {i}",
                quantity=i,
            )
            await bus.publish([event])

        # Start consuming in background
        _consume_task = asyncio.create_task(bus.start_consuming())

        # Wait a bit for some events to be processed
        await asyncio.sleep(0.5)

        # Graceful shutdown
        await bus.shutdown(timeout=10.0)

        assert not bus.is_connected
        assert not bus.is_consuming

    async def test_stop_consuming_without_shutdown(
        self,
        kafka_event_bus: KafkaEventBus,
    ) -> None:
        """Test stopping consumer without full shutdown."""
        received_events: list[DomainEvent] = []

        async def handler(event: DomainEvent) -> None:
            received_events.append(event)

        kafka_event_bus.subscribe(TestItemCreated, handler)

        # Publish an event
        event = TestItemCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            name="Stop Test",
            quantity=1,
        )
        await kafka_event_bus.publish([event])

        # Start consuming
        consume_task = asyncio.create_task(kafka_event_bus.start_consuming())

        # Wait a bit
        await asyncio.sleep(0.5)

        # Stop consuming but stay connected
        await kafka_event_bus.stop_consuming()

        try:
            await asyncio.wait_for(consume_task, timeout=5.0)
        except TimeoutError:
            consume_task.cancel()

        assert not kafka_event_bus.is_consuming
        assert kafka_event_bus.is_connected  # Still connected
