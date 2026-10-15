"""Tests for different RabbitMQ exchange type routing behaviors (direct, fanout, topic)."""

from __future__ import annotations

import asyncio
import contextlib
from uuid import uuid4

import pytest

from eventsource import DomainEvent, EventRegistry, RabbitMQEventBus, RabbitMQEventBusConfig
from tests.integration.conftest import TestItemCreated, TestOrderCreated

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [
    pytest.mark.integration,
    pytest.mark.rabbitmq,
    skip_if_no_rabbitmq_infra,
]


async def _stop_consumer(bus: RabbitMQEventBus, task: asyncio.Task[None]) -> None:
    """Stop consuming on a bus and await or cancel its background task."""
    await bus.stop_consuming()
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except TimeoutError:
        task.cancel()


class TestAdvancedExchangeTypes:
    """Tests for different exchange type routing behaviors (direct, fanout, topic)."""

    async def test_direct_exchange_with_multiple_routing_keys(
        self, rabbitmq_connection_url: str
    ) -> None:
        """Test direct exchange with specific routing key bindings."""
        unique_suffix = uuid4().hex[:8]
        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestOrderCreated)

        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=f"test_direct_routing_{unique_suffix}",
            exchange_type="direct",
            consumer_group=f"direct_consumer_{unique_suffix}",
            durable=False,
            auto_delete=True,
        )
        bus = RabbitMQEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()
            received_events: list[DomainEvent] = []

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)

            bus.subscribe(TestItemCreated, handler)
            bus.subscribe(TestOrderCreated, handler)

            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(0.5)

            item_event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Direct Exchange Test Item",
                quantity=5,
            )
            await bus.publish([item_event])
            await asyncio.sleep(2.0)

            await _stop_consumer(bus, consume_task)
            assert bus.stats.events_published >= 1
        finally:
            await bus.disconnect()

    async def test_fanout_exchange_broadcasts_to_multiple_queues(
        self, rabbitmq_connection_url: str
    ) -> None:
        """Test fanout exchange broadcasts to all bound queues."""
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_fanout_broadcast_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)

        config1 = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            exchange_type="fanout",
            consumer_group=f"fanout_group_a_{unique_suffix}",
            durable=False,
            auto_delete=True,
        )
        config2 = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            exchange_type="fanout",
            consumer_group=f"fanout_group_b_{unique_suffix}",
            durable=False,
            auto_delete=True,
        )
        bus1 = RabbitMQEventBus(config=config1, event_registry=registry)
        bus2 = RabbitMQEventBus(config=config2, event_registry=registry)

        try:
            await bus1.connect()
            await bus2.connect()

            received_group_a: list[DomainEvent] = []
            received_group_b: list[DomainEvent] = []
            event_a_received = asyncio.Event()
            event_b_received = asyncio.Event()

            async def handler_a(event: DomainEvent) -> None:
                received_group_a.append(event)
                event_a_received.set()

            async def handler_b(event: DomainEvent) -> None:
                received_group_b.append(event)
                event_b_received.set()

            bus1.subscribe(TestItemCreated, handler_a)
            bus2.subscribe(TestItemCreated, handler_b)

            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())
            await asyncio.sleep(0.5)

            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Fanout Broadcast Test",
                quantity=10,
            )
            await bus1.publish([event])

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(event_a_received.wait(), event_b_received.wait()),
                    timeout=10.0,
                )

            await _stop_consumer(bus1, task1)
            await _stop_consumer(bus2, task2)

            assert len(received_group_a) == 1, f"Group A received {len(received_group_a)} events"
            assert len(received_group_b) == 1, f"Group B received {len(received_group_b)} events"
            assert received_group_a[0].event_id == event.event_id
            assert received_group_b[0].event_id == event.event_id
        finally:
            await bus1.disconnect()
            await bus2.disconnect()

    async def test_topic_exchange_with_pattern_matching(self, rabbitmq_connection_url: str) -> None:
        """Test topic exchange with wildcard pattern routing."""
        unique_suffix = uuid4().hex[:8]
        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestOrderCreated)

        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=f"test_topic_pattern_{unique_suffix}",
            exchange_type="topic",
            consumer_group=f"topic_consumer_{unique_suffix}",
            routing_key_pattern="#",
            durable=False,
            auto_delete=True,
        )
        bus = RabbitMQEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()
            received_events: list[DomainEvent] = []
            events_received = asyncio.Event()

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)
                if len(received_events) >= 2:
                    events_received.set()

            bus.subscribe(TestItemCreated, handler)
            bus.subscribe(TestOrderCreated, handler)

            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(0.5)

            item_event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name="Topic Test Item", quantity=3
            )
            order_event = TestOrderCreated(
                aggregate_id=uuid4(), aggregate_version=1, customer_id=uuid4(), total_amount=99.99
            )
            await bus.publish([item_event, order_event])

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(events_received.wait(), timeout=10.0)

            await _stop_consumer(bus, consume_task)

            assert len(received_events) >= 2, f"Expected 2 events, got {len(received_events)}"
            event_types = {e.event_type for e in received_events}
            assert "TestItemCreated" in event_types
            assert "TestOrderCreated" in event_types
        finally:
            await bus.disconnect()

    @pytest.mark.slow
    async def test_topic_exchange_selective_pattern(self, rabbitmq_connection_url: str) -> None:
        """Test topic exchange with selective pattern that filters messages."""
        unique_suffix = uuid4().hex[:8]
        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestOrderCreated)

        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=f"test_topic_selective_{unique_suffix}",
            exchange_type="topic",
            consumer_group=f"topic_selective_{unique_suffix}",
            routing_key_pattern="TestItem.*",
            durable=False,
            auto_delete=True,
        )
        bus = RabbitMQEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()
            received_events: list[DomainEvent] = []

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)

            bus.subscribe(TestItemCreated, handler)
            bus.subscribe(TestOrderCreated, handler)

            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(0.5)

            item_event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name="Selective Item", quantity=1
            )
            order_event = TestOrderCreated(
                aggregate_id=uuid4(), aggregate_version=1, customer_id=uuid4(), total_amount=50.0
            )
            await bus.publish([item_event, order_event])
            await asyncio.sleep(3.0)

            await _stop_consumer(bus, consume_task)

            item_events = [e for e in received_events if e.event_type == "TestItemCreated"]
            order_events = [e for e in received_events if e.event_type == "TestOrderCreated"]

            assert len(item_events) == 1, f"Expected 1 item event, got {len(item_events)}"
            assert len(order_events) == 0, f"Expected 0 order events, got {len(order_events)}"
        finally:
            await bus.disconnect()
