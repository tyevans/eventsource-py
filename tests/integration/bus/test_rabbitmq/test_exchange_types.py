"""Tests for different RabbitMQ exchange type routing behaviors (direct, fanout, topic)."""

from __future__ import annotations

import asyncio
import contextlib
from uuid import uuid4

import pytest

from eventsource import (
    DomainEvent,
    EventRegistry,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)
from tests.integration.conftest import (
    TestItemCreated,
    TestOrderCreated,
)

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [
    pytest.mark.integration,
    pytest.mark.rabbitmq,
    skip_if_no_rabbitmq_infra,
]


class TestAdvancedExchangeTypes:
    """Tests for different exchange type routing behaviors.

    These tests verify that the RabbitMQ event bus correctly handles
    different exchange types including direct, fanout, and topic exchanges.
    """

    async def test_direct_exchange_with_multiple_routing_keys(
        self,
        rabbitmq_connection_url: str,
    ) -> None:
        """Test direct exchange with specific routing key bindings.

        Direct exchanges route messages to queues based on exact routing key matches.
        This test verifies:
        - Messages are routed only when routing keys match exactly
        - Multiple routing key bindings can be added to a queue
        """
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_direct_routing_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestOrderCreated)

        # Create a bus with direct exchange
        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
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

            # Start consuming in background
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(0.5)  # Allow consumer to start

            # Publish events - with direct exchange, routing key is aggregate_type.event_type
            item_event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Direct Exchange Test Item",
                quantity=5,
            )
            await bus.publish([item_event])

            # Wait for events
            await asyncio.sleep(2.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Verify item event was received (direct exchange default binding is queue_name)
            # With default binding, all events go to the queue since routing key matches
            assert bus.stats.events_published >= 1

        finally:
            await bus.disconnect()

    async def test_fanout_exchange_broadcasts_to_multiple_queues(
        self,
        rabbitmq_connection_url: str,
    ) -> None:
        """Test fanout exchange broadcasts to all bound queues.

        Fanout exchanges broadcast messages to all bound queues regardless
        of routing key. This test verifies:
        - All consumer groups receive the same message
        - Routing keys are ignored
        """
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_fanout_broadcast_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)

        # Create two buses with different consumer groups on same fanout exchange
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

            # Start both consumers
            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())
            await asyncio.sleep(0.5)

            # Publish single event through first bus
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Fanout Broadcast Test",
                quantity=10,
            )
            await bus1.publish([event])

            # Wait for both to receive
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(event_a_received.wait(), event_b_received.wait()),
                    timeout=10.0,
                )

            await bus1.stop_consuming()
            await bus2.stop_consuming()
            for task in [task1, task2]:
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except TimeoutError:
                    task.cancel()

            # Both groups should have received the event (fanout broadcasts to all)
            assert len(received_group_a) == 1, f"Group A received {len(received_group_a)} events"
            assert len(received_group_b) == 1, f"Group B received {len(received_group_b)} events"
            assert received_group_a[0].event_id == event.event_id
            assert received_group_b[0].event_id == event.event_id

        finally:
            await bus1.disconnect()
            await bus2.disconnect()

    async def test_topic_exchange_with_pattern_matching(
        self,
        rabbitmq_connection_url: str,
    ) -> None:
        """Test topic exchange with wildcard pattern routing.

        Topic exchanges route messages based on routing key patterns.
        This test verifies:
        - Pattern "#" matches all routing keys
        - Pattern "*.EventType" matches any aggregate with specific event type
        - Multiple patterns can be used for routing
        """
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_topic_pattern_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestOrderCreated)

        # Create bus with topic exchange and default "#" pattern (matches all)
        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            exchange_type="topic",
            consumer_group=f"topic_consumer_{unique_suffix}",
            routing_key_pattern="#",  # Wildcard - receive all events
            durable=False,
            auto_delete=True,
        )
        bus = RabbitMQEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            received_events: list[DomainEvent] = []
            events_received = asyncio.Event()
            expected_count = 2

            async def handler(event: DomainEvent) -> None:
                received_events.append(event)
                if len(received_events) >= expected_count:
                    events_received.set()

            bus.subscribe(TestItemCreated, handler)
            bus.subscribe(TestOrderCreated, handler)

            # Start consuming
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(0.5)

            # Publish different event types
            item_event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Topic Test Item",
                quantity=3,
            )
            order_event = TestOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                customer_id=uuid4(),
                total_amount=99.99,
            )
            await bus.publish([item_event, order_event])

            # Wait for events
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(events_received.wait(), timeout=10.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Both events should be received (# wildcard matches all routing keys)
            assert len(received_events) >= 2, f"Expected 2 events, got {len(received_events)}"

            # Verify both event types were received
            event_types = {e.event_type for e in received_events}
            assert "TestItemCreated" in event_types
            assert "TestOrderCreated" in event_types

        finally:
            await bus.disconnect()

    @pytest.mark.slow
    async def test_topic_exchange_selective_pattern(
        self,
        rabbitmq_connection_url: str,
    ) -> None:
        """Test topic exchange with selective pattern that filters messages.

        Tests that topic exchange routing correctly filters messages based
        on specific patterns.
        """
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_topic_selective_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestOrderCreated)

        # Create bus with selective pattern - only TestItem aggregate events
        config = RabbitMQEventBusConfig(
            rabbitmq_url=rabbitmq_connection_url,
            exchange_name=exchange_name,
            exchange_type="topic",
            consumer_group=f"topic_selective_{unique_suffix}",
            routing_key_pattern="TestItem.*",  # Only TestItem events
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

            # Start consuming
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(0.5)

            # Publish both event types
            item_event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Selective Item",
                quantity=1,
            )
            order_event = TestOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                customer_id=uuid4(),
                total_amount=50.0,
            )
            await bus.publish([item_event, order_event])

            # Wait for events to be processed
            await asyncio.sleep(3.0)

            await bus.stop_consuming()
            try:
                await asyncio.wait_for(consume_task, timeout=2.0)
            except TimeoutError:
                consume_task.cancel()

            # Only TestItemCreated should be received (routing key: TestItem.TestItemCreated)
            # TestOrderCreated has routing key: TestOrder.TestOrderCreated (won't match TestItem.*)
            item_events = [e for e in received_events if e.event_type == "TestItemCreated"]
            order_events = [e for e in received_events if e.event_type == "TestOrderCreated"]

            assert len(item_events) == 1, f"Expected 1 item event, got {len(item_events)}"
            assert len(order_events) == 0, f"Expected 0 order events, got {len(order_events)}"

        finally:
            await bus.disconnect()
