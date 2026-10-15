"""Tests for different RabbitMQ exchange type routing behaviors (direct, fanout, topic)."""

from __future__ import annotations

import asyncio
import contextlib
from uuid import uuid4

import pytest

from eventsource import DomainEvent, EventRegistry, RabbitMQEventBus, RabbitMQEventBusConfig
from tests.integration.conftest import TestItemCreated, TestOrderCreated

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [pytest.mark.integration, pytest.mark.rabbitmq, skip_if_no_rabbitmq_infra]


async def _stop_consumer(bus: RabbitMQEventBus, task: asyncio.Task[None]) -> None:
    await bus.stop_consuming()
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except TimeoutError:
        task.cancel()


def _make_registry(*events: type[DomainEvent]) -> EventRegistry:
    registry = EventRegistry()
    for evt in events:
        registry.register(evt)
    return registry


def _make_bus(
    url: str,
    exchange_name: str,
    exchange_type: str,
    consumer_group: str,
    registry: EventRegistry,
    routing_key_pattern: str = "#",
) -> RabbitMQEventBus:
    config = RabbitMQEventBusConfig(
        rabbitmq_url=url,
        exchange_name=exchange_name,
        exchange_type=exchange_type,
        consumer_group=consumer_group,
        routing_key_pattern=routing_key_pattern,
        durable=False,
        auto_delete=True,
    )
    return RabbitMQEventBus(config=config, event_registry=registry)


class TestAdvancedExchangeTypes:
    async def test_direct_exchange_with_multiple_routing_keys(
        self, rabbitmq_connection_url: str
    ) -> None:
        sfx = uuid4().hex[:8]
        reg = _make_registry(TestItemCreated, TestOrderCreated)
        bus = _make_bus(
            rabbitmq_connection_url, f"test_direct_routing_{sfx}", "direct", f"direct_{sfx}", reg
        )
        try:
            await bus.connect()
            received_events: list[DomainEvent] = []
            bus.subscribe(TestItemCreated, received_events.append)  # type: ignore[arg-type]
            bus.subscribe(TestOrderCreated, received_events.append)  # type: ignore[arg-type]
            consume_task = asyncio.create_task(bus.start_consuming())
            await asyncio.sleep(0.5)

            item_event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name="Direct Item", quantity=5
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
        sfx = uuid4().hex[:8]
        ex = f"test_fanout_broadcast_{sfx}"
        reg = _make_registry(TestItemCreated)
        bus1 = _make_bus(rabbitmq_connection_url, ex, "fanout", f"fanout_group_a_{sfx}", reg)
        bus2 = _make_bus(rabbitmq_connection_url, ex, "fanout", f"fanout_group_b_{sfx}", reg)

        try:
            await bus1.connect()
            await bus2.connect()
            received_a, received_b = [], []
            evt_a, evt_b = asyncio.Event(), asyncio.Event()

            async def handler_a(event: DomainEvent) -> None:
                received_a.append(event)
                evt_a.set()

            async def handler_b(event: DomainEvent) -> None:
                received_b.append(event)
                evt_b.set()

            bus1.subscribe(TestItemCreated, handler_a)
            bus2.subscribe(TestItemCreated, handler_b)
            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())
            await asyncio.sleep(0.5)

            event = TestItemCreated(
                aggregate_id=uuid4(), aggregate_version=1, name="Fanout Test", quantity=10
            )
            await bus1.publish([event])
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.gather(evt_a.wait(), evt_b.wait()), timeout=10.0)

            await _stop_consumer(bus1, task1)
            await _stop_consumer(bus2, task2)
            assert len(received_a) == 1 and len(received_b) == 1
            assert received_a[0].event_id == event.event_id == received_b[0].event_id
        finally:
            await bus1.disconnect()
            await bus2.disconnect()

    async def test_topic_exchange_with_pattern_matching(self, rabbitmq_connection_url: str) -> None:
        sfx = uuid4().hex[:8]
        reg = _make_registry(TestItemCreated, TestOrderCreated)
        bus = _make_bus(
            rabbitmq_connection_url, f"test_topic_pattern_{sfx}", "topic", f"topic_{sfx}", reg
        )
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
                aggregate_id=uuid4(), aggregate_version=1, name="Topic Item", quantity=3
            )
            order_event = TestOrderCreated(
                aggregate_id=uuid4(), aggregate_version=1, customer_id=uuid4(), total_amount=99.99
            )
            await bus.publish([item_event, order_event])
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(events_received.wait(), timeout=10.0)

            await _stop_consumer(bus, consume_task)
            assert len(received_events) >= 2
            event_types = {e.event_type for e in received_events}
            assert {"TestItemCreated", "TestOrderCreated"} <= event_types
        finally:
            await bus.disconnect()

    @pytest.mark.slow
    async def test_topic_exchange_selective_pattern(self, rabbitmq_connection_url: str) -> None:
        sfx = uuid4().hex[:8]
        reg = _make_registry(TestItemCreated, TestOrderCreated)
        bus = _make_bus(
            rabbitmq_connection_url,
            f"test_topic_selective_{sfx}",
            "topic",
            f"topic_sel_{sfx}",
            reg,
            routing_key_pattern="TestItem.*",
        )
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
            assert len(item_events) == 1 and len(order_events) == 0
        finally:
            await bus.disconnect()
