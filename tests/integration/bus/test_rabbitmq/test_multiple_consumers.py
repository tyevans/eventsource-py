"""Tests for multiple consumer scenarios (competing consumers and pub/sub consumer groups)."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from typing import Any
from uuid import uuid4

import pytest

from eventsource import DomainEvent, EventRegistry, RabbitMQEventBus, RabbitMQEventBusConfig
from tests.integration.conftest import TestItemCreated

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [pytest.mark.integration, pytest.mark.rabbitmq, skip_if_no_rabbitmq_infra]


class TestAdvancedMultipleConsumers:
    """Advanced tests for multiple consumer scenarios."""

    @pytest.mark.slow
    async def test_competing_consumers_load_distribution(
        self, rabbitmq_connection_url: str
    ) -> None:
        """Test that messages are distributed across competing consumers."""
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_competing_{unique_suffix}"
        consumer_group = f"competing_group_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)

        configs = [
            RabbitMQEventBusConfig(
                rabbitmq_url=rabbitmq_connection_url,
                exchange_name=exchange_name,
                exchange_type="topic",
                consumer_group=consumer_group,
                consumer_name=f"consumer_{i}_{unique_suffix}",
                prefetch_count=1,
                durable=False,
                auto_delete=True,
            )
            for i in (1, 2)
        ]
        bus1 = RabbitMQEventBus(config=configs[0], event_registry=registry)
        bus2 = RabbitMQEventBus(config=configs[1], event_registry=registry)

        try:
            await bus1.connect()
            await bus2.connect()

            received_by_consumer1: list[DomainEvent] = []
            received_by_consumer2: list[DomainEvent] = []
            all_received = asyncio.Event()
            event_count = 20

            async def handler1(event: DomainEvent) -> None:
                received_by_consumer1.append(event)
                await asyncio.sleep(0.05)
                if len(received_by_consumer1) + len(received_by_consumer2) >= event_count:
                    all_received.set()

            async def handler2(event: DomainEvent) -> None:
                received_by_consumer2.append(event)
                await asyncio.sleep(0.05)
                if len(received_by_consumer1) + len(received_by_consumer2) >= event_count:
                    all_received.set()

            bus1.subscribe(TestItemCreated, handler1)
            bus2.subscribe(TestItemCreated, handler2)

            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())
            await asyncio.sleep(0.5)

            events: list[DomainEvent] = [
                TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"Competing Consumer Item {i}",
                    quantity=i,
                )
                for i in range(event_count)
            ]
            await bus1.publish(events)

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(all_received.wait(), timeout=30.0)

            await bus1.stop_consuming()
            await bus2.stop_consuming()
            for task in [task1, task2]:
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except TimeoutError:
                    task.cancel()

            total_received = len(received_by_consumer1) + len(received_by_consumer2)
            assert total_received == event_count, f"Expected {event_count}, got {total_received}"
            all_event_ids = [e.event_id for e in received_by_consumer1 + received_by_consumer2]
            assert len(all_event_ids) == len(set(all_event_ids)), "Duplicate events detected"
        finally:
            await bus1.disconnect()
            await bus2.disconnect()

    @pytest.mark.slow
    async def test_multiple_consumer_groups_each_receive_all(
        self, rabbitmq_connection_url: str
    ) -> None:
        """Test that different consumer groups each receive all messages."""
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_multi_group_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)

        configs = [
            RabbitMQEventBusConfig(
                rabbitmq_url=rabbitmq_connection_url,
                exchange_name=exchange_name,
                exchange_type="topic",
                consumer_group=f"group_{i}_{unique_suffix}",
                durable=False,
                auto_delete=True,
            )
            for i in range(3)
        ]
        buses = [RabbitMQEventBus(config=config, event_registry=registry) for config in configs]

        try:
            for bus in buses:
                await bus.connect()

            received_per_group: list[list[DomainEvent]] = [[], [], []]
            events_ready = [asyncio.Event() for _ in range(3)]
            event_count = 5

            def create_handler(group_idx: int) -> Callable[[DomainEvent], Any]:
                async def handler(event: DomainEvent) -> None:
                    received_per_group[group_idx].append(event)
                    if len(received_per_group[group_idx]) >= event_count:
                        events_ready[group_idx].set()

                return handler

            for idx, bus in enumerate(buses):
                bus.subscribe(TestItemCreated, create_handler(idx))

            tasks = [asyncio.create_task(bus.start_consuming()) for bus in buses]
            await asyncio.sleep(0.5)

            events: list[DomainEvent] = [
                TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"Multi-Group Item {i}",
                    quantity=i,
                )
                for i in range(event_count)
            ]
            await buses[0].publish(events)

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(*[ev.wait() for ev in events_ready]), timeout=15.0
                )

            for bus in buses:
                await bus.stop_consuming()
            for task in tasks:
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except TimeoutError:
                    task.cancel()

            for idx, received in enumerate(received_per_group):
                assert len(received) == event_count, (
                    f"Group {idx} received {len(received)} events, expected {event_count}"
                )

            event_ids_per_group = [
                {e.event_id for e in received} for received in received_per_group
            ]
            assert event_ids_per_group[0] == event_ids_per_group[1] == event_ids_per_group[2], (
                "Consumer groups received different events"
            )
        finally:
            for bus in buses:
                await bus.disconnect()
